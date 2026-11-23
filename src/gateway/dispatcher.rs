//! Gateway のイベントの振り分けと、アクターを通さないコマンドの実行。
//! - `route` はメッセージを振り分ける純粋な関数。Bot、DM、起動の準備中、メンション、コマンドを見る
//! - `Dispatcher::dispatch` は 1 イベントを振り分ける。shard のループから受信の順に呼ぶ。
//!   アクターへの送信はその場で行い (順序を保つ)、待ちのある処理だけを別タスクで行う
//! - 起動の準備: `Ready` と `GuildCreate` を `ReadyGate` に渡し、開いたら復元 (`restore_all`) を待ってから受付を始める。
//!   それまでのコマンドには `error.starting` を返す
//! - 権限: 実行者の役職は `MessageCreate` の `member` から、ギルドの所有者と役職の権限はキャッシュから取る
//!   (`GUILD_MEMBERS` の Intent は使わない)。サーバー管理者の判定はギルド全体の権限で行い、チャンネルは見ない。
//!   タイムアウト中のメンバーは、サーバー管理者でなければ
//!   `?setting` の権限を持たないものとして扱う (`PermissionCalculator` はタイムアウトを見ないため)
//! - DB を待つ処理は `DB_WAIT_LIMIT` で打ち切り、`error.read_failed` などを返す
//! - Discord への送信の失敗はログだけ残す

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use twilight_cache_inmemory::DefaultInMemoryCache;
use twilight_gateway::Event;
use twilight_http::Client;
use twilight_model::application::interaction::{Interaction, InteractionData, InteractionType};
use twilight_model::channel::message::component::Component;
use twilight_model::channel::Message;
use twilight_model::http::interaction::{InteractionResponse, InteractionResponseType};
use twilight_model::util::Timestamp;
use twilight_util::builder::InteractionResponseDataBuilder;

use super::cache_view::{
	guild_permission_data, listeners_in_channel, restore_listeners, voice_channel_of,
	GuildPermissionData,
};
use super::notifier::{within_send_timeout, DiscordNotifier};
use super::page_timers::{Due, PageTimers, Settled};
use super::permissions::is_server_admin;
use super::ready::ReadyGate;
use crate::commands::admin::apply_setting;
use crate::commands::info::{help_page, history_page, queue_page, Reply};
use crate::commands::{parse, ParseError, ParsedCommand, SettingCommand};
use crate::config::{Config, LimitsConfig};
use crate::db::{now_secs, settings, DbError, DbHandles};
use crate::domain::{ChannelId, GuildId, UserId};
use crate::guild::message::{CommandContext, GuildCommand, GuildMessage};
use crate::guild::notifier::{MessageRef, Notice, Notifier};
use crate::guild::registry::{GuildRegistry, SendError};
use crate::guild::restore::restore_all;
use crate::lang::GuildLangs;
use crate::messages::{MessageCatalog, Messages};
use crate::ui::pagination::{decode, disable_all, is_expired, PageKind};

/// ボイスの状態の変化をアクターへ送り直すまでの間隔。混雑時だけ使う。
const VOICE_RETRY_INTERVAL: Duration = Duration::from_millis(100);
/// ボタンのページを作るのを待つ上限 (ボタンが押された時刻から数える)。
/// Discord はボタンへの応答を 3 秒まで待つため、送信の分を残して打ち切る。
pub const PAGE_BUILD_LIMIT: Duration = Duration::from_secs(2);
/// ボタンが押されてから受信するまでの遅れとして、上限から差し引く最大の値。
/// 時計のずれで遅れを大きく見積もっても、ページを作る時間がこれ以上は削られないようにする
pub const MAX_TRUSTED_EVENT_DELAY: Duration = Duration::from_secs(1);
/// Discord の snowflake の時刻の起点 (2015-01-01T00:00:00Z の UNIX ミリ秒)。
const DISCORD_EPOCH_MS: u64 = 1_420_070_400_000;
/// snowflake の中で時刻より下にあるビットの数。
const SNOWFLAKE_TIMESTAMP_SHIFT: u32 = 22;
/// テキストコマンドが DB の読み書きを待つ上限。過ぎたら失敗として利用者へ返す
/// (DB の混雑で返信が無いまま待たせないため)。
pub const DB_WAIT_LIMIT: Duration = Duration::from_secs(5);

/// 利用者へ返す文面のキー。
mod keys {
	pub const STARTING: &str = "error.starting";
	pub const MENTION: &str = "help.mention";
	pub const BUSY: &str = "error.busy";
	pub const NO_PERMISSION: &str = "error.no_permission";
	pub const INVALID_ARGUMENT: &str = "error.invalid_argument";
	pub const OUT_OF_RANGE: &str = "error.out_of_range";
	pub const READ_FAILED: &str = "error.read_failed";
	pub const SAVE_FAILED: &str = "error.save_failed";
	pub const SAVE_SLOW: &str = "error.save_slow";
}

/// 振り分けに使うメッセージの中身。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomingMessage {
	/// DM なら None
	pub guild_id: Option<GuildId>,
	pub author_is_bot: bool,
	pub content: String,
	/// メンションされた利用者 (返信の通知を含む)
	pub mentions: Vec<UserId>,
}

/// メッセージの振り分けの結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
	/// 応答しない
	Ignore,
	/// 起動の準備中 (復元の前)。`error.starting` を返す
	Starting,
	/// Bot へのメンション。`help.mention` を返す
	Mention,
	/// コマンドの引数の誤り
	ParseError(ParseError),
	/// 解析済みのコマンド
	Command(ParsedCommand),
}

/// メッセージを振り分ける (純粋)。
///
/// - DM と Bot のメッセージは無視する
/// - コマンドなら、受付の前は `Starting`、受付中は解析の結果
/// - コマンドでなく、本文に Bot へのメンション (`<@id>` か `<@!id>`) があり、`mentions` にも Bot がいれば `Mention`
///   (受付の前でも返す。状態に触れない案内のため)。返信の通知だけで本文にメンションが無いものは無視する
///
/// # 引数
/// - `prefix`: コマンドの接頭辞
/// - `limits`: 数値引数の上限
/// - `accepting`: コマンドを受け付けているか (起動の準備が済んだか)
/// - `bot_id`: この Bot のユーザー ID
/// - `message`: メッセージの中身
///
/// # 呼び出し例
/// ```ignore
/// match route("?", &config.limits, true, bot_id, &incoming) { Route::Command(cmd) => { /* ... */ } _ => {} }
/// ```
pub fn route(
	prefix: &str,
	limits: &LimitsConfig,
	accepting: bool,
	bot_id: UserId,
	message: &IncomingMessage,
) -> Route {
	if message.guild_id.is_none() || message.author_is_bot {
		return Route::Ignore;
	}
	if let Some(parsed) = parse(prefix, &message.content, limits) {
		if !accepting {
			return Route::Starting;
		}
		return match parsed {
			Ok(command) => Route::Command(command),
			Err(error) => Route::ParseError(error),
		};
	}
	if mentions_bot(message, bot_id) {
		return Route::Mention;
	}
	Route::Ignore
}

/// 本文と `mentions` の両方に Bot へのメンションがあるか。
fn mentions_bot(message: &IncomingMessage, bot_id: UserId) -> bool {
	if !message.mentions.contains(&bot_id) {
		return false;
	}
	let plain = format!("<@{bot_id}>");
	let nick = format!("<@!{bot_id}>");
	message.content.contains(&plain) || message.content.contains(&nick)
}

/// Discord の時刻を UNIX 秒にする (秒未満は切り捨て)。
///
/// # 呼び出し例
/// ```ignore
/// let at = unix_secs(message.timestamp);
/// ```
pub fn unix_secs(timestamp: Timestamp) -> i64 {
	timestamp.as_secs()
}

/// タイムアウト中か。`until` が今より後なら true。
///
/// # 引数
/// - `until`: メンバーの `communication_disabled_until`
/// - `now`: 今の UNIX 秒
///
/// # 呼び出し例
/// ```ignore
/// let timed_out = is_timed_out(member.communication_disabled_until, now_secs());
/// ```
pub fn is_timed_out(until: Option<Timestamp>, now: i64) -> bool {
	until.is_some_and(|until| unix_secs(until) > now)
}

/// `?setting` の権限の判定の結果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingAccess {
	/// サーバー管理者なので許す
	Allowed,
	/// Bot 管理者 (DB) かどうかで決める
	CheckBotAdmin,
	/// タイムアウト中のため許さない
	Denied,
}

/// `?setting` の権限を、DB を引く前に判定する。
///
/// - サーバー管理者は許す。それ以外は Bot 管理者かを DB で確かめる
/// - タイムアウト中のメンバーは、Bot 管理者でも許さない
///
/// # 呼び出し例
/// ```ignore
/// match setting_access(admin, timed_out) { SettingAccess::CheckBotAdmin => { /* DB */ } _ => {} }
/// ```
pub fn setting_access(server_admin: bool, timed_out: bool) -> SettingAccess {
	if server_admin {
		SettingAccess::Allowed
	} else if timed_out {
		SettingAccess::Denied
	} else {
		SettingAccess::CheckBotAdmin
	}
}

/// Discord の snowflake (ID) が作られた時刻を UNIX ミリ秒にする。
///
/// # 引数
/// - `snowflake`: ID の値
///
/// # 戻り値
/// 作られた時刻 (UNIX ミリ秒)
///
/// # 呼び出し例
/// ```ignore
/// let created = snowflake_unix_ms(interaction.id.get());
/// ```
pub fn snowflake_unix_ms(snowflake: u64) -> u64 {
	(snowflake >> SNOWFLAKE_TIMESTAMP_SHIFT).saturating_add(DISCORD_EPOCH_MS)
}

/// 今の時刻の UNIX ミリ秒。時計が起点より前なら None。
fn unix_ms_now() -> Option<u64> {
	let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
	u64::try_from(elapsed.as_millis()).ok()
}

/// ボタンを受信した時点から、ページを作るのに使える時間。
///
/// - `PAGE_BUILD_LIMIT` から、押されてから受信するまでの遅れを差し引く
/// - 遅れは `MAX_TRUSTED_EVENT_DELAY` までしか差し引かない (手元と Discord の時計のずれへの備え)
/// - 受信の時刻が押された時刻より前 (時計のずれ) なら遅れは 0 とする
///
/// # 引数
/// - `pressed_unix_ms`: ボタンが押された時刻 (インタラクションの ID の時刻)
/// - `received_unix_ms`: 受信した時刻
///
/// # 戻り値
/// 使える時間
///
/// # 呼び出し例
/// ```ignore
/// let deadline = tokio::time::Instant::now() + page_build_budget(pressed, received);
/// ```
pub fn page_build_budget(pressed_unix_ms: u64, received_unix_ms: u64) -> Duration {
	let delay = Duration::from_millis(received_unix_ms.saturating_sub(pressed_unix_ms));
	PAGE_BUILD_LIMIT.saturating_sub(delay.min(MAX_TRUSTED_EVENT_DELAY))
}

/// Dispatcher が使う部品。アプリの起動時に組み立てて渡す。
pub struct DispatcherParts {
	pub http: Arc<Client>,
	pub cache: Arc<DefaultInMemoryCache>,
	/// Discord への通知の送信口 (アクターと共有する)
	pub notifier: Arc<DiscordNotifier>,
	pub registry: Arc<GuildRegistry>,
	pub db: DbHandles,
	pub config: Arc<Config>,
	/// 言語ごとのメッセージ表
	pub catalog: Arc<MessageCatalog>,
	/// サーバーごとの言語の表
	pub langs: Arc<GuildLangs>,
	/// この Bot のユーザー ID
	pub bot_id: UserId,
}

/// Gateway のイベントの振り分け役。
///
/// 呼び出し方:
/// ```ignore
/// let dispatcher = Dispatcher::new(parts);
/// dispatcher.dispatch(event);
/// // 終了の始めに
/// dispatcher.stop_accepting();
/// ```
pub struct Dispatcher {
	http: Arc<Client>,
	cache: Arc<DefaultInMemoryCache>,
	registry: Arc<GuildRegistry>,
	db: DbHandles,
	config: Arc<Config>,
	catalog: Arc<MessageCatalog>,
	langs: Arc<GuildLangs>,
	notifier: Arc<DiscordNotifier>,
	/// ページ送りのボタンを灰色にする期限
	page_timers: Arc<PageTimers>,
	bot_id: UserId,
	/// 起動の準備 (復元) が済み、コマンドを受け付けているか
	accepting: AtomicBool,
	/// 終了の処理に入ったか。以後はメッセージとボタンに応答しない
	stopped: AtomicBool,
	ready: Mutex<ReadyGate>,
}

impl Dispatcher {
	/// 振り分け役を作る。受付は起動の準備が済むまで閉じている。
	///
	/// # 引数
	/// - `parts`: 使う部品
	///
	/// # 呼び出し例
	/// ```ignore
	/// let dispatcher = Dispatcher::new(DispatcherParts { http, cache, notifier, registry, db, config, catalog, langs, bot_id });
	/// ```
	pub fn new(parts: DispatcherParts) -> Arc<Self> {
		let wait = Duration::from_secs(parts.config.gateway.ready_wait_secs);
		Arc::new(Self {
			http: parts.http,
			cache: parts.cache,
			registry: parts.registry,
			db: parts.db,
			config: parts.config,
			catalog: parts.catalog,
			langs: parts.langs,
			notifier: parts.notifier,
			page_timers: Arc::new(PageTimers::new()),
			bot_id: parts.bot_id,
			accepting: AtomicBool::new(false),
			stopped: AtomicBool::new(false),
			ready: Mutex::new(ReadyGate::new(wait)),
		})
	}

	/// 1 イベントを振り分ける。shard のループから受信の順に呼ぶ。待たずに戻る。
	///
	/// - キャッシュと Songbird への反映は呼び出し側 (shard のループ) が先に済ませる
	/// - コマンドの解析とアクターへの送信、聴者の数の送信はその場で行う
	///   (受信の順にアクターへ届けるため。イベントごとに spawn すると `?clear` と `?p` などの順序が入れ替わりうる)
	/// - 返信、DB、HTTP など待ちのある処理だけを別タスクで行う
	///
	/// # 引数
	/// - `event`: Gateway のイベント
	///
	/// # 呼び出し例
	/// ```ignore
	/// dispatcher.dispatch(event);
	/// ```
	pub fn dispatch(self: &Arc<Self>, event: Event) {
		match event {
			Event::Ready(ready) => {
				let guilds: Vec<GuildId> = ready.guilds.iter().map(|guild| guild.id).collect();
				tracing::info!(guilds = guilds.len(), "Gateway の準備ができました");
				tokio::spawn(Arc::clone(self).on_ready(guilds));
			}
			Event::GuildCreate(guild) => {
				self.lock_ready().on_guild_create(guild.id());
				let this = Arc::clone(self);
				tokio::spawn(async move { this.check_ready().await });
			}
			Event::MessageCreate(message) => self.on_message(message.0),
			Event::InteractionCreate(interaction) => {
				// 応答の期限はボタンが押された時刻から数えるため、受信した時点で打ち切りの時刻を決める
				let pressed = snowflake_unix_ms(interaction.id.get());
				let budget = unix_ms_now().map_or(PAGE_BUILD_LIMIT, |received| {
					page_build_budget(pressed, received)
				});
				let deadline = tokio::time::Instant::now() + budget;
				let this = Arc::clone(self);
				tokio::spawn(async move { this.on_interaction(interaction.0, deadline).await });
			}
			Event::VoiceStateUpdate(update) => {
				if let Some(guild) = update.guild_id {
					self.on_voice_state(guild, update.user_id);
				}
			}
			_ => {}
		}
	}

	/// 新しいコマンドの受付を止める (終了の 1 段目)。以後はメッセージとボタンに応答しない。
	///
	/// # 呼び出し例
	/// ```ignore
	/// dispatcher.stop_accepting();
	/// ```
	pub fn stop_accepting(&self) {
		self.stopped.store(true, Ordering::SeqCst);
		self.accepting.store(false, Ordering::SeqCst);
	}

	/// 起動の準備の門のロック。毒されていても中身を使う (状態は単純な集合と時刻のため)。
	fn lock_ready(&self) -> std::sync::MutexGuard<'_, ReadyGate> {
		self.ready.lock().unwrap_or_else(PoisonError::into_inner)
	}

	/// `Ready` を受けた。門に渡し、期限の確認を予約する。
	///
	/// - 2 回目以降 (再接続) は門が無視する。予約した確認も門が開かないので何もしない
	async fn on_ready(self: Arc<Self>, guilds: Vec<GuildId>) {
		self.lock_ready().on_ready(guilds, Instant::now());
		let wait = Duration::from_secs(self.config.gateway.ready_wait_secs);
		let this = Arc::clone(&self);
		tokio::spawn(async move {
			tokio::time::sleep(wait).await;
			this.check_ready().await;
		});
		self.check_ready().await;
	}

	/// 門が開いたら、復元を待ってからコマンドの受付を始める。
	///
	/// - 門は 1 回しか開かないので、復元も 1 回だけ行う
	/// - 終了の処理に入っていたら受付を始めない
	async fn check_ready(&self) {
		let opened = self.lock_ready().poll(Instant::now());
		if !opened {
			return;
		}
		tracing::info!(
			"全ギルドの情報が揃った (または待ちの期限を過ぎた) ため、再生状態を復元します"
		);
		let stagger = Duration::from_millis(self.config.restore.stagger_ms);
		let cache = &self.cache;
		let bot_id = self.bot_id;
		restore_all(
			&self.registry,
			&self.db,
			|guild, channel| restore_listeners(cache, guild, channel, bot_id),
			stagger,
		)
		.await;
		self.accepting.store(true, Ordering::SeqCst);
		// 受付を開いた後に終了の合図を確かめる (確かめてから開くと、その間に `stop_accepting` が入ったとき
		// 終了中に受付が開いたままになるため)
		if self.stopped.load(Ordering::SeqCst) {
			self.accepting.store(false, Ordering::SeqCst);
			return;
		}
		tracing::info!("コマンドの受付を始めます");
	}

	/// サーバーの言語のメッセージ表。サーバーが None (DM など) か、設定が無ければ既定の言語。
	///
	/// # 引数
	/// - `guild`: メッセージやボタンのサーバー
	///
	/// # 戻り値
	/// そのサーバーの言語のメッセージ表
	///
	/// # 呼び出し例
	/// ```ignore
	/// let text = self.messages_for(Some(guild)).render("help.usage_vol", &[]);
	/// ```
	fn messages_for(&self, guild: Option<GuildId>) -> &Messages {
		self.catalog.get(self.langs.get(guild))
	}

	/// 通知を送る。失敗は送信側でログに残す。
	async fn reply(
		&self,
		channel: ChannelId,
		key: &'static str,
		args: Vec<(&'static str, String)>,
	) {
		self.notifier.send(channel, Notice::text(key, args)).await;
	}

	/// メッセージを振り分ける。待たずに戻る。
	///
	/// - アクターへ渡すコマンドはその場で送る (受信の順序を保つため)
	/// - 返信とアクターを通さないコマンドは別タスクで行う
	fn on_message(self: &Arc<Self>, mut message: Message) {
		if self.stopped.load(Ordering::SeqCst) {
			return;
		}
		let incoming = IncomingMessage {
			guild_id: message.guild_id,
			author_is_bot: message.author.bot,
			content: std::mem::take(&mut message.content),
			mentions: message.mentions.iter().map(|mention| mention.id).collect(),
		};
		let accepting = self.accepting.load(Ordering::SeqCst);
		let routed = route(
			&self.config.bot.prefix,
			&self.config.limits,
			accepting,
			self.bot_id,
			&incoming,
		);
		let channel = message.channel_id;
		let this = Arc::clone(self);
		match routed {
			Route::Ignore => {}
			Route::Starting => {
				tokio::spawn(async move { this.reply(channel, keys::STARTING, Vec::new()).await });
			}
			Route::Mention => {
				tokio::spawn(async move { this.reply(channel, keys::MENTION, Vec::new()).await });
			}
			Route::ParseError(error) => {
				let guild = incoming.guild_id;
				tokio::spawn(async move { this.reply_parse_error(guild, channel, error).await });
			}
			Route::Command(ParsedCommand::Guild(command)) => {
				// route は DM を Ignore にするため、ここでは常に Some
				if let Some(guild) = incoming.guild_id {
					self.send_to_actor(guild, &message, command);
				}
			}
			Route::Command(command) => {
				if let Some(guild) = incoming.guild_id {
					tokio::spawn(async move { this.run_command(guild, &message, command).await });
				}
			}
		}
	}

	/// 引数の誤りを返す。
	///
	/// - 使い方の誤りは `error.invalid_argument` に、そのコマンドの使い方 (`help.usage_*`) を添える
	/// - 範囲外は `error.out_of_range`
	/// - 使い方はサーバーの言語で描く (サーバーが無ければ既定の言語)
	async fn reply_parse_error(
		&self,
		guild: Option<GuildId>,
		channel: ChannelId,
		error: ParseError,
	) {
		match error {
			ParseError::Usage { usage } => {
				let prefix = self.config.bot.prefix.as_str();
				let text = self
					.messages_for(guild)
					.render(usage, &[("prefix", prefix)]);
				self.reply(channel, keys::INVALID_ARGUMENT, vec![("usage", text)])
					.await;
			}
			ParseError::OutOfRange { min, max } => {
				let args = vec![("min", min.to_string()), ("max", max.to_string())];
				self.reply(channel, keys::OUT_OF_RANGE, args).await;
			}
		}
	}

	/// 解析済みのコマンドを実行する。
	async fn run_command(
		self: &Arc<Self>,
		guild: GuildId,
		message: &Message,
		command: ParsedCommand,
	) {
		let channel = message.channel_id;
		match command {
			ParsedCommand::Guild(command) => self.send_to_actor(guild, message, command),
			ParsedCommand::Help => {
				let prefix = self.config.bot.prefix.as_str();
				let reply = help_page(
					self.messages_for(Some(guild)),
					&self.config.ui,
					&self.config.limits,
					prefix,
					0,
					now_secs(),
				)
				.await;
				self.send_reply(channel, reply).await;
			}
			ParsedCommand::QueueList => self.send_list(guild, channel, PageKind::Queue).await,
			ParsedCommand::History => self.send_list(guild, channel, PageKind::History).await,
			ParsedCommand::Setting(command) => self.setting(guild, message, command).await,
		}
	}

	/// アクターへコマンドをその場で送る。メールボックスが満杯なら `error.busy` を別タスクで返す。
	fn send_to_actor(self: &Arc<Self>, guild: GuildId, message: &Message, command: GuildCommand) {
		let context = CommandContext {
			text_channel: message.channel_id,
			user: message.author.id,
			voice_channel: voice_channel_of(&self.cache, guild, message.author.id),
		};
		let sent = self
			.registry
			.get_or_spawn(guild)
			.try_send(GuildMessage::Command { command, context });
		if sent.is_err() {
			tracing::warn!(
				guild = guild.get(),
				"アクターが混み合っているためコマンドを断りました"
			);
			let this = Arc::clone(self);
			let channel = message.channel_id;
			tokio::spawn(async move { this.reply(channel, keys::BUSY, Vec::new()).await });
		}
	}

	/// ページの内容を作る。
	///
	/// # 戻り値
	/// キューと履歴でギルドが無い (DM) なら None
	///
	/// # エラー
	/// - DB の読み取りの失敗
	async fn page(
		&self,
		kind: PageKind,
		guild: Option<GuildId>,
		page: u32,
		issued_at: i64,
	) -> Result<Option<Reply>, DbError> {
		let messages = self.messages_for(guild);
		let ui = &self.config.ui;
		let reply = match (kind, guild) {
			(PageKind::Help, _) => {
				let prefix = self.config.bot.prefix.as_str();
				help_page(messages, ui, &self.config.limits, prefix, page, issued_at).await
			}
			(PageKind::Queue, Some(guild)) => {
				let prefix = self.config.bot.prefix.as_str();
				queue_page(&self.db, messages, ui, prefix, guild, page, issued_at).await?
			}
			(PageKind::History, Some(guild)) => {
				let prefix = self.config.bot.prefix.as_str();
				history_page(&self.db, messages, ui, prefix, guild, page, issued_at).await?
			}
			(PageKind::Queue | PageKind::History, None) => return Ok(None),
		};
		Ok(Some(reply))
	}

	/// `?qlist` と `?history` の最初のページを送る。
	///
	/// - 読めない、または `DB_WAIT_LIMIT` を過ぎたら `error.read_failed`
	async fn send_list(&self, guild: GuildId, channel: ChannelId, kind: PageKind) {
		let page = self.page(kind, Some(guild), 0, now_secs());
		match tokio::time::timeout(DB_WAIT_LIMIT, page).await {
			Ok(Ok(Some(reply))) => self.send_reply(channel, reply).await,
			Ok(Ok(None)) => {}
			Ok(Err(error)) => {
				tracing::error!(guild = guild.get(), %error, "一覧を読めませんでした");
				self.reply(channel, keys::READ_FAILED, Vec::new()).await;
			}
			Err(_) => {
				tracing::warn!(
					guild = guild.get(),
					"一覧の読み取りが上限時間を過ぎたため打ち切りました"
				);
				self.reply(channel, keys::READ_FAILED, Vec::new()).await;
			}
		}
	}

	/// Embed とボタンを送る。ボタンがあれば、灰色にする期限を記録する。
	async fn send_reply(&self, channel: ChannelId, reply: Reply) {
		let sent = self
			.notifier
			.send_embed(channel, reply.embed, &reply.components)
			.await;
		if let Some(target) = sent {
			self.arm_buttons(target, reply.components);
		}
	}

	/// ボタンを灰色にする期限を、今から `ui.pagination_timeout_secs` 後にする (旧 Bot と同じく最後の操作から数える)。
	///
	/// - 記録が無ければ作り、期限のタスクを起動する。あれば期限とボタンを差し替えるだけ (タスクが待ち直す)
	/// - 期限が来たら、記録したボタンを灰色にする。編集の失敗 (削除済みなど) は debug のログだけ残す
	/// - 灰色にする編集の送信中にボタンが押されて作り直されたら、送った後に新しいボタンへ戻す
	///   (灰色の編集が押下の応答より後に届くと、新しいページのボタンが灰色のまま残るため)
	/// - ボタンが無ければ記録を消す (ボタンの無いページに書き換えた後に、古いボタンを灰色で戻さないため)
	///
	/// # 引数
	/// - `target`: ボタンを付けたメッセージ
	/// - `components`: 今のボタン
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.arm_buttons(target, reply.components);
	/// ```
	fn arm_buttons(&self, target: MessageRef, components: Vec<Component>) {
		if components.is_empty() {
			self.page_timers.forget(target.message);
			return;
		}
		let wait = Duration::from_secs(self.config.ui.pagination_timeout_secs);
		let deadline = tokio::time::Instant::now() + wait;
		if !self.page_timers.touch(target, components, deadline) {
			return;
		}
		let timers = Arc::clone(&self.page_timers);
		let notifier = Arc::clone(&self.notifier);
		tokio::spawn(async move {
			let mut at = deadline;
			loop {
				tokio::time::sleep_until(at).await;
				let (target, components) =
					match timers.due(target.message, tokio::time::Instant::now()) {
						Due::Wait(next) => {
							at = next;
							continue;
						}
						Due::Gone => return,
						Due::Expired { target, components } => (target, components),
					};
				let mut sending = disable_all(&components);
				loop {
					notifier.set_components(target, &sending).await;
					match timers.settle(target.message) {
						Settled::Done => return,
						Settled::Resend(components) => sending = components,
						Settled::Wait(next) => {
							at = next;
							break;
						}
					}
				}
			}
		});
	}

	/// 実行者の、ギルド全体の権限の入力と、タイムアウト中か。
	///
	/// # 戻り値
	/// (権限の入力、タイムアウト中か)。`member` が無い、またはギルドか `@everyone` がキャッシュに無ければ入力は None
	fn author_permissions(
		&self,
		guild: GuildId,
		message: &Message,
	) -> (Option<GuildPermissionData>, bool) {
		let Some(member) = message.member.as_ref() else {
			return (None, false);
		};
		let timed_out = is_timed_out(member.communication_disabled_until, now_secs());
		let data = guild_permission_data(&self.cache, guild, &member.roles);
		(data, timed_out)
	}

	/// `?setting`: サーバー管理者か Bot 管理者なら設定を変える。
	///
	/// - タイムアウト中のメンバーは、サーバー管理者でなければ断る
	/// - Bot 管理者かを読めない、または `DB_WAIT_LIMIT` を過ぎたら `error.read_failed`
	async fn setting(&self, guild: GuildId, message: &Message, command: SettingCommand) {
		let channel = message.channel_id;
		let user = message.author.id;
		let (data, timed_out) = self.author_permissions(guild, message);
		let server_admin = data
			.as_ref()
			.is_some_and(|data| is_server_admin(&data.input(guild, user)));
		let allowed = match setting_access(server_admin, timed_out) {
			SettingAccess::Allowed => true,
			SettingAccess::Denied => false,
			SettingAccess::CheckBotAdmin => {
				let read = self
					.db
					.reader
					.read(move |c| settings::is_admin(c, guild, user));
				match tokio::time::timeout(DB_WAIT_LIMIT, read).await {
					Ok(Ok(found)) => found,
					Ok(Err(error)) => {
						tracing::error!(guild = guild.get(), %error, "Bot 管理者かを読めませんでした");
						self.reply(channel, keys::READ_FAILED, Vec::new()).await;
						return;
					}
					Err(_) => {
						tracing::warn!(
							guild = guild.get(),
							"Bot 管理者かの読み取りが上限時間を過ぎたため打ち切りました"
						);
						self.reply(channel, keys::READ_FAILED, Vec::new()).await;
						return;
					}
				}
			}
		};
		if !allowed {
			self.reply(channel, keys::NO_PERMISSION, Vec::new()).await;
			return;
		}
		self.apply_and_reply(guild, channel, command).await;
	}

	/// 設定を保存し、結果を返信する。
	///
	/// - 保存は別タスクで行い、`DB_WAIT_LIMIT` を過ぎたら `error.save_slow` を先に返して保存の終わりを待ち続ける
	///   (打ち切ると、書き込みだけ済んで言語の表やアクターへの通知が抜け、DB とずれるため)
	/// - 保存が終わったら結果の通知を返す。書き込めなければ `error.save_failed`
	///
	/// # 引数
	/// - `guild`: 対象のギルド
	/// - `channel`: 返信先
	/// - `command`: 解析済みの操作 (権限の確認は済ませておく)
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.apply_and_reply(guild, channel, command).await;
	/// ```
	async fn apply_and_reply(&self, guild: GuildId, channel: ChannelId, command: SettingCommand) {
		let db = self.db.clone();
		let registry = Arc::clone(&self.registry);
		let langs = Arc::clone(&self.langs);
		let mut task =
			tokio::spawn(
				async move { apply_setting(&db, &registry, &langs, guild, &command).await },
			);
		let joined = match tokio::time::timeout(DB_WAIT_LIMIT, &mut task).await {
			Ok(joined) => joined,
			Err(_) => {
				tracing::warn!(
					guild = guild.get(),
					"設定の保存が上限時間を過ぎたため、待っていることを先に返します"
				);
				self.reply(channel, keys::SAVE_SLOW, Vec::new()).await;
				task.await
			}
		};
		match joined {
			Ok(Ok(notice)) => {
				self.notifier.send(channel, notice).await;
			}
			Ok(Err(error)) => {
				tracing::error!(guild = guild.get(), %error, "設定を保存できませんでした");
				self.reply(channel, keys::SAVE_FAILED, Vec::new()).await;
			}
			Err(error) => {
				tracing::error!(guild = guild.get(), %error, "設定の保存の処理が異常終了しました");
				self.reply(channel, keys::SAVE_FAILED, Vec::new()).await;
			}
		}
	}

	/// ボタンが押された。ページ送りのボタンなら、そのページに書き換える。
	///
	/// - 期限内なら、ボタンの発行時刻を今にして作り直し、灰色にする期限を延ばす (旧 Bot と同じく最後の操作から数える)
	/// - 期限切れなら、本文はそのままでボタンをすべて灰色にする (再起動で期限の記録が消えた場合も同じ)
	/// - 他の機能のボタンは無視する。ページを作れなければ (DB の失敗) 何も変えずに応答だけする
	/// - ページの作成は `deadline` で打ち切る (ボタンが押された時刻から数えた、Discord の応答の期限の手前)
	/// - 灰色にする期限の記録は応答を送る前に更新する (期限のタスクの灰色の編集と入れ違ったときに戻せるように)
	///
	/// # 引数
	/// - `interaction`: 受けたインタラクション
	/// - `deadline`: ページの作成を打ち切る時刻
	async fn on_interaction(&self, interaction: Interaction, deadline: tokio::time::Instant) {
		if self.stopped.load(Ordering::SeqCst)
			|| interaction.kind != InteractionType::MessageComponent
		{
			return;
		}
		let Some(InteractionData::MessageComponent(data)) = interaction.data.as_ref() else {
			return;
		};
		let Some(token) = decode(&data.custom_id) else {
			return;
		};
		let target = interaction.message.as_ref().map(|message| MessageRef {
			channel: message.channel_id,
			message: message.id,
		});
		let response = if is_expired(&token, now_secs(), self.config.ui.pagination_timeout_secs) {
			let disabled = interaction
				.message
				.as_ref()
				.map(|message| disable_all(&message.components))
				.unwrap_or_default();
			update_message(InteractionResponseDataBuilder::new().components(disabled))
		} else {
			let page = self.page(token.kind, interaction.guild_id, token.target, now_secs());
			// DB の読み取りが混んでいても Discord の応答の期限 (3 秒) を過ぎないよう、上限で打ち切って受け付けだけ返す
			match tokio::time::timeout_at(deadline, page).await {
				Ok(Ok(Some(reply))) => {
					// ボタンの無いページなら記録を消し、あれば灰色にする期限を延ばす
					if let Some(target) = target {
						self.arm_buttons(target, reply.components.clone());
					}
					update_message(
						InteractionResponseDataBuilder::new()
							.embeds([reply.embed])
							.components(reply.components),
					)
				}
				Ok(Ok(None)) => return,
				Ok(Err(error)) => {
					tracing::error!(%error, "ページを読めませんでした");
					deferred_update()
				}
				Err(_) => {
					tracing::warn!("ページの作成が上限時間を過ぎたため、受け付けだけ返します");
					deferred_update()
				}
			}
		};
		let interaction_client = self.http.interaction(interaction.application_id);
		let request =
			interaction_client.create_response(interaction.id, &interaction.token, &response);
		let Some(result) = within_send_timeout(request).await else {
			tracing::warn!("ボタンへの応答が上限時間を過ぎました");
			return;
		};
		if let Err(error) = result {
			// 先に延ばした期限はそのまま残す (期限には、届かなかったページのボタンを灰色にして送るだけのため)
			tracing::warn!(%error, "ボタンに応答できませんでした");
		}
	}

	/// ボイスの状態が変わった。Bot がいるチャンネルの聴者の数をアクターへ送る。
	///
	/// - Bot がいるギルドでは、誰の更新でも送る (更新の前のチャンネルは分からないため、
	///   Bot のチャンネルから出た人を取りこぼさないよう、すべての更新で数え直す)
	/// - Bot 自身の更新 (接続の直後、`?move`) でも送る。Bot が切断されていれば 0 人として送る
	/// - アクターが動いていなければ送らない。混雑で送れなければ `gateway.event_send_timeout_secs` まで
	///   数え直して送り直し、それでも送れなければ warn のログを残す
	/// - 最初の送信はその場で行い、混雑のときだけ送り直しを別タスクで行う
	fn on_voice_state(self: &Arc<Self>, guild: GuildId, user: UserId) {
		let own = user == self.bot_id;
		if self.try_send_listeners(guild, own) != Err(SendError::Busy) {
			return;
		}
		let this = Arc::clone(self);
		tokio::spawn(async move {
			let limit = Duration::from_secs(this.config.gateway.event_send_timeout_secs);
			let deadline = Instant::now() + limit;
			loop {
				if Instant::now() >= deadline {
					tracing::warn!(
						guild = guild.get(),
						"アクターが混み合っているため聴者の数を届けられませんでした"
					);
					return;
				}
				tokio::time::sleep(VOICE_RETRY_INTERVAL).await;
				if this.try_send_listeners(guild, own) != Err(SendError::Busy) {
					return;
				}
			}
		});
	}

	/// 聴者の数を数えてアクターへ 1 回送る。
	///
	/// # 戻り値
	/// 送った、送る必要が無い (Bot が接続していない)、またはアクターが動いていなければ Ok 相当。
	/// メールボックスが満杯なら `Err(SendError::Busy)`
	fn try_send_listeners(&self, guild: GuildId, own: bool) -> Result<(), SendError> {
		let Some(listeners) = self.bot_listeners(guild, own) else {
			return Ok(());
		};
		match self
			.registry
			.send_existing(guild, GuildMessage::VoiceMembersChanged { listeners })
		{
			Ok(()) | Err(SendError::NotRunning) => Ok(()),
			Err(error) => Err(error),
		}
	}

	/// Bot がいるボイスチャンネルの聴者の数。
	///
	/// # 戻り値
	/// Bot が接続していれば聴者の数。接続していなければ、Bot 自身の更新のときだけ 0、それ以外は None (送らない)
	fn bot_listeners(&self, guild: GuildId, own: bool) -> Option<u32> {
		match voice_channel_of(&self.cache, guild, self.bot_id) {
			Some(channel) => Some(listeners_in_channel(&self.cache, channel, self.bot_id)),
			None => own.then_some(0),
		}
	}
}

/// 何も変えずに受け付けたことだけを返す応答を作る (ボタンの「失敗しました」の表示を避ける)。
fn deferred_update() -> InteractionResponse {
	InteractionResponse {
		kind: InteractionResponseType::DeferredUpdateMessage,
		data: None,
	}
}

/// メッセージの書き換えの応答を作る。
fn update_message(data: InteractionResponseDataBuilder) -> InteractionResponse {
	InteractionResponse {
		kind: InteractionResponseType::UpdateMessage,
		data: Some(data.build()),
	}
}
