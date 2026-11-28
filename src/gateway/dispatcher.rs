//! Gateway のイベントの振り分けと、アクターを通さないコマンドの実行。
//! - メッセージの振り分けの判定は `route` (純粋な関数) に置く
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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use twilight_cache_inmemory::DefaultInMemoryCache;
use twilight_gateway::Event;
use twilight_http::Client;
use twilight_model::application::interaction::{Interaction, InteractionData, InteractionType};
use twilight_model::channel::message::component::Component;
use twilight_model::channel::message::Embed;
use twilight_model::channel::Message;
use twilight_model::http::interaction::{InteractionResponse, InteractionResponseType};
use twilight_util::builder::interaction_response::UpdateMessageBuilder;

use super::cache_view::{
	guild_permission_data, listeners_in_channel, restore_listeners, voice_channel_of,
	GuildPermissionData,
};
use super::notifier::{within_send_timeout, DiscordNotifier};
use super::page_timers::{Due, PageTimers, Settled};
use super::permissions::is_server_admin;
use super::ready::ReadyGate;
use super::route::{
	is_timed_out, page_build_budget, route, setting_access, snowflake_unix_ms, unix_ms_now,
	IncomingMessage, Route, SettingAccess, PAGE_BUILD_LIMIT,
};
use crate::commands::admin::apply_setting;
use crate::commands::info::{help_page, history_page, queue_page, Reply};
use crate::commands::{ParseError, ParsedCommand, SettingCommand};
use crate::config::Config;
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
	/// 聴者の数の送り直しをしているギルドと、送り直しに Bot 自身の更新が含まれるか。
	/// 送り直しはギルドごとに 1 本にまとめる (混雑の間にボイスの更新が続いても、タスクとメールボックスの枠を増やさない)
	voice_resends: Mutex<HashMap<GuildId, bool>>,
}

impl Dispatcher {
	/// 振り分け役を作る。受付は起動の準備が済むまで閉じている。
	///
	/// # 引数
	/// - `parts`: 使う部品
	///
	/// # 戻り値
	/// 振り分け役 (shard のループと終了の処理で共有する)
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
			voice_resends: Mutex::new(HashMap::new()),
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
			Event::MessageCreate(message) => self.on_message(message.message),
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
	///
	/// # 戻り値
	/// 門のロックのガード
	///
	/// # 呼び出し例
	/// `self.lock_ready().on_guild_create(guild);`
	fn lock_ready(&self) -> std::sync::MutexGuard<'_, ReadyGate> {
		self.ready.lock().unwrap_or_else(PoisonError::into_inner)
	}

	/// `Ready` を受けた。門に渡し、期限の確認を予約する。
	///
	/// - 2 回目以降 (再接続) は門が無視する。予約した確認も門が開かないので何もしない
	///
	/// # 引数
	/// - `guilds`: `Ready` に載っていたギルド
	///
	/// # 呼び出し例
	/// ```ignore
	/// tokio::spawn(Arc::clone(self).on_ready(guilds));
	/// ```
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
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.check_ready().await;
	/// ```
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
	///
	/// # 引数
	/// - `channel`: 送り先
	/// - `key`: 文面のキー
	/// - `args`: 文面に埋める値 (名前と値)
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.reply(channel, keys::BUSY, Vec::new()).await;
	/// ```
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
	/// - 終了の処理に入っていたら何もしない
	///
	/// # 引数
	/// - `message`: 受けたメッセージ (本文は取り出して使う)
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_message(message.0);
	/// ```
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
	///
	/// # 引数
	/// - `guild`: メッセージのサーバー (DM なら None)
	/// - `channel`: 返信先
	/// - `error`: 解析の誤り
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.reply_parse_error(guild, channel, error).await;
	/// ```
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
	///
	/// - アクターへのコマンドは `send_to_actor` へ、`?help`・一覧・`?setting` はここで処理する
	///
	/// # 引数
	/// - `guild`: メッセージのサーバー
	/// - `message`: 受けたメッセージ (返信先と実行者に使う)
	/// - `command`: 解析済みのコマンド
	///
	/// # 呼び出し例
	/// ```ignore
	/// this.run_command(guild, &message, command).await;
	/// ```
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
	///
	/// - アクターが無ければ起動する
	/// - 実行者のボイスチャンネルはキャッシュから取る
	///
	/// # 引数
	/// - `guild`: 対象のギルド
	/// - `message`: 受けたメッセージ (返信先と実行者に使う)
	/// - `command`: アクターへ渡すコマンド
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.send_to_actor(guild, &message, command);
	/// ```
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
	/// # 引数
	/// - `kind`: ページの種類
	/// - `guild`: 対象のギルド (DM なら None)
	/// - `page`: ページの番号 (0 始まり)
	/// - `issued_at`: ボタンの発行時刻 (UNIX 秒)
	///
	/// # 戻り値
	/// ページの Embed とボタン。キューと履歴でギルドが無い (DM) なら None
	///
	/// # エラー
	/// - DB の読み取りの失敗
	///
	/// # 呼び出し例
	/// ```ignore
	/// let reply = self.page(PageKind::Queue, Some(guild), 0, now_secs()).await?;
	/// ```
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
	///
	/// # 引数
	/// - `guild`: 対象のギルド
	/// - `channel`: 送り先
	/// - `kind`: 一覧の種類 (`Queue` か `History`)
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.send_list(guild, channel, PageKind::Queue).await;
	/// ```
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
	///
	/// - 送れなければ (失敗は送信側でログに残す) 期限を記録しない
	///
	/// # 引数
	/// - `channel`: 送り先
	/// - `reply`: 送る Embed とボタン
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.send_reply(channel, reply).await;
	/// ```
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
	/// # 引数
	/// - `guild`: メッセージのサーバー
	/// - `message`: 受けたメッセージ (`member` の役職とタイムアウトの期限を使う)
	///
	/// # 戻り値
	/// (権限の入力、タイムアウト中か)。`member` が無い、またはギルドか `@everyone` がキャッシュに無ければ入力は None。
	/// `member` が無ければタイムアウト中でないものとする
	///
	/// # 呼び出し例
	/// ```ignore
	/// let (data, timed_out) = self.author_permissions(guild, message);
	/// ```
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
	/// - 権限が無ければ `error.no_permission`
	///
	/// # 引数
	/// - `guild`: 対象のギルド
	/// - `message`: 受けたメッセージ (返信先と実行者に使う)
	/// - `command`: 解析済みの操作
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.setting(guild, message, command).await;
	/// ```
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
	///
	/// # 呼び出し例
	/// ```ignore
	/// tokio::spawn(async move { this.on_interaction(interaction.0, deadline).await });
	/// ```
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
			update_message_components(disabled)
		} else {
			let page = self.page(token.kind, interaction.guild_id, token.target, now_secs());
			// DB の読み取りが混んでいても Discord の応答の期限 (3 秒) を過ぎないよう、上限で打ち切って受け付けだけ返す
			match tokio::time::timeout_at(deadline, page).await {
				Ok(Ok(Some(reply))) => {
					// ボタンの無いページなら記録を消し、あれば灰色にする期限を延ばす
					if let Some(target) = target {
						self.arm_buttons(target, reply.components.clone());
					}
					update_message_page(reply.embed, reply.components)
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
	/// - 送り直しはギルドごとに 1 本だけ。送り直しの間の更新は、送り直しのタスクが毎回数え直すため拾われる
	///   (Bot 自身の更新があったことだけを引き継ぐ)
	///
	/// # 引数
	/// - `guild`: 更新のあったギルド
	/// - `user`: 状態が変わった利用者
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_voice_state(guild, state.user_id);
	/// ```
	fn on_voice_state(self: &Arc<Self>, guild: GuildId, user: UserId) {
		let own = user == self.bot_id;
		// 印の確認・最初の送信・印の登録を 1 つのロックの中で行い、送り直しのタスクが 2 本にならないようにする
		{
			let mut resends = self.lock_voice_resends();
			if let Some(pending_own) = resends.get_mut(&guild) {
				*pending_own |= own;
				return;
			}
			if self.try_send_listeners(guild, own) != Err(SendError::Busy) {
				return;
			}
			resends.insert(guild, own);
		}
		let this = Arc::clone(self);
		tokio::spawn(async move {
			// タスクが終わる・止められる・panic するどの場合も印を外す (残ると以後の更新が送られなくなる)
			let _resend = ResendMark {
				dispatcher: Arc::clone(&this),
				guild,
			};
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
				// 送り直しの間に届いた Bot 自身の更新も含めて数え直す。
				// 送れたら同じロックの中で印を外す (外した後の更新は新しい送信として扱われる)
				let mut resends = this.lock_voice_resends();
				let own = resends.get(&guild).copied().unwrap_or(own);
				if this.try_send_listeners(guild, own) != Err(SendError::Busy) {
					resends.remove(&guild);
					return;
				}
			}
		});
	}

	/// 送り直しの表をロックする。毒されていても中身を使う (値は単純な印のため)。
	///
	/// # 戻り値
	/// 送り直しの表のロックのガード
	///
	/// # 呼び出し例
	/// `self.lock_voice_resends().remove(&guild);`
	fn lock_voice_resends(&self) -> std::sync::MutexGuard<'_, HashMap<GuildId, bool>> {
		self.voice_resends
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
	}

	/// 聴者の数を数えてアクターへ 1 回送る。
	///
	/// # 引数
	/// - `guild`: 対象のギルド
	/// - `own`: Bot 自身の更新を含むか (Bot が切断されていれば 0 人として送るため)
	///
	/// # 戻り値
	/// 送った、送る必要が無い (Bot が接続していない)、またはアクターが動いていなければ Ok
	///
	/// # エラー
	/// - メールボックスが満杯なら `SendError::Busy`
	///
	/// # 呼び出し例
	/// ```ignore
	/// if self.try_send_listeners(guild, own) == Err(SendError::Busy) { /* 送り直す */ }
	/// ```
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
	/// # 引数
	/// - `guild`: 対象のギルド
	/// - `own`: Bot 自身の更新を含むか
	///
	/// # 戻り値
	/// Bot が接続していれば聴者の数。接続していなければ、Bot 自身の更新のときだけ 0、それ以外は None (送らない)
	///
	/// # 呼び出し例
	/// ```ignore
	/// let Some(listeners) = self.bot_listeners(guild, own) else { return Ok(()) };
	/// ```
	fn bot_listeners(&self, guild: GuildId, own: bool) -> Option<u32> {
		match voice_channel_of(&self.cache, guild, self.bot_id) {
			Some(channel) => Some(listeners_in_channel(&self.cache, channel, self.bot_id)),
			None => own.then_some(0),
		}
	}
}

/// 聴者の数の送り直しの印。破棄で表から外す。
///
/// - 送り直しのタスクが持ち、終わり方 (送れた、期限切れ、取り消し、panic) にかかわらず印を残さない
struct ResendMark {
	dispatcher: Arc<Dispatcher>,
	guild: GuildId,
}

impl Drop for ResendMark {
	/// 送り直しの印を外す。すでに外していれば何もしない。
	fn drop(&mut self) {
		self.dispatcher.lock_voice_resends().remove(&self.guild);
	}
}

/// 何も変えずに受け付けたことだけを返す応答を作る (ボタンの「失敗しました」の表示を避ける)。
///
/// # 戻り値
/// `DeferredUpdateMessage` の応答
///
/// # 呼び出し例
/// `let response = deferred_update();`
fn deferred_update() -> InteractionResponse {
	InteractionResponse {
		kind: InteractionResponseType::DeferredUpdateMessage,
		data: None,
	}
}

/// ボタンだけを書き換える応答を作る。
///
/// # 引数
/// - `components`: 差し替えるボタン (期限切れで灰色にしたものなど)
///
/// # 戻り値
/// `UpdateMessage` の応答。Embed は変えない
///
/// # 呼び出し例
/// ```ignore
/// let response = update_message_components(disable_all(&message.components));
/// ```
fn update_message_components(components: Vec<Component>) -> InteractionResponse {
	UpdateMessageBuilder::new().components(components).build()
}

/// Embed とボタンを書き換える応答を作る (ページ送り)。
///
/// # 引数
/// - `embed`: 新しいページの Embed
/// - `components`: 新しいページのボタン
///
/// # 戻り値
/// `UpdateMessage` の応答
///
/// # 呼び出し例
/// ```ignore
/// let response = update_message_page(reply.embed, reply.components);
/// ```
fn update_message_page(embed: Embed, components: Vec<Component>) -> InteractionResponse {
	UpdateMessageBuilder::new()
		.embeds([embed])
		.components(components)
		.build()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ui::pagination::page_buttons;
	use twilight_util::builder::embed::EmbedBuilder;

	/// ボタンだけを書き換える応答が、UpdateMessage の種類で、渡したボタンをそのまま持つこと。
	#[test]
	fn update_message_components_keeps_kind_and_components() {
		let components = page_buttons(PageKind::Queue, 0, 2, 1_700_000_000);
		let response = update_message_components(components.clone());
		assert_eq!(response.kind, InteractionResponseType::UpdateMessage);
		let data = response.data.expect("UpdateMessage の応答は data を持つ");
		assert_eq!(data.components, Some(components));
		assert_eq!(data.embeds, None);
	}

	/// Embed とボタンを書き換える応答が、UpdateMessage の種類で、両方をそのまま持つこと。
	#[test]
	fn update_message_page_keeps_embed_and_components() {
		let embed = EmbedBuilder::new().title("page").build();
		let components = page_buttons(PageKind::History, 1, 3, 1_700_000_000);
		let response = update_message_page(embed.clone(), components.clone());
		assert_eq!(response.kind, InteractionResponseType::UpdateMessage);
		let data = response.data.expect("UpdateMessage の応答は data を持つ");
		assert_eq!(data.embeds, Some(vec![embed]));
		assert_eq!(data.components, Some(components));
	}
}
