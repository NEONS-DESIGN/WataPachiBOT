//! Gateway の Shard の受信ループ。
//! - 1 イベントごとに、キャッシュの更新 → Songbird への反映 → `Dispatcher::dispatch` の順に行う (同じタスクの中で受信の順に処理する)
//!   (キャッシュと Songbird は受信の順に反映し、Dispatcher の処理はループを待たせない)
//! - Songbird への反映は上限時間付きで待つ。Songbird の内部のロックで受信が止まると、
//!   Twilight は `next_event` を待つ間しか heartbeat を送らないため、Gateway の接続が切れる
//! - 終了の合図 (`watch` が true) を受けたら Shard を閉じ、閉じた応答を待ってから戻る
//! - 戻り値 (`ShardExit`) で、終了の合図で閉じたのか、Gateway が再接続しない形で終わったのかを返す
//! - Bot のステータス (`presence`) は接続時の設定で渡す

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use songbird::Songbird;
use tokio::sync::watch;
use twilight_cache_inmemory::{DefaultInMemoryCache, ResourceType};
use twilight_gateway::{CloseFrame, Event, EventTypeFlags, Intents, Shard, StreamExt as _};
use twilight_model::gateway::payload::outgoing::update_presence::UpdatePresencePayload;
use twilight_model::gateway::presence::{ActivityType, MinimalActivity, Status};
use twilight_model::gateway::CloseCode;

use super::dispatcher::Dispatcher;
use crate::config::BotConfig;

/// Gateway の Intent。`MESSAGE_CONTENT` は特権 Intent のため、開発者ポータルで有効にしておく。
pub const INTENTS: Intents = Intents::GUILDS
	.union(Intents::GUILD_VOICE_STATES)
	.union(Intents::GUILD_MESSAGES)
	.union(Intents::MESSAGE_CONTENT);

/// キャッシュする資源。権限の計算 (ギルド、チャンネル、役職、Bot のメンバー) と聴者の数え方 (ボイスの状態、利用者) に使う。
pub const CACHE_RESOURCES: ResourceType = ResourceType::GUILD
	.union(ResourceType::CHANNEL)
	.union(ResourceType::ROLE)
	.union(ResourceType::MEMBER)
	.union(ResourceType::USER)
	.union(ResourceType::VOICE_STATE);

/// 受け取るイベントの種類。キャッシュと Songbird が要るイベントを漏らさないよう、すべて受け取る
/// (Intent で絞っているため、不要なイベントはほとんど届かない)。
const WANTED_EVENTS: EventTypeFlags = EventTypeFlags::all();

/// Shard を閉じた後、閉じた応答を待つ上限。過ぎたら待たずに戻る (終了の処理を止めないため)。
const CLOSE_WAIT: Duration = Duration::from_secs(5);

/// `run_shard` の終わり方。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardExit {
	/// 終了の合図 (または合図の送信側の消滅) で閉じた
	Shutdown,
	/// Gateway が再接続しない形で終わった (トークンの誤り、許されていない Intent など)。
	/// `close_code` は最後に受けた閉じる理由のコード (受けていなければ None)
	Fatal { close_code: Option<u16> },
}

/// Bot のステータスを組み立てる。接続時に Shard の設定で渡す (再接続でも同じものが送られる)。
///
/// - 種類は「プレイ中」(旧 Bot と同じ)。状態はオンライン
/// - 名前は `bot.activity_name` の `{prefix}` を `bot.prefix` に置き換えたもの
///
/// # 引数
/// - `bot`: `[bot]` の設定
///
/// # エラー
/// - Discord の制約に合わないとき (活動が空など。名前は設定の検証で空にならない)
///
/// # 呼び出し例
/// ```ignore
/// let config = ConfigBuilder::new(token, INTENTS).presence(presence(&config.bot)?).build();
/// ```
pub fn presence(bot: &BotConfig) -> anyhow::Result<UpdatePresencePayload> {
	let activity = MinimalActivity {
		kind: ActivityType::Playing,
		name: bot.activity_name.replace("{prefix}", &bot.prefix),
		url: None,
	};
	UpdatePresencePayload::new(vec![activity.into()], false, None, Status::Online)
		.context("Bot のステータスを組み立てられません")
}

/// `CACHE_RESOURCES` だけをキャッシュする `InMemoryCache` を作る。
///
/// # 呼び出し例
/// ```ignore
/// let cache = Arc::new(new_cache());
/// ```
pub fn new_cache() -> DefaultInMemoryCache {
	DefaultInMemoryCache::builder()
		.resource_types(CACHE_RESOURCES)
		.build()
}

/// Shard の受信ループを回す。
///
/// - 受信のエラーは warn のログを残して続ける (再接続は Twilight に任せる)
/// - Songbird への反映は `songbird_timeout` まで待ち、過ぎたら warn のログを残して次のイベントへ進む
/// - `shutdown` が true になったら (送信側が無くなったときも) Shard を閉じ、`Event::GatewayClose` を
///   `CLOSE_WAIT` まで待ってから `ShardExit::Shutdown` を返す
/// - 受信が終わった (Shard が再接続しない) ときは error のログを残して `ShardExit::Fatal` を返す。
///   呼び出し側は起動の失敗として終了する
///
/// # 引数
/// - `shard`: 接続前の Shard
/// - `cache`: 受信のたびに更新するキャッシュ
/// - `songbird`: 声の接続の管理 (ボイスの状態とサーバーの更新を渡す)
/// - `songbird_timeout`: Songbird への反映 1 回を待つ上限 (`voice.call_timeout_secs`)
/// - `dispatcher`: イベントの振り分け役
/// - `shutdown`: 終了の合図
///
/// # 戻り値
/// 終わり方
///
/// # 呼び出し例
/// ```ignore
/// let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
/// let bound = Duration::from_secs(config.voice.call_timeout_secs);
/// let task = tokio::spawn(run_shard(shard, cache, songbird, bound, dispatcher, stop_rx));
/// // 終了のとき
/// let _ = stop_tx.send(true);
/// let exit = task.await?;
/// ```
pub async fn run_shard(
	mut shard: Shard,
	cache: Arc<DefaultInMemoryCache>,
	songbird: Arc<Songbird>,
	songbird_timeout: Duration,
	dispatcher: Arc<Dispatcher>,
	mut shutdown: watch::Receiver<bool>,
) -> ShardExit {
	let mut last_close: Option<u16> = None;
	loop {
		if *shutdown.borrow_and_update() {
			break;
		}
		tokio::select! {
			changed = shutdown.changed() => {
				if changed.is_err() {
					tracing::info!("終了の合図の送信側が無くなったため Shard を閉じます");
					break;
				}
			}
			item = shard.next_event(WANTED_EVENTS) => {
				let Some(item) = item else {
					log_fatal(last_close);
					return ShardExit::Fatal { close_code: last_close };
				};
				let event = match item {
					Ok(event) => event,
					Err(error) => {
						tracing::warn!(%error, "Gateway の受信に失敗しました");
						continue;
					}
				};
				if let Event::GatewayClose(frame) = &event {
					last_close = frame.as_ref().map(|frame| frame.code);
					tracing::info!(code = ?last_close, "Gateway の接続が閉じられました");
				}
				// 新しいセッション (再開できない再接続を含む) の Ready では、キャッシュを空にしてから作り直す。
				// twilight のキャッシュは GuildCreate でボイスの状態を消さないため、切れていた間に抜けた人が
				// 聴者として残り続ける。Ready の後に全ギルドの GuildCreate が届くため、空にしても作り直される
				if matches!(event, Event::Ready(_)) {
					cache.clear();
				}
				cache.update(&event);
				let processed = tokio::time::timeout(songbird_timeout, songbird.process(&event)).await;
				if processed.is_err() {
					tracing::warn!(
						kind = ?event.kind(),
						"Songbird へのイベントの反映が上限時間を過ぎたため、待たずに進みます"
					);
				}
				dispatcher.dispatch(event);
			}
		}
	}
	close(&mut shard).await;
	ShardExit::Shutdown
}

/// 再接続しない形で受信が終わったことをログに残す。閉じる理由のコードが分かれば名前も添える。
fn log_fatal(close_code: Option<u16>) {
	let reason = close_code.and_then(|code| CloseCode::try_from(code).ok());
	tracing::error!(
		code = ?close_code,
		reason = ?reason,
		"Gateway の受信が終わりました (再接続しません)"
	);
}

/// Shard を閉じ、閉じた応答を `CLOSE_WAIT` まで待つ。
async fn close(shard: &mut Shard) {
	shard.close(CloseFrame::NORMAL);
	let drained = tokio::time::timeout(CLOSE_WAIT, async {
		while let Some(item) = shard.next_event(EventTypeFlags::empty()).await {
			if matches!(item, Ok(Event::GatewayClose(_))) {
				break;
			}
		}
	})
	.await;
	match drained {
		Ok(()) => tracing::info!("Gateway を閉じました"),
		Err(_) => tracing::warn!("Gateway の閉じた応答を待たずに戻ります"),
	}
}
