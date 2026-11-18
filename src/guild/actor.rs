//! ギルドごとのアクター。
//! - 1 ギルドにつき 1 つの tokio タスク。ギルドの再生状態はここだけが持つ
//! - `tokio::select!` で、メールボックス、準備の遅延の通知のタイマー、待機の終了のタイマー、
//!   自動退出のタイマー、起動した処理の終了を待つ
//! - 接続、解決、`?p` の問い合わせは別タスクで実行し、結果をメッセージで受け取る
//! - 音声セッションの呼び出しは短いため、`voice.call_timeout_secs` で包んでその場で待つ。
//!   接続、移動、切断は、バックエンドの内側の上限と後始末の合計より長い上限 (`VoiceLimits`) で包む
//! - 通知は専用のタスクへ順に渡し、Discord への送信を待たない
//! - `playback_state` は曲の切替時に書き、再生位置は `persist.rs` が定期的に保存する
//! - 曲が変わるたびに世代 (`generation`) を増やし、古い解決結果を捨てる
//! - `?p` 以外の操作コマンドと自動退出は `commands.rs` (この下のモジュール) が処理する
//! - 解決と再生の失敗、音声接続の監視と回復は `supervise.rs` (この下のモジュール) が処理する
//! - 再生位置の保存、起動時の復元、終了は `persist.rs` (この下のモジュール) が処理する
//! - 音声の呼び出しが時間切れになったら、そのセッションは壊れたとみなし、ループの先頭で回復を始める
//! - 呼び出し: 登録簿 (`registry.rs`) が `actor::spawn` で起動する

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::{AbortHandle, Id, JoinError, JoinSet};
use tokio::time::Instant;

use super::message::{CommandContext, GuildMessage, LookupKind, QueryResult, TickKind, VoiceLink};
use super::notices::{counted, keys, lookup_failure_key, plain, titled, titled_code, with_args};
use super::notifier::{Card, MessageRef, Notice, Notifier, PlaylistCard, TrackCard};
use super::prefetch::Prefetcher;
use super::query::{self, Query};
use super::registry::{ActorDeps, RegistryInner};
use super::state::{CurrentTrack, PlayerState, SessionChannels};
use super::store::{self, item_from_meta, item_from_stream};
use crate::audio::backend::{AudioError, AudioEvent, PlayRequest, TrackId, VoiceSession};
use crate::audio::limits::VoiceLimits;
use crate::config::DefaultsConfig;
use crate::db::playback::PlaybackState;
use crate::db::queue::{self, NewQueueItem};
use crate::db::settings::{self, GuildSettings};
use crate::db::{now_secs, Connection, DbError};
use crate::domain::{
	ChannelId, GuildId, Priority, ResolveError, ResolveErrorCode, ResolvedStream, VideoId,
};
use supervise::Supervision;

/// ChunkedRequest で再生できるストリームの `protocol` の値 (yt-dlp の表記)。他は PipeInput で再生する。
const CHUNKED_PROTOCOL: &str = "https";

// 操作コマンドの処理。アクターの非公開の状態を直接扱うため、別ファイルにした子のモジュールとして持つ
#[path = "commands.rs"]
mod commands;
// 失敗時の手順と接続の回復。同じ理由で子のモジュールとして持つ
#[path = "supervise.rs"]
mod supervise;
// 再生位置の保存、起動時の復元、終了。同じ理由で子のモジュールとして持つ
#[path = "persist.rs"]
mod persist;

/// アクターを起動し、メールボックスの送信口を返す。
///
/// - メールボックスの容量は `limits.actor_mailbox`
/// - tokio のランタイムの中で呼ぶ
///
/// # 引数
/// - `guild`: 担当のギルド
/// - `id`: 登録簿が振る通し番号
/// - `deps`: 外部の部品
/// - `registry`: 終了時に登録を消すための登録簿
///
/// # 戻り値
/// メールボックスの送信口
///
/// # 呼び出し例
/// ```ignore
/// let tx = actor::spawn(guild, id, deps.clone(), Arc::downgrade(&inner));
/// ```
pub(crate) fn spawn(
	guild: GuildId,
	id: u64,
	deps: ActorDeps,
	registry: Weak<RegistryInner>,
) -> mpsc::Sender<GuildMessage> {
	let (tx, rx) = mpsc::channel(deps.config.limits.actor_mailbox.max(1));
	let notices = spawn_notice_task(deps.notifier.clone());
	let prefetch = Prefetcher::new(deps.config.playback.prefetch_tracks);
	let sup = Supervision::new(&deps.config);
	let actor = GuildActor {
		guild,
		id,
		deps,
		registry,
		tx: tx.clone(),
		rx,
		notices,
		state: PlayerState::Idle,
		voice: None,
		channels: None,
		session: 0,
		generation: 0,
		current: None,
		played_in_session: false,
		pending_lookups: 0,
		prefetch,
		settings: None,
		slow: None,
		idle: None,
		listeners: 0,
		alone: None,
		sup,
		save_at: None,
		pending_restore: None,
		tasks: JoinSet::new(),
		task_kinds: HashMap::new(),
	};
	tokio::spawn(actor.run());
	tx
}

/// 通知を順に送る専用のタスクを起動する。アクターが送信口を捨てると、残りを送り切って終わる。
///
/// - `play.preparing` は送り、その場所を準備中のメッセージとして覚える
/// - `play.slow` は準備中のメッセージを編集する。無いか編集に失敗したら新しく送り、それを準備中のメッセージにする
/// - それ以外は、同じチャンネルの準備中のメッセージがあれば編集して置き換え、覚えを消す。編集に失敗したら新しく送る
///
/// # 引数
/// - `notifier`: 通知の送り先
///
/// # 戻り値
/// 通知を渡す送信口 (待たずに渡せる)
///
/// # 呼び出し例
/// ```ignore
/// let notices = spawn_notice_task(deps.notifier.clone());
/// notices.send((channel, notice))?;
/// ```
fn spawn_notice_task(notifier: Arc<dyn Notifier>) -> mpsc::UnboundedSender<(ChannelId, Notice)> {
	let (tx, mut rx) = mpsc::unbounded_channel::<(ChannelId, Notice)>();
	tokio::spawn(async move {
		let mut preparing: Option<MessageRef> = None;
		while let Some((channel, notice)) = rx.recv().await {
			match notice.key {
				keys::PREPARING => {
					preparing = notifier.send(channel, notice).await;
				}
				keys::SLOW => {
					if let Some(target) = preparing.filter(|t| t.channel == channel) {
						if notifier.edit(target, notice.clone()).await {
							continue;
						}
					}
					preparing = notifier.send(channel, notice).await;
				}
				_ => {
					if let Some(target) = preparing.filter(|t| t.channel == channel) {
						preparing = None;
						if notifier.edit(target, notice.clone()).await {
							continue;
						}
					}
					notifier.send(channel, notice).await;
				}
			}
		}
	});
	tx
}

/// メッセージの処理の後に続けるか。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flow {
	Continue,
	Stop,
}

/// 今の曲の処理の後に、呼び出し側が次の曲へ進むか。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
enum Next {
	/// 今の曲を飛ばした。呼び出し側は次の曲を始める
	Advance,
	/// 再生を始めた、解決や回復を待つ、または再生を止めた。呼び出し側は何もしない
	Stay,
}

/// 世代を進めるときの、`rate_limited` の解決のやり直しの待ち (`retry_at`) の扱い。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryWait {
	/// 待ちを捨てる (曲を差し替えた、曲を作り直す)
	Discard,
	/// 待ちの時刻を残し、新しい世代の待ちにする (同じ曲のまま再接続する回復)
	Carry,
}

/// 起動した処理の種類。処理が panic したとき、代わりに失敗のメッセージを作るために持つ。
/// - `session` は処理を起動したときのセッションの番号
enum TaskKind {
	Lookup {
		session: u64,
		context: CommandContext,
		kind: LookupKind,
	},
	Connect(u64),
	Resolve {
		generation: u64,
		video_id: VideoId,
	},
	Prefetch {
		session: u64,
		video_id: VideoId,
	},
}

impl TaskKind {
	/// 処理が結果を返さずに終わったときの、失敗のメッセージを作る。
	///
	/// - 解決の失敗は `Unavailable`、接続の失敗は `Failed` として扱う
	///
	/// # 戻り値
	/// 処理が送るはずだったメッセージの、失敗の形
	///
	/// # 呼び出し例
	/// ```ignore
	/// let message = kind.into_failure();
	/// ```
	fn into_failure(self) -> GuildMessage {
		match self {
			Self::Lookup {
				session,
				context,
				kind,
			} => GuildMessage::QueryResolved {
				session,
				context,
				kind,
				result: Err(ResolveError::Unavailable),
			},
			Self::Connect(session) => GuildMessage::Connected {
				session,
				result: Err(AudioError::Failed(
					"接続の処理が異常終了しました".to_owned(),
				)),
			},
			Self::Resolve {
				generation,
				video_id,
			} => GuildMessage::Resolved {
				generation,
				video_id,
				result: Err(ResolveError::Unavailable),
			},
			Self::Prefetch { session, video_id } => GuildMessage::Prefetched {
				session,
				video_id,
				result: Err(ResolveError::Unavailable),
			},
		}
	}
}

/// 1 ギルドのアクター。
struct GuildActor {
	guild: GuildId,
	/// 登録簿が振った通し番号。
	id: u64,
	deps: ActorDeps,
	registry: Weak<RegistryInner>,
	/// 自分のメールボックスへの送信口。起動した処理と音声バックエンドへ渡す。
	tx: mpsc::Sender<GuildMessage>,
	rx: mpsc::Receiver<GuildMessage>,
	notices: mpsc::UnboundedSender<(ChannelId, Notice)>,
	state: PlayerState,
	voice: Option<Box<dyn VoiceSession>>,
	/// 再生のセッションの宛先。Idle 以外のときに持つ。
	channels: Option<SessionChannels>,
	/// セッションの番号。`?p` で Idle から接続を始めるたびに増やす。
	/// 古いセッションの接続、問い合わせ、先読みの結果を見分ける。
	session: u64,
	/// 曲の世代。今の曲が変わるたびに増やす。
	generation: u64,
	current: Option<CurrentTrack>,
	/// 今のセッションで、曲を今の曲にしたか。キューが空で退出するときに、再生の終了を通知するかを決める。
	/// - `set_current(Some(..))` で立て、`reset_to_idle` で下ろす
	played_in_session: bool,
	/// 今のセッションで結果を待っている `?p` の問い合わせの数。0 になるまでは、キューが空でも退出しない。
	pending_lookups: u32,
	prefetch: Prefetcher,
	/// ギルドの設定。セッションの間だけ持ち、退出で捨てる。
	settings: Option<GuildSettings>,
	/// 準備が遅いことを知らせる時刻。
	slow: Option<Instant>,
	/// 待機を終えてアクターを終了する時刻。Idle のときだけ持つ。
	idle: Option<Instant>,
	/// ボイスチャンネルの聴者 (Bot 以外) の人数。未受信の間は 0 (未暗号化を判定しない側に倒す)。
	listeners: u32,
	/// 聞き手が居ないため自動退出する時刻。聞き手が 0 人になってから、戻るまでの間だけ持つ。
	alone: Option<Instant>,
	/// 失敗と健全性の監視の状態 (健全性の確認の時刻、解決のやり直しの時刻、再接続の制限など)。
	sup: Supervision,
	/// 次に再生位置を定期的に保存する時刻。再生中 (Playing) の間だけ持つ。
	save_at: Option<Instant>,
	/// 復元の接続を待っている再生状態。復元で接続を始めてから、接続の完了までの間だけ持つ。
	pending_restore: Option<PlaybackState>,
	tasks: JoinSet<()>,
	/// 起動中の処理の種類と、個別に止めるためのハンドル
	task_kinds: HashMap<Id, (TaskKind, AbortHandle)>,
}

impl GuildActor {
	/// アクターの本体。終了するまでメッセージとタイマーを待つ。
	///
	/// - Idle のまま `limits.actor_idle_timeout_secs` を過ぎると終了する。メッセージが届くたびに数え直す
	/// - 前の処理で音声の呼び出しが時間切れになっていたら、先に回復を始める
	/// - 再生中は `playback.position_save_interval_secs` ごとに再生位置を保存する
	/// - 終了時は登録簿から自分を消す
	///
	/// # 呼び出し例
	/// ```ignore
	/// tokio::spawn(actor.run());
	/// ```
	async fn run(mut self) {
		loop {
			self.recover_if_timed_out().await;
			self.refresh_idle(false);
			self.refresh_save_timer();
			let slow = self.slow;
			let save = self.save_at;
			let idle = self.idle;
			let alone = self.alone;
			let health = self.sup.health_at;
			// 回復中は解決のやり直しを待たせる (時刻は残し、再接続の後に過ぎていればすぐやり直す)
			let retry = self
				.sup
				.retry_at
				.filter(|_| self.state != PlayerState::Recovering)
				.map(|(at, _)| at);
			tokio::select! {
				message = self.rx.recv() => {
					let Some(message) = message else {
						break;
					};
					if self.handle(message).await == Flow::Stop {
						return;
					}
					self.refresh_idle(true);
				}
				() = wait_deadline(slow) => self.on_slow(),
				() = wait_deadline(alone) => self.on_alone_timeout().await,
				() = wait_deadline(health) => self.on_health_due().await,
				() = wait_deadline(save) => self.on_save_due().await,
				() = wait_deadline(retry) => self.on_retry_due(),
				() = wait_deadline(idle) => {
					tracing::debug!(guild = self.guild.get(), "待機の時間を過ぎたためアクターを終了します");
					break;
				}
				Some(joined) = self.tasks.join_next_with_id() => self.on_task_joined(joined).await,
			}
		}
		self.retire();
	}

	/// メッセージを種類ごとの処理へ振り分ける。
	///
	/// - コマンドは `on_command` (`commands.rs`) へ渡す
	/// - 未対応のメッセージは debug ログに出して無視する
	///
	/// # 引数
	/// - `message`: 届いたメッセージ
	///
	/// # 戻り値
	/// `Shutdown` なら `Flow::Stop`、それ以外は `Flow::Continue`
	///
	/// # 呼び出し例
	/// ```ignore
	/// if self.handle(message).await == Flow::Stop { return; }
	/// ```
	async fn handle(&mut self, message: GuildMessage) -> Flow {
		match message {
			GuildMessage::Command { command, context } => self.on_command(command, context).await,
			GuildMessage::VoiceMembersChanged { listeners } => {
				self.on_voice_members(listeners).await;
			}
			GuildMessage::QueryResolved {
				session,
				context,
				kind,
				result,
			} => {
				self.on_query_resolved(session, context, kind, result).await;
			}
			GuildMessage::Connected { session, result } => self.on_connected(session, result).await,
			GuildMessage::Resolved {
				generation,
				video_id,
				result,
			} => self.on_resolved(generation, video_id, result).await,
			GuildMessage::Prefetched {
				session,
				video_id,
				result,
			} => {
				self.on_prefetched(session, video_id, result).await;
			}
			GuildMessage::Audio(AudioEvent::TrackStarted { track }) => self.on_track_started(track),
			GuildMessage::Audio(AudioEvent::TrackEnded { track }) => {
				self.on_track_ended(track).await
			}
			GuildMessage::Audio(AudioEvent::TrackFailed { track, error }) => {
				self.on_track_failed(track, error).await;
			}
			GuildMessage::Audio(AudioEvent::Disconnected { reason }) => {
				self.on_disconnected(reason).await;
			}
			GuildMessage::Audio(AudioEvent::Reconnected) => {
				tracing::info!(guild = self.guild.get(), "音声の接続が自動で回復しました");
			}
			GuildMessage::SettingsChanged => self.on_settings_changed().await,
			GuildMessage::Tick(TickKind::Health) => self.check_health().await,
			GuildMessage::Tick(TickKind::Position) => self.on_save_due().await,
			GuildMessage::Restore(request) => self.on_restore(request).await,
			GuildMessage::Shutdown { done } => {
				self.on_shutdown(done).await;
				return Flow::Stop;
			}
			other => {
				tracing::debug!(
					guild = self.guild.get(),
					?other,
					"未対応のメッセージを無視します"
				);
			}
		}
		Flow::Continue
	}

	/// `?p` を受ける。
	///
	/// - 実行者がボイスチャンネルに居なければ、エラーを通知して終える
	/// - 対応外の URL、空の検索語 (`<>` だけなど) は、問い合わせず、接続も始めずにエラーを通知する
	/// - Idle なら準備中を通知して接続を始める。問い合わせ (解決、検索、一覧) は常に別タスクで行う
	///
	/// # 引数
	/// - `text`: `?p` の引数
	/// - `context`: コマンドの発行元
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_play(&query, context).await;
	/// ```
	async fn on_play(&mut self, text: &str, context: CommandContext) {
		let Some(voice) = context.voice_channel else {
			self.notify(context.text_channel, plain(keys::NOT_IN_VOICE));
			return;
		};
		let query = query::classify(text);
		let rejected = match query {
			Query::Unsupported => Some(keys::UNSUPPORTED_URL),
			Query::Empty => Some(keys::LOOKUP_SEARCH_EMPTY),
			Query::Video(_) | Query::Playlist(_) | Query::Search(_) => None,
		};
		if let Some(key) = rejected {
			self.notify(context.text_channel, plain(key));
			return;
		}
		if self.state == PlayerState::Idle {
			self.begin_session(SessionChannels {
				voice,
				text: context.text_channel,
			});
		}
		self.spawn_lookup(query, context).await;
	}

	/// 再生のセッションを始める。準備中を通知し、接続を別タスクで始める。
	///
	/// - セッションの番号を 1 つ進める。以後、前のセッションで起動した処理の結果は捨てる
	/// - 接続は `VoiceLimits::connect` で打ち切る。結果は `Connected` で届く
	///
	/// # 引数
	/// - `channels`: 接続するボイスチャンネルと、通知先のテキストチャンネル
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.begin_session(SessionChannels { voice, text });
	/// ```
	fn begin_session(&mut self, channels: SessionChannels) {
		self.notify(channels.text, plain(keys::PREPARING));
		self.open_session(channels);
	}

	/// セッションを始め、接続を別タスクで始める。通知はしない (`?p` と復元の共通の部分)。
	///
	/// - Connecting にし、セッションの番号を 1 つ進め、準備の遅延の通知の時刻を決める
	///
	/// # 引数
	/// - `channels`: 接続するボイスチャンネルと、通知先のテキストチャンネル
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.open_session(SessionChannels { voice, text });
	/// ```
	fn open_session(&mut self, channels: SessionChannels) {
		self.state = PlayerState::Connecting;
		self.channels = Some(channels);
		// Idle の間のコマンド (?loop、?vol) で読んだ設定を持ち越さず、セッションの始めに読み直す
		self.settings = None;
		self.slow = Some(Instant::now() + self.slow_threshold());
		self.session += 1;
		self.spawn_connect(channels.voice, None);
	}

	/// ボイスチャンネルへの接続を別タスクで始める。結果は今のセッションの番号を添えた `Connected` で届く。
	///
	/// - 接続は `VoiceLimits::connect` で打ち切る (バックエンドの参加の時間切れと Call の削除が先に終わる長さ)
	/// - `previous` があれば、同じタスクの中で先に切断してから接続する (切断と接続の順を保ち、アクターは待たない)。
	///   切断は `VoiceLimits::disconnect` で打ち切り、失敗はログに残すだけ
	/// - セッションの始めと、回復の再接続で使う (回復ではセッションの番号を変えない)
	///
	/// # 引数
	/// - `channel`: 接続するボイスチャンネル
	/// - `previous`: 接続の前に切断するセッション (回復の再接続)。無ければ None
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.spawn_connect(channels.voice, None);
	/// ```
	fn spawn_connect(&mut self, channel: ChannelId, previous: Option<Box<dyn VoiceSession>>) {
		let session = self.session;
		let audio = self.deps.audio.clone();
		let guild = self.guild;
		let events = self.tx.clone();
		let limit = self.voice_limits().connect();
		let disconnect_limit = self.voice_limits().disconnect();
		self.spawn_task(TaskKind::Connect(session), async move {
			if let Some(previous) = previous {
				disconnect_within(guild, disconnect_limit, previous).await;
			}
			let result = with_timeout(limit, audio.connect(guild, channel, events)).await;
			GuildMessage::Connected {
				session,
				result: result.map(VoiceLink),
			}
		});
	}

	/// 結果が今のセッションのものか。
	///
	/// - 番号が違う、または Idle (同じ番号のセッションが終わった後) なら古い
	///
	/// # 引数
	/// - `session`: 結果を起動したときのセッションの番号
	///
	/// # 戻り値
	/// 今のセッションの結果なら true
	///
	/// # 呼び出し例
	/// ```ignore
	/// if !self.is_current_session(session) { return; }
	/// ```
	fn is_current_session(&self, session: u64) -> bool {
		session == self.session && self.state != PlayerState::Idle
	}

	/// `?p` の問い合わせを別タスクで始める。結果は `QueryResolved` で届く。
	///
	/// - 動画は解決 (`Priority::Interactive`)、検索語は検索、プレイリストは一覧の取得
	///   (上限はギルドの `playlist_limit`)
	///
	/// # 引数
	/// - `query`: 分類済みの引数 (`Unsupported` と `Empty` 以外)
	/// - `context`: コマンドの発行元。結果に添えて返す
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.spawn_lookup(query, context).await;
	/// ```
	async fn spawn_lookup(&mut self, query: Query, context: CommandContext) {
		// 設定の読み込み (DB) はプレイリストのときだけ待つ。動画と検索の問い合わせを遅らせない
		let playlist_limit = match query {
			Query::Playlist(_) => self.settings().await.playlist_limit,
			_ => 0,
		};
		let resolver = self.deps.resolver.clone();
		let task_context = context.clone();
		let session = self.session;
		let kind = match query {
			Query::Search(_) => LookupKind::Search,
			Query::Playlist(_) => LookupKind::Playlist,
			Query::Video(_) | Query::Unsupported | Query::Empty => LookupKind::Video,
		};
		self.pending_lookups += 1;
		let task_kind = TaskKind::Lookup {
			session,
			context,
			kind,
		};
		self.spawn_task(task_kind, async move {
			let result = match query {
				Query::Video(id) => resolver
					.resolve(&id, Priority::Interactive)
					.await
					.map(QueryResult::Video),
				Query::Search(text) => resolver
					.search(&text, Priority::Interactive)
					.await
					.map(QueryResult::Search),
				Query::Playlist(url) => resolver
					.flat_playlist(&url, playlist_limit)
					.await
					.map(|meta| QueryResult::Playlist { url, meta }),
				Query::Unsupported | Query::Empty => Err(ResolveError::Worker {
					code: ResolveErrorCode::UnsupportedUrl,
					message: String::new(),
				}),
			};
			GuildMessage::QueryResolved {
				session,
				context: task_context,
				kind,
				result,
			}
		});
	}

	/// `?p` の問い合わせの結果を受ける。
	///
	/// - 前のセッション (接続の失敗や退出の前) の結果なら捨てる。待ちの数も減らさない
	///   (待ちの数はセッションの終わりで 0 に戻してあり、今のセッションの問い合わせだけを数える)
	/// - 成功ならキューへ入れ、今の曲が無ければ次の曲を始める。失敗なら通知する
	///   (YouTube の利用制限とプールの一時停止は専用の文言。自動では取り直さない)
	///
	/// # 引数
	/// - `session`: 問い合わせを起動したときのセッションの番号
	/// - `context`: コマンドの発行元
	/// - `result`: 問い合わせの結果
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_query_resolved(session, context, kind, result).await;
	/// ```
	async fn on_query_resolved(
		&mut self,
		session: u64,
		context: CommandContext,
		kind: LookupKind,
		result: Result<QueryResult, ResolveError>,
	) {
		if !self.is_current_session(session) {
			tracing::debug!(
				guild = self.guild.get(),
				session,
				"前のセッションの ?p の結果を捨てます"
			);
			return;
		}
		self.pending_lookups = self.pending_lookups.saturating_sub(1);
		match result {
			Ok(found) => self.enqueue_found(found, &context).await,
			Err(error) => {
				tracing::warn!(guild = self.guild.get(), %error, "?p の問い合わせに失敗しました");
				self.notify(
					context.text_channel,
					plain(lookup_failure_key(&error, kind)),
				);
			}
		}
		if self.voice.is_some() && self.current.is_none() {
			self.start_next().await;
		} else {
			self.kick_prefetch().await;
		}
	}

	/// 問い合わせで見つかった曲をキューの末尾へ入れ、結果を通知する。
	///
	/// - 上限 (`queue_limit`) を超える分は入れない
	/// - プレイリストは追加数と断った数を通知する。1 曲ならすぐ再生しない場合だけ追加を通知する。
	///   復元の接続を待つ間 (`pending_restore`) は、保存されていた曲を先に再生するため、すぐ再生しない扱いにする
	/// - 動画 URL の解決結果は先読みの状態へ預け、再生時に解決し直さない
	/// - 動画 URL がライブ配信なら、キューに入れずに `error.live_unsupported` を通知する
	///
	/// # 引数
	/// - `found`: 問い合わせの結果
	/// - `context`: コマンドの発行元 (依頼者と通知先)
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.enqueue_found(found, &context).await;
	/// ```
	async fn enqueue_found(&mut self, found: QueryResult, context: &CommandContext) {
		let user = context.user;
		// ライブ配信には対応しない (PipeInput の出力を Songbird が読めないため)。キューに入れずに断る
		if let QueryResult::Video(stream) = &found {
			if stream.is_live {
				self.notify(context.text_channel, plain(keys::LIVE_UNSUPPORTED));
				return;
			}
		}
		// プレイリストのカードに出す URL と名前
		let mut playlist: Option<(String, Option<String>)> = None;
		let (items, stash, is_playlist) = match found {
			QueryResult::Video(stream) => {
				(vec![item_from_stream(&stream, user)], Some(stream), false)
			}
			QueryResult::Search(meta) => (vec![item_from_meta(meta, user)], None, false),
			QueryResult::Playlist { url, meta: list } => {
				playlist = Some((url, list.title.clone()));
				let items = list
					.entries
					.into_iter()
					.map(|meta| item_from_meta(meta, user))
					.collect();
				(items, None, true)
			}
		};
		let first_item = items.first().cloned();
		let Some(title) = items.first().map(|item| item.title.clone()) else {
			self.notify(context.text_channel, plain(keys::PLAYLIST_EMPTY));
			return;
		};
		let limit = self.settings().await.queue_limit;
		let guild = self.guild;
		let appended = match self
			.write(move |c| store::enqueue(c, guild, &items, limit))
			.await
		{
			Ok(result) => result,
			Err(error) => {
				tracing::error!(guild = guild.get(), %error, "キューに追加できません");
				self.notify(context.text_channel, plain(keys::LOOKUP_FAILED));
				return;
			}
		};
		let before = appended.before;
		if let (Some(stream), true) = (stash, appended.added > 0) {
			self.prefetch.stash(stream);
		}
		// 復元の接続を待つ間は、接続の後に保存されていた曲を先に再生するため、すぐには再生しない
		let starts_now = self.current.is_none() && before == 0 && self.pending_restore.is_none();
		let notice = if appended.added == 0 {
			with_args(keys::QUEUE_FULL, [(keys::ARG_LIMIT, limit.to_string())])
		} else if is_playlist {
			let (url, name) = playlist.unwrap_or_default();
			counted(PlaylistCard {
				guild,
				title: name,
				url,
				first_video: first_item.map(|item| item.video_id),
				added: appended.added,
				rejected: appended.rejected,
				requester: user,
			})
		} else if starts_now {
			return;
		} else {
			let mut notice = titled(keys::QUEUED, &title);
			notice.card = first_item.map(|item| {
				Card::Queued(TrackCard {
					guild,
					video_id: item.video_id,
					title: item.title,
					duration_s: item.duration_s,
					requester: item.requester_id,
					waiting: before + appended.added,
				})
			});
			notice
		};
		self.notify(context.text_channel, notice);
	}

	/// 接続の完了を受ける。
	///
	/// - 古い接続 (セッションの番号が違う、または Connecting / Recovering でない) なら、別タスクで切断して捨てる
	///   (アクターは切断を待たない。バックエンドは Call の持ち主の記録で、新しい接続の Call に触れない)
	/// - 成功なら次の曲を始める。復元の接続なら保存されていた曲を、回復中なら今の曲を、保存した位置から再生する
	/// - 失敗なら通知し、`?leave` と同じくキューと `playback_state` を消して Idle に戻る
	///   (接続の前に問い合わせが終わってキューに入った曲を、次のセッションで再生しないため)
	/// - 復元の接続の失敗はキューを残す。保存されていた曲をキューの先頭へ戻し、`playback_state` を消す
	///   (`on_restore_connect_failed`、起動時の復元の手順)
	/// - 回復中の失敗は `on_rejoin_failed` が処理する
	///
	/// # 引数
	/// - `session`: 接続を起動したときのセッションの番号
	/// - `result`: 接続の結果
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_connected(session, result).await;
	/// ```
	async fn on_connected(&mut self, session: u64, result: Result<VoiceLink, AudioError>) {
		let expected = session == self.session
			&& matches!(
				self.state,
				PlayerState::Connecting | PlayerState::Recovering
			);
		let rejoining = self.state == PlayerState::Recovering;
		match result {
			Ok(VoiceLink(voice)) if !expected => {
				tracing::debug!(guild = self.guild.get(), session, "古い接続を切断します");
				let limit = self.voice_limits().disconnect();
				tokio::spawn(disconnect_within(self.guild, limit, voice));
			}
			Err(error) if !expected => {
				tracing::debug!(guild = self.guild.get(), session, %error, "古い接続の失敗を無視します");
			}
			Ok(VoiceLink(voice)) => {
				self.attach_voice(voice);
				if rejoining {
					self.on_rejoined().await;
				} else {
					self.state = PlayerState::Preparing;
					match self.pending_restore.take() {
						Some(saved) => self.start_restored(saved),
						None => self.start_next().await,
					}
				}
			}
			Err(error) if rejoining => self.on_rejoin_failed(error).await,
			Err(error) => {
				tracing::warn!(guild = self.guild.get(), %error, "ボイスチャンネルに接続できません");
				if let Some(saved) = self.pending_restore.take() {
					self.on_restore_connect_failed(saved).await;
					return;
				}
				if let Some(channels) = self.channels {
					self.notify(channels.text, plain(keys::CONNECT_FAILED));
				}
				// 削除の失敗は leave_and_clear がログに残す。接続の失敗はすでに通知した
				let _ = self.leave_and_clear().await;
			}
		}
	}

	/// キューの先頭を次の曲にして再生を始める。
	///
	/// - 取り出しと `playback_state` の書き込みは 1 トランザクション
	/// - 取り出しに失敗したら、終わった曲の `playback_state` を消す (次の起動で再生し直さないため)
	/// - 再生に失敗した曲は通知して飛ばし、次の曲へ進む
	/// - キューが空なら、問い合わせ待ちがあれば待ち、無ければ退出する
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.start_next().await;
	/// ```
	async fn start_next(&mut self) {
		let Some(channels) = self.channels else {
			return;
		};
		let guild = self.guild;
		let popped = self
			.write(move |c| store::pop_next(c, guild, channels))
			.await;
		self.start_popped(popped).await;
	}

	/// 終えた曲 (終了、または `?skip` で飛ばした曲) を記録し、次の曲を始める。
	///
	/// - 記録と次の曲の取り出しは 1 回の書き込み (1 トランザクション) で行う (`store::finish_and_pop_next`)。
	///   ループ中は終えた曲をキューの末尾へ戻してから取り出す
	/// - 再生を始めていた曲だけを履歴に記録する。記録の失敗はログに残すだけで、取り出しは続ける
	/// - 取り出しに失敗したときの扱いは `start_next` と同じ
	///
	/// # 引数
	/// - `finished`: 今の曲から外した曲
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.finish_and_start_next(finished).await;
	/// ```
	async fn finish_and_start_next(&mut self, finished: CurrentTrack) {
		let Some(channels) = self.channels else {
			self.record_finished(finished).await;
			return;
		};
		let settings = self.settings().await;
		let guild = self.guild;
		let played_at = finished.started_at;
		let loop_limit = settings.loop_enabled.then_some(settings.queue_limit);
		let item = finished.item;
		let popped = self
			.write(move |c| {
				store::finish_and_pop_next(c, guild, &item, played_at, loop_limit, channels)
			})
			.await;
		self.start_popped(popped).await;
	}

	/// 取り出した曲で再生を始める。曲を飛ばしたら、次の曲を取り出して繰り返す。
	///
	/// - 取り出しに失敗したら、終わった曲の `playback_state` を消す (次の起動で再生し直さないため)
	/// - キューが空なら、問い合わせ待ちがあれば待ち、無ければ退出する
	///
	/// # 引数
	/// - `popped`: 取り出しの結果 (`store::pop_next` などの戻り値)
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.start_popped(popped).await;
	/// ```
	async fn start_popped(&mut self, mut popped: Result<Option<NewQueueItem>, DbError>) {
		loop {
			let item = match popped {
				Ok(Some(item)) => item,
				Ok(None) => {
					self.wait_or_leave(true).await;
					return;
				}
				Err(error) => {
					tracing::error!(guild = self.guild.get(), %error, "次の曲を取り出せません");
					self.discard_saved_state().await;
					self.wait_or_leave(false).await;
					return;
				}
			};
			if self.start_track(item).await == Next::Stay {
				return;
			}
			let Some(channels) = self.channels else {
				return;
			};
			let guild = self.guild;
			popped = self
				.write(move |c| store::pop_next(c, guild, channels))
				.await;
		}
	}

	/// 曲を今の曲にして再生を始める。世代を 1 つ進める。
	///
	/// - 準備済み (`preload`) の曲、解決済みの曲の順に使う。どちらも無ければ解決を別タスクで始める
	/// - 準備済みの曲を始められなければ、バックエンドから捨ててから解決済みのストリームで作り直す
	/// - `playback_state` は呼び出し側が書いておく
	///
	/// # 引数
	/// - `item`: 今の曲にする曲
	///
	/// # 戻り値
	/// 再生を始めた、または解決を始めたら `Next::Stay`。再生に失敗して曲を飛ばしたら `Next::Advance`
	///
	/// # 呼び出し例
	/// ```ignore
	/// if self.start_track(item).await == Next::Advance { self.start_next().await; }
	/// ```
	async fn start_track(&mut self, item: NewQueueItem) -> Next {
		let video_id = item.video_id.clone();
		self.set_current(Some(item));
		self.state = PlayerState::Preparing;
		if let Some((track, format_id)) = self.prefetch.take_preloaded(&video_id) {
			if self.start_preloaded(track, format_id).await {
				return Next::Stay;
			}
			self.drop_preloaded(track).await;
		}
		if let Some(stream) = self.prefetch.take_stream(&video_id) {
			return self.play_stream(stream).await;
		}
		self.spawn_resolve(video_id);
		if self.slow.is_none() {
			self.slow = Some(Instant::now() + self.slow_threshold());
		}
		Next::Stay
	}

	/// キューが空のときの処理。問い合わせ待ちがあれば Preparing で待ち、無ければ退出する。
	///
	/// - 退出したら、退出の後にセッションのテキストチャンネルへ `play.completed` を送る。
	///   ただし `completed` が false のとき、またはこのセッションで 1 曲も今の曲にしていないときは送らない
	///   (例: 最初の `?p` の問い合わせの失敗、ライブ配信を断ったとき。失敗の通知はすでに出ている)
	///
	/// # 引数
	/// - `completed`: キューを読み切って空になったら true。取り出しが DB のエラーで失敗したら false
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.wait_or_leave(true).await;
	/// ```
	async fn wait_or_leave(&mut self, completed: bool) {
		self.set_current(None);
		if self.pending_lookups > 0 {
			self.state = PlayerState::Preparing;
			return;
		}
		// 退出で消えるので、先に控える
		let channels = self.channels;
		let played = self.played_in_session;
		self.leave().await;
		if let (Some(channels), true, true) = (channels, completed, played) {
			self.notify(channels.text, plain(keys::COMPLETED));
		}
	}

	/// 準備済みの曲の再生を始める。
	///
	/// # 引数
	/// - `track`: `preload` で得た識別子
	/// - `format_id`: 準備した形式
	///
	/// # 戻り値
	/// 始めたら true。失敗したら false (呼び出し側は解決済みのストリームで作り直す)
	///
	/// # 呼び出し例
	/// ```ignore
	/// if self.start_preloaded(track, format_id).await { return; }
	/// ```
	async fn start_preloaded(&mut self, track: TrackId, format_id: String) -> bool {
		match self.call_voice(|voice| voice.start_preloaded(track)).await {
			Ok(()) => {
				self.on_started(track, format_id, false).await;
				true
			}
			Err(error) => {
				tracing::warn!(guild = self.guild.get(), %error, "準備済みの曲を始められません。作り直します");
				false
			}
		}
	}

	/// 解決済みのストリームで今の曲を再生する。
	///
	/// - 位置と方式は今の曲の `start_at` と `use_pipe` に従う (失敗や回復の後の再開)
	/// - 時間切れなら曲は飛ばさない。セッションが壊れたとみなし、回復の後に再生し直す
	/// - 失敗は曲に固有でない飛ばしとして数え、続いていれば飛ばさずに再生を止める (`skip_or_halt`)
	/// - ライブ配信は再生せず、曲に固有の飛ばしとして `error.live_skipped` を通知する
	///
	/// # 引数
	/// - `stream`: 今の曲のストリーム
	///
	/// # 戻り値
	/// 始めた、回復を待つ、または再生を止めたなら `Next::Stay`。失敗して曲を飛ばしたら (通知済み) `Next::Advance`
	///
	/// # 呼び出し例
	/// ```ignore
	/// if self.play_stream(stream).await == Next::Advance { self.start_next().await; }
	/// ```
	async fn play_stream(&mut self, stream: ResolvedStream) -> Next {
		// 検索やプレイリストで入った曲は、解決して初めてライブ配信と分かる。曲に固有の理由として飛ばす
		if stream.is_live {
			tracing::info!(guild = self.guild.get(), video_id = %stream.video_id.0, "ライブ配信のため飛ばします");
			return self.skip_or_halt(keys::LIVE_SKIPPED, true).await;
		}
		let volume = self.settings().await.volume;
		let (start_at, force_pipe, paused) = self
			.current
			.as_ref()
			.map(|current| (current.start_at, current.use_pipe, current.pause_on_start))
			.unwrap_or_default();
		let format_id = stream.format_id.clone();
		// 一時停止に戻す曲は、一時停止のまま載せる (始めてから止めると一瞬音が出るため)
		let request = play_request(stream, volume, start_at, force_pipe, paused);
		match self.call_voice(move |voice| voice.play(request)).await {
			Ok(track) => {
				self.on_started(track, format_id, paused).await;
				Next::Stay
			}
			Err(AudioError::Timeout) => {
				tracing::warn!(
					guild = self.guild.get(),
					"再生の開始が時間切れになりました。回復の後に再生し直します"
				);
				Next::Stay
			}
			Err(error) => {
				tracing::warn!(guild = self.guild.get(), %error, "曲を再生できません");
				self.skip_or_halt(keys::TRACK_SKIPPED, false).await
			}
		}
	}

	/// 再生の開始を記録し、通知して先読みを進める。
	///
	/// - 再生中の通知は曲ごとに 1 回。失敗や回復の後の再開では通知しない。復元の曲は復元の通知を送る
	/// - 一時停止に戻す予定 (`pause_on_start`) があれば Paused にする (通知の前に)。
	///   一時停止のまま載せた曲はそのまま記録し、そうでない曲 (準備済みの曲) は始めてすぐ一時停止に戻す
	/// - 作り直した曲 (2 回目以降の再生) は、始めた位置と一時停止の状態をすぐ保存する
	///   (`?rejoin` の前に一時停止中として保存した状態を、再開した状態に直すため)
	/// - 取得の不調による飛ばしの連続 (`skip_streak`) を数え直す
	///
	/// # 引数
	/// - `track`: 再生を始めた曲の識別子
	/// - `format_id`: 再生した形式 (曲の失敗の後の取り直しで同じ形式に固定するため)
	/// - `loaded_paused`: 一時停止のまま載せたか (`PlayRequest::paused`)
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_started(track, format_id, false).await;
	/// ```
	async fn on_started(&mut self, track: TrackId, format_id: String, loaded_paused: bool) {
		let Some(current) = self.current.as_mut() else {
			return;
		};
		let first_start = current.started_at.is_none();
		current.mark_started(track, now_secs());
		current.format_id = Some(format_id);
		let pause = current.pause_on_start;
		let restored = current.restored;
		let start_at = current.start_at;
		let item = current.item.clone();
		self.state = PlayerState::Playing;
		self.slow = None;
		self.sup.skip_streak = 0;
		// 健全性の猶予は、この曲の開始 (`TrackStarted`) が届いてから数える
		self.sup.started_at = None;
		if pause && loaded_paused {
			self.mark_paused_after_restart();
		} else if pause {
			self.pause_after_restart().await;
		}
		if first_start && !restored {
			// 再生中のカードの待機数と先読みの対象を、1 回の読み取りでまとめて読む
			match self.read_queue_head().await {
				Ok((waiting, upcoming)) => {
					self.announce_start(&item, waiting, restored);
					self.prefetch_upcoming(upcoming).await;
				}
				Err(error) => {
					tracing::warn!(guild = self.guild.get(), %error, "キューを読めません。待機数を 0 として扱い、先読みしません");
					self.announce_start(&item, 0, restored);
				}
			}
			return;
		}
		if first_start {
			self.announce_start(&item, 0, restored);
		} else {
			let paused = self.persisted_paused();
			self.persist_position(start_at, paused).await;
		}
		self.kick_prefetch().await;
	}

	/// キューの曲数と、先頭から `playback.prefetch_tracks` 曲の動画を DbReader から 1 回で読む。
	///
	/// # 戻り値
	/// キューの曲数と、先頭からの動画 (キューの順)
	///
	/// # エラー
	/// 読み取りの失敗
	///
	/// # 呼び出し例
	/// ```ignore
	/// let (waiting, upcoming) = self.read_queue_head().await?;
	/// ```
	async fn read_queue_head(&mut self) -> Result<(u32, Vec<VideoId>), DbError> {
		let guild = self.guild;
		let depth = self.prefetch.depth();
		self.deps
			.db
			.reader
			.read(move |c| {
				let count = queue::count(c, guild)?;
				let upcoming = queue::peek(c, guild, depth)?
					.into_iter()
					.map(|q| q.item.video_id)
					.collect();
				Ok((count, upcoming))
			})
			.await
	}

	/// 今の曲を飛ばす。曲名を添えて通知する。
	///
	/// - 取得の不調による飛ばしの連続を数える場面では `skip_or_halt` を使う
	///
	/// # 引数
	/// - `key`: 通知のキー (理由ごとの文面。曲名を差し込む)
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.skip_current(keys::TRACK_SKIPPED);
	/// ```
	fn skip_current(&mut self, key: &'static str) {
		if let (Some(current), Some(channels)) = (&self.current, self.channels) {
			self.notify(channels.text, titled_code(key, &current.item.title));
		}
		self.set_current(None);
	}

	/// 今の曲の解決を別タスクで始める。結果は今の世代を添えた `Resolved` で届く。
	///
	/// - 今の曲に取り直しの指定 (曲の失敗の後) があれば、キャッシュを使わずに同じ形式で取り直す
	///
	/// # 引数
	/// - `video_id`: 今の曲
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.spawn_resolve(video_id);
	/// ```
	fn spawn_resolve(&mut self, video_id: VideoId) {
		let generation = self.generation;
		let resolver = self.deps.resolver.clone();
		let refresh = self
			.current
			.as_ref()
			.filter(|current| current.refresh && current.item.video_id == video_id)
			.map(|current| current.format_id.clone());
		let kind = TaskKind::Resolve {
			generation,
			video_id: video_id.clone(),
		};
		self.spawn_task(kind, async move {
			let result = match refresh {
				Some(format_id) => resolver.refresh(&video_id, format_id.as_deref()).await,
				None => resolver.resolve(&video_id, Priority::Interactive).await,
			};
			GuildMessage::Resolved {
				generation,
				video_id,
				result,
			}
		});
	}

	/// 今の曲の解決結果を受ける。
	///
	/// - 世代が違う、曲が違う、または再生を始めた後なら捨てる (古い結果で再生を始めない)
	/// - 失敗ならエラーコードごとに、やり直すか飛ばすかを決める (`on_resolve_failed`)
	/// - 飛ばした、または再生に失敗したら次の曲へ進む
	///
	/// # 引数
	/// - `generation`: 解決を始めたときの世代
	/// - `video_id`: 解決した曲
	/// - `result`: 解決結果
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_resolved(generation, video_id, result).await;
	/// ```
	async fn on_resolved(
		&mut self,
		generation: u64,
		video_id: VideoId,
		result: Result<ResolvedStream, ResolveError>,
	) {
		let fresh = self.current.as_ref().is_some_and(|current| {
			current.generation == generation
				&& current.is_preparing()
				&& current.item.video_id == video_id
		});
		if !fresh {
			tracing::debug!(
				guild = self.guild.get(),
				generation,
				"古い解決結果を捨てます"
			);
			return;
		}
		let next = match result {
			Ok(stream) => {
				if let Some(current) = self.current.as_mut() {
					current.attempts.on_resolved();
				}
				self.play_stream(stream).await
			}
			Err(error) => self.on_resolve_failed(&error).await,
		};
		if next == Next::Advance {
			self.start_next().await;
		}
	}

	/// 先読みの解決結果を受け、先読みを進める。
	///
	/// - 前のセッションの結果なら捨てる (今のセッションの解決中の記録を消したり、失敗の記録を残したりしない)
	/// - 失敗した曲は再生するときに通常の優先度で解決し直す
	///
	/// # 引数
	/// - `session`: 先読みを起動したときのセッションの番号
	/// - `video_id`: 解決した曲
	/// - `result`: 解決結果
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_prefetched(session, video_id, result).await;
	/// ```
	async fn on_prefetched(
		&mut self,
		session: u64,
		video_id: VideoId,
		result: Result<ResolvedStream, ResolveError>,
	) {
		if !self.is_current_session(session) {
			tracing::debug!(
				guild = self.guild.get(),
				session,
				"前のセッションの先読みの結果を捨てます"
			);
			return;
		}
		if let Err(error) = &result {
			tracing::warn!(guild = self.guild.get(), video_id = %video_id.0, %error, "先読みに失敗しました");
		}
		let succeeded = result.is_ok();
		self.prefetch.complete(video_id.clone(), result);
		if succeeded && self.waiting_for(&video_id) {
			// 今の曲が同じ曲の解決を待っていれば、その解決を待たずに先読みの結果で始める。
			// 遅れて届く今の曲の解決結果は、再生を始めた後なので捨てられる
			if let Some(stream) = self.prefetch.take_stream(&video_id) {
				if let Some(current) = self.current.as_mut() {
					current.attempts.on_resolved();
				}
				if self.play_stream(stream).await == Next::Advance {
					self.start_next().await;
				}
				return;
			}
		}
		self.kick_prefetch().await;
	}

	/// 今の曲がこの曲の解決を待っているか (先読みの結果をそのまま使えるか)。
	///
	/// - 準備中で、取り直しの指定 (曲の失敗の後の同じ形式での取り直し) が無いときだけ true
	///
	/// # 引数
	/// - `video_id`: 先読みで解決した曲
	///
	/// # 呼び出し例
	/// ```ignore
	/// if self.waiting_for(&video_id) { ... }
	/// ```
	fn waiting_for(&self, video_id: &VideoId) -> bool {
		self.voice.is_some()
			&& self.current.as_ref().is_some_and(|current| {
				current.is_preparing() && !current.refresh && current.item.video_id == *video_id
			})
	}

	/// 先読みを進める。キューを変えた後にも呼ぶ。
	///
	/// - `preload` 済みの曲がキューの先頭 (次の曲) でなくなっていれば、バックエンドから捨てる。
	///   これは再生中でなくても行う
	/// - 再生中 (一時停止を含む) なら、キューの先頭から `playback.prefetch_tracks` 曲を読み、
	///   先頭が解決済みなら `preload` し、未解決の曲があれば 1 曲だけ解決を始める
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.kick_prefetch().await;
	/// ```
	async fn kick_prefetch(&mut self) {
		if !self.prefetch_wanted() {
			return;
		}
		let guild = self.guild;
		let depth = self.prefetch.depth();
		let upcoming: Vec<VideoId> = match self
			.deps
			.db
			.reader
			.read(move |c| queue::peek(c, guild, depth))
			.await
		{
			Ok(items) => items.into_iter().map(|q| q.item.video_id).collect(),
			Err(error) => {
				tracing::warn!(guild = guild.get(), %error, "先読みのためにキューを読めません");
				return;
			}
		};
		self.prefetch_upcoming(upcoming).await;
	}

	/// 先読みを見直す必要があるか (`kick_prefetch` の前提)。
	///
	/// # 戻り値
	/// 接続していて、再生中 (一時停止を含む) か準備済みの曲があれば true
	///
	/// # 呼び出し例
	/// ```ignore
	/// if !self.prefetch_wanted() { return; }
	/// ```
	fn prefetch_wanted(&self) -> bool {
		let playing = matches!(self.state, PlayerState::Playing | PlayerState::Paused);
		self.voice.is_some() && (playing || self.prefetch.has_preloaded())
	}

	/// 読み込み済みのキューの先頭で先読みを進める (`kick_prefetch` の読み取りの後半)。
	///
	/// - 前提 (`prefetch_wanted`) を満たさなければ何もしない
	///
	/// # 引数
	/// - `upcoming`: キューの先頭から `playback.prefetch_tracks` 曲の動画 (キューの順)
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.prefetch_upcoming(upcoming).await;
	/// ```
	async fn prefetch_upcoming(&mut self, upcoming: Vec<VideoId>) {
		if !self.prefetch_wanted() {
			return;
		}
		if let Some(track) = self.prefetch.take_stale_preload(upcoming.first()) {
			self.drop_preloaded(track).await;
		}
		if !matches!(self.state, PlayerState::Playing | PlayerState::Paused) {
			return;
		}
		let plan = self.prefetch.plan(&upcoming);
		if let Some(stream) = plan.preload {
			self.preload(stream).await;
		}
		if let Some(video_id) = plan.resolve {
			let resolver = self.deps.resolver.clone();
			let session = self.session;
			let kind = TaskKind::Prefetch {
				session,
				video_id: video_id.clone(),
			};
			self.spawn_task(kind, async move {
				let result = resolver.resolve(&video_id, Priority::Prefetch).await;
				GuildMessage::Prefetched {
					session,
					video_id,
					result,
				}
			});
		}
	}

	/// 次の曲を `preload` で準備する。失敗したらその曲では繰り返さない。
	///
	/// - ライブ配信は準備しない (準備の失敗として扱う)。再生の直前に解決し直され、`play_stream` で飛ばす
	///
	/// # 引数
	/// - `stream`: 次の曲のストリーム
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.preload(stream).await;
	/// ```
	async fn preload(&mut self, stream: ResolvedStream) {
		let video_id = stream.video_id.clone();
		// ライブ配信は準備しない。再生の直前に解決し直し、play_stream で飛ばす
		if stream.is_live {
			self.prefetch.mark_preload_failed(video_id);
			return;
		}
		let format_id = stream.format_id.clone();
		let volume = self.settings().await.volume;
		let request = play_request(stream, volume, Duration::ZERO, false, false);
		match self.call_voice(move |voice| voice.preload(request)).await {
			Ok(track) => self.prefetch.mark_preloaded(video_id, track, format_id),
			Err(error) => {
				tracing::warn!(guild = self.guild.get(), %error, "次の曲を準備できません");
				self.prefetch.mark_preload_failed(video_id);
			}
		}
	}

	/// 再生の開始のイベントを受ける。
	///
	/// - 状態は `play` の成功で変えているため、状態は変えない
	/// - 今の曲なら、健全性の猶予の起点をこの時刻にする (それまでは判定しない)
	/// - 今の曲でなければログに残すだけ
	///
	/// # 引数
	/// - `track`: 始まった曲
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_track_started(track);
	/// ```
	fn on_track_started(&mut self, track: TrackId) {
		let is_current = self
			.current
			.as_ref()
			.is_some_and(|current| current.track == Some(track));
		tracing::debug!(
			guild = self.guild.get(),
			track = track.0,
			is_current,
			"曲が始まりました"
		);
		if is_current {
			self.sup.started_at = Some(Instant::now());
			self.sup.judge.reset();
		}
	}

	/// 曲の終了を受ける。
	///
	/// - 今の曲でなければ無視する
	/// - 履歴に記録し、ループ中ならキューの末尾へ戻してから、次の曲を始める (`finish_and_start_next`)
	///
	/// # 引数
	/// - `track`: 終わった曲
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_track_ended(track).await;
	/// ```
	async fn on_track_ended(&mut self, track: TrackId) {
		let Some(finished) = self.current.take_if(|current| current.track == Some(track)) else {
			tracing::debug!(
				guild = self.guild.get(),
				track = track.0,
				"今の曲ではない終了を無視します"
			);
			return;
		};
		self.set_current(None);
		self.finish_and_start_next(finished).await;
	}

	/// 終えた曲を記録するだけで、次の曲は取り出さない (セッションの宛先が無いとき)。
	///
	/// - 再生を始めていた曲だけを履歴に記録する
	/// - ループ中ならキューの末尾へ戻す (飛ばした曲も戻す)
	/// - 書き込みに失敗したらログに残すだけ
	///
	/// # 引数
	/// - `finished`: 今の曲から外した曲
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.record_finished(finished).await;
	/// ```
	async fn record_finished(&mut self, finished: CurrentTrack) {
		let settings = self.settings().await;
		let guild = self.guild;
		let played_at = finished.started_at;
		let loop_limit = settings.loop_enabled.then_some(settings.queue_limit);
		let item = finished.item;
		if let Err(error) = self
			.write(move |c| store::finish(c, guild, &item, played_at, loop_limit))
			.await
		{
			tracing::error!(guild = guild.get(), %error, "終わった曲を記録できません");
		}
	}

	/// 準備が `playback.slow_notice_secs` を超えたことを通知する。1 回の準備につき 1 回。
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_slow();
	/// ```
	fn on_slow(&mut self) {
		self.slow = None;
		let waiting = matches!(self.state, PlayerState::Connecting | PlayerState::Preparing);
		if let (true, Some(channels)) = (waiting, self.channels) {
			self.notify(channels.text, plain(keys::SLOW));
		}
	}

	/// 切断して Idle に戻る。キューと `playback_state` は消さない。
	///
	/// - キューが空になったときに使う (`playback_state` は取り出しのときに消してある)
	/// - `preload` 済みの曲があれば、切断の前にバックエンドから捨てる
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.leave().await;
	/// ```
	async fn leave(&mut self) {
		self.drop_preload_before_leave().await;
		if let Some(session) = self.voice.take() {
			self.disconnect_session(session).await;
		}
		self.reset_to_idle();
	}

	/// 退出の前に、`preload` 済みの曲をバックエンドから捨てる (切断の前に行う)。
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.drop_preload_before_leave().await;
	/// ```
	async fn drop_preload_before_leave(&mut self) {
		if let Some(track) = self.prefetch.take_stale_preload(None) {
			self.drop_preloaded(track).await;
		}
	}

	/// 切断して Idle に戻り、キューと `playback_state` を消す (`?leave`、自動退出、接続の失敗)。
	///
	/// - `preload` 済みの曲を捨ててから、切断と DB の削除を並行して行い、両方の終わりを待つ
	/// - 起動中の処理は止め、遅れて届いた結果はセッションの番号と世代で捨てる。退出の後に再生は始まらない
	/// - DB の削除に失敗したらログに残してエラーを返す (切断と Idle への遷移は済ませる)
	///
	/// # 戻り値
	/// キューと `playback_state` を消せたら `Ok(())`
	///
	/// # エラー
	/// 削除の失敗 (やり直しても失敗したとき)。呼び出し側は成功の通知の代わりに `error.save_failed` を送る
	///
	/// # 呼び出し例
	/// ```ignore
	/// let key = if self.leave_and_clear().await.is_ok() { keys::LEFT } else { keys::SAVE_FAILED };
	/// ```
	async fn leave_and_clear(&mut self) -> Result<(), DbError> {
		self.drop_preload_before_leave().await;
		let guild = self.guild;
		let limit = self.voice_limits().disconnect();
		let session = self.voice.take();
		let disconnect = async move {
			if let Some(session) = session {
				disconnect_within(guild, limit, session).await;
			}
		};
		let clear = self.write(move |c| store::clear_session(c, guild));
		let ((), result) = tokio::join!(disconnect, clear);
		self.reset_to_idle();
		if let Err(error) = &result {
			tracing::error!(guild = guild.get(), %error, "退出の後にキューと再生状態を消せません");
		}
		result
	}

	/// 今の曲をキューの先頭へ戻し、`playback_state` を消してから、切断して Idle に戻る。キューは残す。
	///
	/// - 取得の不調や YouTube の利用制限で再生を止めるとき、回復の再接続に失敗したときに使う。利用者は `?p` でキューの先頭から再開できる
	/// - 戻すのは今の曲だけで、位置は残さない (次の再生は先頭から)
	/// - DB の書き込みに失敗したらログに残してエラーを返す (切断と Idle への遷移は済ませる)
	///
	/// # 戻り値
	/// 曲を戻して `playback_state` を消せたら `Ok(())`
	///
	/// # エラー
	/// 書き込みの失敗 (やり直しても失敗したとき)。呼び出し側は案内の代わりに `error.save_failed` を送る
	///
	/// # 呼び出し例
	/// ```ignore
	/// let key = if self.leave_keeping_queue().await.is_ok() { keys::REJOIN_FAILED } else { keys::SAVE_FAILED };
	/// ```
	async fn leave_keeping_queue(&mut self) -> Result<(), DbError> {
		let guild = self.guild;
		let item = self.current.as_ref().map(|current| current.item.clone());
		let result = self
			.write(move |c| store::requeue_current(c, guild, item.as_ref()))
			.await;
		if let Err(error) = &result {
			tracing::error!(guild = guild.get(), %error, "今の曲をキューへ戻せません");
		}
		self.leave().await;
		result
	}

	/// 音声セッションを切断する。失敗とタイムアウトはログに残すだけ。
	///
	/// - 上限は `VoiceLimits::disconnect` (バックエンドの Call の停止と削除の上限の合計より長い)
	///
	/// # 引数
	/// - `session`: 切断するセッション
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.disconnect_session(session).await;
	/// ```
	async fn disconnect_session(&mut self, session: Box<dyn VoiceSession>) {
		let limit = self.voice_limits().disconnect();
		disconnect_within(self.guild, limit, session).await;
	}

	/// 準備済みの曲をバックエンドから捨てる。失敗とタイムアウトはログに残すだけ。
	///
	/// # 引数
	/// - `track`: `preload` で得た識別子
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.drop_preloaded(track).await;
	/// ```
	async fn drop_preloaded(&mut self, track: TrackId) {
		if self.voice.is_none() {
			return;
		}
		if let Err(error) = self.call_voice(|voice| voice.drop_preloaded(track)).await {
			tracing::warn!(guild = self.guild.get(), %error, "準備済みの曲を捨てられません");
		}
	}

	/// 今の曲の再生を止める (準備済みの曲は残る)。失敗とタイムアウトはログに残すだけ。
	///
	/// - 止めた曲の終了のイベントが後で届いても、今の曲を差し替えてあれば無視される
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.stop_playback().await;
	/// ```
	async fn stop_playback(&mut self) {
		if self.voice.is_none() {
			return;
		}
		if let Err(error) = self.call_voice(|voice| voice.stop()).await {
			tracing::warn!(guild = self.guild.get(), %error, "再生を止められません");
		}
	}

	/// セッションの状態を捨てて Idle に戻る。起動中の処理も止める。
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.reset_to_idle();
	/// ```
	fn reset_to_idle(&mut self) {
		self.state = PlayerState::Idle;
		self.channels = None;
		self.set_current(None);
		self.played_in_session = false;
		self.slow = None;
		self.alone = None;
		self.pending_lookups = 0;
		self.settings = None;
		self.prefetch.clear();
		self.sup.end_session();
		self.save_at = None;
		self.pending_restore = None;
		self.abort_session_tasks();
	}

	/// 起動中の処理のうち、接続以外を止める。
	///
	/// - 接続は止めない。Songbird の参加は future を捨てても続くため、止めると Bot がチャンネルに残る。
	///   遅れて届いた接続は `on_connected` が古い接続として切断する
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.abort_session_tasks();
	/// ```
	fn abort_session_tasks(&mut self) {
		for (kind, handle) in self.task_kinds.values() {
			if !matches!(kind, TaskKind::Connect(_)) {
				handle.abort();
			}
		}
	}

	/// 登録簿から自分を消し、メールボックスを閉じる。
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.retire();
	/// ```
	fn retire(&mut self) {
		match self.registry.upgrade() {
			Some(registry) => registry.retire(self.guild, self.id, &mut self.rx),
			None => self.rx.close(),
		}
	}

	/// 起動した処理の終了を受ける。panic したなら、送るはずだった失敗のメッセージを代わりに処理する。
	///
	/// # 引数
	/// - `joined`: `JoinSet` から受け取った結果
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_task_joined(joined).await;
	/// ```
	async fn on_task_joined(&mut self, joined: Result<(Id, ()), JoinError>) {
		let error = match joined {
			Ok((id, ())) => {
				self.task_kinds.remove(&id);
				return;
			}
			Err(error) => error,
		};
		let kind = self.task_kinds.remove(&error.id()).map(|(kind, _)| kind);
		if error.is_cancelled() {
			return;
		}
		tracing::error!(guild = self.guild.get(), %error, "アクターが起動した処理が異常終了しました");
		if let Some(kind) = kind {
			// 失敗のメッセージは Shutdown ではないので、常に処理を続ける
			let _ = self.handle(kind.into_failure()).await;
		}
	}

	/// 処理を別タスクで起動する。処理の戻り値のメッセージを自分のメールボックスへ送る。
	///
	/// - アクターが終了していて送れず、それが成功した接続なら、その場で切断する (Bot をチャンネルに残さない)
	///
	/// # 引数
	/// - `kind`: 処理の種類 (panic したときの代わりのメッセージを作るため)
	/// - `work`: 処理の本体
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.spawn_task(TaskKind::Connect(session), async move { ... });
	/// ```
	fn spawn_task(
		&mut self,
		kind: TaskKind,
		work: impl Future<Output = GuildMessage> + Send + 'static,
	) {
		let tx = self.tx.clone();
		let guild = self.guild;
		let disconnect_limit = self.voice_limits().disconnect();
		let handle = self.tasks.spawn(async move {
			let message = work.await;
			let Err(mpsc::error::SendError(message)) = tx.send(message).await else {
				return;
			};
			tracing::debug!(
				guild = guild.get(),
				"アクターが終了していたため結果を捨てます"
			);
			if let GuildMessage::Connected {
				result: Ok(VoiceLink(voice)),
				..
			} = message
			{
				if let Err(error) = with_timeout(disconnect_limit, voice.disconnect()).await {
					tracing::warn!(guild = guild.get(), %error, "捨てた接続を切断できません");
				}
			}
		});
		self.task_kinds.insert(handle.id(), (kind, handle));
	}

	/// 今の曲を差し替え、世代を 1 つ進める。
	///
	/// # 引数
	/// - `item`: 次の曲。None なら今の曲を無しにする
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.set_current(Some(item));
	/// ```
	fn set_current(&mut self, item: Option<NewQueueItem>) {
		// 前の曲の解決のやり直しの待ちは捨てる
		let generation = self.advance_generation(RetryWait::Discard);
		self.played_in_session |= item.is_some();
		self.current = item.map(|item| CurrentTrack::new(item, generation));
	}

	/// 曲の世代を 1 つ進める。世代を進める処理はすべてここを通す。
	///
	/// - 古い世代の解決結果、前の再生の終了・失敗のイベントは、以後の照合で捨てられる
	/// - `rate_limited` の解決のやり直しの待ち (`retry_at`) は `retry` に従って捨てるか、新しい世代へ移す
	/// - 今の曲の世代は呼び出し側が書き換える (`CurrentTrack::new` か `CurrentTrack::restart`)
	///
	/// # 引数
	/// - `retry`: 解決のやり直しの待ちの扱い
	///
	/// # 戻り値
	/// 新しい世代
	///
	/// # 呼び出し例
	/// ```ignore
	/// let generation = self.advance_generation(RetryWait::Discard);
	/// ```
	fn advance_generation(&mut self, retry: RetryWait) -> u64 {
		self.generation += 1;
		let generation = self.generation;
		self.sup.retry_at = match retry {
			RetryWait::Discard => None,
			RetryWait::Carry => self.sup.retry_at.map(|(at, _)| (at, generation)),
		};
		generation
	}

	/// 待機の終了の時刻を更新する。
	///
	/// - Idle でなければ消す。Idle で、メッセージを処理した後か未設定なら、今から数え直す
	///
	/// # 引数
	/// - `activity`: メッセージを処理した直後なら true
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.refresh_idle(true);
	/// ```
	fn refresh_idle(&mut self, activity: bool) {
		if self.state != PlayerState::Idle {
			self.idle = None;
		} else if activity || self.idle.is_none() {
			let timeout = Duration::from_secs(self.deps.config.limits.actor_idle_timeout_secs);
			self.idle = Some(Instant::now() + timeout);
		}
	}

	/// ギルドの設定を返す。セッションの間は読み込んだものを使い回す。
	///
	/// - 読めなければ警告して既定値を返す (保持せず、次の呼び出しで読み直す)
	///
	/// # 戻り値
	/// 既定値を解決した設定
	///
	/// # 呼び出し例
	/// ```ignore
	/// let volume = self.settings().await.volume;
	/// ```
	async fn settings(&mut self) -> GuildSettings {
		if let Some(settings) = &self.settings {
			return settings.clone();
		}
		let guild = self.guild;
		let defaults = self.deps.config.defaults.clone();
		match self
			.deps
			.db
			.reader
			.read(move |c| settings::load(c, guild, &defaults))
			.await
		{
			Ok(loaded) => {
				self.settings = Some(loaded.clone());
				loaded
			}
			Err(error) => {
				tracing::warn!(guild = guild.get(), %error, "ギルドの設定を読めません。既定値を使います");
				default_settings(&self.deps.config.defaults)
			}
		}
	}

	/// 書き込みスレッドで処理を実行する。失敗したら 1 回だけやり直す (`store::write_with_retry`)。
	///
	/// # 引数
	/// - `f`: 接続を受け取る処理。やり直しのため Fn を求める (取り込んだ値は複製しない)
	///
	/// # 戻り値
	/// `f` の戻り値
	///
	/// # エラー
	/// 2 回とも失敗したときの 2 回目のエラー
	///
	/// # 呼び出し例
	/// ```ignore
	/// let item = self.write(move |c| store::pop_next(c, guild, channels)).await?;
	/// ```
	fn write<T, F>(&self, f: F) -> impl Future<Output = Result<T, DbError>> + Send + 'static
	where
		T: Send + 'static,
		F: Fn(&Connection) -> Result<T, DbError> + Send + Sync + 'static,
	{
		// アクターを借りたまま待たない (切断などと並行して待てるように、書き込みの口を複製して渡す)
		let writer = self.deps.db.writer.clone();
		let guild = self.guild;
		async move { store::write_with_retry(&writer, guild, f).await }
	}

	/// 通知を通知のタスクへ渡す。待たない。
	///
	/// # 引数
	/// - `channel`: 送り先のテキストチャンネル
	/// - `notice`: 通知
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.notify(channel, plain(keys::SLOW));
	/// ```
	fn notify(&self, channel: ChannelId, notice: Notice) {
		if self.notices.send((channel, notice)).is_err() {
			tracing::warn!(
				guild = self.guild.get(),
				"通知のタスクが終了しているため通知を捨てます"
			);
		}
	}

	/// 音声セッションを呼び出す。`voice.call_timeout_secs` で打ち切る。
	///
	/// - 未接続なら呼ばずに `NotConnected`
	/// - 時間切れならセッションが壊れたとみなして記録する。ループの先頭で回復を始める。
	///   回復を始めるまでの呼び出しは、待たずに `Timeout` を返す
	/// - 成功したら、応答の無いセッションとしての記録 (`Supervision::broken`) を消す
	///
	/// # 引数
	/// - `call`: セッションを受け取り、呼び出しの future を返す処理
	///
	/// # 戻り値
	/// 呼び出しの結果
	///
	/// # エラー
	/// 呼び出しのエラー、`NotConnected`、`Timeout`
	///
	/// # 呼び出し例
	/// ```ignore
	/// let track = self.call_voice(move |voice| voice.play(request)).await?;
	/// ```
	async fn call_voice<T>(
		&mut self,
		call: impl for<'a> FnOnce(&'a mut dyn VoiceSession) -> VoiceCall<'a, T>,
	) -> Result<T, AudioError> {
		let limit = self.call_timeout();
		self.call_voice_within(limit, call).await
	}

	/// 音声セッションを、`limit` で打ち切って呼び出す。それ以外は `call_voice` と同じ。
	///
	/// - バックエンドの内側の上限が `voice.call_timeout_secs` より長い呼び出し (`move_to`) に使う
	///
	/// # 引数
	/// - `limit`: 上限時間
	/// - `call`: セッションを受け取り、呼び出しの future を返す処理
	///
	/// # 戻り値
	/// 呼び出しの結果
	///
	/// # エラー
	/// 呼び出しのエラー、`NotConnected`、`Timeout`
	///
	/// # 呼び出し例
	/// ```ignore
	/// let limit = self.voice_limits().move_to();
	/// self.call_voice_within(limit, |voice| voice.move_to(target)).await?;
	/// ```
	async fn call_voice_within<T>(
		&mut self,
		limit: Duration,
		call: impl for<'a> FnOnce(&'a mut dyn VoiceSession) -> VoiceCall<'a, T>,
	) -> Result<T, AudioError> {
		if self.sup.timed_out {
			return Err(AudioError::Timeout);
		}
		let Some(voice) = self.voice.as_deref_mut() else {
			return Err(AudioError::NotConnected);
		};
		let result = with_timeout(limit, call(voice)).await;
		match &result {
			Ok(_) => self.sup.broken = false,
			Err(AudioError::Timeout) => {
				tracing::warn!(
					guild = self.guild.get(),
					"音声の呼び出しが時間切れになりました。切断として扱います"
				);
				self.sup.timed_out = true;
			}
			Err(_) => {}
		}
		result
	}

	/// 音声セッションの呼び出しの上限時間 (`voice.call_timeout_secs`)。
	///
	/// # 戻り値
	/// 上限時間
	///
	/// # 呼び出し例
	/// ```ignore
	/// with_timeout(self.call_timeout(), voice.pause()).await?;
	/// ```
	fn call_timeout(&self) -> Duration {
		self.voice_limits().call()
	}

	/// 音声の呼び出しの上限時間の組 (`[voice]` から導く)。
	///
	/// # 戻り値
	/// 上限時間の組
	///
	/// # 呼び出し例
	/// ```ignore
	/// let limit = self.voice_limits().connect();
	/// ```
	fn voice_limits(&self) -> VoiceLimits {
		VoiceLimits::new(&self.deps.config.voice)
	}

	/// 準備が遅いと知らせるまでの時間 (`playback.slow_notice_secs`)。
	///
	/// # 戻り値
	/// しきい値
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.slow = Some(Instant::now() + self.slow_threshold());
	/// ```
	fn slow_threshold(&self) -> Duration {
		Duration::from_secs(self.deps.config.playback.slow_notice_secs)
	}
}

/// `VoiceSession` のメソッドが返す future (async_trait の形)。
type VoiceCall<'a, T> = Pin<Box<dyn Future<Output = Result<T, AudioError>> + Send + 'a>>;

/// 音声の呼び出しを上限時間で打ち切る。
///
/// # 引数
/// - `limit`: 上限時間
/// - `call`: 呼び出し
///
/// # 戻り値
/// 呼び出しの結果
///
/// # エラー
/// 呼び出しのエラー。時間切れなら `AudioError::Timeout`
///
/// # 呼び出し例
/// ```ignore
/// let track = with_timeout(limit, voice.play(request)).await?;
/// ```
async fn with_timeout<T>(
	limit: Duration,
	call: impl Future<Output = Result<T, AudioError>>,
) -> Result<T, AudioError> {
	tokio::time::timeout(limit, call)
		.await
		.unwrap_or(Err(AudioError::Timeout))
}

/// 音声セッションを上限時間で打ち切って切断する。失敗とタイムアウトはログに残すだけ。
///
/// - アクターの外 (別タスク) でも使えるよう、アクターを借りない
///
/// # 引数
/// - `guild`: ログに出すギルド
/// - `limit`: 上限時間 (`VoiceLimits::disconnect`)
/// - `session`: 切断するセッション
///
/// # 呼び出し例
/// ```ignore
/// tokio::spawn(disconnect_within(guild, limit, session));
/// ```
async fn disconnect_within(guild: GuildId, limit: Duration, session: Box<dyn VoiceSession>) {
	if let Err(error) = with_timeout(limit, session.disconnect()).await {
		tracing::warn!(guild = guild.get(), %error, "切断に失敗しました");
	}
}

/// 時刻まで待つ。None なら永遠に待つ (`select!` の枝を止めるため)。
///
/// # 引数
/// - `deadline`: 待つ時刻
///
/// # 呼び出し例
/// ```ignore
/// tokio::select! { () = wait_deadline(self.slow) => ..., }
/// ```
async fn wait_deadline(deadline: Option<Instant>) {
	match deadline {
		Some(at) => tokio::time::sleep_until(at).await,
		None => std::future::pending().await,
	}
}

/// 再生の要求を作る。ライブ配信と https 以外の配信、`force_pipe` の指定は PipeInput で再生する。
///
/// # 引数
/// - `stream`: 解決済みのストリーム
/// - `volume`: 音量 (1.0 が等倍)
/// - `start_at`: 再生を始める位置
/// - `force_pipe`: 配信の種類によらず PipeInput で再生するか (曲の失敗の 2 回目)
/// - `paused`: 一時停止のまま載せるか (一時停止中の曲の作り直し、一時停止の状態での復元)
///
/// # 戻り値
/// 再生の要求
///
/// # 呼び出し例
/// ```ignore
/// voice.play(play_request(stream, settings.volume, Duration::ZERO, false, false)).await?;
/// ```
fn play_request(
	stream: ResolvedStream,
	volume: f32,
	start_at: Duration,
	force_pipe: bool,
	paused: bool,
) -> PlayRequest {
	let use_pipe = force_pipe || stream.is_live || stream.protocol != CHUNKED_PROTOCOL;
	PlayRequest {
		video_id: stream.video_id.clone(),
		stream: Some(stream),
		start_at,
		volume,
		use_pipe,
		paused,
	}
}

/// 設定を読めないときの既定値。ループは無効。
///
/// # 引数
/// - `defaults`: `[defaults]` の設定
///
/// # 戻り値
/// 既定値の設定
///
/// # 呼び出し例
/// ```ignore
/// let settings = default_settings(&config.defaults);
/// ```
fn default_settings(defaults: &DefaultsConfig) -> GuildSettings {
	GuildSettings {
		volume: defaults.volume,
		queue_limit: defaults.queue_limit,
		playlist_limit: defaults.playlist_limit,
		alone_timeout_secs: defaults.alone_timeout_secs,
		loop_enabled: false,
	}
}
