//! Songbird による `AudioBackend` の実装。
//!
//! - 接続は `Songbird::join` で作り、`VoiceSession` の操作は `Call` と `TrackHandle` へ委ねる
//! - 入力は `InputFactory` が作る。通常は `ChunkedRequest`、`use_pipe` なら `PipeInput`
//! - Songbird の呼び出しはすべて `tokio::time::timeout` で包み、超えたら `AudioError::Timeout` を返す
//!
//! イベントの契約 (`AudioEvent` の doc) の守り方:
//! - `TrackId` はバックエンド全体の `AtomicU64` から払い出す。接続をまたいでも重複しない
//! - セッションごとに「開いている」印 (`EventSink`) を持ち、ハンドラは印を見てから送る。
//!   `disconnect` と破棄は最初に印を閉じるため、自分の切断の `Disconnected` も、古いセッションの曲の
//!   イベントもアクターへ届かない
//! - 曲ごとに `TrackGate` を持ち、開始は最初の 1 回、終わり (`TrackEnded` / `TrackFailed`) も 1 回だけ送る
//! - 送信はその場の `try_send` で行い、メールボックスが満杯のときはセッションごとの待ち行列に積んで
//!   `tokio::spawn` した先で上限時間付きで送る。待ち行列がある間の後続も積むため、届く順序は送った順のまま。
//!   届かなければ種類と曲をログに残す
//! - 曲を止める・捨てるときは、入力の取得を取り消す (`ChunkedRequest` は `ChunkCancel`、PipeInput は
//!   `PipeCancel` で子のツリーを終わらせる)。ミキサーは入力を同期で読むため、取得の待ち (再試行、
//!   `available_at`、取り直し、yt-dlp の詰まり) に入ったままだと止める命令も処理できない
//! - 入力の読み取りを `ReadActivity` で見張る。ミキサーが取得の待ちで止まっている間の位置の問い合わせは、
//!   時間切れ (切断として回復する) にせず `Failed` で返す (呼び出し元は経過時間からの見積もりを使う)
//! - PipeInput の起動 (子の起動、Job への登録) は同期の処理のため `spawn_blocking` で行う
//!
//! 注意:
//! - Songbird の `TrackEvent::Play` は曲の最初の開始では発火しない (一時停止からの再開だけ)。
//!   そのため `TrackEvent::Playable` を受けたときに再生中の状態なら、それを開始として扱う
//! - 接続の無いミキサーは曲を読み進めず、`TrackEvent` も発火しない。一方で `get_info` の位置は
//!   20ms ごとの Tick で進み続ける (発見 K)。`position` は切断中に実際の再生位置より先へ進みうる
//! - `Call::leave` はドライバーのトラックを止めない (発見 J)。退出の前に `Call::stop` を呼ぶ
//!
//! 呼び出し方:
//! ```ignore
//! let backend = SongbirdBackend::new(manager, inputs, config.voice.clone(), send_timeout);
//! let mut session = backend.connect(guild, channel, events_tx).await?;
//! let track = session.play(request).await?;
//! session.disconnect().await?;
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use songbird::events::{CoreEvent, Event, EventContext, EventData, EventHandler, TrackEvent};
use songbird::input::Input;
use songbird::tracks::{ControlError, PlayMode, Track, TrackHandle, TrackState};
use songbird::{Call, Songbird};
use tokio::sync::{mpsc, Mutex, OwnedMutexGuard};
use tokio::time::timeout;

use crate::audio::backend::{
	AudioBackend, AudioError, AudioEvent, DisconnectKind, PlayRequest, TrackId, VoiceHealth,
	VoiceSession,
};
use crate::audio::chunked::{ChunkCancel, ChunkConfig, ChunkFailure, ChunkedRequest, UrlRefresher};
use crate::audio::pipe::{PipeCancel, PipeInput, PipeSpec};
use crate::audio::watch::ReadActivity;
use crate::config::VoiceConfig;
use crate::domain::{ChannelId, GuildId};
use crate::guild::message::GuildMessage;
use crate::platform::job::Job;

mod events;
mod gate;

pub use events::{disconnect_kind, ended_event, is_left_channel, EventSink};
pub use gate::{effective_position, track_start_ready, TrackGate};

/// `watch_url` の中で動画 ID に置き換える部分。
const VIDEO_ID_PLACEHOLDER: &str = "{id}";
/// `TrackId` の払い出しの最初の値。
const FIRST_TRACK_ID: u64 = 1;
/// Songbird が失敗の中身を伝えなかったときの `TrackFailed` の文言。
const UNKNOWN_PLAY_ERROR: &str = "曲の再生でエラーが起きました";
/// 位置の問い合わせで、入力の 1 回の読み取りがこれより長く続いていたら、取得の待ちで止まっているとみなす。
/// 通常の読み取りはバッファから写すだけで一瞬で終わる。設定項目に無いため固定値にする。
const INPUT_STALL_GRACE: Duration = Duration::from_millis(500);

/// 入力の取得の取り消しの口。
#[derive(Clone)]
pub enum InputCancel {
	/// `ChunkedRequest` の取得の取り消し
	Chunk(ChunkCancel),
	/// PipeInput の子のプロセスツリーの終了
	Pipe(PipeCancel),
}

impl InputCancel {
	/// 取得を取り消す。2 回目以降は何もしない。
	///
	/// # 呼び出し例
	/// `built.cancel.cancel();`
	pub fn cancel(&self) {
		match self {
			Self::Chunk(cancel) => cancel.cancel(),
			Self::Pipe(cancel) => cancel.cancel(),
		}
	}

	/// 取り消したか。
	///
	/// # 呼び出し例
	/// `assert!(built.cancel.is_cancelled());`
	pub fn is_cancelled(&self) -> bool {
		match self {
			Self::Chunk(cancel) => cancel.is_cancelled(),
			Self::Pipe(cancel) => cancel.is_cancelled(),
		}
	}
}

/// `InputFactory::build` が作った入力と、その付属物。
pub struct BuiltInput {
	/// Songbird の入力
	pub input: Input,
	/// 入力の失敗の記録。曲の終わりに見て `TrackEnded` か `TrackFailed` かを決める
	pub failure: ChunkFailure,
	/// 取得の取り消しの口
	pub cancel: InputCancel,
	/// ミキサーの読み取りの見張り
	pub activity: ReadActivity,
}

/// 再生の入力を作る。`SongbirdBackend` が全セッションで共有する。
///
/// 呼び出し方:
/// ```ignore
/// let built = inputs.build(&request)?;
/// ```
pub struct InputFactory {
	pub client: reqwest::Client,
	pub refresher: Arc<dyn UrlRefresher>,
	pub chunk: ChunkConfig,
	/// PipeInput の起動方法。更新で環境が替わると入れ替わるため、`build` のたびに読む
	pub pipe: Arc<ArcSwap<PipeSpec>>,
	pub job: Arc<Job>,
	/// PipeInput に渡す URL の雛形 (ui.watch_url)。`{id}` を動画 ID に置き換える
	pub watch_url: String,
}

impl InputFactory {
	/// 要求から Songbird の入力を作る。
	///
	/// - `use_pipe` なら `watch_url` の `{id}` を動画 ID にした URL で `PipeInput` を起動する。
	///   子の起動を伴う同期の処理のため、非同期の文脈からは `spawn_blocking` の中で呼ぶ
	/// - そうでなければ `request.stream` から `ChunkedRequest` を作る。通信は最初の読み取りまで行わない
	///
	/// # 引数
	/// - `request`: 再生の要求
	///
	/// # 戻り値
	/// 入力、失敗の記録 (曲の終わりに見る)、取り消しの口、読み取りの見張り
	///
	/// # エラー
	/// `use_pipe` でなく `stream` が無い場合、`PipeInput` の起動に失敗した場合は `AudioError::Failed`
	///
	/// # 例
	/// `let built = inputs.build(&request)?;`
	pub fn build(&self, request: &PlayRequest) -> Result<BuiltInput, AudioError> {
		if request.use_pipe {
			let url = self
				.watch_url
				.replace(VIDEO_ID_PLACEHOLDER, &request.video_id.0);
			let pipe = PipeInput::spawn(&self.pipe.load(), &url, &self.job)?;
			let failure = pipe.failure();
			let cancel = InputCancel::Pipe(pipe.cancel_handle());
			let activity = pipe.read_activity();
			return Ok(BuiltInput {
				input: pipe.into_input(),
				failure,
				cancel,
				activity,
			});
		}
		let Some(stream) = request.stream.clone() else {
			return Err(AudioError::Failed(format!(
				"解決結果が無いため再生できません: {}",
				request.video_id.0
			)));
		};
		let chunked = ChunkedRequest::new(
			self.client.clone(),
			stream,
			Arc::clone(&self.refresher),
			self.chunk.clone(),
		);
		let failure = chunked.failure();
		let cancel = InputCancel::Chunk(chunked.cancel_handle());
		let activity = chunked.read_activity();
		Ok(BuiltInput {
			input: Input::from(chunked),
			failure,
			cancel,
			activity,
		})
	}
}

/// 曲のイベントのうち、ハンドラが扱うもの。
#[derive(Debug, Clone, Copy)]
enum TrackHook {
	/// 再生を始めた (一時停止からの再開、準備済みの曲の開始)。準備の前にも届く。
	Play,
	/// 入力の準備ができた (最初の開始と、準備の前に `Play` を受けた曲の開始はこれで知る)。
	Playable,
	/// 終わった (止めた、最後まで読んだ、失敗した)。
	End,
	/// 失敗した。
	Error,
}

impl TrackHook {
	/// 登録する Songbird の曲のイベント。
	fn event(self) -> TrackEvent {
		match self {
			Self::Play => TrackEvent::Play,
			Self::Playable => TrackEvent::Playable,
			Self::End => TrackEvent::End,
			Self::Error => TrackEvent::Error,
		}
	}
}

/// 曲のイベントのハンドラ。`TrackGate` を通ったものだけを `EventSink` へ渡す。
struct TrackHandler {
	hook: TrackHook,
	gate: Arc<TrackGate>,
	sink: EventSink,
}

#[async_trait]
impl EventHandler for TrackHandler {
	/// イベントを `AudioEvent` に変えて送る。ハンドラは登録したまま残す (None を返す)。
	async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
		let state: Option<&TrackState> = match ctx {
			EventContext::Track(tracks) => tracks.first().map(|(state, _)| *state),
			_ => None,
		};
		let event = match self.hook {
			// どちらも、再生中で準備ができているときだけ開始とする (2 回目以降は門が捨てる)
			TrackHook::Play | TrackHook::Playable => {
				if track_start_ready(state) {
					self.gate.start()
				} else {
					None
				}
			}
			TrackHook::End | TrackHook::Error => {
				if let Some(state) = state {
					self.gate.record_position(state.position);
				}
				let error = state.and_then(|s| match &s.playing {
					PlayMode::Errored(error) => Some(error.to_string()),
					_ => None,
				});
				match (self.hook, error) {
					(TrackHook::Error, None) => self.gate.finish(Some(UNKNOWN_PLAY_ERROR)),
					(_, error) => self.gate.finish(error.as_deref()),
				}
			}
		};
		if let Some(event) = event {
			self.sink.emit(event);
		}
		None
	}
}

/// 接続のイベント (接続、切断、自動の再接続) のハンドラ。
struct CoreHandler {
	sink: EventSink,
}

#[async_trait]
impl EventHandler for CoreHandler {
	/// `DriverDisconnect` を `Disconnected`、`DriverReconnect` を `Reconnected` にして送る。
	///
	/// - `DriverConnect` (自動の再接続でない新しい接続) は接続の状態を戻すだけで、イベントは送らない
	async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
		match ctx {
			EventContext::DriverConnect(_) => {
				self.sink.set_link_up(true);
			}
			EventContext::DriverDisconnect(data) => {
				let reason = disconnect_kind(data.reason);
				let channel = data.channel_id.0.get();
				let left = is_left_channel(self.sink.voice_channel(), channel);
				tracing::info!(
					guild = self.sink.guild().get(),
					channel,
					songbird_reason = ?data.reason,
					songbird_kind = ?data.kind,
					kind = ?reason,
					left,
					"ボイスの接続が切れました"
				);
				// 前のチャンネルの終わりでも、新しい接続ができるまではつながっていない
				self.sink.set_link_up(false);
				if left {
					// 移動の前のチャンネルの接続の終わり。新しい接続は Songbird が続けて作る (`DriverConnect`)
					tracing::info!(
						guild = self.sink.guild().get(),
						channel,
						"移動の前のチャンネルの接続の終わりのため、切断として扱いません"
					);
					self.sink.hold_disconnect();
				} else {
					self.sink.emit(AudioEvent::Disconnected { reason });
				}
			}
			EventContext::DriverReconnect(_) => {
				self.sink.set_link_up(true);
				self.sink.emit(AudioEvent::Reconnected);
			}
			_ => {}
		}
		None
	}
}

/// 上限時間で打ち切る。時間切れは `AudioError::Timeout`。
///
/// # 例
/// `let info = bounded(limit, async { ... }).await?;`
async fn bounded<T>(
	limit: Duration,
	call: impl Future<Output = Result<T, AudioError>>,
) -> Result<T, AudioError> {
	timeout(limit, call)
		.await
		.unwrap_or(Err(AudioError::Timeout))
}

/// 曲の操作のエラーを変える。
///
/// - 曲がもう終わっている (`Finished`) なら成功として扱う (終わりはイベントでアクターへ届く)
///
/// # 例
/// `ignore_finished(handle.pause())?;`
fn ignore_finished(result: Result<(), ControlError>) -> Result<(), AudioError> {
	match result {
		Ok(()) | Err(ControlError::Finished) => Ok(()),
		Err(error) => Err(AudioError::Failed(error.to_string())),
	}
}

/// 曲の操作のエラーを `AudioError::Failed` に変える。
///
/// # 例
/// `let position = handle.seek_async(at).await.map_err(control_error)?;`
fn control_error(error: ControlError) -> AudioError {
	AudioError::Failed(error.to_string())
}

/// 再生中または準備中の 1 曲。
struct Slot {
	track: TrackId,
	handle: TrackHandle,
	gate: Arc<TrackGate>,
	seekable: bool,
	/// 取得の取り消しの口
	cancel: InputCancel,
	/// ミキサーの読み取りの見張り
	activity: ReadActivity,
}

impl Slot {
	/// 曲を止める。終わりのイベントは送る (`TrackEnded`)。
	///
	/// - 止める命令を送ってから取得を取り消す。ミキサーが取得の待ちで止まっていても、
	///   取り消しで読み取りが終わり、命令を処理できるようになる
	/// - 取り消しで読めなくなったことを Songbird が失敗と報告しても、門が `TrackEnded` にする
	fn stop(&self) {
		self.gate.mark_stopped();
		if let Err(error) = ignore_finished(self.handle.stop()) {
			tracing::debug!(track = self.track.0, %error, "曲を止められません");
		}
		self.cancel.cancel();
	}

	/// イベントを送らずに曲を止めて捨てる。
	fn discard(self) {
		self.gate.silence();
		self.stop();
	}

	/// 曲への指示を頼めなかったとき、イベントを送らずに曲を捨て、呼び出し元へ返す失敗を作る。
	///
	/// # 引数
	/// - `what`: 頼めなかった指示の説明 (例: "シークを頼めません")
	///
	/// # 戻り値
	/// 曲の番号を添えた `AudioError::Failed`
	///
	/// # 呼び出し例
	/// ```ignore
	/// return Err(slot.discard_failed("シークを頼めません"));
	/// ```
	fn discard_failed(self, what: &str) -> AudioError {
		let track = self.track;
		self.discard();
		AudioError::Failed(format!("{what} (曲 {})", track.0))
	}
}

/// Songbird の 1 つの接続のセッション。
struct SongbirdSession {
	guild: GuildId,
	call: Arc<Mutex<Call>>,
	manager: Option<Arc<Songbird>>,
	inputs: Arc<InputFactory>,
	voice: VoiceConfig,
	track_ids: Arc<AtomicU64>,
	sink: EventSink,
	current: Option<Slot>,
	preloaded: Option<Slot>,
	last_health: std::sync::Mutex<VoiceHealth>,
	/// Call の持ち主の記録と、このセッションの通し番号。None は持ち主の確認をしない (試験の Call)
	owner: Option<(Arc<CallOwners>, u64)>,
}

/// セッションを作る。試験と `SongbirdBackend::connect` が使う。
///
/// - 接続のイベント (切断、再接続) は登録しない。登録は `connect` が行う
/// - Call の持ち主の確認をしない (常に持ち主として退出する)
///
/// # 引数
/// - `guild`: サーバー
/// - `call`: 接続済みの Call、または `Call::standalone`
/// - `manager`: 退出と移動に使う。None なら退出で `remove` せず、`move_to` は `NotConnected`
/// - `inputs`: 入力を作る口
/// - `voice`: タイムアウトの設定
/// - `track_ids`: `TrackId` の払い出し。バックエンド全体で共有する
/// - `events`: アクターのメールボックス
/// - `event_send_timeout`: イベント 1 件の送信を待つ上限
///
/// # 戻り値
/// 音声セッション
///
/// # 例
/// `let session = session_from_call(guild, call, None, inputs, voice, ids, tx, Duration::from_secs(5));`
#[allow(clippy::too_many_arguments)]
pub fn session_from_call(
	guild: GuildId,
	call: Arc<Mutex<Call>>,
	manager: Option<Arc<Songbird>>,
	inputs: Arc<InputFactory>,
	voice: VoiceConfig,
	track_ids: Arc<AtomicU64>,
	events: mpsc::Sender<GuildMessage>,
	event_send_timeout: Duration,
) -> Box<dyn VoiceSession> {
	Box::new(SongbirdSession::new(
		guild,
		call,
		manager,
		inputs,
		voice,
		track_ids,
		EventSink::new(guild, events, event_send_timeout),
		None,
	))
}

impl SongbirdSession {
	/// セッションを組み立てる。
	#[allow(clippy::too_many_arguments)]
	fn new(
		guild: GuildId,
		call: Arc<Mutex<Call>>,
		manager: Option<Arc<Songbird>>,
		inputs: Arc<InputFactory>,
		voice: VoiceConfig,
		track_ids: Arc<AtomicU64>,
		sink: EventSink,
		owner: Option<(Arc<CallOwners>, u64)>,
	) -> Self {
		Self {
			guild,
			call,
			manager,
			inputs,
			voice,
			track_ids,
			sink,
			current: None,
			preloaded: None,
			last_health: std::sync::Mutex::new(VoiceHealth::default()),
			owner,
		}
	}

	/// Songbird の 1 回の呼び出しの上限 (`voice.call_timeout_secs`)。
	fn call_timeout(&self) -> Duration {
		Duration::from_secs(self.voice.call_timeout_secs)
	}

	/// 接続の上限 (`voice.join_timeout_secs`)。
	fn join_timeout(&self) -> Duration {
		Duration::from_secs(self.voice.join_timeout_secs)
	}

	/// 入力を作る。PipeInput は tokio のワーカーを塞がないよう `spawn_blocking` の中で起動する。
	///
	/// - PipeInput の起動の上限は `call_timeout`。超えたら待たずに失敗を返す。
	///   起動が後から終わった入力は捨てられ、子は kill される
	///
	/// # 引数
	/// - `request`: 再生の要求
	///
	/// # 戻り値
	/// 作った入力
	///
	/// # エラー
	/// 入力の作成の失敗、PipeInput の起動の時間切れ・異常終了は `Failed`
	///
	/// # 例
	/// `let built = self.build_input(&request).await?;`
	async fn build_input(&self, request: &PlayRequest) -> Result<BuiltInput, AudioError> {
		if !request.use_pipe {
			return self.inputs.build(request);
		}
		let inputs = Arc::clone(&self.inputs);
		let owned = request.clone();
		let task = tokio::task::spawn_blocking(move || inputs.build(&owned));
		match timeout(self.call_timeout(), task).await {
			Ok(Ok(built)) => built,
			Ok(Err(error)) => Err(AudioError::Failed(format!(
				"yt-dlp の起動が異常終了しました: {error}"
			))),
			Err(_) => Err(AudioError::Failed(
				"yt-dlp の起動が時間内に終わりません".to_owned(),
			)),
		}
	}

	/// 曲を作り、イベントのハンドラを付けて Call に載せる。
	///
	/// - ハンドラは Call に載せる前に付ける (載せた直後のイベントを取りこぼさない)
	/// - `paused` なら一時停止のまま載せる
	///
	/// # エラー
	/// 入力の作成の失敗は `Failed`、Call のロックの時間切れは `Timeout`
	async fn load(&self, request: &PlayRequest, paused: bool) -> Result<Slot, AudioError> {
		let BuiltInput {
			input,
			failure,
			cancel,
			activity,
		} = self.build_input(request).await?;
		let track = TrackId(self.track_ids.fetch_add(1, Ordering::SeqCst));
		let gate = Arc::new(TrackGate::new(track, Some(failure)));
		let mut songbird_track = Track::new(input).volume(request.volume);
		if paused {
			songbird_track = songbird_track.pause();
		}
		for hook in [
			TrackHook::Play,
			TrackHook::Playable,
			TrackHook::End,
			TrackHook::Error,
		] {
			let handler = TrackHandler {
				hook,
				gate: Arc::clone(&gate),
				sink: self.sink.clone(),
			};
			songbird_track.events.add_event(
				EventData::new(Event::Track(hook.event()), handler),
				Duration::ZERO,
			);
		}
		let call = Arc::clone(&self.call);
		let played = bounded(self.call_timeout(), async move {
			Ok(call.lock().await.play(songbird_track))
		})
		.await;
		let handle = match played {
			Ok(handle) => handle,
			Err(error) => {
				// 載せられなかった入力の取得を残さない
				cancel.cancel();
				return Err(error);
			}
		};
		Ok(Slot {
			track,
			handle,
			gate,
			seekable: !request.use_pipe,
			cancel,
			activity,
		})
	}

	/// Call をロックして処理する。上限は `call_timeout`。
	async fn with_call<T: Send>(
		&self,
		action: impl FnOnce(&mut Call) -> T + Send,
	) -> Result<T, AudioError> {
		let call = Arc::clone(&self.call);
		bounded(self.call_timeout(), async move {
			Ok(action(&mut *call.lock().await))
		})
		.await
	}

	/// 今の健全性を読む。Call がロック中なら前の値を返す。
	fn read_health(&self) -> VoiceHealth {
		let mut last = self
			.last_health
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		if let Ok(call) = self.call.try_lock() {
			let stats = call.dave_tx_stats();
			*last = VoiceHealth {
				dave_version: Some(stats.protocol_version),
				encrypted_frames: stats.encrypted_frames,
				plaintext_frames: stats.plaintext_frames,
			};
		}
		last.clone()
	}
}

#[async_trait]
impl VoiceSession for SongbirdSession {
	/// 今の曲を止め、新しい曲を再生する。
	///
	/// - `start_at` が 0 より後なら、シークを頼んで待たずに返す。シークの失敗は曲の失敗
	///   (`TrackFailed`) として届く。PipeInput はシークせず、先頭から再生してログに残す
	/// - 待たないのは、シークが入力の作成と取得 (`available_at` までの待ちを含む) を伴い、
	///   呼び出しの上限を超えると切断として扱われるため
	/// - シークが終わるまで `position` は `start_at` より前を返さない (位置の保存と回復が 0 に戻らないように)
	/// - `paused` なら一時停止のまま載せる。シークが無ければ入力の準備 (`make_playable`) を頼み、再開を速くする
	///   (シークを頼んだときは、シークが入力を作る)
	async fn play(&mut self, request: PlayRequest) -> Result<TrackId, AudioError> {
		if let Some(old) = self.current.take() {
			old.stop();
		}
		let slot = self.load(&request, request.paused).await?;
		let seeking = !request.start_at.is_zero() && slot.seekable;
		if request.paused && !seeking && slot.handle.make_playable().is_hung_up() {
			return Err(slot.discard_failed("曲の準備を頼めません"));
		}
		if !request.start_at.is_zero() {
			if slot.seekable {
				slot.gate.set_pending_start(request.start_at);
				if slot.handle.seek(request.start_at).is_hung_up() {
					return Err(slot.discard_failed("シークを頼めません"));
				}
			} else {
				tracing::info!(
					guild = self.guild.get(),
					track = slot.track.0,
					start_at = ?request.start_at,
					"PipeInput はシークできないため先頭から再生します"
				);
			}
		}
		let track = slot.track;
		self.current = Some(slot);
		Ok(track)
	}

	/// 次の曲を一時停止のまま載せ、入力の準備を頼む。準備した曲は 1 つだけ持つ。
	///
	/// - 前の準備はイベントを送らずに捨てる
	/// - 準備の完了は待たない (取得の待ちで呼び出しの上限を超えないように)。
	///   準備の失敗は `start_preloaded` の失敗、または曲の失敗として届く
	async fn preload(&mut self, request: PlayRequest) -> Result<TrackId, AudioError> {
		if let Some(old) = self.preloaded.take() {
			old.discard();
		}
		let slot = self.load(&request, true).await?;
		if slot.handle.make_playable().is_hung_up() {
			return Err(slot.discard_failed("曲の準備を頼めません"));
		}
		let track = slot.track;
		self.preloaded = Some(slot);
		Ok(track)
	}

	/// 今の曲を止め、準備した曲の再生を始める。
	///
	/// # エラー
	/// 準備した曲が `track` でない、または始められない (準備に失敗して終わっていた) なら `Failed`
	async fn start_preloaded(&mut self, track: TrackId) -> Result<(), AudioError> {
		let slot = match self.preloaded.take() {
			Some(slot) if slot.track == track => slot,
			other => {
				self.preloaded = other;
				return Err(AudioError::Failed(format!(
					"準備していない曲です (曲 {})",
					track.0
				)));
			}
		};
		if let Some(old) = self.current.take() {
			old.stop();
		}
		if let Err(error) = slot.handle.play() {
			slot.discard();
			return Err(control_error(error));
		}
		self.current = Some(slot);
		Ok(())
	}

	/// 準備した曲が `track` なら、イベントを送らずに止めて捨てる。知らなければ何もしない。
	async fn drop_preloaded(&mut self, track: TrackId) -> Result<(), AudioError> {
		match self.preloaded.take() {
			Some(slot) if slot.track == track => slot.discard(),
			other => self.preloaded = other,
		}
		Ok(())
	}

	/// 今の曲を止める (`TrackEnded` が届く)。準備した曲は残す。
	async fn stop(&mut self) -> Result<(), AudioError> {
		if let Some(slot) = self.current.take() {
			slot.stop();
		}
		Ok(())
	}

	/// 今の曲を一時停止する。曲が無い、または終わっていれば何もしない。
	async fn pause(&mut self) -> Result<(), AudioError> {
		match &self.current {
			Some(slot) => ignore_finished(slot.handle.pause()),
			None => Ok(()),
		}
	}

	/// 今の曲の一時停止を解除する。曲が無い、または終わっていれば何もしない。
	async fn resume(&mut self) -> Result<(), AudioError> {
		match &self.current {
			Some(slot) => ignore_finished(slot.handle.play()),
			None => Ok(()),
		}
	}

	/// 今の曲の位置を移すよう頼み、移る先の位置を返す。シークの完了は待たない。
	///
	/// - 待たないのは、シークが入力の取得 (`available_at` までの待ち、再試行を含む) を伴い、
	///   呼び出しの上限 (`call_timeout`) を超えると切断として扱われるため
	/// - シークの失敗は曲の失敗 (`TrackFailed`) として届く
	/// - シークが終わるまで `position` は移る先より前を返さない (`play` の `start_at` と同じ扱い)
	///
	/// # エラー
	/// 曲が無い、PipeInput の曲、シークを頼めない (曲が終わっている) ときは `Failed`
	async fn seek(&mut self, position: Duration) -> Result<Duration, AudioError> {
		let Some(slot) = &self.current else {
			return Err(AudioError::Failed("再生中の曲がありません".to_owned()));
		};
		if !slot.seekable {
			return Err(AudioError::Failed(
				"PipeInput の曲はシークできません".to_owned(),
			));
		}
		// 利用者のシークの位置が新しい基準になる。終わるまでは移る先の位置で補う
		slot.gate.set_pending_start(position);
		if slot.handle.seek(position).is_hung_up() {
			slot.gate.clear_pending_start();
			return Err(AudioError::Failed(format!(
				"シークを頼めません (曲 {})",
				slot.track.0
			)));
		}
		Ok(position)
	}

	/// 今の曲と準備した曲の音量を変える。終わった曲は無視する。
	///
	/// # エラー
	/// どちらかが失敗したら最初のエラー。片方が失敗しても、もう片方には反映する
	async fn set_volume(&mut self, volume: f32) -> Result<(), AudioError> {
		let mut first_error = None;
		for slot in [&self.current, &self.preloaded].into_iter().flatten() {
			if let Err(error) = ignore_finished(slot.handle.set_volume(volume)) {
				first_error.get_or_insert(error);
			}
		}
		first_error.map_or(Ok(()), Err)
	}

	/// 今の曲の位置 (`get_info().position`)。
	///
	/// - 曲が無ければ 0
	/// - 曲が終わっていれば、終わったときの位置 (失敗の後の再開に使う)
	/// - `play` の `start_at` のシークが終わるまでは、`start_at` より前を返さない (`TrackGate::observe_position`)
	/// - 切断している間も進む (発見 K)。実際に送った位置より先になりうる
	///
	/// # エラー
	/// - 接続が切れている間は待たずに `NotConnected` (Songbird が曲への命令を処理せず、上限まで止まるため)
	/// - ミキサーが入力の取得の待ちで止まっていれば、`INPUT_STALL_GRACE` の後に `Failed`。
	///   止まったミキサーは問い合わせに答えず、時間切れにすると切断として回復が始まるため
	async fn position(&self) -> Result<Duration, AudioError> {
		let Some(slot) = &self.current else {
			return Ok(Duration::ZERO);
		};
		if !self.sink.link_up() {
			return Err(AudioError::NotConnected);
		}
		let handle = slot.handle.clone();
		let gate = Arc::clone(&slot.gate);
		let activity = slot.activity.clone();
		let track = slot.track;
		bounded(self.call_timeout(), async move {
			let info = tokio::select! {
				info = handle.get_info() => info,
				() = activity.wait_stalled(INPUT_STALL_GRACE) => {
					return Err(AudioError::Failed(format!(
						"入力の取得を待っているため位置を得られません (曲 {})",
						track.0
					)));
				}
			};
			match info {
				Ok(info) => Ok(gate.observe_position(info.position)),
				Err(ControlError::Finished) => gate
					.last_position()
					.map(|last| gate.observe_position(last))
					.ok_or_else(|| control_error(ControlError::Finished)),
				Err(error) => Err(control_error(error)),
			}
		})
		.await
	}

	/// 同じ Call のまま別のボイスチャンネルへ移る。上限は `join_timeout`。
	///
	/// - 参加を頼む前に、今のチャンネルを移動先にする。Discord が前のチャンネルの接続を 4022 で閉じても、
	///   `CoreHandler` が前のチャンネルの終わりとして切断を送らない (送ると回復が走り、音が途切れる)
	/// - 参加に失敗したら今のチャンネルを戻し、送らなかった切断があれば `Lost` として送る
	///   (前のチャンネルの接続はもう無いため、アクターの回復に任せる)。時間切れはアクターが切断として扱うため送らない
	///
	/// # 引数
	/// - `channel`: 移動先のボイスチャンネル
	///
	/// # エラー
	/// manager が無ければ `NotConnected`、参加の失敗は `Connect`、時間切れは `Timeout`
	///
	/// # 呼び出し例
	/// ```ignore
	/// session.move_to(target).await?;
	/// ```
	async fn move_to(&mut self, channel: ChannelId) -> Result<(), AudioError> {
		let Some(manager) = self.manager.clone() else {
			return Err(AudioError::NotConnected);
		};
		let guild = self.guild;
		// 前の移動で送らなかった切断の記録は、この移動に持ち込まない
		self.sink.take_held_disconnect();
		let previous = self.sink.voice_channel();
		self.sink.set_voice_channel(channel.get());
		let result = bounded(self.join_timeout(), async move {
			manager
				.join(guild, channel)
				.await
				.map(|_| ())
				.map_err(|error| AudioError::Connect(error.to_string()))
		})
		.await;
		if let Err(error) = &result {
			self.sink.set_voice_channel(previous);
			if self.sink.take_held_disconnect() && !matches!(error, AudioError::Timeout) {
				tracing::warn!(
					guild = guild.get(),
					%error,
					"移動に失敗し、前のチャンネルの接続も終わっているため、切断として知らせます"
				);
				self.sink.emit(AudioEvent::Disconnected {
					reason: DisconnectKind::Lost,
				});
			}
		}
		result
	}

	/// 切断する。この切断ではイベントを送らない。
	///
	/// - 最初に印を閉じ、自分の曲 (今の曲と準備した曲) を止める
	/// - まだ Call の持ち主なら: `Call::stop` (`leave` はトラックを止めないため) → 接続のイベントを外す
	///   → `Songbird::remove`。途中で失敗しても `remove` までは試み、最初のエラーを返す
	/// - 持ち主でなければ (同じサーバーの新しいセッションが接続を始めていれば) Call に触れない。
	///   同じ Call を新しいセッションが使っているため
	/// - 持ち主の確認から `remove` の終わりまで持ち主の記録をロックし、新しい接続はその後に始まる
	async fn disconnect(mut self: Box<Self>) -> Result<(), AudioError> {
		self.sink.close();
		for slot in [self.current.take(), self.preloaded.take()]
			.into_iter()
			.flatten()
		{
			slot.discard();
		}
		let mut owner_guard = match &self.owner {
			Some((owners, serial)) => {
				let guard = owners.lock(self.guild).await;
				if *guard != *serial {
					tracing::info!(
						guild = self.guild.get(),
						"新しいセッションが Call を使っているため、Call に触れずに退出します"
					);
					return Ok(());
				}
				Some(guard)
			}
			None => None,
		};
		let stopped = self
			.with_call(|call| {
				call.stop();
				call.remove_all_global_events();
			})
			.await;
		let removed = match &self.manager {
			Some(manager) => {
				let manager = Arc::clone(manager);
				let guild = self.guild;
				bounded(self.call_timeout(), async move {
					match manager.remove(guild).await {
						Ok(()) | Err(songbird::error::JoinError::NoCall) => Ok(()),
						Err(error) => Err(AudioError::Failed(error.to_string())),
					}
				})
				.await
			}
			None => Ok(()),
		};
		if let Some(guard) = owner_guard.as_mut() {
			**guard = NO_OWNER;
		}
		stopped.and(removed)
	}

	/// DAVE の送信の統計。Call がロック中なら前の値を返す。
	fn health(&self) -> VoiceHealth {
		self.read_health()
	}
}

impl Drop for SongbirdSession {
	/// 破棄されたセッションのイベントを送らず、残った曲を止めて取得を取り消す。
	///
	/// - 通常は `disconnect` が曲を取り出した後のため、何もしない
	fn drop(&mut self) {
		self.sink.close();
		for slot in [self.current.take(), self.preloaded.take()]
			.into_iter()
			.flatten()
		{
			slot.discard();
		}
	}
}

/// 持ち主がいないことを表すセッションの通し番号。
const NO_OWNER: u64 = 0;

/// サーバーごとの Call の持ち主 (セッションの通し番号) の記録。
///
/// - Songbird の Call はサーバーに 1 つで、同じサーバーのセッションはそれを共有する。
///   古いセッションの `disconnect` が新しいセッションの Call を止めたり消したりしないよう、
///   最後に接続を始めたセッションだけを持ち主とする
/// - サーバーごとに非同期のロックを持つ。持ち主の確認と Call の後始末をまとめて行うため
///
/// 呼び出し方:
/// ```ignore
/// let owners = CallOwners::default();
/// let serial = owners.claim(guild).await;
/// assert!(owners.is_owner(guild, serial).await);
/// ```
#[derive(Default)]
pub struct CallOwners {
	last_serial: AtomicU64,
	guilds: std::sync::Mutex<HashMap<GuildId, Arc<Mutex<u64>>>>,
}

impl CallOwners {
	/// サーバーの持ち主の欄を返す。無ければ作る。
	fn entry(&self, guild: GuildId) -> Arc<Mutex<u64>> {
		let mut guilds = self
			.guilds
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		Arc::clone(
			guilds
				.entry(guild)
				.or_insert_with(|| Arc::new(Mutex::new(NO_OWNER))),
		)
	}

	/// サーバーの持ち主の欄をロックする。後始末の間、新しい持ち主を待たせる。
	///
	/// # 例
	/// `let mut guard = owners.lock(guild).await; if *guard == serial { ... }`
	async fn lock(&self, guild: GuildId) -> OwnedMutexGuard<u64> {
		self.entry(guild).lock_owned().await
	}

	/// 新しい通し番号を払い出し、サーバーの持ち主にする。接続を始めるときに呼ぶ。
	///
	/// - 前の持ち主の後始末 (`disconnect` の `remove`) が進行中なら、その終わりを待つ
	///
	/// # 戻り値
	/// このセッションの通し番号 (0 にはならない)
	///
	/// # 例
	/// `let serial = owners.claim(guild).await;`
	pub async fn claim(&self, guild: GuildId) -> u64 {
		let serial = self.last_serial.fetch_add(1, Ordering::SeqCst) + 1;
		*self.lock(guild).await = serial;
		serial
	}

	/// `serial` が今のサーバーの持ち主か。
	///
	/// # 例
	/// `if owners.is_owner(guild, serial).await { ... }`
	pub async fn is_owner(&self, guild: GuildId, serial: u64) -> bool {
		*self.lock(guild).await == serial
	}

	/// `serial` が持ち主なら手放す。持ち主でなければ何もしない。
	///
	/// # 戻り値
	/// 手放したら true
	///
	/// # 例
	/// `owners.release(guild, serial).await;`
	pub async fn release(&self, guild: GuildId, serial: u64) -> bool {
		let mut guard = self.lock(guild).await;
		let owned = *guard == serial;
		if owned {
			*guard = NO_OWNER;
		}
		owned
	}
}

/// Songbird による `AudioBackend`。
///
/// 呼び出し方:
/// ```ignore
/// let backend = SongbirdBackend::new(manager, inputs, config.voice.clone(), Duration::from_secs(5));
/// let session = backend.connect(guild, channel, tx).await?;
/// ```
pub struct SongbirdBackend {
	manager: Arc<Songbird>,
	inputs: Arc<InputFactory>,
	voice: VoiceConfig,
	track_ids: Arc<AtomicU64>,
	event_send_timeout: Duration,
	owners: Arc<CallOwners>,
}

impl SongbirdBackend {
	/// バックエンドを作る。
	///
	/// # 引数
	/// - `manager`: Gateway のイベントを受けている Songbird
	/// - `inputs`: 入力を作る口
	/// - `voice`: タイムアウトの設定
	/// - `event_send_timeout`: イベント 1 件の送信を待つ上限
	///
	/// # 例
	/// `let backend = SongbirdBackend::new(manager, inputs, config.voice.clone(), timeout);`
	pub fn new(
		manager: Arc<Songbird>,
		inputs: Arc<InputFactory>,
		voice: VoiceConfig,
		event_send_timeout: Duration,
	) -> Self {
		Self {
			manager,
			inputs,
			voice,
			track_ids: Arc::new(AtomicU64::new(FIRST_TRACK_ID)),
			event_send_timeout,
			owners: Arc::new(CallOwners::default()),
		}
	}

	/// 失敗した接続の Call を消す。失敗はログに残すだけ。
	///
	/// - `serial` がもう持ち主でなければ (新しい接続が始まっていれば) 消さない
	async fn discard_call(&self, guild: GuildId, serial: u64) {
		let mut guard = self.owners.lock(guild).await;
		if *guard != serial {
			return;
		}
		*guard = NO_OWNER;
		let limit = Duration::from_secs(self.voice.call_timeout_secs);
		match timeout(limit, self.manager.remove(guild)).await {
			Ok(Ok(())) | Ok(Err(songbird::error::JoinError::NoCall)) => {}
			Ok(Err(error)) => {
				tracing::warn!(guild = guild.get(), %error, "接続に失敗した Call を消せません")
			}
			Err(_) => tracing::warn!(
				guild = guild.get(),
				"接続に失敗した Call の削除が時間切れです"
			),
		}
	}
}

#[async_trait]
impl AudioBackend for SongbirdBackend {
	/// ボイスチャンネルへ参加し、セッションを作る。
	///
	/// - 参加の前に Call の持ち主になる (`CallOwners::claim`)。前のセッションの `disconnect` は以後 Call に触れない
	/// - 参加の上限は `join_timeout_secs`。失敗したら Call を消す (Songbird は失敗でも Call を残す)
	/// - 参加の後、前の接続の残りのトラックとイベントを消してから、接続・切断・再接続のイベントを登録する
	///
	/// # エラー
	/// 参加の失敗は `Connect`、時間切れは `Timeout`
	async fn connect(
		&self,
		guild: GuildId,
		channel: ChannelId,
		events: mpsc::Sender<GuildMessage>,
	) -> Result<Box<dyn VoiceSession>, AudioError> {
		let serial = self.owners.claim(guild).await;
		let join_limit = Duration::from_secs(self.voice.join_timeout_secs);
		let call = match timeout(join_limit, self.manager.join(guild, channel)).await {
			Ok(Ok(call)) => call,
			Ok(Err(error)) => {
				self.discard_call(guild, serial).await;
				return Err(AudioError::Connect(error.to_string()));
			}
			Err(_) => {
				self.discard_call(guild, serial).await;
				return Err(AudioError::Timeout);
			}
		};
		let sink = EventSink::new(guild, events, self.event_send_timeout);
		sink.set_voice_channel(channel.get());
		let session = SongbirdSession::new(
			guild,
			call,
			Some(Arc::clone(&self.manager)),
			Arc::clone(&self.inputs),
			self.voice.clone(),
			Arc::clone(&self.track_ids),
			sink.clone(),
			Some((Arc::clone(&self.owners), serial)),
		);
		let registered = session
			.with_call(move |call| {
				call.stop();
				call.remove_all_global_events();
				for event in [
					CoreEvent::DriverConnect,
					CoreEvent::DriverDisconnect,
					CoreEvent::DriverReconnect,
				] {
					call.add_global_event(Event::Core(event), CoreHandler { sink: sink.clone() });
				}
			})
			.await;
		if let Err(error) = registered {
			drop(session);
			self.discard_call(guild, serial).await;
			return Err(error);
		}
		Ok(Box::new(session))
	}
}
