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
//! - 曲を止める・捨てるときは、`ChunkedRequest` の取得を取り消す (`ChunkCancel`)。ミキサーは入力を同期で
//!   読むため、取得の待ち (再試行、`available_at`、取り直し) に入ったままだと止める命令も処理できない
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

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use songbird::events::context_data::DisconnectReason;
use songbird::events::{CoreEvent, Event, EventContext, EventData, EventHandler, TrackEvent};
use songbird::input::Input;
use songbird::model::CloseCode as VoiceCloseCode;
use songbird::tracks::{ControlError, PlayMode, ReadyState, Track, TrackHandle, TrackState};
use songbird::{Call, Songbird};
use tokio::sync::{mpsc, Mutex, OwnedMutexGuard};
use tokio::time::timeout;

use crate::audio::backend::{
	AudioBackend, AudioError, AudioEvent, DisconnectKind, PlayRequest, TrackId, VoiceHealth,
	VoiceSession,
};
use crate::audio::chunked::{ChunkCancel, ChunkConfig, ChunkFailure, ChunkedRequest, UrlRefresher};
use crate::audio::pipe::{PipeInput, PipeSpec};
use crate::config::VoiceConfig;
use crate::domain::{ChannelId, GuildId};
use crate::guild::message::GuildMessage;
use crate::platform::job::Job;

/// `watch_url` の中で動画 ID に置き換える部分。
const VIDEO_ID_PLACEHOLDER: &str = "{id}";
/// `TrackId` の払い出しの最初の値。
const FIRST_TRACK_ID: u64 = 1;
/// Songbird が失敗の中身を伝えなかったときの `TrackFailed` の文言。
const UNKNOWN_PLAY_ERROR: &str = "曲の再生でエラーが起きました";

/// `InputFactory::build` が作った入力と、その付属物。
pub struct BuiltInput {
	/// Songbird の入力
	pub input: Input,
	/// 入力の失敗の記録。曲の終わりに見て `TrackEnded` か `TrackFailed` かを決める
	pub failure: ChunkFailure,
	/// 取得の取り消しの口。`ChunkedRequest` のときだけ Some
	pub cancel: Option<ChunkCancel>,
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
	/// 入力、失敗の記録 (曲の終わりに見る)、`ChunkedRequest` のときは取り消しの口
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
			return Ok(BuiltInput {
				input: pipe.into_input(),
				failure,
				cancel: None,
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
		let cancel = chunked.cancel_handle();
		Ok(BuiltInput {
			input: Input::from(chunked),
			failure,
			cancel: Some(cancel),
		})
	}
}

/// 終わった曲のイベントを決める。
///
/// - `ChunkFailure` に失敗が記録されていれば `TrackFailed`、無ければ `TrackEnded`
///
/// # 引数
/// - `track`: 終わった曲
/// - `failure`: 入力 (`ChunkedRequest`、`PipeInput`) の失敗の記録。None は記録が無い入力
///
/// # 戻り値
/// アクターへ送るイベント
///
/// # 例
/// `let event = ended_event(track, failure.as_ref());`
pub fn ended_event(track: TrackId, failure: Option<&ChunkFailure>) -> AudioEvent {
	match failure.and_then(ChunkFailure::get) {
		Some(error) => AudioEvent::TrackFailed {
			track,
			error: AudioError::Failed(error.to_string()),
		},
		None => AudioEvent::TrackEnded { track },
	}
}

/// Songbird の切断の理由を、アクターに伝える切断の種類に変える。
///
/// - Discord に切られた (`WsClosed(Disconnected)`、4014) と、切断の要求 (`Requested`、
///   利用者による移動・追い出しを含む) は `Kicked`
/// - それ以外 (タイムアウト、通信の失敗、理由なし) は `Lost`
///
/// # 引数
/// - `reason`: `DisconnectData::reason`
///
/// # 戻り値
/// 切断の種類
///
/// # 例
/// `let kind = disconnect_kind(data.reason);`
pub fn disconnect_kind(reason: Option<DisconnectReason>) -> DisconnectKind {
	match reason {
		Some(DisconnectReason::WsClosed(Some(VoiceCloseCode::Disconnected)))
		| Some(DisconnectReason::Requested) => DisconnectKind::Kicked,
		_ => DisconnectKind::Lost,
	}
}

/// メールボックスが満杯の間に積んだイベント。`EventSink` の複製で共有する。
#[derive(Default)]
struct Backlog {
	/// 送る順に並べたイベント
	queue: VecDeque<AudioEvent>,
	/// 待ち行列を送るタスクが動いているか。true の間は `emit` もその場で送らずに積む
	draining: bool,
}

/// セッションからアクターへイベントを送る口。「開いている」印を持つ。
///
/// - 閉じた後は何も送らない。閉じるのは `disconnect` とセッションの破棄
/// - その場で送れなければ (メールボックスが満杯)、待ち行列に積み、別タスクが `send_timeout` を上限に
///   1 件ずつ待って送る (捨てない)。待ち行列がある間の後続も積むため、届く順序は `emit` の順のまま
/// - 上限を超えた、またはアクターが終わっていたら、種類と曲をログに残す
///
/// 呼び出し方:
/// ```ignore
/// let sink = EventSink::new(guild, events_tx, send_timeout);
/// sink.emit(AudioEvent::Reconnected);
/// sink.close();
/// ```
#[derive(Clone)]
pub struct EventSink {
	guild: GuildId,
	open: Arc<AtomicBool>,
	/// 音声の接続がつながっているか。`DriverDisconnect` で false、`DriverConnect` と `DriverReconnect` で true にする
	link_up: Arc<AtomicBool>,
	tx: mpsc::Sender<GuildMessage>,
	send_timeout: Duration,
	backlog: Arc<std::sync::Mutex<Backlog>>,
}

impl EventSink {
	/// 開いた状態の送信口を作る。
	///
	/// # 引数
	/// - `guild`: ログに出すサーバー
	/// - `tx`: アクターのメールボックス
	/// - `send_timeout`: 1 件の送信を待つ上限
	///
	/// # 例
	/// `let sink = EventSink::new(guild, tx, Duration::from_secs(5));`
	pub fn new(guild: GuildId, tx: mpsc::Sender<GuildMessage>, send_timeout: Duration) -> Self {
		Self {
			guild,
			open: Arc::new(AtomicBool::new(true)),
			link_up: Arc::new(AtomicBool::new(true)),
			tx,
			send_timeout,
			backlog: Arc::new(std::sync::Mutex::new(Backlog::default())),
		}
	}

	/// 音声の接続がつながっているか。
	///
	/// - 切れている間、Songbird は曲への命令 (`get_info` など) を処理しない (ミキサーが待機側へ移るため)。
	///   呼び出し側は待たずに諦める判断に使う
	///
	/// # 例
	/// `if !sink.link_up() { return Err(AudioError::NotConnected); }`
	pub fn link_up(&self) -> bool {
		self.link_up.load(Ordering::SeqCst)
	}

	/// 音声の接続の状態を記録する。
	///
	/// # 例
	/// `sink.set_link_up(false);`
	pub fn set_link_up(&self, up: bool) {
		self.link_up.store(up, Ordering::SeqCst);
	}

	/// 開いているか。
	///
	/// # 例
	/// `if sink.is_open() { ... }`
	pub fn is_open(&self) -> bool {
		self.open.load(Ordering::SeqCst)
	}

	/// 印を閉じる。以後のイベントは送らない。何度呼んでもよい。
	///
	/// # 例
	/// `sink.close();`
	pub fn close(&self) {
		self.open.store(false, Ordering::SeqCst);
	}

	/// イベントを送る。閉じていれば捨てる。
	///
	/// - 呼び出し元を待たせない。待ち行列が空ならその場で送り、メールボックスが満杯のときだけ
	///   待ち行列に積んで tokio のタスク (`drain`) に送らせる。ランタイムの外では送れずログに残す
	/// - 待ち行列があれば、その場で送れる状態でも後ろに積む (先に積んだイベントを追い越さない)
	///
	/// # 引数
	/// - `event`: 送るイベント
	///
	/// # 例
	/// `sink.emit(AudioEvent::TrackEnded { track });`
	pub fn emit(&self, event: AudioEvent) {
		if !self.is_open() {
			tracing::debug!(
				guild = self.guild.get(),
				?event,
				"閉じたセッションのイベントを捨てます"
			);
			return;
		}
		let mut backlog = self.lock_backlog();
		if backlog.draining {
			backlog.queue.push_back(event);
			return;
		}
		let event = match self.tx.try_send(GuildMessage::Audio(event)) {
			Ok(()) => return,
			Err(mpsc::error::TrySendError::Closed(_)) => {
				tracing::warn!(
					guild = self.guild.get(),
					"アクターが終了しているためイベントを捨てます"
				);
				return;
			}
			Err(mpsc::error::TrySendError::Full(GuildMessage::Audio(event))) => event,
			Err(mpsc::error::TrySendError::Full(_)) => return,
		};
		let Ok(runtime) = tokio::runtime::Handle::try_current() else {
			tracing::warn!(
				guild = self.guild.get(),
				?event,
				"ランタイムの外のためイベントを送れません"
			);
			return;
		};
		backlog.queue.push_back(event);
		backlog.draining = true;
		drop(backlog);
		runtime.spawn(self.clone().drain());
	}

	/// 待ち行列をロックする。毒されていても中身を使う (イベントを捨てないため)。
	///
	/// # 例
	/// `let mut backlog = self.lock_backlog();`
	fn lock_backlog(&self) -> std::sync::MutexGuard<'_, Backlog> {
		self.backlog
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
	}

	/// 待ち行列のイベントを先頭から 1 件ずつ送る。空になったら終わる。
	///
	/// - 1 件ごとに `send_timeout` を上限にメールボックスの空きを待つ (`reserve`)。空きを得てから
	///   先頭を取り出して送るため、待ちの間に積まれたイベントも順序を保つ
	/// - 上限を超えたイベントは捨ててログに残し、次へ進む
	/// - 閉じた、またはアクターが終わっていたら、残りを捨てて終わる
	///
	/// # 例
	/// `runtime.spawn(self.clone().drain());`
	async fn drain(self) {
		loop {
			let permit = if self.is_open() {
				Some(timeout(self.send_timeout, self.tx.reserve()).await)
			} else {
				None
			};
			let mut backlog = self.lock_backlog();
			let Some(event) = backlog.queue.pop_front() else {
				backlog.draining = false;
				return;
			};
			match permit {
				Some(Ok(Ok(permit))) if self.is_open() => permit.send(GuildMessage::Audio(event)),
				Some(Ok(Err(_))) => {
					tracing::warn!(
						guild = self.guild.get(),
						?event,
						dropped_after = backlog.queue.len(),
						"アクターが終了しているためイベントを捨てます"
					);
					backlog.queue.clear();
				}
				Some(Err(_)) => tracing::warn!(
					guild = self.guild.get(),
					?event,
					"アクターのメールボックスが空かないためイベントを捨てます"
				),
				_ => {
					tracing::debug!(
						guild = self.guild.get(),
						?event,
						dropped_after = backlog.queue.len(),
						"閉じたセッションのイベントを捨てます"
					);
					backlog.queue.clear();
				}
			}
			if backlog.queue.is_empty() {
				backlog.draining = false;
				return;
			}
		}
	}
}

/// 1 曲のイベントの門。開始と終わりを、それぞれ 1 回だけ通す。
///
/// - `start`: 最初の 1 回だけ `TrackStarted`
/// - `finish`: 最初の 1 回だけ `TrackFailed` か `TrackEnded` (`ended_event`)
/// - `silence`: 以後どちらも通さない (捨てた準備の曲、始められなかった曲)
/// - `mark_stopped`: 止めた曲の印。以後の終わりは、Songbird が失敗を報告しても入力の失敗の記録だけで決める
///   (止めるときの取得の取り消しで入力が読めなくなり、Songbird が失敗として報告しうるため)
/// - 曲の終わりの位置を覚え、終わった後の `position` に使う
/// - 頼んだだけでまだ終わっていない開始位置のシーク (`start_at`) を覚え、`observe_position` で
///   位置を補う (シークが終わるまで Songbird の位置は 0 付近のため)
///
/// 呼び出し方:
/// ```ignore
/// let gate = TrackGate::new(track, failure);
/// if let Some(event) = gate.finish(None) { sink.emit(event); }
/// let position = gate.observe_position(info.position);
/// ```
pub struct TrackGate {
	track: TrackId,
	failure: Option<ChunkFailure>,
	started: AtomicBool,
	finished: AtomicBool,
	/// こちらから止めた曲か
	stopped: AtomicBool,
	/// 終わったときの位置 (ミリ秒)。`NO_POSITION` は未記録
	last_position_ms: AtomicU64,
	/// まだ終わっていない開始位置のシークの位置 (ミリ秒)。`NO_POSITION` は無し
	pending_start_ms: AtomicU64,
}

/// `TrackGate` の位置の欄で「値が無い」ことを表す値。
const NO_POSITION: u64 = u64::MAX;

/// Duration を `TrackGate` の位置の欄の値 (ミリ秒) にする。`NO_POSITION` にはならない。
fn position_to_ms(position: Duration) -> u64 {
	u64::try_from(position.as_millis()).unwrap_or(NO_POSITION - 1)
}

/// 位置の欄の値を Duration に戻す。`NO_POSITION` なら None。
fn ms_to_position(millis: u64) -> Option<Duration> {
	(millis != NO_POSITION).then(|| Duration::from_millis(millis))
}

/// アクターに返す再生位置を決める。
///
/// - 開始位置のシークが終わっていなければ (`pending_start` が Some)、観測した位置と開始位置の大きい方
/// - 終わっていれば観測した位置
///
/// # 引数
/// - `observed`: Songbird が返した位置 (`get_info().position`、または終わったときの位置)
/// - `pending_start`: 頼んだがまだ届いていない開始位置
///
/// # 戻り値
/// 位置
///
/// # 例
/// `let position = effective_position(info.position, Some(Duration::from_secs(90)));`
pub fn effective_position(observed: Duration, pending_start: Option<Duration>) -> Duration {
	match pending_start {
		Some(start) => observed.max(start),
		None => observed,
	}
}

impl TrackGate {
	/// 門を作る。
	///
	/// # 引数
	/// - `track`: 曲
	/// - `failure`: `ChunkedRequest` の失敗の記録。`PipeInput` なら None
	///
	/// # 例
	/// `let gate = TrackGate::new(TrackId(1), Some(failure));`
	pub fn new(track: TrackId, failure: Option<ChunkFailure>) -> Self {
		Self {
			track,
			failure,
			started: AtomicBool::new(false),
			finished: AtomicBool::new(false),
			stopped: AtomicBool::new(false),
			last_position_ms: AtomicU64::new(NO_POSITION),
			pending_start_ms: AtomicU64::new(NO_POSITION),
		}
	}

	/// 開始のイベントを返す。2 回目以降と、終わった後は None。
	///
	/// # 例
	/// `if let Some(event) = gate.start() { sink.emit(event); }`
	pub fn start(&self) -> Option<AudioEvent> {
		if self.finished.load(Ordering::SeqCst) || self.started.swap(true, Ordering::SeqCst) {
			return None;
		}
		Some(AudioEvent::TrackStarted { track: self.track })
	}

	/// 終わりのイベントを返す。2 回目以降は None。
	///
	/// # 引数
	/// - `play_error`: Songbird が報告した再生の失敗の文言。Some なら `TrackFailed`。
	///   止めた曲 (`mark_stopped`) では無視する
	///
	/// # 戻り値
	/// `play_error` があれば `TrackFailed`、無ければ `ended_event` の結果
	///
	/// # 例
	/// `let event = gate.finish(Some("parsing failed"));`
	pub fn finish(&self, play_error: Option<&str>) -> Option<AudioEvent> {
		if self.finished.swap(true, Ordering::SeqCst) {
			return None;
		}
		let play_error = play_error.filter(|_| !self.stopped.load(Ordering::SeqCst));
		Some(match play_error {
			Some(message) => AudioEvent::TrackFailed {
				track: self.track,
				error: AudioError::Failed(message.to_owned()),
			},
			None => ended_event(self.track, self.failure.as_ref()),
		})
	}

	/// 以後のイベントを通さない。
	///
	/// # 例
	/// `gate.silence();`
	pub fn silence(&self) {
		self.started.store(true, Ordering::SeqCst);
		self.finished.store(true, Ordering::SeqCst);
	}

	/// こちらから止めた曲として印を付ける。曲を止める前に呼ぶ。
	///
	/// - 以後の `finish` は Songbird が報告した失敗を無視する (止めた曲には `TrackEnded` を送る契約のため)
	/// - 入力の失敗の記録 (`ChunkFailure`) に止める前の失敗があれば、`TrackFailed` のまま
	///
	/// # 例
	/// `gate.mark_stopped();`
	pub fn mark_stopped(&self) {
		self.stopped.store(true, Ordering::SeqCst);
	}

	/// 曲の位置を覚える。終わりのイベントを受けたときに呼ぶ。
	///
	/// # 例
	/// `gate.record_position(state.position);`
	pub fn record_position(&self, position: Duration) {
		self.last_position_ms
			.store(position_to_ms(position), Ordering::SeqCst);
	}

	/// 覚えた位置。終わりを受けていなければ None。
	///
	/// # 例
	/// `let position = gate.last_position().unwrap_or_default();`
	pub fn last_position(&self) -> Option<Duration> {
		ms_to_position(self.last_position_ms.load(Ordering::SeqCst))
	}

	/// 開始位置のシークを頼んだことを覚える。`play` が `start_at` のシークを頼んだときに呼ぶ。
	///
	/// # 例
	/// `gate.set_pending_start(request.start_at);`
	pub fn set_pending_start(&self, start_at: Duration) {
		self.pending_start_ms
			.store(position_to_ms(start_at), Ordering::SeqCst);
	}

	/// 開始位置のシークを忘れる。利用者のシークが終わったときに呼ぶ (その位置が新しい基準になる)。
	///
	/// # 例
	/// `gate.clear_pending_start();`
	pub fn clear_pending_start(&self) {
		self.pending_start_ms.store(NO_POSITION, Ordering::SeqCst);
	}

	/// 観測した位置から、アクターに返す位置を決める (`effective_position`)。
	///
	/// - 観測した位置が開始位置に達していれば、シークが終わったとみなして開始位置を忘れる
	///
	/// # 引数
	/// - `observed`: Songbird が返した位置
	///
	/// # 戻り値
	/// 開始位置のシークが終わるまでは、観測した位置と開始位置の大きい方
	///
	/// # 例
	/// `let position = gate.observe_position(info.position);`
	pub fn observe_position(&self, observed: Duration) -> Duration {
		let pending = ms_to_position(self.pending_start_ms.load(Ordering::SeqCst));
		if pending.is_some_and(|start| observed >= start) {
			self.clear_pending_start();
		}
		effective_position(observed, pending)
	}
}

/// 曲のイベントの時点の状態が、曲の開始 (`TrackStarted`) にあたるか。
///
/// - 再生中 (`PlayMode::Play`) で、入力の準備ができている (`ReadyState::Playable`) ときだけ true
/// - Songbird は `TrackCommand::Play` を受けると準備の状態に関係なく `TrackEvent::Play` を出す。
///   準備済みの曲 (`preload`) を `start_preloaded` で始めたとき、入力の準備 (`available_at` までの待ちを含む)
///   の前に `Play` が届きうる。そのときは開始とせず、後から再生中の状態で届く `Playable` に任せる
///   (健全性の猶予が早く始まり、送信なしと取り違えないように)
///
/// # 引数
/// - `state`: イベントに添えられた曲の状態。無ければ false
///
/// # 戻り値
/// 開始にあたれば true
///
/// # 例
/// `if track_start_ready(state) { gate.start() }`
pub fn track_start_ready(state: Option<&TrackState>) -> bool {
	state.is_some_and(|s| matches!(s.playing, PlayMode::Play) && s.ready == ReadyState::Playable)
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
				tracing::info!(
					guild = self.sink.guild.get(),
					songbird_reason = ?data.reason,
					songbird_kind = ?data.kind,
					kind = ?reason,
					"ボイスの接続が切れました"
				);
				self.sink.set_link_up(false);
				self.sink.emit(AudioEvent::Disconnected { reason });
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
	/// 取得の取り消しの口。`ChunkedRequest` のときだけ Some
	cancel: Option<ChunkCancel>,
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
		if let Some(cancel) = &self.cancel {
			cancel.cancel();
		}
	}

	/// イベントを送らずに曲を止めて捨てる。
	fn discard(self) {
		self.gate.silence();
		self.stop();
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
				if let Some(cancel) = &cancel {
					cancel.cancel();
				}
				return Err(error);
			}
		};
		Ok(Slot {
			track,
			handle,
			gate,
			seekable: !request.use_pipe,
			cancel,
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
			let track = slot.track;
			slot.discard();
			return Err(AudioError::Failed(format!(
				"曲の準備を頼めません (曲 {})",
				track.0
			)));
		}
		if !request.start_at.is_zero() {
			if slot.seekable {
				slot.gate.set_pending_start(request.start_at);
				if slot.handle.seek(request.start_at).is_hung_up() {
					let track = slot.track;
					slot.discard();
					return Err(AudioError::Failed(format!(
						"シークを頼めません (曲 {})",
						track.0
					)));
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
			let track = slot.track;
			slot.discard();
			return Err(AudioError::Failed(format!(
				"曲の準備を頼めません (曲 {})",
				track.0
			)));
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
	/// 接続が切れている間は待たずに `NotConnected` (Songbird が曲への命令を処理せず、上限まで止まるため)
	async fn position(&self) -> Result<Duration, AudioError> {
		let Some(slot) = &self.current else {
			return Ok(Duration::ZERO);
		};
		if !self.sink.link_up() {
			return Err(AudioError::NotConnected);
		}
		let handle = slot.handle.clone();
		let gate = Arc::clone(&slot.gate);
		bounded(self.call_timeout(), async move {
			match handle.get_info().await {
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
	/// # エラー
	/// manager が無ければ `NotConnected`、参加の失敗は `Connect`、時間切れは `Timeout`
	async fn move_to(&mut self, channel: ChannelId) -> Result<(), AudioError> {
		let Some(manager) = self.manager.clone() else {
			return Err(AudioError::NotConnected);
		};
		let guild = self.guild;
		bounded(self.join_timeout(), async move {
			manager
				.join(guild, channel)
				.await
				.map(|_| ())
				.map_err(|error| AudioError::Connect(error.to_string()))
		})
		.await
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
	/// 破棄されたセッションのイベントを送らない。
	fn drop(&mut self) {
		self.sink.close();
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
