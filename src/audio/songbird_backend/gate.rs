//! 1 曲のイベントの門と再生位置の補正。
//!
//! - `TrackGate` は曲の開始と終わりをそれぞれ 1 回だけ通し、終わった位置と開始位置のシークを覚える
//! - `effective_position` と `track_start_ready` は Songbird の状態から位置と開始を決める (純粋)

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use songbird::tracks::{PlayMode, ReadyState, TrackState};

use super::events::ended_event;
use crate::audio::backend::{AudioError, AudioEvent, TrackId};
use crate::audio::chunked::ChunkFailure;

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
	/// - `failure`: 入力の失敗の記録 (`ChunkedRequest` と `PipeInput` のどちらも持つ)。None なら常に `TrackEnded`
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
