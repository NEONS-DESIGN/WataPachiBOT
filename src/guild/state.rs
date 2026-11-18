//! GuildActor の状態。
//! - `PlayerState` が再生の段階。遷移はアクターだけが行う
//! - `CurrentTrack` が再生中 (または準備中) の曲、`SessionChannels` が再生のセッションの宛先

use std::time::Duration;

use tokio::time::Instant;

use super::recovery::{PositionClock, TrackAttempts};
use crate::audio::backend::TrackId;
use crate::db::queue::NewQueueItem;
use crate::domain::ChannelId;

/// 再生の段階。
///
/// ```text
/// Idle ─?p→ Connecting ─→ Preparing ─→ Playing ⇄ Paused
///                             ↑            │
///                             └── 次の曲 ──┘
/// Playing/Paused ─異常検知・?rejoin→ Recovering ─→ Preparing
/// どの状態からも ─?leave・自動退出→ Leaving ─→ Idle
/// ```
///
/// - `Paused` は一時停止、`Recovering` は接続の回復 (切断から再接続まで) が使う。`Leaving` は未使用
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlayerState {
	/// ボイスチャンネルに居ない。
	Idle,
	/// ボイスチャンネルへ接続している。
	Connecting,
	/// 接続済みで、次の曲を準備している (解決待ち、または `?p` の問い合わせ待ち)。
	Preparing,
	/// 再生中。
	Playing,
	/// 一時停止中。
	Paused,
	/// 接続の回復中。
	Recovering,
	/// 退出中。
	Leaving,
}

/// 再生中、または準備中の曲。
///
/// - キューから取り出した時点で作る。`track` は再生を始めたときに埋まる
/// - 失敗の後の再開 (`restart`) では `track` を空に戻し、次の再生の位置と方式を持つ
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentTrack {
	/// 曲の情報 (`playback_state` と履歴に書く内容)。
	pub item: NewQueueItem,
	/// この曲を取り出したときの世代。解決結果の照合に使う。
	pub generation: u64,
	/// バックエンドが割り当てた曲の識別子。再生を始める前は None。
	pub track: Option<TrackId>,
	/// 再生を始めた UNIX 秒。履歴の `played_at` に使う。再開しても最初の値を残す。
	pub started_at: Option<i64>,
	/// 次の再生を始める位置。
	pub start_at: Duration,
	/// 次の再生を PipeInput で行うか (曲の失敗の 2 回目)。
	pub use_pipe: bool,
	/// 次の解決でキャッシュを使わずに取り直すか (曲の失敗の後)。
	pub refresh: bool,
	/// 再生した形式 (`format_id`)。取り直しで同じ形式に固定する。分からなければ None。
	pub format_id: Option<String>,
	/// 失敗の回数。
	pub attempts: TrackAttempts,
	/// 音声から位置を得られないときの見積もり。
	pub clock: PositionClock,
	/// 次の再生を始めたらすぐ一時停止するか (一時停止中の曲を、自動の回復や曲の失敗の後に作り直すとき、
	/// 一時停止の状態で復元するとき)。一時停止が成功するまで残す (失敗しても次の作り直しで一時停止に戻すため)。
	pub pause_on_start: bool,
	/// 起動時の復元で再生する曲か。最初の再生で、再生中の通知の代わりに復元の通知を送る。
	pub restored: bool,
}

impl CurrentTrack {
	/// 準備中の曲を作る。
	///
	/// # 引数
	/// - `item`: キューから取り出した曲
	/// - `generation`: 取り出したときの世代
	///
	/// # 戻り値
	/// `track` と `started_at` が None の `CurrentTrack`
	///
	/// # 呼び出し例
	/// ```ignore
	/// let current = CurrentTrack::new(item, generation);
	/// ```
	pub fn new(item: NewQueueItem, generation: u64) -> Self {
		Self {
			item,
			generation,
			track: None,
			started_at: None,
			start_at: Duration::ZERO,
			use_pipe: false,
			refresh: false,
			format_id: None,
			attempts: TrackAttempts::default(),
			clock: PositionClock::default(),
			pause_on_start: false,
			restored: false,
		}
	}

	/// 曲を再生し直す準備をする。再生を始める前の状態に戻し、次の再生の位置と方式を決める。
	///
	/// - 世代を差し替えるので、古い解決結果と、前の再生の終了・失敗のイベントは捨てられる
	/// - 失敗の回数、最初の再生の時刻、形式は残す
	///
	/// # 引数
	/// - `generation`: 新しい世代
	/// - `start_at`: 再生を始める位置
	/// - `use_pipe`: PipeInput で再生するか (一度 true にしたら戻さない)
	/// - `refresh`: キャッシュを使わずに取り直すか
	///
	/// # 呼び出し例
	/// ```ignore
	/// current.restart(generation, position, false, true);
	/// ```
	pub fn restart(&mut self, generation: u64, start_at: Duration, use_pipe: bool, refresh: bool) {
		self.generation = generation;
		self.track = None;
		self.start_at = start_at;
		self.use_pipe |= use_pipe;
		self.refresh |= refresh;
		self.clock.set(start_at);
	}

	/// 再生を始めた曲として記録する。
	///
	/// - 位置の見積もりを `start_at` から進め始める。取り直しの指定は消す
	///
	/// # 引数
	/// - `track`: バックエンドが割り当てた識別子
	/// - `started_at`: 再生を始めた UNIX 秒 (再開のときは最初の値を残す)
	///
	/// # 呼び出し例
	/// ```ignore
	/// current.mark_started(track, now_secs());
	/// ```
	pub fn mark_started(&mut self, track: TrackId, started_at: i64) {
		self.track = Some(track);
		self.started_at.get_or_insert(started_at);
		self.refresh = false;
		self.clock.set(self.start_at);
		self.clock.run(Instant::now());
	}

	/// 再生を始める前 (解決待ち) か。
	///
	/// # 戻り値
	/// `track` が無ければ true
	///
	/// # 呼び出し例
	/// ```ignore
	/// if current.is_preparing() { ... }
	/// ```
	pub fn is_preparing(&self) -> bool {
		self.track.is_none()
	}
}

/// 再生のセッションの宛先。`?p` でセッションを始めたときに決まる。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionChannels {
	/// 接続するボイスチャンネル。
	pub voice: ChannelId,
	/// 曲の開始や失敗を知らせるテキストチャンネル。
	pub text: ChannelId,
}
