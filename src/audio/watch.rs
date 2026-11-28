//! 入力の読み取りの見張り。ミキサーのスレッドが入力の読み取り (取得の待ち) で止まっているかを外から見る。
//!
//! - Songbird のミキサーは入力を同期で読む。取得の待ち (再試行、`available_at`、取り直し、yt-dlp の詰まり) の間は
//!   曲への問い合わせ (`get_info`) にも答えない
//! - 問い合わせの時間切れが、ミキサーの固まり (切断として回復する) か、入力の待ちかを分けるために使う
//! - 読み取りとシークの開始の時刻を原子変数に置くだけで、ロックは取らない (ミキサーのホットパスのため)
//!
//! 呼び出し方:
//! ```ignore
//! let activity = ReadActivity::new();
//! let source = WatchedSource::new(inner, activity.clone());
//! // 別のタスクで
//! activity.wait_stalled(INPUT_STALL_GRACE).await; // 読み取りが猶予より長く続いたら戻る
//! ```

use std::io::{Read, Result as IoResult, Seek, SeekFrom};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use symphonia::core::io::MediaSource;

/// 読み取り中でないことを表す値。
const NOT_READING: u64 = 0;
/// 止まっているかを確かめる間隔。設定項目に無いため固定値にする。
const STALL_POLL: Duration = Duration::from_millis(50);

/// 時刻の基準。原子変数に置くため、プロセスの中で最初に使った時刻からのミリ秒にする。
static BASE: OnceLock<Instant> = OnceLock::new();

/// 基準からの経過ミリ秒。`NOT_READING` と区別するため 1 を足す。
///
/// # 戻り値
/// 1 以上のミリ秒
///
/// # 呼び出し例
/// `let now = now_ms();`
fn now_ms() -> u64 {
	let base = BASE.get_or_init(Instant::now);
	let elapsed = u64::try_from(base.elapsed().as_millis()).unwrap_or(u64::MAX - 1);
	elapsed.saturating_add(1)
}

/// 入力の読み取りの状態。複製しても同じ状態を指す。
#[derive(Debug, Clone, Default)]
pub struct ReadActivity {
	/// 読み取りを始めた時刻 (`now_ms`)。読み取り中でなければ `NOT_READING`
	since_ms: Arc<AtomicU64>,
}

impl ReadActivity {
	/// 読み取り中でない状態を作る。
	///
	/// # 呼び出し例
	/// `let activity = ReadActivity::new();`
	pub fn new() -> Self {
		Self::default()
	}

	/// 今の読み取りが続いている時間。読み取り中でなければ None。
	///
	/// # 呼び出し例
	/// `if activity.reading_for().is_some_and(|d| d > grace) { ... }`
	pub fn reading_for(&self) -> Option<Duration> {
		match self.since_ms.load(Ordering::Acquire) {
			NOT_READING => None,
			since => Some(Duration::from_millis(now_ms().saturating_sub(since))),
		}
	}

	/// 1 回の読み取りが `grace` より長く続くまで待つ。続かない限り戻らない。
	///
	/// - 通常の読み取りはバッファから写すだけで一瞬で終わるため、猶予を超えるのは取得の待ちのときだけ
	/// - 呼び出し側は `select!` で問い合わせと競わせ、こちらが先に終われば入力の待ちと判断する
	///
	/// # 引数
	/// - `grace`: 止まっているとみなすまでの時間
	///
	/// # 呼び出し例
	/// `tokio::select! { info = handle.get_info() => ..., () = activity.wait_stalled(grace) => ... }`
	pub async fn wait_stalled(&self, grace: Duration) {
		loop {
			if self.reading_for().is_some_and(|reading| reading >= grace) {
				return;
			}
			tokio::time::sleep(STALL_POLL).await;
		}
	}

	/// 読み取りの開始を記録し、終わりで消す印を返す。
	///
	/// # 呼び出し例
	/// `let _reading = self.activity.begin();`
	fn begin(&self) -> Reading<'_> {
		self.since_ms.store(now_ms(), Ordering::Release);
		Reading { activity: self }
	}
}

/// 読み取りの間だけ生きる印。破棄で読み取り中でない状態に戻す (panic でも戻る)。
struct Reading<'a> {
	activity: &'a ReadActivity,
}

impl Drop for Reading<'_> {
	/// 読み取り中でない状態に戻す。
	fn drop(&mut self) {
		self.activity.since_ms.store(NOT_READING, Ordering::Release);
	}
}

/// 読み取りとシークの間を `ReadActivity` に記録する入力の包み。
///
/// 呼び出し方:
/// ```ignore
/// let source: Box<dyn MediaSource> = Box::new(WatchedSource::new(adapter, activity.clone()));
/// ```
pub struct WatchedSource<S> {
	inner: S,
	activity: ReadActivity,
}

impl<S> WatchedSource<S> {
	/// 入力を包む。
	///
	/// # 引数
	/// - `inner`: 包む入力
	/// - `activity`: 記録先
	///
	/// # 呼び出し例
	/// `let source = WatchedSource::new(reader, activity.clone());`
	pub fn new(inner: S, activity: ReadActivity) -> Self {
		Self { inner, activity }
	}
}

impl<S: Read> Read for WatchedSource<S> {
	/// 中の入力から読む。読んでいる間を記録する。
	///
	/// # エラー
	/// 中の入力のエラー
	fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
		let _reading = self.activity.begin();
		self.inner.read(buf)
	}
}

impl<S: Seek> Seek for WatchedSource<S> {
	/// 中の入力をシークする。シークしている間も読み取りとして記録する (取得の待ちを伴うため)。
	///
	/// # エラー
	/// 中の入力のエラー
	fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
		let _reading = self.activity.begin();
		self.inner.seek(pos)
	}
}

impl<S: MediaSource> MediaSource for WatchedSource<S> {
	/// 中の入力がシークできるか。
	fn is_seekable(&self) -> bool {
		self.inner.is_seekable()
	}

	/// 中の入力の全長。
	fn byte_len(&self) -> Option<u64> {
		self.inner.byte_len()
	}
}
