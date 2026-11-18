//! 毎日の運用の処理 (Scheduler)。期限切れのキャッシュの掃除と、yt-dlp の更新・ワーカーの入れ替えを行う。
//!
//! - 起動直後に掃除を 1 回行う
//! - `update.time` があれば、`MAX_SLEEP` ずつ眠りながら起きるたびにローカル時刻で判定し直し、
//!   `update.time` から `UPDATE_GRACE` の窓の中で 1 日 1 回、掃除 → 更新 (一時停止中は飛ばす) を行う
//! - 窓の外で起動した日、窓を過ぎてからスリープから戻った日は実行せず翌日に回す
//! - 起動から `startup_hold` (本番は `STARTUP_UPDATE_HOLD`) の間は、窓の中でも毎日の処理を始めない。
//!   再起動の直後の復元とワーカーを取り合わないため。猶予の後も窓の中なら実行し、窓を過ぎていれば翌日に回す。
//!   起動直後の掃除 1 回は DB だけの短い処理のため待たない
//! - `update.time` が空なら `CLEANUP_INTERVAL` ごとに掃除だけ行う
//! - 停止の合図で戻る。uv の段 (作成・版の読み取り・検査) は future ごと捨てる (子は `kill_on_drop` で終わる)。
//!   入れ替えの段に入っていれば捨てずに終わるまで待つ (プールの終了で `Stopped` が返る)。
//!   そのため呼び出し側は、プールを止めてからタスクの終わりを待つこと
//!
//! 呼び出し方:
//! ```ignore
//! let (stop, stop_rx) = tokio::sync::watch::channel(false);
//! let task = scheduler::spawn(deps, stop_rx);
//! // 終了時: 合図を送り、プールを止めた後で task を待つ
//! let _ = stop.send(true);
//! ```

use std::path::PathBuf;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jiff::civil::{Date, DateTime, Time};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::config::CacheConfig;
use crate::db::{cache, DbWriter};
use crate::resolver::pool::{RollingRestartError, WorkerPool};
use crate::update::procedure::{Updater, WorkerSwitch};

/// 実行の窓の長さ。update.time からこの間だけ実行してよい
pub const UPDATE_GRACE: jiff::SignedDuration = jiff::SignedDuration::from_mins(30);
/// 眠る長さの上限 (起きるたびにローカル時刻で判定し直す)
pub const MAX_SLEEP: Duration = Duration::from_secs(60);
/// update.time が空のときの掃除の間隔
pub const CLEANUP_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
/// 起動から毎日の処理 (掃除と更新) を始めない時間。
/// Windows Update の夜間の再起動で窓の中に起動したとき、再生の復元とワーカーを取り合わないため
pub const STARTUP_UPDATE_HOLD: Duration = Duration::from_secs(5 * 60);

/// `HH:MM` の時と分の区切り。
const TIME_SEPARATOR: char = ':';

/// Scheduler が使う部品。
pub struct SchedulerDeps {
	/// 掃除を実行する DbWriter
	pub writer: DbWriter,
	/// 掃除の期限 (`[cache]`)
	pub cache: CacheConfig,
	/// 毎日の更新の時刻。None なら更新せず、`CLEANUP_INTERVAL` ごとに掃除だけ行う
	pub update_at: Option<Time>,
	/// 更新の手順
	pub updater: Arc<Updater>,
	/// 動いているワーカーの切り替え (本番は `app::LiveSwitch`)
	pub switch: Arc<dyn WorkerSwitch>,
	/// 一時停止中かを見るプール
	pub pool: Arc<WorkerPool>,
	/// 今のローカル時刻。本番は jiff::ZonedDateTime::now、試験は固定の時刻
	pub clock: Arc<dyn Fn() -> jiff::ZonedDateTime + Send + Sync>,
	/// 起動から毎日の処理を始めない時間。本番は `STARTUP_UPDATE_HOLD`、試験は短い値か 0
	pub startup_hold: Duration,
}

/// "HH:MM" を時刻にする (純粋)。
///
/// - 形式の検査は `Config::validate` が済ませている前提。範囲外や形式の誤りは None にする
///
/// # 引数
/// - `text`: `update.time` の値
///
/// # 戻り値
/// 時刻。空、または読めなければ None
///
/// # 例
/// `assert_eq!(parse_time("04:00"), Some(jiff::civil::time(4, 0, 0, 0)));`
pub fn parse_time(text: &str) -> Option<Time> {
	let (hour, minute) = text.split_once(TIME_SEPARATOR)?;
	let hour: i8 = hour.parse().ok()?;
	let minute: i8 = minute.parse().ok()?;
	Time::new(hour, minute, 0, 0).ok()
}

/// 今が実行の窓の中で、その窓をまだ実行していなければ、窓の開始日を返す (純粋)。
///
/// - 窓は「今日の at」と「昨日の at」から `UPDATE_GRACE` の間 (日をまたぐ窓のため)。始まりを含み、終わりを含まない
/// - `last_run` が窓の開始日以後なら実行済みとして None
///
/// # 引数
/// - `now`: 今のローカル時刻
/// - `at`: `update.time`
/// - `last_run`: 最後に実行した窓の開始日
///
/// # 戻り値
/// 実行すべき窓の開始日。実行しないなら None
///
/// # 例
/// `if let Some(date) = due_window(now.datetime(), at, last_run) { last_run = Some(date); }`
pub fn due_window(now: DateTime, at: Time, last_run: Option<Date>) -> Option<Date> {
	let today = now.date();
	[Some(today), today.yesterday().ok()]
		.into_iter()
		.flatten()
		.find(|&start| {
			let elapsed = now.duration_since(start.to_datetime(at));
			let inside = !elapsed.is_negative() && elapsed < UPDATE_GRACE;
			inside && last_run.is_none_or(|last| last < start)
		})
}

/// Scheduler を起動する。stop に true が来たら、進行中の uv の子を終わらせて戻る。
///
/// - 入れ替えの途中に stop が来たときは、入れ替えが終わるまで戻らない (プールの終了で終わる)
/// - stop の送り手が破棄されたときも停止として扱う
///
/// # 引数
/// - `deps`: 使う部品
/// - `stop`: 停止の合図
///
/// # 戻り値
/// タスクの JoinHandle
///
/// # 例
/// `let task = scheduler::spawn(deps, stop_rx);`
pub fn spawn(deps: SchedulerDeps, stop: watch::Receiver<bool>) -> JoinHandle<()> {
	tokio::spawn(run(deps, stop))
}

/// Scheduler の本体。停止の合図で戻る。
///
/// # 引数
/// - `deps`: 使う部品
/// - `stop`: 停止の合図
///
/// # 例
/// `tokio::spawn(run(deps, stop));`
async fn run(deps: SchedulerDeps, mut stop: watch::Receiver<bool>) {
	tracing::info!(update_at = ?deps.update_at, "Scheduler を始めます");
	if !cleanup_or_stop(&deps, &mut stop).await {
		return;
	}
	match deps.update_at {
		Some(at) => daily_loop(&deps, at, &mut stop).await,
		None => cleanup_loop(&deps, &mut stop).await,
	}
	tracing::info!("Scheduler を終えました");
}

/// `update.time` があるときのループ。窓の判定は眠る前に行う。
///
/// - 起動から `deps.startup_hold` の間は、窓の中でも実行しない (窓を実行済みにもしない)。
///   猶予の終わりに起きて判定し直す
///
/// # 引数
/// - `deps`: 使う部品
/// - `at`: `update.time`
/// - `stop`: 停止の合図
///
/// # 例
/// `daily_loop(&deps, at, &mut stop).await;`
async fn daily_loop(deps: &SchedulerDeps, at: Time, stop: &mut watch::Receiver<bool>) {
	let mut last_run: Option<Date> = None;
	let started = tokio::time::Instant::now();
	let mut hold_logged = false;
	loop {
		let now = (deps.clock)();
		let held = startup_hold_left(started.elapsed(), deps.startup_hold);
		if let Some(date) = due_window(now.datetime(), at, last_run) {
			match held {
				Some(left) => {
					if !hold_logged {
						hold_logged = true;
						tracing::info!(
							wait_secs = left.as_secs(),
							"起動の直後のため、毎日の処理 (掃除と yt-dlp の更新) を待ちます"
						);
					}
				}
				None => {
					last_run = Some(date);
					if !daily_work(deps, now, stop).await {
						return;
					}
				}
			}
		}
		let nap = held.map_or(MAX_SLEEP, |left| left.min(MAX_SLEEP));
		tokio::select! {
			() = stopped(stop) => return,
			() = tokio::time::sleep(nap) => {}
		}
	}
}

/// 起動の直後の猶予の残りを返す (純粋)。
///
/// # 引数
/// - `elapsed`: 起動 (Scheduler の開始) からの経過時間
/// - `hold`: 猶予の長さ (`SchedulerDeps::startup_hold`)
///
/// # 戻り値
/// 猶予の中なら残りの時間。過ぎていれば None
///
/// # 例
/// ```
/// use std::time::Duration;
/// use watapachi_bot::scheduler::startup_hold_left;
///
/// let hold = Duration::from_secs(300);
/// assert_eq!(startup_hold_left(Duration::from_secs(60), hold), Some(Duration::from_secs(240)));
/// assert_eq!(startup_hold_left(hold, hold), None);
/// ```
pub fn startup_hold_left(elapsed: Duration, hold: Duration) -> Option<Duration> {
	hold.checked_sub(elapsed).filter(|left| !left.is_zero())
}

/// `update.time` が空のときのループ。`CLEANUP_INTERVAL` ごとに掃除だけ行う。
///
/// # 引数
/// - `deps`: 使う部品
/// - `stop`: 停止の合図
///
/// # 例
/// `cleanup_loop(&deps, &mut stop).await;`
async fn cleanup_loop(deps: &SchedulerDeps, stop: &mut watch::Receiver<bool>) {
	loop {
		tokio::select! {
			() = stopped(stop) => return,
			() = tokio::time::sleep(CLEANUP_INTERVAL) => {}
		}
		if !cleanup_or_stop(deps, stop).await {
			return;
		}
	}
}

/// 毎日の処理: 掃除 → 一時停止中なら更新を飛ばす → 更新。
///
/// - uv の段で停止の合図が来たら更新の future を捨てる
/// - 入れ替えの段に入っていたら、停止の合図の後も更新が終わるまで待つ
///
/// # 引数
/// - `deps`: 使う部品
/// - `now`: 今のローカル時刻 (環境の名前に使う)
/// - `stop`: 停止の合図
///
/// # 戻り値
/// 続けるなら true、停止の合図を受けたら false
///
/// # 例
/// `if !daily_work(deps, now, stop).await { return; }`
async fn daily_work(
	deps: &SchedulerDeps,
	now: jiff::ZonedDateTime,
	stop: &mut watch::Receiver<bool>,
) -> bool {
	if !cleanup_or_stop(deps, stop).await {
		return false;
	}
	if deps.pool.is_paused() {
		tracing::info!("回数制限でプールが一時停止中のため、今日の yt-dlp の更新とワーカーの入れ替えを飛ばします");
		return true;
	}

	let switch = MarkedSwitch {
		inner: Arc::clone(&deps.switch),
		entered: AtomicBool::new(false),
	};
	let mut update = pin!(deps.updater.run(Some(&switch as &dyn WorkerSwitch), now));
	tokio::select! {
		// 結果のログは Updater::run が出す
		_ = &mut update => return true,
		() = stopped(stop) => {}
	}
	if switch.entered() {
		// 入れ替えは途中で捨てると元へ戻らないため、プールの終了で Stopped が返るまで待つ
		tracing::info!("入れ替えの途中のため、ワーカーの終了で止まるまで待ちます");
		let _ = update.await;
	} else {
		tracing::info!("終了のため yt-dlp の更新を打ち切ります");
	}
	false
}

/// 掃除を停止の合図と競わせて行う。
///
/// # 引数
/// - `deps`: 使う部品
/// - `stop`: 停止の合図
///
/// # 戻り値
/// 掃除を終えたら true、先に停止の合図を受けたら false
///
/// # 例
/// `if !cleanup_or_stop(&deps, &mut stop).await { return; }`
async fn cleanup_or_stop(deps: &SchedulerDeps, stop: &mut watch::Receiver<bool>) -> bool {
	tokio::select! {
		() = cleanup(deps) => true,
		() = stopped(stop) => false,
	}
}

/// 期限切れの行を消す。消した行数を info、失敗を warn に出す。
///
/// - 時刻は `deps.clock` から取る
/// - 表ごとに上限の件数ずつ、別々の書き込みで消す。間にコマンドの書き込みが入れるようにするため
///
/// # 引数
/// - `deps`: 使う部品
///
/// # 例
/// `cleanup(&deps).await;`
async fn cleanup(deps: &SchedulerDeps) {
	let now = (deps.clock)().timestamp().as_second();
	match cache::purge_expired_in_chunks(&deps.writer, now, &deps.cache).await {
		Ok(removed) => tracing::info!(removed, "期限切れのキャッシュと履歴を消しました"),
		Err(error) => tracing::warn!(%error, "期限切れのキャッシュと履歴を消せませんでした"),
	}
}

/// 停止の合図 (true か送り手の破棄) を待つ。
///
/// # 引数
/// - `stop`: 停止の合図。既に true なら直ちに戻る
///
/// # 例
/// `tokio::select! { () = stopped(&mut stop) => return, () = tokio::time::sleep(MAX_SLEEP) => {} }`
async fn stopped(stop: &mut watch::Receiver<bool>) {
	// 送り手の破棄 (Err) も停止として扱う
	let _ = stop.wait_for(|stopped| *stopped).await;
}

/// 入れ替えの段に入ったかを記録する `WorkerSwitch` の包み。
///
/// - `switch` が一度でも呼ばれたら `entered` を立てる (戻った後も下ろさない)
/// - 入れ替えの後の記録と片付けは待っても短いため、立った後は更新の終わりまで待つ
struct MarkedSwitch {
	inner: Arc<dyn WorkerSwitch>,
	entered: AtomicBool,
}

impl MarkedSwitch {
	/// 入れ替えの段に入ったか。
	///
	/// # 戻り値
	/// `switch` が呼ばれていれば true
	///
	/// # 例
	/// `if switch.entered() { let _ = update.await; }`
	fn entered(&self) -> bool {
		self.entered.load(Ordering::SeqCst)
	}
}

#[async_trait]
impl WorkerSwitch for MarkedSwitch {
	/// 記録してから包んだ切り替えを呼ぶ。
	///
	/// # 引数
	/// - `python`: 新しい環境の python.exe。None なら同じ環境で入れ替える
	///
	/// # エラー
	/// 包んだ切り替えのエラーをそのまま返す
	///
	/// # 例
	/// `updater.run(Some(&marked as &dyn WorkerSwitch), now).await;`
	async fn switch(&self, python: Option<PathBuf>) -> Result<(), RollingRestartError> {
		self.entered.store(true, Ordering::SeqCst);
		self.inner.switch(python).await
	}
}
