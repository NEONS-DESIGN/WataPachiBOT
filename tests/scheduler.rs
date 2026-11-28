//! Scheduler の試験。
//!
//! - 窓の判定 (`due_window`) と時刻の読み取り (`parse_time`) は純粋な関数として確かめる
//! - タスクの試験は時計を固定し、環境の作成と入れ替えを偽物に置き換える
//! - 一時停止中のプールは tests/fixtures/fake_worker.py に `limited` を投げて作る

#[path = "support/python.rs"]
mod python;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use jiff::civil::{Date, DateTime, Time};
use tempfile::TempDir;
use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;
use watapachi_bot::config::{CacheConfig, DbConfig, WorkerConfig};
use watapachi_bot::db::{cache, Database};
use watapachi_bot::domain::{Priority, TrackMeta, VideoId};
use watapachi_bot::platform::job::Job;
use watapachi_bot::resolver::pool::{RollingRestartError, WorkerClient, WorkerPool};
use watapachi_bot::resolver::process::WorkerSpec;
use watapachi_bot::resolver::protocol::WorkerRequest;
use watapachi_bot::scheduler::{self, due_window, parse_time, startup_hold_left, SchedulerDeps};
use watapachi_bot::update::envs::{EnvRecord, EnvStore};
use watapachi_bot::update::procedure::{EnvTools, UpdateError, Updater, WorkerSwitch};

/// 停止の合図から JoinHandle が終わるまでの上限。
const STOP_LIMIT: Duration = Duration::from_secs(1);
/// 待ち合わせの上限。
const WAIT_LIMIT: Duration = Duration::from_secs(30);
/// 一時停止のプールで、更新を飛ばしたと判断するまでに待つ時間。
const SETTLE: Duration = Duration::from_millis(500);
/// 時計を固定する時刻 (update.time と同じ 04:00)。
const FIXED_NOW: &str = "2026-11-18T04:00:00+09:00[Asia/Tokyo]";
/// 時計を固定する時刻 (窓の外の昼間)。
const DAYTIME_NOW: &str = "2026-11-18T13:00:00+09:00[Asia/Tokyo]";
/// 窓 (04:00 から 30 分) の終わりの直前。
const WINDOW_END_NOW: &str = "2026-11-18T04:29:00+09:00[Asia/Tokyo]";
/// 窓を過ぎた時刻。
const AFTER_WINDOW_NOW: &str = "2026-11-18T04:31:00+09:00[Asia/Tokyo]";
/// 起動の直後の猶予を確かめる試験で使う猶予。
const TEST_HOLD: Duration = Duration::from_secs(1);
/// 古い環境の名前と版。
const OLD: &str = "old";
const OLD_VERSION: &str = "2026.11.15";
/// 偽の環境が返す新しい版。
const NEW_VERSION: &str = "2026.11.17";

/// 試験で使う時刻 (h 時 m 分 0 秒) を作る。
fn at(h: i8, m: i8) -> Time {
	jiff::civil::time(h, m, 0, 0)
}

/// 2026 年 11 月 d 日 h 時 m 分 0 秒の日時を作る。
fn dt(d: i8, h: i8, m: i8) -> DateTime {
	jiff::civil::date(2026, 11, d).at(h, m, 0, 0)
}

/// 2026 年 11 月 d 日の日付を作る。
fn day(d: i8) -> Date {
	jiff::civil::date(2026, 11, d)
}

#[test]
fn due_inside_window_once() {
	assert_eq!(due_window(dt(18, 4, 0), at(4, 0), None), Some(day(18)));
	assert_eq!(due_window(dt(18, 4, 10), at(4, 0), Some(day(18))), None);
	assert_eq!(
		due_window(dt(19, 4, 5), at(4, 0), Some(day(18))),
		Some(day(19))
	);
}

#[test]
fn not_due_outside_window() {
	assert_eq!(due_window(dt(18, 3, 59), at(4, 0), None), None);
	// 窓は 30 分 (終わりを含まない)
	assert_eq!(due_window(dt(18, 4, 30), at(4, 0), None), None);
	assert_eq!(due_window(dt(18, 4, 29), at(4, 0), None), Some(day(18)));
	// 昼間の起動では実行しない
	assert_eq!(due_window(dt(18, 13, 0), at(4, 0), None), None);
	// 04:31 の起動 (窓を過ぎた) でも実行しない
	assert_eq!(due_window(dt(18, 4, 31), at(4, 0), None), None);
}

#[test]
fn window_crossing_midnight() {
	assert_eq!(due_window(dt(19, 0, 10), at(23, 50), None), Some(day(18)));
	assert_eq!(due_window(dt(19, 0, 10), at(23, 50), Some(day(18))), None);
	assert_eq!(due_window(dt(18, 23, 55), at(23, 50), None), Some(day(18)));
	assert_eq!(due_window(dt(19, 0, 20), at(23, 50), None), None);
}

#[test]
fn startup_hold_left_counts_down() {
	let hold = Duration::from_secs(300);
	assert_eq!(startup_hold_left(Duration::ZERO, hold), Some(hold));
	assert_eq!(
		startup_hold_left(Duration::from_secs(299), hold),
		Some(Duration::from_secs(1))
	);
	assert_eq!(startup_hold_left(hold, hold), None);
	assert_eq!(startup_hold_left(Duration::from_secs(301), hold), None);
	// 猶予 0 は始めから過ぎている
	assert_eq!(startup_hold_left(Duration::ZERO, Duration::ZERO), None);
}

#[test]
fn parse_time_values() {
	assert_eq!(parse_time("04:00"), Some(at(4, 0)));
	assert_eq!(parse_time("23:59"), Some(at(23, 59)));
	assert_eq!(parse_time(""), None);
	assert_eq!(parse_time("24:00"), None);
}

/// 偽の `EnvTools` が記録する呼び出し。
type Calls = Arc<Mutex<Vec<String>>>;

/// 偽の `EnvTools`。呼ばれた順を記録し、作成の代わりに眠ることができる。
struct FakeTools {
	create_sleep: Option<Duration>,
	calls: Calls,
}

#[async_trait]
impl EnvTools for FakeTools {
	async fn create(&self, dir: &Path) -> Result<(), UpdateError> {
		self.calls.lock().unwrap().push("create".to_owned());
		if let Some(sleep) = self.create_sleep {
			tokio::time::sleep(sleep).await;
		}
		let python = dir.join("Scripts").join("python.exe");
		fs::create_dir_all(python.parent().unwrap()).unwrap();
		fs::write(python, b"x").unwrap();
		Ok(())
	}

	async fn yt_dlp_version(&self, _python: &Path) -> Result<String, UpdateError> {
		self.calls.lock().unwrap().push("version".to_owned());
		Ok(NEW_VERSION.to_owned())
	}

	async fn check(&self, _python: &Path) -> Result<(), UpdateError> {
		self.calls.lock().unwrap().push("check".to_owned());
		Ok(())
	}
}

/// 偽の `WorkerSwitch`。呼ばれたことを記録し、`hold` なら解放されるまで戻らない。
#[derive(Default)]
struct FakeSwitch {
	entered: AtomicBool,
	hold: bool,
	release: Notify,
}

#[async_trait]
impl WorkerSwitch for FakeSwitch {
	async fn switch(&self, _python: Option<PathBuf>) -> Result<(), RollingRestartError> {
		self.entered.store(true, Ordering::SeqCst);
		if self.hold {
			self.release.notified().await;
			return Err(RollingRestartError::Stopped);
		}
		Ok(())
	}
}

/// 試験の部品。一時ディレクトリ、DB、環境の置き場、プールを持つ。
struct Fixture {
	dir: TempDir,
	database: Database,
	store: EnvStore,
	pool: Arc<WorkerPool>,
	calls: Calls,
}

/// キャッシュの設定。検索は 1 日で期限切れ。
fn cache_config() -> CacheConfig {
	CacheConfig {
		playlist_ttl_secs: 100,
		search_days: 1,
		track_days: 2,
		history_days: 3,
	}
}

/// プールの設定。回数制限の一時停止は試験中に明けない長さにする。
fn worker_config() -> WorkerConfig {
	WorkerConfig {
		worker_count: 1,
		prefetch_reserved_workers: 0,
		resolve_timeout_secs: 30,
		startup_timeout_secs: 60,
		ping_interval_secs: 3600,
		ping_timeout_secs: 10,
		max_retries: 3,
		rate_limit_backoff_secs: 600,
		rate_limit_backoff_max_secs: 600,
		cookies_browser: String::new(),
		youtube_premium: "auto".to_owned(),
		user_agent: String::new(),
	}
}

/// fake_worker.py の起動指定。
fn fake_worker_spec(python: PathBuf, log: &Path) -> WorkerSpec {
	WorkerSpec {
		python,
		script: Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake_worker.py"),
		args: vec!["--log".to_owned(), log.display().to_string()],
	}
}

impl Fixture {
	/// 起動できない python のプール (一時停止しない) で作る。
	fn new() -> Self {
		let dir = TempDir::new().unwrap();
		let spec = fake_worker_spec(
			PathBuf::from("no-such-python-executable"),
			&dir.path().join("worker.log"),
		);
		Self::with_spec(dir, spec)
	}

	/// fake_worker.py のプールで作る。
	fn with_fake_worker() -> Self {
		let dir = TempDir::new().unwrap();
		let spec = fake_worker_spec(python::test_python(), &dir.path().join("worker.log"));
		Self::with_spec(dir, spec)
	}

	/// 指定のワーカーの起動指定でプールを作り、DB と、古い環境 `old` を指す `current.json` を用意する。
	fn with_spec(dir: TempDir, spec: WorkerSpec) -> Self {
		let database = Database::open(
			&dir.path().join("test.db"),
			&DbConfig {
				batch_size: 256,
				read_connections: 2,
				writer_queue: 1024,
			},
		)
		.unwrap();
		let store = EnvStore::new(dir.path().join("envs"));
		let python = store.python_of(OLD);
		fs::create_dir_all(python.parent().unwrap()).unwrap();
		fs::write(&python, b"x").unwrap();
		store.write_current(&old_record(&store)).unwrap();
		let job = Arc::new(Job::new().unwrap());
		let pool = Arc::new(WorkerPool::start(spec, &worker_config(), job));
		Self {
			dir,
			database,
			store,
			pool,
			calls: Calls::default(),
		}
	}

	/// Scheduler の依存を作る。起動の直後の猶予は 0 (すぐ判定する)。
	fn deps(
		&self,
		now: &str,
		update_at: Option<Time>,
		create_sleep: Option<Duration>,
		switch: Arc<FakeSwitch>,
	) -> SchedulerDeps {
		let tools = FakeTools {
			create_sleep,
			calls: Arc::clone(&self.calls),
		};
		let fixed: jiff::ZonedDateTime = now.parse().unwrap();
		SchedulerDeps {
			writer: self.database.handles.writer.clone(),
			cache: cache_config(),
			update_at,
			updater: Arc::new(Updater::new(self.store.clone(), Arc::new(tools), true)),
			switch,
			pool: Arc::clone(&self.pool),
			clock: Arc::new(move || fixed.clone()),
			startup_hold: Duration::ZERO,
		}
	}

	/// 偽の環境で呼ばれた処理の名前。
	fn calls(&self) -> Vec<String> {
		self.calls.lock().unwrap().clone()
	}

	/// 検索のキャッシュの行数。
	async fn search_rows(&self) -> i64 {
		self.database
			.handles
			.reader
			.read(|c| Ok(c.query_row("SELECT count(*) FROM search_cache", [], |r| r.get(0))?))
			.await
			.unwrap()
	}

	/// 後始末。プールと DB を閉じる。
	async fn close(self) {
		self.pool.shutdown().await;
		self.database.close().await.unwrap();
		drop(self.dir);
	}
}

/// 古い環境の記録。
fn old_record(store: &EnvStore) -> EnvRecord {
	EnvRecord {
		name: OLD.to_owned(),
		python: store.python_of(OLD),
		yt_dlp_version: OLD_VERSION.to_owned(),
		created_at: 1_800_000_000,
		previous: None,
	}
}

/// 条件が真になるまで待つ。上限を超えたら失敗にする。
async fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
	let started = tokio::time::Instant::now();
	while !condition() {
		assert!(started.elapsed() < WAIT_LIMIT, "{what} を待ちきれません");
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

/// 停止を送り、上限までに JoinHandle が終わることを確かめる。
async fn stop_and_join(stop: &watch::Sender<bool>, task: JoinHandle<()>) {
	stop.send(true).unwrap();
	tokio::time::timeout(STOP_LIMIT, task)
		.await
		.expect("停止の合図から 1 秒以内に終わるはず")
		.expect("タスクが異常終了しない");
}

#[tokio::test]
async fn stop_ends_the_task_promptly() {
	let fixture = Fixture::new();
	let switch = Arc::new(FakeSwitch::default());
	let (stop, stop_rx) = watch::channel(false);
	let task = scheduler::spawn(fixture.deps(DAYTIME_NOW, None, None, switch), stop_rx);
	tokio::time::sleep(Duration::from_millis(100)).await;
	assert!(!task.is_finished(), "停止の合図まで動き続ける");

	stop_and_join(&stop, task).await;
	fixture.close().await;
}

#[tokio::test]
async fn stop_during_update_ends_promptly() {
	let fixture = Fixture::new();
	let switch = Arc::new(FakeSwitch::default());
	let deps = fixture.deps(
		FIXED_NOW,
		Some(at(4, 0)),
		Some(Duration::from_secs(3600)),
		Arc::clone(&switch),
	);
	let (stop, stop_rx) = watch::channel(false);
	let task = scheduler::spawn(deps, stop_rx);
	wait_until("create の呼び出し", || {
		fixture.calls().contains(&"create".to_owned())
	})
	.await;

	stop_and_join(&stop, task).await;
	assert_eq!(
		fixture.store.read_current().unwrap().unwrap(),
		old_record(&fixture.store)
	);
	assert!(
		!switch.entered.load(Ordering::SeqCst),
		"入れ替えは呼ばれない"
	);
	fixture.close().await;
}

#[tokio::test]
async fn stop_during_switch_waits_for_it() {
	let fixture = Fixture::new();
	let switch = Arc::new(FakeSwitch {
		hold: true,
		..FakeSwitch::default()
	});
	let deps = fixture.deps(FIXED_NOW, Some(at(4, 0)), None, Arc::clone(&switch));
	let (stop, stop_rx) = watch::channel(false);
	let task = scheduler::spawn(deps, stop_rx);
	wait_until("入れ替えの開始", || {
		switch.entered.load(Ordering::SeqCst)
	})
	.await;

	stop.send(true).unwrap();
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert!(!task.is_finished(), "入れ替えの途中では捨てずに待つ");

	// プールの終了で入れ替えが Stopped を返したのと同じ状況にする
	switch.release.notify_one();
	tokio::time::timeout(STOP_LIMIT, task)
		.await
		.expect("入れ替えが終われば戻るはず")
		.expect("タスクが異常終了しない");
	assert_eq!(
		fixture.store.read_current().unwrap().unwrap(),
		old_record(&fixture.store)
	);
	fixture.close().await;
}

#[tokio::test]
async fn paused_pool_skips_update() {
	let fixture = Fixture::with_fake_worker();
	let reply = fixture
		.pool
		.submit(
			WorkerRequest::Resolve {
				url: "limited".to_owned(),
				format_id: None,
			},
			Priority::Interactive,
		)
		.await;
	assert!(reply.is_err(), "limited は回数制限で失敗する");
	assert!(fixture.pool.is_paused());
	insert_expired_search(&fixture).await;

	let switch = Arc::new(FakeSwitch::default());
	let deps = fixture.deps(FIXED_NOW, Some(at(4, 0)), None, Arc::clone(&switch));
	let (stop, stop_rx) = watch::channel(false);
	let task = scheduler::spawn(deps, stop_rx);
	tokio::time::sleep(SETTLE).await;

	assert_eq!(fixture.search_rows().await, 0, "一時停止中も掃除はする");
	assert!(fixture.calls().is_empty(), "一時停止中は更新しない");
	assert!(!switch.entered.load(Ordering::SeqCst));
	stop_and_join(&stop, task).await;
	fixture.close().await;
}

#[tokio::test]
async fn due_update_runs_and_switches() {
	let fixture = Fixture::new();
	let switch = Arc::new(FakeSwitch::default());
	let deps = fixture.deps(FIXED_NOW, Some(at(4, 0)), None, Arc::clone(&switch));
	let (stop, stop_rx) = watch::channel(false);
	let task = scheduler::spawn(deps, stop_rx);
	wait_until("更新の記録", || {
		fixture
			.store
			.read_current()
			.ok()
			.flatten()
			.is_some_and(|record| record.yt_dlp_version == NEW_VERSION)
	})
	.await;

	assert!(switch.entered.load(Ordering::SeqCst));
	assert_eq!(fixture.calls(), ["create", "version", "check"]);
	stop_and_join(&stop, task).await;
	// 同じ窓では 2 回目を行わない
	assert_eq!(fixture.calls(), ["create", "version", "check"]);
	fixture.close().await;
}

#[tokio::test]
async fn startup_cleanup_runs_once() {
	let fixture = Fixture::new();
	insert_expired_search(&fixture).await;
	assert_eq!(fixture.search_rows().await, 1);

	let switch = Arc::new(FakeSwitch::default());
	let (stop, stop_rx) = watch::channel(false);
	let task = scheduler::spawn(fixture.deps(DAYTIME_NOW, None, None, switch), stop_rx);
	let started = tokio::time::Instant::now();
	while fixture.search_rows().await != 0 {
		assert!(
			started.elapsed() < WAIT_LIMIT,
			"起動直後の掃除を待ちきれません"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	stop_and_join(&stop, task).await;
	assert!(fixture.calls().is_empty(), "update.time が空なら更新しない");
	fixture.close().await;
}

/// 期限切れの検索のキャッシュ (UNIX 秒 0 に取得) を 1 行入れる。
async fn insert_expired_search(fixture: &Fixture) {
	let meta = TrackMeta {
		video_id: VideoId("vid00000001".to_owned()),
		title: "title".to_owned(),
		duration_s: Some(1),
		thumbnail: None,
	};
	fixture
		.database
		.handles
		.writer
		.run(move |c| cache::put_search(c, "old", &meta, 0))
		.await
		.unwrap();
}

#[tokio::test]
async fn update_waits_for_startup_hold() {
	let fixture = Fixture::new();
	let switch = Arc::new(FakeSwitch::default());
	let mut deps = fixture.deps(FIXED_NOW, Some(at(4, 0)), None, Arc::clone(&switch));
	deps.startup_hold = TEST_HOLD;
	let (stop, stop_rx) = watch::channel(false);
	let started = tokio::time::Instant::now();
	let task = scheduler::spawn(deps, stop_rx);
	tokio::time::sleep(TEST_HOLD / 2).await;
	assert!(fixture.calls().is_empty(), "猶予の間は窓の中でも更新しない");

	wait_until("猶予の後の更新", || {
		fixture.calls().contains(&"create".to_owned())
	})
	.await;
	assert!(started.elapsed() >= TEST_HOLD, "更新は猶予の後に始まる");
	wait_until("入れ替え", || switch.entered.load(Ordering::SeqCst)).await;
	stop_and_join(&stop, task).await;
	fixture.close().await;
}

#[tokio::test]
async fn window_passed_during_startup_hold_is_skipped() {
	let fixture = Fixture::new();
	let switch = Arc::new(FakeSwitch::default());
	let mut deps = fixture.deps(WINDOW_END_NOW, Some(at(4, 0)), None, Arc::clone(&switch));
	deps.startup_hold = TEST_HOLD;
	let now: Arc<Mutex<jiff::ZonedDateTime>> =
		Arc::new(Mutex::new(WINDOW_END_NOW.parse().unwrap()));
	let clock = Arc::clone(&now);
	deps.clock = Arc::new(move || clock.lock().unwrap().clone());
	let (stop, stop_rx) = watch::channel(false);
	let task = scheduler::spawn(deps, stop_rx);
	tokio::time::sleep(TEST_HOLD / 2).await;
	// 猶予の間に窓を過ぎる
	*now.lock().unwrap() = AFTER_WINDOW_NOW.parse().unwrap();
	tokio::time::sleep(TEST_HOLD + SETTLE).await;

	assert!(fixture.calls().is_empty(), "窓を過ぎたら翌日に回す");
	assert!(!switch.entered.load(Ordering::SeqCst));
	stop_and_join(&stop, task).await;
	fixture.close().await;
}
