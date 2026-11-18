//! WorkerPool の試験。tests/fixtures/fake_worker.py を実際に起動して確かめる。
//! - 起動する python は support/python.rs で決める (PATH のランチャーを避けて実体を使う)
//! - 時間の判定は余裕を持たせ、可能な限り順序と記録ファイルで判定する

#[path = "support/python.rs"]
mod python;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;
use tokio::process::Command;
use tokio::task::JoinHandle;
use watapachi_bot::config::WorkerConfig;
use watapachi_bot::domain::{Priority, ResolveError, ResolveErrorCode};
use watapachi_bot::platform::job::Job;
use watapachi_bot::resolver::pool::{
	pause_duration, RollingRestartError, WorkerClient, WorkerPool,
};
use watapachi_bot::resolver::process::WorkerSpec;
use watapachi_bot::resolver::protocol::WorkerRequest;

/// 待ち合わせの上限。通常は数秒で終わる。
const WAIT_LIMIT: Duration = Duration::from_secs(30);
/// 試験全体が止まったと判断する上限。
const TEST_LIMIT: Duration = Duration::from_secs(120);

type Reply = Result<Value, ResolveError>;

/// 試験用のプールと、その記録ファイル。
struct Fixture {
	pool: Arc<WorkerPool>,
	dir: TempDir,
}

/// プールの設定。ping は既定で試験中に動かない長さにする。
fn config(workers: u32, reserved: u32, timeout_secs: u64) -> WorkerConfig {
	WorkerConfig {
		worker_count: workers,
		prefetch_reserved_workers: reserved,
		resolve_timeout_secs: timeout_secs,
		startup_timeout_secs: 60,
		ping_interval_secs: 3600,
		max_retries: 3,
		rate_limit_backoff_secs: 5,
		rate_limit_backoff_max_secs: 60,
		cookies_browser: String::new(),
		youtube_premium: "auto".to_string(),
		user_agent: String::new(),
	}
}

/// fake_worker.py の起動方法を作る。
fn spec(python: impl Into<PathBuf>, log: &Path, grandchild: bool) -> WorkerSpec {
	let mut args = vec!["--log".to_string(), log.display().to_string()];
	if grandchild {
		args.push("--grandchild".to_string());
	}
	WorkerSpec {
		python: python.into(),
		script: Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake_worker.py"),
		args,
	}
}

impl Fixture {
	/// 設定どおりにプールを起動し、全ワーカーの起動を待つ。
	async fn start(config: WorkerConfig) -> Self {
		Self::start_with(config, false).await
	}

	/// `grandchild` を指定してプールを起動し、全ワーカーの起動を待つ。
	async fn start_with(config: WorkerConfig, grandchild: bool) -> Self {
		let dir = tempfile::tempdir().expect("一時ディレクトリ");
		Self::start_in(dir, python::test_python(), config, grandchild).await
	}

	/// 一時ディレクトリと Python を指定してプールを起動し、全ワーカーの起動を待つ。
	async fn start_in(
		dir: TempDir,
		python: PathBuf,
		config: WorkerConfig,
		grandchild: bool,
	) -> Self {
		let job = Arc::new(Job::new().expect("Job を作れること"));
		let workers = config.worker_count as usize;
		let spec = spec(python, &dir.path().join("worker.log"), grandchild);
		let pool = Arc::new(WorkerPool::start(spec, &config, job));
		let fixture = Self { pool, dir };
		fixture.wait_for_starts(workers).await;
		fixture
	}

	/// fake_worker.py へ引数を足してプールを起動し、全ワーカーの起動 (start の行) を待つ。
	/// 受付に入る (起動の ping に答える) のは待たない。
	async fn start_with_args(config: WorkerConfig, extra: &[String]) -> Self {
		let dir = tempfile::tempdir().expect("一時ディレクトリ");
		let job = Arc::new(Job::new().expect("Job を作れること"));
		let workers = config.worker_count as usize;
		let mut spec = spec(python::test_python(), &dir.path().join("worker.log"), false);
		spec.args.extend(extra.iter().cloned());
		let pool = Arc::new(WorkerPool::start(spec, &config, job));
		let fixture = Self { pool, dir };
		fixture.wait_for_starts(workers).await;
		fixture
	}

	/// この試験のプロセスを見分ける文字列 (一時ディレクトリ名)。
	fn marker(&self) -> String {
		dir_marker(self.dir.path())
	}

	fn log_path(&self) -> PathBuf {
		self.dir.path().join("worker.log")
	}

	/// 全ワーカーの記録の行を返す。ファイルはプロセスごとのため、行の順序に意味は無い。
	fn log_lines(&self) -> Vec<String> {
		let entries = std::fs::read_dir(self.dir.path()).expect("一時ディレクトリを読めること");
		entries
			.filter_map(|entry| Some(entry.ok()?.path()))
			.filter(|path| {
				let name = path.file_name().unwrap_or_default().to_string_lossy();
				name.starts_with("worker.log.")
			})
			.filter_map(|path| std::fs::read_to_string(path).ok())
			.flat_map(|text| text.lines().map(str::to_string).collect::<Vec<_>>())
			.collect()
	}

	/// 記録のうち `kind` で始まる行の、空白区切りの 2 番目 (PID) を返す。
	fn pids(&self, kind: &str) -> Vec<u32> {
		self.log_lines()
			.iter()
			.filter(|line| line.split(' ').next() == Some(kind))
			.filter_map(|line| line.split(' ').nth(1)?.parse().ok())
			.collect()
	}

	/// ワーカーへ届いた要求の URL を返す (`received` の行の 3 番目)。順序に意味は無い。
	fn received(&self) -> Vec<String> {
		self.log_lines()
			.iter()
			.filter(|line| line.split(' ').next() == Some("received"))
			.filter_map(|line| line.splitn(3, ' ').nth(2).map(str::to_string))
			.collect()
	}

	/// `start` の行が `count` 件以上になるまで待つ。
	async fn wait_for_starts(&self, count: usize) {
		wait_until("ワーカーの起動", || {
			self.pids("start").len() >= count
		})
		.await;
	}

	/// 要求を別タスクで投げる。
	fn spawn(&self, url: &str, priority: Priority) -> JoinHandle<Reply> {
		let pool = self.pool.clone();
		let request = resolve(url);
		tokio::spawn(async move { pool.submit(request, priority).await })
	}

	/// 要求を投げて結果を待つ。
	async fn submit(&self, url: &str) -> Reply {
		self.pool.submit(resolve(url), Priority::Interactive).await
	}
}

/// 一時ディレクトリ名を返す。ワーカーと孫のコマンドラインに含まれる。
fn dir_marker(dir: &Path) -> String {
	dir.file_name()
		.expect("一時ディレクトリの名前")
		.to_string_lossy()
		.into_owned()
}

fn resolve(url: &str) -> WorkerRequest {
	WorkerRequest::Resolve {
		url: url.to_string(),
		format_id: None,
	}
}

fn pid_of(value: &Value) -> u64 {
	value["pid"].as_u64().expect("応答に pid がある")
}

/// 条件が真になるまで待つ。上限を超えたら失敗にする。
async fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
	let deadline = Instant::now() + WAIT_LIMIT;
	while !condition() {
		assert!(Instant::now() < deadline, "{what} を待ちきれない");
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

/// PID のプロセスが生きていて、そのコマンドラインに `marker` を含むかを確かめる。
///
/// - PID は終了後すぐ再利用される。多数のプロセスを作る試験の並行実行では、別のプロセスが
///   同じ PID を得て「終了しない」と誤判定したため、コマンドラインで同じプロセスかを見分ける
/// - `marker` は試験ごとの一時ディレクトリ名。ワーカーと孫のコマンドラインに含まれる
async fn process_exists(pid: u32, marker: &str) -> bool {
	let query = format!("(Get-CimInstance Win32_Process -Filter 'ProcessId={pid}').CommandLine");
	let output = Command::new("powershell")
		.args(["-NoProfile", "-NonInteractive", "-Command", &query])
		.stdin(Stdio::null())
		.output()
		.await
		.expect("powershell を実行できること");
	String::from_utf8_lossy(&output.stdout).contains(marker)
}

/// 列挙した PID がすべて終了するまで待つ。
async fn assert_all_gone(pids: &[u32], marker: &str) {
	let deadline = Instant::now() + WAIT_LIMIT;
	for pid in pids {
		while process_exists(*pid, marker).await {
			assert!(Instant::now() < deadline, "{pid} が終了しない");
			tokio::time::sleep(Duration::from_millis(100)).await;
		}
	}
}

/// `dir` の下に pip なしの venv を作り、その python.exe (ランチャー) のパスを返す。
async fn make_venv(dir: &Path) -> PathBuf {
	let venv = dir.join("venv");
	let status = Command::new(python::test_python())
		.args(["-m", "venv", "--without-pip"])
		.arg(&venv)
		.stdin(Stdio::null())
		.stdout(Stdio::null())
		.status()
		.await
		.expect("venv を作れること");
	assert!(status.success());
	venv.join("Scripts").join("python.exe")
}

/// 2 台へ同時に要求を投げ、それぞれの起動記録の行を返す。どの環境で動いているかを見る。
async fn start_lines_of_both_workers(fixture: &Fixture) -> Vec<String> {
	let first = fixture.spawn("slow:0.5", Priority::Interactive);
	let second = fixture.spawn("slow:0.5", Priority::Interactive);
	let first = first.await.expect("タスク").expect("成功");
	let second = second.await.expect("タスク").expect("成功");
	assert_ne!(pid_of(&first), pid_of(&second));
	let log = fixture.log_lines();
	[first, second]
		.iter()
		.map(|value| {
			let prefix = format!("start {} ", pid_of(value));
			log.iter()
				.find(|line| line.starts_with(&prefix))
				.expect("起動記録がある")
				.clone()
		})
		.collect()
}

/// 試験本体に上限を付ける。プールの不具合で試験が止まり続けるのを防ぐ。
async fn within<T>(body: impl std::future::Future<Output = T>) -> T {
	tokio::time::timeout(TEST_LIMIT, body)
		.await
		.expect("試験が上限内に終わること")
}

#[tokio::test]
async fn resolves_through_a_worker() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 30)).await;
		let value = fixture.submit("ok").await.expect("成功");
		assert_eq!(value["url"], "ok");
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn worker_error_response_maps_to_resolve_error() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 30)).await;
		let error = fixture.submit("error").await.expect_err("失敗");
		assert_eq!(
			error,
			ResolveError::Worker {
				code: ResolveErrorCode::NotFound,
				message: "none".to_string(),
			}
		);
		// エラー応答ではワーカーを再起動しない
		fixture.submit("ok").await.expect("成功");
		assert_eq!(fixture.pids("start").len(), 1);
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn spawner_sets_pythonutf8() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 30)).await;
		let value = fixture.submit("env").await.expect("成功");
		assert_eq!(value["pythonutf8"], "1");
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn stderr_flood_does_not_block_the_worker() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 20)).await;
		// パイプの容量を十分に超える量を複数回書かせる。読まなければ止まってタイムアウトになる
		for _ in 0..3 {
			fixture.submit("noisy").await.expect("成功");
		}
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn concurrency_equals_worker_count() {
	within(async {
		let fixture = Fixture::start(config(2, 0, 30)).await;
		let started = Instant::now();
		let first = fixture.spawn("slow:1", Priority::Interactive);
		let second = fixture.spawn("slow:1", Priority::Interactive);
		let first = first.await.expect("タスク").expect("成功");
		let second = second.await.expect("タスク").expect("成功");
		let elapsed = started.elapsed();
		assert_ne!(pid_of(&first), pid_of(&second), "別のワーカーで動く");
		// 直列なら 2 秒以上かかる
		assert!(elapsed >= Duration::from_millis(900), "{elapsed:?}");
		assert!(elapsed < Duration::from_millis(1900), "{elapsed:?}");
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn interactive_runs_before_queued_prefetch() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 30)).await;
		let order = Arc::new(Mutex::new(Vec::new()));
		let run = |label: &'static str, url: &str, priority| {
			let order = order.clone();
			let task = fixture.spawn(url, priority);
			tokio::spawn(async move {
				task.await.expect("タスク").expect("成功");
				order.lock().expect("ロック").push(label);
			})
		};
		let busy = run("busy", "slow:1", Priority::Interactive);
		tokio::time::sleep(Duration::from_millis(200)).await;
		let prefetch = run("prefetch", "ok", Priority::Prefetch);
		tokio::time::sleep(Duration::from_millis(200)).await;
		let interactive = run("interactive", "ok", Priority::Interactive);
		for task in [busy, prefetch, interactive] {
			task.await.expect("タスク");
		}
		assert_eq!(
			*order.lock().expect("ロック"),
			vec!["busy", "interactive", "prefetch"]
		);
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn prefetch_leaves_reserved_worker_free() {
	within(async {
		let fixture = Fixture::start(config(2, 1, 30)).await;
		let busy = fixture.spawn("slow:3", Priority::Interactive);
		tokio::time::sleep(Duration::from_millis(300)).await;
		let prefetch = fixture.spawn("ok", Priority::Prefetch);
		tokio::time::sleep(Duration::from_millis(1000)).await;
		assert!(!prefetch.is_finished(), "空き 1 では Prefetch を実行しない");
		// 残した 1 台は Interactive がすぐ使える
		fixture.submit("ok").await.expect("成功");
		assert!(!busy.is_finished(), "Interactive は slow:3 の間に終わる");
		assert!(!prefetch.is_finished(), "空き 1 では Prefetch を実行しない");
		busy.await.expect("タスク").expect("成功");
		prefetch
			.await
			.expect("タスク")
			.expect("空き 2 で実行される");
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn prefetch_is_rejected_when_reservation_cannot_be_met() {
	within(async {
		let fixture = Fixture::start(config(1, 1, 30)).await;
		let error = fixture
			.pool
			.submit(resolve("ok"), Priority::Prefetch)
			.await
			.expect_err("実行できる見込みが無ければ待たせない");
		assert_eq!(error, ResolveError::Unavailable);
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn cancelled_queued_request_is_not_executed() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 30)).await;
		let busy = fixture.spawn("slow:1", Priority::Interactive);
		tokio::time::sleep(Duration::from_millis(200)).await;
		let queued = fixture.spawn("die", Priority::Interactive);
		tokio::time::sleep(Duration::from_millis(200)).await;
		queued.abort();
		busy.await.expect("タスク").expect("成功");
		fixture.submit("ok").await.expect("成功");
		assert!(fixture.pids("die").is_empty());
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn timeout_kills_and_restarts_worker() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 1)).await;
		let first = fixture.pids("start")[0];
		let error = fixture.submit("hang").await.expect_err("失敗");
		assert_eq!(error, ResolveError::Timeout);
		// 上限が 1 秒のため、Python の起動を待ってから投げる。負荷が高いと起動だけで超える
		fixture.wait_for_starts(2).await;
		let value = fixture.submit("ok").await.expect("再起動後に成功");
		assert_ne!(pid_of(&value), u64::from(first));
		assert_all_gone(&[first], &fixture.marker()).await;
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn timeout_kill_reaches_the_whole_process_tree() {
	within(async {
		let fixture = Fixture::start_with(config(1, 0, 1), true).await;
		let worker = fixture.pids("start")[0];
		let grandchild = fixture.pids("grandchild")[0];
		assert!(process_exists(grandchild, &fixture.marker()).await);
		let error = fixture.submit("hang").await.expect_err("失敗");
		assert_eq!(error, ResolveError::Timeout);
		// Job の全体を閉じる前 (プールの稼働中) に孫まで消えていること
		assert_all_gone(&[worker, grandchild], &fixture.marker()).await;
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn dead_worker_request_is_retried_once() {
	within(async {
		let fixture = Fixture::start(config(2, 0, 30)).await;
		let marker = fixture.dir.path().join("marker");
		let url = format!("die-once:{}", marker.display());
		let value = fixture.submit(&url).await.expect("再試行で成功");
		let dead = fixture.pids("die");
		assert_eq!(dead.len(), 1);
		assert_ne!(
			pid_of(&value),
			u64::from(dead[0]),
			"別のワーカーで再試行する"
		);
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn repeated_death_reports_worker_died() {
	within(async {
		let fixture = Fixture::start(config(2, 0, 30)).await;
		let error = fixture.submit("die").await.expect_err("失敗");
		assert_eq!(error, ResolveError::WorkerDied);
		let dead = fixture.pids("die");
		assert_eq!(dead.len(), 2, "最初の 1 回と再試行の 1 回だけ");
		assert_ne!(dead[0], dead[1]);
		// どちらのワーカーも戻り、処理を続けられる
		fixture.submit("ok").await.expect("再起動後に成功");
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn timed_out_request_is_retried_once_on_another_worker() {
	within(async {
		let fixture = Fixture::start(config(2, 0, 2)).await;
		let marker = fixture.dir.path().join("marker");
		let url = format!("hang-once:{}", marker.display());
		let value = fixture.submit(&url).await.expect("別の台での再試行で成功");
		let hung = fixture.pids("hang");
		assert_eq!(hung.len(), 1);
		assert_ne!(
			pid_of(&value),
			u64::from(hung[0]),
			"別のワーカーで再試行する"
		);
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn repeated_timeout_reports_timeout() {
	within(async {
		let fixture = Fixture::start(config(2, 0, 2)).await;
		let error = fixture.submit("hang").await.expect_err("失敗");
		assert_eq!(error, ResolveError::Timeout);
		let hung = fixture.pids("hang");
		assert_eq!(hung.len(), 2, "最初の 1 回と再試行の 1 回だけ");
		assert_ne!(hung[0], hung[1]);
		fixture.pool.shutdown().await;
	})
	.await;
}

/// 長いプレイリストのタイムアウトは別の台でも同じだけかかるため、再試行しない。
#[tokio::test]
async fn timed_out_flat_playlist_is_not_retried() {
	within(async {
		let fixture = Fixture::start(config(2, 0, 2)).await;
		let request = WorkerRequest::FlatPlaylist {
			url: "hang".to_string(),
			limit: 10,
		};
		let error = fixture
			.pool
			.submit(request, Priority::Interactive)
			.await
			.expect_err("失敗");
		assert_eq!(error, ResolveError::Timeout);
		assert_eq!(fixture.pids("hang").len(), 1, "別の台では再試行しない");
		// 落とした台は再起動し、処理を続けられる
		fixture.submit("ok").await.expect("成功");
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn single_worker_retry_uses_the_restarted_worker() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 30)).await;
		let marker = fixture.dir.path().join("marker");
		let url = format!("die-once:{}", marker.display());
		fixture.submit(&url).await.expect("再起動後の再試行で成功");
		fixture.pool.shutdown().await;
	})
	.await;
}

/// 別の台が長い要求を実行中なら、再試行はその終わりを待たずに、再起動した同じ台で行う。
#[tokio::test]
async fn retry_uses_the_restarted_worker_while_the_other_is_busy() {
	within(async {
		let fixture = Fixture::start(config(2, 0, 60)).await;
		// 再起動 (Python の起動) より十分に長くする。終わりを待つ割り当てなら、再試行はこの後になる
		let busy = fixture.spawn("slow:20", Priority::Prefetch);
		wait_until("長い要求の受け付け", || {
			fixture.received().iter().any(|url| url == "slow:20")
		})
		.await;
		let marker = fixture.dir.path().join("marker");
		let url = format!("die-once:{}", marker.display());
		let value = fixture.submit(&url).await.expect("再起動後の再試行で成功");
		assert!(!busy.is_finished(), "再試行は実行中の台の終わりを待たない");
		let dead = fixture.pids("die");
		assert_eq!(dead.len(), 1);
		assert_ne!(pid_of(&value), u64::from(dead[0]), "新しいプロセスで行う");
		busy.await.expect("タスク").expect("成功");
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn garbage_line_restarts_only_that_worker() {
	within(async {
		let fixture = Fixture::start(config(2, 0, 30)).await;
		let other = fixture.spawn("slow:2", Priority::Interactive);
		tokio::time::sleep(Duration::from_millis(300)).await;
		let error = fixture.submit("garbage").await.expect_err("失敗");
		assert!(
			matches!(
				error,
				ResolveError::Worker {
					code: ResolveErrorCode::Internal,
					..
				}
			),
			"{error:?}"
		);
		let other = other.await.expect("タスク").expect("他方は影響を受けない");
		fixture.wait_for_starts(3).await;
		tokio::time::sleep(Duration::from_millis(300)).await;
		let starts = fixture.pids("start");
		assert_eq!(starts.len(), 3, "再起動は 1 台だけ");
		assert!(starts.contains(&(pid_of(&other) as u32)));
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn huge_response_line_is_received() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 30)).await;
		let started = Instant::now();
		let value = match fixture.submit("huge").await {
			Ok(value) => value,
			// ワーカー側の書き込みの記録と合わせて出し、どちらで止まったかを分かるようにする
			Err(error) => panic!(
				"{error:?} after {:?}; worker log: {:?}",
				started.elapsed(),
				fixture.log_lines()
			),
		};
		let data = value["data"].as_str().expect("文字列");
		assert_eq!(data.len(), 10 * 1024 * 1024);
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn idle_worker_death_is_detected_by_eof() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 30)).await;
		fixture.submit("exit-after-reply").await.expect("成功");
		// 要求を投げなくても再起動する
		fixture.wait_for_starts(2).await;
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn ping_detects_a_hung_idle_worker() {
	within(async {
		// 上限を短くしすぎない。起動中のワーカーも ping の対象で、間隔と上限の合計より
		// Python の起動が遅いと、記録を残す前に落とされ続ける
		let mut settings = config(1, 0, 5);
		settings.ping_interval_secs = 1;
		let fixture = Fixture::start(settings).await;
		let first = fixture.pids("start")[0];
		let value = fixture.submit("freeze").await.expect("成功");
		assert_eq!(pid_of(&value), u64::from(first));
		fixture.wait_for_starts(2).await;
		assert_all_gone(&[first], &fixture.marker()).await;
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn rolling_restart_keeps_serving() {
	within(async {
		let fixture = Fixture::start(config(2, 0, 30)).await;
		let old = fixture.pids("start");
		let running = fixture.spawn("slow:2", Priority::Interactive);
		tokio::time::sleep(Duration::from_millis(300)).await;

		let pool = fixture.pool.clone();
		let restart = tokio::spawn(async move { pool.rolling_restart(None).await });
		while !restart.is_finished() {
			fixture.submit("ok").await.expect("入れ替え中も成功");
			tokio::time::sleep(Duration::from_millis(50)).await;
		}
		restart.await.expect("タスク").expect("入れ替えに成功");
		running
			.await
			.expect("タスク")
			.expect("実行中の要求は失敗しない");

		assert_eq!(
			fixture.pids("start").len(),
			4,
			"2 台とも 1 回ずつ入れ替わる"
		);
		assert_all_gone(&old, &fixture.marker()).await;
		fixture.submit("ok").await.expect("入れ替え後も成功");
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn rolling_restart_switches_python_path() {
	within(async {
		let fixture = Fixture::start(config(2, 0, 30)).await;
		let venv_python = make_venv(fixture.dir.path()).await;

		let before = fixture
			.pool
			.submit(WorkerRequest::Ping, Priority::Interactive)
			.await
			.expect("成功");
		assert!(!before["prefix"].as_str().expect("文字列").ends_with("venv"));

		let new_spec = spec(venv_python, &fixture.log_path(), false);
		fixture
			.pool
			.rolling_restart(Some(new_spec))
			.await
			.expect("入れ替えに成功");

		// 2 台とも新しい環境で動いている
		for line in start_lines_of_both_workers(&fixture).await {
			assert!(line.ends_with("venv"), "{line}");
		}
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn rolling_restart_failure_rolls_every_worker_back_to_the_old_python() {
	within(async {
		let fixture = Fixture::start(config(2, 0, 30)).await;
		let venv_python = make_venv(fixture.dir.path()).await;
		// 1 台目は marker を作って動き、2 台目は marker を見て起動直後に終了する (ping に答えない)
		let mut new_spec = spec(venv_python, &fixture.log_path(), false);
		new_spec.args.push("--exit-if-exists".to_string());
		new_spec
			.args
			.push(fixture.dir.path().join("marker").display().to_string());

		let error = fixture
			.pool
			.rolling_restart(Some(new_spec))
			.await
			.expect_err("2 台目が ping に答えない");
		assert_eq!(
			error,
			RollingRestartError::Failed {
				slot: 1,
				not_restored: vec![],
			}
		);

		// 新しい環境で動いたワーカーは残っていない
		let venv_pids: Vec<u32> = fixture
			.log_lines()
			.iter()
			.filter(|line| line.starts_with("start ") && line.ends_with("venv"))
			.filter_map(|line| line.split(' ').nth(1)?.parse().ok())
			.collect();
		assert_eq!(venv_pids.len(), 1, "新しい環境で起動したのは 1 台目だけ");
		assert_all_gone(&venv_pids, &fixture.marker()).await;
		// 2 台とも元の環境で動いている
		for line in start_lines_of_both_workers(&fixture).await {
			assert!(!line.ends_with("venv"), "{line}");
		}
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn timeout_kill_reaches_the_whole_tree_under_the_venv_launcher() {
	within(async {
		let dir = tempfile::tempdir().expect("一時ディレクトリ");
		let venv_python = make_venv(dir.path()).await;
		let fixture = Fixture::start_in(dir, venv_python, config(1, 0, 1), true).await;
		let worker = fixture.pids("start")[0];
		let grandchild = fixture.pids("grandchild")[0];
		assert!(process_exists(worker, &fixture.marker()).await);
		assert!(process_exists(grandchild, &fixture.marker()).await);
		let error = fixture.submit("hang").await.expect_err("失敗");
		assert_eq!(error, ResolveError::Timeout);
		// venv のランチャーの下の実体と孫まで、プールの稼働中に消えていること
		assert_all_gone(&[worker, grandchild], &fixture.marker()).await;
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn idle_ping_does_not_hold_up_a_request() {
	within(async {
		let mut settings = config(2, 0, 8);
		settings.ping_interval_secs = 1;
		let fixture = Fixture::start(settings).await;
		// 番号の小さい台が応答後に止まる。以後この台への ping は上限の 8 秒まで返らない
		fixture.submit("freeze").await.expect("成功");
		tokio::time::sleep(Duration::from_millis(1500)).await;
		// ping 中の台を空きと見なすと、この要求は ping の上限まで待たされる
		let started = Instant::now();
		fixture.submit("ok").await.expect("成功");
		let elapsed = started.elapsed();
		assert!(elapsed < Duration::from_secs(4), "{elapsed:?}");
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn rolling_restart_failure_keeps_the_old_spec() {
	within(async {
		let fixture = Fixture::start(config(2, 0, 30)).await;
		let bad = spec("no-such-python-executable", &fixture.log_path(), false);
		let error = fixture
			.pool
			.rolling_restart(Some(bad))
			.await
			.expect_err("起動できない環境へは切り替えない");
		assert_eq!(
			error,
			RollingRestartError::Failed {
				slot: 0,
				not_restored: vec![],
			}
		);
		fixture.submit("ok").await.expect("残りのワーカーで成功");
		// 失敗した台も旧環境で戻る
		fixture.wait_for_starts(3).await;
		fixture
			.pool
			.rolling_restart(None)
			.await
			.expect("元の環境に戻っているので成功");
		fixture.pool.shutdown().await;
	})
	.await;
}

/// 入れ替えの途中で死んで再起動した台は、すでに新しい起動方法で動いているため入れ替え直さない。
#[tokio::test]
async fn rolling_restart_skips_a_worker_restarted_during_the_rollout() {
	within(async {
		let fixture = Fixture::start(config(2, 0, 30)).await;
		// 2 台とも受付に入れ、空きにする。次の要求は番号の小さい台 (0 番) へ割り当てられる
		start_lines_of_both_workers(&fixture).await;
		tokio::time::sleep(Duration::from_millis(200)).await;
		// 0 番を長い要求で埋める。0 番の入れ替えはこの要求の終わりを待つ
		let busy = fixture.spawn("slow:4", Priority::Interactive);
		wait_until("長い要求の受け付け", || {
			fixture.received().iter().any(|url| url == "slow:4")
		})
		.await;
		let pool = fixture.pool.clone();
		let restart = tokio::spawn(async move { pool.rolling_restart(None).await });
		// 新しい世代が決まってから、1 番を死なせる。1 番は新しい起動方法で再起動する
		tokio::time::sleep(Duration::from_millis(300)).await;
		let marker = fixture.dir.path().join("marker");
		let url = format!("die-once:{}", marker.display());
		fixture.submit(&url).await.expect("再起動後の再試行で成功");
		restart.await.expect("タスク").expect("入れ替えに成功");
		busy.await
			.expect("タスク")
			.expect("実行中の要求は失敗しない");
		assert_eq!(fixture.pids("die").len(), 1);
		// 最初の 2 台、死んだ 1 番の再起動、0 番の入れ替えの 4 回。1 番は入れ替え直さない
		assert_eq!(fixture.pids("start").len(), 4);
		fixture.submit("ok").await.expect("入れ替え後も成功");
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn rolling_restart_rejects_a_worker_that_does_not_answer() {
	within(async {
		let fixture = Fixture::start(config(2, 0, 30)).await;
		let mut bad = spec(python::test_python(), &fixture.log_path(), false);
		bad.script = fixture.dir.path().join("missing_script.py");
		let error = fixture
			.pool
			.rolling_restart(Some(bad))
			.await
			.expect_err("ping に答えない環境へは切り替えない");
		assert_eq!(
			error,
			RollingRestartError::Failed {
				slot: 0,
				not_restored: vec![],
			}
		);
		fixture.submit("ok").await.expect("残りのワーカーで成功");
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn shutdown_leaves_no_process() {
	within(async {
		let fixture = Fixture::start_with(config(2, 0, 30), true).await;
		wait_until("孫の記録", || fixture.pids("grandchild").len() >= 2).await;
		let mut pids = fixture.pids("start");
		pids.extend(fixture.pids("grandchild"));
		assert_eq!(pids.len(), 4);
		fixture.pool.shutdown().await;
		assert_all_gone(&pids, &fixture.marker()).await;
	})
	.await;
}

#[tokio::test]
async fn shutdown_finishes_with_requests_in_flight_and_queued() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 3600)).await;
		let pids = fixture.pids("start");
		let running = fixture.spawn("hang", Priority::Interactive);
		tokio::time::sleep(Duration::from_millis(300)).await;
		let queued = [
			fixture.spawn("ok", Priority::Interactive),
			fixture.spawn("ok", Priority::Prefetch),
		];
		tokio::time::sleep(Duration::from_millis(100)).await;

		let started = Instant::now();
		fixture.pool.shutdown().await;
		assert!(started.elapsed() < Duration::from_secs(15));

		let error = running.await.expect("タスク").expect_err("失敗");
		assert_eq!(error, ResolveError::Unavailable);
		for task in queued {
			let error = task.await.expect("タスク").expect_err("失敗");
			assert_eq!(error, ResolveError::Unavailable);
		}
		assert_all_gone(&pids, &fixture.marker()).await;

		// 終了後の要求と 2 回目の終了も止まらない
		let error = fixture.submit("ok").await.expect_err("失敗");
		assert_eq!(error, ResolveError::Unavailable);
		fixture.pool.shutdown().await;
		let error = fixture
			.pool
			.rolling_restart(None)
			.await
			.expect_err("終了後は入れ替えない");
		assert_eq!(error, RollingRestartError::Stopped);
	})
	.await;
}

#[tokio::test]
async fn spawn_failure_does_not_leave_requests_waiting() {
	within(async {
		let dir = tempfile::tempdir().expect("一時ディレクトリ");
		let job = Arc::new(Job::new().expect("Job を作れること"));
		let spec = spec("no-such-python-executable", &dir.path().join("log"), false);
		let pool = WorkerPool::start(spec, &config(2, 1, 30), job);
		for priority in [Priority::Interactive, Priority::Prefetch] {
			let error = pool
				.submit(resolve("ok"), priority)
				.await
				.expect_err("失敗");
			assert_eq!(error, ResolveError::Unavailable);
		}
		pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn worker_receives_the_request_budget() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 30)).await;
		wait_until("持ち時間の記録", || {
			fixture
				.log_lines()
				.iter()
				.any(|line| line.starts_with("budget "))
		})
		.await;
		let budgets: Vec<String> = fixture
			.log_lines()
			.iter()
			.filter(|line| line.starts_with("budget "))
			.filter_map(|line| line.split(' ').nth(2).map(str::to_string))
			.collect();
		let expected = 30 - watapachi_bot::resolver::pool::REQUEST_BUDGET_MARGIN_SECS;
		assert_eq!(budgets, vec![expected.to_string()]);
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn request_waits_for_the_startup_ping() {
	within(async {
		// 準備に 3 秒かかるワーカーへ、上限 1 秒の要求を送る。準備の途中で送ると時間切れになる
		let delay = ["--start-delay".to_string(), "3".to_string()];
		let fixture = Fixture::start_with_args(config(1, 0, 1), &delay).await;
		let first = fixture.pids("start")[0];
		let value = fixture.submit("ok").await.expect("準備を終えてから送る");
		assert_eq!(pid_of(&value), u64::from(first));
		assert_eq!(fixture.pids("start").len(), 1, "落として起動し直さない");
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn worker_missing_the_startup_ping_is_treated_as_a_failed_start() {
	within(async {
		let mut settings = config(1, 0, 30);
		settings.startup_timeout_secs = 1;
		let delay = ["--start-delay".to_string(), "60".to_string()];
		let fixture = Fixture::start_with_args(settings, &delay).await;
		let first = fixture.pids("start")[0];
		// 起動の失敗と同じく、動く台が無ければ待ち行列の要求を待たせ続けない
		let error = fixture.submit("ok").await.expect_err("失敗");
		assert_eq!(error, ResolveError::Unavailable);
		// 落として、待ち時間の後に起動し直す
		assert_all_gone(&[first], &fixture.marker()).await;
		fixture.wait_for_starts(2).await;
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn dropping_the_pool_stops_workers() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 30)).await;
		let pids = fixture.pids("start");
		let marker = fixture.marker();
		let Fixture { pool, dir } = fixture;
		drop(pool);
		assert_all_gone(&pids, &marker).await;
		drop(dir);
	})
	.await;
}

/// rate_limited の失敗であることを確かめる。
fn assert_rate_limited(reply: Reply) {
	match reply {
		Err(ResolveError::Worker {
			code: ResolveErrorCode::RateLimited,
			..
		}) => {}
		other => panic!("rate_limited の失敗ではない: {other:?}"),
	}
}

/// `Paused` の再開の時刻を取り出す。
fn retry_at_of(reply: Reply) -> tokio::time::Instant {
	match reply {
		Err(ResolveError::Paused { retry_at }) => retry_at,
		other => panic!("Paused ではない: {other:?}"),
	}
}

/// 再開の時刻が、停止を起こした要求の前後の時刻に `pause` を足した範囲に収まることを確かめる。
fn assert_retry_at(
	retry_at: tokio::time::Instant,
	before: tokio::time::Instant,
	after: tokio::time::Instant,
	pause: Duration,
) {
	assert!(
		retry_at >= before + pause && retry_at <= after + pause,
		"再開の時刻が {pause:?} 後でない: before からの差 {:?}、after からの差 {:?}",
		retry_at.saturating_duration_since(before),
		retry_at.saturating_duration_since(after),
	);
}

#[test]
fn pause_duration_doubles_up_to_max() {
	let (base, max) = (Duration::from_secs(5), Duration::from_secs(60));
	assert_eq!(pause_duration(1, base, max), Duration::from_secs(5));
	assert_eq!(pause_duration(2, base, max), Duration::from_secs(10));
	assert_eq!(pause_duration(5, base, max), Duration::from_secs(60));
	// 桁あふれしない
	assert_eq!(pause_duration(40, base, max), Duration::from_secs(60));
	assert_eq!(pause_duration(u32::MAX, base, max), Duration::from_secs(60));
}

#[tokio::test]
async fn rate_limited_pauses_the_pool() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 30)).await;
		assert!(!fixture.pool.is_paused());
		assert_eq!(fixture.pool.paused_until(), None);
		let before = tokio::time::Instant::now();
		assert_rate_limited(fixture.submit("limited").await);
		let after = tokio::time::Instant::now();
		assert!(fixture.pool.is_paused());

		let retry_at = retry_at_of(fixture.submit("ok").await);
		assert_retry_at(retry_at, before, after, Duration::from_secs(5));
		assert_eq!(fixture.pool.paused_until(), Some(retry_at));
		// 先読みも同じく止まる
		let prefetch = fixture.pool.submit(resolve("ok"), Priority::Prefetch).await;
		assert_eq!(prefetch, Err(ResolveError::Paused { retry_at }));
		assert_eq!(fixture.received(), vec!["limited"], "止めた後は送らない");
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn consecutive_rate_limited_doubles_the_pause() {
	within(async {
		let mut settings = config(1, 0, 30);
		settings.rate_limit_backoff_secs = 1;
		let fixture = Fixture::start(settings).await;
		assert_rate_limited(fixture.submit("limited").await);
		let first = fixture.pool.paused_until().expect("一時停止中");
		tokio::time::sleep_until(first).await;
		assert!(!fixture.pool.is_paused(), "期限で明ける");

		let before = tokio::time::Instant::now();
		assert_rate_limited(fixture.submit("limited").await);
		let after = tokio::time::Instant::now();
		let second = fixture.pool.paused_until().expect("一時停止中");
		assert_retry_at(second, before, after, Duration::from_secs(2));
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn pause_ends_and_success_resets_strikes() {
	within(async {
		let mut settings = config(1, 0, 30);
		settings.rate_limit_backoff_secs = 1;
		let fixture = Fixture::start(settings).await;
		assert_rate_limited(fixture.submit("limited").await);
		let first = fixture.pool.paused_until().expect("一時停止中");
		tokio::time::sleep_until(first).await;
		assert!(!fixture.pool.is_paused(), "期限で明ける");
		fixture.submit("ok").await.expect("明けた後は送る");

		let before = tokio::time::Instant::now();
		assert_rate_limited(fixture.submit("limited").await);
		let after = tokio::time::Instant::now();
		let second = fixture.pool.paused_until().expect("一時停止中");
		// 成功で strikes が 0 に戻ったため、2 秒にならない
		assert_retry_at(second, before, after, Duration::from_secs(1));
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn concurrent_rate_limited_counts_once() {
	within(async {
		let fixture = Fixture::start(config(2, 0, 30)).await;
		let before = tokio::time::Instant::now();
		let first = fixture.spawn("slow-limited:1", Priority::Interactive);
		let second = fixture.spawn("slow-limited:1", Priority::Interactive);
		assert_rate_limited(first.await.expect("タスク"));
		assert_rate_limited(second.await.expect("タスク"));
		let after = tokio::time::Instant::now();
		assert_eq!(fixture.received().len(), 2, "2 件とも停止の前に送った");
		let retry_at = fixture.pool.paused_until().expect("一時停止中");
		// 2 件目は停止中に届いたため数えない。数えると 10 秒になる
		assert_retry_at(retry_at, before, after, Duration::from_secs(5));
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn ping_is_not_paused() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 30)).await;
		assert_rate_limited(fixture.submit("limited").await);
		assert!(fixture.pool.is_paused());
		fixture
			.pool
			.submit(WorkerRequest::Ping, Priority::Interactive)
			.await
			.expect("ping は止めない");
		fixture
			.pool
			.submit(WorkerRequest::Ping, Priority::Prefetch)
			.await
			.expect("ping は止めない");
		fixture.pool.shutdown().await;
	})
	.await;
}

#[tokio::test]
async fn queued_request_is_rejected_when_pause_begins() {
	within(async {
		let fixture = Fixture::start(config(1, 0, 30)).await;
		let limited = fixture.spawn("slow-limited:2", Priority::Interactive);
		wait_until("slow-limited の受信", || !fixture.received().is_empty()).await;
		// 唯一の台が使用中のため待ち行列に入る
		let queued = fixture.spawn("ok", Priority::Interactive);
		let prefetch = fixture.spawn("ok", Priority::Prefetch);
		assert_rate_limited(limited.await.expect("タスク"));
		let retry_at = retry_at_of(queued.await.expect("タスク"));
		assert_eq!(
			prefetch.await.expect("タスク"),
			Err(ResolveError::Paused { retry_at })
		);
		assert_eq!(
			fixture.received(),
			vec!["slow-limited:2"],
			"待っていた要求を送らない"
		);
		fixture.pool.shutdown().await;
	})
	.await;
}
