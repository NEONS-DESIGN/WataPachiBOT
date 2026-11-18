//! yt-dlp の更新の手順 (`Updater::run`) の試験。
//!
//! - 環境の作成・版の読み取り・検査・入れ替えは偽物に置き換え、記録 (`current.json`) とディレクトリの変化を確かめる
//! - 本物の uv を使う試験 (`real_uv_creates_and_checks`) は `#[ignore]`。ネットワークが要る

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tempfile::TempDir;
use watapachi_bot::resolver::pool::RollingRestartError;
use watapachi_bot::update::envs::{EnvError, EnvRecord, EnvStore, CURRENT_FILE};
use watapachi_bot::update::procedure::{
	EnvTools, UpdateError, UpdateOutcome, UpdateStage, Updater, WorkerSwitch,
};

#[path = "support/python.rs"]
mod python;

/// 古い環境の名前。
const OLD: &str = "old";
/// 古い環境の版。
const OLD_VERSION: &str = "2026.11.15";
/// 新しい環境の版。
const NEW_VERSION: &str = "2026.11.17";
/// 試験の現在時刻。環境の名前は `20261117-040000` になる。
const NOW: &str = "2026-11-17T04:00:00+09:00[Asia/Tokyo]";
/// 現在時刻から決まる新しい環境の名前。
const NEW_NAME: &str = "20261117-040000";

/// 偽の `EnvTools` が記録する呼び出し。
type Calls = Arc<Mutex<Vec<String>>>;

/// 偽の `EnvTools`。版と検査の成否を指定し、呼ばれた順を記録する。
struct FakeTools {
	version: String,
	check: Result<(), String>,
	/// 作成の代わりに眠る時間 (None なら作る)
	create_sleep: Option<Duration>,
	calls: Calls,
}

impl FakeTools {
	/// 版を指定して作る。検査は成功する。
	fn new(version: &str) -> (Self, Calls) {
		let calls: Calls = Arc::default();
		let tools = Self {
			version: version.to_owned(),
			check: Ok(()),
			create_sleep: None,
			calls: Arc::clone(&calls),
		};
		(tools, calls)
	}
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
		Ok(self.version.clone())
	}

	async fn check(&self, _python: &Path) -> Result<(), UpdateError> {
		self.calls.lock().unwrap().push("check".to_owned());
		self.check.clone().map_err(UpdateError::Check)
	}
}

/// 偽の `WorkerSwitch`。返す結果を指定し、渡された python を記録する。
struct FakeSwitch {
	result: Result<(), RollingRestartError>,
	seen: Mutex<Vec<Option<PathBuf>>>,
}

impl FakeSwitch {
	/// 返す結果を指定して作る。
	fn new(result: Result<(), RollingRestartError>) -> Self {
		Self {
			result,
			seen: Mutex::default(),
		}
	}

	/// 渡された python の履歴。
	fn seen(&self) -> Vec<Option<PathBuf>> {
		self.seen.lock().unwrap().clone()
	}
}

#[async_trait]
impl WorkerSwitch for FakeSwitch {
	async fn switch(&self, python: Option<PathBuf>) -> Result<(), RollingRestartError> {
		self.seen.lock().unwrap().push(python);
		self.result.clone()
	}
}

/// 置き場を作り、古い環境 `old` と、それを指す `current.json` を置く。
fn setup_with_old() -> (TempDir, EnvStore) {
	let dir = TempDir::new().unwrap();
	let store = EnvStore::new(dir.path());
	let python = store.python_of(OLD);
	fs::create_dir_all(python.parent().unwrap()).unwrap();
	fs::write(&python, b"x").unwrap();
	store.write_current(&old_record(&store)).unwrap();
	(dir, store)
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

/// 現在時刻。
fn now() -> jiff::ZonedDateTime {
	NOW.parse().unwrap()
}

/// 環境のディレクトリが在るか。
fn exists(store: &EnvStore, name: &str) -> bool {
	store.python_of(name).exists()
}

#[tokio::test]
async fn update_switches_and_records() {
	let (_dir, store) = setup_with_old();
	let (tools, calls) = FakeTools::new(NEW_VERSION);
	let switch = FakeSwitch::new(Ok(()));
	let updater = Updater::new(store.clone(), Arc::new(tools), true);

	let outcome = updater.run(Some(&switch), now()).await;

	assert_eq!(
		outcome,
		UpdateOutcome::Updated {
			version: NEW_VERSION.to_owned()
		}
	);
	let record = store.read_current().unwrap().unwrap();
	assert_eq!(record.name, NEW_NAME);
	assert_eq!(record.python, store.python_of(NEW_NAME));
	assert_eq!(record.yt_dlp_version, NEW_VERSION);
	assert_eq!(record.previous.as_deref(), Some(OLD));
	assert_eq!(record.created_at, now().timestamp().as_second());
	assert_eq!(switch.seen(), vec![Some(store.python_of(NEW_NAME))]);
	assert_eq!(*calls.lock().unwrap(), ["create", "version", "check"]);
	assert!(exists(&store, OLD) && exists(&store, NEW_NAME));
}

#[tokio::test]
async fn update_cleans_up_generations_older_than_previous() {
	let (_dir, store) = setup_with_old();
	let ancient = "ancient";
	fs::create_dir_all(store.python_of(ancient).parent().unwrap()).unwrap();
	fs::write(store.python_of(ancient), b"x").unwrap();
	let (tools, _calls) = FakeTools::new(NEW_VERSION);
	let switch = FakeSwitch::new(Ok(()));

	Updater::new(store.clone(), Arc::new(tools), true)
		.run(Some(&switch), now())
		.await;

	assert!(!exists(&store, ancient));
	assert!(exists(&store, OLD));
}

#[tokio::test]
async fn same_version_recycles_and_discards() {
	let (_dir, store) = setup_with_old();
	let (tools, calls) = FakeTools::new(OLD_VERSION);
	let switch = FakeSwitch::new(Ok(()));
	let updater = Updater::new(store.clone(), Arc::new(tools), true);

	let outcome = updater.run(Some(&switch), now()).await;

	assert_eq!(
		outcome,
		UpdateOutcome::Unchanged {
			version: OLD_VERSION.to_owned(),
			recycled: true
		}
	);
	assert!(!store.python_of(NEW_NAME).parent().unwrap().exists());
	assert_eq!(switch.seen(), vec![None]);
	assert_eq!(store.read_current().unwrap().unwrap(), old_record(&store));
	assert_eq!(*calls.lock().unwrap(), ["create", "version"]);
}

#[tokio::test]
async fn same_version_without_recycle_does_not_switch() {
	let (_dir, store) = setup_with_old();
	let (tools, _calls) = FakeTools::new(OLD_VERSION);
	let switch = FakeSwitch::new(Ok(()));
	let updater = Updater::new(store.clone(), Arc::new(tools), false);

	let outcome = updater.run(Some(&switch), now()).await;

	assert_eq!(
		outcome,
		UpdateOutcome::Unchanged {
			version: OLD_VERSION.to_owned(),
			recycled: false
		}
	);
	assert!(switch.seen().is_empty());
	assert!(!store.python_of(NEW_NAME).parent().unwrap().exists());
}

#[tokio::test]
async fn recycle_failure_is_only_logged() {
	let (_dir, store) = setup_with_old();
	let (tools, _calls) = FakeTools::new(OLD_VERSION);
	let switch = FakeSwitch::new(Err(RollingRestartError::Failed {
		slot: 0,
		not_restored: vec![],
	}));
	let updater = Updater::new(store.clone(), Arc::new(tools), true);

	let outcome = updater.run(Some(&switch), now()).await;

	assert!(matches!(outcome, UpdateOutcome::Unchanged { .. }));
	assert_eq!(switch.seen(), vec![None]);
}

#[tokio::test]
async fn check_failure_keeps_old_env() {
	let (_dir, store) = setup_with_old();
	let (mut tools, _calls) = FakeTools::new(NEW_VERSION);
	tools.check = Err("ping に答えない".to_owned());
	let switch = FakeSwitch::new(Ok(()));
	let updater = Updater::new(store.clone(), Arc::new(tools), true);

	let outcome = updater.run(Some(&switch), now()).await;

	match outcome {
		UpdateOutcome::Failed { stage, message } => {
			assert_eq!(stage, UpdateStage::Check);
			assert!(message.contains("ping に答えない"), "{message}");
		}
		other => panic!("unexpected: {other:?}"),
	}
	assert!(!store.python_of(NEW_NAME).parent().unwrap().exists());
	assert_eq!(store.read_current().unwrap().unwrap(), old_record(&store));
	assert!(switch.seen().is_empty());
	// 片付けをしないため、現役でも直前でもない古い環境 a が残る
	let stale = "a";
	let (_dir2, store2) = setup_with_old();
	fs::create_dir_all(store2.python_of(stale).parent().unwrap()).unwrap();
	fs::write(store2.python_of(stale), b"x").unwrap();
	let (mut tools2, _calls2) = FakeTools::new(NEW_VERSION);
	tools2.check = Err("x".to_owned());
	Updater::new(store2.clone(), Arc::new(tools2), true)
		.run(Some(&switch), now())
		.await;
	assert!(exists(&store2, stale));
}

#[tokio::test]
async fn switch_failure_not_restored_keeps_new_env() {
	let (_dir, store) = setup_with_old();
	let (tools, _calls) = FakeTools::new(NEW_VERSION);
	let switch = FakeSwitch::new(Err(RollingRestartError::Failed {
		slot: 1,
		not_restored: vec![1],
	}));
	let updater = Updater::new(store.clone(), Arc::new(tools), true);

	let outcome = updater.run(Some(&switch), now()).await;

	assert!(matches!(
		outcome,
		UpdateOutcome::Failed {
			stage: UpdateStage::Switch,
			..
		}
	));
	assert!(exists(&store, NEW_NAME));
	assert_eq!(store.read_current().unwrap().unwrap(), old_record(&store));
}

#[tokio::test]
async fn switch_failure_restored_discards_new_env() {
	let (_dir, store) = setup_with_old();
	let (tools, _calls) = FakeTools::new(NEW_VERSION);
	let switch = FakeSwitch::new(Err(RollingRestartError::Failed {
		slot: 1,
		not_restored: vec![],
	}));
	let updater = Updater::new(store.clone(), Arc::new(tools), true);

	let outcome = updater.run(Some(&switch), now()).await;

	assert!(matches!(
		outcome,
		UpdateOutcome::Failed {
			stage: UpdateStage::Switch,
			..
		}
	));
	assert!(!store.python_of(NEW_NAME).parent().unwrap().exists());
	assert_eq!(store.read_current().unwrap().unwrap(), old_record(&store));
}

#[tokio::test]
async fn stopped_leaves_everything() {
	let (_dir, store) = setup_with_old();
	let (tools, _calls) = FakeTools::new(NEW_VERSION);
	let switch = FakeSwitch::new(Err(RollingRestartError::Stopped));
	let updater = Updater::new(store.clone(), Arc::new(tools), true);

	let outcome = updater.run(Some(&switch), now()).await;

	assert_eq!(outcome, UpdateOutcome::Stopped);
	assert!(exists(&store, NEW_NAME));
	assert_eq!(store.read_current().unwrap().unwrap(), old_record(&store));
}

#[tokio::test]
async fn first_update_without_record() {
	let dir = TempDir::new().unwrap();
	let store = EnvStore::new(dir.path());
	let (tools, calls) = FakeTools::new(NEW_VERSION);
	let updater = Updater::new(store.clone(), Arc::new(tools), true);

	let outcome = updater.run(None, now()).await;

	assert_eq!(
		outcome,
		UpdateOutcome::Updated {
			version: NEW_VERSION.to_owned()
		}
	);
	let record = store.read_current().unwrap().unwrap();
	assert_eq!(record.name, NEW_NAME);
	assert_eq!(record.previous, None);
	assert_eq!(*calls.lock().unwrap(), ["create", "version", "check"]);
}

#[tokio::test]
async fn corrupt_record_is_rewritten() {
	let dir = TempDir::new().unwrap();
	let store = EnvStore::new(dir.path());
	fs::write(dir.path().join(CURRENT_FILE), b"{").unwrap();
	assert!(matches!(store.read_current(), Err(EnvError::Corrupt(_))));
	let (tools, _calls) = FakeTools::new(NEW_VERSION);
	let updater = Updater::new(store.clone(), Arc::new(tools), true);

	let outcome = updater.run(None, now()).await;

	assert!(matches!(outcome, UpdateOutcome::Updated { .. }));
	let record = store.read_current().unwrap().unwrap();
	assert_eq!(record.name, NEW_NAME);
	assert_eq!(record.previous, None);
}

#[tokio::test]
async fn record_with_missing_python_is_repaired() {
	let (dir, store) = setup_with_old();
	// 記録は読めるが、指す python が無い (起動時は paths.worker_python で動いている)
	fs::remove_file(store.python_of(OLD)).unwrap();
	// 版が記録と同じでも、比べずに検査と入れ替えへ進む
	let (tools, calls) = FakeTools::new(OLD_VERSION);
	let switch = FakeSwitch::new(Ok(()));
	let updater = Updater::new(store.clone(), Arc::new(tools), true);

	let outcome = updater.run(Some(&switch), now()).await;

	assert_eq!(
		outcome,
		UpdateOutcome::Updated {
			version: OLD_VERSION.to_owned()
		}
	);
	assert_eq!(*calls.lock().unwrap(), ["create", "version", "check"]);
	assert_eq!(switch.seen(), vec![Some(store.python_of(NEW_NAME))]);
	let record = store.read_current().unwrap().unwrap();
	assert_eq!(record.name, NEW_NAME);
	assert_eq!(record.python, store.python_of(NEW_NAME));
	assert_eq!(record.yt_dlp_version, OLD_VERSION);
	// 壊れた環境は戻り先にしない。previous に無いため片付けで消える
	assert_eq!(record.previous, None);
	assert!(!dir.path().join(OLD).exists());
}

#[tokio::test]
async fn dropping_run_during_create_leaves_record() {
	let (_dir, store) = setup_with_old();
	let (mut tools, _calls) = FakeTools::new(NEW_VERSION);
	tools.create_sleep = Some(Duration::from_secs(3600));
	let switch = FakeSwitch::new(Ok(()));
	let updater = Updater::new(store.clone(), Arc::new(tools), true);

	let result = tokio::time::timeout(
		Duration::from_millis(100),
		updater.run(Some(&switch), now()),
	)
	.await;

	assert!(result.is_err(), "打ち切られるはず");
	assert_eq!(store.read_current().unwrap().unwrap(), old_record(&store));
	assert!(switch.seen().is_empty());
}

/// 本物の uv で環境を作り、yt-dlp の版と検査 (ping と解決) まで通す。ネットワークが要る。
///
/// 実行: `cargo test --test update_procedure real_uv -- --ignored --nocapture`
#[tokio::test]
#[ignore = "uv とネットワークが要る"]
async fn real_uv_creates_and_checks() {
	use watapachi_bot::config::Config;
	use watapachi_bot::platform::job::Job;
	use watapachi_bot::update::tools::UvTools;

	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut config = Config::load(&root.join("config.example.toml")).unwrap();
	let dir = TempDir::new().unwrap();
	config.paths.worker_envs = dir.path().to_path_buf();
	config.paths.worker_script = root.join("worker/worker.py");
	config.paths.ytdlp_cache = dir.path().join("cache");
	// Cookie と Premium の判定はこの検査に要らない。開発機の Firefox に依存させない
	config.worker.cookies_browser = String::new();
	let spec = watapachi_bot::app::worker_spec(&config, &python::test_python());
	let job = Arc::new(Job::new().unwrap());
	let tools = UvTools::from_config(&config, spec, job);
	let store = EnvStore::new(&config.paths.worker_envs);
	let updater = Updater::new(store.clone(), Arc::new(tools), false);

	let started = std::time::Instant::now();
	let outcome = updater.run(None, now()).await;
	println!(
		"結果: {outcome:?}、{:.1} 秒",
		started.elapsed().as_secs_f64()
	);

	assert!(
		matches!(outcome, UpdateOutcome::Updated { .. }),
		"{outcome:?}"
	);
	let record = store.read_current().unwrap().unwrap();
	assert!(record.python.is_file());
}
