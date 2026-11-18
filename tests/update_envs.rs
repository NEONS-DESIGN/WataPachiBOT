//! ワーカーの環境の記録 (`current.json`)、片付け、起動時の python の選択の試験。
//!
//! - 実際のディレクトリ (tempfile) を使う
//! - 使用中の環境は、本物の子プロセスで作る。環境の中の exe を動かす、環境の中の pyd を読み込ませる
//! - 子プロセスは Job に入れ、`Running` の Drop で終わらせる (試験が途中で失敗しても残さない)

use std::fs;
use std::io::{BufRead as _, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tempfile::TempDir;
use watapachi_bot::platform::job::Job;
use watapachi_bot::update::envs::{
	select_python, CleanupReport, EnvError, EnvRecord, EnvStore, FallbackReason, PythonSource,
	CURRENT_FILE,
};

#[path = "support/python.rs"]
mod python;

/// 動かし続ける exe (System32 の中の名前)。環境の python.exe の代わりに複製して使う。
const SLEEPER_EXE: &str = "PING.EXE";
/// `SLEEPER_EXE` を長く動かす引数。試験の後は `Running` が終わらせる。
const SLEEPER_ARGS: [&str; 3] = ["-n", "3600", "127.0.0.1"];
/// pyd の代わりに読み込ませる DLL (System32 の中の名前)。依存が少ないもの。
const LOADABLE_DLL: &str = "version.dll";
/// 引数の DLL を読み込み、準備ができたら 1 行書いて眠る python のコード。
const LOAD_AND_SLEEP: &str =
	"import ctypes, sys, time; ctypes.WinDLL(sys.argv[1]); print('ready', flush=True); time.sleep(3600)";
/// `LOAD_AND_SLEEP` が準備できたときに書く行。
const READY_LINE: &str = "ready";
/// 起動した exe がすぐに終わっていないかを確かめるまでの待ち。
const ALIVE_CHECK_DELAY: Duration = Duration::from_millis(300);
/// System32 の場所を得る環境変数。
const ENV_SYSTEM_ROOT: &str = "SystemRoot";

/// 使用中の環境を作るための子プロセス。Drop で Job ごと終わらせる。
struct Running {
	child: Child,
	job: Option<Job>,
}

impl Running {
	/// 標準入出力を閉じて (標準出力だけはパイプで) 起動し、Job に入れる。
	///
	/// # 引数
	/// - `command`: 起動するコマンド
	///
	/// # 例
	/// `let running = Running::spawn(Command::new(exe).args(SLEEPER_ARGS));`
	fn spawn(command: &mut Command) -> Self {
		let child = command
			.stdin(Stdio::null())
			.stdout(Stdio::piped())
			.stderr(Stdio::null())
			.spawn()
			.unwrap();
		let job = Job::new().unwrap();
		let running = Self {
			child,
			job: Some(job),
		};
		if let Some(job) = &running.job {
			job.assign_std(&running.child).unwrap();
		}
		running
	}

	/// 少し待ってもまだ動いていることを確かめる。
	///
	/// # 例
	/// `running.assert_alive();`
	fn assert_alive(&mut self) {
		std::thread::sleep(ALIVE_CHECK_DELAY);
		assert!(
			self.child.try_wait().unwrap().is_none(),
			"子プロセスがすぐに終わった"
		);
	}

	/// 標準出力に `READY_LINE` が出るまで待つ。先に終われば失敗にする。
	///
	/// # 例
	/// `running.wait_ready();`
	fn wait_ready(&mut self) {
		let stdout = self.child.stdout.take().unwrap();
		let mut line = String::new();
		BufReader::new(stdout).read_line(&mut line).unwrap();
		assert_eq!(line.trim(), READY_LINE, "子プロセスが準備できなかった");
	}

	/// 終わらせて、終わるまで待つ。2 回目以降は何もしない。
	///
	/// # 例
	/// `running.stop();`
	fn stop(&mut self) {
		let _ = self.child.kill();
		let _ = self.child.wait();
		self.job = None;
	}
}

impl Drop for Running {
	/// 試験が途中で失敗しても子プロセスを残さない。
	fn drop(&mut self) {
		self.stop();
	}
}

/// System32 の中のファイルのパス。
///
/// # 例
/// `let ping = system_exe(SLEEPER_EXE);`
fn system_exe(name: &str) -> PathBuf {
	let root = std::env::var_os(ENV_SYSTEM_ROOT).unwrap();
	Path::new(&root).join("System32").join(name)
}

/// ディレクトリの下のファイルの相対パスを名前順で返す。
///
/// # 例
/// `let before = files_under(&env_dir);`
fn files_under(dir: &Path) -> Vec<PathBuf> {
	let mut files = Vec::new();
	let mut pending = vec![dir.to_path_buf()];
	while let Some(current) = pending.pop() {
		for entry in fs::read_dir(&current).unwrap() {
			let path = entry.unwrap().path();
			if path.is_dir() {
				pending.push(path);
			} else {
				files.push(path.strip_prefix(dir).unwrap().to_path_buf());
			}
		}
	}
	files.sort();
	files
}

/// 置き場を一時ディレクトリに作る。
fn setup() -> (TempDir, EnvStore) {
	let dir = TempDir::new().unwrap();
	let store = EnvStore::new(dir.path());
	(dir, store)
}

/// 環境のディレクトリと python.exe (中身は空) を作る。
fn make_env(store: &EnvStore, name: &str) -> PathBuf {
	let python = store.python_of(name);
	fs::create_dir_all(python.parent().unwrap()).unwrap();
	fs::write(&python, b"x").unwrap();
	python
}

/// 記録を作る。
fn record(store: &EnvStore, name: &str, previous: Option<&str>) -> EnvRecord {
	EnvRecord {
		name: name.to_owned(),
		python: store.python_of(name),
		yt_dlp_version: "2026.11.01".to_owned(),
		created_at: 1_800_000_000,
		previous: previous.map(str::to_owned),
	}
}

#[test]
fn write_then_read_round_trip() {
	let (dir, store) = setup();
	let written = record(&store, "a", Some("b"));
	store.write_current(&written).unwrap();
	assert_eq!(store.read_current().unwrap(), Some(written));
	// 一時ファイルが残らない
	let names: Vec<String> = fs::read_dir(dir.path())
		.unwrap()
		.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
		.collect();
	assert_eq!(names, [CURRENT_FILE]);
}

#[test]
fn write_current_replaces_existing() {
	let (_dir, store) = setup();
	store.write_current(&record(&store, "a", None)).unwrap();
	let second = record(&store, "b", Some("a"));
	store.write_current(&second).unwrap();
	assert_eq!(store.read_current().unwrap(), Some(second));
}

#[test]
fn read_current_missing_is_none() {
	let (_dir, store) = setup();
	assert_eq!(store.read_current().unwrap(), None);
}

#[test]
fn read_current_corrupt_is_error() {
	let (dir, store) = setup();
	fs::write(dir.path().join(CURRENT_FILE), "{").unwrap();
	assert!(matches!(store.read_current(), Err(EnvError::Corrupt(_))));
}

#[test]
fn allocate_adds_suffix_when_taken() {
	let (_dir, store) = setup();
	let now = jiff::civil::date(2026, 11, 18).at(4, 0, 0, 0);
	let (first, dir) = store.allocate(now).unwrap();
	assert_eq!(first, "20261118-040000");
	assert_eq!(
		dir,
		store.python_of(&first).parent().unwrap().parent().unwrap()
	);
	fs::create_dir_all(&dir).unwrap();
	let (second, dir2) = store.allocate(now).unwrap();
	assert_eq!(second, "20261118-040000-2");
	fs::create_dir_all(dir2).unwrap();
	assert_eq!(store.allocate(now).unwrap().0, "20261118-040000-3");
}

#[test]
fn allocate_creates_root_only() {
	let dir = TempDir::new().unwrap();
	let root = dir.path().join("nested").join("envs");
	let store = EnvStore::new(&root);
	let (_, env_dir) = store
		.allocate(jiff::civil::date(2026, 11, 18).at(4, 0, 0, 0))
		.unwrap();
	assert!(root.is_dir());
	assert!(!env_dir.exists());
}

#[test]
fn cleanup_keeps_current_and_previous() {
	let (dir, store) = setup();
	for name in ["a", "b", "c", "d"] {
		make_env(&store, name);
	}
	fs::create_dir_all(dir.path().join("x.deleting").join("Lib")).unwrap();
	store
		.write_current(&record(&store, "c", Some("b")))
		.unwrap();
	let report = store.cleanup().unwrap();
	assert_eq!(
		report,
		CleanupReport {
			removed: vec!["a".into(), "d".into(), "x.deleting".into()],
			kept_busy: vec![],
		}
	);
	assert!(store.python_of("b").is_file());
	assert!(store.python_of("c").is_file());
	assert!(!dir.path().join("a").exists());
	assert!(!dir.path().join("a.deleting").exists());
	assert!(!dir.path().join("x.deleting").exists());
	// current.json は触らない
	assert!(dir.path().join(CURRENT_FILE).is_file());
}

#[test]
fn cleanup_keeps_env_of_configured_python() {
	let dir = TempDir::new().unwrap();
	// 設定の paths.worker_python が置き場の下の環境 "a" を指している (字面で前方一致する形)
	let plain = EnvStore::new(dir.path());
	let fallback = plain.python_of("a");
	let store = plain.keep_python(&fallback);
	for name in ["a", "b", "c"] {
		make_env(&store, name);
	}
	store.write_current(&record(&store, "c", None)).unwrap();
	let report = store.cleanup().unwrap();
	assert_eq!(report.removed, ["b"]);
	assert!(fallback.is_file(), "予備の python の環境は残るはず");
	assert!(store.python_of("c").is_file());
}

#[test]
fn cleanup_keeps_env_of_configured_python_written_differently() {
	let dir = TempDir::new().unwrap();
	let root = dir.path().join("envs");
	// 置き場は "." を挟み、python は大文字で書く。字面では一致せず、正規化で一致する
	let store = EnvStore::new(root.join(".")).keep_python(
		dir.path()
			.join("ENVS")
			.join("A")
			.join("Scripts")
			.join("python.exe"),
	);
	for name in ["a", "b", "c"] {
		make_env(&store, name);
	}
	store.write_current(&record(&store, "c", None)).unwrap();
	let report = store.cleanup().unwrap();
	assert_eq!(report.removed, ["b"]);
	assert!(store.python_of("a").is_file());
}

#[test]
fn configured_python_outside_root_changes_nothing() {
	let dir = TempDir::new().unwrap();
	let root = dir.path().join("envs");
	let outside = dir.path().join(".venv").join("Scripts").join("python.exe");
	fs::create_dir_all(outside.parent().unwrap()).unwrap();
	fs::write(&outside, b"x").unwrap();
	let store = EnvStore::new(&root).keep_python(&outside);
	for name in ["a", "b"] {
		make_env(&store, name);
	}
	store.write_current(&record(&store, "b", None)).unwrap();
	let report = store.cleanup().unwrap();
	assert_eq!(report.removed, ["a"]);
	assert!(outside.is_file());
}

#[test]
fn cleanup_without_record_removes_nothing() {
	let (dir, store) = setup();
	make_env(&store, "a");
	fs::create_dir_all(dir.path().join("x.deleting")).unwrap();
	let report = store.cleanup().unwrap();
	assert_eq!(report, CleanupReport::default());
	assert!(store.python_of("a").is_file());
	assert!(dir.path().join("x.deleting").is_dir());
}

#[test]
fn cleanup_skips_env_with_running_python() {
	let (dir, store) = setup();
	let busy_python = store.python_of("a");
	fs::create_dir_all(busy_python.parent().unwrap()).unwrap();
	fs::copy(system_exe(SLEEPER_EXE), &busy_python).unwrap();
	let lib = dir.path().join("a").join("Lib");
	fs::create_dir_all(&lib).unwrap();
	fs::write(lib.join("other.pyd"), b"y").unwrap();
	fs::write(lib.join("module.py"), b"z").unwrap();
	make_env(&store, "b");
	make_env(&store, "c");
	store.write_current(&record(&store, "c", None)).unwrap();
	let before = files_under(&dir.path().join("a"));

	// 環境の中の python.exe (中身は ping.exe) を動かしたままにする
	let mut running = Running::spawn(Command::new(&busy_python).args(SLEEPER_ARGS));
	running.assert_alive();
	// 前提: 動いている exe を含むディレクトリでも、Windows は名前の変更を拒まない (開発機で確認)。
	// 名前の変更だけでは使用中を見分けられない
	fs::rename(dir.path().join("a"), dir.path().join("a.probe")).unwrap();
	fs::rename(dir.path().join("a.probe"), dir.path().join("a")).unwrap();

	let report = store.cleanup().unwrap();
	assert_eq!(report.kept_busy, ["a"]);
	assert_eq!(report.removed, ["b"]);
	// 使用中の環境は 1 つも消えず、名前も元のまま
	assert_eq!(files_under(&dir.path().join("a")), before);
	assert!(!dir.path().join("a.deleting").exists());
	assert!(matches!(store.discard("a"), Err(EnvError::InUse(_))));
	assert_eq!(files_under(&dir.path().join("a")), before);
	assert!(!dir.path().join("a.deleting").exists());

	// 終われば次の片付けで消える
	running.stop();
	let report = store.cleanup().unwrap();
	assert_eq!(
		report,
		CleanupReport {
			removed: vec!["a".into()],
			kept_busy: vec![],
		}
	);
	assert!(!dir.path().join("a").exists());
	assert!(!dir.path().join("a.deleting").exists());
}

#[test]
fn cleanup_keeps_deleting_residue_with_loaded_pyd() {
	let (dir, store) = setup();
	make_env(&store, "c");
	store.write_current(&record(&store, "c", None)).unwrap();
	let residue = dir.path().join("x.deleting");
	let site = residue.join("Lib").join("site-packages");
	fs::create_dir_all(&site).unwrap();
	let pyd = site.join("loaded.pyd");
	fs::copy(system_exe(LOADABLE_DLL), &pyd).unwrap();
	// 読み込み中の pyd より先に消されうる名前のファイルも置く (途中まで消えたことを見分けるため)
	fs::write(site.join("aaa_first.py"), b"z").unwrap();
	fs::write(residue.join("Lib").join("aaa_first.py"), b"z").unwrap();
	fs::write(site.join("module.py"), b"z").unwrap();
	let before = files_under(&residue);

	// 環境の外の python が、残骸の中の pyd を読み込んだままにする
	let mut command = Command::new(python::test_python());
	command.args(["-c", LOAD_AND_SLEEP]).arg(&pyd);
	let mut running = Running::spawn(&mut command);
	running.wait_ready();

	let report = store.cleanup().unwrap();
	assert_eq!(report.kept_busy, ["x.deleting"]);
	assert!(report.removed.is_empty());
	assert_eq!(files_under(&residue), before);

	running.stop();
	let report = store.cleanup().unwrap();
	assert_eq!(report.removed, ["x.deleting"]);
	assert!(report.kept_busy.is_empty());
	assert!(!residue.exists());
}

#[test]
fn discard_removes_env_and_stale_residue() {
	let (dir, store) = setup();
	make_env(&store, "a");
	fs::create_dir_all(dir.path().join("a.deleting")).unwrap();
	store.discard("a").unwrap();
	assert!(!dir.path().join("a").exists());
	assert!(!dir.path().join("a.deleting").exists());
}

#[test]
fn select_python_prefers_current() {
	let (_dir, store) = setup();
	let python = make_env(&store, "a");
	store.write_current(&record(&store, "a", None)).unwrap();
	let fallback = Path::new("fallback/python.exe");
	assert_eq!(
		select_python(&store, fallback),
		(python, PythonSource::Current("a".to_owned()))
	);
}

#[test]
fn select_python_falls_back() {
	let (dir, store) = setup();
	let fallback = Path::new("fallback/python.exe");
	let expect = |reason| (fallback.to_path_buf(), PythonSource::Fallback(reason));

	assert_eq!(
		select_python(&store, fallback),
		expect(FallbackReason::NoRecord)
	);

	fs::write(dir.path().join(CURRENT_FILE), "{").unwrap();
	assert_eq!(
		select_python(&store, fallback),
		expect(FallbackReason::Corrupt)
	);

	store.write_current(&record(&store, "a", None)).unwrap();
	assert_eq!(
		select_python(&store, fallback),
		expect(FallbackReason::MissingPython)
	);
}

#[test]
fn select_python_prefers_current_over_previous() {
	let (_dir, store) = setup();
	make_env(&store, "a");
	let current = make_env(&store, "b");
	store
		.write_current(&record(&store, "b", Some("a")))
		.unwrap();
	let fallback = Path::new("fallback/python.exe");
	assert_eq!(
		select_python(&store, fallback),
		(current, PythonSource::Current("b".to_owned()))
	);
}

#[test]
fn select_python_uses_previous_when_current_python_is_missing() {
	let (_dir, store) = setup();
	let previous = make_env(&store, "a");
	store
		.write_current(&record(&store, "b", Some("a")))
		.unwrap();
	let fallback = Path::new("fallback/python.exe");
	assert_eq!(
		select_python(&store, fallback),
		(
			previous,
			PythonSource::Previous {
				name: "a".to_owned(),
				current: "b".to_owned(),
			}
		)
	);
}

#[test]
fn select_python_falls_back_when_previous_python_is_missing_too() {
	let (_dir, store) = setup();
	let fallback = Path::new("fallback/python.exe");
	let expect = (
		fallback.to_path_buf(),
		PythonSource::Fallback(FallbackReason::MissingPython),
	);

	// previous の環境も無い
	store
		.write_current(&record(&store, "b", Some("a")))
		.unwrap();
	assert_eq!(select_python(&store, fallback), expect);

	// previous が置き場の直下の名前でなければ、python があっても使わない
	make_env(&store, "a");
	store
		.write_current(&record(&store, "b", Some("a/../a")))
		.unwrap();
	assert_eq!(select_python(&store, fallback), expect);
}

#[test]
fn select_python_with_missing_root_falls_back() {
	let dir = TempDir::new().unwrap();
	let store = EnvStore::new(dir.path().join("no-such-dir"));
	let fallback = Path::new("fallback/python.exe");
	assert_eq!(
		select_python(&store, fallback),
		(
			fallback.to_path_buf(),
			PythonSource::Fallback(FallbackReason::NoRecord)
		)
	);
}
