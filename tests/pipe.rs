//! PipeInput の試験。偽の yt-dlp (tests/fixtures/fake_ytdlp.py) を子として起動する。
//! - 起動する python は support/python.rs で決める

#[path = "support/python.rs"]
mod python;
#[path = "support/wav.rs"]
mod wav;

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use songbird::input::{Input, LiveInput};
use watapachi_bot::audio::backend::AudioError;
use watapachi_bot::audio::chunked::ChunkError;
use watapachi_bot::audio::pipe::{PipeInput, PipeSpec};
use watapachi_bot::platform::job::Job;

/// 偽 yt-dlp に渡す URL の土台。偽 yt-dlp は問い合わせの部分だけを読む。
const FIXTURE_URL: &str = "http://fixture.invalid/";

/// 偽 yt-dlp への指示を URL の問い合わせにする。
///
/// - `mode`: `file` (中身を書く) か `infinite` (PID を書いて無限に書く)
/// - `path`: 対象のファイル
fn fixture_url(mode: &str, path: &Path) -> String {
	url::Url::parse_with_params(FIXTURE_URL, [(mode, path.to_string_lossy())])
		.unwrap()
		.to_string()
}

/// tests/fixtures 以下のファイルの絶対パス (文字列)。
fn fixture(name: &str) -> String {
	Path::new(env!("CARGO_MANIFEST_DIR"))
		.join("tests/fixtures")
		.join(name)
		.to_string_lossy()
		.into_owned()
}

/// 偽 yt-dlp を起動する PipeSpec。
fn fake_spec() -> PipeSpec {
	PipeSpec {
		python: python::test_python(),
		args: vec![fixture("fake_ytdlp.py")],
	}
}

/// `into_input` した入力のバイト列を最後まで読む。
fn read_all_raw(input: Input) -> Vec<u8> {
	let Input::Live(LiveInput::Raw(mut stream), None) = input else {
		panic!("Live(Raw) の入力であること");
	};
	let mut bytes = Vec::new();
	stream
		.input
		.read_to_end(&mut bytes)
		.expect("最後まで読めること");
	bytes
}

/// 指定した PID のプロセスが生きているか。
fn process_alive(pid: u32) -> bool {
	let output = Command::new("tasklist")
		.args(["/FI", &format!("PID eq {pid}"), "/NH"])
		.output()
		.expect("tasklist を実行できること");
	String::from_utf8_lossy(&output.stdout).contains(&pid.to_string())
}

/// ファイルが現れるまで待ち、PID を読む。
fn read_pid(path: &PathBuf) -> u32 {
	let deadline = Instant::now() + Duration::from_secs(30);
	loop {
		if let Ok(text) = std::fs::read_to_string(path) {
			if let Ok(pid) = text.trim().parse() {
				return pid;
			}
		}
		assert!(Instant::now() < deadline, "PID のファイルが現れない");
		std::thread::sleep(Duration::from_millis(50));
	}
}

#[test]
fn pipe_streams_child_stdout() {
	let dir = tempfile::tempdir().unwrap();
	let wav_path = dir.path().join("sine.wav");
	std::fs::write(&wav_path, wav::sine_wav(1.0)).unwrap();
	let job = Job::new().unwrap();
	let url = fixture_url("file", &wav_path);
	let input = PipeInput::spawn(&fake_spec(), &url, &job).unwrap();
	let failure = input.failure();
	let bytes = read_all_raw(input.into_input());
	assert_eq!(bytes, std::fs::read(&wav_path).unwrap());
	assert!(
		failure.get().is_none(),
		"終了コード 0 の子を失敗として記録した"
	);
}

/// 子が 0 以外の終了コードで終わったら、標準出力の終わりで失敗を記録すること。
///
/// - Songbird は入力の終わりと失敗を区別しないため、記録が無いと失敗した曲が `TrackEnded` になる
#[test]
fn pipe_nonzero_exit_records_failure() {
	// 問い合わせに file も infinite も無い URL。偽 yt-dlp は何も書かずに終了コード 2 で終わる。
	// Job は読み終えるまで残す (捨てると子が kill され、終了コードが変わる)
	let job = Job::new().unwrap();
	let input = PipeInput::spawn(&fake_spec(), FIXTURE_URL, &job).unwrap();
	let failure = input.failure();
	let bytes = read_all_raw(input.into_input());
	assert!(bytes.is_empty());
	let recorded = failure.get().expect("異常終了が記録されていない");
	assert!(
		matches!(*recorded, ChunkError::Pipe(_)),
		"想定外のエラー: {recorded:?}"
	);
}

#[test]
fn pipe_spawn_failure_is_audio_error() {
	let spec = PipeSpec {
		python: PathBuf::from("Z:/no/such/python.exe"),
		args: vec![],
	};
	let result = PipeInput::spawn(&spec, FIXTURE_URL, &Job::new().unwrap());
	assert!(matches!(result, Err(AudioError::Failed(_))));
}

#[test]
fn dropping_input_ends_child() {
	let dir = tempfile::tempdir().unwrap();
	let pid_file = dir.path().join("pid");
	let job = Job::new().unwrap();
	let url = fixture_url("infinite", &pid_file);
	let input = PipeInput::spawn(&fake_spec(), &url, &job)
		.unwrap()
		.into_input();
	let pid = read_pid(&pid_file);
	assert!(process_alive(pid), "捨てる前は子が生きている");
	drop(input);
	let deadline = Instant::now() + Duration::from_secs(5);
	while process_alive(pid) {
		assert!(Instant::now() < deadline, "5 秒以内に子が終わらない");
		std::thread::sleep(Duration::from_millis(100));
	}
}

#[test]
fn dropping_job_ends_child() {
	let dir = tempfile::tempdir().unwrap();
	let pid_file = dir.path().join("pid");
	let job = Job::new().unwrap();
	let url = fixture_url("infinite", &pid_file);
	let input = PipeInput::spawn(&fake_spec(), &url, &job).unwrap();
	let pid = read_pid(&pid_file);
	drop(job);
	let deadline = Instant::now() + Duration::from_secs(5);
	while process_alive(pid) {
		assert!(Instant::now() < deadline, "Job の破棄で子が終わらない");
		std::thread::sleep(Duration::from_millis(100));
	}
	drop(input);
}

#[test]
fn spawn_rejects_option_like_url() {
	let result = PipeInput::spawn(&fake_spec(), "--exec=calc", &Job::new().unwrap());
	assert!(matches!(result, Err(AudioError::Failed(_))));
}

#[test]
fn spawn_rejects_non_http_url() {
	let dir = tempfile::tempdir().unwrap();
	let wav_path = dir.path().join("sine.wav");
	std::fs::write(&wav_path, wav::sine_wav(0.1)).unwrap();
	let url = url::Url::from_file_path(&wav_path).unwrap().to_string();
	assert!(url.starts_with("file:///"));
	let result = PipeInput::spawn(&fake_spec(), &url, &Job::new().unwrap());
	assert!(matches!(result, Err(AudioError::Failed(_))));
}

#[test]
fn spec_from_config_builds_ytdlp_args() {
	let mut config = watapachi_bot::config::Config::load(
		&Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.toml"),
	)
	.unwrap();
	config.worker.cookies_browser = "firefox".to_owned();
	// 設定の paths.worker_python ではなく、引数で渡した python が使われる
	let python = Path::new("worker/envs/20261118-040000/Scripts/python.exe");
	let spec = PipeSpec::from_config(&config, python);
	assert_ne!(spec.python, config.paths.worker_python);
	assert_eq!(spec.python, python);
	assert_eq!(&spec.args[..2], ["-m", "yt_dlp"]);
	assert!(spec.args.windows(2).any(|w| w == ["-o", "-"]));
	assert!(spec
		.args
		.windows(2)
		.any(|w| w == ["--cookies-from-browser", "firefox"]));
	config.worker.cookies_browser.clear();
	assert!(!PipeSpec::from_config(&config, python)
		.args
		.contains(&"--cookies-from-browser".to_owned()));
}
