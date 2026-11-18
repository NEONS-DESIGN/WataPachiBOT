//! PipeInput。`python -m yt_dlp -o -` の標準出力を Songbird の入力にする予備の経路。
//!
//! - シークできない。先頭から再生する
//! - 子は Job に入れる。Bot が異常終了しても OS が子を終わらせる
//! - 入力が捨てられると `ChildContainer` の Drop が子を kill して回収する
//! - 標準出力の終わり (EOF) で子の終了コードを見て、0 でなければ失敗を `ChunkFailure` に記録する。
//!   Songbird は入力の終わりと失敗を区別しないため、バックエンドが曲の終わりにこれを見て
//!   `TrackEnded` か `TrackFailed` かを決める。読み取りのエラーも同じく記録する
//! - 子の標準エラーは読み取りスレッドで 1 行ずつ `tracing::debug!` へ流す。URL は出さない
//! - URL は http / https だけを受け、`--` の後に渡す (オプションやローカルのファイルとして読ませない)
//!
//! 呼び出し方:
//! ```no_run
//! # use watapachi_bot::audio::pipe::{PipeInput, PipeSpec};
//! # use watapachi_bot::platform::job::Job;
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let config: watapachi_bot::config::Config = todo!();
//! let spec = PipeSpec::from_config(&config, &config.paths.worker_python);
//! let job = Job::new()?;
//! let input = PipeInput::spawn(&spec, "https://www.youtube.com/watch?v=...", &job)?;
//! let failure = input.failure(); // 曲の終わりに failure.get() を見る
//! let songbird_input = input.into_input();
//! # Ok(())
//! # }
//! ```

use std::io::{BufRead, BufReader, ErrorKind, Read, Result as IoResult};
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{ChildStderr, Command, Stdio};
use std::time::{Duration, Instant};

use songbird::input::{AudioStream, ChildContainer, Input, LiveInput};
use symphonia::core::io::{MediaSource, ReadOnlySource};

use crate::audio::backend::AudioError;
use crate::audio::chunked::{ChunkError, ChunkFailure};
use crate::config::Config;
use crate::platform::job::Job;

/// Windows でコンソールの窓を出さない起動フラグ (CREATE_NO_WINDOW)。
const CREATE_NO_WINDOW: u32 = 0x0800_0000;
/// 標準エラーの 1 行に含まれる URL の置き換え先。
const URL_PLACEHOLDER: &str = "<url>";
/// 子に渡してよい URL のスキーム。
const ALLOWED_SCHEMES: [&str; 2] = ["http", "https"];
/// これより後の引数をオプションとして読ませない区切り。
const END_OF_OPTIONS: &str = "--";
/// 標準出力の終わりから子の終了を待つ上限。設定項目に無いため固定値にする。
/// ミキサーのスレッドで待つため短くする (yt-dlp は標準出力を閉じるとすぐ終わる)。
const EXIT_WAIT: Duration = Duration::from_millis(500);
/// 子の終了を確かめる間隔。設定項目に無いため固定値にする。
const EXIT_POLL: Duration = Duration::from_millis(10);

/// 子の起動方法。
#[derive(Debug, Clone)]
pub struct PipeSpec {
	/// 起動する python の実行ファイル。
	pub python: PathBuf,
	/// python の後に付ける引数。本番は
	/// `["-m", "yt_dlp", "--quiet", "--no-warnings", "--no-playlist", "-f", <pipe_format>, "-o", "-"]`
	/// と Cookie の指定。`--` と URL は `PipeInput::spawn` が最後に足す。
	pub args: Vec<String>,
}

impl PipeSpec {
	/// 設定から本番の起動方法を作る。
	///
	/// - 引数の `python` を python にする (起動時に選んだ環境の python.exe)
	/// - `playback.pipe_format` を `-f` に渡す
	/// - `worker.cookies_browser` が空でなければ `--cookies-from-browser` を付ける
	/// - `worker.user_agent` が空でなければ `--user-agent` を付ける
	///
	/// # 引数
	/// - `config`: 設定
	/// - `python`: 起動する python.exe
	///
	/// # 戻り値
	/// 本番の起動方法
	///
	/// # 例
	/// `let spec = PipeSpec::from_config(&config, &python);`
	pub fn from_config(config: &Config, python: &Path) -> PipeSpec {
		let mut args: Vec<String> = [
			"-m",
			"yt_dlp",
			"--quiet",
			"--no-warnings",
			"--no-playlist",
			"-f",
		]
		.iter()
		.map(|s| (*s).to_owned())
		.collect();
		args.push(config.playback.pipe_format.clone());
		args.push("-o".to_owned());
		args.push("-".to_owned());
		if !config.worker.cookies_browser.is_empty() {
			args.push("--cookies-from-browser".to_owned());
			args.push(config.worker.cookies_browser.clone());
		}
		if !config.worker.user_agent.is_empty() {
			args.push("--user-agent".to_owned());
			args.push(config.worker.user_agent.clone());
		}
		PipeSpec {
			python: python.to_path_buf(),
			args,
		}
	}
}

/// 子の標準出力を読み、終わりで子の終了コードを確かめる `Read`。
///
/// - EOF で一度だけ子の終了を `EXIT_WAIT` まで待ち、0 以外なら失敗を記録する
/// - 読み取りのエラー (割り込みを除く) も失敗として記録する
/// - 捨てると中の `ChildContainer` が子を kill する
struct PipeReader {
	container: ChildContainer,
	failure: ChunkFailure,
	/// 終了コードを確かめたか
	exit_checked: bool,
}

impl PipeReader {
	/// 子の終了を待ち、異常終了なら失敗を記録する。2 回目以降は何もしない。
	///
	/// - `EXIT_WAIT` の間に終わらなければ警告だけ残す (終了コードが分からないため失敗にしない)
	///
	/// # 例
	/// `self.check_exit();`
	fn check_exit(&mut self) {
		if self.exit_checked {
			return;
		}
		self.exit_checked = true;
		let Some(child) = self.container.0.last_mut() else {
			return;
		};
		let deadline = Instant::now() + EXIT_WAIT;
		loop {
			match child.try_wait() {
				Ok(Some(status)) if status.success() => return,
				Ok(Some(status)) => {
					let code = status
						.code()
						.map_or_else(|| "不明".to_owned(), |code| code.to_string());
					tracing::warn!(%code, "yt-dlp が異常終了しました");
					self.failure
						.record(ChunkError::Pipe(format!("終了コード {code}")));
					return;
				}
				Ok(None) if Instant::now() < deadline => std::thread::sleep(EXIT_POLL),
				Ok(None) => {
					tracing::warn!("yt-dlp が標準出力を閉じた後も終わりません");
					return;
				}
				Err(error) => {
					tracing::warn!(%error, "yt-dlp の終了を確かめられません");
					return;
				}
			}
		}
	}
}

impl Read for PipeReader {
	/// 子の標準出力から読む。EOF で終了コードを確かめ、エラーは失敗として記録する。
	///
	/// # エラー
	/// 標準出力の読み取りのエラー
	fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
		match self.container.read(buf) {
			Ok(0) if !buf.is_empty() => {
				self.check_exit();
				Ok(0)
			}
			Ok(count) => Ok(count),
			Err(error) if error.kind() == ErrorKind::Interrupted => Err(error),
			Err(error) => {
				tracing::warn!(%error, "yt-dlp の標準出力を読めません");
				self.failure.record(ChunkError::Pipe(error.to_string()));
				Err(error)
			}
		}
	}
}

/// 起動済みの子 (yt-dlp) と、その標準出力。
pub struct PipeInput {
	reader: PipeReader,
}

impl PipeInput {
	/// 子を起動して Job に入れる。
	///
	/// - `url` は http / https だけを受ける。`--` の後の最後の引数として渡す (シェルを通さない)
	/// - 標準入力は閉じ、標準出力はパイプにする。標準エラーは読み取りスレッドへ渡す
	///
	/// # 引数
	/// - `spec`: 起動方法
	/// - `url`: 動画の URL
	/// - `job`: 子を入れる Job
	///
	/// # エラー
	/// `url` が http / https の URL でない場合、起動に失敗した場合、Job へ入れるのに失敗した場合は
	/// `AudioError::Failed` (最後の場合は子を終わらせてから返す)。
	///
	/// # 例
	/// `let input = PipeInput::spawn(&spec, url, &job)?;`
	pub fn spawn(spec: &PipeSpec, url: &str, job: &Job) -> Result<PipeInput, AudioError> {
		check_url(url)?;
		let mut child = Command::new(&spec.python)
			.args(&spec.args)
			.arg(END_OF_OPTIONS)
			.arg(url)
			.stdin(Stdio::null())
			.stdout(Stdio::piped())
			.stderr(Stdio::piped())
			.creation_flags(CREATE_NO_WINDOW)
			.spawn()
			.map_err(|error| AudioError::Failed(format!("yt-dlp を起動できません: {error}")))?;
		if let Err(error) = job.assign_std(&child) {
			let _ = child.kill();
			let _ = child.wait();
			return Err(AudioError::Failed(format!(
				"yt-dlp を Job に入れられません: {error}"
			)));
		}
		if let Some(stderr) = child.stderr.take() {
			spawn_stderr_logger(stderr, url.to_owned());
		}
		Ok(PipeInput {
			reader: PipeReader {
				container: ChildContainer::from(child),
				failure: ChunkFailure::default(),
				exit_checked: false,
			},
		})
	}

	/// 失敗の記録を返す。`into_input` の前に取っておき、曲の終わりに見る。
	///
	/// # 例
	/// `let failure = pipe.failure(); let input = pipe.into_input();`
	pub fn failure(&self) -> ChunkFailure {
		self.reader.failure.clone()
	}

	/// Songbird の入力にする。シークできない。
	///
	/// - 入力を捨てると子が kill される
	///
	/// # 例
	/// `let input: Input = pipe.into_input();`
	pub fn into_input(self) -> Input {
		let stream = AudioStream {
			input: Box::new(ReadOnlySource::new(self.reader)) as Box<dyn MediaSource>,
		};
		Input::Live(LiveInput::Raw(stream), None)
	}
}

/// 子に渡す URL を確かめる。
///
/// - URL として読めて、スキームが http / https であること
/// - エラーの文言に URL を入れない (署名などが入りうるため)
///
/// # エラー
/// 条件を満たさなければ `AudioError::Failed`
///
/// # 例
/// `check_url("https://www.youtube.com/watch?v=...")?;`
fn check_url(url: &str) -> Result<(), AudioError> {
	let parsed = url::Url::parse(url)
		.map_err(|error| AudioError::Failed(format!("yt-dlp に渡す URL が不正です: {error}")))?;
	if ALLOWED_SCHEMES.contains(&parsed.scheme()) {
		Ok(())
	} else {
		Err(AudioError::Failed(format!(
			"yt-dlp に渡せない URL のスキームです: {}",
			parsed.scheme()
		)))
	}
}

/// 子の標準エラーを 1 行ずつ `debug!` へ流すスレッドを起動する。
///
/// - 行に含まれる URL は伏せる
/// - 子が終わって標準エラーが閉じるとスレッドも終わる
///
/// # 引数
/// - `stderr`: 子の標準エラー
/// - `url`: 伏せる URL
fn spawn_stderr_logger(stderr: ChildStderr, url: String) {
	let spawned = std::thread::Builder::new()
		.name("pipe-stderr".to_owned())
		.spawn(move || {
			for line in BufReader::new(stderr).split(b'\n') {
				let Ok(line) = line else { break };
				let text = String::from_utf8_lossy(&line);
				let text = if url.is_empty() {
					text.into_owned()
				} else {
					text.replace(&url, URL_PLACEHOLDER)
				};
				tracing::debug!(target: "pipe", "yt-dlp: {}", text.trim_end());
			}
		});
	if let Err(error) = spawned {
		tracing::warn!(%error, "標準エラーの読み取りスレッドを起動できません");
	}
}
