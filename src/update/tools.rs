//! `EnvTools` の本物。uv で環境を作り、python で版を読み、実際のワーカーで検査する。
//!
//! - uv と python は `tokio::process::Command` で起動する。`kill_on_drop`、`CREATE_NO_WINDOW`、Job に入れる
//! - コマンドごとに専用の Job を持ち、上限時間を超えたときや打ち切られたときは孫まで終わらせる
//! - 標準出力と標準エラーは debug に出し、失敗したら末尾を `UpdateError::Command` に入れる
//!
//! 呼び出し方:
//! ```no_run
//! # use std::sync::Arc;
//! # use watapachi_bot::config::Config;
//! # use watapachi_bot::platform::job::Job;
//! # use watapachi_bot::resolver::process::WorkerSpec;
//! # use watapachi_bot::update::tools::UvTools;
//! # fn demo(config: &Config, spec: WorkerSpec, job: Arc<Job>) {
//! let tools = UvTools::from_config(config, spec, job);
//! # }
//! ```

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::process::Command;

use crate::config::Config;
use crate::platform::job::Job;
use crate::resolver::process::{WorkerProcess, WorkerSpec};
use crate::resolver::protocol::WorkerRequest;
use crate::update::envs::PYTHON_RELATIVE;
use crate::update::procedure::{EnvTools, UpdateError};

/// コンソールを持たせない。
const CREATE_NO_WINDOW: u32 = 0x0800_0000;
/// 失敗の詳細に入れる出力の末尾の行数。
const TAIL_LINES: usize = 20;
/// 検査のワーカーが自発的に終わるのを待つ上限。超えたら強制終了する。
const WORKER_CLOSE_GRACE: Duration = Duration::from_secs(5);
/// URL の雛形で動画 ID に置き換える部分。
const ID_PLACEHOLDER: &str = "{id}";

/// uv を使う `EnvTools`。
pub struct UvTools {
	uv: PathBuf,
	packages: Vec<String>,
	python_version: String,
	install_timeout: Duration,
	check_timeout: Duration,
	check_url: String,
	worker_template: WorkerSpec,
	job: Arc<Job>,
}

impl UvTools {
	/// 設定から作る。
	///
	/// - `update.packages` は空白で分ける
	/// - 検査の動画の URL は `ui.watch_url` の `{id}` を `update.check_video` に置き換えて作る
	///
	/// # 引数
	/// - `config`: 設定
	/// - `worker_template`: 検査のワーカーの起動方法 (python だけ新しい環境のものに差し替える)
	/// - `job`: 子プロセスを入れる全体の Job
	///
	/// # 例
	/// `let tools = UvTools::from_config(&config, worker_spec(&config, &python), Arc::clone(&job));`
	pub fn from_config(config: &Config, worker_template: WorkerSpec, job: Arc<Job>) -> Self {
		let update = &config.update;
		Self {
			uv: config.paths.uv.clone(),
			packages: update
				.packages
				.split_whitespace()
				.map(str::to_owned)
				.collect(),
			python_version: update.python.clone(),
			install_timeout: Duration::from_secs(update.install_timeout_secs),
			check_timeout: Duration::from_secs(update.check_timeout_secs),
			check_url: config
				.ui
				.watch_url
				.replace(ID_PLACEHOLDER, &update.check_video),
			worker_template,
			job,
		}
	}

	/// 外部コマンドを 1 つ実行し、標準出力を返す。
	///
	/// - 専用の Job に入れ、上限時間の超過や打ち切りで孫まで終わらせる
	/// - 標準出力と標準エラーは debug に出す
	///
	/// # 引数
	/// - `what`: 何のコマンドか (エラーとログに使う)
	/// - `program`: 実行ファイル
	/// - `args`: 引数
	/// - `limit`: 上限時間
	///
	/// # 戻り値
	/// 標準出力 (UTF-8 として読めない部分は置き換える)
	///
	/// # エラー
	/// - 起動か Job への割り当てに失敗、終了コードが 0 でない: `UpdateError::Command` (出力の末尾 20 行つき)
	/// - 上限を超えた: `UpdateError::Timeout`
	async fn run_command(
		&self,
		what: &str,
		program: &Path,
		args: &[OsString],
		limit: Duration,
	) -> Result<String, UpdateError> {
		let command_error = |detail: String| UpdateError::Command {
			what: what.to_owned(),
			detail,
		};
		let tree = Job::new().map_err(|error| command_error(error.to_string()))?;
		let child = Command::new(program)
			.args(args)
			.stdin(Stdio::null())
			.stdout(Stdio::piped())
			.stderr(Stdio::piped())
			.kill_on_drop(true)
			.creation_flags(CREATE_NO_WINDOW)
			.spawn()
			.map_err(|error| {
				command_error(format!("{} を起動できません: {error}", program.display()))
			})?;
		// 起動直後に入れる。孫が先に作られると Job から漏れる
		self.job
			.assign(&child)
			.and_then(|()| tree.assign(&child))
			.map_err(|error| command_error(format!("Job へ入れられません: {error}")))?;

		// 打ち切られると child が破棄されて終了し、tree の破棄で孫も終わる
		let output = match tokio::time::timeout(limit, child.wait_with_output()).await {
			Ok(Ok(output)) => output,
			Ok(Err(error)) => return Err(command_error(error.to_string())),
			Err(_) => return Err(UpdateError::Timeout(what.to_owned())),
		};
		drop(tree);

		let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
		let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
		for line in stdout.lines().chain(stderr.lines()) {
			tracing::debug!(what, "{line}");
		}
		if output.status.success() {
			Ok(stdout)
		} else {
			let combined = format!("{stdout}\n{stderr}");
			Err(command_error(format!(
				"終了コード {}: {}",
				output
					.status
					.code()
					.map_or_else(|| "なし".to_owned(), |code| code.to_string()),
				tail(&combined, TAIL_LINES)
			)))
		}
	}
}

#[async_trait]
impl EnvTools for UvTools {
	/// `uv venv` で環境を作り、`uv pip install` でパッケージを入れる。
	///
	/// # エラー
	/// どちらかの失敗か上限時間の超過。失敗時のディレクトリは呼び出し側が消す
	async fn create(&self, dir: &Path) -> Result<(), UpdateError> {
		let venv_args: Vec<OsString> = vec![
			"venv".into(),
			"--python".into(),
			self.python_version.clone().into(),
			dir.as_os_str().to_owned(),
		];
		self.run_command("uv venv", &self.uv, &venv_args, self.install_timeout)
			.await?;

		let mut install_args: Vec<OsString> = vec![
			"pip".into(),
			"install".into(),
			"--python".into(),
			python_in(dir).into_os_string(),
			"--prerelease=allow".into(),
		];
		install_args.extend(self.packages.iter().map(OsString::from));
		self.run_command(
			"uv pip install",
			&self.uv,
			&install_args,
			self.install_timeout,
		)
		.await?;
		Ok(())
	}

	/// `python -m yt_dlp --version` の 1 行目を返す。
	///
	/// # エラー
	/// 起動の失敗、上限時間の超過、出力が空の場合
	async fn yt_dlp_version(&self, python: &Path) -> Result<String, UpdateError> {
		let args: Vec<OsString> = vec!["-m".into(), "yt_dlp".into(), "--version".into()];
		let stdout = self
			.run_command("yt_dlp --version", python, &args, self.check_timeout)
			.await?;
		match stdout.lines().next().map(str::trim) {
			Some(version) if !version.is_empty() => Ok(version.to_owned()),
			_ => Err(UpdateError::Command {
				what: "yt_dlp --version".to_owned(),
				detail: "版が出力されませんでした".to_owned(),
			}),
		}
	}

	/// 新しい環境のワーカーを 1 つ起動し、ping と `check_video` の resolve を確かめる。
	///
	/// - 全体を `check_timeout` で打ち切る。終わったら (失敗でも) ワーカーを終了させる
	/// - 解決が予備設定 (fallback) で通ったときは成功のまま WARN を出す。主経路が壊れている兆しのため
	///
	/// # エラー
	/// 起動・ping・解決の失敗 (`UpdateError::Check`)、上限時間の超過 (`UpdateError::Timeout`)
	async fn check(&self, python: &Path) -> Result<(), UpdateError> {
		let mut spec = self.worker_template.clone();
		spec.python = python.to_path_buf();
		let mut worker = WorkerProcess::spawn(&spec, &self.job)
			.map_err(|error| UpdateError::Check(format!("ワーカーを起動できません: {error}")))?;

		let steps = async {
			worker
				.call(&WorkerRequest::Ping, self.check_timeout)
				.await
				.map_err(|error| UpdateError::Check(format!("ping に失敗しました: {error}")))?;
			let resolve = WorkerRequest::Resolve {
				url: self.check_url.clone(),
				format_id: None,
			};
			let resolved = worker
				.call(&resolve, self.check_timeout)
				.await
				.map_err(|error| UpdateError::Check(format!("解決に失敗しました: {error}")))?;
			if resolved_via_fallback(&resolved) {
				tracing::warn!(
					url = %self.check_url,
					"更新の検査の解決は予備設定 (fallback) で通りました。主経路の解決が失敗しています"
				);
			}
			Ok(())
		};
		let result = match tokio::time::timeout(self.check_timeout, steps).await {
			Ok(result) => result,
			Err(_) => Err(UpdateError::Timeout("新しい環境の検査".to_owned())),
		};
		match result {
			Ok(()) => worker.close(WORKER_CLOSE_GRACE).await,
			Err(_) => worker.kill().await,
		}
		result
	}
}

/// 解決の結果が予備設定 (fallback) で得られたものか。
///
/// - ワーカーの `resolve` の結果の `fallback` が true のときだけ true
/// - 項目が無い (古いワーカー)、真偽値でないときは false
///
/// # 引数
/// - `result`: `WorkerProcess::call` が返した成功応答の `result`
///
/// # 例
/// `assert!(resolved_via_fallback(&serde_json::json!({"fallback": true})));`
fn resolved_via_fallback(result: &serde_json::Value) -> bool {
	result
		.get("fallback")
		.and_then(serde_json::Value::as_bool)
		.unwrap_or(false)
}

/// 環境のディレクトリの python.exe。
fn python_in(dir: &Path) -> PathBuf {
	let mut path = dir.to_path_buf();
	path.extend(PYTHON_RELATIVE);
	path
}

/// 文字列の末尾の `count` 行 (空行を除く)。
///
/// # 引数
/// - `text`: 出力
/// - `count`: 残す行数
///
/// # 例
/// `assert_eq!(tail("a\nb\nc", 2), "b\nc");`
fn tail(text: &str, count: usize) -> String {
	let lines: Vec<&str> = text
		.lines()
		.filter(|line| !line.trim().is_empty())
		.collect();
	let start = lines.len().saturating_sub(count);
	lines[start..].join("\n")
}

#[cfg(test)]
mod tests {
	use super::{resolved_via_fallback, tail};
	use serde_json::json;

	#[test]
	fn resolved_via_fallback_reads_the_flag() {
		assert!(resolved_via_fallback(&json!({"id": "x", "fallback": true})));
		assert!(!resolved_via_fallback(
			&json!({"id": "x", "fallback": false})
		));
		// 古いワーカーは項目を返さない。予備設定を使っていないとみなす
		assert!(!resolved_via_fallback(&json!({"id": "x"})));
		assert!(!resolved_via_fallback(
			&json!({"id": "x", "fallback": "yes"})
		));
	}

	#[test]
	fn tail_keeps_last_lines_without_blanks() {
		assert_eq!(tail("a\n\nb\nc\n", 2), "b\nc");
		assert_eq!(tail("a", 5), "a");
		assert_eq!(tail("", 5), "");
	}
}
