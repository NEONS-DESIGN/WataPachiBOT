//! ワーカーの起動指定と python の選択、起動中の Bot のワーカーの切り替え。
//! - `worker_spec`: 設定と python.exe からワーカーの起動指定を作る (純粋)
//! - `choose_python`: `current.json` の記録から使う python を選び、ログに残す
//! - `check_worker_files`: python とスクリプトの有無を確かめる (起動は止めない)
//! - `LiveSwitch`: 更新の手順の `WorkerSwitch`。プールと `PipeSpec` を入れ替える
//!
//! 呼び出し方:
//! ```ignore
//! let (python, origin) = choose_python(&config);
//! check_worker_files(&config, &python, origin);
//! let pool = WorkerPool::start(worker_spec(&config, &python), &config.worker, job);
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::audio::pipe::PipeSpec;
use crate::config::Config;
use crate::resolver::pool::RollingRestartError;
use crate::resolver::pool::WorkerPool;
use crate::resolver::process::WorkerSpec;
use crate::update::envs::{select_python, EnvStore, FallbackReason, PythonSource};
use crate::update::procedure::WorkerSwitch;

/// ワーカーへの引数の名前。worker.py の argparse と揃える。
const ARG_CACHEDIR: &str = "--cachedir";
const ARG_PREMIUM: &str = "--premium";
const ARG_COOKIES: &str = "--cookies";
const ARG_USER_AGENT: &str = "--user-agent";

/// 起動時の python の出どころ (ログの文言): current.json の環境。
const PYTHON_ORIGIN_CURRENT: &str = "current.json の環境の python";
/// 起動時の python の出どころ (ログの文言): current.json の previous の環境。
const PYTHON_ORIGIN_PREVIOUS: &str = "直前の環境 (current.json の previous) の python";
/// 起動時の python の出どころ (ログの文言): 設定。
const PYTHON_ORIGIN_CONFIG: &str = "paths.worker_python";

/// 起動中の Bot の切り替え。ワーカーのプールと、`InputFactory` と共有する `PipeSpec` を持つ。
///
/// - 更新の手順 (`Updater::run`) の `WorkerSwitch` として使う
/// - `pipe` は起動時に作った `Arc<ArcSwap<PipeSpec>>` と同じものを渡す
///
/// # 例
/// `let live = LiveSwitch { pool, template: worker_spec(&config, &python), pipe };`
pub struct LiveSwitch {
	/// ワーカーのプール
	pub pool: Arc<WorkerPool>,
	/// 起動指定の雛形。python だけ差し替える
	pub template: WorkerSpec,
	/// PipeInput の起動方法 (`InputFactory` と共有)
	pub pipe: Arc<ArcSwap<PipeSpec>>,
}

#[async_trait::async_trait]
impl WorkerSwitch for LiveSwitch {
	/// ワーカーを入れ替える。
	///
	/// - `Some` なら雛形の python を差し替えた起動指定で `rolling_restart` し、成功したら `pipe` の python も差し替える
	/// - `None` なら同じ環境で入れ替える (`pipe` は変えない)
	///
	/// # 引数
	/// - `python`: 新しい環境の python.exe
	///
	/// # エラー
	/// `WorkerPool::rolling_restart` の失敗。失敗時は `pipe` を変えない
	async fn switch(&self, python: Option<PathBuf>) -> Result<(), RollingRestartError> {
		let spec = python.as_ref().map(|python| WorkerSpec {
			python: python.clone(),
			..self.template.clone()
		});
		self.pool.rolling_restart(spec).await?;
		if let Some(python) = python {
			let mut next = PipeSpec::clone(&self.pipe.load());
			next.python = python;
			self.pipe.store(Arc::new(next));
		}
		Ok(())
	}
}

/// ワーカーの起動指定を作る (純粋)。
///
/// - `python` は引数 (起動時に選んだ環境の python.exe)、`script = paths.worker_script`
/// - 引数は `--cachedir <paths.ytdlp_cache> --premium <worker.youtube_premium>` に、
///   `worker.cookies_browser` が空でなければ `--cookies <cookies_browser>`、
///   `worker.user_agent` が空でなければ `--user-agent <user_agent>` を足す
/// - `--request-budget-secs` は `WorkerPool` が足すため含めない
///
/// # 引数
/// - `config`: 設定
/// - `python`: ワーカーを起動する python.exe
///
/// # 戻り値
/// ワーカーの起動指定
///
/// # 例
/// `let pool = WorkerPool::start(worker_spec(&config, &python), &config.worker, job);`
pub fn worker_spec(config: &Config, python: &Path) -> WorkerSpec {
	let worker = &config.worker;
	let mut args = vec![
		ARG_CACHEDIR.to_owned(),
		config.paths.ytdlp_cache.to_string_lossy().into_owned(),
		ARG_PREMIUM.to_owned(),
		worker.youtube_premium.clone(),
	];
	if !worker.cookies_browser.is_empty() {
		args.push(ARG_COOKIES.to_owned());
		args.push(worker.cookies_browser.clone());
	}
	if !worker.user_agent.is_empty() {
		args.push(ARG_USER_AGENT.to_owned());
		args.push(worker.user_agent.clone());
	}
	WorkerSpec {
		python: python.to_path_buf(),
		script: config.paths.worker_script.clone(),
		args,
	}
}

/// ワーカーと PipeInput が使う python を選び、結果をログに残す。
///
/// - `paths.worker_envs` の `current.json` が指す python があればそれを使う
/// - 無ければ直前の環境 (`previous`) の python を、警告を出して使う
/// - 記録なし、壊れている、どちらの python も無いときは警告と理由を出し、`paths.worker_python` を使う
///
/// # 引数
/// - `config`: 設定
///
/// # 戻り値
/// (使う python.exe, その出どころの説明 (`PYTHON_ORIGIN_*`))
///
/// # 例
/// `let (python, origin) = choose_python(&config);`
pub(super) fn choose_python(config: &Config) -> (PathBuf, &'static str) {
	let store = EnvStore::new(&config.paths.worker_envs);
	let (python, source) = select_python(&store, &config.paths.worker_python);
	let origin = match source {
		PythonSource::Current(name) => {
			tracing::info!(env = %name, python = %python.display(), "更新済みの環境の python を使います");
			PYTHON_ORIGIN_CURRENT
		}
		PythonSource::Previous { name, current } => {
			tracing::warn!(
				env = %name,
				current = %current,
				python = %python.display(),
				"current.json が指す python が無いため、直前の環境の python を使います"
			);
			PYTHON_ORIGIN_PREVIOUS
		}
		PythonSource::Fallback(reason) => {
			let reason = match reason {
				FallbackReason::NoRecord => "current.json がありません",
				FallbackReason::Corrupt => "current.json が壊れています",
				FallbackReason::MissingPython => {
					"current.json が指す python も、直前の環境の python もありません"
				}
			};
			tracing::warn!(
				reason,
				python = %python.display(),
				"paths.worker_python を使います"
			);
			PYTHON_ORIGIN_CONFIG
		}
	};
	(python, origin)
}

/// ワーカーの python とスクリプトの有無を確かめ、無ければエラーのログを残す。
///
/// - 起動は止めない (ワーカーの起動の失敗はプールが扱い、解決は Unavailable になる)
/// - python は yt-dlp (`yt-dlp[default,curl-cffi]`) を入れた venv の python.exe を指すこと。
///   リポジトリの `worker/.venv` は試験用で yt-dlp を入れていない
///
/// # 引数
/// - `config`: 設定
/// - `python`: 起動時に選んだ python.exe
/// - `origin`: `python` の出どころの説明 (`choose_python` の戻り値)
///
/// # 例
/// `check_worker_files(&config, &python, origin);`
pub(super) fn check_worker_files(config: &Config, python: &Path, origin: &str) {
	tracing::info!(
		python = %python.display(),
		origin,
		"ワーカーの python。yt-dlp[default,curl-cffi] を入れた venv の python.exe を指すこと"
	);
	if !python.is_file() {
		tracing::error!(
			path = %python.display(),
			"{origin} がありません。yt-dlp[default,curl-cffi] を入れた venv の python.exe を指定してください"
		);
	}
	let script = &config.paths.worker_script;
	if !script.is_file() {
		tracing::error!(path = %script.display(), "paths.worker_script がありません");
	}
}
