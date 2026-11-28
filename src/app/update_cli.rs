//! Bot を止めている間のワーカーの環境の更新 (`watapachi-bot update-worker`) と、更新役の組み立て。
//! - `update_worker`: 設定とログを用意し、二重起動の防止の mutex を取って更新の手順を 1 回行う
//! - `build_updater`: 常駐の Scheduler と `update-worker` で同じ更新役を作る
//! - `describe_outcome`: 更新の結果を運用者向けの 1 行にする (純粋)
//!
//! 呼び出し方:
//! ```ignore
//! let finished = runtime.block_on(app::update_worker(AppPaths::standard()));
//! let outcome = app::shutdown_runtime(runtime, finished)?;
//! println!("{}", app::describe_outcome(&outcome));
//! ```

use std::sync::Arc;

use anyhow::{anyhow, Context as _};

use super::{
	acquire_instance, init_logging, install_crypto_provider, load_config, worker_spec, AppPaths,
	Finished,
};
use crate::config::Config;
use crate::logging::LogGuard;
use crate::platform::job::Job;
use crate::resolver::process::WorkerSpec;
use crate::update::envs::EnvStore;
use crate::update::procedure::{UpdateOutcome, Updater};
use crate::update::tools::UvTools;

/// `update-worker` で Bot が動いていたときの文言。
pub const UPDATE_WORKER_BOT_RUNNING: &str = "Bot の起動中は使えません。止めてから実行してください";
/// 更新の結果の表示: 更新した。
const OUTCOME_UPDATED: &str = "更新しました: yt-dlp";
/// 更新の結果の表示: 同じ版だった。
const OUTCOME_UNCHANGED: &str = "更新はありません: yt-dlp";
/// 更新の結果の表示: 失敗した。
const OUTCOME_FAILED: &str = "失敗しました";
/// 更新の結果の表示: 途中で終了した。
const OUTCOME_STOPPED: &str = "途中で終了しました。現在の環境は変わっていません";

/// Bot を止めている間にワーカーの環境を作る (`watapachi-bot update-worker`)。
///
/// - 設定を読み、ログを初期化し、二重起動の防止の mutex を取る (Bot の起動中は取れず Err)
/// - 入れ替え先なしで `Updater::run` を実行する (検査まで成功したら `current.json` を書く)
/// - 最初の環境を作る用途にも使う
/// - ログの guard は結果と一緒に返す。ランタイムを閉じるまで残すため、`shutdown_runtime` に渡すこと
///
/// # 引数
/// - `paths`: 設定とメッセージの場所
///
/// # 戻り値
/// 更新の結果とログの guard。`Failed` と `Stopped` を終了コードに変えるのは呼び出し側
///
/// # エラー
/// (`Finished::result` の Err) 設定の読み込み、ログの初期化、Job の作成の失敗。Bot が起動中のとき
///
/// # 例
/// `let finished = runtime.block_on(update_worker(AppPaths::standard()));`
pub async fn update_worker(paths: AppPaths) -> Finished<UpdateOutcome> {
	let mut log_guard = None;
	let result = run_update_worker(&paths, &mut log_guard).await;
	Finished { result, log_guard }
}

/// `update_worker` の本体。ログを初期化したら guard を `log_guard` に置く。
///
/// # 引数
/// - `paths`: 設定とメッセージの場所
/// - `log_guard`: ログの guard の置き場所。ログの初期化の前に失敗したら None のまま
///
/// # 戻り値
/// 更新の結果
///
/// # エラー
/// `update_worker` と同じ
///
/// # 例
/// `let result = run_update_worker(&paths, &mut log_guard).await;`
async fn run_update_worker(
	paths: &AppPaths,
	log_guard: &mut Option<LogGuard>,
) -> anyhow::Result<UpdateOutcome> {
	install_crypto_provider();

	let config = load_config(&paths.config)?;
	*log_guard = Some(init_logging(&config)?);

	let _instance = acquire_instance(|| anyhow!(UPDATE_WORKER_BOT_RUNNING))?;
	let job = Arc::new(Job::new().context("Job Object を作れません")?);

	let template = worker_spec(&config, &config.paths.worker_python);
	let updater = build_updater(&config, template, job);
	let outcome = updater.run(None, jiff::ZonedDateTime::now()).await;
	tracing::info!(?outcome, "update-worker を終えました");
	Ok(outcome)
}

/// ワーカーの環境の更新役を作る。常駐の Scheduler と `update-worker` で同じ組み立てを使う。
///
/// # 引数
/// - `config`: 設定全体 (環境の置き場所、残す Python、毎日の入れ替えの有無)
/// - `template`: 検査用のワーカーの起動の雛形
/// - `job`: 起動する子プロセスを入れる Job Object
///
/// # 戻り値
/// 更新役
///
/// # 例
/// `let updater = build_updater(&config, worker_spec(&config, python), job);`
pub(super) fn build_updater(config: &Config, template: WorkerSpec, job: Arc<Job>) -> Updater {
	let tools = UvTools::from_config(config, template, job);
	Updater::new(
		EnvStore::new(&config.paths.worker_envs).keep_python(&config.paths.worker_python),
		Arc::new(tools),
		config.update.recycle_workers_daily,
	)
}

/// `UpdateOutcome` を運用者向けの 1 行にする (純粋)。
///
/// # 引数
/// - `outcome`: 更新の結果
///
/// # 戻り値
/// 版や失敗の段と理由を含む 1 行
///
/// # 例
/// `println!("{}", describe_outcome(&outcome));`
pub fn describe_outcome(outcome: &UpdateOutcome) -> String {
	match outcome {
		UpdateOutcome::Updated { version } => format!("{OUTCOME_UPDATED} {version}"),
		UpdateOutcome::Unchanged { version, .. } => format!("{OUTCOME_UNCHANGED} {version}"),
		UpdateOutcome::Failed { stage, message } => {
			format!("{OUTCOME_FAILED} ({}): {message}", stage.label())
		}
		UpdateOutcome::Stopped => OUTCOME_STOPPED.to_owned(),
	}
}
