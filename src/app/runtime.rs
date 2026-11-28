//! 非同期ランタイムの作成と片付け。コンソールの実行、`update-worker`、サービスで同じものを使う。
//! - ランタイムを閉じてからログの guard を破棄する。閉じる間に残ったタスクや panic が出すログを
//!   書き出すため (guard を先に破棄すると、その後のログは書かれずに失われる)
//!
//! 呼び出し方:
//! ```ignore
//! let runtime = app::build_runtime()?;
//! let finished = runtime.block_on(app::run(AppPaths::standard()));
//! app::shutdown_runtime(runtime, finished)?;
//! ```

use std::io;
use std::time::Duration;

use tokio::runtime::Runtime;

use crate::logging::LogGuard;

/// 実行を終えた後、ランタイムに残った処理 (ブロッキングのスレッドなど) を待つ上限。
/// 終了処理の上限 (`shutdown.timeout_secs`) の後に足されるため短く取る。
const RUNTIME_SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

/// 実行の結果と、ログの guard。`shutdown_runtime` に渡して片付ける。
#[must_use = "shutdown_runtime に渡し、ランタイムを閉じてからログの guard を破棄すること"]
pub struct Finished<T> {
	/// 実行の結果
	pub result: anyhow::Result<T>,
	/// ログの guard。ログの初期化の前に失敗したときは None
	pub log_guard: Option<LogGuard>,
}

/// マルチスレッドのランタイムを作る (I/O と時計を有効にする)。
///
/// # 戻り値
/// 作ったランタイム
///
/// # エラー
/// OS がスレッドなどを用意できないとき
///
/// # 例
/// `let runtime = build_runtime()?;`
pub fn build_runtime() -> io::Result<Runtime> {
	tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
}

/// ランタイムを `RUNTIME_SHUTDOWN_GRACE` を上限に閉じ、その後でログの guard を破棄する。
///
/// - guard の破棄は残りのバッファの書き出しを待つ (1 本あたり最長でおよそ 1 秒)
///
/// # 引数
/// - `runtime`: `block_on` を終えたランタイム
/// - `finished`: `block_on` の戻り値
///
/// # 戻り値
/// `finished` の実行の結果
///
/// # エラー
/// `finished.result` の Err をそのまま返す
///
/// # 例
/// `let outcome = shutdown_runtime(runtime, finished)?;`
pub fn shutdown_runtime<T>(runtime: Runtime, finished: Finished<T>) -> anyhow::Result<T> {
	let Finished { result, log_guard } = finished;
	runtime.shutdown_timeout(RUNTIME_SHUTDOWN_GRACE);
	drop(log_guard);
	result
}
