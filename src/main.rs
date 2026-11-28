//! WataPachiBOT の実行入口。
//! - `watapachi-bot run` (引数なしも同じ) でコンソールで動かす
//! - `watapachi-bot update-worker` でワーカーの環境を作る (Bot の停止中だけ)
//! - `watapachi-bot stop` で動いている Bot に終了処理をさせ、止まるまで待つ
//! - `watapachi-bot service install|uninstall|run` で Windows サービスを操作する
//! - それ以外は使い方を表示して終了コード 2 で終える
//! - 相対パスはカレントディレクトリ基準
//! - どのサブコマンドでも、最初に自分の優先度を確かめ、低ければ NORMAL へ上げる
//!   (タスクスケジューラの既定の優先度で起動しても、ワーカーなどの子プロセスを NORMAL で起動するため)

use std::future::Future;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::Context as _;
use watapachi_bot::app::{self, AppPaths, Finished};
use watapachi_bot::config::Config;
use watapachi_bot::platform::priority;
use watapachi_bot::platform::service::{self, InstallOptions};
use watapachi_bot::platform::single_instance::{
	is_already_running, EXIT_CODE_ALREADY_RUNNING, MUTEX_NAME,
};
use watapachi_bot::platform::stop_request::{request_stop, StopOutcome, STOP_EVENT_NAME};
use watapachi_bot::update::procedure::UpdateOutcome;

/// コンソールで実行するサブコマンド。
const COMMAND_RUN: &str = "run";
/// ワーカーの環境を作るサブコマンド。
const COMMAND_UPDATE_WORKER: &str = "update-worker";
/// 動いている Bot を止めるサブコマンド。
const COMMAND_STOP: &str = "stop";
/// Windows サービスの操作のサブコマンド。
const COMMAND_SERVICE: &str = "service";
/// `service` の操作: 登録する。
const SERVICE_INSTALL: &str = "install";
/// `service` の操作: 解除する。
const SERVICE_UNINSTALL: &str = "uninstall";
/// `service` の操作: サービスとして実行する (サービス制御マネージャーが呼ぶ)。
const SERVICE_RUN: &str = "run";

/// 使い方の表示。
const USAGE: &str = "使い方: watapachi-bot [run | update-worker | stop | service <install | uninstall>]
  run                コンソールで実行する (省略時も同じ)。Ctrl+C で終了する
  update-worker      Bot を止めている間に yt-dlp のワーカーの環境を作る (最初の環境もこれで作る)
  stop               動いている Bot に終了処理をさせ、止まるまで待つ (別のセッションからも送れる)
  service install    Windows サービスとして登録する (管理者権限が要る。カレントディレクトリを作業ディレクトリにする)
  service uninstall  Windows サービスを解除する (管理者権限が要る)";
/// `service` の後の操作が不正なときの表示。
const SERVICE_USAGE: &str = "使い方: watapachi-bot service <install | uninstall>";
/// `service run` を手で実行したときの表示。
const SERVICE_NOT_STARTED: &str =
	"service run は Windows のサービス制御マネージャーが呼ぶものです。手では実行できません。
コンソールで動かすには `watapachi-bot run` を使ってください";
/// 実行するアカウントを決める環境変数。
const ENV_USERNAME: &str = "USERNAME";
/// ローカルアカウントを指す接頭辞。
const LOCAL_ACCOUNT_PREFIX: &str = ".\\";
/// パスワードの入力の案内。
const PASSWORD_PROMPT: &str = "このアカウントの Windows のパスワード (画面には出ません): ";
/// 登録後の起動の案内。
const SERVICE_START_HINT: &str =
	"登録しました。`sc start WataPachiBOT` で起動できます (次回の Windows 起動からは自動で起動します)";
/// 解除後の案内。
const SERVICE_UNINSTALLED: &str = "解除しました";
/// 設定を読めないときに使う `shutdown.timeout_secs` の既定値 (秒)。
const DEFAULT_SHUTDOWN_TIMEOUT_SECS: u64 = 20;
/// 解除で停止を待つ時間に、終了処理の上限へ足す余裕 (秒)。
const UNINSTALL_WAIT_MARGIN_SECS: u64 = 10;
/// `stop` で Bot が終わったときの表示。
const STOP_DONE: &str = "Bot は終了処理をして止まりました";
/// `stop` で Bot が動いていなかったときの表示。
const STOP_NOT_RUNNING: &str = "Bot は動いていません";

/// 優先度クラスを上げたときの表示 (後に元のクラスの名前を続ける)。
const PRIORITY_CLASS_RAISED: &str = "プロセスの優先度クラスを NORMAL へ上げました。元のクラス";
/// 優先度クラスを確かめられないときの表示 (後に OS のエラーを続ける)。
const PRIORITY_CLASS_FAILED: &str = "プロセスの優先度クラスを確かめられません (このまま続けます)";
/// メモリ優先度を上げたときの表示 (後に元の値を続ける)。
const PRIORITY_MEMORY_RAISED: &str = "プロセスのメモリ優先度を NORMAL へ上げました。元の値";
/// メモリ優先度を確かめられないときの表示 (後に OS のエラーを続ける)。
const PRIORITY_MEMORY_FAILED: &str = "プロセスのメモリ優先度を確かめられません (このまま続けます)";

/// 使い方の誤りの終了コード。
const EXIT_USAGE: u8 = 2;

/// 引数を解釈して実行する。
///
/// # 戻り値
/// 正常終了は 0、起動か実行の失敗は 1、使い方の誤りと未実装のサブコマンドは 2、
/// `run` で既に起動していたときは `EXIT_CODE_ALREADY_RUNNING`
///
/// # 例
/// `watapachi-bot run`
fn main() -> ExitCode {
	raise_priority();
	let args: Vec<String> = std::env::args().skip(1).collect();
	match args.first().map(String::as_str) {
		None => run(),
		Some(COMMAND_RUN) if args.len() == 1 => run(),
		Some(COMMAND_UPDATE_WORKER) if args.len() == 1 => update_worker(),
		Some(COMMAND_STOP) if args.len() == 1 => stop(),
		Some(COMMAND_SERVICE) => service_command(&args[1..]),
		Some(_) => {
			eprintln!("{USAGE}");
			ExitCode::from(EXIT_USAGE)
		}
	}
}

/// 自分の優先度が低ければ NORMAL へ上げ、結果を標準出力か標準エラーへ書く。
///
/// - ログの初期化の前に呼ぶため、tracing ではなく標準出力と標準エラーへ書く
/// - 失敗しても起動は続ける
///
/// # 例
/// `raise_priority();`
fn raise_priority() {
	let report = priority::raise_to_normal();
	match report.class {
		Ok(Some(from)) => println!("{PRIORITY_CLASS_RAISED}: {}", priority::class_name(from)),
		Ok(None) => {}
		Err(error) => eprintln!("{PRIORITY_CLASS_FAILED}: {error}"),
	}
	match report.memory {
		Ok(Some(from)) => println!("{PRIORITY_MEMORY_RAISED}: {from}"),
		Ok(None) => {}
		Err(error) => eprintln!("{PRIORITY_MEMORY_FAILED}: {error}"),
	}
}

/// Tokio のランタイムを作り、`app::run` を最後まで実行する。
///
/// - 失敗は標準エラーへ書く (ログの初期化の前の失敗もあるため)
///
/// # 戻り値
/// 成功なら 0、既に起動していたなら `EXIT_CODE_ALREADY_RUNNING`、それ以外の失敗なら 1
///
/// # 例
/// `let code = run();`
fn run() -> ExitCode {
	let Some(result) = block_on_app(app::run(AppPaths::standard())) else {
		return ExitCode::FAILURE;
	};
	match result {
		Ok(()) => ExitCode::SUCCESS,
		Err(error) => {
			eprintln!("エラー: {error:#}");
			// 起動スクリプトが起動し直しをやめられるよう、二重起動だけは別の終了コードにする
			if is_already_running(&error) {
				ExitCode::from(EXIT_CODE_ALREADY_RUNNING)
			} else {
				ExitCode::FAILURE
			}
		}
	}
}

/// Tokio のランタイムを作って `future` を最後まで動かし、ランタイムを閉じてからログの guard を破棄する。
///
/// - ランタイムを作れないときは標準エラーへ書く (ログの初期化の前のため)
///
/// # 引数
/// - `future`: `app::run` か `app::update_worker`
///
/// # 戻り値
/// 実行の結果。ランタイムを作れなかったときは None
///
/// # 例
/// `let Some(result) = block_on_app(app::run(AppPaths::standard())) else { return ExitCode::FAILURE };`
fn block_on_app<T>(future: impl Future<Output = Finished<T>>) -> Option<anyhow::Result<T>> {
	let runtime = match app::build_runtime() {
		Ok(runtime) => runtime,
		Err(error) => {
			eprintln!("非同期ランタイムを作れません: {error}");
			return None;
		}
	};
	let finished = runtime.block_on(future);
	Some(app::shutdown_runtime(runtime, finished))
}

/// `update-worker`: ワーカーの環境を作り、結果を標準出力へ書く。
///
/// # 戻り値
/// `Updated` と `Unchanged` は 0、`Failed`・`Stopped`・Bot の起動中・その他の失敗は 1
///
/// # 例
/// `let code = update_worker();`
fn update_worker() -> ExitCode {
	let Some(result) = block_on_app(app::update_worker(AppPaths::standard())) else {
		return ExitCode::FAILURE;
	};
	match result {
		Ok(outcome) => {
			println!("{}", app::describe_outcome(&outcome));
			match outcome {
				UpdateOutcome::Updated { .. } | UpdateOutcome::Unchanged { .. } => {
					ExitCode::SUCCESS
				}
				UpdateOutcome::Failed { .. } | UpdateOutcome::Stopped => ExitCode::FAILURE,
			}
		}
		Err(error) => {
			eprintln!("エラー: {error:#}");
			ExitCode::FAILURE
		}
	}
}

/// `stop`: 動いている Bot に停止の命令を送り、止まるまで待つ。
///
/// - 待つ上限は `service::stop_wait_hint(shutdown.timeout_secs)` (サービスの停止の待ちの目安と同じ)
/// - 動いていなかったときも 0 を返す (スクリプトから何度呼んでもよいように)
///
/// # 戻り値
/// 止まった、動いていなかったは 0。対応していない、上限までに止まらない、OS の拒否は 1
///
/// # 例
/// `let code = stop();`
fn stop() -> ExitCode {
	let wait = service::stop_wait_hint(shutdown_timeout_secs());
	match request_stop(STOP_EVENT_NAME, MUTEX_NAME, wait) {
		Ok(StopOutcome::Stopped) => {
			println!("{STOP_DONE}");
			ExitCode::SUCCESS
		}
		Ok(StopOutcome::NotRunning) => {
			println!("{STOP_NOT_RUNNING}");
			ExitCode::SUCCESS
		}
		Err(error) => {
			eprintln!("エラー: {error}");
			ExitCode::FAILURE
		}
	}
}

/// `service` の後の操作を振り分ける。
///
/// # 引数
/// - `args`: `service` の後の引数 (`install`、`uninstall`、`run`)
///
/// # 戻り値
/// 成功は 0、失敗は 1、使い方の誤りは 2
///
/// # 例
/// `service_command(&["install".to_string()])`
fn service_command(args: &[String]) -> ExitCode {
	let operation = args.first().map(String::as_str);
	let result = match operation {
		Some(SERVICE_INSTALL) if args.len() == 1 => service_install(),
		Some(SERVICE_UNINSTALL) if args.len() == 1 => service_uninstall(),
		// 起動引数 (`--dir`) は service 側が読む
		Some(SERVICE_RUN) => return service_run(),
		_ => {
			eprintln!("{SERVICE_USAGE}");
			return ExitCode::from(EXIT_USAGE);
		}
	};
	match result {
		Ok(()) => ExitCode::SUCCESS,
		Err(error) => {
			eprintln!("エラー: {error:#}");
			ExitCode::FAILURE
		}
	}
}

/// カレントディレクトリの `shutdown.timeout_secs` を読む。読めなければ既定値。
///
/// - `service uninstall` と `stop` が使う (設定が壊れていても行えるようにするため)。
///   `service install` は設定を検証して、読めなければ登録しない
///
/// # 戻り値
/// 終了処理の上限 (秒)
///
/// # 例
/// `let secs = shutdown_timeout_secs();`
fn shutdown_timeout_secs() -> u64 {
	Config::load(std::path::Path::new(app::CONFIG_PATH))
		.map(|config| config.shutdown.timeout_secs)
		.unwrap_or(DEFAULT_SHUTDOWN_TIMEOUT_SECS)
}

/// `service install`: カレントディレクトリを作業ディレクトリとして登録する。
///
/// - アカウントは `.\<USERNAME>`。パスワードは対話で読み、保存しない
/// - パスワードを尋ねる前に、作業ディレクトリの config.toml を読んで検証する。誤りがあれば登録しない
///
/// # エラー
/// 環境の取得、設定の読み込みか検証、パスワードの入力、登録のいずれかの失敗
///
/// # 例
/// `service_install()?;`
fn service_install() -> anyhow::Result<()> {
	let exe = std::env::current_exe().context("実行ファイルの場所を得られません")?;
	let dir = std::env::current_dir().context("カレントディレクトリを得られません")?;
	// 登録の中でも読み直す。ここではパスワードを尋ねる前に誤りを知らせるために読む
	service::load_install_config(&dir)?;
	let user = std::env::var(ENV_USERNAME)
		.with_context(|| format!("環境変数 {ENV_USERNAME} を読めません"))?;
	let account = format!("{LOCAL_ACCOUNT_PREFIX}{user}");
	println!("アカウント: {account}");
	println!("作業ディレクトリ: {}", dir.display());
	let password = rpassword::prompt_password(PASSWORD_PROMPT).context("パスワードを読めません")?;
	service::install(&InstallOptions {
		exe,
		dir,
		account,
		password,
	})?;
	println!("{SERVICE_START_HINT}");
	Ok(())
}

/// `service uninstall`: 動いていれば止めて解除する。
///
/// # エラー
/// 登録が無い、停止が間に合わない、管理者権限が無いなど
///
/// # 例
/// `service_uninstall()?;`
fn service_uninstall() -> anyhow::Result<()> {
	let wait = Duration::from_secs(shutdown_timeout_secs() + UNINSTALL_WAIT_MARGIN_SECS);
	service::uninstall(wait)?;
	println!("{SERVICE_UNINSTALLED}");
	Ok(())
}

/// `service run`: サービス制御マネージャーへ接続して実行する。
///
/// # 戻り値
/// 正常終了は 0。手で実行した (制御マネージャーにつながらない) ときは案内を出して 1
///
/// # 例
/// `service_run()`
fn service_run() -> ExitCode {
	match service::run_dispatcher() {
		Ok(()) => ExitCode::SUCCESS,
		Err(error) if error.is_not_started_as_service() => {
			eprintln!("{SERVICE_NOT_STARTED}");
			ExitCode::FAILURE
		}
		Err(error) => {
			eprintln!("エラー: {error}");
			ExitCode::FAILURE
		}
	}
}
