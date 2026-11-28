//! ログの初期化。標準エラーと、日付ごとのファイルへ出す。
//! - ファイル名は `watapachi-bot.YYYY-MM-DD.log`。古いファイルは `bot.log_retention_days` 個まで残す
//! - 水準は環境変数 `RUST_LOG` があればそれ、無ければ `QUIET_DEPENDENCIES` と `bot.log_level`。
//!   `RUST_LOG` が不正なら標準エラーに警告を出して、無いときと同じにする
//! - 標準エラーとファイルは、どちらも専用のスレッドで非ブロッキングに書く。ANSI の色コードは標準エラーだけに付く
//!   (コンソールの簡易編集モードで選択している間は標準エラーへの書き込みが止まるため、
//!   ログを出したワーカースレッドや Shard の heartbeat を巻き込まないようにする)
//! - 書き込みが詰まって待ちの行が `BUFFERED_LINES_LIMIT` を超えたら、待たずにその行を捨てる (lossy)
//! - `LogGuard` を破棄するまでにバッファが書き出される。プロセスの終わりまで保持する
//! - `install_panic_hook` で panic の場所と内容をログへ出す。サービスでは標準エラーの出力先が無く、
//!   既定のフックの出力 (spawn したタスクの panic を含む) が失われるため
//!
//! 呼び出し方:
//! ```no_run
//! # use watapachi_bot::config::BotConfig;
//! # fn demo(bot: &BotConfig) -> Result<(), watapachi_bot::logging::LoggingError> {
//! let _guard = watapachi_bot::logging::init(bot, std::path::Path::new("logs"))?;
//! watapachi_bot::logging::install_panic_hook();
//! tracing::info!("started");
//! # Ok(())
//! # }
//! ```

use std::backtrace::{Backtrace, BacktraceStatus};
use std::io::Write;
use std::path::Path;

use tracing::Subscriber;
use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_appender::rolling::{Builder, Rotation};
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

use crate::config::BotConfig;

/// `RUST_LOG` が無いときに `bot.log_level` の前に置く、依存クレートの水準。
/// - songbird は曲のイベントごとに info を出す
/// - twilight_http の debug は、インタラクションのトークンを含む経路を出しうる
///
/// `bot.log_level` に同じクレートの指定があれば、後に置く `bot.log_level` の方が勝つ。
const QUIET_DEPENDENCIES: &str = "songbird=warn,twilight_http=info,twilight_gateway=info";

/// ログファイル名の前置き。
const FILE_PREFIX: &str = "watapachi-bot";
/// ログファイル名の拡張子。
const FILE_SUFFIX: &str = "log";
/// panic の内容が文字列でないときにログへ出す値。
const UNKNOWN_PANIC_PAYLOAD: &str = "(文字列でない panic の値)";
/// panic の場所が分からないときにログへ出す値。
const UNKNOWN_PANIC_LOCATION: &str = "(不明)";
/// 名前の無いスレッドをログへ出すときの値。
const UNNAMED_THREAD: &str = "(名前なし)";
/// 書き込みを待つ行の上限。超えた行は捨てる (tracing-appender の既定値と同じ)。
const BUFFERED_LINES_LIMIT: usize = tracing_appender::non_blocking::DEFAULT_BUFFERED_LINES_LIMIT;
/// 書き込みが詰まったときに、ログを出す側を待たせず行を捨てるか。
/// 待たせると、標準エラーが止まったときにログを出したワーカースレッドまで止まるため true にする
const LOSSY: bool = true;
/// 標準エラーへ書くスレッドの名前。
const CONSOLE_THREAD_NAME: &str = "log-console";
/// ファイルへ書くスレッドの名前。
const FILE_THREAD_NAME: &str = "log-file";

/// ログの初期化で起きるエラー。
#[derive(Debug, thiserror::Error)]
pub enum LoggingError {
	/// ログのディレクトリを作れなかった。
	#[error("ログのディレクトリを作れません: {0}")]
	Io(#[from] std::io::Error),
	/// ログファイルの作成に失敗した。
	#[error("ログファイルを作れません: {0}")]
	Appender(#[from] tracing_appender::rolling::InitError),
	/// `bot.log_level` が水準の指定として解釈できない。
	#[error("ログの水準が不正です: {0}")]
	Filter(#[from] tracing_subscriber::filter::ParseError),
	/// グローバルな購読者が既に設定されている。
	#[error("ログを初期化できません: {0}")]
	Install(#[from] tracing_subscriber::util::TryInitError),
}

/// 標準エラーとファイルへの非ブロッキング書き込みの guard。破棄で残りのバッファを書き出す。
///
/// - 破棄は書き出しを 1 本あたり最長でおよそ 1 秒待つ (tracing-appender の上限)。
///   標準エラーが止まっていても終了は止まらない
pub struct LogGuard {
	_console: WorkerGuard,
	_file: WorkerGuard,
}

/// ログを初期化し、グローバルな購読者として設定する。
///
/// # 引数
/// - `bot`: 水準 (`log_level`) と保持数 (`log_retention_days`)
/// - `logs_dir`: ログファイルのディレクトリ (無ければ作る)
///
/// # 戻り値
/// 保持し続ける `LogGuard`
///
/// # エラー
/// ファイルの作成、水準の解釈、二重の初期化のいずれかに失敗した場合
///
/// # 例
/// `let _guard = init(&config.bot, &config.paths.logs)?;`
pub fn init(bot: &BotConfig, logs_dir: &Path) -> Result<LogGuard, LoggingError> {
	let (subscriber, guard) = build(bot, logs_dir)?;
	subscriber.try_init()?;
	Ok(guard)
}

/// panic の場所・内容・スレッド名を `tracing::error!` へ出すフックを設定する。
///
/// - 既定のフック (標準エラーへの出力) は呼ばない。コンソールでは標準エラーの層が同じ内容を出すため、
///   サービスでは出力先が無いため
/// - バックトレースは `RUST_BACKTRACE` (または `RUST_LIB_BACKTRACE`) で有効なときだけ添える
/// - `catch_unwind` で捕まえる panic (DbWriter の処理など) でもフックは呼ばれ、ログに残る
/// - 出力はそのスレッドの購読者へ行く。`init` の後に呼ぶこと
/// - 後から呼ぶと前のフックを置き換える
///
/// # 例
/// ```no_run
/// # use watapachi_bot::config::BotConfig;
/// # fn demo(bot: &BotConfig) -> Result<(), watapachi_bot::logging::LoggingError> {
/// let _guard = watapachi_bot::logging::init(bot, std::path::Path::new("logs"))?;
/// watapachi_bot::logging::install_panic_hook();
/// # Ok(())
/// # }
/// ```
pub fn install_panic_hook() {
	std::panic::set_hook(Box::new(|info| {
		// PanicHookInfo::payload_as_str は MSRV (1.89) より新しいため、型を見て取り出す
		let payload = info.payload();
		let message = payload
			.downcast_ref::<&str>()
			.copied()
			.or_else(|| payload.downcast_ref::<String>().map(String::as_str))
			.unwrap_or(UNKNOWN_PANIC_PAYLOAD);
		let location = info.location().map_or_else(
			|| UNKNOWN_PANIC_LOCATION.to_owned(),
			|location| location.to_string(),
		);
		let thread = std::thread::current();
		let thread_name = thread.name().unwrap_or(UNNAMED_THREAD);
		let backtrace = Backtrace::capture();
		if backtrace.status() == BacktraceStatus::Captured {
			tracing::error!(
				thread = thread_name,
				%location,
				%backtrace,
				"panic が起きました: {message}"
			);
		} else {
			tracing::error!(thread = thread_name, %location, "panic が起きました: {message}");
		}
	}));
}

/// `init` の中身。購読者を作るだけで設定はしない (試験で `with_default` に渡すため分ける)。
///
/// - コンソールの出力先は標準エラー
///
/// # 引数
/// - `bot`: 水準と保持数
/// - `logs_dir`: ログファイルのディレクトリ
///
/// # 戻り値
/// 購読者と、書き込みの guard
///
/// # エラー
/// ディレクトリかファイルの作成、水準の解釈に失敗した場合
///
/// # 例
/// `let (subscriber, guard) = build(&bot, dir)?;`
pub fn build(
	bot: &BotConfig,
	logs_dir: &Path,
) -> Result<(impl Subscriber + Send + Sync, LogGuard), LoggingError> {
	build_with_console(bot, logs_dir, std::io::stderr())
}

/// `build` のコンソールの出力先を差し替えられる版 (試験で止まる出力先を渡すため分ける)。
///
/// - `console` へは専用のスレッドから書く。`console` が止まっても、ログを出す側は待たない
///
/// # 引数
/// - `bot`: 水準と保持数
/// - `logs_dir`: ログファイルのディレクトリ
/// - `console`: コンソールの出力先 (本番は標準エラー)。ANSI の色コード付きで書く
///
/// # 戻り値
/// 購読者と、書き込みの guard
///
/// # エラー
/// ディレクトリかファイルの作成、水準の解釈に失敗した場合
///
/// # 例
/// `let (subscriber, guard) = build_with_console(&bot, dir, std::io::stderr())?;`
pub fn build_with_console<W: Write + Send + 'static>(
	bot: &BotConfig,
	logs_dir: &Path,
	console: W,
) -> Result<(impl Subscriber + Send + Sync, LogGuard), LoggingError> {
	let rust_log = std::env::var(EnvFilter::DEFAULT_ENV).ok();
	let (filter, warning) = choose_filter(rust_log.as_deref(), &bot.log_level)?;
	if let Some(warning) = warning {
		// 購読者を設定する前のため、標準エラーへ直に出す
		eprintln!("{warning}");
	}
	let mut builder = Builder::new()
		.rotation(Rotation::DAILY)
		.filename_prefix(FILE_PREFIX)
		.filename_suffix(FILE_SUFFIX);
	// 0 は「保持なし」ではなく無制限として扱う (誤設定でログが消えるのを避ける)
	if bot.log_retention_days > 0 {
		builder = builder.max_log_files(bot.log_retention_days as usize);
	}
	ensure_logs_dir(logs_dir)?;
	let appender = builder.build(logs_dir)?;
	let (file_writer, file_guard) = non_blocking(FILE_THREAD_NAME).finish(appender);
	let (console_writer, console_guard) = non_blocking(CONSOLE_THREAD_NAME).finish(console);
	let subscriber = Registry::default()
		.with(filter)
		.with(fmt::layer().with_writer(console_writer))
		.with(fmt::layer().with_writer(file_writer).with_ansi(false));
	let guard = LogGuard {
		_console: console_guard,
		_file: file_guard,
	};
	Ok((subscriber, guard))
}

/// 非ブロッキングの書き手の設定 (待ちの行の上限、lossy、スレッド名)。
///
/// # 引数
/// - `thread_name`: 書き込みのスレッドの名前
///
/// # 戻り値
/// `finish(writer)` で書き手と guard を作る設定
///
/// # 例
/// `let (writer, guard) = non_blocking(FILE_THREAD_NAME).finish(appender);`
fn non_blocking(thread_name: &str) -> NonBlockingBuilder {
	NonBlockingBuilder::default()
		.buffered_lines_limit(BUFFERED_LINES_LIMIT)
		.lossy(LOSSY)
		.thread_name(thread_name)
}

/// ログのディレクトリを (親も含めて) 作る。既にあれば何もしない。
///
/// tracing-appender は、ディレクトリが無いと古いファイルの確認で警告を 1 行出すため、先に作っておく。
///
/// # 引数
/// - `logs_dir`: ログファイルのディレクトリ
///
/// # 戻り値
/// 作れた (または既にあった) 場合は `Ok(())`
///
/// # エラー
/// 作れなかった場合 (パスの途中がファイルなど) は `LoggingError::Io`
///
/// # 例
/// `ensure_logs_dir(Path::new("logs"))?;`
pub fn ensure_logs_dir(logs_dir: &Path) -> Result<(), LoggingError> {
	std::fs::create_dir_all(logs_dir)?;
	Ok(())
}

/// ログの水準の指定を決める (環境変数を読まない。試験のため分ける)。
///
/// - `rust_log` が空でなく解釈できれば、それだけを使う
/// - 無いか空なら、`QUIET_DEPENDENCIES` の後に `log_level` を置いた指定
/// - 解釈できなければ、無いときと同じ指定に戻し、警告の文言を返す
///
/// # 引数
/// - `rust_log`: 環境変数 `RUST_LOG` の値 (無ければ None)
/// - `log_level`: `bot.log_level`
///
/// # 戻り値
/// 水準の指定と、標準エラーへ出す警告 (無ければ None)
///
/// # エラー
/// `log_level` を解釈できない
///
/// # 例
/// `let (filter, warning) = choose_filter(std::env::var("RUST_LOG").ok().as_deref(), "info")?;`
pub fn choose_filter(
	rust_log: Option<&str>,
	log_level: &str,
) -> Result<(EnvFilter, Option<String>), LoggingError> {
	let fallback = || EnvFilter::try_new(format!("{QUIET_DEPENDENCIES},{log_level}"));
	let Some(value) = rust_log.filter(|value| !value.trim().is_empty()) else {
		return Ok((fallback()?, None));
	};
	match EnvFilter::try_new(value) {
		Ok(filter) => Ok((filter, None)),
		Err(error) => {
			let warning = format!(
				"環境変数 {} を解釈できないため、bot.log_level ({log_level}) を使います: {error}",
				EnvFilter::DEFAULT_ENV
			);
			Ok((fallback()?, Some(warning)))
		}
	}
}
