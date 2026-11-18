//! ログの初期化の試験。日付ごとのファイルへ書かれ、ANSI の色コードが混ざらないことを確かめる。
//! コンソールの出力先が止まっても、ログを出す側が待たないことも確かめる。
//! 水準の指定 (`RUST_LOG` が無いときの依存クレートの水準、不正な `RUST_LOG` の扱い) も確かめる。

use std::path::Path;

use tracing::Level;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry};
use watapachi_bot::config::BotConfig;
use watapachi_bot::logging::{
	build, build_with_console, choose_filter, ensure_logs_dir, LoggingError,
};

/// 試験用の BotConfig を作る。
fn bot_config(level: &str) -> BotConfig {
	BotConfig {
		prefix: "?".to_string(),
		log_level: level.to_string(),
		log_retention_days: 30,
		activity_name: "{prefix}help".to_string(),
	}
}

/// ログのディレクトリにあるファイルがちょうど 1 つであることを確かめ、その内容を返す。
fn read_single_log(dir: &Path) -> String {
	let files: Vec<_> = std::fs::read_dir(dir)
		.unwrap()
		.map(|entry| entry.unwrap().path())
		.collect();
	assert_eq!(files.len(), 1, "log files: {files:?}");
	let name = files[0].file_name().unwrap().to_string_lossy().into_owned();
	assert!(name.starts_with("watapachi-bot."), "name: {name}");
	assert!(name.ends_with(".log"), "name: {name}");
	std::fs::read_to_string(&files[0]).unwrap()
}

#[test]
fn log_file_is_written() {
	let dir = tempfile::tempdir().unwrap();
	let (subscriber, guard) = build(&bot_config("info"), dir.path()).unwrap();
	tracing::subscriber::with_default(subscriber, || tracing::info!("hello-log-test"));
	drop(guard);
	let text = read_single_log(dir.path());
	assert!(text.contains("hello-log-test"));
	assert!(!text.contains('\u{1b}'), "ANSI escape in file: {text:?}");
}

/// 開くまで書き込みを止めるコンソールの出力先 (簡易編集モードで選択中の標準エラーの代わり)。
struct StalledConsole {
	gate: std::sync::mpsc::Receiver<()>,
	opened: bool,
	written: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
}

impl std::io::Write for StalledConsole {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		if !self.opened {
			// 開く合図か、送り手が消えるまで止まる
			let _ = self.gate.recv();
			self.opened = true;
		}
		self.written.lock().unwrap().extend_from_slice(buf);
		Ok(buf.len())
	}

	fn flush(&mut self) -> std::io::Result<()> {
		Ok(())
	}
}

#[test]
fn stalled_console_does_not_block_logging() {
	let dir = tempfile::tempdir().unwrap();
	let (open, gate) = std::sync::mpsc::channel();
	let written = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
	let console = StalledConsole {
		gate,
		opened: false,
		written: std::sync::Arc::clone(&written),
	};
	let (subscriber, guard) = build_with_console(&bot_config("info"), dir.path(), console).unwrap();
	let started = std::time::Instant::now();
	tracing::subscriber::with_default(subscriber, || {
		for i in 0..100 {
			tracing::info!("stalled-console-{i}");
		}
	});
	// コンソールが止まっていても、ログを出す側は待たない
	assert!(
		started.elapsed() < std::time::Duration::from_secs(5),
		"{:?}",
		started.elapsed()
	);
	open.send(()).unwrap();
	drop(guard);
	let text = String::from_utf8_lossy(&written.lock().unwrap()).into_owned();
	assert!(text.contains("stalled-console-99"), "{text}");
	assert!(read_single_log(dir.path()).contains("stalled-console-99"));
}

#[test]
fn build_creates_missing_log_directory() {
	let root = tempfile::tempdir().unwrap();
	let logs = root.path().join("nested").join("logs");
	let (_subscriber, _guard) = build(&bot_config("info"), &logs).unwrap();
	assert!(logs.is_dir());
}

#[test]
fn ensure_logs_dir_reports_io_error_under_a_file() {
	let root = tempfile::tempdir().unwrap();
	let file = root.path().join("f");
	std::fs::write(&file, b"x").unwrap();
	let result = ensure_logs_dir(&file.join("logs"));
	assert!(matches!(result, Err(LoggingError::Io(_))), "{result:?}");
}

#[test]
fn level_filters_lower_events() {
	let dir = tempfile::tempdir().unwrap();
	let (subscriber, guard) = build(&bot_config("warn"), dir.path()).unwrap();
	tracing::subscriber::with_default(subscriber, || {
		tracing::info!("quiet-info");
		tracing::warn!("loud-warn");
	});
	drop(guard);
	let text = read_single_log(dir.path());
	assert!(text.contains("loud-warn"));
	assert!(!text.contains("quiet-info"));
}

/// その指定で依存クレートのイベントが通るか。`songbird_info` が true なら songbird の info、
/// false なら twilight_http の debug を確かめる。
fn passes(filter: EnvFilter, songbird_info: bool) -> bool {
	let subscriber = Registry::default().with(filter);
	tracing::subscriber::with_default(subscriber, || {
		if songbird_info {
			tracing::enabled!(target: "songbird::driver", Level::INFO)
		} else {
			tracing::enabled!(target: "twilight_http::client", Level::DEBUG)
		}
	})
}

#[test]
fn default_filter_quiets_dependencies() {
	let (filter, warning) = choose_filter(None, "debug").unwrap();
	assert!(warning.is_none());
	let text = filter.to_string();
	assert!(text.contains("songbird=warn"), "{text}");
	assert!(text.contains("twilight_http=info"), "{text}");
	assert!(text.contains("twilight_gateway=info"), "{text}");
	// bot.log_level が debug でも、songbird の info と twilight_http の debug は出さない
	assert!(!passes(choose_filter(None, "debug").unwrap().0, true));
	assert!(!passes(choose_filter(None, "debug").unwrap().0, false));
	// 空の RUST_LOG は無いときと同じ
	let (empty, warning) = choose_filter(Some("  "), "debug").unwrap();
	assert!(warning.is_none());
	assert_eq!(empty.to_string(), text);
}

#[test]
fn rust_log_overrides_or_falls_back_with_warning() {
	// 正しい RUST_LOG はそのまま使う (依存クレートも絞らない)
	let (filter, warning) = choose_filter(Some("trace"), "info").unwrap();
	assert!(warning.is_none());
	assert!(passes(filter, false));

	// 不正な RUST_LOG は捨てずに警告し、bot.log_level に戻る
	let (filter, warning) = choose_filter(Some("songbird=notalevel"), "warn").unwrap();
	let warning = warning.expect("警告がある");
	assert!(warning.contains("RUST_LOG"), "{warning}");
	assert!(warning.contains("warn"), "{warning}");
	assert!(filter.to_string().contains("songbird=warn"));

	// bot.log_level 自体が不正ならエラー
	assert!(choose_filter(None, "nope=[").is_err());
}
