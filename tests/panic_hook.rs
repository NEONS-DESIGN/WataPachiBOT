//! panic のフック (`logging::install_panic_hook`) の試験。
//!
//! - フックはプロセス全体で 1 つのため、この試験ファイルには試験を 1 つだけ置く
//!   (他の試験の失敗の表示をフックが奪わないようにする)

use watapachi_bot::config::BotConfig;
use watapachi_bot::logging::{build, install_panic_hook};

/// 同期の処理で起こす panic の内容。
const SYNC_MESSAGE: &str = "panic-hook-sync-message";
/// spawn したタスクで起こす panic の内容。
const TASK_MESSAGE: &str = "panic-hook-task-message";

/// 試験用の BotConfig を作る。
fn bot_config() -> BotConfig {
	BotConfig {
		prefix: "?".to_string(),
		log_level: "info".to_string(),
		log_retention_days: 30,
		activity_name: "{prefix}help".to_string(),
	}
}

#[test]
fn panic_location_and_message_are_logged() {
	let dir = tempfile::tempdir().unwrap();
	let (subscriber, guard) = build(&bot_config(), dir.path()).unwrap();
	install_panic_hook();
	tracing::subscriber::with_default(subscriber, || {
		let caught = std::panic::catch_unwind(|| panic!("{}", SYNC_MESSAGE));
		assert!(caught.is_err());
		// spawn したタスクの panic は JoinError になり、内容はフックにしか残らない
		let runtime = tokio::runtime::Builder::new_current_thread()
			.build()
			.unwrap();
		let joined =
			runtime.block_on(async { tokio::spawn(async { panic!("{}", TASK_MESSAGE) }).await });
		assert!(joined.unwrap_err().is_panic());
	});
	// 既定のフックへ戻す (この後の失敗の表示のため)
	let _ = std::panic::take_hook();
	drop(guard);

	let files: Vec<_> = std::fs::read_dir(dir.path())
		.unwrap()
		.map(|entry| entry.unwrap().path())
		.collect();
	assert_eq!(files.len(), 1, "log files: {files:?}");
	let text = std::fs::read_to_string(&files[0]).unwrap();
	for message in [SYNC_MESSAGE, TASK_MESSAGE] {
		let line = text
			.lines()
			.find(|line| line.contains(message))
			.unwrap_or_else(|| panic!("{message} がログにありません: {text}"));
		assert!(line.contains("ERROR"), "line: {line}");
		assert!(line.contains("panic_hook.rs"), "場所が無い: {line}");
	}
}
