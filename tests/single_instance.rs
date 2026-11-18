//! 二重起動の防止の試験。Local 名前空間の名前を使い、特権なしで動かす。

use anyhow::Context as _;
use watapachi_bot::platform::single_instance::{
	is_already_running, SingleInstance, SingleInstanceError, EXIT_CODE_ALREADY_RUNNING,
};

/// 起動スクリプト。二重起動の終了コードを同じ値で持っているかを確かめる。
const START_SCRIPT: &str = include_str!("../tools/start.ps1");

#[test]
fn second_acquire_fails_until_drop() {
	let name = format!("Local\\WataPachiBOT-test-{}", std::process::id());
	let first = SingleInstance::acquire(&name).unwrap();
	assert!(matches!(
		SingleInstance::acquire(&name),
		Err(SingleInstanceError::AlreadyRunning)
	));
	drop(first);
	assert!(SingleInstance::acquire(&name).is_ok());
}

#[test]
fn already_running_is_found_through_context() {
	let bare = anyhow::Error::new(SingleInstanceError::AlreadyRunning);
	assert!(is_already_running(&bare));
	let wrapped: anyhow::Result<()> =
		Err(SingleInstanceError::AlreadyRunning).context("起動できません");
	assert!(is_already_running(&wrapped.unwrap_err()));
}

#[test]
fn other_errors_are_not_already_running() {
	let os = anyhow::Error::new(SingleInstanceError::Os(std::io::Error::other("x")));
	assert!(!is_already_running(&os));
	// 文字列にした場合は型が失われ、見分けられない
	let text = anyhow::anyhow!("既に起動しています");
	assert!(!is_already_running(&text));
}

#[test]
fn start_script_uses_same_exit_code() {
	let line = format!("$ExitCodeAlreadyRunning = {EXIT_CODE_ALREADY_RUNNING}");
	assert!(
		START_SCRIPT.lines().any(|l| l.trim() == line),
		"tools/start.ps1 に `{line}` が無い"
	);
}
