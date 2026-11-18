//! Windows サービスの純粋な部分の試験。登録そのものは管理者権限が要るため行わない。
//! - 登録の前の設定の検証 (`load_install_config`) は、一時ディレクトリの config.toml で確かめる

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tempfile::TempDir;
use watapachi_bot::app;
use watapachi_bot::config::ConfigError;
use watapachi_bot::platform::service::{
	failure_actions, launch_arguments, load_install_config, parse_dir, stop_wait_hint, ServiceError,
};
use windows_service::service::ServiceFailureResetPeriod;

#[test]
fn launch_arguments_round_trip() {
	let dir = PathBuf::from(r"C:\bot dir\WataPachiBOT");
	let args = launch_arguments(&dir);
	assert_eq!(
		args[..2],
		[OsString::from("service"), OsString::from("run")]
	);
	assert_eq!(parse_dir(&args[2..]), Some(dir));
}

#[test]
fn parse_dir_missing() {
	assert_eq!(parse_dir(&[OsString::from("--dir")]), None);
	assert_eq!(parse_dir(&[]), None);
}

#[test]
fn failure_actions_restart_with_backoff() {
	let actions = failure_actions();
	let delays: Vec<u64> = actions
		.actions
		.unwrap()
		.iter()
		.map(|a| a.delay.as_secs())
		.collect();
	assert_eq!(delays, [10, 60, 300]);
	assert_eq!(
		actions.reset_period,
		ServiceFailureResetPeriod::After(Duration::from_secs(86400))
	);
}

#[test]
fn stop_wait_hint_adds_margin() {
	assert_eq!(stop_wait_hint(20), Duration::from_secs(25));
}

/// 試験用の config.toml の元にする例の設定。
const EXAMPLE_CONFIG: &str = "config.example.toml";
/// 例の設定の `shutdown.timeout_secs` の行。
const EXAMPLE_TIMEOUT_LINE: &str = "timeout_secs = 20";

/// 例の設定の `shutdown.timeout_secs` の行を `line` に置き換えて、`dir` に config.toml として書く。
///
/// # 例
/// `write_config(dir.path(), "timeout_secs = 37");`
fn write_config(dir: &Path, line: &str) {
	let example =
		std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(EXAMPLE_CONFIG))
			.unwrap();
	assert_eq!(
		example
			.lines()
			.filter(|l| l.trim_end() == EXAMPLE_TIMEOUT_LINE)
			.count(),
		1,
		"例の設定に置き換える行が 1 つだけあること"
	);
	let text: Vec<&str> = example
		.lines()
		.map(|l| {
			if l.trim_end() == EXAMPLE_TIMEOUT_LINE {
				line
			} else {
				l
			}
		})
		.collect();
	std::fs::write(dir.join(app::CONFIG_PATH), text.join("\n")).unwrap();
}

#[test]
fn install_config_uses_loaded_shutdown_timeout() {
	let dir = TempDir::new().unwrap();
	write_config(dir.path(), "timeout_secs = 37");
	let config = load_install_config(dir.path()).unwrap();
	assert_eq!(config.shutdown.timeout_secs, 37);
	assert_eq!(
		stop_wait_hint(config.shutdown.timeout_secs),
		Duration::from_secs(42)
	);
}

#[test]
fn install_config_rejects_missing_and_invalid() {
	let dir = TempDir::new().unwrap();
	assert!(matches!(
		load_install_config(dir.path()),
		Err(ServiceError::ConfigMissing(_))
	));
	// 範囲の不正 (0 秒) は登録しない
	write_config(dir.path(), "timeout_secs = 0");
	assert!(matches!(
		load_install_config(dir.path()),
		Err(ServiceError::Config(ConfigError::Invalid { .. }))
	));
	// TOML として読めない設定も登録しない
	std::fs::write(dir.path().join(app::CONFIG_PATH), "[shutdown\n").unwrap();
	assert!(matches!(
		load_install_config(dir.path()),
		Err(ServiceError::Config(ConfigError::Parse { .. }))
	));
}
