//! app.rs の純粋な補助関数の試験。
//! - ワーカーの起動指定 (`worker_spec`) の引数の組み立て
//! - `.env` からのトークンの読み取り
//! - 終了の期限の配分 (アクターの段が DB の段の時間を使い切らない)
//! - 終了の段でタスクを期限まで待ち、過ぎたら打ち切る (`join_or_abort`)

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::time::Instant;
use twilight_model::gateway::presence::{ActivityType, Status};
use watapachi_bot::app::{
	actors_deadline, describe_outcome, join_or_abort, parse_env_value, read_token, worker_spec,
	DB_STAGE_RESERVE,
};
use watapachi_bot::config::Config;
use watapachi_bot::gateway::shard::presence;
use watapachi_bot::update::procedure::{UpdateOutcome, UpdateStage};

/// config.example.toml を読む。
fn example_config() -> Config {
	Config::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.toml")).unwrap()
}

/// 引数の並びに `[name, value]` の連続した組があるか。
fn has_pair(args: &[String], name: &str, value: &str) -> bool {
	args.windows(2).any(|w| w[0] == name && w[1] == value)
}

#[test]
fn worker_spec_includes_cookies_and_paths() {
	let mut config = example_config();
	config.worker.cookies_browser = "firefox".into();
	config.worker.user_agent = "UA/1.0".into();
	config.worker.resolve_timeout_secs = 30;
	// 設定の paths.worker_python ではなく、引数で渡した python が使われる
	let python = PathBuf::from("worker/envs/20261118-040000/Scripts/python.exe");
	let spec = worker_spec(&config, &python);
	assert_ne!(spec.python, config.paths.worker_python);
	assert_eq!(spec.python, python);
	assert_eq!(spec.script, PathBuf::from("worker/worker.py"));
	assert!(has_pair(&spec.args, "--cookies", "firefox"));
	assert!(has_pair(&spec.args, "--user-agent", "UA/1.0"));
	assert!(has_pair(&spec.args, "--cachedir", "data/ytdlp-cache"));
	assert!(has_pair(&spec.args, "--premium", "auto"));
	// 持ち時間は WorkerPool が足す。重ねて渡さない
	assert!(!spec.args.iter().any(|a| a == "--request-budget-secs"));
}

#[test]
fn worker_spec_without_cookies() {
	let mut config = example_config();
	config.worker.cookies_browser = String::new();
	config.worker.user_agent = String::new();
	config.worker.youtube_premium = "yes".into();
	let spec = worker_spec(&config, Path::new("py/python.exe"));
	assert!(!spec.args.iter().any(|a| a == "--cookies"));
	assert!(!spec.args.iter().any(|a| a == "--user-agent"));
	assert!(has_pair(&spec.args, "--premium", "yes"));
	assert!(!spec.args.iter().any(|a| a == "--request-budget-secs"));
}

#[test]
fn env_value_is_read_with_comments_and_quotes() {
	let text = "\u{feff}# コメント\r\n\r\nOTHER=1\r\nDISCORD_TOKEN = \"abc.def\"  \r\n";
	assert_eq!(
		parse_env_value(text, "DISCORD_TOKEN").as_deref(),
		Some("abc.def")
	);
	assert_eq!(
		parse_env_value("DISCORD_TOKEN='x y'\n", "DISCORD_TOKEN").as_deref(),
		Some("x y")
	);
	assert_eq!(
		parse_env_value("DISCORD_TOKEN=plain\n", "DISCORD_TOKEN").as_deref(),
		Some("plain")
	);
	// 似た名前やコメント内の記述は拾わない
	assert_eq!(
		parse_env_value("#DISCORD_TOKEN=a\nDISCORD_TOKEN_X=b\n", "DISCORD_TOKEN"),
		None
	);
	// 後に書いた行を優先する
	assert_eq!(
		parse_env_value("DISCORD_TOKEN=a\nDISCORD_TOKEN=b\n", "DISCORD_TOKEN").as_deref(),
		Some("b")
	);
}

#[test]
fn read_token_rejects_missing_or_empty() {
	let dir = tempfile::tempdir().unwrap();
	let missing = dir.path().join("missing.env");
	let error = read_token(&missing).unwrap_err();
	assert!(format!("{error:#}").contains("DISCORD_TOKEN"));

	let empty = dir.path().join("empty.env");
	std::fs::write(&empty, "# Discord ボットのトークン\nDISCORD_TOKEN=\n").unwrap();
	let error = read_token(&empty).unwrap_err();
	assert!(format!("{error:#}").contains("DISCORD_TOKEN"));

	let ok = dir.path().join("ok.env");
	std::fs::write(&ok, "DISCORD_TOKEN=secret-value\n").unwrap();
	let token = read_token(&ok).unwrap();
	assert_eq!(token, "secret-value");
}

#[test]
fn read_token_error_does_not_leak_other_values() {
	let dir = tempfile::tempdir().unwrap();
	let path = dir.path().join("broken.env");
	std::fs::write(&path, "DISCORD_TOKEN=\"\"\nOTHER=secret-value\n").unwrap();
	let error = read_token(&path).unwrap_err();
	assert!(!format!("{error:#}").contains("secret-value"));
}

#[test]
fn actors_deadline_leaves_time_for_database() {
	let now = Instant::now();
	// 既定の 20 秒では、DB の段の取り分を残してアクターの段を打ち切る
	let deadline = now + Duration::from_secs(20);
	let actors = actors_deadline(now, deadline);
	assert_eq!(deadline - actors, DB_STAGE_RESERVE);
	assert!(actors > now);

	// 全体が短ければ、半分ずつに分ける (アクターの段を 0 にしない)
	let short = now + Duration::from_secs(2);
	let actors = actors_deadline(now, short);
	assert_eq!(actors, now + Duration::from_secs(1));

	// 期限を過ぎていれば今を返す
	let past = actors_deadline(now + Duration::from_secs(5), now);
	assert_eq!(past, now + Duration::from_secs(5));
}

#[test]
fn describe_outcome_lines() {
	assert!(describe_outcome(&UpdateOutcome::Updated {
		version: "2026.11.17".into()
	})
	.contains("2026.11.17"));
	assert!(describe_outcome(&UpdateOutcome::Unchanged {
		version: "2026.11.17".into(),
		recycled: false
	})
	.contains("2026.11.17"));
	let failed = describe_outcome(&UpdateOutcome::Failed {
		stage: UpdateStage::Check,
		message: "x".into(),
	});
	assert!(failed.contains("x") && failed.contains("check"));
	assert!(!describe_outcome(&UpdateOutcome::Stopped).is_empty());
}

#[tokio::test]
async fn join_or_abort_returns_the_output() {
	let task = tokio::spawn(async { 7 });
	let deadline = Instant::now() + Duration::from_secs(5);
	assert_eq!(join_or_abort("試験", deadline, task).await, Some(7));
}

#[tokio::test]
async fn join_or_abort_aborts_after_deadline() {
	let (keep, dropped) = tokio::sync::oneshot::channel::<()>();
	// 終わらないタスク。打ち切られると future ごと送り手が破棄される
	let task = tokio::spawn(async move {
		let _keep = keep;
		std::future::pending::<()>().await;
	});
	let started = Instant::now();
	let deadline = started + Duration::from_millis(100);

	assert_eq!(join_or_abort("試験", deadline, task).await, None);
	assert!(started.elapsed() < Duration::from_secs(1), "期限で戻る");
	let closed = tokio::time::timeout(Duration::from_secs(1), dropped)
		.await
		.expect("打ち切りで送り手が破棄されるはず");
	assert!(closed.is_err());
}

#[test]
fn presence_shows_help_as_playing() {
	let config = example_config();
	let payload = presence(&config.bot).unwrap();
	assert_eq!(payload.status, Status::Online);
	assert!(!payload.afk);
	assert_eq!(payload.since, None);
	assert_eq!(payload.activities.len(), 1);
	assert_eq!(payload.activities[0].kind, ActivityType::Playing);
	assert_eq!(payload.activities[0].name, "?help");
}

#[test]
fn presence_uses_configured_prefix() {
	let mut config = example_config();
	config.bot.prefix = "!".to_owned();
	let payload = presence(&config.bot).unwrap();
	assert_eq!(payload.activities[0].name, "!help");
}
