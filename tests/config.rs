//! 設定とメッセージの読み込みに関する統合テスト。
//! - config.example.toml が既定値で読めること
//! - 不正な設定を読み込み時に拒否すること
//! - メッセージの必須キー検査と置換

use std::path::{Path, PathBuf};

use watapachi_bot::audio::limits::VoiceLimits;
use watapachi_bot::config::{
	Config, ConfigError, DEFAULT_ACCESS_CHECK_TIMEOUT_SECS, DEFAULT_PING_TIMEOUT_SECS,
	MAX_DURATION_SECS, MAX_RESTORE_STAGGER_MS, MAX_SHUTDOWN_TIMEOUT_SECS, MAX_WORKER_COUNT,
};
use watapachi_bot::lang::Lang;
use watapachi_bot::messages::{Messages, NoticeColor, NOTICE_KEYS};

/// リポジトリ直下のパスを返す。
fn root(rel: &str) -> PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR")).join(rel)
}

/// config.example.toml を読み、文字列で返す。
fn example_text() -> String {
	std::fs::read_to_string(root("config.example.toml")).unwrap()
}

/// 文字列を一時ファイルへ書いて Config::load に渡す。
fn load_text(text: &str) -> Result<Config, ConfigError> {
	let dir = tempfile::tempdir().unwrap();
	let path = dir.path().join("config.toml");
	std::fs::write(&path, text).unwrap();
	Config::load(&path)
}

#[test]
fn example_config_loads_with_spec_defaults() {
	let config = Config::load(&root("config.example.toml")).unwrap();
	assert_eq!(config.playback.chunk_size_bytes, 1_048_576);
	assert_eq!(config.worker.worker_count, 2);
	assert_eq!(config.defaults.queue_limit, 5000);
	assert_eq!(config.bot.prefix, "?");
	assert_eq!(config.restore.stagger_ms, 500);
	assert_eq!(
		config.paths.worker_python,
		Path::new("worker/.venv/Scripts/python.exe")
	);
	assert_eq!(config.paths.worker_script, Path::new("worker/worker.py"));
	assert_eq!(
		config.playback.pipe_format,
		"bestaudio[acodec=opus]/bestaudio/best"
	);
}

#[test]
fn unknown_key_is_rejected() {
	let text = format!("{}\n[bot_extra]\nfoo = 1\n", example_text());
	assert!(matches!(load_text(&text), Err(ConfigError::Parse { .. })));
	let text = example_text().replace("prefix = \"?\"", "prefix = \"?\"\nunknown_key = 1");
	assert!(matches!(load_text(&text), Err(ConfigError::Parse { .. })));
}

#[test]
fn zero_worker_count_is_rejected() {
	let text = example_text().replace("worker_count = 2", "worker_count = 0");
	match load_text(&text) {
		Err(ConfigError::Invalid { key, .. }) => assert_eq!(key, "worker.worker_count"),
		other => panic!("unexpected: {other:?}"),
	}
}

/// 例の設定のうち、`key = ` で始まる行 (1 行だけあること) を `key = value` に置き換えた文字列を返す。
///
/// # 例
/// `let text = replace_line("timeout_secs", 600);`
fn replace_line(key: &str, value: impl std::fmt::Display) -> String {
	let prefix = format!("{key} = ");
	let example = example_text();
	let lines: Vec<String> = example
		.lines()
		.map(|line| {
			if line.starts_with(&prefix) {
				format!("{prefix}{value}")
			} else {
				line.to_string()
			}
		})
		.collect();
	assert_eq!(
		example.lines().filter(|l| l.starts_with(&prefix)).count(),
		1,
		"例の設定に {key} の行が 1 つだけあること"
	);
	lines.join("\n")
}

#[test]
fn worker_count_above_max_is_rejected() {
	let text = replace_line("worker_count", MAX_WORKER_COUNT);
	assert_eq!(
		load_text(&text).unwrap().worker.worker_count,
		MAX_WORKER_COUNT
	);
	let text = replace_line("worker_count", MAX_WORKER_COUNT + 1);
	match load_text(&text) {
		Err(ConfigError::Invalid { key, .. }) => assert_eq!(key, "worker.worker_count"),
		other => panic!("unexpected: {other:?}"),
	}
}

#[test]
fn restore_stagger_above_max_is_rejected() {
	let text = replace_line("stagger_ms", MAX_RESTORE_STAGGER_MS);
	assert_eq!(
		load_text(&text).unwrap().restore.stagger_ms,
		MAX_RESTORE_STAGGER_MS
	);
	let text = replace_line("stagger_ms", MAX_RESTORE_STAGGER_MS + 1);
	match load_text(&text) {
		Err(ConfigError::Invalid { key, .. }) => assert_eq!(key, "restore.stagger_ms"),
		other => panic!("unexpected: {other:?}"),
	}
}

#[test]
fn shutdown_timeout_is_bounded() {
	// 行頭が "timeout_secs = " の行は [shutdown] だけにある
	let replace = |value: u64| replace_line("timeout_secs", value);
	assert_eq!(
		load_text(&replace(MAX_SHUTDOWN_TIMEOUT_SECS))
			.unwrap()
			.shutdown
			.timeout_secs,
		MAX_SHUTDOWN_TIMEOUT_SECS
	);
	// TOML の整数の上限は i64::MAX
	let toml_max = u64::try_from(i64::MAX).unwrap();
	for value in [0, MAX_SHUTDOWN_TIMEOUT_SECS + 1, toml_max] {
		match load_text(&replace(value)) {
			Err(ConfigError::Invalid { key, .. }) => assert_eq!(key, "shutdown.timeout_secs"),
			other => panic!("{value}: unexpected: {other:?}"),
		}
	}
}

#[test]
fn rate_limit_backoff_max_must_not_be_below_base() {
	let config = Config::load(&root("config.example.toml")).unwrap();
	assert_eq!(config.worker.rate_limit_backoff_secs, 5);
	assert_eq!(config.worker.rate_limit_backoff_max_secs, 60);

	let text = example_text().replace(
		"rate_limit_backoff_max_secs = 60",
		"rate_limit_backoff_max_secs = 4",
	);
	match load_text(&text) {
		Err(ConfigError::Invalid { key, .. }) => {
			assert_eq!(key, "worker.rate_limit_backoff_max_secs")
		}
		other => panic!("unexpected: {other:?}"),
	}
	let text = example_text().replace(
		"rate_limit_backoff_secs = 5 ",
		"rate_limit_backoff_secs = 0 ",
	);
	match load_text(&text) {
		Err(ConfigError::Invalid { key, .. }) => {
			assert_eq!(key, "worker.rate_limit_backoff_secs")
		}
		other => panic!("unexpected: {other:?}"),
	}
}

#[test]
fn prefetch_reserved_must_be_less_than_worker_count() {
	let text = example_text().replace(
		"prefetch_reserved_workers = 1",
		"prefetch_reserved_workers = 2",
	);
	match load_text(&text) {
		Err(ConfigError::Invalid { key, .. }) => {
			assert_eq!(key, "worker.prefetch_reserved_workers")
		}
		other => panic!("unexpected: {other:?}"),
	}
}

#[test]
fn worker_timeouts_are_validated() {
	let config = Config::load(&root("config.example.toml")).unwrap();
	assert_eq!(config.worker.startup_timeout_secs, 60);
	let text = example_text().replace("startup_timeout_secs = 60 ", "startup_timeout_secs = 0 ");
	match load_text(&text) {
		Err(ConfigError::Invalid { key, .. }) => assert_eq!(key, "worker.startup_timeout_secs"),
		other => panic!("unexpected: {other:?}"),
	}
	// ワーカーの持ち時間 (余裕を引いた値) が 1 秒以上残らない上限は拒否する
	let margin = watapachi_bot::resolver::pool::REQUEST_BUDGET_MARGIN_SECS;
	let text = example_text().replace(
		"resolve_timeout_secs = 30 ",
		&format!("resolve_timeout_secs = {margin} "),
	);
	match load_text(&text) {
		Err(ConfigError::Invalid { key, .. }) => assert_eq!(key, "worker.resolve_timeout_secs"),
		other => panic!("unexpected: {other:?}"),
	}
	let text = example_text().replace(
		"resolve_timeout_secs = 30 ",
		&format!("resolve_timeout_secs = {} ", margin + 1),
	);
	assert!(load_text(&text).is_ok());
}

#[test]
fn skip_streak_limit_must_be_positive() {
	let config = Config::load(&root("config.example.toml")).unwrap();
	assert_eq!(config.playback.skip_streak_limit, 3);
	let text = example_text().replace("skip_streak_limit = 3 ", "skip_streak_limit = 0 ");
	match load_text(&text) {
		Err(ConfigError::Invalid { key, .. }) => assert_eq!(key, "playback.skip_streak_limit"),
		other => panic!("unexpected: {other:?}"),
	}
}

#[test]
fn missing_message_key_fails_at_load() {
	let dir = tempfile::tempdir().unwrap();
	let path = dir.path().join("ja.toml");
	std::fs::write(&path, "[play]\nqueued = \"x\"\n").unwrap();
	assert!(matches!(
		Messages::load(&path),
		Err(ConfigError::MissingMessageKey { .. })
	));
}

#[test]
fn bundled_messages_load() {
	Messages::load(&root("messages/ja.toml")).unwrap();
}

#[test]
fn render_replaces_named_arguments() {
	let messages = Messages::load(&root("messages/ja.toml")).unwrap();
	let out = messages.render("ui.footer_requester", &[("name", "{artist} - song")]);
	assert!(out.contains("{artist} - song"));
	assert!(!out.contains("{name}"));
	// 値の中の {x} は再置換しない
	let out = messages.render("ui.footer_requester", &[("name", "{x}"), ("x", "NG")]);
	assert!(out.contains("{x}"));
	assert!(!out.contains("NG"));
}

/// 同梱の ja.toml の `from` を `to` に置き換えて読み込む。
fn load_messages_replacing(from: &str, to: &str) -> Result<Messages, ConfigError> {
	let text = std::fs::read_to_string(root("messages/ja.toml")).unwrap();
	assert!(text.contains(from), "置き換え元が無い: {from}");
	let dir = tempfile::tempdir().unwrap();
	let path = dir.path().join("ja.toml");
	std::fs::write(&path, text.replacen(from, to, 1)).unwrap();
	Messages::load(&path)
}

/// 同梱の ja.toml にある control.paused の行。
const PAUSED_LINE: &str =
	"paused = { title = \"⏸️ 一時停止\", text = \"再生を一時停止しました。\", color = \"yellow\" }";

#[test]
fn messages_notice_has_title_text_color() {
	let m = Messages::load(&root("messages/ja.toml")).unwrap();
	let n = m.notice("control.paused", &[]);
	assert_eq!(n.title.as_deref(), Some("⏸️ 一時停止"));
	assert_eq!(n.text.as_deref(), Some("再生を一時停止しました。"));
	assert_eq!(n.color, NoticeColor::Yellow);
	assert_eq!(n.admin, None);
}

#[test]
fn messages_notice_replaces_args_in_all_parts() {
	let m = Messages::load(&root("messages/ja.toml")).unwrap();
	let n = m.notice("control.volume", &[("percent", "50")]);
	assert_eq!(
		n.title.as_deref(),
		Some("🔊 再生音量を 50% に設定しました。")
	);
	assert_eq!(n.text, None);
	let n = m.notice("error.rate_limited_lookup", &[]);
	assert!(n.admin.is_some());
}

#[test]
fn messages_unknown_notice_key_is_safe() {
	let m = Messages::load(&root("messages/ja.toml")).unwrap();
	let n = m.notice("no.such", &[]);
	assert_eq!(n.text.as_deref(), Some("no.such"));
	assert_eq!(n.color, NoticeColor::Red);
}

#[test]
fn messages_reject_bad_color() {
	let bad = PAUSED_LINE.replace("yellow", "pink");
	assert!(matches!(
		load_messages_replacing(PAUSED_LINE, &bad),
		Err(ConfigError::InvalidMessage { key, .. }) if key == "control.paused"
	));
}

#[test]
fn messages_reject_notice_without_title_and_text() {
	assert!(matches!(
		load_messages_replacing(PAUSED_LINE, "paused = { color = \"yellow\" }"),
		Err(ConfigError::InvalidMessage { key, .. }) if key == "control.paused"
	));
}

#[test]
fn messages_reject_string_for_notice_key() {
	assert!(matches!(
		load_messages_replacing(PAUSED_LINE, "paused = \"一時停止しました。\""),
		Err(ConfigError::InvalidMessage { key, .. }) if key == "control.paused"
	));
}

#[test]
fn messages_fields_for_help_page() {
	let m = Messages::load(&root("messages/ja.toml")).unwrap();
	let fields = m.fields("help.page_1", &[("prefix", "?")]);
	assert_eq!(
		fields.first().map(|(n, v)| (n.as_str(), v.as_str())),
		Some(("?help", "このヘルプ画面を表示します。"))
	);
	assert!(m.fields("no.such", &[]).is_empty());
}

#[test]
fn messages_every_notice_key_renders() {
	let m = Messages::load(&root("messages/ja.toml")).unwrap();
	for key in NOTICE_KEYS {
		let n = m.notice(key, &[]);
		assert!(n.title.is_some() || n.text.is_some(), "{key}");
		assert_ne!(n.text.as_deref(), Some(*key), "{key} が表に無い");
	}
}

#[test]
fn available_wait_max_secs_is_loaded() {
	let config = Config::load(&root("config.example.toml")).unwrap();
	assert_eq!(config.playback.available_wait_max_secs, 60);
	let text = example_text().replace(
		"available_wait_max_secs = 60 ",
		"available_wait_max_secs = 0 ",
	);
	assert_eq!(
		load_text(&text).unwrap().playback.available_wait_max_secs,
		0
	);
}

#[test]
fn ui_section_is_loaded_and_validated() {
	let config = Config::load(&root("config.example.toml")).unwrap();
	assert_eq!(
		config.ui.thumbnail_url,
		"https://i.ytimg.com/vi/{id}/hqdefault.jpg"
	);
	assert_eq!(config.ui.pagination_timeout_secs, 120);

	let text = example_text().replace(
		"pagination_timeout_secs = 120",
		"pagination_timeout_secs = 0",
	);
	match load_text(&text) {
		Err(ConfigError::Invalid { key, .. }) => assert_eq!(key, "ui.pagination_timeout_secs"),
		other => panic!("unexpected: {other:?}"),
	}
	let text = example_text().replace("/vi/{id}/", "/vi/x/");
	match load_text(&text) {
		Err(ConfigError::Invalid { key, .. }) => assert_eq!(key, "ui.thumbnail_url"),
		other => panic!("unexpected: {other:?}"),
	}
}

#[test]
fn gateway_section_is_loaded_and_validated() {
	let config = Config::load(&root("config.example.toml")).unwrap();
	assert_eq!(config.gateway.ready_wait_secs, 30);
	assert_eq!(config.gateway.event_send_timeout_secs, 5);

	let text = example_text().replace("ready_wait_secs = 30", "ready_wait_secs = 0");
	match load_text(&text) {
		Err(ConfigError::Invalid { key, .. }) => assert_eq!(key, "gateway.ready_wait_secs"),
		other => panic!("unexpected: {other:?}"),
	}
	let text = example_text().replace("event_send_timeout_secs = 5", "event_send_timeout_secs = 0");
	match load_text(&text) {
		Err(ConfigError::Invalid { key, .. }) => {
			assert_eq!(key, "gateway.event_send_timeout_secs")
		}
		other => panic!("unexpected: {other:?}"),
	}
}

/// 設定を読み、`Invalid` の項目名を返す。読めたら None。
fn invalid_key(text: &str) -> Option<String> {
	match load_text(text) {
		Ok(_) => None,
		Err(ConfigError::Invalid { key, .. }) => Some(key),
		Err(other) => panic!("unexpected: {other:?}"),
	}
}

#[test]
fn durations_are_bounded() {
	// 巨大な値は Instant + Duration があふれる前に拒否する
	let cases = [
		("ready_wait_secs", "gateway.ready_wait_secs"),
		("event_send_timeout_secs", "gateway.event_send_timeout_secs"),
		("pagination_timeout_secs", "ui.pagination_timeout_secs"),
		("startup_timeout_secs", "worker.startup_timeout_secs"),
		("ping_interval_secs", "worker.ping_interval_secs"),
		("ping_timeout_secs", "worker.ping_timeout_secs"),
		("install_timeout_secs", "update.install_timeout_secs"),
		("check_timeout_secs", "update.check_timeout_secs"),
		(
			"chunk_fetch_timeout_secs",
			"playback.chunk_fetch_timeout_secs",
		),
		(
			"url_refresh_margin_secs",
			"playback.url_refresh_margin_secs",
		),
		(
			"available_wait_max_secs",
			"playback.available_wait_max_secs",
		),
		("slow_notice_secs", "playback.slow_notice_secs"),
		(
			"position_save_interval_secs",
			"playback.position_save_interval_secs",
		),
		("check_interval_secs", "health.check_interval_secs"),
		("dave_grace_secs", "health.dave_grace_secs"),
		("auto_rejoin_window_secs", "health.auto_rejoin_window_secs"),
		("call_timeout_secs", "voice.call_timeout_secs"),
		("join_timeout_secs", "voice.join_timeout_secs"),
		(
			"access_check_timeout_secs",
			"voice.access_check_timeout_secs",
		),
		("resolve_timeout_secs", "worker.resolve_timeout_secs"),
		(
			"rate_limit_backoff_max_secs",
			"worker.rate_limit_backoff_max_secs",
		),
		("actor_idle_timeout_secs", "limits.actor_idle_timeout_secs"),
	];
	// TOML の整数の上限は i64::MAX
	let toml_max = u64::try_from(i64::MAX).unwrap();
	for (name, key) in cases {
		for value in [MAX_DURATION_SECS + 1, toml_max] {
			assert_eq!(
				invalid_key(&replace_line(name, value)).as_deref(),
				Some(key),
				"{name} = {value}"
			);
		}
	}
	// 上限ちょうどは受け付ける (待機の時間は接続の上限より長い必要がある)
	let config = load_text(&replace_line("ready_wait_secs", MAX_DURATION_SECS)).unwrap();
	assert_eq!(config.gateway.ready_wait_secs, MAX_DURATION_SECS);
	let config = load_text(&replace_line("actor_idle_timeout_secs", MAX_DURATION_SECS)).unwrap();
	assert_eq!(config.limits.actor_idle_timeout_secs, MAX_DURATION_SECS);
}

#[test]
fn actor_idle_timeout_must_exceed_voice_connect_limit() {
	// 例の設定の接続のタスクの上限は (access 3 + 2) + (join 15 + call 10 × 4 + 2) = 62 秒
	// (call × 4 は、前のセッションの切断の待ち × 2 と、イベントの登録と失敗した Call の削除)
	let config = Config::load(&root("config.example.toml")).unwrap();
	let limits = VoiceLimits::new(&config.voice);
	assert_eq!(limits.connect().as_secs(), 57);
	// 再生と準備の上限は、PipeInput の起動と Call のロックの合計に余裕を足したもの
	assert_eq!(limits.play().as_secs(), 22);
	let connect = (limits.access() + limits.connect()).as_secs();
	assert_eq!(connect, 62);
	assert_eq!(
		invalid_key(&replace_line("actor_idle_timeout_secs", connect)).as_deref(),
		Some("limits.actor_idle_timeout_secs")
	);
	assert_eq!(
		invalid_key(&replace_line("actor_idle_timeout_secs", connect + 1)),
		None
	);
	// 音声の上限を延ばすと、待機の時間も長くする必要がある
	let text = replace_line("join_timeout_secs", 600);
	assert_eq!(
		invalid_key(&text).as_deref(),
		Some("limits.actor_idle_timeout_secs")
	);
}

#[test]
fn ping_timeout_is_checked_and_defaults_when_omitted() {
	let config = Config::load(&root("config.example.toml")).unwrap();
	assert_eq!(config.worker.ping_timeout_secs, 10);
	assert_eq!(
		invalid_key(&replace_line("ping_timeout_secs", 0)).as_deref(),
		Some("worker.ping_timeout_secs")
	);
	// この項目より前に作った config.toml も読める
	let omitted: String = example_text()
		.lines()
		.filter(|line| !line.starts_with("ping_timeout_secs = "))
		.map(|line| format!("{line}\n"))
		.collect();
	let config = load_text(&omitted).unwrap();
	assert_eq!(config.worker.ping_timeout_secs, DEFAULT_PING_TIMEOUT_SECS);
}

#[test]
fn access_check_timeout_is_checked_and_defaults_when_omitted() {
	let config = Config::load(&root("config.example.toml")).unwrap();
	assert_eq!(config.voice.access_check_timeout_secs, 3);
	// アクターの待ちは REST の上限に余裕 (2 秒) を足したもの
	assert_eq!(
		VoiceLimits::new(&config.voice).access(),
		std::time::Duration::from_secs(5)
	);
	assert_eq!(
		invalid_key(&replace_line("access_check_timeout_secs", 0)).as_deref(),
		Some("voice.access_check_timeout_secs")
	);
	// この項目より前に作った config.toml も読める
	let omitted: String = example_text()
		.lines()
		.filter(|line| !line.starts_with("access_check_timeout_secs = "))
		.map(|line| format!("{line}\n"))
		.collect();
	let config = load_text(&omitted).unwrap();
	assert_eq!(
		config.voice.access_check_timeout_secs,
		DEFAULT_ACCESS_CHECK_TIMEOUT_SECS
	);
}

#[test]
fn update_defaults_are_loaded() {
	let config = Config::load(&root("config.example.toml")).unwrap();
	assert_eq!(config.paths.uv, Path::new("uv"));
	assert_eq!(config.update.packages, "yt-dlp[default,curl-cffi]");
	assert_eq!(config.update.python, "3.14");
	assert_eq!(config.update.check_video, "dQw4w9WgXcQ");
	assert_eq!(config.update.install_timeout_secs, 600);
	assert_eq!(config.update.check_timeout_secs, 90);
}

#[test]
fn invalid_update_settings_are_rejected() {
	let cases = [
		(
			"install_timeout_secs = 600",
			"install_timeout_secs = 0",
			"update.install_timeout_secs",
		),
		(
			"check_timeout_secs = 90",
			"check_timeout_secs = 0",
			"update.check_timeout_secs",
		),
		(
			"packages = \"yt-dlp[default,curl-cffi]\"",
			"packages = \"\"",
			"update.packages",
		),
		(
			"check_video = \"dQw4w9WgXcQ\"",
			"check_video = \"\"",
			"update.check_video",
		),
		("python = \"3.14\"", "python = \"\"", "update.python"),
		("uv = \"uv\"", "uv = \"\"", "paths.uv"),
	];
	for (from, to, key) in cases {
		let text = example_text().replace(from, to);
		assert_ne!(text, example_text(), "{from} が見つからない");
		match load_text(&text) {
			Err(ConfigError::Invalid { key: actual, .. }) => assert_eq!(actual, key),
			other => panic!("{key}: unexpected: {other:?}"),
		}
	}
}

#[test]
fn ui_avatar_urls_have_defaults() {
	// 前の版の config.toml (アイコンの URL の雛形が無い) も読める
	let text: String = example_text()
		.lines()
		.filter(|line| !line.contains("avatar_url"))
		.map(|line| format!("{line}\n"))
		.collect();
	let config = load_text(&text).unwrap();
	assert_eq!(
		config.ui.user_avatar_url,
		"https://cdn.discordapp.com/avatars/{user}/{hash}.png"
	);
	assert_eq!(
		config.ui.member_avatar_url,
		"https://cdn.discordapp.com/guilds/{guild}/users/{user}/avatars/{hash}.png"
	);
	assert_eq!(
		config.ui.default_avatar_url,
		"https://cdn.discordapp.com/embed/avatars/{index}.png"
	);
}

#[test]
fn lang_and_activity_have_defaults_when_omitted() {
	let text = example_text()
		.lines()
		.filter(|line| !line.starts_with("lang = ") && !line.starts_with("activity_name = "))
		.collect::<Vec<_>>()
		.join("\n");
	let config = load_text(&text).unwrap();
	assert_eq!(config.defaults.lang, Lang::Ja);
	assert_eq!(config.bot.activity_name, "{prefix}help");
}

#[test]
fn lang_accepts_en_and_rejects_unknown() {
	let config = load_text(&replace_line("lang", "\"en\"")).unwrap();
	assert_eq!(config.defaults.lang, Lang::En);
	assert!(matches!(
		load_text(&replace_line("lang", "\"fr\"")),
		Err(ConfigError::Parse { .. })
	));
}

#[test]
fn empty_activity_name_is_rejected() {
	match load_text(&replace_line("activity_name", "\"\"")) {
		Err(ConfigError::Invalid { key, .. }) => assert_eq!(key, "bot.activity_name"),
		other => panic!("unexpected: {other:?}"),
	}
}
