//! アクターを通さないコマンド (help、qlist、history、setting) の試験。
//! - DB は一時ディレクトリの実 DB。登録簿は偽実装で組み立てた環境のものを使う

#[path = "support/actor_env.rs"]
mod actor_env;
#[path = "support/fake_audio.rs"]
mod fake_audio;
#[path = "support/fake_notifier.rs"]
mod fake_notifier;
#[path = "support/fake_resolver.rs"]
mod fake_resolver;

use actor_env::*;
use twilight_model::channel::message::component::Component;
use twilight_model::channel::message::Embed;
use watapachi_bot::commands::admin::apply_setting;
use watapachi_bot::commands::info::{help_page, history_page, queue_page, Reply};
use watapachi_bot::commands::SettingCommand;
use watapachi_bot::db::history;
use watapachi_bot::db::queue::{self, NewQueueItem};
use watapachi_bot::db::settings;
use watapachi_bot::domain::{UserId, VideoId};
use watapachi_bot::lang::{GuildLangs, Lang};
use watapachi_bot::messages::Messages;
use watapachi_bot::ui::embed::COLOR_GREEN;

const ISSUED_AT: i64 = 1_793_000_000;

fn messages() -> Messages {
	Messages::load(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("messages/ja.toml"))
		.unwrap()
}

fn new_item(n: u32) -> NewQueueItem {
	NewQueueItem {
		video_id: VideoId(format!("vid{n:08}")),
		title: format!("title {n}"),
		duration_s: Some(n),
		requester_id: UserId::new(7),
	}
}

/// Reply のボタンの数。
fn button_count(reply: &Reply) -> usize {
	reply
		.components
		.iter()
		.map(|c| match c {
			Component::ActionRow(row) => row.components.len(),
			_ => 0,
		})
		.sum()
}

fn description(embed: &Embed) -> String {
	embed.description.clone().unwrap_or_default()
}

#[tokio::test]
async fn help_pages_clamp_and_have_buttons() {
	let env = Env::new();
	let m = messages();
	let first = help_page(&m, &env.config.ui, &env.config.limits, "?", 0, ISSUED_AT).await;
	assert_eq!(first.embed.color, Some(COLOR_GREEN));
	assert_eq!(first.embed.fields[0].name, "?help");
	assert_eq!(button_count(&first), 4);
	let clamped = help_page(&m, &env.config.ui, &env.config.limits, "?", 99, ISSUED_AT).await;
	assert_eq!(clamped.embed.title, Some(m.render("help.title_3", &[])));
}

#[tokio::test]
async fn queue_page_lists_ten_and_clamps() {
	let env = Env::new();
	let items: Vec<_> = (1..=25).map(new_item).collect();
	env.db
		.handles
		.writer
		.run(move |c| queue::append(c, guild(), &items, 1000).map(|_| ()))
		.await
		.unwrap();
	let m = messages();
	let first = queue_page(
		&env.db.handles,
		&m,
		&env.config.ui,
		"?",
		guild(),
		0,
		ISSUED_AT,
	)
	.await
	.unwrap();
	let text = description(&first.embed);
	assert!(text.starts_with("**1.**"), "{text}");
	assert!(text.contains("**10.**") && !text.contains("**11.**"));
	assert_eq!(button_count(&first), 4);
	let last = queue_page(
		&env.db.handles,
		&m,
		&env.config.ui,
		"?",
		guild(),
		9,
		ISSUED_AT,
	)
	.await
	.unwrap();
	let text = description(&last.embed);
	assert!(
		text.starts_with("**21.**") && text.contains("**25.**"),
		"{text}"
	);
	assert_eq!(
		last.embed.title.as_deref(),
		Some("📝 キューリスト (3/3ページ)")
	);
}

#[tokio::test]
async fn empty_list_renders_prefix() {
	// 空の一覧の文面に {prefix} があれば、渡した接頭辞に置き換える
	let env = Env::new();
	let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("messages/ja.toml");
	let text = std::fs::read_to_string(source).unwrap();
	let from = "queue_empty = { title = \"📝 キューが空です。曲を追加してください。\"";
	assert!(text.contains(from));
	let text = text.replace(from, "queue_empty = { title = \"{prefix}p で追加\"");
	let dir = tempfile::tempdir().unwrap();
	let path = dir.path().join("ja.toml");
	std::fs::write(&path, text).unwrap();
	let m = Messages::load(&path).unwrap();
	let reply = queue_page(
		&env.db.handles,
		&m,
		&env.config.ui,
		"!",
		guild(),
		0,
		ISSUED_AT,
	)
	.await
	.unwrap();
	assert_eq!(reply.embed.title.as_deref(), Some("!p で追加"));
}

#[tokio::test]
async fn queue_page_empty() {
	let env = Env::new();
	let m = messages();
	let reply = queue_page(
		&env.db.handles,
		&m,
		&env.config.ui,
		"?",
		guild(),
		0,
		ISSUED_AT,
	)
	.await
	.unwrap();
	assert_eq!(
		reply.embed.title.as_deref(),
		Some("📝 キューが空です。曲を追加してください。")
	);
	assert_eq!(reply.embed.color, Some(0xFF4686));
	assert!(reply.components.is_empty());
}

#[tokio::test]
async fn history_page_newest_first() {
	let env = Env::new();
	for n in 1..=12u32 {
		let item = new_item(n);
		env.db
			.handles
			.writer
			.run(move |c| history::record(c, guild(), &item, 1000 + i64::from(n)))
			.await
			.unwrap();
	}
	let m = messages();
	let reply = history_page(
		&env.db.handles,
		&m,
		&env.config.ui,
		"?",
		guild(),
		0,
		ISSUED_AT,
	)
	.await
	.unwrap();
	let text = description(&reply.embed);
	assert!(text.starts_with("**1.** [title 12]"), "{text}");
	assert_eq!(button_count(&reply), 2);
}

#[tokio::test]
async fn setting_writes_and_notifies_actor() {
	let env = Env::new();
	let notice = apply_setting(
		&env.db.handles,
		&env.registry,
		&langs(),
		guild(),
		&SettingCommand::QueueLimit(30),
	)
	.await
	.unwrap();
	assert_eq!(notice.key, "setting.queue_limit");
	assert!(notice.args.contains(&("value", "30".to_owned())));
	let defaults = env.config.defaults.clone();
	let loaded = env
		.db
		.handles
		.reader
		.read(move |c| settings::load(c, guild(), &defaults))
		.await
		.unwrap();
	assert_eq!(loaded.queue_limit, 30);
}

#[tokio::test]
async fn autoleave_zero_reports_off() {
	let env = Env::new();
	let notice = apply_setting(
		&env.db.handles,
		&env.registry,
		&langs(),
		guild(),
		&SettingCommand::AutoLeave(0),
	)
	.await
	.unwrap();
	assert_eq!(notice.key, "setting.autoleave_off");
}

#[tokio::test]
async fn admin_add_twice_reports_already() {
	let env = Env::new();
	let command = SettingCommand::AdminAdd(UserId::new(55));
	let first = apply_setting(&env.db.handles, &env.registry, &langs(), guild(), &command)
		.await
		.unwrap();
	assert_eq!(first.key, "setting.admin_added");
	assert!(first.args.contains(&("user", "<@55>".to_owned())));
	let second = apply_setting(&env.db.handles, &env.registry, &langs(), guild(), &command)
		.await
		.unwrap();
	assert_eq!(second.key, "setting.admin_already");
	let remove = SettingCommand::AdminRemove(UserId::new(55));
	let removed = apply_setting(&env.db.handles, &env.registry, &langs(), guild(), &remove)
		.await
		.unwrap();
	assert_eq!(removed.key, "setting.admin_removed");
	let missing = apply_setting(&env.db.handles, &env.registry, &langs(), guild(), &remove)
		.await
		.unwrap();
	assert_eq!(missing.key, "setting.admin_missing");
}

/// ヘルプの全ページの項目を (名前, 値, 横並び) で返す。
async fn all_help_fields(env: &Env) -> Vec<(String, String, bool)> {
	let m = messages();
	let mut out = Vec::new();
	for page in 0..3 {
		let reply = help_page(&m, &env.config.ui, &env.config.limits, "?", page, ISSUED_AT).await;
		out.extend(
			reply
				.embed
				.fields
				.iter()
				.map(|f| (f.name.clone(), f.value.clone(), f.inline)),
		);
	}
	out
}

#[tokio::test]
async fn help_page_uses_fields() {
	let env = Env::new();
	let m = messages();
	let first = help_page(&m, &env.config.ui, &env.config.limits, "?", 0, ISSUED_AT).await;
	assert_eq!(
		first.embed.title.as_deref(),
		Some("📖 コマンドヘルプ #1 (再生・基本操作)")
	);
	assert!(first.embed.description.is_none());
	assert_eq!(
		(
			first.embed.fields[0].name.as_str(),
			first.embed.fields[0].value.as_str(),
			first.embed.fields[0].inline
		),
		("?help", "このヘルプ画面を表示します。", false)
	);
}

#[tokio::test]
async fn help_page_3_shows_limits() {
	let env = Env::new();
	let fields = all_help_fields(&env).await;
	let limits = &env.config.limits;
	let (_, value, _) = fields
		.iter()
		.find(|(name, _, _)| name == "?setting limit [queue/playlist] [値]")
		.expect("setting limit の項目が無い");
	assert!(
		value.contains(&format!(
			"キュー 1〜{}、プレイリスト 1〜{}",
			limits.queue_limit_max, limits.playlist_limit_max
		)),
		"{value}"
	);
	let autoleave = format!("?setting autoleave [0-{}]", limits.alone_timeout_max_secs);
	assert!(fields.iter().any(|(name, _, _)| *name == autoleave));
	assert!(fields
		.iter()
		.all(|(name, value, _)| !name.contains('{') && !value.contains('{')));
}

#[tokio::test]
async fn help_has_no_removed_commands() {
	let env = Env::new();
	let names: Vec<String> = all_help_fields(&env)
		.await
		.into_iter()
		.map(|f| f.0)
		.collect();
	assert!(
		names
			.iter()
			.all(|n| !n.contains("purge") && !n.contains("speed")),
		"{names:?}"
	);
	assert!(names.iter().any(|n| n == "?history"));
	assert!(names.iter().any(|n| n == "?rejoin / ?rj"));
}

/// 別名のあるコマンドは、ヘルプの項目名に別名を並べること。
#[tokio::test]
async fn help_shows_aliases() {
	let env = Env::new();
	let names: Vec<String> = all_help_fields(&env)
		.await
		.into_iter()
		.map(|f| f.0)
		.collect();
	for expected in [
		"?p / ?play [URL・タイトル]",
		"?pause / ?ps",
		"?resume / ?rs",
		"?replay / ?rp",
		"?skip / ?s",
		"?qlist / ?ql",
		"?pnow / ?pn [番号]",
		"?sh / ?shuffle",
		"?vol / ?volume [1-200]",
		"?rejoin / ?rj",
	] {
		assert!(names.iter().any(|n| n == expected), "{expected}: {names:?}");
	}
}

#[tokio::test]
async fn history_empty_is_red_title() {
	let env = Env::new();
	let m = messages();
	let reply = history_page(
		&env.db.handles,
		&m,
		&env.config.ui,
		"?",
		guild(),
		0,
		ISSUED_AT,
	)
	.await
	.unwrap();
	assert_eq!(
		reply.embed.title.as_deref(),
		Some("🕘 再生履歴はありません。")
	);
	assert_eq!(reply.embed.color, Some(0xFF4686));
	assert!(reply.components.is_empty());
}

/// 言語の設定が無い表（既定は日本語）。
fn langs() -> GuildLangs {
	GuildLangs::new(Lang::Ja, Vec::new())
}

#[tokio::test]
async fn setting_lang_updates_table_after_save() {
	let env = Env::new();
	let langs = langs();
	let notice = apply_setting(
		&env.db.handles,
		&env.registry,
		&langs,
		guild(),
		&SettingCommand::Lang(Lang::En),
	)
	.await
	.unwrap();
	assert_eq!(notice.key, "setting.lang");
	assert_eq!(langs.get(Some(guild())), Lang::En);
	let saved = env
		.db
		.handles
		.reader
		.read(settings::load_langs)
		.await
		.unwrap();
	assert_eq!(saved, vec![(guild(), Lang::En)]);
}
