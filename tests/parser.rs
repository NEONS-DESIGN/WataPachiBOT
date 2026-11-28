//! コマンド解析の試験。

use twilight_model::id::Id;
use watapachi_bot::commands::{parse, ParseError, ParsedCommand, SettingCommand};
use watapachi_bot::config::LimitsConfig;
use watapachi_bot::guild::message::{ClearRange, GuildCommand};
use watapachi_bot::lang::Lang;

/// 試験用の上限値。
fn limits() -> LimitsConfig {
	LimitsConfig {
		queue_limit_max: 10000,
		playlist_limit_max: 500,
		alone_timeout_max_secs: 600,
		actor_mailbox: 64,
		actor_idle_timeout_secs: 60,
	}
}

fn usage(key: &'static str) -> Option<Result<ParsedCommand, ParseError>> {
	Some(Err(ParseError::Usage { usage: key }))
}

#[test]
fn parses_play_query_with_spaces() {
	assert_eq!(
		parse("?", "?p  yoasobi 夜に駆ける ", &limits()),
		Some(Ok(ParsedCommand::Guild(GuildCommand::Play {
			query: "yoasobi 夜に駆ける".into()
		})))
	);
}

#[test]
fn play_keeps_case_of_query() {
	assert_eq!(
		parse("?", "?P Hello World", &limits()),
		Some(Ok(ParsedCommand::Guild(GuildCommand::Play {
			query: "Hello World".into()
		})))
	);
}

#[test]
fn ignores_non_commands() {
	for text in ["?", "??", "? p", "?unknown", "hello", "", "？p x"] {
		assert_eq!(parse("?", text, &limits()), None, "{text}");
	}
}

#[test]
fn case_insensitive_names() {
	assert!(matches!(
		parse("?", "?SKIP", &limits()),
		Some(Ok(ParsedCommand::Guild(GuildCommand::Skip)))
	));
}

#[test]
fn simple_commands_and_extra_text_ignored() {
	let cases = [
		("?help", ParsedCommand::Help),
		("?pause", ParsedCommand::Guild(GuildCommand::Pause)),
		("?resume", ParsedCommand::Guild(GuildCommand::Resume)),
		("?replay", ParsedCommand::Guild(GuildCommand::Replay)),
		("?skip now", ParsedCommand::Guild(GuildCommand::Skip)),
		("?qlist", ParsedCommand::QueueList),
		("?loop", ParsedCommand::Guild(GuildCommand::ToggleLoop)),
		("?sh", ParsedCommand::Guild(GuildCommand::Shuffle)),
		("?history", ParsedCommand::History),
		("?move", ParsedCommand::Guild(GuildCommand::Move)),
		("?rejoin", ParsedCommand::Guild(GuildCommand::Rejoin)),
		("?leave", ParsedCommand::Guild(GuildCommand::Leave)),
	];
	for (text, expected) in cases {
		assert_eq!(parse("?", text, &limits()), Some(Ok(expected)), "{text}");
	}
}

#[test]
fn clear_forms() {
	let clear = |range| Some(Ok(ParsedCommand::Guild(GuildCommand::Clear { range })));
	assert_eq!(parse("?", "?clear", &limits()), clear(ClearRange::All));
	assert_eq!(
		parse("?", "?clear 5", &limits()),
		clear(ClearRange::First(5))
	);
	assert_eq!(
		parse("?", "?clear 4 8", &limits()),
		clear(ClearRange::Range { start: 4, end: 8 })
	);
	for text in [
		"?clear 8 4",
		"?clear 0",
		"?clear x",
		"?clear 1 2 3",
		"?clear 0 3",
	] {
		assert_eq!(
			parse("?", text, &limits()),
			usage("help.usage_clear"),
			"{text}"
		);
	}
}

#[test]
fn pnow_forms() {
	assert_eq!(
		parse("?", "?pnow 3", &limits()),
		Some(Ok(ParsedCommand::Guild(GuildCommand::PlayNow { index: 3 })))
	);
	for text in ["?pnow", "?pnow 0", "?pnow x", "?pnow +2"] {
		assert_eq!(
			parse("?", text, &limits()),
			usage("help.usage_pnow"),
			"{text}"
		);
	}
}

#[test]
fn vol_forms() {
	assert_eq!(
		parse("?", "?vol 80", &limits()),
		Some(Ok(ParsedCommand::Guild(GuildCommand::SetVolume {
			percent: 80
		})))
	);
	// 範囲の判定はアクター側。u16 に収まれば通す
	assert_eq!(
		parse("?", "?vol 500", &limits()),
		Some(Ok(ParsedCommand::Guild(GuildCommand::SetVolume {
			percent: 500
		})))
	);
	for text in ["?vol", "?vol x", "?vol 70000", "?vol -1", "?vol +80"] {
		assert_eq!(
			parse("?", text, &limits()),
			usage("help.usage_vol"),
			"{text}"
		);
	}
}

#[test]
fn purge_is_not_a_command() {
	// 必要な権限を減らすため削除した。未知のコマンドとして扱う
	for text in ["?purge", "?purge 10"] {
		assert_eq!(parse("?", text, &limits()), None, "{text}");
	}
}

#[test]
fn setting_admin_mentions() {
	let add = |text: &str| parse("?", text, &limits());
	for text in [
		"?setting admin add <@123>",
		"?setting admin add <@!123>",
		"?setting admin add 123",
	] {
		assert_eq!(
			add(text),
			Some(Ok(ParsedCommand::Setting(SettingCommand::AdminAdd(
				Id::new(123)
			)))),
			"{text}"
		);
	}
	assert_eq!(
		add("?setting admin remove <@123>"),
		Some(Ok(ParsedCommand::Setting(SettingCommand::AdminRemove(
			Id::new(123)
		))))
	);
	for text in [
		"?setting admin add <@&123>",
		"?setting admin add abc",
		"?setting admin add 0",
		"?setting admin add",
		"?setting admin",
		"?setting",
		"?setting foo",
	] {
		assert_eq!(add(text), usage("help.usage_setting"), "{text}");
	}
}

#[test]
fn setting_limits_ranges() {
	let p = |text: &str| parse("?", text, &limits());
	assert_eq!(
		p("?setting limit queue 10000"),
		Some(Ok(ParsedCommand::Setting(SettingCommand::QueueLimit(
			10000
		))))
	);
	assert_eq!(
		p("?setting limit queue 10001"),
		Some(Err(ParseError::OutOfRange { min: 1, max: 10000 }))
	);
	assert_eq!(
		p("?setting limit playlist 500"),
		Some(Ok(ParsedCommand::Setting(SettingCommand::PlaylistLimit(
			500
		))))
	);
	assert_eq!(
		p("?setting limit playlist 0"),
		Some(Err(ParseError::OutOfRange { min: 1, max: 500 }))
	);
	assert_eq!(
		p("?setting autoleave 0"),
		Some(Ok(ParsedCommand::Setting(SettingCommand::AutoLeave(0))))
	);
	assert_eq!(
		p("?setting autoleave 601"),
		Some(Err(ParseError::OutOfRange { min: 0, max: 600 }))
	);
	assert_eq!(p("?setting limit queue x"), usage("help.usage_setting"));
	assert_eq!(p("?setting limit foo 3"), usage("help.usage_setting"));
	assert_eq!(p("?setting autoleave"), usage("help.usage_setting"));
}

#[test]
fn play_without_query_is_usage() {
	assert_eq!(parse("?", "?p", &limits()), usage("help.usage_p"));
	assert_eq!(parse("?", "?p   ", &limits()), usage("help.usage_p"));
}

#[test]
fn play_with_only_empty_brackets_is_usage() {
	assert_eq!(parse("?", "?p <>", &limits()), usage("help.usage_p"));
	assert_eq!(parse("?", "?p < >", &limits()), usage("help.usage_p"));
}

#[test]
fn custom_prefix() {
	assert!(parse("!!", "!!skip", &limits()).is_some());
	assert!(parse("!!", "?skip", &limits()).is_none());
}

#[test]
fn setting_lang() {
	let parse_text = |text: &str| parse("?", text, &limits());
	assert_eq!(
		parse_text("?setting lang en"),
		Some(Ok(ParsedCommand::Setting(SettingCommand::Lang(Lang::En))))
	);
	assert_eq!(
		parse_text("?setting lang JA"),
		Some(Ok(ParsedCommand::Setting(SettingCommand::Lang(Lang::Ja))))
	);
	for text in ["?setting lang fr", "?setting lang", "?setting lang en ja"] {
		assert_eq!(parse_text(text), usage("help.usage_setting"), "{text}");
	}
}

/// サブコマンドはコマンド名と同じく大文字と小文字を区別しないこと。値に `+` は付けられないこと。
#[test]
fn setting_subcommands_ignore_case() {
	let p = |text: &str| parse("?", text, &limits());
	assert_eq!(
		p("?SETTING Lang EN"),
		Some(Ok(ParsedCommand::Setting(SettingCommand::Lang(Lang::En))))
	);
	assert_eq!(
		p("?setting Limit Queue 10"),
		Some(Ok(ParsedCommand::Setting(SettingCommand::QueueLimit(10))))
	);
	assert_eq!(
		p("?setting AutoLeave 0"),
		Some(Ok(ParsedCommand::Setting(SettingCommand::AutoLeave(0))))
	);
	assert_eq!(
		p("?setting Admin ADD <@123>"),
		Some(Ok(ParsedCommand::Setting(SettingCommand::AdminAdd(
			Id::new(123)
		))))
	);
	assert_eq!(p("?setting limit queue +10"), usage("help.usage_setting"));
}
