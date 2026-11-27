//! UI (Embed の組み立てとページ送り) の統合テスト。

use std::path::{Path, PathBuf};

use twilight_model::channel::message::component::{ButtonStyle, Component};
use twilight_model::channel::message::Embed;
use twilight_model::id::Id;

use watapachi_bot::config::{Config, UiConfig};
use watapachi_bot::domain::VideoId;
use watapachi_bot::error::ConfigError;
use watapachi_bot::gateway::requester::{requester_from, RequesterSource};
use watapachi_bot::guild::notifier::{Card, Notice, PlaylistCard, TrackCard};
use watapachi_bot::lang::{GuildLangs, Lang};
use watapachi_bot::messages::{MessageCatalog, Messages};
use watapachi_bot::ui::embed::{
	card_fallback_embed, format_duration, notice_embed, title_link, track_page, Requester,
	TrackLine, COLOR_BLUE, COLOR_GREEN, COLOR_RED, COLOR_YELLOW, DESCRIPTION_LIMIT, PER_PAGE,
};
use watapachi_bot::ui::pagination::{
	clamp_page, decode, disable_all, encode, is_expired, page_buttons, total_pages, PageKind,
	PageToken,
};

/// リポジトリ直下のパスを返す。
fn root(rel: &str) -> PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR")).join(rel)
}

/// 実物の messages/ja.toml。
fn messages() -> Messages {
	Messages::load(&root("messages/ja.toml")).unwrap()
}

/// config.example.toml の `[ui]`。
fn ui() -> UiConfig {
	Config::load(&root("config.example.toml")).unwrap().ui
}

/// 引数なしの通知。
fn notice(key: &'static str) -> Notice {
	Notice {
		key,
		args: Vec::new(),
		card: None,
		lists: Vec::new(),
	}
}

/// 参加の権限の不足の通知 (見る・接続が足りない)。
fn denied_notice() -> Notice {
	Notice {
		key: "error.voice_no_permission",
		args: vec![("channel", "<#42>".to_string())],
		card: None,
		lists: vec![(
			"permissions",
			vec!["ui.perm_view_channel", "ui.perm_connect"],
		)],
	}
}

#[test]
fn listed_parts_are_rendered_in_the_notice_language() {
	let ja = notice_embed(&messages(), &ui(), "?", &denied_notice(), None);
	let text = ja.description.unwrap();
	assert!(text.contains("<#42>"), "{text}");
	assert!(text.contains("チャンネルを見る、接続"), "{text}");
	let en_messages = Messages::load(&root("messages/en.toml")).unwrap();
	let en = notice_embed(&en_messages, &ui(), "?", &denied_notice(), None);
	let text = en.description.unwrap();
	assert!(text.contains("View Channel, Connect"), "{text}");
}

#[test]
fn full_channel_notice_names_the_channel() {
	let mut full = notice("error.voice_full");
	full.args.push(("channel", "<#42>".to_string()));
	let text = notice_embed(&messages(), &ui(), "?", &full, None)
		.description
		.unwrap();
	assert!(text.contains("<#42>"), "{text}");
}

/// ボタンの並びから (custom_id, label, style, disabled) を取り出す。
fn buttons(components: &[Component]) -> Vec<(String, String, ButtonStyle, bool)> {
	let mut out = Vec::new();
	for component in components {
		let Component::ActionRow(row) = component else {
			panic!("ActionRow 以外: {component:?}");
		};
		for inner in &row.components {
			let Component::Button(button) = inner else {
				panic!("Button 以外: {inner:?}");
			};
			out.push((
				button.custom_id.clone().unwrap(),
				button.label.clone().unwrap(),
				button.style,
				button.disabled,
			));
		}
	}
	out
}

#[test]
fn title_link_escapes_brackets_and_parens() {
	assert_eq!(
		title_link("a [b] c", "https://x/(y)", 1024),
		"[a \\[b\\] c](https://x/%28y%29)"
	);
}

#[test]
fn title_link_falls_back_to_plain_when_too_long() {
	let long = "あ".repeat(100);
	let out = title_link(&long, "https://example.com/watch?v=abc", 50);
	assert!(!out.contains("]("), "{out}");
	assert_eq!(out.chars().count(), 50);
	assert!(out.ends_with("..."));
	// 収まるならリンクのまま
	assert!(title_link("短い", "https://x/", 50).starts_with("[短い]("));
}

#[test]
fn title_link_escapes_markdown_and_newlines() {
	let url = "https://x/";
	assert_eq!(title_link("a\nb\r\nc", url, 100), "[a b  c](https://x/)");
	// 末尾のバックスラッシュは先に逃がすので、続く ] の逃がしを打ち消さない
	assert_eq!(title_link("end\\", url, 100), "[end\\\\](https://x/)");
	assert_eq!(
		title_link("*bold* _i_", url, 100),
		"[\\*bold\\* \\_i\\_](https://x/)"
	);
	assert_eq!(
		title_link("a`b`~c~|d|", url, 100),
		"[a\\`b\\`\\~c\\~\\|d\\|](https://x/)"
	);
}

#[test]
fn title_link_fallback_is_escaped_and_within_limit() {
	let out = title_link(
		"*bold*\nsecond [x] `code`",
		"https://example.com/watch?v=abc",
		12,
	);
	assert!(!out.contains("]("), "{out}");
	assert!(!out.contains('\n'));
	assert!(out.chars().count() <= 12, "{out}");
	assert!(out.starts_with("\\*bold\\*"), "{out}");
	assert!(out.ends_with("..."));
	// 切り口が逃がしの途中になっても、末尾の ... を打ち消すバックスラッシュを残さない
	for limit in 4..10 {
		let out = title_link("***********", "https://example.com/", limit);
		assert!(out.chars().count() <= limit, "{out}");
		let body = out.strip_suffix("...").unwrap_or(&out);
		let trailing = body.chars().rev().take_while(|c| *c == '\\').count();
		assert_eq!(trailing % 2, 0, "{out}");
	}
}

#[test]
fn notice_embed_truncates_description() {
	let mut n = notice("error.invalid_argument");
	n.args.push(("usage", "あ".repeat(5000)));
	let embed = notice_embed(&messages(), &ui(), "?", &n, None);
	let description = embed.description.unwrap();
	assert_eq!(description.chars().count(), DESCRIPTION_LIMIT);
	assert!(description.ends_with("..."));
}

#[test]
fn notice_embed_colors_by_key() {
	let m = messages();
	let u = ui();
	let color = |key: &'static str| notice_embed(&m, &u, "?", &notice(key), None).color.unwrap();
	// 色は ja.toml の color で決まる (旧 Bot と同じ値)
	assert_eq!(color("error.nothing_playing"), 0xFF4686);
	assert_eq!(color("control.paused"), 0xF1C40F);
	assert_eq!(color("control.resumed"), 0x00976B);
	assert_eq!(color("control.moved"), 0x1F64E1);
	assert_eq!(color("queue.cleared"), COLOR_GREEN);
	assert_eq!(color("error.rejoin_limit"), COLOR_YELLOW);
	assert_eq!(color("play.preparing"), COLOR_YELLOW);
	assert_eq!(color("error.audio_failed"), COLOR_RED);
	assert_eq!(color("play.playlist_queued"), COLOR_BLUE);
}

#[test]
fn notice_embed_has_title_and_admin_field() {
	let embed = notice_embed(
		&messages(),
		&ui(),
		"?",
		&notice("error.rate_limited_lookup"),
		None,
	);
	assert_eq!(embed.title.as_deref(), Some("⚠️ 読み込みエラー"));
	assert!(embed
		.description
		.unwrap()
		.starts_with("動画サイトへのアクセスが一時的に制限されています。"));
	assert_eq!(embed.fields.len(), 1);
	assert_eq!(embed.fields[0].name, "🔧 管理者向け");
	assert!(!embed.fields[0].inline);
}

#[test]
fn notice_embed_title_only() {
	let embed = notice_embed(&messages(), &ui(), "?", &notice("queue.shuffled"), None);
	assert_eq!(
		embed.title.as_deref(),
		Some("🔀 キューをシャッフルしました。")
	);
	assert!(embed.description.is_none());
}

#[test]
fn notice_embed_fills_prefix() {
	let embed = notice_embed(&messages(), &ui(), "!", &notice("error.rejoin_limit"), None);
	assert!(embed.description.unwrap().contains("!rejoin"));
}

/// 試験用の曲のカード。
fn track_card() -> TrackCard {
	TrackCard {
		guild: Id::new(1),
		video_id: VideoId("abcdefghijk".to_owned()),
		title: "曲名".to_owned(),
		duration_s: Some(225),
		requester: Id::new(30),
		waiting: 2,
	}
}

/// 試験用のリクエストした人。
fn requester() -> Requester {
	Requester {
		name: "花子".to_owned(),
		icon_url: "https://x/a.png".to_owned(),
	}
}

/// カード付きの通知を Embed にする。
fn card_notice_embed(key: &'static str, card: Card, requester: Option<&Requester>) -> Embed {
	let mut n = notice(key);
	n.card = Some(card);
	notice_embed(&messages(), &ui(), "?", &n, requester)
}

/// 項目を (名前, 値, 横並び) で取り出す。
fn fields(embed: &Embed) -> Vec<(String, String, bool)> {
	embed
		.fields
		.iter()
		.map(|f| (f.name.clone(), f.value.clone(), f.inline))
		.collect()
}

#[test]
fn now_playing_card_fields() {
	let r = requester();
	let embed = card_notice_embed("play.now_playing", Card::NowPlaying(track_card()), Some(&r));
	assert_eq!(embed.title.as_deref(), Some("🎵 再生中"));
	assert_eq!(embed.color, Some(COLOR_GREEN));
	assert_eq!(
		fields(&embed),
		vec![
			(
				"タイトル".to_owned(),
				"[曲名](https://www.youtube.com/watch?v=abcdefghijk)".to_owned(),
				false
			),
			("再生時間".to_owned(), "03:45".to_owned(), true),
			("待機数".to_owned(), "2 曲".to_owned(), true),
		]
	);
	assert_eq!(
		embed.image.unwrap().url,
		"https://i.ytimg.com/vi/abcdefghijk/hqdefault.jpg"
	);
	let footer = embed.footer.unwrap();
	assert_eq!(footer.text, "Requested by: 花子");
	assert_eq!(footer.icon_url.as_deref(), Some("https://x/a.png"));
}

#[test]
fn queued_card_fields() {
	let r = requester();
	let embed = card_notice_embed("play.queued", Card::Queued(track_card()), Some(&r));
	assert_eq!(embed.title.as_deref(), Some("✅ キューに追加"));
	assert_eq!(embed.color, Some(COLOR_BLUE));
	let names: Vec<String> = fields(&embed).into_iter().map(|f| f.0).collect();
	assert_eq!(names, vec!["タイトル", "再生時間", "待機数"]);
	assert_eq!(embed.footer.unwrap().text, "Requested by: 花子");
}

#[test]
fn card_without_requester_uses_mention_field() {
	let embed = card_notice_embed("play.now_playing", Card::NowPlaying(track_card()), None);
	assert!(embed.footer.is_none());
	assert_eq!(
		fields(&embed).last().cloned(),
		Some(("リクエスト".to_owned(), "<@30>".to_owned(), true))
	);
}

#[test]
fn card_title_is_escaped() {
	let mut card = track_card();
	card.title = "a]b*c".to_owned();
	let embed = card_notice_embed("play.queued", Card::Queued(card), None);
	assert_eq!(
		embed.fields[0].value,
		r"[a\]b\*c](https://www.youtube.com/watch?v=abcdefghijk)"
	);
}

/// 試験用のプレイリストのカード。
fn playlist_card() -> PlaylistCard {
	PlaylistCard {
		guild: Id::new(1),
		title: Some("名前".to_owned()),
		url: "https://www.youtube.com/playlist?list=PLx".to_owned(),
		first_video: Some(VideoId("abcdefghijk".to_owned())),
		added: 25,
		rejected: 0,
		requester: Id::new(30),
	}
}

#[test]
fn playlist_card_fields() {
	let r = requester();
	let embed = card_notice_embed(
		"play.playlist_queued",
		Card::Playlist(playlist_card()),
		Some(&r),
	);
	assert_eq!(
		embed.title.as_deref(),
		Some("📝 プレイリストをキューに追加")
	);
	assert_eq!(embed.color, Some(COLOR_BLUE));
	assert_eq!(
		fields(&embed),
		vec![
			(
				"プレイリスト名".to_owned(),
				"[名前](https://www.youtube.com/playlist?list=PLx)".to_owned(),
				false
			),
			("追加曲数".to_owned(), "25 曲".to_owned(), true),
		]
	);
	assert_eq!(
		embed.image.unwrap().url,
		"https://i.ytimg.com/vi/abcdefghijk/hqdefault.jpg"
	);
	assert_eq!(embed.footer.unwrap().text, "Requested by: 花子");
}

#[test]
fn playlist_card_truncated_has_rejected_field() {
	let mut card = playlist_card();
	card.rejected = 5;
	let embed = card_notice_embed("play.playlist_truncated", Card::Playlist(card), None);
	let all = fields(&embed);
	assert_eq!(
		all[2],
		(
			"追加できなかった曲".to_owned(),
			"5 曲（キューの上限）".to_owned(),
			true
		)
	);
	assert_eq!(all[3].0, "リクエスト");
}

#[test]
fn playlist_card_without_title() {
	let mut card = playlist_card();
	card.title = None;
	card.first_video = None;
	let embed = card_notice_embed("play.playlist_queued", Card::Playlist(card), None);
	assert_eq!(
		embed.fields[0].value,
		"[Unknown Title](https://www.youtube.com/playlist?list=PLx)"
	);
	assert!(embed.image.is_none());
}

#[test]
fn now_playing_without_card_is_plain() {
	let mut n = notice("play.now_playing");
	n.args.push(("title", "曲名".to_owned()));
	let embed = notice_embed(&messages(), &ui(), "?", &n, None);
	assert!(embed.fields.is_empty());
	assert!(embed.image.is_none());
	assert_eq!(embed.title.as_deref(), Some("🎵 再生中"));
}

#[test]
fn format_duration_cases() {
	// 旧 Bot と同じ MM:SS / HH:MM:SS。不明と 0 は 00:00
	assert_eq!(format_duration(None), "00:00");
	assert_eq!(format_duration(Some(65)), "01:05");
	assert_eq!(format_duration(Some(3 * 3600 + 69)), "03:01:09");
	assert_eq!(format_duration(Some(3725)), "01:02:05");
	assert_eq!(format_duration(Some(0)), "00:00");
}

#[test]
fn track_page_lines() {
	let lines = vec![
		TrackLine {
			number: 1,
			video_id: VideoId("aaaaaaaaaaa".to_owned()),
			title: "一曲目".to_owned(),
			duration_s: Some(125),
		},
		TrackLine {
			number: 2,
			video_id: VideoId("bbbbbbbbbbb".to_owned()),
			title: "い".repeat(60),
			duration_s: None,
		},
	];
	let embed = track_page("キュー 1/1".to_owned(), &lines, &ui());
	assert_eq!(embed.title.as_deref(), Some("キュー 1/1"));
	assert_eq!(embed.color, Some(COLOR_BLUE));
	let description = embed.description.unwrap();
	let rows: Vec<&str> = description.lines().collect();
	assert_eq!(
		rows[0],
		"**1.** [一曲目](https://www.youtube.com/watch?v=aaaaaaaaaaa) `[02:05]`"
	);
	let cut = format!("{}...", "い".repeat(42));
	assert_eq!(
		rows[1],
		format!("**2.** [{cut}](https://www.youtube.com/watch?v=bbbbbbbbbbb) `[00:00]`")
	);
}

#[test]
fn track_page_drops_links_near_limit() {
	let lines: Vec<TrackLine> = (1..=PER_PAGE * 20)
		.map(|n| TrackLine {
			number: n,
			video_id: VideoId("aaaaaaaaaaa".to_owned()),
			title: "x".repeat(45),
			duration_s: Some(1),
		})
		.collect();
	let embed = track_page("t".to_owned(), &lines, &ui());
	assert!(embed.description.unwrap().chars().count() <= DESCRIPTION_LIMIT);
}

#[test]
fn track_page_plain_rows_are_escaped() {
	// 長い URL の雛形でリンク付きの行を 1 行 1000 文字超にし、4 行目からリンクなしにする
	let mut config_ui = ui();
	config_ui.watch_url = format!("https://example.com/{}/{{id}}", "p".repeat(1000));
	let lines: Vec<TrackLine> = (1..=10)
		.map(|n| TrackLine {
			number: n,
			video_id: VideoId("aaaaaaaaaaa".to_owned()),
			title: "*x*\nsecond".to_owned(),
			duration_s: Some(1),
		})
		.collect();
	let embed = track_page("t".to_owned(), &lines, &config_ui);
	let description = embed.description.unwrap();
	assert!(description.chars().count() <= DESCRIPTION_LIMIT);
	let plain: Vec<&str> = description
		.lines()
		.filter(|row| !row.contains("]("))
		.collect();
	assert!(!plain.is_empty(), "リンクなしの行が無い");
	for row in &plain {
		assert!(row.contains(r"\*x\* second"), "{row}");
	}
	// 改行が行の中に漏れていない: すべての行が番号で始まる
	assert!(description.lines().all(|row| row.starts_with("**")));
	assert_eq!(description.lines().count(), 10);
}

#[test]
fn page_token_roundtrip_and_reject() {
	let token = PageToken {
		kind: PageKind::Queue,
		target: 3,
		slot: 'n',
		issued_at: 1_793_000_000,
	};
	let id = encode(&token);
	assert!(id.len() <= 100);
	assert_eq!(decode(&id), Some(token));
	for kind in [PageKind::Help, PageKind::History] {
		let t = PageToken {
			kind,
			target: 0,
			slot: 'f',
			issued_at: 1,
		};
		assert_eq!(decode(&encode(&t)), Some(t));
	}
	for bad in [
		"",
		"qlist",
		"qlist:n:x:1",
		"other:n:1:1",
		"qlist:z:1:1",
		"qlist:n:1:1:9",
		"qlist:nn:1:1",
	] {
		assert_eq!(decode(bad), None, "{bad}");
	}
}

#[test]
fn buttons_hide_jump_under_three_pages() {
	assert!(page_buttons(PageKind::Queue, 0, 0, 1).is_empty());
	assert!(page_buttons(PageKind::Queue, 0, 1, 1).is_empty());
	assert_eq!(buttons(&page_buttons(PageKind::Queue, 0, 2, 1)).len(), 2);
	assert_eq!(buttons(&page_buttons(PageKind::Queue, 0, 3, 1)).len(), 4);
}

#[test]
fn buttons_labels_styles_and_disabled() {
	let first = buttons(&page_buttons(PageKind::Queue, 0, 5, 1));
	let labels: Vec<&str> = first.iter().map(|b| b.1.as_str()).collect();
	assert_eq!(labels, ["❚◀", "◀", "▶", "▶❚"]);
	assert_eq!(first[0].2, ButtonStyle::Primary);
	assert_eq!(first[1].2, ButtonStyle::Success);
	assert_eq!(first[2].2, ButtonStyle::Success);
	assert_eq!(first[3].2, ButtonStyle::Primary);
	assert_eq!(
		first.iter().map(|b| b.3).collect::<Vec<_>>(),
		[true, true, false, false]
	);
	let last = buttons(&page_buttons(PageKind::Queue, 4, 5, 1));
	assert_eq!(
		last.iter().map(|b| b.3).collect::<Vec<_>>(),
		[false, false, true, true]
	);
	// 2 ページの先頭: 前が disabled、次が有効
	let two = buttons(&page_buttons(PageKind::Help, 0, 2, 1));
	assert_eq!(two.iter().map(|b| b.3).collect::<Vec<_>>(), [true, false]);
	// 押したときの移動先
	let token = decode(&first[2].0).unwrap();
	assert_eq!((token.target, token.slot, token.issued_at), (1, 'n', 1));
	let token = decode(&first[3].0).unwrap();
	assert_eq!(token.target, 4);
}

#[test]
fn buttons_custom_ids_unique() {
	for page in [0, 2, 4] {
		let ids: Vec<String> = buttons(&page_buttons(PageKind::Queue, page, 5, 1))
			.into_iter()
			.map(|b| b.0)
			.collect();
		let mut sorted = ids.clone();
		sorted.sort();
		sorted.dedup();
		assert_eq!(sorted.len(), 4, "{ids:?}");
	}
}

#[test]
fn expiry_and_clamp() {
	let token = PageToken {
		kind: PageKind::Help,
		target: 0,
		slot: 'f',
		issued_at: 1000,
	};
	assert!(!is_expired(&token, 1000, 600));
	assert!(!is_expired(&token, 1600, 600));
	assert!(is_expired(&token, 1601, 600));
	// 時計が戻っても期限切れにしない
	assert!(!is_expired(&token, 900, 600));
	assert_eq!(clamp_page(9, 3), 2);
	assert_eq!(clamp_page(1, 3), 1);
	assert_eq!(clamp_page(5, 0), 0);
	assert_eq!(total_pages(0, 10), 1);
	assert_eq!(total_pages(10, 10), 1);
	assert_eq!(total_pages(11, 10), 2);
	assert_eq!(total_pages(5, 0), 1);
}

#[test]
fn help_pages_exist_and_use_prefix() {
	let m = messages();
	for n in 1..=3 {
		let title = m.render(&format!("help.title_{n}"), &[]);
		let fields = m.fields(&format!("help.page_{n}"), &[("prefix", "?")]);
		assert!(!title.starts_with("help."), "{title}");
		assert!(!fields.is_empty(), "page {n}");
		assert!(
			fields.iter().all(|(name, _)| name.starts_with('?')),
			"page {n}"
		);
	}
}

/// キャッシュから取った体の、リクエストした人の情報。
fn source() -> RequesterSource {
	RequesterSource {
		nick: None,
		member_avatar: None,
		global_name: None,
		username: None,
		user_avatar: None,
	}
}

#[test]
fn requester_prefers_nick_and_member_avatar() {
	let mut src = source();
	src.nick = Some("花".to_owned());
	src.global_name = Some("はな".to_owned());
	src.username = Some("hana".to_owned());
	src.member_avatar = Some("abc".to_owned());
	src.user_avatar = Some("def".to_owned());
	let r = requester_from(&ui(), Id::new(1), Id::new(30), &src).unwrap();
	assert_eq!(r.name, "花");
	assert_eq!(
		r.icon_url,
		"https://cdn.discordapp.com/guilds/1/users/30/avatars/abc.png"
	);
}

#[test]
fn requester_falls_back_to_global_name_then_username() {
	let mut src = source();
	src.global_name = Some("はな".to_owned());
	src.username = Some("hana".to_owned());
	src.user_avatar = Some("def".to_owned());
	let r = requester_from(&ui(), Id::new(1), Id::new(30), &src).unwrap();
	assert_eq!(r.name, "はな");
	assert_eq!(r.icon_url, "https://cdn.discordapp.com/avatars/30/def.png");
	src.global_name = None;
	let r = requester_from(&ui(), Id::new(1), Id::new(30), &src).unwrap();
	assert_eq!(r.name, "hana");
}

#[test]
fn requester_default_avatar_index() {
	let mut src = source();
	src.username = Some("hana".to_owned());
	// アバターが無ければ既定のアバター。番号は (ユーザー ID >> 22) % 6
	let user = (5u64 << 22) | 7;
	let r = requester_from(&ui(), Id::new(1), Id::new(user), &src).unwrap();
	assert_eq!(r.icon_url, "https://cdn.discordapp.com/embed/avatars/5.png");
}

#[test]
fn requester_none_without_any_name() {
	let mut src = source();
	src.user_avatar = Some("def".to_owned());
	assert!(requester_from(&ui(), Id::new(1), Id::new(30), &src).is_none());
}

#[test]
fn card_fallback_embed_is_short_and_red() {
	let mut card = track_card();
	card.title = "あ".repeat(60);
	let embed = card_fallback_embed(&messages(), "play.now_playing", &Card::NowPlaying(card));
	assert_eq!(embed.title.as_deref(), Some("🎵 再生中"));
	assert_eq!(embed.color, Some(COLOR_RED));
	assert!(embed.image.is_none());
	let description = embed.description.unwrap();
	let rows: Vec<&str> = description.lines().collect();
	assert_eq!(rows[0].chars().count(), 50);
	assert!(rows[0].ends_with("..."));
	assert_eq!(rows[1], "(詳細情報の表示に失敗しました)");
}

#[test]
fn card_fallback_embed_for_playlist_uses_name() {
	let embed = card_fallback_embed(
		&messages(),
		"play.playlist_queued",
		&Card::Playlist(playlist_card()),
	);
	assert_eq!(
		embed.title.as_deref(),
		Some("📝 プレイリストをキューに追加")
	);
	assert!(embed.description.unwrap().starts_with("名前\n"));
}

#[test]
fn disable_all_keeps_labels_and_disables() {
	let components = page_buttons(PageKind::Queue, 1, 5, 1_793_000_000);
	let before = buttons(&components);
	let after = buttons(&disable_all(&components));
	assert_eq!(after.len(), 4);
	for (b, a) in before.iter().zip(&after) {
		assert_eq!((&b.0, &b.1, b.2), (&a.0, &a.1, a.2));
		assert!(a.3, "灰色になっていない: {a:?}");
	}
}

#[test]
fn card_fallback_embed_escapes_markdown() {
	let mut card = track_card();
	card.title = "# **a** __b__".to_owned();
	let embed = card_fallback_embed(&messages(), "play.queued", &Card::Queued(card));
	let description = embed.description.unwrap();
	let first = description.lines().next().unwrap();
	assert_eq!(first, r"\# \*\*a\*\* \_\_b\_\_");
}

#[test]
fn card_fallback_embed_escapes_line_start_after_spaces() {
	for (title, expected) in [("  # a", r"\# a"), ("  -# a", r"\-# a"), (" > a", r"\> a")] {
		let mut card = track_card();
		card.title = title.to_owned();
		let embed = card_fallback_embed(&messages(), "play.queued", &Card::Queued(card));
		let description = embed.description.unwrap();
		assert_eq!(description.lines().next().unwrap(), expected, "{title:?}");
	}
}

#[test]
fn catalog_loads_both_languages() {
	let catalog = MessageCatalog::load(&root("messages")).unwrap();
	let ja = catalog.get(Lang::Ja).notice("setting.lang", &[]);
	let en = catalog.get(Lang::En).notice("setting.lang", &[]);
	assert!(ja.text.is_some());
	assert_ne!(ja.text, en.text);
}

#[test]
fn catalog_fails_when_a_language_file_is_missing() {
	let dir = tempfile::tempdir().unwrap();
	std::fs::copy(root("messages/ja.toml"), dir.path().join("ja.toml")).unwrap();
	assert!(matches!(
		MessageCatalog::load(dir.path()),
		Err(ConfigError::Read { .. })
	));
}

#[test]
fn lang_parse_ignores_case() {
	assert_eq!(Lang::parse("EN"), Some(Lang::En));
	assert_eq!(Lang::parse("ja"), Some(Lang::Ja));
	assert_eq!(Lang::parse("fr"), None);
	assert_eq!(Lang::parse(""), None);
}

#[test]
fn guild_langs_falls_back_to_default() {
	let guild = |n: u64| Id::new(n);
	let langs = GuildLangs::new(Lang::Ja, vec![(guild(1), Lang::En)]);
	assert_eq!(langs.get(Some(guild(1))), Lang::En);
	assert_eq!(langs.get(Some(guild(2))), Lang::Ja);
	assert_eq!(langs.get(None), Lang::Ja);
	langs.set(guild(2), Lang::En);
	assert_eq!(langs.get(Some(guild(2))), Lang::En);
}

/// 英語の文面で、数が 1 のときに「1 tracks」「1 seconds」のような複数形が出ないこと。
#[test]
fn english_counts_read_correctly_for_one() {
	let catalog = MessageCatalog::load(&root("messages")).unwrap();
	let en = catalog.get(Lang::En);
	let one = [
		("count", "1"),
		("value", "1"),
		("secs", "1"),
		("limit", "1"),
	];
	let mut texts: Vec<String> = [
		"ui.field_waiting",
		"ui.field_added_value",
		"ui.field_rejected_value",
	]
	.iter()
	.map(|key| en.render(key, &one))
	.collect();
	for key in [
		"queue.cleared",
		"setting.queue_limit",
		"setting.playlist_limit",
		"setting.autoleave",
		"play.queue_full",
	] {
		let notice = en.notice(key, &one);
		texts.extend(notice.title);
		texts.extend(notice.text);
	}
	for text in texts {
		let plain = text.replace("**", "");
		for plural in ["1 tracks", "1 seconds"] {
			assert!(!plain.contains(plural), "{text}");
		}
	}
}

#[test]
fn inline_title_escapes_markdown_and_code_span_keeps_the_span() {
	use watapachi_bot::ui::embed::{code_span_text, escape_markdown};
	assert_eq!(escape_markdown("[x](y) *a*"), r"\[x\](y) \*a\*");
	assert_eq!(escape_markdown("a\nb"), "a b");
	assert_eq!(code_span_text("a`b_c\r\nd"), "a'b_c  d");
}
