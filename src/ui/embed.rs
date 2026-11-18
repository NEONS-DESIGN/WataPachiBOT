//! 通知と曲の一覧を Embed にする。
//! - 本文は messages の key を args で描画したもの。色は key の分類で決める
//! - Discord の上限 (説明 4096 文字、フィールド 1024 文字) を超える文字列は末尾を `...` にして切る

use twilight_model::channel::message::Embed;
use twilight_util::builder::embed::{
	EmbedBuilder, EmbedFieldBuilder, EmbedFooterBuilder, ImageSource,
};

use crate::config::UiConfig;
use crate::domain::VideoId;
use crate::guild::notifier::{Card, Notice, PlaylistCard, TrackCard};
use crate::messages::{Messages, NoticeColor, RenderedNotice};

/// エラーの色。
pub const COLOR_RED: u32 = 0xFF4686;
/// 注意の色。
pub const COLOR_YELLOW: u32 = 0xF1C40F;
/// 操作の結果の色。
pub const COLOR_GREEN: u32 = 0x00976B;
/// 再生と一覧の色。
pub const COLOR_BLUE: u32 = 0x1F64E1;

/// Embed の説明の上限文字数。
pub const DESCRIPTION_LIMIT: usize = 4096;
/// Embed のフィールド値の上限文字数。
pub const FIELD_VALUE_LIMIT: usize = 1024;
/// Embed のフィールド名の上限文字数。
pub const FIELD_NAME_LIMIT: usize = 256;
/// Embed のフッターの上限文字数。
pub const FOOTER_LIMIT: usize = 2048;
/// 1 つの Embed に付けられるフィールドの上限数。
pub const MAX_FIELDS: usize = 25;
/// 再生中のカードのタイトルの上限文字数。
const CARD_TITLE_LIMIT: usize = 100;
/// カードを拒まれたときの簡易表示の、曲名の上限文字数 (旧 Bot と同じ)。
const FALLBACK_TITLE_LIMIT: usize = 50;
/// 一覧 1 行のタイトルの上限文字数。
const LIST_TITLE_LIMIT: usize = 45;
/// 一覧の 1 ページの件数。
pub const PER_PAGE: u32 = 10;
/// Embed の題名の上限文字数。
pub const TITLE_LIMIT: usize = 256;
/// 長さが分からない (0 を含む) ときの表示。旧 Bot と同じ
const UNKNOWN_DURATION: &str = "00:00";
/// 切り詰めた文字列の末尾。
const ELLIPSIS: &str = "...";

/// 一覧の 1 行。`?qlist` と `?history` で共用する。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackLine {
	/// 一覧に出す番号 (1 始まり)
	pub number: u32,
	pub video_id: VideoId,
	pub title: String,
	pub duration_s: Option<u32>,
}

/// 文字数 (バイト数ではない) が `limit` を超えるなら、末尾を `...` にして `limit` 文字に収める。
///
/// # 例
/// ```
/// # use watapachi_bot::ui::embed::truncate;
/// assert_eq!(truncate("abcdef", 5), "ab...");
/// assert_eq!(truncate("abc", 5), "abc");
/// ```
pub fn truncate(text: &str, limit: usize) -> String {
	if text.chars().count() <= limit {
		return text.to_owned();
	}
	let keep = limit.saturating_sub(ELLIPSIS.chars().count());
	let mut out: String = text.chars().take(keep).collect();
	out.push_str(ELLIPSIS);
	out
}

/// 秒数を `MM:SS` (1 時間以上は `HH:MM:SS`) にする。None と 0 は `00:00` (旧 Bot と同じ)。
///
/// # 例
/// ```
/// # use watapachi_bot::ui::embed::format_duration;
/// assert_eq!(format_duration(Some(65)), "01:05");
/// assert_eq!(format_duration(None), "00:00");
/// ```
pub fn format_duration(secs: Option<u32>) -> String {
	let Some(secs) = secs.filter(|s| *s > 0) else {
		return UNKNOWN_DURATION.to_owned();
	};
	let (hours, minutes, seconds) = (secs / 3600, secs / 60 % 60, secs % 60);
	if hours > 0 {
		format!("{hours:02}:{minutes:02}:{seconds:02}")
	} else {
		format!("{minutes:02}:{seconds:02}")
	}
}

/// 色の分類を Embed の色の値にする。
///
/// # 例
/// ```
/// # use watapachi_bot::messages::NoticeColor;
/// # use watapachi_bot::ui::embed::{color_value, COLOR_RED};
/// assert_eq!(color_value(NoticeColor::Red), COLOR_RED);
/// ```
pub fn color_value(color: NoticeColor) -> u32 {
	match color {
		NoticeColor::Red => COLOR_RED,
		NoticeColor::Yellow => COLOR_YELLOW,
		NoticeColor::Green => COLOR_GREEN,
		NoticeColor::Blue => COLOR_BLUE,
	}
}

/// 行頭にあると見出し・引用・箇条書きとして効く文字。
const LINE_START_SPECIALS: &[char] = &['#', '>', '-'];
/// Markdown の書式として効く文字 (バックスラッシュは別に扱う)。
const MARKDOWN_SPECIALS: &[char] = &['[', ']', '*', '_', '`', '~', '|'];
/// コードの囲み (バッククォート) の中へ入れる文字列で、バッククォートの代わりに使う文字。
/// - コードの中ではバックスラッシュの逃がしが効かず、バッククォートがあると囲みが途中で閉じるため
const CODE_SPAN_BACKTICK_SUBSTITUTE: char = '\'';

/// タイトルを Discord の Markdown として効かない文字列にする。
///
/// - 改行 (CR、LF) は空白にする (リンクの記法が途切れるため)
/// - バックスラッシュを先に逃がし、続けて `[` `]` `*` `_` `` ` `` `~` `|` を逃がす
/// - 地の文へ差し込む値に使う。コードの囲みの中へ入れる値には `code_span_text` を使う
///
/// # 引数
/// - `title`: 逃がす文字列
///
/// # 戻り値
/// 逃がした文字列
///
/// # 例
/// ```ignore
/// assert_eq!(escape_markdown("a_b"), "a\\_b");
/// ```
pub fn escape_markdown(title: &str) -> String {
	let mut out = String::with_capacity(title.len());
	for c in title.chars() {
		match c {
			'\r' | '\n' => out.push(' '),
			'\\' => out.push_str("\\\\"),
			c if MARKDOWN_SPECIALS.contains(&c) => {
				out.push('\\');
				out.push(c);
			}
			c => out.push(c),
		}
	}
	out
}

/// コードの囲み (`` `{title}` ``) の中へ入れても囲みが壊れない文字列にする。
///
/// - バッククォートは `CODE_SPAN_BACKTICK_SUBSTITUTE` に、改行 (CR、LF) は空白に置き換える
/// - 他の文字はそのまま (コードの中では Markdown が効かないため)
///
/// # 引数
/// - `text`: 囲みの中へ入れる文字列
///
/// # 戻り値
/// 置き換えた文字列
///
/// # 例
/// ```ignore
/// assert_eq!(code_span_text("a`b"), "a'b");
/// ```
pub fn code_span_text(text: &str) -> String {
	text.chars()
		.map(|c| match c {
			'`' => CODE_SPAN_BACKTICK_SUBSTITUTE,
			'\r' | '\n' => ' ',
			c => c,
		})
		.collect()
}

/// 行頭で見出し・引用・箇条書きとして効く文字 (`#`・`>`・`-`) で始まるなら、その 1 文字を逃がす。
///
/// - `escape_markdown` の後に使う (改行は空白になっているため、行頭は文字列の先頭だけ)
/// - 先頭の空白は除いてから判定する (`"  # a"` のように空白を前に置いても見出しとして効かないようにする)
///
/// # 例
/// ```ignore
/// assert_eq!(escape_line_start("  # a"), "\\# a");
/// ```
fn escape_line_start(text: &str) -> String {
	let trimmed = text.trim_start();
	if trimmed.starts_with(LINE_START_SPECIALS) {
		format!("\\{trimmed}")
	} else {
		trimmed.to_owned()
	}
}

/// 逃がし済みの文字列を `limit` 文字に収める。
///
/// - 切り口が逃がしの途中 (末尾が奇数個のバックスラッシュ) になったら、その 1 文字を落とす
fn truncate_escaped(escaped: &str, limit: usize) -> String {
	if escaped.chars().count() <= limit {
		return escaped.to_owned();
	}
	let keep = limit.saturating_sub(ELLIPSIS.chars().count());
	let mut out: String = escaped.chars().take(keep).collect();
	let trailing = out.chars().rev().take_while(|c| *c == '\\').count();
	if trailing % 2 == 1 {
		out.pop();
	}
	out.push_str(ELLIPSIS);
	out
}

/// `[タイトル](URL)` の記法を作る。
///
/// - タイトルは Markdown として効かないように逃がす (改行は空白、バックスラッシュと `[` `]` `*` `_` `` ` `` `~` `|`)
/// - URL の丸括弧は記法を壊すので `%28` `%29` にする
/// - 記法が `limit` 文字を超えるなら、逃がしたタイトルだけ (`limit` 文字に切る) にする
///
/// # 例
/// ```
/// # use watapachi_bot::ui::embed::title_link;
/// assert_eq!(title_link("a", "https://x/", 100), "[a](https://x/)");
/// ```
pub fn title_link(title: &str, url: &str, limit: usize) -> String {
	let text = escape_markdown(title);
	let url = url.replace('(', "%28").replace(')', "%29");
	let link = format!("[{text}]({url})");
	if link.chars().count() <= limit {
		link
	} else {
		truncate_escaped(&text, limit)
	}
}

/// 動画の URL。`ui.watch_url` の `{id}` を動画 ID に置き換える。
pub fn watch_url(ui: &UiConfig, id: &VideoId) -> String {
	ui.watch_url.replace("{id}", &id.0)
}

/// 動画のサムネイルの URL。`ui.thumbnail_url` の `{id}` を動画 ID に置き換える。
pub fn thumbnail_url(ui: &UiConfig, id: &VideoId) -> String {
	ui.thumbnail_url.replace("{id}", &id.0)
}

/// カードのフッターに出す、リクエストした人の表示名とアイコン。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Requester {
	pub name: String,
	pub icon_url: String,
}

/// 通知 1 件を Embed にする。
///
/// - 題名・本文・色は `notice.key` の通知を `notice.args` と `{prefix}` で描画したもの
///   (題名は 256 文字、本文は 4096 文字で切る)
/// - 管理者向けの文があれば、`ui.admin_field` の項目 (横に並べない) にする
/// - `card` があれば、`card_embed` でカードにする
///
/// # 引数
/// - `messages`: メッセージ表
/// - `ui`: URL の雛形
/// - `prefix`: コマンドの接頭辞 (本文の `{prefix}` を置き換える)
/// - `notice`: 通知
/// - `requester`: カードを頼んだ人の表示。分からなければ None (カードにメンションの項目を足す)
///
/// # 例
/// ```ignore
/// let embed = notice_embed(&messages, &config.ui, "?", &notice, None);
/// ```
pub fn notice_embed(
	messages: &Messages,
	ui: &UiConfig,
	prefix: &str,
	notice: &Notice,
	requester: Option<&Requester>,
) -> Embed {
	if let Some(card) = &notice.card {
		return card_embed(messages, ui, notice.key, card, requester);
	}
	let mut args: Vec<(&str, &str)> = notice
		.args
		.iter()
		.map(|(name, value)| (*name, value.as_str()))
		.collect();
	if !args.iter().any(|(name, _)| *name == "prefix") {
		args.push(("prefix", prefix));
	}
	let rendered = messages.notice(notice.key, &args);
	let mut builder = titled_builder(&rendered);
	if let Some(text) = &rendered.text {
		builder = builder.description(truncate(text, DESCRIPTION_LIMIT));
	}
	if let Some(admin) = &rendered.admin {
		builder = builder.field(EmbedFieldBuilder::new(
			messages.render("ui.admin_field", &[]),
			truncate(admin, FIELD_VALUE_LIMIT),
		));
	}
	builder.build()
}

/// 題名 (256 文字で切る) と色だけを持つ Embed の組み立てを始める。
fn titled_builder(rendered: &RenderedNotice) -> EmbedBuilder {
	let builder = EmbedBuilder::new().color(color_value(rendered.color));
	match &rendered.title {
		Some(title) => builder.title(truncate(title, TITLE_LIMIT)),
		None => builder,
	}
}

/// 曲のカードの Embed。題名と色は `key` の通知から取る。
///
/// - 曲 (再生中・キューに追加)：タイトル (100 文字で切ったリンク)、再生時間と待機数 (横並び)、サムネイル
/// - プレイリスト：プレイリスト名のリンク、追加曲数 (と断った曲数。横並び)、1 曲目のサムネイル
/// - `requester` があれば「Requested by: 名前」とアイコンのフッター、無ければメンションの項目 (横並び) を足す
///
/// # 引数
/// - `messages`: メッセージ表
/// - `ui`: URL の雛形
/// - `key`: 通知のキー (`play.now_playing` など)
/// - `card`: カードの中身
/// - `requester`: 頼んだ人の表示
///
/// # 例
/// ```ignore
/// let embed = card_embed(&messages, &config.ui, "play.queued", &card, Some(&requester));
/// ```
pub fn card_embed(
	messages: &Messages,
	ui: &UiConfig,
	key: &str,
	card: &Card,
	requester: Option<&Requester>,
) -> Embed {
	let rendered = messages.notice(key, &[]);
	let mut builder = titled_builder(&rendered);
	let image = match card {
		Card::NowPlaying(track) | Card::Queued(track) => {
			builder = track_fields(builder, messages, ui, track);
			Some(thumbnail_url(ui, &track.video_id))
		}
		Card::Playlist(list) => {
			builder = playlist_fields(builder, messages, list);
			list.first_video.as_ref().map(|id| thumbnail_url(ui, id))
		}
	};
	builder = with_requester(builder, messages, card, requester);
	// 雛形が不正な URL になっても、画像を付けないだけで通知は出す
	if let Some(image) = image.and_then(|url| ImageSource::url(url).ok()) {
		builder = builder.image(image);
	}
	builder.build()
}

/// Discord がカードを拒んだときに送り直す簡易表示 (旧 Bot と同じ)。
///
/// - 題名はカードの通知の題名、本文は `ui.card_failed` に曲名かプレイリスト名を 50 文字で切って差し込んだもの、色は赤
/// - 名前は切った後に Markdown として効かないように逃がす (行頭の `#`・`>`・`-` も逃がす)
/// - 項目、画像、フッターは付けない (拒まれた原因になりうるため)
///
/// # 引数
/// - `messages`: メッセージ表
/// - `key`: カードの通知のキー
/// - `card`: カードの中身
///
/// # 例
/// ```ignore
/// let embed = card_fallback_embed(&messages, "play.now_playing", &card);
/// ```
pub fn card_fallback_embed(messages: &Messages, key: &str, card: &Card) -> Embed {
	let rendered = messages.notice(key, &[]);
	let name = match card {
		Card::NowPlaying(track) | Card::Queued(track) => track.title.clone(),
		Card::Playlist(list) => list
			.title
			.clone()
			.unwrap_or_else(|| messages.render("ui.unknown_title", &[])),
	};
	let shown = escape_line_start(&escape_markdown(&truncate(&name, FALLBACK_TITLE_LIMIT)));
	let text = messages.render("ui.card_failed", &[("title", &shown)]);
	let mut builder = EmbedBuilder::new()
		.color(COLOR_RED)
		.description(truncate(&text, DESCRIPTION_LIMIT));
	if let Some(title) = &rendered.title {
		builder = builder.title(truncate(title, TITLE_LIMIT));
	}
	builder.build()
}

/// 曲のカードの項目 (タイトル、再生時間、待機数) を足す。
fn track_fields(
	builder: EmbedBuilder,
	messages: &Messages,
	ui: &UiConfig,
	track: &TrackCard,
) -> EmbedBuilder {
	let link = title_link(
		&truncate(&track.title, CARD_TITLE_LIMIT),
		&watch_url(ui, &track.video_id),
		FIELD_VALUE_LIMIT,
	);
	let waiting = messages.render("ui.field_waiting", &[("count", &track.waiting.to_string())]);
	builder
		.field(EmbedFieldBuilder::new(
			messages.render("ui.field_title", &[]),
			link,
		))
		.field(
			EmbedFieldBuilder::new(
				messages.render("ui.field_duration", &[]),
				format_duration(track.duration_s),
			)
			.inline(),
		)
		.field(
			EmbedFieldBuilder::new(messages.render("ui.field_waiting_name", &[]), waiting).inline(),
		)
}

/// プレイリストのカードの項目 (プレイリスト名、追加曲数、断った曲数) を足す。
///
/// - 名前が無ければ `ui.unknown_title` を出す
/// - 断った曲数は 1 以上のときだけ足す
fn playlist_fields(
	builder: EmbedBuilder,
	messages: &Messages,
	list: &PlaylistCard,
) -> EmbedBuilder {
	let name = list
		.title
		.clone()
		.unwrap_or_else(|| messages.render("ui.unknown_title", &[]));
	let link = title_link(
		&truncate(&name, CARD_TITLE_LIMIT),
		&list.url,
		FIELD_VALUE_LIMIT,
	);
	let count = |key: &str, n: u32| messages.render(key, &[("count", &n.to_string())]);
	let mut builder = builder
		.field(EmbedFieldBuilder::new(
			messages.render("ui.field_playlist_name", &[]),
			link,
		))
		.field(
			EmbedFieldBuilder::new(
				messages.render("ui.field_added", &[]),
				count("ui.field_added_value", list.added),
			)
			.inline(),
		);
	if list.rejected > 0 {
		builder = builder.field(
			EmbedFieldBuilder::new(
				messages.render("ui.field_rejected", &[]),
				count("ui.field_rejected_value", list.rejected),
			)
			.inline(),
		);
	}
	builder
}

/// 頼んだ人のフッター、または (表示が分からなければ) メンションの項目を足す。
fn with_requester(
	builder: EmbedBuilder,
	messages: &Messages,
	card: &Card,
	requester: Option<&Requester>,
) -> EmbedBuilder {
	let Some(requester) = requester else {
		let (_, user) = card.requester();
		return builder.field(
			EmbedFieldBuilder::new(
				messages.render("ui.field_requester", &[]),
				format!("<@{}>", user.get()),
			)
			.inline(),
		);
	};
	let text = messages.render("ui.footer_requester", &[("name", &requester.name)]);
	let mut footer = EmbedFooterBuilder::new(truncate(&text, FOOTER_LIMIT));
	if let Ok(icon) = ImageSource::url(&requester.icon_url) {
		footer = footer.icon_url(icon);
	}
	builder.footer(footer)
}

/// 曲の一覧の 1 ページ (`?qlist` と `?history` で共用する)。
///
/// - 1 行は `**{番号}.** [タイトル](URL) `[長さ]``。タイトルは 45 文字で切る
/// - 説明の上限 (4096 文字) に収まらない行はリンクを外し、それでも収まらなければそこで打ち切る
///
/// # 引数
/// - `title`: Embed のタイトル (描画済み)
/// - `lines`: このページの行 (最大 `PER_PAGE` 件を想定)
/// - `ui`: URL の雛形
///
/// # 例
/// ```ignore
/// let title = messages.render("ui.queue_title", &[("page", "1"), ("total", "3")]);
/// let embed = track_page(title, &lines, &config.ui);
/// ```
pub fn track_page(title: String, lines: &[TrackLine], ui: &UiConfig) -> Embed {
	let mut description = String::new();
	let mut used = 0usize;
	for line in lines {
		let shown = truncate(&line.title, LIST_TITLE_LIMIT);
		let head = format!("**{}.** ", line.number);
		let tail = format!(" `[{}]`", format_duration(line.duration_s));
		let linked = format!(
			"{head}{}{tail}",
			title_link(&shown, &watch_url(ui, &line.video_id), DESCRIPTION_LIMIT)
		);
		let separator = usize::from(!description.is_empty());
		let row = if used + separator + linked.chars().count() <= DESCRIPTION_LIMIT {
			linked
		} else {
			// リンクを外した行も逃がす。残りの予算に収まるよう、逃がした後で切る
			let fixed = head.chars().count() + tail.chars().count();
			let room = DESCRIPTION_LIMIT.saturating_sub(used + separator + fixed);
			format!(
				"{head}{}{tail}",
				truncate_escaped(&escape_markdown(&shown), room)
			)
		};
		let row_len = row.chars().count();
		if used + separator + row_len > DESCRIPTION_LIMIT {
			break;
		}
		if separator == 1 {
			description.push('\n');
		}
		description.push_str(&row);
		used += separator + row_len;
	}
	EmbedBuilder::new()
		.title(title)
		.description(description)
		.color(COLOR_BLUE)
		.build()
}
