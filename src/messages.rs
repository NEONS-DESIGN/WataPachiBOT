//! ユーザー向けメッセージ（messages/ja.toml・messages/en.toml）の読み込みと置換。
//! - 言語ごとの表は `MessageCatalog` に束ねる。どの言語の表を使うかは呼び出し側が決める
//! - 読み込みは起動時の 1 回のみ。必須キーが 1 つでも無いか、形が違えば読み込みで失敗させる
//! - 通知は `notice`、部品の文字列は `render`、ヘルプの項目は `fields` で引く。どれもファイルを再読込しない

use std::collections::HashMap;
use std::path::Path;

use crate::error::ConfigError;
use crate::lang::Lang;

/// 通知のキー（`節.キー` の形式）。`{ title, text, color, admin }` のテーブルで持つ。
/// - 新しい通知を使うコードを足すときは、ここと messages/ja.toml・messages/en.toml に足す
pub const NOTICE_KEYS: &[&str] = &[
	"play.queued",
	"play.preparing",
	"play.slow",
	"play.now_playing",
	"play.playlist_queued",
	"play.playlist_truncated",
	"play.queue_full",
	"play.rate_limited",
	"play.pipe_fallback",
	"play.restored",
	"play.restored_paused",
	"play.completed",
	"error.not_in_voice",
	"error.video_private",
	"error.unsupported_url",
	"error.lookup_failed",
	"error.lookup_search_empty",
	"error.lookup_not_found",
	"error.lookup_private",
	"error.lookup_geo_blocked",
	"error.lookup_age_restricted",
	"error.lookup_timeout",
	"error.lookup_playlist_unavailable",
	"error.playlist_empty",
	"error.track_skipped",
	"error.connect_failed",
	"error.voice_no_permission",
	"error.voice_full",
	"error.nothing_playing",
	"error.already_paused",
	"error.not_paused",
	"error.not_connected",
	"error.same_channel",
	"error.index_out_of_range",
	"error.clear_out_of_range",
	"error.queue_empty",
	"error.history_empty",
	"error.volume_out_of_range",
	"error.audio_failed",
	"error.save_failed",
	"error.save_slow",
	"error.not_same_channel",
	"error.recovering",
	"error.video_not_found",
	"error.video_geo_blocked",
	"error.video_age_restricted",
	"error.rejoin_limit",
	"error.rejoin_failed",
	"error.fetch_stopped",
	"error.rate_limited_lookup",
	"error.rate_limited_stopped",
	"error.restore_failed",
	"error.live_unsupported",
	"error.live_skipped",
	"error.no_permission",
	"error.invalid_argument",
	"error.busy",
	"error.starting",
	"error.out_of_range",
	"error.read_failed",
	"control.rejoined",
	"control.kicked",
	"control.paused",
	"control.resumed",
	"control.skipped",
	"control.left",
	"control.auto_left",
	"control.moved",
	"control.volume",
	"control.loop_on",
	"control.loop_off",
	"queue.cleared",
	"queue.shuffled",
	"setting.admin_added",
	"setting.admin_already",
	"setting.admin_removed",
	"setting.admin_missing",
	"setting.queue_limit",
	"setting.playlist_limit",
	"setting.autoleave",
	"setting.autoleave_off",
	"setting.lang",
	"help.mention",
];

/// 部品の文字列のキー。Embed の項目名や一覧の題名など、単なる文字列で持つ。
pub const TEXT_KEYS: &[&str] = &[
	"ui.field_title",
	"ui.field_duration",
	"ui.field_waiting_name",
	"ui.field_waiting",
	"ui.field_playlist_name",
	"ui.field_added",
	"ui.field_added_value",
	"ui.field_rejected",
	"ui.field_rejected_value",
	"ui.field_requester",
	"ui.footer_requester",
	"ui.card_failed",
	"ui.unknown_title",
	"ui.admin_field",
	"ui.list_separator",
	"ui.perm_view_channel",
	"ui.perm_connect",
	"ui.perm_speak",
	"ui.queue_title",
	"ui.history_title",
	"help.title_1",
	"help.title_2",
	"help.title_3",
	"help.usage_p",
	"help.usage_pnow",
	"help.usage_clear",
	"help.usage_vol",
	"help.usage_setting",
];

/// 項目の並びのキー。`[[節.キー]]` の `name` と `value` を持つテーブルの配列で持つ。
pub const FIELD_KEYS: &[&str] = &["help.page_1", "help.page_2", "help.page_3"];

/// 通知の Embed の色の分類。値は `ui::embed::color_value` で引く。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeColor {
	Red,
	Yellow,
	Green,
	Blue,
}

impl NoticeColor {
	/// 文面のファイルの色の名前から読む。知らない名前は None。
	fn parse(name: &str) -> Option<NoticeColor> {
		match name {
			"red" => Some(NoticeColor::Red),
			"yellow" => Some(NoticeColor::Yellow),
			"green" => Some(NoticeColor::Green),
			"blue" => Some(NoticeColor::Blue),
			_ => None,
		}
	}
}

/// 差し込み済みの通知。題名と本文のどちらかは必ずある (未知のキーは本文にキー名)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedNotice {
	pub title: Option<String>,
	pub text: Option<String>,
	pub color: NoticeColor,
	/// 「管理者向け」の項目に出す文
	pub admin: Option<String>,
}

/// 通知の雛形。
#[derive(Debug, Clone)]
struct NoticeTemplate {
	title: Option<String>,
	text: Option<String>,
	color: NoticeColor,
	admin: Option<String>,
}

/// 表の 1 項目。
#[derive(Debug, Clone)]
enum Entry {
	Text(String),
	Notice(NoticeTemplate),
	Fields(Vec<(String, String)>),
}

/// 読み込み済みのメッセージ表。
#[derive(Debug, Clone)]
pub struct Messages {
	table: HashMap<String, Entry>,
}

impl Messages {
	/// メッセージファイルを読み込み、必須キーの存在と形を確かめる。
	///
	/// - 節（テーブル）直下の値を `節.キー` として取り込む。通知のテーブル、文字列、項目の配列のどれか
	///
	/// # 引数
	/// - `path`: 文面のファイル (messages/ja.toml など) のパス
	///
	/// # エラー
	/// - 読めない: `ConfigError::Read`
	/// - 構文エラー: `ConfigError::Parse`
	/// - 必須キーが無い: `ConfigError::MissingMessageKey`
	/// - 必須キーの形が違う (通知が文字列、色の名前が不正、題名も本文も無いなど): `ConfigError::InvalidMessage`
	///
	/// # 例
	/// ```no_run
	/// # use std::path::Path;
	/// # use watapachi_bot::messages::Messages;
	/// let messages = Messages::load(Path::new("messages/ja.toml"))?;
	/// # Ok::<(), watapachi_bot::error::ConfigError>(())
	/// ```
	pub fn load(path: &Path) -> Result<Messages, ConfigError> {
		let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
			path: path.to_path_buf(),
			source,
		})?;
		Self::parse_text(&text, path)
	}

	/// TOML 文字列から表を作る。`load` と同じ検査をする。
	///
	/// # 引数
	/// - `text`: TOML の本文
	/// - `path`: エラー表示用のパス
	///
	/// # エラー
	/// `load` と同じ（`Read` を除く）。
	fn parse_text(text: &str, path: &Path) -> Result<Messages, ConfigError> {
		let root: toml::Table = toml::from_str(text).map_err(|source| ConfigError::Parse {
			path: path.to_path_buf(),
			source: Box::new(source),
		})?;
		let mut raw: HashMap<String, &toml::Value> = HashMap::new();
		for (section, value) in &root {
			let Some(inner) = value.as_table() else {
				continue;
			};
			for (key, value) in inner {
				raw.insert(format!("{section}.{key}"), value);
			}
		}
		// 形の検査より先に、無いキーを報告する (書きかけのファイルで原因を分かりやすくするため)
		for key in NOTICE_KEYS.iter().chain(TEXT_KEYS).chain(FIELD_KEYS) {
			required(&raw, key)?;
		}
		let mut table = HashMap::new();
		for key in NOTICE_KEYS {
			let value = required(&raw, key)?;
			table.insert((*key).to_owned(), Entry::Notice(parse_notice(key, value)?));
		}
		for key in TEXT_KEYS {
			let value = required(&raw, key)?;
			let text = value
				.as_str()
				.ok_or_else(|| invalid(key, "文字列ではありません"))?;
			table.insert((*key).to_owned(), Entry::Text(text.to_owned()));
		}
		for key in FIELD_KEYS {
			let value = required(&raw, key)?;
			table.insert((*key).to_owned(), Entry::Fields(parse_fields(key, value)?));
		}
		Ok(Messages { table })
	}

	/// 通知を取り出し、題名・本文・管理者向けの文の `{name}` を引数の値で置換する。
	///
	/// - 未知のキー、または通知でないキーは、本文にキー名を入れた赤の通知を返す (パニックしない)
	///
	/// # 引数
	/// - `key`: `節.キー`
	/// - `args`: `(名前, 値)` の並び
	///
	/// # 戻り値
	/// 差し込み済みの通知。
	///
	/// # 例
	/// ```no_run
	/// # use watapachi_bot::messages::Messages;
	/// # fn f(messages: &Messages) {
	/// let notice = messages.notice("control.volume", &[("percent", "50")]);
	/// # }
	/// ```
	pub fn notice(&self, key: &str, args: &[(&str, &str)]) -> RenderedNotice {
		let Some(Entry::Notice(template)) = self.table.get(key) else {
			return RenderedNotice {
				title: None,
				text: Some(key.to_owned()),
				color: NoticeColor::Red,
				admin: None,
			};
		};
		let fill = |part: &Option<String>| part.as_deref().map(|t| substitute(t, args));
		RenderedNotice {
			title: fill(&template.title),
			text: fill(&template.text),
			color: template.color,
			admin: fill(&template.admin),
		}
	}

	/// 項目の並びを取り出し、名前と値の `{name}` を置換する。
	///
	/// # 引数
	/// - `key`: `節.キー` (`FIELD_KEYS` のどれか)
	/// - `args`: `(名前, 値)` の並び
	///
	/// # 戻り値
	/// `(名前, 値)` の並び。未知のキー、または項目でないキーは空
	///
	/// # 例
	/// ```no_run
	/// # use watapachi_bot::messages::Messages;
	/// # fn f(messages: &Messages) {
	/// let fields = messages.fields("help.page_1", &[("prefix", "?")]);
	/// # }
	/// ```
	pub fn fields(&self, key: &str, args: &[(&str, &str)]) -> Vec<(String, String)> {
		let Some(Entry::Fields(fields)) = self.table.get(key) else {
			return Vec::new();
		};
		fields
			.iter()
			.map(|(name, value)| (substitute(name, args), substitute(value, args)))
			.collect()
	}

	/// 部品の文字列を取り出し、`{name}` を引数の値で置換する。
	///
	/// - 1 回の走査で置換するので、値の中の `{x}` は再置換されない
	/// - 引数に無い `{name}` はそのまま残す
	/// - 未知のキー、または文字列でないキーはキー名そのものを返す（パニックしない）
	///
	/// # 引数
	/// - `key`: `節.キー` (`TEXT_KEYS` のどれか)
	/// - `args`: `(名前, 値)` の並び
	///
	/// # 戻り値
	/// 置換後の文字列。
	///
	/// # 例
	/// ```no_run
	/// # use watapachi_bot::messages::Messages;
	/// # fn f(messages: &Messages) {
	/// let text = messages.render("ui.footer_requester", &[("name", "花子")]);
	/// # }
	/// ```
	pub fn render(&self, key: &str, args: &[(&str, &str)]) -> String {
		match self.table.get(key) {
			Some(Entry::Text(template)) => substitute(template, args),
			_ => key.to_owned(),
		}
	}
}

/// 言語ごとのメッセージ表の束。
///
/// 呼び出し方:
/// ```no_run
/// # use std::path::Path;
/// # use watapachi_bot::lang::Lang;
/// # use watapachi_bot::messages::MessageCatalog;
/// let catalog = MessageCatalog::load(Path::new("messages"))?;
/// let text = catalog.get(Lang::En).render("ui.field_title", &[]);
/// # Ok::<(), watapachi_bot::error::ConfigError>(())
/// ```
#[derive(Debug, Clone)]
pub struct MessageCatalog {
	ja: Messages,
	en: Messages,
}

impl MessageCatalog {
	/// `dir/{code}.toml` を言語の数だけ読み込む。どれか 1 つでも読めなければ失敗する。
	///
	/// # 引数
	/// - `dir`: 文面のファイルを置いたディレクトリ（`messages`）
	///
	/// # エラー
	/// - `Messages::load` と同じ。どの言語のファイルかはエラーのパスで分かる
	///
	/// # 例
	/// ```no_run
	/// # use std::path::Path;
	/// # use watapachi_bot::messages::MessageCatalog;
	/// let catalog = MessageCatalog::load(Path::new("messages"))?;
	/// # Ok::<(), watapachi_bot::error::ConfigError>(())
	/// ```
	pub fn load(dir: &Path) -> Result<MessageCatalog, ConfigError> {
		let load = |lang: Lang| Messages::load(&dir.join(format!("{}.toml", lang.code())));
		Ok(MessageCatalog {
			ja: load(Lang::Ja)?,
			en: load(Lang::En)?,
		})
	}

	/// 言語のメッセージ表を返す。
	///
	/// # 引数
	/// - `lang`: 使う言語
	///
	/// # 戻り値
	/// その言語のメッセージ表 (読み込み時に検査済み)
	///
	/// # 呼び出し例
	/// ```ignore
	/// let messages = catalog.get(Lang::Ja);
	/// ```
	pub fn get(&self, lang: Lang) -> &Messages {
		match lang {
			Lang::Ja => &self.ja,
			Lang::En => &self.en,
		}
	}
}

/// 必須キーの値を取り出す。無ければ `MissingMessageKey`。
fn required<'a>(
	raw: &HashMap<String, &'a toml::Value>,
	key: &str,
) -> Result<&'a toml::Value, ConfigError> {
	raw.get(key)
		.copied()
		.ok_or_else(|| ConfigError::MissingMessageKey {
			key: key.to_owned(),
		})
}

/// `InvalidMessage` を作る。
fn invalid(key: &str, reason: &str) -> ConfigError {
	ConfigError::InvalidMessage {
		key: key.to_owned(),
		reason: reason.to_owned(),
	}
}

/// 通知のテーブルを読む。
///
/// # エラー
/// テーブルでない、色が無いか不正、題名と本文の両方が無い、文字列でない値がある: `InvalidMessage`
fn parse_notice(key: &str, value: &toml::Value) -> Result<NoticeTemplate, ConfigError> {
	let table = value
		.as_table()
		.ok_or_else(|| invalid(key, "title・text・color を持つテーブルではありません"))?;
	let text_of = |name: &str| -> Result<Option<String>, ConfigError> {
		match table.get(name) {
			None => Ok(None),
			Some(v) => v
				.as_str()
				.map(|s| Some(s.to_owned()))
				.ok_or_else(|| invalid(key, "文字列でない値があります")),
		}
	};
	let color_name = text_of("color")?.ok_or_else(|| invalid(key, "color がありません"))?;
	let color = NoticeColor::parse(&color_name)
		.ok_or_else(|| invalid(key, "color は red・yellow・green・blue のどれかです"))?;
	let title = text_of("title")?;
	let text = text_of("text")?;
	if title.is_none() && text.is_none() {
		return Err(invalid(key, "title と text のどちらかが要ります"));
	}
	Ok(NoticeTemplate {
		title,
		text,
		color,
		admin: text_of("admin")?,
	})
}

/// 項目の配列を読む。
///
/// # エラー
/// 配列でない、要素が `name` と `value` の文字列を持たない: `InvalidMessage`
fn parse_fields(key: &str, value: &toml::Value) -> Result<Vec<(String, String)>, ConfigError> {
	let items = value
		.as_array()
		.ok_or_else(|| invalid(key, "name と value を持つテーブルの配列ではありません"))?;
	items
		.iter()
		.map(|item| {
			let get = |name: &str| item.get(name).and_then(toml::Value::as_str);
			match (get("name"), get("value")) {
				(Some(name), Some(value)) => Ok((name.to_owned(), value.to_owned())),
				_ => Err(invalid(key, "項目に name と value がありません")),
			}
		})
		.collect()
}

/// `{name}` を引数の値で置換する。
///
/// - 1 回の走査で置換するので、値の中の `{x}` は再置換されない
/// - 引数に無い `{name}` はそのまま残す
fn substitute(template: &str, args: &[(&str, &str)]) -> String {
	let mut out = String::with_capacity(template.len());
	let mut rest = template;
	while let Some(start) = rest.find('{') {
		out.push_str(&rest[..start]);
		let after = &rest[start + 1..];
		let replaced = after.find('}').and_then(|end| {
			let name = &after[..end];
			args.iter()
				.find(|(k, _)| *k == name)
				.map(|(_, value)| (*value, end))
		});
		match replaced {
			Some((value, end)) => {
				out.push_str(value);
				rest = &after[end + 1..];
			}
			None => {
				out.push('{');
				rest = after;
			}
		}
	}
	out.push_str(rest);
	out
}
