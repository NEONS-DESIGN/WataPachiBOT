//! テキストコマンドを `ParsedCommand` に変換する純粋な解析器。入出力は持たない。

use crate::config::LimitsConfig;
use crate::domain::UserId;
use crate::guild::message::{ClearRange, GuildCommand};
use crate::guild::query::{self, Query};
use crate::lang::Lang;

/// `?p` の使い方を示す messages のキー。
pub const USAGE_PLAY: &str = "help.usage_p";
/// `?pnow` の使い方を示す messages のキー。
pub const USAGE_PNOW: &str = "help.usage_pnow";
/// `?clear` の使い方を示す messages のキー。
pub const USAGE_CLEAR: &str = "help.usage_clear";
/// `?vol` の使い方を示す messages のキー。
pub const USAGE_VOL: &str = "help.usage_vol";
/// `?setting` の使い方を示す messages のキー。
pub const USAGE_SETTING: &str = "help.usage_setting";

/// ユーザーのメンションの前置き。`<@123>` と `<@!123>` を受け付ける。
const MENTION_OPEN: &str = "<@";
/// ニックネーム形式のメンションの印。
const MENTION_NICK_MARK: &str = "!";
/// メンションの閉じ。
const MENTION_CLOSE: &str = ">";

/// 解析済みのコマンド。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedCommand {
	Help,
	/// ギルドのアクターへ渡すコマンド。
	Guild(GuildCommand),
	QueueList,
	History,
	Setting(SettingCommand),
}

/// `?setting` の操作。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingCommand {
	AdminAdd(UserId),
	AdminRemove(UserId),
	QueueLimit(u32),
	PlaylistLimit(u32),
	AutoLeave(u32),
	/// 文面の言語
	Lang(Lang),
}

/// 解析の失敗。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
	/// 引数が足りない、または形式が違う。usage は messages の help.usage_* のキー。
	Usage { usage: &'static str },
	/// 数値が範囲外。
	OutOfRange { min: u32, max: u32 },
}

/// メッセージ本文を解析する。
///
/// - prefix で始まり、既知のコマンド名のときだけ Some を返す
/// - 未知の名前、prefix だけ、prefix の直後が空白のときは None
/// - コマンド名は大文字小文字を区別しない。引数は空白で区切る
/// - 引数を取らないコマンドは、後ろの余分な文字列を無視する
///
/// # 引数
/// - `prefix`: コマンドの接頭辞
/// - `content`: メッセージ本文
/// - `limits`: 数値引数の上限
///
/// # 戻り値
/// コマンドでなければ None。コマンドなら解析の結果
///
/// # エラー
/// - 既知のコマンドで引数の形式が違えば `Some(Err(ParseError::Usage))`、数値が範囲外なら `Some(Err(ParseError::OutOfRange))`
///
/// # 呼び出し例
/// ```ignore
/// if let Some(result) = parse("?", "?skip", &limits) { /* ... */ }
/// ```
pub fn parse(
	prefix: &str,
	content: &str,
	limits: &LimitsConfig,
) -> Option<Result<ParsedCommand, ParseError>> {
	if prefix.is_empty() {
		return None;
	}
	let rest = content.strip_prefix(prefix)?;
	if rest.chars().next().is_none_or(char::is_whitespace) {
		return None;
	}
	let (name, args) = match rest.split_once(char::is_whitespace) {
		Some((name, args)) => (name, args.trim()),
		None => (rest, ""),
	};
	// コマンド名は ASCII だけのため、ASCII の小文字化で足りる (Unicode の小文字化で別の名前に化けない)
	let name = name.to_ascii_lowercase();
	let parsed = match name.as_str() {
		"help" => Ok(ParsedCommand::Help),
		"p" => parse_play(args),
		"pause" => Ok(ParsedCommand::Guild(GuildCommand::Pause)),
		"resume" => Ok(ParsedCommand::Guild(GuildCommand::Resume)),
		"replay" => Ok(ParsedCommand::Guild(GuildCommand::Replay)),
		"skip" => Ok(ParsedCommand::Guild(GuildCommand::Skip)),
		"qlist" => Ok(ParsedCommand::QueueList),
		"pnow" => parse_pnow(args),
		"clear" => parse_clear(args),
		"loop" => Ok(ParsedCommand::Guild(GuildCommand::ToggleLoop)),
		"sh" => Ok(ParsedCommand::Guild(GuildCommand::Shuffle)),
		"vol" => parse_vol(args),
		"history" => Ok(ParsedCommand::History),
		"move" => Ok(ParsedCommand::Guild(GuildCommand::Move)),
		"rejoin" => Ok(ParsedCommand::Guild(GuildCommand::Rejoin)),
		"leave" => Ok(ParsedCommand::Guild(GuildCommand::Leave)),
		"setting" => parse_setting(args, limits),
		_ => return None,
	};
	Some(parsed)
}

/// 使い方の誤りを作る。
///
/// # 引数
/// - `key`: 使い方を示す messages のキー (`USAGE_*`)
///
/// # 戻り値
/// 常に `Err`
///
/// # エラー
/// - 常に `ParseError::Usage` (`key` を持つ)
///
/// # 呼び出し例
/// `return usage(USAGE_CLEAR);`
fn usage<T>(key: &'static str) -> Result<T, ParseError> {
	Err(ParseError::Usage { usage: key })
}

/// 非負の整数を読む。符号 (`+` を含む) や数字以外を含めば None。
///
/// # 引数
/// - `token`: 読む文字列
///
/// # 戻り値
/// 読めた値。空、数字以外を含む、`u32` を超えるなら None
///
/// # 呼び出し例
/// `let Some(value) = parse_u32(token) else { return usage(USAGE_VOL); };`
fn parse_u32(token: &str) -> Option<u32> {
	// `str::parse::<u32>` は先頭の `+` を受け付けるため、先に数字だけかを確かめる
	if token.is_empty() || !token.bytes().all(|b| b.is_ascii_digit()) {
		return None;
	}
	token.parse::<u32>().ok()
}

/// `?p <検索語または URL>`。残り全体を元の文字列のまま query にする。
///
/// - 囲みと空白を除いて何も残らない引数 (`<>`、`< >`) も、引数なしと同じく使い方の誤りにする
///
/// # 引数
/// - `args`: コマンド名の後ろの文字列 (前後の空白は除いてある)
///
/// # 戻り値
/// `GuildCommand::Play`
///
/// # エラー
/// - 引数が無い、または中身が空なら `help.usage_p` の使い方の誤り
///
/// # 呼び出し例
/// `"p" => parse_play(args),`
fn parse_play(args: &str) -> Result<ParsedCommand, ParseError> {
	if args.is_empty() || query::classify(args) == Query::Empty {
		return usage(USAGE_PLAY);
	}
	Ok(ParsedCommand::Guild(GuildCommand::Play {
		query: args.to_string(),
	}))
}

/// `?pnow <番号>`。1 以上の整数。2 つ目以降の引数は無視する。
///
/// # 引数
/// - `args`: コマンド名の後ろの文字列
///
/// # 戻り値
/// `GuildCommand::PlayNow`
///
/// # エラー
/// - 番号が無い、整数でない、0 なら `help.usage_pnow` の使い方の誤り
///
/// # 呼び出し例
/// `"pnow" => parse_pnow(args),`
fn parse_pnow(args: &str) -> Result<ParsedCommand, ParseError> {
	match args.split_whitespace().next().and_then(parse_u32) {
		Some(index) if index >= 1 => Ok(ParsedCommand::Guild(GuildCommand::PlayNow { index })),
		_ => usage(USAGE_PNOW),
	}
}

/// `?clear [開始] [終了]`。引数なしで全件、1 つで先頭から、2 つで範囲。
///
/// # 引数
/// - `args`: コマンド名の後ろの文字列
///
/// # 戻り値
/// `GuildCommand::Clear`
///
/// # エラー
/// - 整数でない、0、開始が終了より大きい、引数が 3 つ以上なら `help.usage_clear` の使い方の誤り
///
/// # 呼び出し例
/// `"clear" => parse_clear(args),`
fn parse_clear(args: &str) -> Result<ParsedCommand, ParseError> {
	let tokens: Vec<&str> = args.split_whitespace().collect();
	let range = match tokens.as_slice() {
		[] => ClearRange::All,
		[n] => match parse_u32(n) {
			Some(n) if n >= 1 => ClearRange::First(n),
			_ => return usage(USAGE_CLEAR),
		},
		[start, end] => match (parse_u32(start), parse_u32(end)) {
			(Some(start), Some(end)) if start >= 1 && start <= end => {
				ClearRange::Range { start, end }
			}
			_ => return usage(USAGE_CLEAR),
		},
		_ => return usage(USAGE_CLEAR),
	};
	Ok(ParsedCommand::Guild(GuildCommand::Clear { range }))
}

/// `?vol <百分率>`。u16 に収まれば通し、範囲の判定はアクターに任せる。
///
/// # 引数
/// - `args`: コマンド名の後ろの文字列
///
/// # 戻り値
/// `GuildCommand::SetVolume`
///
/// # エラー
/// - 値が無い、整数でない、u16 に収まらなければ `help.usage_vol` の使い方の誤り
///
/// # 呼び出し例
/// `"vol" => parse_vol(args),`
fn parse_vol(args: &str) -> Result<ParsedCommand, ParseError> {
	match args
		.split_whitespace()
		.next()
		.and_then(parse_u32)
		.and_then(|value| u16::try_from(value).ok())
	{
		Some(percent) => Ok(ParsedCommand::Guild(GuildCommand::SetVolume { percent })),
		None => usage(USAGE_VOL),
	}
}

/// 値が min..=max に収まるか確かめる。
///
/// # 引数
/// - `value`: 確かめる値
/// - `min`: 下限 (含む)
/// - `max`: 上限 (含む)
///
/// # 戻り値
/// 収まれば `value` をそのまま返す
///
/// # エラー
/// - 範囲外なら `ParseError::OutOfRange` (`min` と `max` を持つ)
///
/// # 呼び出し例
/// `SettingCommand::QueueLimit(check_range(value, 1, limits.queue_limit_max)?)`
fn check_range(value: u32, min: u32, max: u32) -> Result<u32, ParseError> {
	if (min..=max).contains(&value) {
		Ok(value)
	} else {
		Err(ParseError::OutOfRange { min, max })
	}
}

/// ユーザーの指定 (`<@id>`、`<@!id>`、`id`) を読む。ロールのメンションや 0 は None。
///
/// # 引数
/// - `token`: 読む文字列
///
/// # 戻り値
/// 読めたユーザー ID。形式が違う、数字以外を含む、`u64` を超える、0 なら None
///
/// # 呼び出し例
/// `let Some(user) = parse_user(target) else { return usage(USAGE_SETTING); };`
fn parse_user(token: &str) -> Option<UserId> {
	let digits = match token.strip_prefix(MENTION_OPEN) {
		Some(inner) => {
			let inner = inner.strip_suffix(MENTION_CLOSE)?;
			inner.strip_prefix(MENTION_NICK_MARK).unwrap_or(inner)
		}
		None => token,
	};
	if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
		return None;
	}
	UserId::new_checked(digits.parse::<u64>().ok()?)
}

/// `?setting` の各サブコマンド。形式の誤りは `help.usage_setting`。
///
/// - `admin add|remove <ユーザー>`、`limit queue|playlist <値>`、`autoleave <秒>`、`lang <言語>` を受け付ける
/// - サブコマンドと値は大文字小文字を区別しない
///
/// # 引数
/// - `args`: コマンド名の後ろの文字列
/// - `limits`: 数値引数の上限
///
/// # 戻り値
/// `ParsedCommand::Setting`
///
/// # エラー
/// - 形式が違えば `help.usage_setting` の使い方の誤り
/// - 上限は 1 以上 (`autoleave` は 0 以上) で上限の設定まで。外れたら `ParseError::OutOfRange`
///
/// # 呼び出し例
/// `"setting" => parse_setting(args, limits),`
fn parse_setting(args: &str, limits: &LimitsConfig) -> Result<ParsedCommand, ParseError> {
	// サブコマンドはコマンド名と同じく大文字と小文字を区別しない (メンションと数字は小文字化で変わらない)
	let lowered: Vec<String> = args
		.split_whitespace()
		.map(str::to_ascii_lowercase)
		.collect();
	let tokens: Vec<&str> = lowered.iter().map(String::as_str).collect();
	let command = match tokens.as_slice() {
		["admin", op @ ("add" | "remove"), target] => {
			let Some(user) = parse_user(target) else {
				return usage(USAGE_SETTING);
			};
			if *op == "add" {
				SettingCommand::AdminAdd(user)
			} else {
				SettingCommand::AdminRemove(user)
			}
		}
		["limit", kind @ ("queue" | "playlist"), value] => {
			let Some(value) = parse_u32(value) else {
				return usage(USAGE_SETTING);
			};
			if *kind == "queue" {
				SettingCommand::QueueLimit(check_range(value, 1, limits.queue_limit_max)?)
			} else {
				SettingCommand::PlaylistLimit(check_range(value, 1, limits.playlist_limit_max)?)
			}
		}
		["autoleave", value] => {
			let Some(value) = parse_u32(value) else {
				return usage(USAGE_SETTING);
			};
			SettingCommand::AutoLeave(check_range(value, 0, limits.alone_timeout_max_secs)?)
		}
		["lang", value] => {
			let Some(lang) = Lang::parse(value) else {
				return usage(USAGE_SETTING);
			};
			SettingCommand::Lang(lang)
		}
		_ => return usage(USAGE_SETTING),
	};
	Ok(ParsedCommand::Setting(command))
}
