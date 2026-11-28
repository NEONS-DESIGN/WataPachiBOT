//! `.env` からのトークンの読み取り。
//! - 1 行 1 組の `KEY=VALUE` を読む
//! - トークンはエラーの文言に出さない
//!
//! 呼び出し方:
//! ```ignore
//! let token = read_token(Path::new(".env"))?;
//! ```

use std::path::Path;

use anyhow::{bail, Context as _};

/// `.env` でトークンを置くキー。
const TOKEN_KEY: &str = "DISCORD_TOKEN";
/// `.env` のコメント行の先頭。
const ENV_COMMENT: char = '#';
/// UTF-8 の BOM。メモ帳で保存した `.env` の先頭に付くことがある。
const BOM: char = '\u{feff}';

/// `.env` の本文から 1 つのキーの値を取り出す (純粋)。
///
/// - 1 行 1 組の `KEY=VALUE`。`#` で始まる行と空行は飛ばす。先頭の BOM と行末の CR は無視する
/// - キーと値の前後の空白を除き、値が同じ引用符 (`"` か `'`) で囲まれていれば外す
/// - 同じキーが複数あれば後の行を使う
///
/// # 引数
/// - `text`: `.env` の本文
/// - `key`: 探すキー
///
/// # 戻り値
/// 値。キーが無ければ None (空の値は `Some("")`)
///
/// # 例
/// `assert_eq!(parse_env_value("A=1\n", "A").as_deref(), Some("1"));`
pub fn parse_env_value(text: &str, key: &str) -> Option<String> {
	let text = text.strip_prefix(BOM).unwrap_or(text);
	let mut found = None;
	for line in text.lines() {
		let line = line.trim();
		if line.is_empty() || line.starts_with(ENV_COMMENT) {
			continue;
		}
		let Some((name, value)) = line.split_once('=') else {
			continue;
		};
		if name.trim() != key {
			continue;
		}
		found = Some(unquote(value.trim()).to_owned());
	}
	found
}

/// 値を囲む同じ引用符を 1 組外す。
///
/// # 引数
/// - `value`: 前後の空白を除いた値
///
/// # 戻り値
/// 引用符を外した値。囲まれていなければそのまま
///
/// # 例
/// `assert_eq!(unquote("\"a\""), "a");`
fn unquote(value: &str) -> &str {
	for quote in ['"', '\''] {
		if let Some(inner) = value
			.strip_prefix(quote)
			.and_then(|rest| rest.strip_suffix(quote))
		{
			return inner;
		}
	}
	value
}

/// `.env` から `DISCORD_TOKEN` を読む。
///
/// - エラーの文言にトークンや他の値を含めない
///
/// # 引数
/// - `path`: `.env` のパス
///
/// # 戻り値
/// 空でないトークン
///
/// # エラー
/// ファイルを読めない、`DISCORD_TOKEN` が無いか空
///
/// # 例
/// `let token = read_token(Path::new(".env"))?;`
pub fn read_token(path: &Path) -> anyhow::Result<String> {
	let text = std::fs::read_to_string(path).with_context(|| {
		format!(
			"{} を読めません。{TOKEN_KEY} を書いた .env を置いてください (.env.example を写して作る)",
			path.display()
		)
	})?;
	match parse_env_value(&text, TOKEN_KEY) {
		Some(token) if !token.is_empty() => Ok(token),
		_ => bail!("{} に {TOKEN_KEY} がありません", path.display()),
	}
}
