//! 文面の言語。
//! - 言語はサーバーごとに 1 つ。設定が無いサーバーは `config.toml` の `[defaults] lang` に従う
//! - 文面のファイルは `messages/{code}.toml`（`Lang::code` の名前）
//! - サーバーごとの言語は `GuildLangs` にメモリで持つ

use std::collections::HashMap;
use std::sync::{PoisonError, RwLock};

use serde::Deserialize;

use crate::domain::GuildId;

/// 文面の言語。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Lang {
	Ja,
	En,
}

impl Lang {
	/// 対応しているすべての言語。`Lang::parse` が受け付ける値の一覧でもある。
	/// - 言語を足すときは、ここ、`MessageCatalog`、`messages/{code}.toml` の 3 つに足す
	pub const ALL: [Lang; 2] = [Lang::Ja, Lang::En];

	/// 言語のコード。文面のファイル名と DB の値に使う。
	///
	/// # 戻り値
	/// `"ja"` または `"en"`
	///
	/// # 呼び出し例
	/// ```
	/// # use watapachi_bot::lang::Lang;
	/// assert_eq!(Lang::En.code(), "en");
	/// ```
	pub fn code(self) -> &'static str {
		match self {
			Lang::Ja => "ja",
			Lang::En => "en",
		}
	}

	/// コードから言語を読む。大文字小文字を区別しない。
	///
	/// # 引数
	/// - `text`: `?setting lang` の値、または DB の値
	///
	/// # 戻り値
	/// 対応していないコードなら None
	///
	/// # 呼び出し例
	/// ```
	/// # use watapachi_bot::lang::Lang;
	/// assert_eq!(Lang::parse("EN"), Some(Lang::En));
	/// ```
	pub fn parse(text: &str) -> Option<Lang> {
		Lang::ALL
			.into_iter()
			.find(|lang| lang.code().eq_ignore_ascii_case(text))
	}
}

/// サーバーごとの言語の表。通知を送るたびに引くため、DB ではなくメモリに持つ。
///
/// - 起動時に DB の設定から作り、`?setting lang` の保存に成功したら `set` で更新する
/// - ロックは表の読み書きの間だけ持つ。毒されていても中身を使う（値は単純な対応のため）
///
/// 呼び出し方:
/// ```
/// # use watapachi_bot::lang::{GuildLangs, Lang};
/// # use twilight_model::id::Id;
/// let langs = GuildLangs::new(Lang::Ja, vec![(Id::new(1), Lang::En)]);
/// assert_eq!(langs.get(Some(Id::new(1))), Lang::En);
/// assert_eq!(langs.get(None), Lang::Ja);
/// ```
#[derive(Debug)]
pub struct GuildLangs {
	default: Lang,
	table: RwLock<HashMap<GuildId, Lang>>,
}

impl GuildLangs {
	/// 表を作る。
	///
	/// # 引数
	/// - `default`: 設定が無いサーバーと、サーバーの分からない送り先の言語（`[defaults] lang`）
	/// - `entries`: 言語を設定したサーバー（`settings::load_langs` の結果）
	///
	/// # 呼び出し例
	/// ```ignore
	/// let langs = GuildLangs::new(config.defaults.lang, entries);
	/// ```
	pub fn new(default: Lang, entries: Vec<(GuildId, Lang)>) -> GuildLangs {
		GuildLangs {
			default,
			table: RwLock::new(entries.into_iter().collect()),
		}
	}

	/// サーバーの言語を返す。サーバーが None か、設定が無ければ既定の言語。
	///
	/// # 引数
	/// - `guild`: 送り先のサーバー。DM などサーバーが無ければ None
	///
	/// # 戻り値
	/// そのサーバーの言語
	///
	/// # 呼び出し例
	/// ```ignore
	/// let lang = langs.get(message.guild_id);
	/// ```
	pub fn get(&self, guild: Option<GuildId>) -> Lang {
		let Some(guild) = guild else {
			return self.default;
		};
		let table = self.table.read().unwrap_or_else(PoisonError::into_inner);
		table.get(&guild).copied().unwrap_or(self.default)
	}

	/// サーバーの言語を変える。DB への保存に成功した後に呼ぶ。
	///
	/// # 引数
	/// - `guild`: 対象のサーバー
	/// - `lang`: 新しい言語
	///
	/// # 呼び出し例
	/// ```ignore
	/// langs.set(guild, Lang::En);
	/// ```
	pub fn set(&self, guild: GuildId, lang: Lang) {
		let mut table = self.table.write().unwrap_or_else(PoisonError::into_inner);
		table.insert(guild, lang);
	}
}
