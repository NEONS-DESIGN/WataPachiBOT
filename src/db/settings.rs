//! ギルドごとの設定と管理者の操作。
//! - 設定値の列は NULL を許し、NULL は `config.toml` の既定値に従う（`load` で解決する）
//! - 設定は 1 項目ずつ更新する。更新した項目以外は NULL のままにして、既定値の変更が効くようにする
//! - 呼び出し: `writer.run(move |c| settings::set_volume(c, guild, 0.8)).await`

use rusqlite::{params, Connection};

use super::DbError;
use crate::config::DefaultsConfig;
use crate::domain::{GuildId, UserId};
use crate::lang::Lang;

/// 既定値を解決したあとのギルド設定。
#[derive(Debug, Clone, PartialEq)]
pub struct GuildSettings {
	pub volume: f32,
	pub queue_limit: u32,
	pub playlist_limit: u32,
	pub alone_timeout_secs: u32,
	pub loop_enabled: bool,
}

impl GuildSettings {
	/// ループ中なら、終えた曲をキューへ戻すときの上限件数 (`queue_limit`)。ループしていなければ None。
	///
	/// # 呼び出し例
	/// `store::finish(c, guild, &item, played_at, settings.loop_limit())`
	pub fn loop_limit(&self) -> Option<u32> {
		self.loop_enabled.then_some(self.queue_limit)
	}
}

/// 設定を読む。行が無い、または列が NULL の項目は既定値を使う。
///
/// # 引数
/// - `guild`: 対象のギルド
/// - `defaults`: `[defaults]` の設定
///
/// # エラー
/// - SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// let s = settings::load(c, guild, &config.defaults)?;
/// ```
pub fn load(
	c: &Connection,
	guild: GuildId,
	defaults: &DefaultsConfig,
) -> Result<GuildSettings, DbError> {
	let row = c
		.prepare_cached(
			"SELECT volume, queue_limit, playlist_limit, alone_timeout, loop_enabled
			 FROM guild_settings WHERE guild_id = ?1",
		)?
		.query_row([guild.get() as i64], |r| {
			Ok((
				r.get::<_, Option<f64>>(0)?,
				r.get::<_, Option<u32>>(1)?,
				r.get::<_, Option<u32>>(2)?,
				r.get::<_, Option<u32>>(3)?,
				r.get::<_, bool>(4)?,
			))
		});
	let (volume, queue_limit, playlist_limit, alone_timeout, loop_enabled) = match row {
		Ok(v) => v,
		Err(rusqlite::Error::QueryReturnedNoRows) => (None, None, None, None, false),
		Err(e) => return Err(e.into()),
	};
	Ok(GuildSettings {
		volume: volume.map_or(defaults.volume, |v| v as f32),
		queue_limit: queue_limit.unwrap_or(defaults.queue_limit),
		playlist_limit: playlist_limit.unwrap_or(defaults.playlist_limit),
		alone_timeout_secs: alone_timeout.unwrap_or(defaults.alone_timeout_secs),
		loop_enabled,
	})
}

/// 1 列だけを挿入または更新する。
///
/// - `column` は呼び出し元の固定文字列のみ（SQL へ埋め込むため、外部入力を渡さない）
/// - 準備済みの文は SQL の文字列で引くため、列ごとに 1 つずつ使い回される
fn set_column(
	c: &Connection,
	guild: GuildId,
	column: &'static str,
	value: impl rusqlite::ToSql,
) -> Result<(), DbError> {
	c.prepare_cached(&format!(
		"INSERT INTO guild_settings (guild_id, {column}) VALUES (?1, ?2)
		 ON CONFLICT(guild_id) DO UPDATE SET {column} = excluded.{column}"
	))?
	.execute(params![guild.get() as i64, value])?;
	Ok(())
}

/// 音量を設定する。
///
/// # エラー
/// - SQLite のエラー
pub fn set_volume(c: &Connection, guild: GuildId, value: f32) -> Result<(), DbError> {
	set_column(c, guild, "volume", f64::from(value))
}

/// キューの上限を設定する。
///
/// # エラー
/// - SQLite のエラー
pub fn set_queue_limit(c: &Connection, guild: GuildId, value: u32) -> Result<(), DbError> {
	set_column(c, guild, "queue_limit", value)
}

/// プレイリストの上限を設定する。
///
/// # エラー
/// - SQLite のエラー
pub fn set_playlist_limit(c: &Connection, guild: GuildId, value: u32) -> Result<(), DbError> {
	set_column(c, guild, "playlist_limit", value)
}

/// 無人時の退出までの秒数を設定する。
///
/// # エラー
/// - SQLite のエラー
pub fn set_alone_timeout(c: &Connection, guild: GuildId, value: u32) -> Result<(), DbError> {
	set_column(c, guild, "alone_timeout", value)
}

/// ループの有効・無効を設定する。
///
/// # エラー
/// - SQLite のエラー
pub fn set_loop(c: &Connection, guild: GuildId, value: bool) -> Result<(), DbError> {
	set_column(c, guild, "loop_enabled", value)
}

/// 文面の言語を設定する。
///
/// # エラー
/// - SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// writer.run(move |c| settings::set_lang(c, guild, Lang::En)).await?;
/// ```
pub fn set_lang(c: &Connection, guild: GuildId, lang: Lang) -> Result<(), DbError> {
	set_column(c, guild, "lang", lang.code())
}

/// 言語を設定したサーバーの一覧を読む。起動時に `GuildLangs` を作るのに使う。
///
/// - 言語が NULL の行は含めない（既定の言語に従うため）
/// - 解釈できない値の行は warn のログを残して飛ばす（手で書き換えた DB でも起動できるようにする）
///
/// # 戻り値
/// `(サーバー, 言語)` の並び。サーバーの ID の昇順
///
/// # エラー
/// - SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// let entries = reader.read(settings::load_langs).await?;
/// ```
pub fn load_langs(c: &Connection) -> Result<Vec<(GuildId, Lang)>, DbError> {
	let mut statement = c.prepare_cached(
		"SELECT guild_id, lang FROM guild_settings WHERE lang IS NOT NULL ORDER BY guild_id",
	)?;
	let rows = statement.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?;
	let mut entries = Vec::new();
	for row in rows {
		let (raw_guild, code) = row?;
		let guild = u64::try_from(raw_guild).ok().and_then(GuildId::new_checked);
		match (guild, Lang::parse(&code)) {
			(Some(guild), Some(lang)) => entries.push((guild, lang)),
			_ => tracing::warn!(
				guild = raw_guild,
				lang = code.as_str(),
				"解釈できない言語の設定を飛ばしました"
			),
		}
	}
	Ok(entries)
}

/// 管理者を追加する。
///
/// # 戻り値
/// 新しく追加したなら true、すでに管理者なら false。
///
/// # エラー
/// - SQLite のエラー
pub fn add_admin(c: &Connection, guild: GuildId, user: UserId) -> Result<bool, DbError> {
	let changed = c
		.prepare_cached("INSERT OR IGNORE INTO bot_admins (guild_id, user_id) VALUES (?1, ?2)")?
		.execute(params![guild.get() as i64, user.get() as i64])?;
	Ok(changed > 0)
}

/// 管理者を外す。
///
/// # 戻り値
/// 外したなら true、管理者ではなかったなら false。
///
/// # エラー
/// - SQLite のエラー
pub fn remove_admin(c: &Connection, guild: GuildId, user: UserId) -> Result<bool, DbError> {
	let changed = c
		.prepare_cached("DELETE FROM bot_admins WHERE guild_id = ?1 AND user_id = ?2")?
		.execute(params![guild.get() as i64, user.get() as i64])?;
	Ok(changed > 0)
}

/// 管理者かどうか。
///
/// # エラー
/// - SQLite のエラー
pub fn is_admin(c: &Connection, guild: GuildId, user: UserId) -> Result<bool, DbError> {
	let found: bool = c
		.prepare_cached(
			"SELECT EXISTS (SELECT 1 FROM bot_admins WHERE guild_id = ?1 AND user_id = ?2)",
		)?
		.query_row(params![guild.get() as i64, user.get() as i64], |r| r.get(0))?;
	Ok(found)
}
