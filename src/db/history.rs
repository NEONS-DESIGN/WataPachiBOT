//! 再生履歴の操作。
//! - 保持期間を過ぎた行は `cache::purge_expired` (または `cache::purge_expired_in_chunks`) が消す
//! - 呼び出し: `writer.run(move |c| history::record(c, guild, &item, now)).await`

use rusqlite::{params, Connection};

use super::queue::NewQueueItem;
use super::{id_from_sql, DbError};
use crate::domain::{GuildId, VideoId};

/// 履歴の 1 件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryEntry {
	pub item: NewQueueItem,
	/// 再生を始めた UNIX 秒。
	pub played_at: i64,
}

/// 履歴に 1 件記録する。
///
/// # 引数
/// - `guild`: 対象のギルド
/// - `item`: 再生した曲
/// - `played_at`: 再生を始めた UNIX 秒
///
/// # エラー
/// - SQLite のエラー
pub fn record(
	c: &Connection,
	guild: GuildId,
	item: &NewQueueItem,
	played_at: i64,
) -> Result<(), DbError> {
	c.prepare_cached(
		"INSERT INTO history (guild_id, video_id, title, duration_s, requester_id, played_at)
		 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
	)?
	.execute(params![
		guild.get() as i64,
		item.video_id.0,
		item.title,
		item.duration_s,
		item.requester_id.get() as i64,
		played_at,
	])?;
	Ok(())
}

/// 新しい順に履歴を返す。
///
/// # 引数
/// - `offset`: 0 始まりの開始位置
/// - `limit`: 最大件数
///
/// # エラー
/// - SQLite のエラー
pub fn list(
	c: &Connection,
	guild: GuildId,
	offset: u32,
	limit: u32,
) -> Result<Vec<HistoryEntry>, DbError> {
	let mut stmt = c.prepare_cached(
		"SELECT video_id, title, duration_s, requester_id, played_at FROM history
		 WHERE guild_id = ?1 ORDER BY played_at DESC, id DESC LIMIT ?2 OFFSET ?3",
	)?;
	let rows = stmt
		.query_map(params![guild.get() as i64, limit, offset], |r| {
			Ok(HistoryEntry {
				item: NewQueueItem {
					video_id: VideoId(r.get(0)?),
					title: r.get(1)?,
					duration_s: r.get(2)?,
					requester_id: id_from_sql(r.get(3)?)?,
				},
				played_at: r.get(4)?,
			})
		})?
		.collect::<Result<Vec<_>, _>>()?;
	Ok(rows)
}

/// ギルドの履歴の件数を返す。
///
/// # エラー
/// - SQLite のエラー
pub fn count(c: &Connection, guild: GuildId) -> Result<u32, DbError> {
	Ok(
		c.prepare_cached("SELECT count(*) FROM history WHERE guild_id = ?1")?
			.query_row([guild.get() as i64], |r| r.get(0))?,
	)
}
