//! キューの操作。
//! - すべて `&Connection` 上の関数。複数文の操作は自前でトランザクションを開かない
//!   （DbWriter::run がすでに SAVEPOINT で囲んでいるため。単体で呼ぶ場合は呼び出し側で囲む）
//! - `position` は並びだけを表す。連番でなくてよく、先頭への追加で負になる
//! - 呼び出し: `writer.run(move |c| queue::append(c, guild, &items, limit)).await`

use rand::seq::SliceRandom;
use rand::Rng;
use rusqlite::{params, Connection, OptionalExtension, Row};

use super::{id_from_sql, now_secs, DbError};
use crate::domain::{GuildId, UserId, VideoId};

/// キューへ入れる曲。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewQueueItem {
	pub video_id: VideoId,
	pub title: String,
	pub duration_s: Option<u32>,
	pub requester_id: UserId,
}

/// キューに入っている曲。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueItem {
	pub id: i64,
	pub position: i64,
	pub item: NewQueueItem,
	pub added_at: i64,
}

/// `append` の結果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
	/// 実際に追加した件数。
	pub added: u32,
	/// 上限のため追加しなかった件数。
	pub rejected: u32,
	/// 追加する前のキューの件数。
	pub before: u32,
}

/// 取得系の SELECT の列。`row_to_item` と順序を合わせる。
const SELECT_COLUMNS: &str = "id, position, video_id, title, duration_s, requester_id, added_at";

/// SELECT_COLUMNS の行を `QueueItem` にする。
fn row_to_item(row: &Row<'_>) -> rusqlite::Result<QueueItem> {
	Ok(QueueItem {
		id: row.get(0)?,
		position: row.get(1)?,
		item: NewQueueItem {
			video_id: VideoId(row.get(2)?),
			title: row.get(3)?,
			duration_s: row.get(4)?,
			requester_id: id_from_sql(row.get(5)?)?,
		},
		added_at: row.get(6)?,
	})
}

/// 1 行を挿入する。
fn insert(
	c: &Connection,
	guild: GuildId,
	position: i64,
	item: &NewQueueItem,
	now: i64,
) -> Result<(), DbError> {
	c.prepare_cached(
		"INSERT INTO queue_items (guild_id, position, video_id, title, duration_s, requester_id, added_at)
		 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
	)?
	.execute(params![
		guild.get() as i64,
		position,
		item.video_id.0,
		item.title,
		item.duration_s,
		item.requester_id.get() as i64,
		now,
	])?;
	Ok(())
}

/// 末尾に追加する。上限を超える分は追加しない。
///
/// # 引数
/// - `guild`: 対象のギルド
/// - `items`: 追加する曲（先頭から順に入れる）
/// - `limit`: キューの上限件数
///
/// # 戻り値
/// 追加した件数、上限のため断った件数、追加する前の件数。
///
/// # エラー
/// - SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// let r = queue::append(c, guild, &items, 1000)?;
/// ```
pub fn append(
	c: &Connection,
	guild: GuildId,
	items: &[NewQueueItem],
	limit: u32,
) -> Result<AppendResult, DbError> {
	let current = count(c, guild)?;
	let room = limit.saturating_sub(current) as usize;
	let take = items.len().min(room);
	let first: i64 = c
		.prepare_cached(
			"SELECT COALESCE(MAX(position), 0) + 1 FROM queue_items WHERE guild_id = ?1",
		)?
		.query_row([guild.get() as i64], |r| r.get(0))?;
	let now = now_secs();
	for (offset, item) in items[..take].iter().enumerate() {
		insert(c, guild, first + offset as i64, item, now)?;
	}
	Ok(AppendResult {
		added: take as u32,
		rejected: (items.len() - take) as u32,
		before: current,
	})
}

/// 先頭に入れる（再生中の曲を戻す用途）。上限は確認しない。
///
/// # 引数
/// - `guild`: 対象のギルド
/// - `item`: 戻す曲
///
/// # エラー
/// - SQLite のエラー
pub fn push_front(c: &Connection, guild: GuildId, item: &NewQueueItem) -> Result<(), DbError> {
	let position: i64 = c
		.prepare_cached(
			"SELECT COALESCE(MIN(position), 1) - 1 FROM queue_items WHERE guild_id = ?1",
		)?
		.query_row([guild.get() as i64], |r| r.get(0))?;
	insert(c, guild, position, item, now_secs())
}

/// 先頭の曲を取り出して削除する。
///
/// # 戻り値
/// 取り出した曲。空なら None。
///
/// # エラー
/// - SQLite のエラー
pub fn pop_front(c: &Connection, guild: GuildId) -> Result<Option<QueueItem>, DbError> {
	take_at(c, guild, 1)
}

/// 指定位置（1 始まり）の曲を取り出して削除する。他の曲の順序は変わらない。
///
/// # 引数
/// - `index`: 1 始まりの位置。0 や範囲外なら None
///
/// # エラー
/// - SQLite のエラー
pub fn take_at(c: &Connection, guild: GuildId, index: u32) -> Result<Option<QueueItem>, DbError> {
	if index == 0 {
		return Ok(None);
	}
	let found = c
		.prepare_cached(&format!(
			"SELECT {SELECT_COLUMNS} FROM queue_items WHERE guild_id = ?1
			 ORDER BY position LIMIT 1 OFFSET ?2"
		))?
		.query_row(params![guild.get() as i64, index - 1], row_to_item)
		.optional()?;
	if let Some(found) = &found {
		c.prepare_cached("DELETE FROM queue_items WHERE id = ?1")?
			.execute([found.id])?;
	}
	Ok(found)
}

/// 先頭から `count` 件を削除せずに返す。
///
/// # エラー
/// - SQLite のエラー
pub fn peek(c: &Connection, guild: GuildId, count: u32) -> Result<Vec<QueueItem>, DbError> {
	list(c, guild, 0, count)
}

/// 一覧を返す。
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
) -> Result<Vec<QueueItem>, DbError> {
	let mut stmt = c.prepare_cached(&format!(
		"SELECT {SELECT_COLUMNS} FROM queue_items WHERE guild_id = ?1
		 ORDER BY position LIMIT ?2 OFFSET ?3"
	))?;
	let rows = stmt
		.query_map(params![guild.get() as i64, limit, offset], row_to_item)?
		.collect::<Result<Vec<_>, _>>()?;
	Ok(rows)
}

/// キューの件数。
///
/// # エラー
/// - SQLite のエラー
pub fn count(c: &Connection, guild: GuildId) -> Result<u32, DbError> {
	Ok(
		c.prepare_cached("SELECT count(*) FROM queue_items WHERE guild_id = ?1")?
			.query_row([guild.get() as i64], |r| r.get(0))?,
	)
}

/// 1 始まりの範囲 `start..=end` の曲を削除する。
///
/// # 戻り値
/// 削除した件数。`start` が 0、または `end < start` なら 0。
///
/// # エラー
/// - SQLite のエラー
pub fn remove_range(c: &Connection, guild: GuildId, start: u32, end: u32) -> Result<u32, DbError> {
	if start == 0 || end < start {
		return Ok(0);
	}
	// end - start + 1 のオーバーフローを避けるため i64 で計算する
	let len = i64::from(end) - i64::from(start) + 1;
	let removed = c
		.prepare_cached(
			"DELETE FROM queue_items WHERE id IN (
				SELECT id FROM queue_items WHERE guild_id = ?1
				ORDER BY position LIMIT ?2 OFFSET ?3)",
		)?
		.execute(params![guild.get() as i64, len, start - 1])?;
	Ok(removed as u32)
}

/// キューを空にする。
///
/// # 戻り値
/// 削除した件数。
///
/// # エラー
/// - SQLite のエラー
pub fn clear(c: &Connection, guild: GuildId) -> Result<u32, DbError> {
	let removed = c
		.prepare_cached("DELETE FROM queue_items WHERE guild_id = ?1")?
		.execute([guild.get() as i64])?;
	Ok(removed as u32)
}

/// 並びをシャッフルする。
///
/// # 引数
/// - `rng`: 乱数源（テストでは種を固定する）
///
/// # エラー
/// - SQLite のエラー
///
/// # 注意
/// - 混ぜた順に、既存の最大値の次から `max + 1`〜`max + n` を振り直す (n 回の UPDATE)。
///   どの値も既存の位置より大きく互いに異なるため、`UNIQUE (guild_id, position)` に当たらない
/// - 位置は連番でなくてよいため詰め直さない。シャッフルのたびに位置の値は n ずつ大きくなる (i64 のため溢れない)
///
/// # 呼び出し例
/// ```ignore
/// queue::shuffle(c, guild, &mut rand::rng())?;
/// ```
pub fn shuffle(c: &Connection, guild: GuildId, rng: &mut impl Rng) -> Result<(), DbError> {
	let g = guild.get() as i64;
	let rows: Vec<(i64, i64)> = c
		.prepare_cached(
			"SELECT id, position FROM queue_items WHERE guild_id = ?1 ORDER BY position",
		)?
		.query_map([g], |r| Ok((r.get(0)?, r.get(1)?)))?
		.collect::<Result<_, _>>()?;
	if rows.len() < 2 {
		return Ok(());
	}
	// 位置の順に並べたので、末尾の位置が最大値
	let max = rows.last().map_or(0, |&(_, position)| position);
	let mut ids: Vec<i64> = rows.into_iter().map(|(id, _)| id).collect();
	ids.shuffle(rng);
	let mut stmt = c.prepare_cached("UPDATE queue_items SET position = ?1 WHERE id = ?2")?;
	for (rank, id) in ids.iter().enumerate() {
		stmt.execute(params![max + 1 + rank as i64, id])?;
	}
	Ok(())
}
