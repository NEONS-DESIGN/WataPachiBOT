//! 再生状態（`playback_state`）の操作。
//! - 再生中の曲はキューから移してここに持つ。ギルドごとに 1 行
//! - 呼び出し: `writer.run(move |c| playback::upsert(c, &state)).await`

use rusqlite::{params, Connection, OptionalExtension, Row};

use super::queue::NewQueueItem;
use super::{id_from_sql, now_secs, DbError};
use crate::domain::{ChannelId, GuildId, VideoId};

/// 保存する再生状態。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaybackState {
	pub guild_id: GuildId,
	pub voice_channel_id: ChannelId,
	pub text_channel_id: ChannelId,
	pub item: NewQueueItem,
	pub position_ms: u64,
	pub paused: bool,
}

/// 再生状態を挿入または更新する。
///
/// # 引数
/// - `state`: 保存する状態。`updated_at` は現在時刻を入れる
///
/// # エラー
/// - SQLite のエラー
pub fn upsert(c: &Connection, state: &PlaybackState) -> Result<(), DbError> {
	c.prepare_cached(
		"INSERT INTO playback_state
			(guild_id, voice_channel_id, text_channel_id, video_id, title, duration_s,
			 requester_id, position_ms, paused, updated_at)
		 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
		 ON CONFLICT(guild_id) DO UPDATE SET
			voice_channel_id = excluded.voice_channel_id,
			text_channel_id  = excluded.text_channel_id,
			video_id         = excluded.video_id,
			title            = excluded.title,
			duration_s       = excluded.duration_s,
			requester_id     = excluded.requester_id,
			position_ms      = excluded.position_ms,
			paused           = excluded.paused,
			updated_at       = excluded.updated_at",
	)?
	.execute(params![
		state.guild_id.get() as i64,
		state.voice_channel_id.get() as i64,
		state.text_channel_id.get() as i64,
		state.item.video_id.0,
		state.item.title,
		state.item.duration_s,
		state.item.requester_id.get() as i64,
		state.position_ms as i64,
		state.paused,
		now_secs(),
	])?;
	Ok(())
}

/// すべてのギルドの再生状態を返す（起動時の復元用）。
///
/// # エラー
/// - SQLite のエラー、または不正な ID が入っているとき
pub fn load_all(c: &Connection) -> Result<Vec<PlaybackState>, DbError> {
	let mut stmt = c.prepare_cached(
		"SELECT guild_id, voice_channel_id, text_channel_id, video_id, title, duration_s,
				requester_id, position_ms, paused
		 FROM playback_state ORDER BY guild_id",
	)?;
	let rows = stmt
		.query_map([], state_from_row)?
		.collect::<Result<Vec<_>, _>>()?;
	Ok(rows)
}

/// 1 ギルドの再生状態を返す。
///
/// # 引数
/// - `guild`: 対象のギルド
///
/// # 戻り値
/// 再生状態。行が無ければ None
///
/// # エラー
/// - SQLite のエラー、または不正な ID が入っているとき
///
/// # 呼び出し例
/// ```ignore
/// let state = writer.run(move |c| playback::load(c, guild)).await?;
/// ```
pub fn load(c: &Connection, guild: GuildId) -> Result<Option<PlaybackState>, DbError> {
	let state = c
		.prepare_cached(
			"SELECT guild_id, voice_channel_id, text_channel_id, video_id, title, duration_s,
					requester_id, position_ms, paused
			 FROM playback_state WHERE guild_id = ?1",
		)?
		.query_row([guild.get() as i64], state_from_row)
		.optional()?;
	Ok(state)
}

/// `load_all` と `load` の SELECT の 1 行を再生状態にする。列の順は両方の SELECT で同じ。
///
/// # 引数
/// - `row`: SELECT の 1 行
///
/// # 戻り値
/// 再生状態
///
/// # エラー
/// - 列の型が違う、または不正な ID が入っているとき
///
/// # 呼び出し例
/// ```ignore
/// stmt.query_map([], state_from_row)?;
/// ```
fn state_from_row(row: &Row<'_>) -> rusqlite::Result<PlaybackState> {
	Ok(PlaybackState {
		guild_id: id_from_sql(row.get(0)?)?,
		voice_channel_id: id_from_sql(row.get(1)?)?,
		text_channel_id: id_from_sql(row.get(2)?)?,
		item: NewQueueItem {
			video_id: VideoId(row.get(3)?),
			title: row.get(4)?,
			duration_s: row.get(5)?,
			requester_id: id_from_sql(row.get(6)?)?,
		},
		position_ms: row.get::<_, i64>(7)?.max(0) as u64,
		paused: row.get(8)?,
	})
}

/// 再生状態のボイスチャンネルを書き換える (`?move` の後)。行が無ければ何もしない。
///
/// # 引数
/// - `guild`: 対象のギルド
/// - `channel`: 移動先のボイスチャンネル
///
/// # エラー
/// - SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// writer.run(move |c| playback::set_voice_channel(c, guild, channel)).await?;
/// ```
pub fn set_voice_channel(
	c: &Connection,
	guild: GuildId,
	channel: ChannelId,
) -> Result<(), DbError> {
	c.prepare_cached(
		"UPDATE playback_state SET voice_channel_id = ?2, updated_at = ?3 WHERE guild_id = ?1",
	)?
	.execute(params![
		guild.get() as i64,
		channel.get() as i64,
		now_secs()
	])?;
	Ok(())
}

/// そのギルドの再生状態を削除する。無ければ何もしない。
///
/// # エラー
/// - SQLite のエラー
pub fn delete(c: &Connection, guild: GuildId) -> Result<(), DbError> {
	c.prepare_cached("DELETE FROM playback_state WHERE guild_id = ?1")?
		.execute([guild.get() as i64])?;
	Ok(())
}
