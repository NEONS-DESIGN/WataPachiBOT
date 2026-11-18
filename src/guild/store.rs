//! アクターの DB の合成処理。
//! - 複数の表にまたがる書き込みを 1 つの関数にまとめ、`DbWriter::run` の 1 回 (1 トランザクション) で実行する
//! - 失敗したら 1 回だけやり直す書き込み (`write_with_retry`) と、キューへ入れる曲の組み立てもここに置く
//! - 呼び出し: `self.write(move |c| store::pop_next(c, guild, channels)).await`

use std::sync::Arc;

use super::message::ClearRange;
use super::state::SessionChannels;
use crate::db::playback::{self, PlaybackState};
use crate::db::queue::{self, AppendResult, NewQueueItem};
use crate::db::{history, Connection, DbError, DbWriter};
use crate::domain::{GuildId, ResolvedStream, TrackMeta, UserId};

/// 書き込みスレッドで処理を実行する。失敗したら 1 回だけやり直す。
///
/// - 処理は `Arc` で 1 つを共有して 2 回目に渡す (取り込んだ値を複製しない)
/// - 1 回目の失敗は warn のログに残す。2 回目の失敗は呼び出し側が扱う
///
/// # 引数
/// - `writer`: 書き込みスレッド
/// - `guild`: ログに出すギルド
/// - `f`: 接続を受け取る処理。2 回呼ばれることがあるため `Fn`
///
/// # 戻り値
/// `f` の戻り値
///
/// # エラー
/// 2 回とも失敗したときの 2 回目のエラー
///
/// # 呼び出し例
/// ```ignore
/// let done = store::write_with_retry(&db.writer, guild, move |c| store::requeue_saved(c, &state)).await?;
/// ```
pub(super) async fn write_with_retry<T, F>(
	writer: &DbWriter,
	guild: GuildId,
	f: F,
) -> Result<T, DbError>
where
	T: Send + 'static,
	F: Fn(&Connection) -> Result<T, DbError> + Send + Sync + 'static,
{
	let job = Arc::new(f);
	let first = Arc::clone(&job);
	match writer.run(move |c| first(c)).await {
		Ok(value) => Ok(value),
		Err(error) => {
			tracing::warn!(guild = guild.get(), %error, "DB の書き込みに失敗しました。1 回だけやり直します");
			writer.run(move |c| job(c)).await
		}
	}
}

/// 番号 (1 始まり) でキューを指す操作の結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum QueueEdit<T> {
	/// 操作した。
	Done(T),
	/// キューが空で、何もしなかった。
	Empty,
	/// 番号が範囲外で、何もしなかった。`count` はキューの件数。
	OutOfRange { count: u32 },
}

/// 解決済みのストリームからキューの曲を作る。
///
/// # 引数
/// - `stream`: 解決結果
/// - `user`: 依頼者
///
/// # 戻り値
/// キューへ入れる曲
///
/// # 呼び出し例
/// ```ignore
/// let item = item_from_stream(&stream, context.user);
/// ```
pub(super) fn item_from_stream(stream: &ResolvedStream, user: UserId) -> NewQueueItem {
	NewQueueItem {
		video_id: stream.video_id.clone(),
		title: stream.title.clone(),
		duration_s: stream.duration_s,
		requester_id: user,
	}
}

/// 検索結果やプレイリストの要素からキューの曲を作る。
///
/// # 引数
/// - `meta`: 動画の情報
/// - `user`: 依頼者
///
/// # 戻り値
/// キューへ入れる曲
///
/// # 呼び出し例
/// ```ignore
/// let item = item_from_meta(meta, context.user);
/// ```
pub(super) fn item_from_meta(meta: TrackMeta, user: UserId) -> NewQueueItem {
	NewQueueItem {
		video_id: meta.video_id,
		title: meta.title,
		duration_s: meta.duration_s,
		requester_id: user,
	}
}

/// キューの末尾へ追加する。追加前の件数も返す (すぐ再生する曲かどうかの判断に使う)。
///
/// - 件数は `queue::append` が数えたもの (`AppendResult::before`) を使い、数え直さない
///
/// # 引数
/// - `c`: 書き込み接続
/// - `guild`: 対象のギルド
/// - `items`: 追加する曲
/// - `limit`: キューの上限件数
///
/// # 戻り値
/// 追加の結果 (追加前の件数を含む)
///
/// # エラー
/// SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// let appended = writer.run(move |c| enqueue(c, guild, &items, limit)).await?;
/// ```
pub(super) fn enqueue(
	c: &Connection,
	guild: GuildId,
	items: &[NewQueueItem],
	limit: u32,
) -> Result<AppendResult, DbError> {
	queue::append(c, guild, items, limit)
}

/// キューの先頭を取り出し、`playback_state` に書く。空なら `playback_state` を消す。
///
/// # 引数
/// - `c`: 書き込み接続
/// - `guild`: 対象のギルド
/// - `channels`: 再生のセッションの宛先
///
/// # 戻り値
/// 取り出した曲。空なら None
///
/// # エラー
/// SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// let next = writer.run(move |c| pop_next(c, guild, channels)).await?;
/// ```
pub(super) fn pop_next(
	c: &Connection,
	guild: GuildId,
	channels: SessionChannels,
) -> Result<Option<NewQueueItem>, DbError> {
	let Some(popped) = queue::pop_front(c, guild)? else {
		playback::delete(c, guild)?;
		return Ok(None);
	};
	save_current(c, guild, channels, &popped.item)?;
	Ok(Some(popped.item))
}

/// 今の曲として `playback_state` に書く (先頭から再生する状態)。
///
/// # 引数
/// - `c`: 書き込み接続
/// - `guild`: 対象のギルド
/// - `channels`: 再生のセッションの宛先
/// - `item`: 今の曲
///
/// # エラー
/// SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// save_current(c, guild, channels, &item)?;
/// ```
fn save_current(
	c: &Connection,
	guild: GuildId,
	channels: SessionChannels,
	item: &NewQueueItem,
) -> Result<(), DbError> {
	playback::upsert(
		c,
		&PlaybackState {
			guild_id: guild,
			voice_channel_id: channels.voice,
			text_channel_id: channels.text,
			item: item.clone(),
			position_ms: 0,
			paused: false,
		},
	)
}

/// `?pnow`: 指定の番号の曲を取り出して今の曲にし、今までの曲をキューの先頭へ戻す。
///
/// - 取り出し、先頭への追加、`playback_state` の書き込みを 1 トランザクションで行う
/// - 番号は今の曲を含まないキューの 1 始まり (`?qlist` の番号と同じ)
///
/// # 引数
/// - `c`: 書き込み接続
/// - `guild`: 対象のギルド
/// - `index`: 再生する曲の番号
/// - `current`: 今の曲。次の曲として先頭へ戻す
/// - `channels`: 再生のセッションの宛先
///
/// # 戻り値
/// 取り出した曲。キューが空、または番号が範囲外なら何もせずにその旨を返す
///
/// # エラー
/// SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// let edit = writer.run(move |c| play_now(c, guild, 2, &current, channels)).await?;
/// ```
pub(super) fn play_now(
	c: &Connection,
	guild: GuildId,
	index: u32,
	current: &NewQueueItem,
	channels: SessionChannels,
) -> Result<QueueEdit<NewQueueItem>, DbError> {
	let count = queue::count(c, guild)?;
	if count == 0 {
		return Ok(QueueEdit::Empty);
	}
	if index == 0 || index > count {
		return Ok(QueueEdit::OutOfRange { count });
	}
	let Some(chosen) = queue::take_at(c, guild, index)? else {
		return Ok(QueueEdit::OutOfRange { count });
	};
	queue::push_front(c, guild, current)?;
	save_current(c, guild, channels, &chosen.item)?;
	Ok(QueueEdit::Done(chosen.item))
}

/// `?clear` の範囲を、両端を含む 1 始まりの番号の組にする。
///
/// - 番号は 1 からキューの件数までに限る。1 つでも外れていれば None
///
/// # 引数
/// - `range`: `?clear` の範囲
/// - `count`: キューの件数
///
/// # 戻り値
/// 削除する先頭と末尾の番号。範囲外なら None
///
/// # 呼び出し例
/// ```ignore
/// let Some((start, end)) = clear_bounds(range, count) else { ... };
/// ```
fn clear_bounds(range: ClearRange, count: u32) -> Option<(u32, u32)> {
	let (start, end) = match range {
		ClearRange::All => (1, count),
		ClearRange::First(n) => (1, n),
		ClearRange::Range { start, end } => (start, end),
	};
	(1 <= start && start <= end && end <= count).then_some((start, end))
}

/// `?clear`: キューの曲を消す (全件、先頭 N 件、範囲)。今の曲はキューに無いため消えない。
///
/// # 引数
/// - `c`: 書き込み接続
/// - `guild`: 対象のギルド
/// - `range`: 消す範囲
///
/// # 戻り値
/// 消した件数。キューが空、または範囲外なら何もせずにその旨を返す
///
/// # エラー
/// SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// let edit = writer.run(move |c| clear(c, guild, ClearRange::First(3))).await?;
/// ```
pub(super) fn clear(
	c: &Connection,
	guild: GuildId,
	range: ClearRange,
) -> Result<QueueEdit<u32>, DbError> {
	let count = queue::count(c, guild)?;
	if count == 0 {
		return Ok(QueueEdit::Empty);
	}
	let Some((start, end)) = clear_bounds(range, count) else {
		return Ok(QueueEdit::OutOfRange { count });
	};
	let removed = queue::remove_range(c, guild, start, end)?;
	Ok(QueueEdit::Done(removed))
}

/// `?sh`: キューの並びをシャッフルする。今の曲はキューに無いため動かない。
///
/// # 引数
/// - `c`: 書き込み接続
/// - `guild`: 対象のギルド
///
/// # 戻り値
/// シャッフルしたキューの件数。空なら `QueueEdit::Empty`
///
/// # エラー
/// SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// let edit = writer.run(move |c| shuffle(c, guild)).await?;
/// ```
pub(super) fn shuffle(c: &Connection, guild: GuildId) -> Result<QueueEdit<u32>, DbError> {
	let count = queue::count(c, guild)?;
	if count == 0 {
		return Ok(QueueEdit::Empty);
	}
	queue::shuffle(c, guild, &mut rand::rng())?;
	Ok(QueueEdit::Done(count))
}

/// `?leave`、自動退出、接続の失敗: キューと `playback_state` をまとめて消す。
///
/// # 引数
/// - `c`: 書き込み接続
/// - `guild`: 対象のギルド
///
/// # エラー
/// SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// writer.run(move |c| clear_session(c, guild)).await?;
/// ```
pub(super) fn clear_session(c: &Connection, guild: GuildId) -> Result<(), DbError> {
	queue::clear(c, guild)?;
	playback::delete(c, guild)
}

/// 復元しない再生状態の曲をキューの先頭へ戻し、`playback_state` を消す。
///
/// - 今の行が、読み込んだときの行 (`expected`) と同じときだけ行う。比べるのは動画、位置、通知先。
///   読み込んだ後に動き始めたセッションの行を消さないため
/// - 行の確認、先頭への追加、削除を 1 トランザクションで行う。行を残すと、次の起動で曲が二重になる
/// - キューの上限は確かめない (再生中だった曲をキューへ戻すだけで、曲は増えないため)
///
/// # 引数
/// - `c`: 書き込み接続
/// - `expected`: 読み込んだときの再生状態 (ギルドもここから取る)
///
/// # 戻り値
/// 戻して消したら true。行が無い、または変わっていたら何もせずに false
///
/// # エラー
/// SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// let done = writer.run(move |c| requeue_saved(c, &state)).await?;
/// ```
pub(super) fn requeue_saved(c: &Connection, expected: &PlaybackState) -> Result<bool, DbError> {
	let guild = expected.guild_id;
	let unchanged = playback::load(c, guild)?.is_some_and(|now| {
		now.item.video_id == expected.item.video_id
			&& now.position_ms == expected.position_ms
			&& now.text_channel_id == expected.text_channel_id
	});
	if !unchanged {
		return Ok(false);
	}
	queue::push_front(c, guild, &expected.item)?;
	playback::delete(c, guild)?;
	Ok(true)
}

/// セッションを途中で止めるとき、今の曲をキューの先頭へ戻し、`playback_state` を消す。キューの残りは消さない。
///
/// - 取得の不調で再生を止めたとき、回復の再接続に失敗したときに使う。利用者は `?p` でキューの先頭から再開できる
/// - 先頭への追加と削除を 1 トランザクションで行う。行を残すと、次の起動で曲が二重になる
/// - キューの上限は確かめない (再生中だった曲をキューへ戻すだけで、曲は増えないため)
///
/// # 引数
/// - `c`: 書き込み接続
/// - `guild`: 対象のギルド
/// - `item`: 今の曲。None なら `playback_state` を消すだけ
///
/// # 戻り値
/// なし
///
/// # エラー
/// SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// writer.run(move |c| requeue_current(c, guild, item.as_ref())).await?;
/// ```
pub(super) fn requeue_current(
	c: &Connection,
	guild: GuildId,
	item: Option<&NewQueueItem>,
) -> Result<(), DbError> {
	if let Some(item) = item {
		queue::push_front(c, guild, item)?;
	}
	playback::delete(c, guild)
}

/// 終わった曲 (`?skip` で飛ばした曲を含む) を履歴に記録し、ループ中ならキューの末尾へ戻す。
///
/// - 再生を始める前に飛ばした曲は履歴に記録しない (`played_at` が None)
///
/// # 引数
/// - `c`: 書き込み接続
/// - `guild`: 対象のギルド
/// - `item`: 終わった曲
/// - `played_at`: 再生を始めた UNIX 秒。再生を始めていなければ None
/// - `loop_limit`: ループ中ならキューの上限件数。None ならループしない
///
/// # エラー
/// SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// writer.run(move |c| finish(c, guild, &item, Some(played_at), Some(limit))).await?;
/// ```
pub(super) fn finish(
	c: &Connection,
	guild: GuildId,
	item: &NewQueueItem,
	played_at: Option<i64>,
	loop_limit: Option<u32>,
) -> Result<(), DbError> {
	if let Some(played_at) = played_at {
		history::record(c, guild, item, played_at)?;
	}
	if let Some(limit) = loop_limit {
		let appended = queue::append(c, guild, std::slice::from_ref(item), limit)?;
		if appended.rejected > 0 {
			tracing::warn!(
				guild = guild.get(),
				"キューが上限のため、ループの曲を戻せませんでした"
			);
		}
	}
	Ok(())
}

/// 終わった曲を記録し (`finish`)、続けて次の曲を取り出す (`pop_next`)。1 トランザクションで行う。
///
/// - ループ中は終わった曲をキューの末尾へ戻してから取り出す (1 曲だけのキューでも同じ曲を続けて再生する)
/// - 記録は内側の SAVEPOINT で囲む。記録に失敗したら記録だけを巻き戻してログに残し、取り出しは続ける
///   (履歴の書き込みの失敗で再生を止めないため)
///
/// # 引数
/// - `c`: 書き込み接続
/// - `guild`: 対象のギルド
/// - `finished`: 終わった曲
/// - `played_at`: 再生を始めた UNIX 秒。再生を始めていなければ None
/// - `loop_limit`: ループ中ならキューの上限件数。None ならループしない
/// - `channels`: 再生のセッションの宛先
///
/// # 戻り値
/// 取り出した曲。空なら None (`playback_state` は消す)
///
/// # エラー
/// 取り出しと SAVEPOINT の操作の SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// let next = writer.run(move |c| finish_and_pop_next(c, guild, &item, played_at, loop_limit, channels)).await?;
/// ```
pub(super) fn finish_and_pop_next(
	c: &Connection,
	guild: GuildId,
	finished: &NewQueueItem,
	played_at: Option<i64>,
	loop_limit: Option<u32>,
	channels: SessionChannels,
) -> Result<Option<NewQueueItem>, DbError> {
	c.execute_batch("SAVEPOINT finish_track")?;
	match finish(c, guild, finished, played_at, loop_limit) {
		Ok(()) => c.execute_batch("RELEASE finish_track")?,
		Err(error) => {
			tracing::error!(guild = guild.get(), %error, "終わった曲を記録できません。次の曲は取り出します");
			c.execute_batch("ROLLBACK TO finish_track; RELEASE finish_track")?;
		}
	}
	pop_next(c, guild, channels)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::domain::{ChannelId, VideoId};

	/// 移行 SQL を適用したメモリ DB。
	fn conn() -> Connection {
		let c = Connection::open_in_memory().expect("メモリ DB を開けない");
		c.execute_batch(include_str!("../../migrations/0001_init.sql"))
			.expect("移行 1 を適用できない");
		c.execute_batch(include_str!(
			"../../migrations/0002_playlist_fetch_limit.sql"
		))
		.expect("移行 2 を適用できない");
		c
	}

	fn item(n: u32) -> NewQueueItem {
		NewQueueItem {
			video_id: VideoId(format!("vid{n:08}")),
			title: format!("title {n}"),
			duration_s: Some(n),
			requester_id: UserId::new(7),
		}
	}

	fn channels() -> SessionChannels {
		SessionChannels {
			voice: ChannelId::new(20),
			text: ChannelId::new(10),
		}
	}

	fn guild() -> GuildId {
		GuildId::new(1)
	}

	#[test]
	fn finish_and_pop_next_requeues_loop_track_before_popping() {
		let c = conn();
		let next = finish_and_pop_next(&c, guild(), &item(1), Some(100), Some(10), channels())
			.expect("記録と取り出しに失敗");
		assert_eq!(next, Some(item(1)));
		assert_eq!(queue::count(&c, guild()).expect("件数"), 0);
		assert_eq!(history::list(&c, guild(), 0, 10).expect("履歴").len(), 1);
		let saved = playback::load(&c, guild()).expect("再生状態");
		assert_eq!(saved.map(|s| s.item), Some(item(1)));
	}

	#[test]
	fn finish_and_pop_next_on_empty_queue_clears_playback_state() {
		let c = conn();
		save_current(&c, guild(), channels(), &item(1)).expect("再生状態の保存");
		let next = finish_and_pop_next(&c, guild(), &item(1), None, None, channels())
			.expect("記録と取り出しに失敗");
		assert_eq!(next, None);
		assert!(playback::load(&c, guild()).expect("再生状態").is_none());
		// 再生を始めていない曲は履歴に残さない
		assert!(history::list(&c, guild(), 0, 10).expect("履歴").is_empty());
	}

	#[test]
	fn finish_failure_does_not_block_popping() {
		let c = conn();
		queue::append(&c, guild(), &[item(2)], 10).expect("追加");
		c.execute_batch(
			"CREATE TRIGGER fail_history BEFORE INSERT ON history
			 BEGIN SELECT RAISE(ABORT, 'test'); END;",
		)
		.expect("トリガーを作れない");
		let next = finish_and_pop_next(&c, guild(), &item(1), Some(100), Some(10), channels())
			.expect("記録の失敗で取り出しまで失敗した");
		assert_eq!(next, Some(item(2)));
		// 記録 (履歴とループの戻し) は巻き戻る
		assert_eq!(queue::count(&c, guild()).expect("件数"), 0);
	}

	#[test]
	fn clear_bounds_accepts_only_numbers_inside_queue() {
		assert_eq!(clear_bounds(ClearRange::All, 5), Some((1, 5)));
		assert_eq!(clear_bounds(ClearRange::First(3), 5), Some((1, 3)));
		assert_eq!(clear_bounds(ClearRange::First(5), 5), Some((1, 5)));
		assert_eq!(clear_bounds(ClearRange::First(6), 5), None);
		assert_eq!(clear_bounds(ClearRange::First(0), 5), None);
		assert_eq!(
			clear_bounds(ClearRange::Range { start: 2, end: 4 }, 5),
			Some((2, 4))
		);
		assert_eq!(
			clear_bounds(ClearRange::Range { start: 3, end: 3 }, 5),
			Some((3, 3))
		);
		assert_eq!(
			clear_bounds(ClearRange::Range { start: 0, end: 2 }, 5),
			None
		);
		assert_eq!(
			clear_bounds(ClearRange::Range { start: 4, end: 2 }, 5),
			None
		);
		assert_eq!(
			clear_bounds(ClearRange::Range { start: 4, end: 6 }, 5),
			None
		);
	}
}
