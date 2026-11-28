//! キャッシュ（ストリーム URL、検索結果、プレイリスト）の操作。
//! - 複数文の操作は自前でトランザクションを開かない（DbWriter::run の SAVEPOINT に任せる）
//! - 時刻は呼び出し側が UNIX 秒で渡す（テストで固定できるようにするため）。
//!   例外は `put_stream` で、`track_meta` の `fetched_at` に現在時刻を自分で入れる (ストリームの期限は `expires_at` が持つ)
//! - 期限切れの掃除は、書き込みスレッドを長く占めないよう表ごと・上限件数ごとに分けて消す
//! - 呼び出し: `writer.run(move |c| cache::put_stream(c, &stream)).await`
//!
//! `resolve_cache` は title、duration_s、is_live、protocol の列を持たない。そのため
//! - title と duration_s は `track_meta` に保存し、`get_stream` で結合して戻す
//! - ライブと `https` 以外のプロトコルは再利用できないので `put_stream` で保存しない
//! - `get_stream` が返す `protocol` は保存対象の `https` で固定する

use std::collections::BTreeMap;

use rusqlite::types::Type;
use rusqlite::{params, Connection, OptionalExtension};

use super::{now_secs, DbError, DbWriter};
use crate::config::CacheConfig;
use crate::domain::{PlaylistMeta, ResolvedStream, TrackMeta, VideoId};

/// キャッシュできるプロトコル。
/// - https 以外 (HLS など) は PipeInput で再生し、URL そのものを再利用しないため保存しない
const CACHEABLE_PROTOCOL: &str = "https";

/// 期限切れの掃除で、1 回の削除 (1 回の `DbWriter::run`) で消す最大の行数。
/// - 大きいと書き込みスレッドを長く占め、間に入る命令 (?skip など) の応答が遅れる
/// - 小さいと回数が増える。索引の無い条件で表を走査するため、数百件程度にとどめる
pub const PURGE_CHUNK_ROWS: u32 = 500;

/// 1 日の秒数。
const SECS_PER_DAY: i64 = 86_400;

/// JSON の変換エラーを rusqlite のエラーにする。
fn json_error(e: serde_json::Error, column: usize) -> rusqlite::Error {
	rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(e))
}

/// `track_meta` を挿入または更新する。サムネイルは `thumbnail` が Some のときだけ更新する。
fn upsert_track_meta(
	c: &Connection,
	video_id: &VideoId,
	title: &str,
	duration_s: Option<u32>,
	thumbnail: Option<&str>,
	fetched_at: i64,
) -> Result<(), DbError> {
	c.prepare_cached(
		"INSERT INTO track_meta (video_id, title, duration_s, thumbnail, fetched_at)
		 VALUES (?1, ?2, ?3, ?4, ?5)
		 ON CONFLICT(video_id) DO UPDATE SET
			title      = excluded.title,
			duration_s = excluded.duration_s,
			thumbnail  = COALESCE(excluded.thumbnail, track_meta.thumbnail),
			fetched_at = excluded.fetched_at",
	)?
	.execute(params![
		video_id.0, title, duration_s, thumbnail, fetched_at
	])?;
	Ok(())
}

/// ストリームのキャッシュを返す。
///
/// # 引数
/// - `video_id`: 動画 ID
/// - `now`: 現在の UNIX 秒
/// - `margin_secs`: 期限までにこの秒数以上の余裕があるものだけ返す
///
/// # 戻り値
/// 使えるキャッシュ。無い、期限切れ、余裕不足、`track_meta` が無い場合は None。
///
/// # エラー
/// - SQLite のエラー、保存された JSON が壊れているとき
///
/// # 呼び出し例
/// ```ignore
/// let hit = cache::get_stream(c, &video_id, now, 300)?;
/// ```
pub fn get_stream(
	c: &Connection,
	video_id: &VideoId,
	now: i64,
	margin_secs: i64,
) -> Result<Option<ResolvedStream>, DbError> {
	let found = c
		.prepare_cached(
			"SELECT r.url, r.http_headers, r.filesize, r.format_id, r.expires_at, t.title, t.duration_s, r.available_at
			 FROM resolve_cache r JOIN track_meta t ON t.video_id = r.video_id
			 WHERE r.video_id = ?1 AND r.expires_at > ?2",
		)?
		.query_row(
			params![video_id.0, now.saturating_add(margin_secs)],
			|r| {
				let headers: String = r.get(1)?;
				let http_headers: BTreeMap<String, String> =
					serde_json::from_str(&headers).map_err(|e| json_error(e, 1))?;
				Ok(ResolvedStream {
					video_id: video_id.clone(),
					title: r.get(5)?,
					duration_s: r.get(6)?,
					url: r.get(0)?,
					http_headers,
					filesize: r.get::<_, i64>(2)?.max(0) as u64,
					format_id: r.get(3)?,
					expires_at: Some(r.get(4)?),
					available_at: r.get(7)?,
					is_live: false,
					protocol: CACHEABLE_PROTOCOL.to_string(),
				})
			},
		)
		.optional()?;
	Ok(found)
}

/// ストリームをキャッシュに保存する。同じ動画の既存の行は置き換える。
///
/// # 引数
/// - `stream`: 解決結果。ライブ、`https` 以外のプロトコル、`expires_at` が None のものは保存せず `Ok(())` を返す
///
/// # エラー
/// - SQLite のエラー
pub fn put_stream(c: &Connection, stream: &ResolvedStream) -> Result<(), DbError> {
	// 期限が不明な URL は再利用の可否を判断できないので保存しない
	let Some(expires_at) = stream.expires_at else {
		return Ok(());
	};
	if stream.is_live || stream.protocol != CACHEABLE_PROTOCOL {
		return Ok(());
	}
	// BTreeMap<String, String> の直列化は失敗しないが、握りつぶさずエラーにする
	let headers = serde_json::to_string(&stream.http_headers)
		.map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
	upsert_track_meta(
		c,
		&stream.video_id,
		&stream.title,
		stream.duration_s,
		None,
		now_secs(),
	)?;
	// 書き込みは切り離して行うため、届く順は解決の順と限らない。期限の古い結果で新しい結果を上書きしない
	c.prepare_cached(
		"INSERT INTO resolve_cache (video_id, url, http_headers, filesize, format_id, expires_at, available_at)
		 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
		 ON CONFLICT(video_id) DO UPDATE SET
			url = excluded.url,
			http_headers = excluded.http_headers,
			filesize = excluded.filesize,
			format_id = excluded.format_id,
			expires_at = excluded.expires_at,
			available_at = excluded.available_at
		 WHERE excluded.expires_at >= resolve_cache.expires_at",
	)?
	.execute(params![
		stream.video_id.0,
		stream.url,
		headers,
		stream.filesize as i64,
		stream.format_id,
		expires_at,
		stream.available_at,
	])?;
	Ok(())
}

/// ストリームのキャッシュを削除する（再生に失敗した URL を捨てる用途）。無ければ何もしない。
///
/// # エラー
/// - SQLite のエラー
pub fn delete_stream(c: &Connection, video_id: &VideoId) -> Result<(), DbError> {
	c.prepare_cached("DELETE FROM resolve_cache WHERE video_id = ?1")?
		.execute([&video_id.0])?;
	Ok(())
}

/// 検索結果のキャッシュを返す。`track_meta` と結合する。
///
/// # 引数
/// - `query`: 正規化済みの検索語（NFKC と小文字化は呼び出し側）
/// - `now`: 現在の UNIX 秒
/// - `ttl_secs`: 有効な秒数
///
/// # 戻り値
/// 有効なキャッシュ。無い、期限切れ、`track_meta` が無い場合は None。
///
/// # エラー
/// - SQLite のエラー
pub fn get_search(
	c: &Connection,
	query: &str,
	now: i64,
	ttl_secs: i64,
) -> Result<Option<TrackMeta>, DbError> {
	let found = c
		.prepare_cached(
			"SELECT t.video_id, t.title, t.duration_s, t.thumbnail
			 FROM search_cache s JOIN track_meta t ON t.video_id = s.video_id
			 WHERE s.query = ?1 AND s.fetched_at + ?2 > ?3",
		)?
		.query_row(params![query, ttl_secs, now], |r| {
			Ok(TrackMeta {
				video_id: VideoId(r.get(0)?),
				title: r.get(1)?,
				duration_s: r.get(2)?,
				thumbnail: r.get(3)?,
			})
		})
		.optional()?;
	Ok(found)
}

/// 検索結果をキャッシュに保存する。`track_meta` も更新する。
///
/// # 引数
/// - `query`: 正規化済みの検索語
/// - `meta`: 検索結果
/// - `now`: 現在の UNIX 秒
///
/// # エラー
/// - SQLite のエラー
pub fn put_search(c: &Connection, query: &str, meta: &TrackMeta, now: i64) -> Result<(), DbError> {
	upsert_track_meta(
		c,
		&meta.video_id,
		&meta.title,
		meta.duration_s,
		meta.thumbnail.as_deref(),
		now,
	)?;
	c.prepare_cached(
		"INSERT OR REPLACE INTO search_cache (query, video_id, fetched_at) VALUES (?1, ?2, ?3)",
	)?
	.execute(params![query, meta.video_id.0, now])?;
	Ok(())
}

/// 動画を指す検索結果のキャッシュをすべて削除する。無ければ何もしない。
///
/// - 動画が削除済み、非公開、地域制限と分かったときに使う。検索語が使えない動画を
///   `search_days` の間指し続けないようにする (次の検索でワーカーに検索し直させる)
/// - `track_meta` は消さない (`resolve_cache` との結合にも使い、期限で消える)
///
/// # 引数
/// - `video_id`: 動画 ID
///
/// # 戻り値
/// 削除した行の数。
///
/// # エラー
/// - SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// let n = writer.run(move |c| cache::delete_search_by_video(c, &id)).await?;
/// ```
pub fn delete_search_by_video(c: &Connection, video_id: &VideoId) -> Result<usize, DbError> {
	let removed = c
		.prepare_cached("DELETE FROM search_cache WHERE video_id = ?1")?
		.execute([&video_id.0])?;
	Ok(removed)
}

/// プレイリストのキャッシュを返す。要素は保存した順。
///
/// # 引数
/// - `url`: プレイリストの URL
/// - `now`: 現在の UNIX 秒
/// - `ttl_secs`: 有効な秒数
/// - `limit`: 呼び出し側が欲しい件数の上限
///
/// # 戻り値
/// 有効で、`limit` を満たすキャッシュ。要素は `limit` 件までに切る。
/// 次の場合は None (呼び出し側が取り直す)。
/// - 無い、期限切れ、取得時の上限が不明 (移行前の行)
/// - 保存件数が `limit` に足りず、かつ全件を取得できたと言えない
///   (保存件数が取得時の上限に達している)
///
/// 要素のサムネイルは保存しないので None。
///
/// # エラー
/// - SQLite のエラー
pub fn get_playlist(
	c: &Connection,
	url: &str,
	now: i64,
	ttl_secs: i64,
	limit: u32,
) -> Result<Option<PlaylistMeta>, DbError> {
	let head = c
		.prepare_cached(
			"SELECT title, fetch_limit FROM playlist_cache WHERE url = ?1 AND fetched_at + ?2 > ?3",
		)?
		.query_row(params![url, ttl_secs, now], |r| {
			Ok((r.get::<_, Option<String>>(0)?, r.get::<_, Option<i64>>(1)?))
		})
		.optional()?;
	let Some((title, Some(fetch_limit))) = head else {
		return Ok(None);
	};
	let entries = c
		.prepare_cached(
			"SELECT video_id, title, duration_s FROM playlist_items WHERE url = ?1 ORDER BY idx",
		)?
		.query_map([url], |r| {
			Ok(TrackMeta {
				video_id: VideoId(r.get(0)?),
				title: r.get(1)?,
				duration_s: r.get(2)?,
				thumbnail: None,
			})
		})?
		.collect::<Result<Vec<_>, _>>()?;
	let stored = entries.len() as i64;
	// 足りない場合でも、保存件数が取得時の上限に満たなければ全件なので使える
	if stored < i64::from(limit) && stored >= fetch_limit {
		return Ok(None);
	}
	let mut entries = entries;
	entries.truncate(limit as usize);
	Ok(Some(PlaylistMeta { title, entries }))
}

/// プレイリストをキャッシュに保存する。既存の要素はすべて置き換える。
///
/// # 引数
/// - `url`: プレイリストの URL
/// - `meta`: プレイリストの情報
/// - `fetch_limit`: 取得時に渡した件数の上限。`meta` の件数がこれに満たなければ全件とみなす
/// - `now`: 現在の UNIX 秒
///
/// # エラー
/// - SQLite のエラー
///
/// # 注意
/// - 複数文だが自前でトランザクションを開かない。`DbWriter::run` 内で呼べば途中失敗は巻き戻る
pub fn put_playlist(
	c: &Connection,
	url: &str,
	meta: &PlaylistMeta,
	fetch_limit: u32,
	now: i64,
) -> Result<(), DbError> {
	c.prepare_cached("DELETE FROM playlist_items WHERE url = ?1")?
		.execute([url])?;
	c.prepare_cached(
		"INSERT OR REPLACE INTO playlist_cache (url, title, fetched_at, fetch_limit)
		 VALUES (?1, ?2, ?3, ?4)",
	)?
	.execute(params![url, meta.title, now, i64::from(fetch_limit)])?;
	let mut stmt = c.prepare_cached(
		"INSERT INTO playlist_items (url, idx, video_id, title, duration_s)
		 VALUES (?1, ?2, ?3, ?4, ?5)",
	)?;
	for (idx, entry) in meta.entries.iter().enumerate() {
		stmt.execute(params![
			url,
			idx as i64,
			entry.video_id.0,
			entry.title,
			entry.duration_s
		])?;
	}
	Ok(())
}

/// 期限切れの掃除で消す対象 (表)。`ALL` の順に消す。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PurgeTarget {
	/// `resolve_cache`: `expires_at` を過ぎた行
	ResolveCache,
	/// `search_cache`: `search_days` を過ぎた行
	SearchCache,
	/// `playlist_cache`: `playlist_ttl_secs` を過ぎた行
	PlaylistCache,
	/// `playlist_items`: 親 (`playlist_cache`) の無い行。親を消した後に消すため `PlaylistCache` より後に置く
	PlaylistItems,
	/// `track_meta`: `track_days` を過ぎた行
	TrackMeta,
	/// `history`: `history_days` を過ぎた行
	History,
}

impl PurgeTarget {
	/// 消す順のすべての対象。
	pub const ALL: [PurgeTarget; 6] = [
		PurgeTarget::ResolveCache,
		PurgeTarget::SearchCache,
		PurgeTarget::PlaylistCache,
		PurgeTarget::PlaylistItems,
		PurgeTarget::TrackMeta,
		PurgeTarget::History,
	];
}

/// 期限切れの判定に使う境目の時刻 (UNIX 秒)。この値以下の行を消す。
///
/// - 条件を `列 <= 境目` の形にして、列に計算を掛けない (索引があれば使え、行ごとの加算も無い)
/// - 境目は `now - 保持期間` を飽和演算で求める。保持期間が巨大でも桁あふれせず、何も消さない
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PurgeThresholds {
	/// `resolve_cache.expires_at` の境目 (現在時刻そのもの)
	pub resolve: i64,
	/// `search_cache.fetched_at` の境目
	pub search: i64,
	/// `playlist_cache.fetched_at` の境目
	pub playlist: i64,
	/// `track_meta.fetched_at` の境目
	pub track: i64,
	/// `history.played_at` の境目
	pub history: i64,
}

impl PurgeThresholds {
	/// 現在時刻と `[cache]` の設定から境目を求める。
	///
	/// # 引数
	/// - `now`: 現在の UNIX 秒
	/// - `config`: `[cache]` の設定（検索、動画情報、履歴は日数、プレイリストは秒数）
	///
	/// # 戻り値
	/// 対象ごとの境目
	///
	/// # 呼び出し例
	/// `let thresholds = PurgeThresholds::new(now, &config.cache);`
	pub fn new(now: i64, config: &CacheConfig) -> Self {
		let days = |d: u32| i64::from(d).saturating_mul(SECS_PER_DAY);
		let playlist_ttl = i64::try_from(config.playlist_ttl_secs).unwrap_or(i64::MAX);
		Self {
			resolve: now,
			search: now.saturating_sub(days(config.search_days)),
			playlist: now.saturating_sub(playlist_ttl),
			track: now.saturating_sub(days(config.track_days)),
			history: now.saturating_sub(days(config.history_days)),
		}
	}
}

/// 1 つの対象から、期限切れの行を最大 `limit` 件消す。
///
/// - 件数の上限は `rowid IN (SELECT rowid ... LIMIT n)` で付ける (SQLite の DELETE ... LIMIT は既定のビルドで使えない)
/// - `playlist_items` は WITHOUT ROWID のため、主キー `(url, idx)` で同じことをする
///
/// # 引数
/// - `target`: 消す対象
/// - `thresholds`: 境目の時刻
/// - `limit`: 消す最大の行数
///
/// # 戻り値
/// 消した行数。`limit` 未満なら、その対象の期限切れは残っていない
///
/// # エラー
/// - SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// let n = writer.run(move |c| cache::purge_chunk(c, PurgeTarget::History, thresholds, 500)).await?;
/// ```
pub fn purge_chunk(
	c: &Connection,
	target: PurgeTarget,
	thresholds: PurgeThresholds,
	limit: u32,
) -> Result<usize, DbError> {
	let (sql, threshold) = match target {
		PurgeTarget::ResolveCache => (
			"DELETE FROM resolve_cache WHERE rowid IN
				(SELECT rowid FROM resolve_cache WHERE expires_at <= ?1 LIMIT ?2)",
			thresholds.resolve,
		),
		PurgeTarget::SearchCache => (
			"DELETE FROM search_cache WHERE rowid IN
				(SELECT rowid FROM search_cache WHERE fetched_at <= ?1 LIMIT ?2)",
			thresholds.search,
		),
		PurgeTarget::PlaylistCache => (
			"DELETE FROM playlist_cache WHERE rowid IN
				(SELECT rowid FROM playlist_cache WHERE fetched_at <= ?1 LIMIT ?2)",
			thresholds.playlist,
		),
		PurgeTarget::PlaylistItems => {
			// 親の無い要素（期限切れで親を消した分を含む）を消す。時刻の境目は使わない
			let removed = c
				.prepare_cached(
					"DELETE FROM playlist_items WHERE (url, idx) IN
						(SELECT url, idx FROM playlist_items
						 WHERE url NOT IN (SELECT url FROM playlist_cache) LIMIT ?1)",
				)?
				.execute([limit])?;
			return Ok(removed);
		}
		PurgeTarget::TrackMeta => (
			"DELETE FROM track_meta WHERE rowid IN
				(SELECT rowid FROM track_meta WHERE fetched_at <= ?1 LIMIT ?2)",
			thresholds.track,
		),
		PurgeTarget::History => (
			"DELETE FROM history WHERE rowid IN
				(SELECT rowid FROM history WHERE played_at <= ?1 LIMIT ?2)",
			thresholds.history,
		),
	};
	Ok(c.prepare_cached(sql)?.execute(params![threshold, limit])?)
}

/// 期限切れの行をすべて削除する (1 つの接続の上で続けて消す)。
///
/// - `DbWriter::run` の中で呼ぶと、全部を消し終えるまで書き込みスレッドを占める。
///   動いている Bot では `purge_expired_in_chunks` を使う
///
/// # 引数
/// - `now`: 現在の UNIX 秒
/// - `config`: `[cache]` の設定（検索、動画情報、履歴は日数、プレイリストは秒数）
///
/// # 戻り値
/// 削除した行の合計 (u32 に収まらなければ u32::MAX)。
///
/// # エラー
/// - SQLite のエラー
///
/// # 呼び出し例
/// ```ignore
/// let n = cache::purge_expired(&conn, now, &cfg)?;
/// ```
pub fn purge_expired(c: &Connection, now: i64, config: &CacheConfig) -> Result<u32, DbError> {
	let thresholds = PurgeThresholds::new(now, config);
	let mut removed = 0usize;
	for target in PurgeTarget::ALL {
		loop {
			let n = purge_chunk(c, target, thresholds, PURGE_CHUNK_ROWS)?;
			removed = removed.saturating_add(n);
			if n < PURGE_CHUNK_ROWS as usize {
				break;
			}
		}
	}
	Ok(u32::try_from(removed).unwrap_or(u32::MAX))
}

/// 期限切れの行をすべて削除する。表ごと・`PURGE_CHUNK_ROWS` 件ごとに別々の `DbWriter::run` で消す。
///
/// - 1 回の削除が終わるたびにコミットされ、間に他の命令 (コマンドの書き込み) が入れる
/// - 途中で失敗したらそこで止め、エラーを返す。それまでに消した分は残る (消し直しは次回の掃除で行う)
///
/// # 引数
/// - `writer`: 書き込みスレッド
/// - `now`: 現在の UNIX 秒
/// - `config`: `[cache]` の設定
///
/// # 戻り値
/// 削除した行の合計 (u32 に収まらなければ u32::MAX)。
///
/// # エラー
/// - SQLite のエラー、書き込みスレッドの終了 (`WriterClosed`)
///
/// # 呼び出し例
/// ```ignore
/// let n = cache::purge_expired_in_chunks(&writer, now, &config.cache).await?;
/// ```
pub async fn purge_expired_in_chunks(
	writer: &DbWriter,
	now: i64,
	config: &CacheConfig,
) -> Result<u32, DbError> {
	let thresholds = PurgeThresholds::new(now, config);
	let mut removed = 0usize;
	for target in PurgeTarget::ALL {
		loop {
			let n = writer
				.run(move |c| purge_chunk(c, target, thresholds, PURGE_CHUNK_ROWS))
				.await?;
			removed = removed.saturating_add(n);
			if n < PURGE_CHUNK_ROWS as usize {
				break;
			}
		}
	}
	Ok(u32::try_from(removed).unwrap_or(u32::MAX))
}
