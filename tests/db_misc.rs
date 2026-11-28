//! 設定、履歴、キャッシュの操作の統合テスト。
//! - メモリ上の接続に移行 SQL を流して使う

use std::collections::BTreeMap;

use rusqlite::Connection;
use watapachi_bot::config::{CacheConfig, DbConfig, DefaultsConfig};
use watapachi_bot::db::queue::NewQueueItem;
use watapachi_bot::db::{cache, history, settings, Database};
use watapachi_bot::domain::{GuildId, PlaylistMeta, ResolvedStream, TrackMeta, UserId, VideoId};
use watapachi_bot::lang::Lang;

fn conn() -> Connection {
	let c = Connection::open_in_memory().unwrap();
	c.execute_batch(include_str!("../migrations/0001_init.sql"))
		.unwrap();
	c.execute_batch(include_str!("../migrations/0002_playlist_fetch_limit.sql"))
		.unwrap();
	c.execute_batch(include_str!("../migrations/0003_available_at.sql"))
		.unwrap();
	c.execute_batch(include_str!("../migrations/0004_history_index.sql"))
		.unwrap();
	c.execute_batch(include_str!("../migrations/0005_lang.sql"))
		.unwrap();
	c
}

fn guild(n: u64) -> GuildId {
	GuildId::new(n)
}

fn defaults() -> DefaultsConfig {
	DefaultsConfig {
		volume: 0.5,
		queue_limit: 1000,
		playlist_limit: 200,
		alone_timeout_secs: 300,
		lang: Lang::Ja,
	}
}

fn cache_config() -> CacheConfig {
	CacheConfig {
		playlist_ttl_secs: 100,
		search_days: 1,
		track_days: 2,
		history_days: 3,
	}
}

const DAY: i64 = 86_400;

fn item(n: u32) -> NewQueueItem {
	NewQueueItem {
		video_id: VideoId(format!("vid{n:08}")),
		title: format!("title {n}"),
		duration_s: Some(n),
		requester_id: UserId::new(7),
	}
}

fn meta(n: u32) -> TrackMeta {
	TrackMeta {
		video_id: VideoId(format!("vid{n:08}")),
		title: format!("title {n}"),
		duration_s: Some(n),
		thumbnail: Some(format!("https://example.invalid/{n}.jpg")),
	}
}

fn stream(n: u32, expires_at: i64) -> ResolvedStream {
	let mut headers = BTreeMap::new();
	headers.insert("User-Agent".to_string(), "ua".to_string());
	ResolvedStream {
		video_id: VideoId(format!("vid{n:08}")),
		title: format!("title {n}"),
		duration_s: Some(n),
		url: format!("https://example.invalid/s/{n}"),
		http_headers: headers,
		filesize: 1234,
		format_id: "251".to_string(),
		expires_at: Some(expires_at),
		available_at: None,
		is_live: false,
		protocol: "https".to_string(),
	}
}

fn row_count(c: &Connection, table: &str) -> i64 {
	c.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
		.unwrap()
}

#[test]
fn null_settings_follow_defaults() {
	let c = conn();
	let s = settings::load(&c, guild(1), &defaults()).unwrap();
	assert_eq!(s.volume, 0.5);
	assert_eq!(s.queue_limit, 1000);
	assert_eq!(s.playlist_limit, 200);
	assert_eq!(s.alone_timeout_secs, 300);
	assert!(!s.loop_enabled);
}

#[test]
fn setting_one_value_keeps_others_null() {
	let c = conn();
	settings::set_volume(&c, guild(1), 0.8).unwrap();
	settings::set_loop(&c, guild(1), true).unwrap();
	// 設定していない項目は NULL のまま。既定値の変更がそのまま効く
	let mut d = defaults();
	d.queue_limit = 50;
	d.alone_timeout_secs = 10;
	let s = settings::load(&c, guild(1), &d).unwrap();
	assert_eq!(s.volume, 0.8);
	assert!(s.loop_enabled);
	assert_eq!(s.queue_limit, 50);
	assert_eq!(s.alone_timeout_secs, 10);
	let nulls: i64 = c
		.query_row(
			"SELECT (queue_limit IS NULL) + (playlist_limit IS NULL) + (alone_timeout IS NULL)
			 FROM guild_settings WHERE guild_id = 1",
			[],
			|r| r.get(0),
		)
		.unwrap();
	assert_eq!(nulls, 3);

	settings::set_queue_limit(&c, guild(1), 7).unwrap();
	settings::set_playlist_limit(&c, guild(1), 8).unwrap();
	settings::set_alone_timeout(&c, guild(1), 9).unwrap();
	settings::set_loop(&c, guild(1), false).unwrap();
	let s = settings::load(&c, guild(1), &d).unwrap();
	assert_eq!(
		(
			s.queue_limit,
			s.playlist_limit,
			s.alone_timeout_secs,
			s.loop_enabled
		),
		(7, 8, 9, false)
	);
	// 他のギルドには影響しない
	assert_eq!(settings::load(&c, guild(2), &d).unwrap().queue_limit, 50);
}

#[test]
fn add_admin_twice_returns_false() {
	let c = conn();
	let g = guild(1);
	let u = UserId::new(5);
	assert!(!settings::is_admin(&c, g, u).unwrap());
	assert!(settings::add_admin(&c, g, u).unwrap());
	assert!(!settings::add_admin(&c, g, u).unwrap());
	assert!(settings::is_admin(&c, g, u).unwrap());
	assert!(!settings::is_admin(&c, guild(2), u).unwrap());
	assert!(settings::remove_admin(&c, g, u).unwrap());
	assert!(!settings::remove_admin(&c, g, u).unwrap());
	assert!(!settings::is_admin(&c, g, u).unwrap());
}

#[test]
fn history_lists_newest_first() {
	let c = conn();
	history::record(&c, guild(1), &item(1), 100).unwrap();
	history::record(&c, guild(1), &item(2), 300).unwrap();
	history::record(&c, guild(1), &item(3), 200).unwrap();
	history::record(&c, guild(2), &item(4), 400).unwrap();
	let all = history::list(&c, guild(1), 0, 10).unwrap();
	let got: Vec<_> = all.iter().map(|e| (e.item.clone(), e.played_at)).collect();
	assert_eq!(got, vec![(item(2), 300), (item(3), 200), (item(1), 100)]);
	let page = history::list(&c, guild(1), 1, 1).unwrap();
	assert_eq!(page.len(), 1);
	assert_eq!(page[0].item, item(3));
}

#[test]
fn expired_stream_is_not_returned() {
	let c = conn();
	cache::put_stream(&c, &stream(1, 1000)).unwrap();
	assert_eq!(
		cache::get_stream(&c, &item(1).video_id, 500, 0).unwrap(),
		Some(stream(1, 1000))
	);
	assert!(cache::get_stream(&c, &item(1).video_id, 1000, 0)
		.unwrap()
		.is_none());
	assert!(cache::get_stream(&c, &item(1).video_id, 2000, 0)
		.unwrap()
		.is_none());
	cache::delete_stream(&c, &item(1).video_id).unwrap();
	assert!(cache::get_stream(&c, &item(1).video_id, 0, 0)
		.unwrap()
		.is_none());
}

#[test]
fn stream_within_margin_is_not_returned() {
	let c = conn();
	cache::put_stream(&c, &stream(1, 1000)).unwrap();
	// 期限の 300 秒前までは使い、それ以降は余裕が足りないので返さない
	assert!(cache::get_stream(&c, &item(1).video_id, 600, 300)
		.unwrap()
		.is_some());
	assert!(cache::get_stream(&c, &item(1).video_id, 701, 300)
		.unwrap()
		.is_none());
}

#[test]
fn put_stream_overwrites_previous_entry() {
	let c = conn();
	cache::put_stream(&c, &stream(1, 1000)).unwrap();
	let mut newer = stream(1, 5000);
	newer.url = "https://example.invalid/new".to_string();
	cache::put_stream(&c, &newer).unwrap();
	assert_eq!(
		cache::get_stream(&c, &item(1).video_id, 0, 0).unwrap(),
		Some(newer)
	);
}

/// 切り離した書き込みが遅れて届いても、期限の古い結果で新しい結果を上書きしないこと。
#[test]
fn put_stream_keeps_entry_with_later_expiry() {
	let c = conn();
	let mut newer = stream(1, 5000);
	newer.url = "https://example.invalid/new".to_string();
	cache::put_stream(&c, &newer).unwrap();
	cache::put_stream(&c, &stream(1, 1000)).unwrap();
	assert_eq!(
		cache::get_stream(&c, &item(1).video_id, 0, 0).unwrap(),
		Some(newer)
	);
}

#[test]
fn live_stream_is_not_cached() {
	let c = conn();
	let mut live = stream(1, 1000);
	live.is_live = true;
	cache::put_stream(&c, &live).unwrap();
	assert!(cache::get_stream(&c, &item(1).video_id, 0, 0)
		.unwrap()
		.is_none());
}

#[test]
fn search_cache_round_trips_and_expires() {
	let c = conn();
	cache::put_search(&c, "query a", &meta(1), 1000).unwrap();
	assert_eq!(
		cache::get_search(&c, "query a", 1500, 1000).unwrap(),
		Some(meta(1))
	);
	assert!(cache::get_search(&c, "query a", 2000, 1000)
		.unwrap()
		.is_none());
	assert!(cache::get_search(&c, "other", 1500, 1000)
		.unwrap()
		.is_none());
	// 同じ検索語の上書き
	cache::put_search(&c, "query a", &meta(2), 3000).unwrap();
	assert_eq!(
		cache::get_search(&c, "query a", 3001, 1000).unwrap(),
		Some(meta(2))
	);
}

fn playlist(n: u32) -> PlaylistMeta {
	PlaylistMeta {
		title: Some("list".to_string()),
		entries: (0..n)
			.map(|i| TrackMeta {
				video_id: VideoId(format!("p{i:010}")),
				title: format!("p {i}"),
				duration_s: if i % 2 == 0 { Some(i) } else { None },
				thumbnail: None,
			})
			.collect(),
	}
}

#[test]
fn playlist_order_is_preserved() {
	let c = conn();
	let p = playlist(30);
	cache::put_playlist(&c, "https://example.invalid/pl", &p, 100, 1000).unwrap();
	let got = cache::get_playlist(&c, "https://example.invalid/pl", 1050, 100, 100)
		.unwrap()
		.unwrap();
	assert_eq!(got, p);
	// TTL を過ぎたら返さない
	assert!(
		cache::get_playlist(&c, "https://example.invalid/pl", 1100, 100, 100)
			.unwrap()
			.is_none()
	);
}

#[test]
fn playlist_cache_serves_only_when_limit_is_satisfied() {
	let c = conn();
	// 上限 5 で取得して 5 件: 続きがあるかもしれない
	cache::put_playlist(&c, "u", &playlist(5), 5, 10).unwrap();
	assert_eq!(
		cache::get_playlist(&c, "u", 11, 100, 3)
			.unwrap()
			.unwrap()
			.entries
			.len(),
		3
	);
	assert_eq!(
		cache::get_playlist(&c, "u", 11, 100, 5)
			.unwrap()
			.unwrap()
			.entries
			.len(),
		5
	);
	assert!(cache::get_playlist(&c, "u", 11, 100, 50).unwrap().is_none());
	// 上限 50 で取得して 5 件: 全件なので大きい上限にも使える
	cache::put_playlist(&c, "v", &playlist(5), 50, 10).unwrap();
	assert_eq!(
		cache::get_playlist(&c, "v", 11, 100, 500)
			.unwrap()
			.unwrap()
			.entries
			.len(),
		5
	);
}

#[test]
fn playlist_without_title_round_trips() {
	let c = conn();
	let mut p = playlist(2);
	p.title = None;
	cache::put_playlist(&c, "u", &p, 100, 10).unwrap();
	assert_eq!(cache::get_playlist(&c, "u", 11, 100, 100).unwrap(), Some(p));
}

#[test]
fn replacing_playlist_removes_old_items() {
	let c = conn();
	cache::put_playlist(&c, "u", &playlist(10), 100, 1000).unwrap();
	cache::put_playlist(&c, "u", &playlist(3), 100, 2000).unwrap();
	assert_eq!(row_count(&c, "playlist_items"), 3);
	let got = cache::get_playlist(&c, "u", 2001, 100, 100)
		.unwrap()
		.unwrap();
	assert_eq!(got.entries.len(), 3);
}

#[test]
fn purge_removes_only_expired_rows() {
	let c = conn();
	let cfg = cache_config();
	let now = 10 * DAY;

	// ストリーム: 期限切れ 1 件、有効 1 件
	cache::put_stream(&c, &stream(1, now - 1)).unwrap();
	cache::put_stream(&c, &stream(2, now + 1000)).unwrap();
	// 検索: search_days = 1
	cache::put_search(&c, "old", &meta(3), now - DAY - 1).unwrap();
	cache::put_search(&c, "new", &meta(4), now - DAY + 10).unwrap();
	// プレイリスト: ttl = 100 秒
	cache::put_playlist(&c, "old", &playlist(2), 100, now - 101).unwrap();
	cache::put_playlist(&c, "new", &playlist(2), 100, now - 10).unwrap();
	// 履歴: history_days = 3
	history::record(&c, guild(1), &item(1), now - 3 * DAY - 1).unwrap();
	history::record(&c, guild(1), &item(2), now - 3 * DAY + 10).unwrap();

	let removed = cache::purge_expired(&c, now, &cfg).unwrap();
	assert!(removed > 0);

	assert!(
		cache::get_stream(&c, &VideoId(format!("vid{:08}", 1)), 0, 0)
			.unwrap()
			.is_none()
	);
	assert!(
		cache::get_stream(&c, &VideoId(format!("vid{:08}", 2)), 0, 0)
			.unwrap()
			.is_some()
	);
	assert_eq!(row_count(&c, "search_cache"), 1);
	assert!(cache::get_search(&c, "new", now, DAY).unwrap().is_some());
	assert_eq!(row_count(&c, "playlist_cache"), 1);
	// 期限切れのプレイリストの要素も消える（新しい側の 2 件だけ残る）
	assert_eq!(row_count(&c, "playlist_items"), 2);
	assert_eq!(history::list(&c, guild(1), 0, 10).unwrap().len(), 1);

	// 2 回目は何も消えない
	assert_eq!(cache::purge_expired(&c, now, &cfg).unwrap(), 0);
}

#[test]
fn purge_removes_old_track_meta() {
	let c = conn();
	let cfg = cache_config();
	let now = 10 * DAY;
	// track_days = 2。検索の保存で track_meta も入る
	cache::put_search(&c, "q1", &meta(1), now - 2 * DAY - 1).unwrap();
	cache::put_search(&c, "q2", &meta(2), now - 10).unwrap();
	cache::purge_expired(&c, now, &cfg).unwrap();
	assert_eq!(row_count(&c, "track_meta"), 1);
}

/// 1 回の削除の上限を超える件数 (上限ちょうどで止まらないことも確かめる)。
const MORE_THAN_CHUNK: u32 = cache::PURGE_CHUNK_ROWS * 2 + 1;

/// 期限切れの履歴を `count` 件、有効な履歴を 1 件入れる。
fn fill_history(c: &Connection, count: u32, now: i64) {
	for n in 0..count {
		history::record(c, guild(1), &item(n), now - 3 * DAY - 1).unwrap();
	}
	history::record(c, guild(1), &item(count), now - 10).unwrap();
}

#[test]
fn purge_removes_rows_beyond_one_chunk() {
	let c = conn();
	let cfg = cache_config();
	let now = 10 * DAY;
	fill_history(&c, MORE_THAN_CHUNK, now);
	// 要素数が上限を超えるプレイリストも、親を消せば要素がすべて消える
	cache::put_playlist(&c, "old", &playlist(MORE_THAN_CHUNK), 10_000, now - 101).unwrap();

	let removed = cache::purge_expired(&c, now, &cfg).unwrap();

	assert_eq!(row_count(&c, "history"), 1);
	assert_eq!(row_count(&c, "playlist_cache"), 0);
	assert_eq!(row_count(&c, "playlist_items"), 0);
	// 履歴、プレイリスト 1 件と要素、put_playlist は track_meta を書かない
	assert_eq!(removed, MORE_THAN_CHUNK * 2 + 1);
}

#[test]
fn purge_chunk_stops_at_limit() {
	let c = conn();
	let now = 10 * DAY;
	fill_history(&c, 5, now);
	let thresholds = cache::PurgeThresholds::new(now, &cache_config());
	let n = cache::purge_chunk(&c, cache::PurgeTarget::History, thresholds, 2).unwrap();
	assert_eq!(n, 2);
	assert_eq!(row_count(&c, "history"), 4);
}

#[test]
fn huge_retention_does_not_overflow_and_keeps_rows() {
	let c = conn();
	let now = 10 * DAY;
	cache::put_playlist(&c, "p", &playlist(2), 100, now - 10_000).unwrap();
	let cfg = CacheConfig {
		playlist_ttl_secs: u64::MAX,
		search_days: u32::MAX,
		track_days: u32::MAX,
		history_days: u32::MAX,
	};
	assert_eq!(cache::purge_expired(&c, now, &cfg).unwrap(), 0);
	assert_eq!(row_count(&c, "playlist_cache"), 1);
}

#[tokio::test]
async fn purge_in_chunks_through_writer_removes_everything() {
	let dir = tempfile::tempdir().unwrap();
	let db = Database::open(
		&dir.path().join("test.db"),
		&DbConfig {
			batch_size: 256,
			read_connections: 2,
			writer_queue: 1024,
		},
	)
	.unwrap();
	let now = 10 * DAY;
	db.handles
		.writer
		.run(move |c| {
			fill_history(c, MORE_THAN_CHUNK, now);
			cache::put_search(c, "old", &meta(1), now - DAY - 1)?;
			cache::put_search(c, "new", &meta(2), now - 10)?;
			Ok(())
		})
		.await
		.unwrap();

	let removed = cache::purge_expired_in_chunks(&db.handles.writer, now, &cache_config())
		.await
		.unwrap();

	assert_eq!(removed, MORE_THAN_CHUNK + 1);
	let (history_rows, search_rows): (i64, i64) = db
		.handles
		.reader
		.read(|c| Ok((row_count(c, "history"), row_count(c, "search_cache"))))
		.await
		.unwrap();
	assert_eq!((history_rows, search_rows), (1, 1));
	db.close().await.unwrap();
}

#[test]
fn stream_cache_keeps_available_at() {
	let c = conn();
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_secs() as i64;
	let mut with = stream(1, now + 3600);
	with.available_at = Some(now + 5);
	let without = stream(2, now + 3600);
	cache::put_stream(&c, &with).unwrap();
	cache::put_stream(&c, &without).unwrap();
	let got = cache::get_stream(&c, &with.video_id, now, 0)
		.unwrap()
		.unwrap();
	assert_eq!(got.available_at, Some(now + 5));
	let got = cache::get_stream(&c, &without.video_id, now, 0)
		.unwrap()
		.unwrap();
	assert_eq!(got.available_at, None);
}

#[test]
fn lang_round_trips_and_unset_guilds_are_omitted() {
	let c = conn();
	settings::set_lang(&c, guild(1), Lang::En).unwrap();
	settings::set_queue_limit(&c, guild(2), 10).unwrap();
	assert_eq!(
		settings::load_langs(&c).unwrap(),
		vec![(guild(1), Lang::En)]
	);
	settings::set_lang(&c, guild(1), Lang::Ja).unwrap();
	assert_eq!(
		settings::load_langs(&c).unwrap(),
		vec![(guild(1), Lang::Ja)]
	);
}

#[test]
fn unknown_lang_in_db_is_skipped() {
	let c = conn();
	c.execute(
		"INSERT INTO guild_settings (guild_id, lang) VALUES (1, 'fr'), (2, 'en')",
		[],
	)
	.unwrap();
	assert_eq!(
		settings::load_langs(&c).unwrap(),
		vec![(guild(2), Lang::En)]
	);
}
