//! キューと再生状態の操作の統合テスト。
//! - 実 DB ではなくメモリ上の接続に移行 SQL を流して使う（関数は Connection 上で動くため）

use std::time::Instant;

use rand::rngs::StdRng;
use rand::SeedableRng;
use rusqlite::Connection;
use watapachi_bot::db::playback::{self, PlaybackState};
use watapachi_bot::db::queue::{self, NewQueueItem};
use watapachi_bot::domain::{ChannelId, GuildId, UserId, VideoId};

/// 移行 SQL を適用したメモリ DB を作る。
fn conn() -> Connection {
	let c = Connection::open_in_memory().unwrap();
	c.execute_batch(include_str!("../migrations/0001_init.sql"))
		.unwrap();
	c.execute_batch(include_str!("../migrations/0002_playlist_fetch_limit.sql"))
		.unwrap();
	c
}

fn guild(n: u64) -> GuildId {
	GuildId::new(n)
}

fn item(n: u32) -> NewQueueItem {
	NewQueueItem {
		video_id: VideoId(format!("vid{n:08}")),
		title: format!("title {n}"),
		duration_s: Some(n),
		requester_id: UserId::new(7),
	}
}

fn items(range: std::ops::Range<u32>) -> Vec<NewQueueItem> {
	range.map(item).collect()
}

/// キューの動画 ID を並び順に取り出す。
fn ids(c: &Connection, g: GuildId) -> Vec<String> {
	queue::list(c, g, 0, 100_000)
		.unwrap()
		.into_iter()
		.map(|q| q.item.video_id.0)
		.collect()
}

fn names(range: std::ops::Range<u32>) -> Vec<String> {
	range.map(|n| item(n).video_id.0).collect()
}

#[test]
fn append_keeps_order() {
	let c = conn();
	let r = queue::append(&c, guild(1), &items(0..3), 100).unwrap();
	assert_eq!((r.added, r.rejected), (3, 0));
	queue::append(&c, guild(1), &items(3..5), 100).unwrap();
	assert_eq!(ids(&c, guild(1)), names(0..5));
	assert_eq!(queue::count(&c, guild(1)).unwrap(), 5);
}

#[test]
fn append_over_limit_adds_partially() {
	let c = conn();
	queue::append(&c, guild(1), &items(0..3), 5).unwrap();
	let r = queue::append(&c, guild(1), &items(3..7), 5).unwrap();
	assert_eq!((r.added, r.rejected), (2, 2));
	assert_eq!(ids(&c, guild(1)), names(0..5));
}

#[test]
fn append_reports_count_before_adding() {
	let c = conn();
	let first = queue::append(&c, guild(1), &items(0..3), 100).unwrap();
	assert_eq!(first.before, 0);
	let second = queue::append(&c, guild(1), &items(3..5), 4).unwrap();
	assert_eq!((second.before, second.added, second.rejected), (3, 1, 1));
}

#[test]
fn repeated_shuffle_keeps_order_operations_working() {
	let c = conn();
	queue::append(&c, guild(1), &items(0..10), 100).unwrap();
	let mut rng = StdRng::seed_from_u64(7);
	for _ in 0..5 {
		queue::shuffle(&c, guild(1), &mut rng).unwrap();
	}
	// 位置は連番でなくなっても、先頭への追加・末尾への追加・取り出しは並びどおりに動く
	let shuffled = ids(&c, guild(1));
	queue::push_front(&c, guild(1), &item(98)).unwrap();
	queue::append(&c, guild(1), std::slice::from_ref(&item(99)), 100).unwrap();
	let mut want = vec![item(98).video_id.0];
	want.extend(shuffled.iter().cloned());
	want.push(item(99).video_id.0);
	assert_eq!(ids(&c, guild(1)), want);
	assert_eq!(
		queue::pop_front(&c, guild(1)).unwrap().unwrap().item,
		item(98)
	);
	let taken = queue::take_at(&c, guild(1), 1).unwrap().unwrap();
	assert_eq!(taken.item.video_id.0, shuffled[0]);
}

#[test]
fn push_front_then_pop_returns_it() {
	let c = conn();
	queue::append(&c, guild(1), &items(0..2), 100).unwrap();
	queue::push_front(&c, guild(1), &item(9)).unwrap();
	queue::push_front(&c, guild(1), &item(8)).unwrap();
	let popped = queue::pop_front(&c, guild(1)).unwrap().unwrap();
	assert_eq!(popped.item, item(8));
	assert_eq!(
		ids(&c, guild(1)),
		vec![item(9).video_id.0, item(0).video_id.0, item(1).video_id.0]
	);
}

#[test]
fn push_front_on_empty_queue_works() {
	let c = conn();
	queue::push_front(&c, guild(1), &item(1)).unwrap();
	assert_eq!(queue::count(&c, guild(1)).unwrap(), 1);
	assert_eq!(queue::peek(&c, guild(1), 5).unwrap().len(), 1);
}

#[test]
fn pop_on_empty_returns_none() {
	let c = conn();
	assert!(queue::pop_front(&c, guild(1)).unwrap().is_none());
}

#[test]
fn guilds_are_isolated() {
	let c = conn();
	queue::append(&c, guild(1), &items(0..3), 100).unwrap();
	queue::append(&c, guild(2), &items(10..12), 100).unwrap();
	assert_eq!(queue::count(&c, guild(1)).unwrap(), 3);
	assert_eq!(queue::count(&c, guild(2)).unwrap(), 2);
	queue::clear(&c, guild(1)).unwrap();
	assert_eq!(ids(&c, guild(2)), names(10..12));
	assert_eq!(
		queue::pop_front(&c, guild(2)).unwrap().unwrap().item,
		item(10)
	);
}

#[test]
fn take_at_preserves_order_of_others() {
	let c = conn();
	queue::append(&c, guild(1), &items(0..5), 100).unwrap();
	let taken = queue::take_at(&c, guild(1), 3).unwrap().unwrap();
	assert_eq!(taken.item, item(2));
	assert_eq!(
		ids(&c, guild(1)),
		vec![0, 1, 3, 4]
			.into_iter()
			.map(|n| item(n).video_id.0)
			.collect::<Vec<_>>()
	);
}

#[test]
fn take_at_out_of_range_returns_none() {
	let c = conn();
	queue::append(&c, guild(1), &items(0..3), 100).unwrap();
	assert!(queue::take_at(&c, guild(1), 0).unwrap().is_none());
	assert!(queue::take_at(&c, guild(1), 4).unwrap().is_none());
	assert_eq!(queue::count(&c, guild(1)).unwrap(), 3);
}

#[test]
fn remove_range_is_inclusive() {
	let c = conn();
	queue::append(&c, guild(1), &items(0..8), 100).unwrap();
	assert_eq!(queue::remove_range(&c, guild(1), 2, 5).unwrap(), 4);
	assert_eq!(
		ids(&c, guild(1)),
		vec![0, 5, 6, 7]
			.into_iter()
			.map(|n| item(n).video_id.0)
			.collect::<Vec<_>>()
	);
	// 範囲が逆、または 0 始まりなら何も消さない
	assert_eq!(queue::remove_range(&c, guild(1), 3, 2).unwrap(), 0);
	assert_eq!(queue::remove_range(&c, guild(1), 0, 2).unwrap(), 0);
}

#[test]
fn peek_and_list_paginate() {
	let c = conn();
	queue::append(&c, guild(1), &items(0..25), 100).unwrap();
	assert_eq!(queue::peek(&c, guild(1), 3).unwrap().len(), 3);
	let page = queue::list(&c, guild(1), 10, 10).unwrap();
	assert_eq!(page[0].item, item(10));
	assert_eq!(page.len(), 10);
	assert_eq!(queue::list(&c, guild(1), 20, 10).unwrap().len(), 5);
}

#[test]
fn shuffle_keeps_all_items() {
	let c = conn();
	queue::append(&c, guild(1), &items(0..50), 100).unwrap();
	// 先頭を戻して position を負にしてから振り直しても落ちないこと
	queue::push_front(&c, guild(1), &item(99)).unwrap();
	queue::shuffle(&c, guild(1), &mut StdRng::seed_from_u64(1)).unwrap();
	let mut got = ids(&c, guild(1));
	assert_eq!(got.len(), 51);
	got.sort();
	let mut want = names(0..50);
	want.push(item(99).video_id.0);
	want.sort();
	assert_eq!(got, want);
}

#[test]
fn shuffle_is_deterministic_with_seeded_rng() {
	let a = conn();
	let b = conn();
	for c in [&a, &b] {
		queue::append(c, guild(1), &items(0..30), 100).unwrap();
		queue::shuffle(c, guild(1), &mut StdRng::seed_from_u64(42)).unwrap();
	}
	assert_eq!(ids(&a, guild(1)), ids(&b, guild(1)));
	assert_ne!(ids(&a, guild(1)), names(0..30));
}

#[test]
fn operations_are_fast_with_5000_items() {
	let c = conn();
	let all = items(0..5000);
	let t = Instant::now();
	let r = queue::append(&c, guild(1), &all, 10_000).unwrap();
	assert_eq!(r.added, 5000);
	assert!(t.elapsed().as_millis() < 200, "append {:?}", t.elapsed());

	let t = Instant::now();
	let page = queue::list(&c, guild(1), 4990, 10).unwrap();
	assert_eq!(page.len(), 10);
	assert!(t.elapsed().as_millis() < 200, "list {:?}", t.elapsed());

	let t = Instant::now();
	queue::shuffle(&c, guild(1), &mut StdRng::seed_from_u64(3)).unwrap();
	assert!(t.elapsed().as_millis() < 200, "shuffle {:?}", t.elapsed());
	assert_eq!(queue::count(&c, guild(1)).unwrap(), 5000);
}

#[test]
fn title_with_emoji_and_newline_round_trips() {
	let c = conn();
	let mut it = item(1);
	it.title = "曲 🎵\n二行目\t\"quote\" 'x'".to_string();
	it.duration_s = None;
	queue::append(&c, guild(1), std::slice::from_ref(&it), 10).unwrap();
	assert_eq!(queue::pop_front(&c, guild(1)).unwrap().unwrap().item, it);
}

#[test]
fn playback_state_round_trips() {
	let c = conn();
	let state = PlaybackState {
		guild_id: guild(1),
		voice_channel_id: ChannelId::new(10),
		text_channel_id: ChannelId::new(11),
		item: item(5),
		position_ms: 12_345,
		paused: true,
	};
	playback::upsert(&c, &state).unwrap();
	let mut moved = state.clone();
	moved.position_ms = 99;
	moved.paused = false;
	playback::upsert(&c, &moved).unwrap();
	let other = PlaybackState {
		guild_id: guild(2),
		..state.clone()
	};
	playback::upsert(&c, &other).unwrap();

	let mut all = playback::load_all(&c).unwrap();
	all.sort_by_key(|s| s.guild_id);
	assert_eq!(all, vec![moved.clone(), other]);
	assert_eq!(playback::load(&c, guild(1)).unwrap(), Some(moved));
	playback::delete(&c, guild(1)).unwrap();
	assert_eq!(playback::load_all(&c).unwrap().len(), 1);
	assert_eq!(playback::load(&c, guild(1)).unwrap(), None);
}
