//! GuildActor の再生の流れの試験。音声、解決、通知は偽実装を使い、DB は一時ディレクトリの実 DB を使う。
//! - 環境と待ちの補助は `support/actor_env.rs` にある
//! - `#[tokio::test]` は現在のスレッドで動くランタイム。試験側が譲るまでアクターは進まない
//! - 時間を止めた試験では、試験側は tokio のタイマーを待たない (仮想時間を勝手に進めないため)

#[path = "support/actor_env.rs"]
mod actor_env;
#[path = "support/fake_audio.rs"]
mod fake_audio;
#[path = "support/fake_notifier.rs"]
mod fake_notifier;
#[path = "support/fake_resolver.rs"]
mod fake_resolver;

use std::time::{Duration, Instant};

use actor_env::*;
use fake_audio::{AudioCall, AudioOp, FakeAudio};
use fake_notifier::Logged::{Edited, Sent};
use watapachi_bot::audio::backend::{AudioError, AudioEvent};
use watapachi_bot::db::queue::NewQueueItem;
use watapachi_bot::db::settings;
use watapachi_bot::domain::{GuildId, Priority, ResolveError, UserId, VideoId};
use watapachi_bot::guild::message::{GuildMessage, LookupKind, QueryResult, TickKind};
use watapachi_bot::guild::notifier::{Card, Notice};
use watapachi_bot::guild::registry::GuildBusy;

#[tokio::test]
async fn play_url_connects_and_starts() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);

	env.play(&watch_url(&a));
	wait_until("再生の開始の通知", || {
		env.has_notice("play.now_playing")
	})
	.await;

	assert_eq!(
		env.audio.calls()[0],
		AudioCall::Connect {
			guild: guild(),
			channel: voice()
		}
	);
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &a)]);
	// 動画 URL の解決結果をそのまま再生に使い、解決し直さない
	assert_eq!(
		env.resolver.resolve_calls(),
		vec![(a.clone(), Priority::Interactive)]
	);
	assert_eq!(
		env.notice_keys(),
		vec!["play.preparing", "play.now_playing"]
	);
	assert_eq!(
		env.notice_args("play.now_playing"),
		Some(vec![("title", title(&a))])
	);
	assert!(env.notices().iter().all(|(channel, _)| *channel == text()));

	let states = env.playback();
	assert_eq!(states.len(), 1);
	assert_eq!(states[0].item.video_id, a);
	assert_eq!(states[0].item.requester_id, UserId::new(USER));
	assert_eq!(
		(states[0].voice_channel_id, states[0].text_channel_id),
		(voice(), text())
	);
	assert_eq!((states[0].position_ms, states[0].paused), (0, false));
	assert!(env.queue_ids().is_empty());
}

#[tokio::test]
async fn play_without_voice_channel_notifies_error() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);

	env.handle()
		.try_send(play_message(&watch_url(&a), None))
		.unwrap();
	wait_until("エラーの通知", || {
		env.has_notice("error.not_in_voice")
	})
	.await;
	settle().await;

	assert_eq!(
		env.notices(),
		vec![(
			text(),
			Notice {
				key: "error.not_in_voice",
				args: vec![],
				card: None,
				lists: vec![],
			}
		)]
	);
	assert!(env.audio.calls().is_empty());
	assert!(env.resolver.resolve_calls().is_empty());
	assert!(env.playback().is_empty());
}

#[tokio::test]
async fn play_while_playing_queues_and_notifies() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_videos(&[&a, &b]);
	env.resolver.set_search("next song", Ok(meta(&b)));

	env.play(&watch_url(&a));
	wait_until("1 曲目の再生", || env.has_notice("play.now_playing")).await;
	env.play("next song");
	wait_until("追加の通知", || env.has_notice("play.queued")).await;

	assert_eq!(
		env.notice_args("play.queued"),
		Some(vec![("title", title(&b))])
	);
	assert_eq!(env.queue_ids(), vec![b.clone()]);
	// 追加のカードは曲と、追加した後のキューの曲数を持つ
	let card = env
		.notices()
		.into_iter()
		.find(|(_, n)| n.key == "play.queued")
		.and_then(|(_, n)| n.card);
	let Some(Card::Queued(card)) = card else {
		panic!("追加のカードが無い: {card:?}");
	};
	assert_eq!(card.video_id, b);
	assert_eq!(card.waiting, 1);
	assert_eq!(card.requester, UserId::new(USER));
	assert_eq!(card.guild, GuildId::new(GUILD));
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &a)]);
	assert_eq!(env.playback()[0].item.video_id, a);
}

#[tokio::test]
async fn playlist_starts_first_and_queues_rest() {
	let env = Env::new();
	let (a, b, c) = (vid('a'), vid('b'), vid('c'));
	env.set_playlist(&[&a, &b, &c]);

	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || env.has_notice("play.now_playing")).await;
	wait_until("追加の通知", || env.has_notice("play.playlist_queued")).await;

	assert_eq!(plays(&env.audio), vec![expected_request(&env, &a)]);
	assert_eq!(env.queue_ids(), vec![b, c]);
	assert_eq!(
		env.notice_args("play.playlist_queued"),
		Some(vec![
			("added", "3".to_string()),
			("rejected", "0".to_string())
		])
	);
	assert_eq!(
		env.resolver.playlist_calls(),
		vec![(PLAYLIST_URL.to_string(), env.config.defaults.playlist_limit)]
	);
	assert_eq!(env.playback()[0].item.video_id, a);
}

#[tokio::test]
async fn playlist_over_limit_reports_rejected_count() {
	let env = Env::new();
	env.write(|c| settings::set_queue_limit(c, guild(), 3))
		.await;
	let ids: Vec<VideoId> = "abcde".chars().map(vid).collect();
	let refs: Vec<&VideoId> = ids.iter().collect();
	env.set_playlist(&refs);

	env.play(PLAYLIST_URL);
	wait_until("追加の通知", || {
		env.has_notice("play.playlist_truncated")
	})
	.await;
	wait_until("1 曲目の再生", || env.has_notice("play.now_playing")).await;

	assert_eq!(
		env.notice_args("play.playlist_truncated"),
		Some(vec![
			("added", "3".to_string()),
			("rejected", "2".to_string())
		])
	);
	assert!(!env.has_notice("play.playlist_queued"));
	assert_eq!(env.queue_ids(), vec![ids[1].clone(), ids[2].clone()]);
	assert_eq!(env.playback()[0].item.video_id, ids[0]);
	let card = env
		.notices()
		.into_iter()
		.find(|(_, n)| n.key == "play.playlist_truncated")
		.and_then(|(_, n)| n.card);
	let Some(Card::Playlist(card)) = card else {
		panic!("プレイリストのカードが無い: {card:?}");
	};
	assert_eq!((card.added, card.rejected), (3, 2));
	assert_eq!(card.url, PLAYLIST_URL);
	assert_eq!(card.first_video, Some(ids[0].clone()));
	assert_eq!(card.requester, UserId::new(USER));
}

#[tokio::test]
async fn playlist_with_full_queue_reports_queue_full() {
	let env = Env::new();
	env.write(|c| settings::set_queue_limit(c, guild(), 1))
		.await;
	let (a, b, c, d) = (vid('a'), vid('b'), vid('c'), vid('d'));
	env.set_videos(&[&a, &b]);
	env.resolver.set_search("next song", Ok(meta(&b)));
	env.set_playlist(&[&c, &d]);

	// 1 曲目を再生し、2 曲目でキューの上限 (1 曲) を埋めてからプレイリストを足す
	env.play(&watch_url(&a));
	wait_until("1 曲目の再生", || env.has_notice("play.now_playing")).await;
	env.play("next song");
	wait_until("2 曲目の追加", || env.has_notice("play.queued")).await;
	env.play(PLAYLIST_URL);
	wait_until("上限の通知", || env.has_notice("play.queue_full")).await;

	assert_eq!(
		env.notice_args("play.queue_full"),
		Some(vec![("limit", "1".to_string())])
	);
	assert!(!env.has_notice("play.playlist_truncated"));
}

#[tokio::test]
async fn track_end_starts_next_from_queue() {
	let env = Env::new();
	let (a, b, c) = (vid('a'), vid('b'), vid('c'));
	env.set_playlist(&[&a, &b, &c]);

	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	end_track(&env, &a).await;
	wait_until("2 曲目の再生", || started(&env.audio, &b)).await;
	wait_until("2 曲目の通知", || {
		env.count_notice("play.now_playing") == 2
	})
	.await;

	assert_eq!(
		env.notice_args("play.now_playing"),
		Some(vec![("title", title(&b))])
	);
	assert_eq!(env.playback()[0].item.video_id, b);
	assert_eq!(env.queue_ids(), vec![c]);
	assert!(!disconnected(&env.audio));
}

#[tokio::test]
async fn track_end_with_empty_queue_leaves_and_clears_state() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);

	env.play(&watch_url(&a));
	wait_until("再生の開始", || env.has_notice("play.now_playing")).await;
	assert_eq!(env.playback().len(), 1);

	end_track(&env, &a).await;
	wait_until("退出", || disconnected(&env.audio)).await;

	assert!(env.playback().is_empty());
	assert!(env.queue_ids().is_empty());
}

/// 最後の曲を再生し終えると、退出の後に再生の終了を 1 回だけ通知すること。
#[tokio::test]
async fn last_track_end_notifies_completed() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);

	env.play(&watch_url(&a));
	wait_until("再生の開始", || env.has_notice("play.now_playing")).await;
	assert!(!env.has_notice("play.completed"));

	end_track(&env, &a).await;
	wait_until("退出", || disconnected(&env.audio)).await;
	wait_until("再生の終了の通知", || {
		env.has_notice("play.completed")
	})
	.await;
	env.barrier().await;

	// 目印 (`barrier`) も通知を出すため、最後の通知ではなく再生の終了の通知を探して見る
	assert_eq!(env.count_notice("play.completed"), 1);
	assert_eq!(
		env.notices()
			.into_iter()
			.find(|(_, notice)| notice.key == "play.completed"),
		Some((
			text(),
			Notice {
				key: "play.completed",
				args: vec![],
				card: None,
				lists: vec![],
			}
		))
	);
}

#[tokio::test]
async fn finished_track_is_recorded_in_history() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);

	env.play(&watch_url(&a));
	wait_until("再生の開始", || env.has_notice("play.now_playing")).await;
	assert!(env.history().is_empty());
	end_track(&env, &a).await;
	wait_until("退出", || disconnected(&env.audio)).await;

	let history = env.history();
	assert_eq!(history.len(), 1);
	assert_eq!(
		history[0].item,
		NewQueueItem {
			video_id: a.clone(),
			title: title(&a),
			duration_s: Some(10),
			requester_id: UserId::new(USER),
		}
	);
	assert!(history[0].played_at > 0);
}

#[tokio::test]
async fn loop_appends_finished_track_to_tail() {
	let env = Env::new();
	env.write(|c| settings::set_loop(c, guild(), true)).await;
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);

	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	end_track(&env, &a).await;
	wait_until("2 曲目の再生", || started(&env.audio, &b)).await;

	assert_eq!(env.queue_ids(), vec![a.clone()]);
	assert_eq!(env.playback()[0].item.video_id, b);
	assert_eq!(env.history().len(), 1);
	assert!(!disconnected(&env.audio));
}

/// 先読みの解決中に前の曲が終わっても、次の曲は 1 回だけ再生すること。
///
/// - 今の曲が同じ曲の解決を待っていれば、先に届いた方 (先読みか通常の解決) で始め、後の結果は捨てる
#[tokio::test]
async fn prefetch_result_starts_waiting_track_once() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.resolver.hold(&b);
	env.set_playlist(&[&a, &b]);

	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	wait_until("2 曲目の先読みの開始", || {
		env.resolver
			.resolve_calls()
			.contains(&(b.clone(), Priority::Prefetch))
	})
	.await;
	end_track(&env, &a).await;
	wait_until("2 曲目の通常の解決の開始", || {
		env.resolver
			.resolve_calls()
			.contains(&(b.clone(), Priority::Interactive))
	})
	.await;
	env.barrier().await;

	env.resolver.release(&b);
	wait_until("2 曲目の再生", || started(&env.audio, &b)).await;
	env.barrier().await;
	let played: Vec<VideoId> = plays(&env.audio)
		.into_iter()
		.map(|request| request.video_id)
		.collect();
	assert_eq!(played, vec![a, b]);
}

#[tokio::test]
async fn prefetch_resolves_next_two_with_prefetch_priority() {
	let env = Env::new();
	let (a, b, c, d) = (vid('a'), vid('b'), vid('c'), vid('d'));
	env.set_playlist(&[&a, &b, &c, &d]);

	env.play(PLAYLIST_URL);
	wait_until("2 曲目まで先読み", || {
		env.resolver
			.resolve_calls()
			.contains(&(c.clone(), Priority::Prefetch))
	})
	.await;
	env.barrier().await;

	// 再生中の曲の次から 2 曲だけを、1 曲ずつ先読みの優先度で解決する
	assert_eq!(
		env.resolver.resolve_calls(),
		vec![
			(a.clone(), Priority::Interactive),
			(b.clone(), Priority::Prefetch),
			(c.clone(), Priority::Prefetch),
		]
	);

	// 曲が進むと、先読みの範囲も 1 曲進む。解決済みの曲は解決し直さない
	end_track(&env, &a).await;
	wait_until("3 曲目の先読み", || {
		env.resolver
			.resolve_calls()
			.contains(&(d.clone(), Priority::Prefetch))
	})
	.await;
	env.barrier().await;
	assert_eq!(
		env.resolver.resolve_calls(),
		vec![
			(a, Priority::Interactive),
			(b, Priority::Prefetch),
			(c, Priority::Prefetch),
			(d, Priority::Prefetch),
		]
	);
}

#[tokio::test]
async fn next_track_is_preloaded() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);

	env.play(PLAYLIST_URL);
	wait_until("次の曲の準備", || !preloads(&env.audio).is_empty()).await;
	assert_eq!(preloads(&env.audio), vec![expected_request(&env, &b)]);
	let track_b = track_for(&env.audio, &b).unwrap();

	end_track(&env, &a).await;
	wait_until("準備済みの曲の開始", || {
		env.audio
			.calls()
			.contains(&AudioCall::StartPreloaded(track_b))
	})
	.await;
	wait_until("2 曲目の通知", || {
		env.count_notice("play.now_playing") == 2
	})
	.await;

	// 準備済みの曲を使い、作り直さない
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &a)]);
	assert_eq!(
		env.notice_args("play.now_playing"),
		Some(vec![("title", title(&b))])
	);
	assert_eq!(env.playback()[0].item.video_id, b);
}

#[tokio::test]
async fn stale_resolve_result_is_ignored() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);
	env.resolver.set_search("song", Ok(meta(&a)));
	env.resolver.hold(&a);
	let mut stale = stream(&a);
	stale.url = "http://localhost/stale".to_string();

	env.play("song");
	wait_until("解決の開始", || {
		env.resolver
			.resolve_calls()
			.contains(&(a.clone(), Priority::Interactive))
	})
	.await;

	// 解決待ちの間に、古い世代の結果が届いても再生を始めない
	env.handle()
		.try_send(GuildMessage::Resolved {
			generation: 0,
			video_id: a.clone(),
			result: Ok(stale.clone()),
		})
		.unwrap();
	env.barrier().await;
	assert!(plays(&env.audio).is_empty());

	env.resolver.release(&a);
	wait_until("再生の開始", || env.has_notice("play.now_playing")).await;
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &a)]);

	// 再生中に遅れて届いた結果は、どの世代の値でも 2 回目の再生を始めない
	for generation in 0..4 {
		env.handle()
			.try_send(GuildMessage::Resolved {
				generation,
				video_id: a.clone(),
				result: Ok(stale.clone()),
			})
			.unwrap();
	}
	env.barrier().await;
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &a)]);
}

#[tokio::test]
async fn full_mailbox_returns_busy_without_blocking() {
	let env = Env::new();
	let handle = env.handle();
	let capacity = env.config.limits.actor_mailbox;

	// 現在のスレッドで動くランタイムなので、譲るまでアクターはメールボックスを読まない
	for _ in 0..capacity {
		handle
			.try_send(GuildMessage::Tick(TickKind::Position))
			.unwrap();
	}
	let begin = Instant::now();
	let result = handle.try_send(GuildMessage::Tick(TickKind::Position));
	assert_eq!(result, Err(GuildBusy));
	assert!(begin.elapsed() < Duration::from_secs(1));

	// 譲るとアクターが読み進め、また送れるようになる
	wait_until("メールボックスの空き", || {
		handle
			.try_send(GuildMessage::Tick(TickKind::Position))
			.is_ok()
	})
	.await;
}

#[tokio::test(start_paused = true)]
async fn slow_resolve_notifies_after_threshold() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);
	env.resolver.hold(&a);
	let threshold = Duration::from_secs(env.config.playback.slow_notice_secs);

	env.play(&watch_url(&a));
	wait_until("準備中の通知と接続", || {
		env.has_notice("play.preparing") && !env.audio.calls().is_empty()
	})
	.await;
	settle().await;

	tokio::time::advance(threshold - Duration::from_secs(1)).await;
	settle().await;
	assert!(!env.has_notice("play.slow"));

	tokio::time::advance(Duration::from_secs(1)).await;
	wait_until("遅延の通知", || env.has_notice("play.slow")).await;
	assert!(env
		.notices()
		.iter()
		.any(|(channel, n)| *channel == text() && n.key == "play.slow"));

	env.resolver.release(&a);
	wait_until("再生の開始", || env.has_notice("play.now_playing")).await;
	assert_eq!(
		env.notice_keys(),
		vec!["play.preparing", "play.slow", "play.now_playing"]
	);
}

#[tokio::test(start_paused = true)]
async fn idle_actor_stops_and_is_respawned() {
	let env = Env::new();
	let handle = env.handle();
	settle().await;
	let idle = Duration::from_secs(env.config.limits.actor_idle_timeout_secs);

	tokio::time::advance(idle - Duration::from_secs(1)).await;
	settle().await;
	assert!(env.registry.contains(guild()));

	tokio::time::advance(Duration::from_secs(1)).await;
	wait_until("アクターの終了", || !env.registry.contains(guild())).await;

	// 終了したアクターのハンドルで送っても、作り直したアクターが受け取る
	let a = vid('a');
	env.set_videos(&[&a]);
	handle
		.try_send(play_message(&watch_url(&a), Some(voice())))
		.unwrap();
	assert!(env.registry.contains(guild()));
	wait_until("再生の開始", || env.has_notice("play.now_playing")).await;
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &a)]);
}

/// 最初のセッションの番号。アクターは 0 から数え、Idle から接続を始めるたびに 1 つ進める。
const FIRST_SESSION: u64 = 1;

/// 最初のセッションを接続の失敗で終わらせる。
/// - `first` の問い合わせは保留したままにする (接続の失敗で止められ、自然には届かない)
/// - 前のセッションの結果は、試験側から `FIRST_SESSION` を付けて送り込んで再現する
async fn fail_first_session(env: &Env, first: &VideoId) {
	env.resolver.hold(first);
	env.audio.fail_next(AudioOp::Connect, AudioError::Timeout);
	env.play(&watch_url(first));
	wait_until("接続の失敗の通知", || {
		env.has_notice("error.connect_failed")
	})
	.await;
	env.barrier().await;
}

fn connects(audio: &FakeAudio) -> usize {
	audio
		.calls()
		.iter()
		.filter(|call| matches!(call, AudioCall::Connect { .. }))
		.count()
}

/// 2 回目の `?p` を、問い合わせを保留して送る。接続の完了まで処理させる。
async fn start_second_session(env: &Env, second: &VideoId) {
	env.resolver.hold(second);
	env.play(&watch_url(second));
	wait_until("2 回目の接続と問い合わせ", || {
		connects(&env.audio) == 2
			&& env
				.resolver
				.resolve_calls()
				.contains(&(second.clone(), Priority::Interactive))
	})
	.await;
	env.barrier().await;
}

#[tokio::test]
async fn stale_query_result_from_previous_session_is_not_queued() {
	let env = Env::new();
	let (x, y) = (vid('x'), vid('y'));
	env.set_videos(&[&x, &y]);
	fail_first_session(&env, &x).await;
	start_second_session(&env, &y).await;

	// 接続の失敗の前にメールボックスへ入っていた、前のセッションの結果が遅れて届いた形
	env.handle()
		.try_send(GuildMessage::QueryResolved {
			session: FIRST_SESSION,
			context: context(Some(voice())),
			kind: LookupKind::Video,
			result: Ok(QueryResult::Video(stream(&x))),
		})
		.unwrap();
	env.barrier().await;
	assert!(env.queue_ids().is_empty());
	assert!(plays(&env.audio).is_empty());
	assert!(!env.has_notice("play.queued"));

	env.resolver.release(&y);
	wait_until("2 回目の曲の再生", || {
		env.has_notice("play.now_playing")
	})
	.await;
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &y)]);
	assert!(env.queue_ids().is_empty());
}

#[tokio::test]
async fn stale_failed_query_does_not_cancel_pending_play() {
	let env = Env::new();
	let (x, y) = (vid('x'), vid('y'));
	env.set_videos(&[&x, &y]);
	fail_first_session(&env, &x).await;
	start_second_session(&env, &y).await;

	// 前のセッションの失敗の結果で、今のセッションの問い合わせ待ちを消さない (退出しない)
	env.handle()
		.try_send(GuildMessage::QueryResolved {
			session: FIRST_SESSION,
			context: context(Some(voice())),
			kind: LookupKind::Video,
			result: Err(ResolveError::Timeout),
		})
		.unwrap();
	env.barrier().await;
	assert!(!disconnected(&env.audio));
	assert!(!env.has_notice("error.lookup_timeout"));

	env.resolver.release(&y);
	wait_until("2 回目の曲の再生", || {
		env.has_notice("play.now_playing")
	})
	.await;
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &y)]);
	assert!(!disconnected(&env.audio));
}

#[tokio::test]
async fn stale_prefetch_result_from_previous_session_is_ignored() {
	let env = Env::new();
	let x = vid('x');
	env.set_videos(&[&x]);
	fail_first_session(&env, &x).await;

	let (a, b, c) = (vid('a'), vid('b'), vid('c'));
	env.set_playlist(&[&a, &b, &c]);
	env.resolver.hold(&b);
	env.play(PLAYLIST_URL);
	wait_until("次の曲の先読み", || {
		env.resolver
			.resolve_calls()
			.contains(&(b.clone(), Priority::Prefetch))
	})
	.await;

	// 前のセッションの先読みの結果で、今の解決中の記録を消さない (先読みは 1 曲ずつのまま)
	env.handle()
		.try_send(GuildMessage::Prefetched {
			session: FIRST_SESSION,
			video_id: b.clone(),
			result: Err(ResolveError::Timeout),
		})
		.unwrap();
	env.barrier().await;
	assert!(!env
		.resolver
		.resolve_calls()
		.contains(&(c.clone(), Priority::Prefetch)));

	// 今のセッションの結果は受け取り、先読みを進める
	env.resolver.release(&b);
	wait_until("2 曲目の先読み", || {
		env.resolver
			.resolve_calls()
			.contains(&(c.clone(), Priority::Prefetch))
	})
	.await;
	wait_until("次の曲の準備", || !preloads(&env.audio).is_empty()).await;
	assert_eq!(preloads(&env.audio), vec![expected_request(&env, &b)]);
}

#[tokio::test(start_paused = true)]
async fn preparing_message_is_replaced_by_now_playing() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);

	env.play(&watch_url(&a));
	wait_until("再生中の通知", || env.notifier.log().len() == 2).await;

	assert_eq!(
		env.notifier.log(),
		vec![
			Sent {
				id: 1,
				key: "play.preparing"
			},
			Edited {
				target: 1,
				key: "play.now_playing"
			}
		]
	);
	let Some(Card::NowPlaying(card)) = env.notifier.card_of(1) else {
		panic!("再生中のカードが無い");
	};
	assert_eq!(card.waiting, 0);
	assert_eq!(card.video_id, a);
	assert_eq!(card.title, title(&a));
	assert_eq!(card.requester, UserId::new(USER));
}

#[tokio::test(start_paused = true)]
async fn slow_edits_preparing_message() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);
	env.resolver.hold(&a);
	let threshold = Duration::from_secs(env.config.playback.slow_notice_secs);

	env.play(&watch_url(&a));
	wait_until("準備中の通知と接続", || {
		env.has_notice("play.preparing") && !env.audio.calls().is_empty()
	})
	.await;
	settle().await;
	tokio::time::advance(threshold).await;
	wait_until("お詫びへの書き換え", || {
		env.notifier.log().len() == 2
	})
	.await;
	env.resolver.release(&a);
	wait_until("再生中への書き換え", || {
		env.notifier.log().len() == 3
	})
	.await;

	assert_eq!(
		env.notifier.log(),
		vec![
			Sent {
				id: 1,
				key: "play.preparing"
			},
			Edited {
				target: 1,
				key: "play.slow"
			},
			Edited {
				target: 1,
				key: "play.now_playing"
			}
		]
	);
}

#[tokio::test(start_paused = true)]
async fn failed_edit_falls_back_to_send() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);
	env.notifier.fail_edits(true);

	env.play(&watch_url(&a));
	wait_until("再生中の通知", || env.notifier.log().len() == 2).await;

	assert_eq!(
		env.notifier.log(),
		vec![
			Sent {
				id: 1,
				key: "play.preparing"
			},
			Sent {
				id: 2,
				key: "play.now_playing"
			}
		]
	);
}

#[tokio::test(start_paused = true)]
async fn now_playing_sent_once_per_track() {
	let env = Env::new();
	let (a, b, c) = (vid('a'), vid('b'), vid('c'));
	env.set_playlist(&[&a, &b, &c]);

	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	env.barrier().await;
	assert_eq!(env.count_notice("play.now_playing"), 1);

	// 失敗の後の作り直しでは、再生中の通知を増やさない
	let track = track_for(&env.audio, &a).unwrap();
	env.audio
		.emit(AudioEvent::TrackFailed {
			track,
			error: AudioError::Failed("test".to_string()),
		})
		.await;
	wait_until("作り直した再生", || plays(&env.audio).len() == 2).await;
	env.barrier().await;
	assert_eq!(env.count_notice("play.now_playing"), 1);

	// 次の曲では 1 回増える。最初のカードは残りの 2 曲を数え、次のカードは 1 曲を数える
	end_track(&env, &a).await;
	wait_until("2 曲目の再生", || started(&env.audio, &b)).await;
	wait_until("2 曲目の通知", || {
		env.count_notice("play.now_playing") == 2
	})
	.await;
	let cards: Vec<u32> = env
		.notices()
		.into_iter()
		.filter_map(|(_, n)| n.card.and_then(|c| c.waiting()))
		.collect();
	assert_eq!(cards, vec![2, 1]);
}

/// ライブ配信の解決結果を返す。
fn live_stream(id: &VideoId) -> watapachi_bot::domain::ResolvedStream {
	let mut live = stream(id);
	live.is_live = true;
	live
}

#[tokio::test]
async fn live_url_is_refused_without_queueing() {
	let env = Env::new();
	let a = vid('a');
	env.resolver.set_result(&a, Ok(live_stream(&a)));

	env.play(&watch_url(&a));
	wait_until("ライブ配信を断る通知", || {
		env.has_notice("error.live_unsupported")
	})
	.await;
	wait_until("退出", || disconnected(&env.audio)).await;
	env.barrier().await;

	assert!(plays(&env.audio).is_empty());
	assert!(preloads(&env.audio).is_empty());
	assert!(env.queue_ids().is_empty());
	assert!(env.playback().is_empty());
	// 1 曲も取り出していないので、再生の終了は通知しない
	assert!(!env.has_notice("play.completed"));
}

#[tokio::test]
async fn live_track_in_queue_is_skipped_as_track_specific() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);
	env.resolver.set_result(&a, Ok(live_stream(&a)));

	env.play(PLAYLIST_URL);
	wait_until("次の曲の再生", || started(&env.audio, &b)).await;

	assert_eq!(plays(&env.audio), vec![expected_request(&env, &b)]);
	assert_eq!(env.count_notice("error.live_skipped"), 1);
	assert_eq!(
		env.notice_args("error.live_skipped"),
		Some(vec![("title", title(&a))])
	);
	assert!(plays(&env.audio).iter().all(|request| !request.use_pipe));
}

#[tokio::test]
async fn live_track_is_not_preloaded_and_is_skipped_when_reached() {
	let env = Env::new();
	let (a, b, c) = (vid('a'), vid('b'), vid('c'));
	env.set_playlist(&[&a, &b, &c]);
	env.resolver.set_result(&b, Ok(live_stream(&b)));

	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	wait_until("ライブ配信の先読みの解決", || {
		env.resolver.resolve_count(&b) >= 1
	})
	.await;
	settle().await;
	assert!(preloads(&env.audio)
		.iter()
		.all(|request| request.video_id != b));

	end_track(&env, &a).await;
	wait_until("3 曲目の再生", || started(&env.audio, &c)).await;
	assert!(!started(&env.audio, &b));
	assert_eq!(env.count_notice("error.live_skipped"), 1);
}

/// 囲みと空白だけの `?p` (`<>`、`< >`) は、検索も接続もせずに断ること。
#[tokio::test]
async fn empty_query_is_refused_without_searching() {
	let env = Env::new();

	env.play("<>");
	env.play("< >");
	wait_until("空の検索語を断る通知", || {
		env.count_notice("error.lookup_search_empty") == 2
	})
	.await;
	env.barrier().await;

	assert!(env.audio.calls().is_empty());
	assert!(!env.has_notice("play.preparing"));
	assert!(env.queue_ids().is_empty());
}

/// ループ中に 1 曲だけのキューの曲が終わると、末尾へ戻した同じ曲を続けて再生すること
/// (終わった曲の記録と次の曲の取り出しを 1 回の書き込みで行っても、戻してから取り出す順を保つ)。
#[tokio::test]
async fn loop_with_single_track_replays_it() {
	let env = Env::new();
	env.write(|c| settings::set_loop(c, guild(), true)).await;
	let a = vid('a');
	env.set_videos(&[&a]);

	env.play(&watch_url(&a));
	wait_until("1 回目の再生", || started(&env.audio, &a)).await;
	env.barrier().await;
	end_track(&env, &a).await;
	wait_until("2 回目の再生", || {
		plays(&env.audio).iter().filter(|r| r.video_id == a).count() == 2
	})
	.await;
	env.barrier().await;

	assert!(env.queue_ids().is_empty());
	assert_eq!(env.playback()[0].item.video_id, a);
	assert_eq!(env.history().len(), 1);
	assert!(!disconnected(&env.audio));
}

/// 履歴の書き込みに失敗しても、次の曲へ進むこと (記録の失敗は取り出しを巻き込まない)。
#[tokio::test]
async fn history_failure_does_not_stop_next_track() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);
	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	env.barrier().await;
	env.write(|c| {
		c.execute_batch(
			"CREATE TRIGGER fail_history_insert BEFORE INSERT ON history
			 BEGIN SELECT RAISE(ABORT, 'test'); END;",
		)?;
		Ok(())
	})
	.await;

	end_track(&env, &a).await;
	wait_until("2 曲目の再生", || started(&env.audio, &b)).await;
	env.barrier().await;

	assert!(env.history().is_empty());
	assert!(env.queue_ids().is_empty());
	assert_eq!(env.playback()[0].item.video_id, b);
}
