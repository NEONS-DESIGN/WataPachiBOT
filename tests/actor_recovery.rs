//! GuildActor の失敗時の手順 (解決の失敗、曲の失敗、接続の回復) の試験。
//! - 環境と待ちの補助は `support/actor_env.rs` にある。音声、解決、通知は偽実装、DB は一時ディレクトリの実 DB
//! - 「起きないこと」は目印 (`Env::barrier`) で、それより前のメッセージの処理を終えたことを確かめてから見る
//! - 時間を止めた試験では、試験側は tokio のタイマーを待たず、`tokio::time::advance` だけで時間を進める
//! - 待ち時間、確認の間隔、回数の上限は config.example.toml の値を読む

#[path = "support/actor_env.rs"]
mod actor_env;
#[path = "support/fake_audio.rs"]
mod fake_audio;
#[path = "support/fake_notifier.rs"]
mod fake_notifier;
#[path = "support/fake_resolver.rs"]
mod fake_resolver;

use std::time::Duration;

use actor_env::*;
use fake_audio::{AudioCall, AudioOp, FakeAudio};
use watapachi_bot::audio::access::{AccessVerdict, Denial};
use watapachi_bot::audio::backend::{
	AudioError, AudioEvent, DisconnectKind, PlayRequest, VoiceHealth,
};
use watapachi_bot::db::settings;
use watapachi_bot::domain::{ChannelId, Priority, ResolveError, ResolveErrorCode, VideoId};
use watapachi_bot::guild::message::GuildCommand;

/// 時間を止めた試験で、時刻の直前を見るために引く時間。
const JUST_BEFORE: Duration = Duration::from_millis(1);
/// 曲の失敗で偽の音声が返す位置。
const FAILED_AT: Duration = Duration::from_secs(30);
/// 回復のときに偽の音声が返す位置。
const LOST_AT: Duration = Duration::from_secs(42);
/// 曲の開始 (`TrackStarted`) が届くまでの時間 (広告の秒数を待つ準備に相当)。猶予と異常の回数の合計より長い。
const START_DELAY: Duration = Duration::from_secs(20);

fn worker_error(code: ResolveErrorCode) -> ResolveError {
	ResolveError::Worker {
		code,
		message: "test".to_string(),
	}
}

fn count_calls(audio: &FakeAudio, pred: impl Fn(&AudioCall) -> bool) -> usize {
	audio.calls().iter().filter(|c| pred(c)).count()
}

fn connects(audio: &FakeAudio) -> usize {
	count_calls(audio, |c| matches!(c, AudioCall::Connect { .. }))
}

fn last_play(audio: &FakeAudio) -> PlayRequest {
	plays(audio).pop().unwrap()
}

/// 指定のキーの通知の、曲名の引数を順に返す。
fn titles_of(env: &Env, key: &str) -> Vec<String> {
	env.notices()
		.into_iter()
		.filter(|(_, n)| n.key == key)
		.filter_map(|(_, n)| {
			n.args
				.into_iter()
				.find(|(name, _)| *name == "title")
				.map(|(_, value)| value)
		})
		.collect()
}

/// 動画 URL の `?p` で 1 曲を再生し、再生の開始まで処理させる。
async fn play_one(env: &Env, id: &VideoId) {
	env.set_videos(&[id]);
	env.play(&watch_url(id));
	wait_until("再生の開始", || started(&env.audio, id)).await;
	env.barrier().await;
}

/// 今の曲の失敗を音声バックエンドから知らせる。
async fn fail_track(env: &Env, id: &VideoId) {
	let track = track_for(&env.audio, id).unwrap();
	env.audio
		.emit(AudioEvent::TrackFailed {
			track,
			error: AudioError::Failed("test".to_string()),
		})
		.await;
}

/// 今の曲の開始を音声バックエンドから知らせる。健全性の猶予はここから数える。
async fn start_track(env: &Env, id: &VideoId) {
	let track = track_for(&env.audio, id).unwrap();
	env.audio.emit(AudioEvent::TrackStarted { track }).await;
	env.barrier().await;
}

/// 送信が無い (暗号化も未暗号化も増えない) 健全性を返すようにする。
fn send_no_frames(env: &Env) {
	env.audio.set_health_step(VoiceHealth {
		dave_version: Some(1),
		encrypted_frames: 0,
		plaintext_frames: 0,
	});
}

async fn lose_connection(env: &Env) {
	env.audio
		.emit(AudioEvent::Disconnected {
			reason: DisconnectKind::Lost,
		})
		.await;
}

/// 接続の回数と再生の回数が揃うまで待つ (回復の完了)。
async fn wait_rejoined(env: &Env, connects_total: usize, plays_total: usize) {
	wait_until("回復の完了", || {
		connects(&env.audio) == connects_total && plays(&env.audio).len() == plays_total
	})
	.await;
	env.barrier().await;
}

/// `rate_limited` の n 回目のやり直しまでの待ち時間 (倍々で、上限あり)。
fn backoff(env: &Env, retry: u32) -> Duration {
	let base = env.config.worker.rate_limit_backoff_secs;
	let max = env.config.worker.rate_limit_backoff_max_secs;
	let secs = base.saturating_mul(1u64 << (retry - 1)).min(max);
	Duration::from_secs(secs)
}

#[tokio::test]
async fn resolve_failures_skip_with_reason() {
	let env = Env::new();
	let (a, b, c, d, e) = (vid('a'), vid('b'), vid('c'), vid('d'), vid('e'));
	env.set_playlist(&[&a, &b, &c, &d, &e]);
	let cases = [
		(&a, ResolveErrorCode::NotFound, "error.video_not_found"),
		(&b, ResolveErrorCode::Private, "error.video_private"),
		(&c, ResolveErrorCode::GeoBlocked, "error.video_geo_blocked"),
		(
			&d,
			ResolveErrorCode::AgeRestricted,
			"error.video_age_restricted",
		),
	];
	for (id, code, _) in &cases {
		env.resolver.set_result(id, Err(worker_error(*code)));
	}

	env.play(PLAYLIST_URL);
	wait_until("5 曲目の再生", || started(&env.audio, &e)).await;
	env.barrier().await;

	// やり直さずに、曲名と理由を通知して飛ばす
	for (id, _, key) in &cases {
		assert_eq!(titles_of(&env, key), vec![title(id)], "{key}");
		assert_eq!(env.resolver.resolve_count(id), 1, "{key}");
		assert!(!started(&env.audio, id));
	}
	assert!(!env.has_notice("error.track_skipped"));
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &e)]);
	assert_eq!(env.playback()[0].item.video_id, e);
}

#[tokio::test]
async fn internal_error_retries_then_skips() {
	let env = Env::new();
	let (a, b, c) = (vid('a'), vid('b'), vid('c'));
	env.set_playlist(&[&a, &b, &c]);
	let internal = || Err(worker_error(ResolveErrorCode::Internal));
	// a はずっと失敗し、b は 2 回失敗してから成功する
	env.resolver.set_result(&a, internal());
	env.resolver.push_results(&b, vec![internal(), internal()]);
	let retries = env.config.worker.max_retries as usize;

	env.play(PLAYLIST_URL);
	wait_until("2 曲目の再生", || started(&env.audio, &b)).await;
	env.barrier().await;

	assert_eq!(env.resolver.resolve_count(&a), 1 + retries);
	assert_eq!(titles_of(&env, "error.track_skipped"), vec![title(&a)]);
	assert!(!started(&env.audio, &a));
	assert_eq!(env.resolver.resolve_count(&b), 3);
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &b)]);
	assert_eq!(
		titles_of(&env, "play.now_playing"),
		vec![title(&b)],
		"やり直して成功した曲は通常どおり再生する"
	);
}

#[tokio::test(start_paused = true)]
async fn rate_limited_waits_with_backoff() {
	let limited = || Err(worker_error(ResolveErrorCode::RateLimited));

	// 2 回の制限の後に成功する曲は、待ってからやり直して再生する
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);
	env.resolver.push_results(&a, vec![limited(), limited()]);
	env.play(PLAYLIST_URL);
	wait_until("待機の通知", || env.has_notice("play.rate_limited")).await;
	env.barrier().await;
	for retry in 1..=2u32 {
		let delay = backoff(&env, retry);
		tokio::time::advance(delay - JUST_BEFORE).await;
		settle().await;
		env.barrier().await;
		assert_eq!(
			env.resolver.resolve_count(&a),
			retry as usize,
			"待ち時間の前にやり直さない"
		);
		tokio::time::advance(JUST_BEFORE).await;
		wait_until("やり直し", || {
			env.resolver.resolve_count(&a) == retry as usize + 1
		})
		.await;
	}
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	env.barrier().await;
	assert_eq!(env.count_notice("play.rate_limited"), 1);
	assert!(!env.has_notice("error.track_skipped"));
}

#[tokio::test(start_paused = true)]
async fn rate_limited_exhaustion_stops_without_losing_tracks() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);
	env.resolver
		.set_result(&a, Err(worker_error(ResolveErrorCode::RateLimited)));
	env.play(PLAYLIST_URL);
	wait_until("待機の通知", || env.has_notice("play.rate_limited")).await;
	env.barrier().await;

	// 上限の回数だけ待ってやり直し、それでも制限されたら飛ばさずに止める
	let retries = env.config.worker.max_retries;
	for retry in 1..=retries {
		tokio::time::advance(backoff(&env, retry)).await;
		wait_until("やり直し", || {
			env.resolver.resolve_count(&a) == retry as usize + 1
		})
		.await;
	}
	wait_until("停止の案内", || {
		env.has_notice("error.rate_limited_stopped")
	})
	.await;
	env.barrier().await;

	assert_eq!(env.queue_ids(), vec![a.clone(), b.clone()]);
	assert!(env.playback().is_empty());
	assert!(disconnected(&env.audio));
	assert!(plays(&env.audio).is_empty());
	assert_eq!(env.resolver.resolve_count(&b), 0);
	assert_eq!(
		env.notice_args("error.rate_limited_stopped"),
		Some(vec![("prefix", env.config.bot.prefix.clone())])
	);
	assert_eq!(env.count_notice("play.rate_limited"), 1);
	assert!(!env.has_notice("error.track_skipped"));
	assert!(!env.has_notice("error.fetch_stopped"));

	// 止めた後は解決も通知も増えない
	let resolves = env.resolver.resolve_count(&a);
	for _ in 0..3 {
		tokio::time::advance(Duration::from_secs(
			env.config.worker.rate_limit_backoff_max_secs,
		))
		.await;
		settle().await;
	}
	env.barrier().await;
	assert_eq!(env.resolver.resolve_count(&a), resolves);
	assert_eq!(env.count_notice("error.rate_limited_stopped"), 1);
}

#[tokio::test(start_paused = true)]
async fn paused_resolve_waits_until_retry_at_without_counting() {
	let env = Env::new();
	let a = vid('a');
	env.set_playlist(&[&a]);
	let wait = Duration::from_secs(env.config.worker.rate_limit_backoff_max_secs);
	let retries = env.config.worker.max_retries as usize;
	// やり直しの上限より多く Paused を返してから成功する
	let paused: Vec<_> = (0..=retries)
		.map(|_| {
			Err(ResolveError::Paused {
				retry_at: tokio::time::Instant::now() + wait,
			})
		})
		.collect();
	env.resolver.push_results(&a, paused);
	env.play(PLAYLIST_URL);
	wait_until("待機の通知", || env.has_notice("play.rate_limited")).await;
	env.barrier().await;

	// retry_at まで待ってからやり直す。Paused は回数に数えないため、上限を超えても飛ばさない
	tokio::time::advance(wait - JUST_BEFORE).await;
	settle().await;
	env.barrier().await;
	assert_eq!(
		env.resolver.resolve_count(&a),
		1,
		"retry_at の前にやり直さない"
	);
	advance_until("1 曲目の再生", || started(&env.audio, &a)).await;
	env.barrier().await;
	assert_eq!(env.resolver.resolve_count(&a), retries + 2);
	assert_eq!(env.count_notice("play.rate_limited"), 1);
	assert!(!env.has_notice("error.track_skipped"));
	assert!(!env.has_notice("error.rate_limited_stopped"));
}

#[tokio::test]
async fn prefetch_while_paused_does_not_spin() {
	let env = Env::new();
	let (a, b, c) = (vid('a'), vid('b'), vid('c'));
	env.set_playlist(&[&a, &b, &c]);
	let paused = || {
		Err(ResolveError::Paused {
			retry_at: tokio::time::Instant::now() + Duration::from_secs(60),
		})
	};
	// 先読みの 1 回目だけ Paused を返し、その後の解決は成功する
	env.resolver.push_results(&b, vec![paused()]);
	env.resolver.push_results(&c, vec![paused()]);
	let prefetches = |env: &Env| {
		env.resolver
			.resolve_calls()
			.into_iter()
			.filter(|(_, priority)| *priority == Priority::Prefetch)
			.count()
	};

	env.play(PLAYLIST_URL);
	wait_until("2 曲の先読み", || prefetches(&env) == 2).await;
	// 失敗として記録し、取り直さない
	tokio::time::sleep(Duration::from_secs(1)).await;
	env.barrier().await;
	assert_eq!(prefetches(&env), 2);
	assert!(preloads(&env.audio).is_empty());

	// 次の曲へ進むときは通常の解決で取り直す
	end_track(&env, &a).await;
	wait_until("2 曲目の再生", || started(&env.audio, &b)).await;
	env.barrier().await;
	assert!(env
		.resolver
		.resolve_calls()
		.contains(&(b.clone(), Priority::Interactive)));
	assert_eq!(prefetches(&env), 2);
}

#[tokio::test]
async fn track_failure_reresolves_at_current_position() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.audio.set_position(FAILED_AT);

	fail_track(&env, &a).await;
	wait_until("再開", || plays(&env.audio).len() == 2).await;
	env.barrier().await;

	// キャッシュを使わずに同じ形式で取り直し、今の位置から再開する
	assert_eq!(
		env.resolver.refresh_calls(),
		vec![(a.clone(), Some(stream(&a).format_id))]
	);
	let expected = PlayRequest {
		start_at: FAILED_AT,
		..expected_request(&env, &a)
	};
	assert_eq!(last_play(&env.audio), expected);
	assert_eq!(env.count_notice("play.now_playing"), 1);
	assert!(!env.has_notice("play.pipe_fallback"));
	assert!(!env.has_notice("error.track_skipped"));

	// 今の曲でない失敗は無視する
	env.audio
		.emit(AudioEvent::TrackFailed {
			track: watapachi_bot::audio::backend::TrackId(999),
			error: AudioError::Failed("test".to_string()),
		})
		.await;
	env.barrier().await;
	assert_eq!(plays(&env.audio).len(), 2);
	assert_eq!(env.resolver.refresh_calls().len(), 1);
}

#[tokio::test]
async fn second_failure_uses_pipe_from_start_and_notifies() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.audio.set_position(FAILED_AT);

	fail_track(&env, &a).await;
	wait_until("1 回目の再開", || plays(&env.audio).len() == 2).await;
	env.barrier().await;
	fail_track(&env, &a).await;
	wait_until("2 回目の再開", || plays(&env.audio).len() == 3).await;
	env.barrier().await;

	// PipeInput で先頭から再生し、その旨を通知する
	let expected = PlayRequest {
		use_pipe: true,
		paused: false,
		..expected_request(&env, &a)
	};
	assert_eq!(last_play(&env.audio), expected);
	assert_eq!(titles_of(&env, "play.pipe_fallback"), vec![title(&a)]);
	assert_eq!(env.count_notice("play.now_playing"), 1);
	assert!(!env.has_notice("error.track_skipped"));
}

#[tokio::test]
async fn third_failure_skips() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);
	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	env.barrier().await;

	for attempt in 1..=2 {
		fail_track(&env, &a).await;
		wait_until("再開", || {
			plays(&env.audio).iter().filter(|r| r.video_id == a).count() == 1 + attempt
		})
		.await;
		env.barrier().await;
	}
	fail_track(&env, &a).await;
	wait_until("2 曲目の再生", || started(&env.audio, &b)).await;
	env.barrier().await;

	assert_eq!(titles_of(&env, "error.track_skipped"), vec![title(&a)]);
	assert_eq!(env.playback()[0].item.video_id, b);
	assert_eq!(
		titles_of(&env, "play.now_playing"),
		vec![title(&a), title(&b)]
	);
}

/// ループ中に、曲に固有でない理由 (3 回目の再生の失敗) で飛ばした曲は、キューの末尾へ戻し、履歴にも残すこと。
#[tokio::test]
async fn loop_keeps_track_skipped_after_third_failure() {
	let env = Env::new();
	env.write(|c| settings::set_loop(c, guild(), true)).await;
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);
	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	env.barrier().await;

	for attempt in 1..=2 {
		fail_track(&env, &a).await;
		wait_until("再開", || {
			plays(&env.audio).iter().filter(|r| r.video_id == a).count() == 1 + attempt
		})
		.await;
		env.barrier().await;
	}
	fail_track(&env, &a).await;
	wait_until("2 曲目の再生", || started(&env.audio, &b)).await;
	env.barrier().await;

	assert_eq!(titles_of(&env, "error.track_skipped"), vec![title(&a)]);
	assert_eq!(env.queue_ids(), vec![a.clone()]);
	assert_eq!(
		env.history()
			.iter()
			.map(|entry| entry.item.video_id.clone())
			.collect::<Vec<_>>(),
		vec![a.clone()]
	);
}

/// ループ中でも、曲に固有の理由 (動画が無い) で飛ばした曲はキューへ戻さないこと。
#[tokio::test]
async fn loop_drops_track_skipped_for_track_specific_reason() {
	let env = Env::new();
	env.write(|c| settings::set_loop(c, guild(), true)).await;
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);
	env.resolver
		.set_result(&a, Err(worker_error(ResolveErrorCode::NotFound)));
	env.play(PLAYLIST_URL);
	wait_until("2 曲目の再生", || started(&env.audio, &b)).await;
	env.barrier().await;

	assert_eq!(titles_of(&env, "error.video_not_found"), vec![title(&a)]);
	assert!(env.queue_ids().is_empty());
	assert!(env.history().is_empty());
}

#[tokio::test]
async fn disconnect_rejoins_and_resumes_position() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.audio.set_position(LOST_AT);

	lose_connection(&env).await;
	wait_rejoined(&env, 2, 2).await;

	// 位置を保存し、切断してから同じボイスチャンネルへ接続し直し、続きから再生する
	let calls = env.audio.calls();
	let disconnect = calls.iter().position(|c| *c == AudioCall::Disconnect);
	let rejoin = calls
		.iter()
		.rposition(|c| matches!(c, AudioCall::Connect { .. }));
	assert!(disconnect.unwrap() < rejoin.unwrap());
	assert_eq!(
		calls[rejoin.unwrap()],
		AudioCall::Connect {
			guild: guild(),
			channel: voice(),
		}
	);
	let expected = PlayRequest {
		start_at: LOST_AT,
		..expected_request(&env, &a)
	};
	assert_eq!(last_play(&env.audio), expected);
	wait_until("位置の保存", || {
		env.playback()
			.first()
			.is_some_and(|s| s.position_ms == LOST_AT.as_millis() as u64)
	})
	.await;
	// 自動の回復は、上限に達するまで通知しない
	assert_eq!(env.count_notice("play.now_playing"), 1);
	assert!(!env.has_notice("control.rejoined"));
	assert!(!env.has_notice("error.rejoin_limit"));
}

#[tokio::test(start_paused = true)]
async fn unhealthy_verdict_triggers_rejoin() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	start_track(&env, &a).await;
	env.audio.set_position(LOST_AT);
	// 暗号化も未暗号化も増えない (送信なし)
	send_no_frames(&env);
	let health = &env.config.health;
	let interval = Duration::from_secs(health.check_interval_secs);
	// 猶予の間の確認と、異常が続く回数の確認
	let ticks =
		health.dave_grace_secs / health.check_interval_secs + u64::from(health.unhealthy_checks);

	for _ in 1..ticks {
		tokio::time::advance(interval).await;
		settle().await;
		env.barrier().await;
		assert_eq!(
			connects(&env.audio),
			1,
			"異常が続く回数に達するまで回復しない"
		);
	}
	tokio::time::advance(interval).await;
	wait_rejoined(&env, 2, 2).await;

	assert!(env.audio.calls().contains(&AudioCall::Disconnect));
	let expected = PlayRequest {
		start_at: LOST_AT,
		..expected_request(&env, &a)
	};
	assert_eq!(last_play(&env.audio), expected);
}

#[tokio::test(start_paused = true)]
async fn health_is_not_judged_until_track_started() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.audio.set_position(LOST_AT);
	send_no_frames(&env);
	let health = &env.config.health;
	let interval = Duration::from_secs(health.check_interval_secs);

	// 曲の開始が届かない間 (入力の準備中) は、送信が無くても回復しない
	let waiting = START_DELAY.as_secs() / health.check_interval_secs;
	for _ in 0..waiting {
		tokio::time::advance(interval).await;
		settle().await;
		env.barrier().await;
		assert_eq!(connects(&env.audio), 1, "開始の前は判定しない");
	}

	// 開始が届いた後は、そこから猶予と異常の回数を数えて回復する
	start_track(&env, &a).await;
	let ticks =
		health.dave_grace_secs / health.check_interval_secs + u64::from(health.unhealthy_checks);
	for _ in 1..ticks {
		tokio::time::advance(interval).await;
		settle().await;
		env.barrier().await;
		assert_eq!(connects(&env.audio), 1, "猶予と異常の回数は開始から数える");
	}
	tokio::time::advance(interval).await;
	wait_rejoined(&env, 2, 2).await;
	assert!(env.audio.calls().contains(&AudioCall::Disconnect));
}

#[tokio::test(start_paused = true)]
async fn auto_rejoin_limit_pauses_and_suggests_rejoin() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	let max = env.config.health.auto_rejoin_max as usize;

	for done in 1..=max {
		lose_connection(&env).await;
		wait_rejoined(&env, 1 + done, 1 + done).await;
	}
	// 上限を超えたら回復せず、一時停止して ?rejoin を案内する
	lose_connection(&env).await;
	wait_until("上限の通知", || env.has_notice("error.rejoin_limit")).await;
	env.barrier().await;
	assert_eq!(connects(&env.audio), 1 + max);
	assert_eq!(env.audio.calls().last(), Some(&AudioCall::Pause));
	assert_eq!(
		env.notice_args("error.rejoin_limit"),
		Some(vec![("prefix", env.config.bot.prefix.clone())])
	);
	env.send(GuildCommand::Resume);
	wait_until("再開の通知", || env.has_notice("control.resumed")).await;

	// 期間を過ぎた回復は数えない
	tokio::time::advance(Duration::from_secs(
		env.config.health.auto_rejoin_window_secs,
	))
	.await;
	settle().await;
	lose_connection(&env).await;
	wait_rejoined(&env, 2 + max, 2 + max).await;
	assert_eq!(env.count_notice("error.rejoin_limit"), 1);
}

#[tokio::test]
async fn manual_rejoin_ignores_limit() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	let max = env.config.health.auto_rejoin_max as usize;
	for done in 1..=max {
		lose_connection(&env).await;
		wait_rejoined(&env, 1 + done, 1 + done).await;
	}
	lose_connection(&env).await;
	wait_until("上限の通知", || env.has_notice("error.rejoin_limit")).await;
	env.barrier().await;

	env.audio.set_position(LOST_AT);
	for done in 1..=2 {
		env.send(GuildCommand::Rejoin);
		wait_rejoined(&env, 1 + max + done, 1 + max + done).await;
		assert_eq!(env.count_notice("control.rejoined"), done);
	}
	let expected = PlayRequest {
		start_at: LOST_AT,
		..expected_request(&env, &a)
	};
	assert_eq!(last_play(&env.audio), expected);
	assert_eq!(env.count_notice("error.rejoin_limit"), 1);
}

#[tokio::test]
async fn commands_during_recovering_do_not_double_join() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	play_one(&env, &a).await;
	env.audio.set_position(LOST_AT);
	env.audio.hold_connect();

	env.send(GuildCommand::Rejoin);
	wait_until("再接続の開始", || connects(&env.audio) == 2).await;
	env.barrier().await;

	// 回復中の ?rejoin、切断、操作コマンドは接続を増やさない。?p はキューへ入れるだけ
	env.send(GuildCommand::Rejoin);
	lose_connection(&env).await;
	env.send(GuildCommand::Pause);
	env.send(GuildCommand::Skip);
	env.set_videos(&[&b]);
	env.play(&watch_url(&b));
	wait_until("キューへの追加", || {
		env.queue_ids() == vec![b.clone()]
	})
	.await;
	env.barrier().await;
	assert_eq!(connects(&env.audio), 2);
	assert_eq!(env.count_notice("error.recovering"), 3);
	assert!(!env.audio.calls().contains(&AudioCall::Pause));
	assert_eq!(plays(&env.audio).len(), 1);
	assert_eq!(env.playback()[0].item.video_id, a);

	env.audio.release_connect();
	wait_rejoined(&env, 2, 2).await;
	let expected = PlayRequest {
		start_at: LOST_AT,
		..expected_request(&env, &a)
	};
	assert_eq!(last_play(&env.audio), expected);
	assert_eq!(env.count_notice("control.rejoined"), 1);
	assert_eq!(env.queue_ids(), vec![b]);
}

#[tokio::test]
async fn songbird_timeout_is_treated_as_disconnect() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;

	env.audio.fail_next(AudioOp::Pause, AudioError::Timeout);
	env.send(GuildCommand::Pause);
	wait_rejoined(&env, 2, 2).await;

	assert!(env.has_notice("error.audio_failed"));
	assert!(env.audio.calls().contains(&AudioCall::Disconnect));
	assert_eq!(last_play(&env.audio).video_id, a);
	// 回復の後は再生中として扱う
	env.send(GuildCommand::Pause);
	wait_until("一時停止の通知", || env.has_notice("control.paused")).await;

	// 再生の開始の時間切れでも、曲は飛ばさずに回復の後で同じ曲を再生する
	let env = Env::new();
	env.set_videos(&[&a]);
	env.audio.fail_next(AudioOp::Play, AudioError::Timeout);
	env.play(&watch_url(&a));
	wait_rejoined(&env, 2, 2).await;
	assert!(plays(&env.audio).iter().all(|r| r.video_id == a));
	assert!(!env.has_notice("error.track_skipped"));
	assert_eq!(titles_of(&env, "play.now_playing"), vec![title(&a)]);
}

#[tokio::test]
async fn kicked_disconnect_ends_session() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);
	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	env.barrier().await;

	env.audio
		.emit(AudioEvent::Disconnected {
			reason: DisconnectKind::Kicked,
		})
		.await;
	wait_until("切断の通知", || env.has_notice("control.kicked")).await;
	env.barrier().await;

	// ?leave と同じく、キューと再生状態を消して接続し直さない
	assert!(disconnected(&env.audio));
	assert_eq!(connects(&env.audio), 1);
	assert!(env.queue_ids().is_empty());
	assert!(env.playback().is_empty());
}

#[tokio::test]
async fn rejoin_when_not_connected_notifies_error() {
	let env = Env::new();
	env.send(GuildCommand::Rejoin);
	wait_until("未接続の通知", || {
		env.has_notice("error.not_connected")
	})
	.await;
	env.barrier().await;
	assert!(env.audio.calls().is_empty());
}

#[tokio::test]
async fn leave_during_recovering_cancels_rejoin() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.audio.hold_connect();
	env.send(GuildCommand::Rejoin);
	wait_until("再接続の開始", || connects(&env.audio) == 2).await;
	env.barrier().await;

	env.send(GuildCommand::Leave);
	wait_until("退出の通知", || env.has_notice("control.left")).await;
	env.audio.release_connect();
	env.barrier().await;
	settle().await;
	env.barrier().await;

	// 退出で再接続の処理を止めるので、保留を解いても再生しない
	assert_eq!(plays(&env.audio).len(), 1);
	assert!(env.playback().is_empty());
	assert!(!env.has_notice("control.rejoined"));
}

/// 回復で前のセッションを切断している間に退出したら、切断の後に入り直さないこと。
#[tokio::test]
async fn leave_while_recovery_disconnects_does_not_rejoin() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.audio.hold_disconnect();
	env.send(GuildCommand::Rejoin);
	env.barrier().await;
	settle().await;
	env.barrier().await;
	// 前のセッションの切断を保留しているため、まだ接続し直していない
	assert_eq!(connects(&env.audio), 1);

	env.send(GuildCommand::Leave);
	wait_until("退出の通知", || env.has_notice("control.left")).await;
	env.audio.release_disconnect();
	env.barrier().await;
	settle().await;
	env.barrier().await;

	assert_eq!(connects(&env.audio), 1, "退出の後に入り直した");
	assert_eq!(plays(&env.audio).len(), 1);
	assert!(!env.has_notice("control.rejoined"));
}

fn pauses(audio: &FakeAudio) -> usize {
	count_calls(audio, |c| *c == AudioCall::Pause)
}

#[tokio::test(start_paused = true)]
async fn rate_limit_wait_survives_recovery() {
	let env = Env::new();
	let a = vid('a');
	env.set_playlist(&[&a]);
	env.resolver
		.push_results(&a, vec![Err(worker_error(ResolveErrorCode::RateLimited))]);
	env.play(PLAYLIST_URL);
	wait_until("待機の通知", || env.has_notice("play.rate_limited")).await;
	env.barrier().await;

	// 待ちの間に回復しても、再接続の直後に解決し直さない
	env.send(GuildCommand::Rejoin);
	wait_until("再接続", || env.has_notice("control.rejoined")).await;
	env.barrier().await;
	assert_eq!(connects(&env.audio), 2);
	assert_eq!(env.resolver.resolve_count(&a), 1);

	// 残りの待ち時間を守ってからやり直す
	let delay = backoff(&env, 1);
	tokio::time::advance(delay - JUST_BEFORE).await;
	settle().await;
	env.barrier().await;
	assert_eq!(
		env.resolver.resolve_count(&a),
		1,
		"待ち時間の前にやり直さない"
	);
	tokio::time::advance(JUST_BEFORE).await;
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	assert_eq!(env.resolver.resolve_count(&a), 2);
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &a)]);
}

#[tokio::test]
async fn auto_recovery_keeps_paused() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.send(GuildCommand::Pause);
	wait_until("一時停止の通知", || env.has_notice("control.paused")).await;
	env.audio.set_position(LOST_AT);

	lose_connection(&env).await;
	wait_rejoined(&env, 2, 2).await;

	// 続きの位置で、一時停止のまま作り直す (始めてから止めない)。一時停止中として保存する
	let expected = PlayRequest {
		start_at: LOST_AT,
		paused: true,
		..expected_request(&env, &a)
	};
	assert_eq!(last_play(&env.audio), expected);
	assert_eq!(pauses(&env.audio), 1);
	wait_until("位置の保存", || {
		env.playback()
			.first()
			.is_some_and(|s| s.position_ms == LOST_AT.as_millis() as u64 && s.paused)
	})
	.await;
	assert_eq!(env.count_notice("play.now_playing"), 1);
	assert_eq!(env.count_notice("control.paused"), 1);

	// 一時停止中として扱う
	env.send(GuildCommand::Resume);
	wait_until("再開の通知", || env.has_notice("control.resumed")).await;
}

#[tokio::test]
async fn track_failure_while_paused_stays_paused() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.send(GuildCommand::Pause);
	wait_until("一時停止の通知", || env.has_notice("control.paused")).await;
	env.audio.set_position(FAILED_AT);

	fail_track(&env, &a).await;
	wait_until("作り直し", || plays(&env.audio).len() == 2).await;
	env.barrier().await;

	assert_eq!(last_play(&env.audio).start_at, FAILED_AT);
	assert!(
		last_play(&env.audio).paused,
		"一時停止のまま作り直していない"
	);
	assert_eq!(pauses(&env.audio), 1);
	env.send(GuildCommand::Resume);
	wait_until("再開の通知", || env.has_notice("control.resumed")).await;
}

#[tokio::test]
async fn manual_rejoin_while_paused_resumes() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.send(GuildCommand::Pause);
	wait_until("一時停止の通知", || env.has_notice("control.paused")).await;
	env.audio.set_position(LOST_AT);

	// ?rejoin は一時停止中でも続きから再生する
	env.send(GuildCommand::Rejoin);
	wait_rejoined(&env, 2, 2).await;
	assert_eq!(last_play(&env.audio).start_at, LOST_AT);
	assert_eq!(pauses(&env.audio), 1);
	env.send(GuildCommand::Pause);
	wait_until("2 回目の一時停止", || {
		env.count_notice("control.paused") == 2
	})
	.await;
}

/// 時間を止めた試験で、条件が成り立つまで仮想時間を少しずつ進める。
/// - やり直しの待ち (倍々で最大 `rate_limit_backoff_max_secs`) をまたぐため、1 秒ずつ進めて毎回譲る
async fn advance_until(what: &str, mut cond: impl FnMut() -> bool) {
	const STEP: Duration = Duration::from_secs(1);
	const ROUNDS: u32 = 3600;
	for _ in 0..ROUNDS {
		if cond() {
			return;
		}
		tokio::time::advance(STEP).await;
		settle().await;
	}
	panic!("仮想時間を進めても待ち切れません: {what}");
}

/// 利用者向けのエラーの通知 (目印の `error.not_in_voice` を除く) の数。
fn error_notices(env: &Env) -> usize {
	env.notice_keys()
		.into_iter()
		.filter(|key| key.starts_with("error.") && *key != "error.not_in_voice")
		.count()
}

#[tokio::test(start_paused = true)]
async fn dead_pool_stops_after_skip_streak_limit() {
	let env = Env::new();
	let ids: Vec<VideoId> = "abcdefg".chars().map(vid).collect();
	env.set_playlist(&ids.iter().collect::<Vec<_>>());
	// 起動できるワーカーが無いプールは、すぐ Unavailable を返し続ける
	for id in &ids {
		env.resolver.set_result(id, Err(ResolveError::Unavailable));
	}
	let limit = env.config.playback.skip_streak_limit as usize;
	let retries = env.config.worker.max_retries as usize;

	env.play(PLAYLIST_URL);
	advance_until("再生の停止の案内", || {
		env.has_notice("error.fetch_stopped")
	})
	.await;
	env.barrier().await;

	// 上限の 1 つ手前までは通常どおり飛ばし、上限の曲は飛ばさずにキューの先頭へ戻す
	let skipped: Vec<String> = ids[..limit - 1].iter().map(title).collect();
	assert_eq!(titles_of(&env, "error.track_skipped"), skipped);
	assert_eq!(env.queue_ids(), ids[limit - 1..].to_vec());
	assert!(env.playback().is_empty());
	assert!(disconnected(&env.audio));
	assert!(plays(&env.audio).is_empty());
	for id in &ids[..limit] {
		assert_eq!(env.resolver.resolve_count(id), 1 + retries, "{id:?}");
	}
	assert_eq!(env.resolver.resolve_count(&ids[limit]), 0);
	assert_eq!(
		env.notice_args("error.fetch_stopped"),
		Some(vec![("prefix", env.config.bot.prefix.clone())])
	);

	// 止めた後は解決も通知も増えない
	let errors = error_notices(&env);
	assert_eq!(errors, limit);
	for _ in 0..3 {
		tokio::time::advance(Duration::from_secs(
			env.config.worker.rate_limit_backoff_max_secs,
		))
		.await;
		settle().await;
	}
	env.barrier().await;
	assert_eq!(error_notices(&env), errors);
	assert_eq!(env.count_notice("error.fetch_stopped"), 1);
	assert_eq!(env.resolver.resolve_count(&ids[limit]), 0);
}

#[tokio::test(start_paused = true)]
async fn skip_streak_resets_when_a_track_plays() {
	let env = Env::new();
	let limit = env.config.playback.skip_streak_limit as usize;
	// 上限の 1 つ手前まで失敗し、1 曲再生し、また上限の 1 つ手前まで失敗する
	let failing_before: Vec<VideoId> = (0..limit - 1)
		.map(|n| vid(char::from(b'a' + n as u8)))
		.collect();
	let playing = vid('p');
	let failing_after: Vec<VideoId> = (0..limit - 1)
		.map(|n| vid(char::from(b'q' + n as u8)))
		.collect();
	let mut ids = failing_before.clone();
	ids.push(playing.clone());
	ids.extend(failing_after.iter().cloned());
	env.set_playlist(&ids.iter().collect::<Vec<_>>());
	for id in failing_before.iter().chain(&failing_after) {
		env.resolver.set_result(id, Err(ResolveError::WorkerDied));
	}

	env.play(PLAYLIST_URL);
	advance_until("間の曲の再生", || started(&env.audio, &playing)).await;
	env.barrier().await;
	end_track(&env, &playing).await;
	advance_until("キューを使い切った退出", || {
		disconnected(&env.audio)
	})
	.await;
	env.barrier().await;

	// 再生が始まると数え直すため、後半の失敗では止めずに飛ばし切る
	assert!(!env.has_notice("error.fetch_stopped"));
	let skipped: Vec<String> = failing_before
		.iter()
		.chain(&failing_after)
		.map(title)
		.collect();
	assert_eq!(titles_of(&env, "error.track_skipped"), skipped);
	assert!(env.queue_ids().is_empty());
}

/// 再接続の失敗の後の状態を確かめる。キューを残し、今の曲を先頭へ戻し、再生状態を消して退出している。
fn assert_left_keeping_queue(env: &Env, expected_queue: &[VideoId]) {
	assert_eq!(env.queue_ids(), expected_queue.to_vec());
	assert!(env.playback().is_empty());
	assert_eq!(
		env.notice_args("error.rejoin_failed"),
		Some(vec![("prefix", env.config.bot.prefix.clone())])
	);
	let channel = env
		.notices()
		.into_iter()
		.find(|(_, n)| n.key == "error.rejoin_failed")
		.map(|(channel, _)| channel);
	assert_eq!(channel, Some(text()));
}

#[tokio::test]
async fn failed_auto_rejoin_keeps_queue() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);
	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	env.barrier().await;

	env.audio
		.fail_next(AudioOp::Connect, AudioError::Failed("test".to_string()));
	lose_connection(&env).await;
	wait_until("再接続の失敗の通知", || {
		env.has_notice("error.rejoin_failed")
	})
	.await;
	env.barrier().await;
	assert_left_keeping_queue(&env, &[a.clone(), b.clone()]);
	assert_eq!(connects(&env.audio), 2);

	// 次の ?p で、戻した曲から再生する
	let c = vid('c');
	env.set_videos(&[&c]);
	env.play(&watch_url(&c));
	wait_until("戻した曲の再生", || {
		plays(&env.audio).iter().filter(|r| r.video_id == a).count() == 2
	})
	.await;
}

#[tokio::test]
async fn failed_manual_rejoin_keeps_queue() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);
	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	env.barrier().await;

	env.audio
		.fail_next(AudioOp::Connect, AudioError::Failed("test".to_string()));
	env.send(GuildCommand::Rejoin);
	wait_until("再接続の失敗の通知", || {
		env.has_notice("error.rejoin_failed")
	})
	.await;
	env.barrier().await;
	assert_left_keeping_queue(&env, &[a.clone(), b.clone()]);
	assert!(!env.has_notice("control.rejoined"));
}

/// 自動の回復の上限で一時停止した後の `?rejoin` は、応答しないセッションへ位置を問い合わせないこと。
///
/// - 問い合わせると、壊れたセッションでは `voice.call_timeout_secs` まで応答しない
/// - 位置は一時停止したときの見積もりを使う。再接続の後のセッションには問い合わせる
#[tokio::test]
async fn rejoin_after_limit_does_not_query_broken_session() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	let max = env.config.health.auto_rejoin_max as usize;
	for done in 1..=max {
		lose_connection(&env).await;
		wait_rejoined(&env, 1 + done, 1 + done).await;
	}
	lose_connection(&env).await;
	wait_until("上限の通知", || env.has_notice("error.rejoin_limit")).await;
	env.barrier().await;

	env.audio.set_position(LOST_AT);
	let positions = count_calls(&env.audio, |c| *c == AudioCall::Position);
	env.send(GuildCommand::Rejoin);
	wait_rejoined(&env, 2 + max, 2 + max).await;
	assert_eq!(
		count_calls(&env.audio, |c| *c == AudioCall::Position),
		positions,
		"壊れたセッションへ位置を問い合わせた"
	);
	assert!(last_play(&env.audio).start_at < LOST_AT);
	// 切断してから接続し直す (同じタスクで順に行う)
	let calls = env.audio.calls();
	let disconnect = calls.iter().rposition(|c| *c == AudioCall::Disconnect);
	let rejoin = calls
		.iter()
		.rposition(|c| matches!(c, AudioCall::Connect { .. }));
	assert!(disconnect.unwrap() < rejoin.unwrap());

	// 再接続の後のセッションには問い合わせる
	env.send(GuildCommand::Rejoin);
	wait_rejoined(&env, 3 + max, 3 + max).await;
	assert_eq!(last_play(&env.audio).start_at, LOST_AT);
}

/// `?move` の後は、健全性の猶予を移動した時刻から数え直すこと。
///
/// - 移動先では送信の暗号化が始め直されるため、曲の開始から猶予を過ぎていても、移動の直後は判定しない
#[tokio::test(start_paused = true)]
async fn move_restarts_health_grace() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	start_track(&env, &a).await;
	let health = &env.config.health;
	let interval = Duration::from_secs(health.check_interval_secs);
	let grace_ticks = health.dave_grace_secs / health.check_interval_secs;
	// 曲の開始からの猶予を、健全な送信のまま過ぎる
	for _ in 0..=grace_ticks {
		tokio::time::advance(interval).await;
		settle().await;
		env.barrier().await;
	}
	assert_eq!(connects(&env.audio), 1);

	let moved_to = ChannelId::new(VOICE + 1);
	env.send_from(GuildCommand::Move, Some(moved_to));
	wait_until("移動の通知", || env.has_notice("control.moved")).await;
	env.barrier().await;
	send_no_frames(&env);
	let ticks = grace_ticks + u64::from(health.unhealthy_checks);
	for _ in 1..ticks {
		tokio::time::advance(interval).await;
		settle().await;
		env.barrier().await;
		assert_eq!(connects(&env.audio), 1, "猶予と異常の回数は移動から数える");
	}
	tokio::time::advance(interval).await;
	wait_until("移動先への再接続", || connects(&env.audio) == 2).await;
	assert_eq!(
		env.audio
			.calls()
			.into_iter()
			.rfind(|c| matches!(c, AudioCall::Connect { .. })),
		Some(AudioCall::Connect {
			guild: guild(),
			channel: moved_to,
		})
	);
}

/// 残りの曲がどれも飛ばされてキューが空になると、飛ばしの通知の後に再生の終了を通知すること。
#[tokio::test]
async fn skipped_last_track_notifies_completed() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);
	env.resolver
		.set_result(&b, Err(worker_error(ResolveErrorCode::NotFound)));

	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	end_track(&env, &a).await;
	wait_until("退出", || disconnected(&env.audio)).await;
	wait_until("再生の終了の通知", || {
		env.has_notice("play.completed")
	})
	.await;

	let keys = env.notice_keys();
	let skipped = keys
		.iter()
		.position(|k| *k == "error.video_not_found")
		.unwrap();
	let completed = keys.iter().position(|k| *k == "play.completed").unwrap();
	assert!(skipped < completed);
	assert!(!started(&env.audio, &b));
}

#[tokio::test]
async fn rejoin_denied_uses_rejoin_failed() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);
	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	env.barrier().await;

	env.access.set(AccessVerdict::Denied(Denial::Full));
	env.send(GuildCommand::Rejoin);
	wait_until("再接続の失敗の通知", || {
		env.has_notice("error.rejoin_failed")
	})
	.await;
	env.barrier().await;
	assert_left_keeping_queue(&env, &[a.clone(), b.clone()]);
	assert!(!env.has_notice("error.voice_full"));
	assert_eq!(connects(&env.audio), 1);
}
