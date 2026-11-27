//! GuildActor の操作コマンド (`?pause` から `?leave`、自動退出) の試験。
//! - 環境と待ちの補助は `support/actor_env.rs` にある。音声、解決、通知は偽実装、DB は一時ディレクトリの実 DB
//! - 「起きないこと」は目印 (`Env::barrier`) で、それより前のメッセージの処理を終えたことを確かめてから見る
//! - 時間を止めた試験では、試験側は tokio のタイマーを待たず、`tokio::time::advance` だけで時間を進める

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
use watapachi_bot::audio::access::{AccessVerdict, Denial, VoicePermission};
use watapachi_bot::audio::backend::{AudioError, AudioEvent, TrackId};
use watapachi_bot::db::settings::{self, GuildSettings};
use watapachi_bot::domain::{ChannelId, Priority, ResolveError, ResolveErrorCode, VideoId};
use watapachi_bot::guild::message::{ClearRange, GuildCommand, GuildMessage};
use watapachi_bot::guild::registry::{GuildRegistry, SendError};

/// `?move` の移動先のボイスチャンネル。
const OTHER_VOICE: u64 = 21;
/// 時間を止めた試験で、自動退出の時刻の直前を見るために引く時間。
const JUST_BEFORE: Duration = Duration::from_secs(1);
/// 自動退出しないことを確かめるために進める時間。どの設定値よりも十分に長い。
const LONG_WAIT: Duration = Duration::from_secs(3600);

fn other_voice() -> ChannelId {
	ChannelId::new(OTHER_VOICE)
}

/// 呼び出しが最初に記録された位置。
fn position_of(audio: &FakeAudio, call: &AudioCall) -> Option<usize> {
	audio.calls().iter().position(|c| c == call)
}

/// 条件に合う呼び出しの数。
fn count_calls(audio: &FakeAudio, pred: impl Fn(&AudioCall) -> bool) -> usize {
	audio.calls().iter().filter(|c| pred(c)).count()
}

fn stops(audio: &FakeAudio) -> usize {
	count_calls(audio, |c| *c == AudioCall::Stop)
}

fn connects(audio: &FakeAudio) -> usize {
	count_calls(audio, |c| matches!(c, AudioCall::Connect { .. }))
}

fn disconnects(audio: &FakeAudio) -> usize {
	count_calls(audio, |c| *c == AudioCall::Disconnect)
}

/// DB に保存されたギルドの設定。
fn saved_settings(env: &Env) -> GuildSettings {
	settings::load(&env.conn(), guild(), &env.config.defaults).unwrap()
}

/// 自動退出までの既定の秒数。
fn alone_timeout(env: &Env) -> Duration {
	Duration::from_secs(u64::from(env.config.defaults.alone_timeout_secs))
}

/// 動画 URL の `?p` で 1 曲を再生し、再生の開始まで処理させる。
async fn play_one(env: &Env, id: &VideoId) {
	env.set_videos(&[id]);
	env.play(&watch_url(id));
	wait_until("再生の開始", || started(&env.audio, id)).await;
	env.barrier().await;
}

/// プレイリストで再生を始め、1 曲目の再生と残りの追加まで処理させる。
async fn start_playlist(env: &Env, ids: &[&VideoId]) {
	env.set_playlist(ids);
	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, ids[0])).await;
	wait_until("プレイリストの追加", || {
		env.has_notice("play.playlist_queued")
	})
	.await;
	env.barrier().await;
}

/// 指定の曲が `preload` で準備されるまで待ち、その識別子を返す。
async fn wait_preloaded(env: &Env, id: &VideoId) -> TrackId {
	wait_until("次の曲の準備", || {
		preloads(&env.audio).iter().any(|r| r.video_id == *id)
	})
	.await;
	env.barrier().await;
	track_for(&env.audio, id).unwrap()
}

#[tokio::test]
async fn pause_and_resume_update_state() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;

	env.send(GuildCommand::Pause);
	wait_until("一時停止の通知", || env.has_notice("control.paused")).await;
	assert!(env.audio.calls().contains(&AudioCall::Pause));

	// 一時停止中の ?pause は何もしない
	env.send(GuildCommand::Pause);
	wait_until("一時停止中の通知", || {
		env.has_notice("error.already_paused")
	})
	.await;
	assert_eq!(count_calls(&env.audio, |c| *c == AudioCall::Pause), 1);

	env.send(GuildCommand::Resume);
	wait_until("再開の通知", || env.has_notice("control.resumed")).await;
	assert!(env.audio.calls().contains(&AudioCall::Resume));

	// 再生中の ?resume は何もしない
	env.send(GuildCommand::Resume);
	wait_until("一時停止していない通知", || {
		env.has_notice("error.not_paused")
	})
	.await;
	env.barrier().await;
	assert_eq!(count_calls(&env.audio, |c| *c == AudioCall::Resume), 1);
	assert!(env.notices().iter().all(|(channel, _)| *channel == text()));
	assert_eq!(env.playback()[0].item.video_id, a);
	assert!(!disconnected(&env.audio));
}

#[tokio::test]
async fn pause_when_idle_notifies_error() {
	let env = Env::new();

	let needs_playback = [
		GuildCommand::Pause,
		GuildCommand::Resume,
		GuildCommand::Skip,
		GuildCommand::Replay,
		GuildCommand::PlayNow { index: 1 },
	];
	let count = needs_playback.len();
	for command in needs_playback {
		env.send(command);
	}
	env.send(GuildCommand::Move);
	env.send(GuildCommand::Leave);
	wait_until("エラーの通知", || {
		env.count_notice("error.nothing_playing") == count
			&& env.count_notice("error.not_connected") == 2
	})
	.await;
	env.barrier().await;

	assert!(env.audio.calls().is_empty());
	assert!(env.resolver.resolve_calls().is_empty());
	assert!(env.playback().is_empty());
	assert!(env.queue_ids().is_empty());
}

#[tokio::test]
async fn skip_starts_next() {
	let env = Env::new();
	let (a, b, c) = (vid('a'), vid('b'), vid('c'));
	start_playlist(&env, &[&a, &b, &c]).await;
	let track_b = wait_preloaded(&env, &b).await;

	env.send(GuildCommand::Skip);
	wait_until("2 曲目の開始", || {
		env.audio
			.calls()
			.contains(&AudioCall::StartPreloaded(track_b))
	})
	.await;
	wait_until("2 曲目の通知", || {
		env.count_notice("play.now_playing") == 2
	})
	.await;

	// 今の曲を止めてから、準備済みの次の曲を始める
	let stop = position_of(&env.audio, &AudioCall::Stop).unwrap();
	let start = position_of(&env.audio, &AudioCall::StartPreloaded(track_b)).unwrap();
	assert!(stop < start);
	assert_eq!(
		env.notice_args("control.skipped"),
		Some(vec![("title", title(&a))])
	);
	assert_eq!(
		env.notice_args("play.now_playing"),
		Some(vec![("title", title(&b))])
	);
	assert_eq!(env.playback()[0].item.video_id, b);
	assert_eq!(env.queue_ids(), vec![c.clone()]);
	let history = env.history();
	assert_eq!(history.len(), 1);
	assert_eq!(history[0].item.video_id, a);

	// 止めた曲の終了が遅れて届いても、次の曲へは進まない
	end_track(&env, &a).await;
	env.barrier().await;
	assert_eq!(env.playback()[0].item.video_id, b);
	assert_eq!(env.queue_ids(), vec![c.clone()]);
	assert!(!started(&env.audio, &c));
}

/// 最後の曲を `?s` で飛ばすと、飛ばしの通知の後に再生の終了を通知して退出すること。
#[tokio::test]
async fn skip_last_track_notifies_skipped_then_completed() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);
	env.play(&watch_url(&a));
	wait_until("再生の開始", || started(&env.audio, &a)).await;
	env.barrier().await;

	env.send(GuildCommand::Skip);
	wait_until("退出", || disconnected(&env.audio)).await;
	wait_until("再生の終了の通知", || {
		env.has_notice("play.completed")
	})
	.await;

	let keys = env.notice_keys();
	let skipped = keys.iter().position(|k| *k == "control.skipped").unwrap();
	let completed = keys.iter().position(|k| *k == "play.completed").unwrap();
	assert!(skipped < completed);
	assert_eq!(env.count_notice("play.completed"), 1);
}

#[tokio::test]
async fn skip_during_resolve_cancels_it() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_videos(&[&a, &b]);
	env.resolver.set_search("song", Ok(meta(&a)));
	env.resolver.set_search("next", Ok(meta(&b)));
	env.resolver.hold(&a);
	env.resolver.hold(&b);

	env.play("song");
	wait_until("1 曲目の解決の開始", || {
		env.resolver
			.resolve_calls()
			.contains(&(a.clone(), Priority::Interactive))
	})
	.await;
	env.play("next");
	wait_until("2 曲目の追加", || env.has_notice("play.queued")).await;

	// 解決中の曲は再生を始めていないので、一時停止できない
	env.send(GuildCommand::Pause);
	wait_until("再生中でない通知", || {
		env.has_notice("error.nothing_playing")
	})
	.await;

	env.send(GuildCommand::Skip);
	wait_until("2 曲目の解決の開始", || {
		env.resolver
			.resolve_calls()
			.contains(&(b.clone(), Priority::Interactive))
	})
	.await;
	env.barrier().await;

	// 次の曲も解決中のうちに、飛ばした曲の解決が届いても、その曲を再生しない
	env.resolver.release(&a);
	env.barrier().await;
	assert!(plays(&env.audio).is_empty());

	env.resolver.release(&b);
	wait_until("2 曲目の再生", || started(&env.audio, &b)).await;
	env.barrier().await;
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &b)]);
	assert_eq!(stops(&env.audio), 0);
	assert_eq!(
		env.notice_args("control.skipped"),
		Some(vec![("title", title(&a))])
	);
	assert_eq!(env.playback()[0].item.video_id, b);
	assert!(env.history().is_empty());
	assert!(env.queue_ids().is_empty());
}

#[tokio::test]
async fn leave_during_resolve_does_not_start_playback() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_videos(&[&a, &b]);

	// 今の曲の解決中に退出する
	env.resolver.set_search("song", Ok(meta(&a)));
	env.resolver.hold(&a);
	env.play("song");
	wait_until("解決の開始", || {
		env.resolver
			.resolve_calls()
			.contains(&(a.clone(), Priority::Interactive))
	})
	.await;
	env.send(GuildCommand::Leave);
	wait_until("退出の通知", || env.has_notice("control.left")).await;
	env.resolver.release(&a);
	env.barrier().await;
	assert!(plays(&env.audio).is_empty());
	assert_eq!(disconnects(&env.audio), 1);
	assert!(env.playback().is_empty());

	// ?p の問い合わせの途中で退出する
	env.resolver.hold(&b);
	env.play(&watch_url(&b));
	wait_until("2 回目の接続と問い合わせ", || {
		connects(&env.audio) == 2
			&& env
				.resolver
				.resolve_calls()
				.contains(&(b.clone(), Priority::Interactive))
	})
	.await;
	env.barrier().await;
	env.send(GuildCommand::Leave);
	wait_until("2 回目の退出の通知", || {
		env.count_notice("control.left") == 2
	})
	.await;
	env.resolver.release(&b);
	env.barrier().await;

	assert!(plays(&env.audio).is_empty());
	assert_eq!(disconnects(&env.audio), 2);
	assert!(env.queue_ids().is_empty());
	assert!(env.playback().is_empty());
	assert!(!env.has_notice("play.now_playing"));
}

#[tokio::test]
async fn leave_clears_queue_and_playback_state() {
	let env = Env::new();
	let (a, b, c, d) = (vid('a'), vid('b'), vid('c'), vid('d'));
	start_playlist(&env, &[&a, &b, &c]).await;
	let track_b = wait_preloaded(&env, &b).await;
	assert_eq!(env.playback().len(), 1);
	assert_eq!(env.queue_ids(), vec![b.clone(), c.clone()]);

	env.send(GuildCommand::Leave);
	wait_until("退出の通知", || env.has_notice("control.left")).await;

	assert!(disconnected(&env.audio));
	// 準備済みの曲を捨ててから切断する
	let dropped = position_of(&env.audio, &AudioCall::DropPreloaded(track_b)).unwrap();
	let disconnect = position_of(&env.audio, &AudioCall::Disconnect).unwrap();
	assert!(dropped < disconnect);
	assert!(env.queue_ids().is_empty());
	assert!(env.playback().is_empty());
	assert_eq!(env.notice_keys().last(), Some(&"control.left"));

	// 次の ?p は新しいセッションで、前のキューを再生しない
	play_one(&env, &d).await;
	assert_eq!(
		plays(&env.audio),
		vec![expected_request(&env, &a), expected_request(&env, &d)]
	);
	assert!(env.queue_ids().is_empty());
}

/// `?replay` は保存した位置と一時停止の状態を先頭・再生中に書き直すこと
/// (次の定期保存の前に再起動しても、前の位置や一時停止で復元しない)。
#[tokio::test]
async fn replay_resets_saved_position() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.audio.set_position(Duration::from_secs(30));
	env.send(GuildCommand::Pause);
	wait_until("一時停止の保存", || {
		env.playback()
			.first()
			.is_some_and(|state| state.paused && state.position_ms > 0)
	})
	.await;

	env.send(GuildCommand::Replay);
	wait_until("作り直した曲の通知", || {
		env.count_notice("play.now_playing") == 2
	})
	.await;
	env.barrier().await;
	wait_until("位置の書き直し", || {
		env.playback()
			.first()
			.is_some_and(|state| !state.paused && state.position_ms == 0)
	})
	.await;
}

#[tokio::test]
async fn replay_creates_new_track_from_start() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	let first = track_for(&env.audio, &a).unwrap();

	env.send(GuildCommand::Replay);
	wait_until("作り直した曲の通知", || {
		env.count_notice("play.now_playing") == 2
	})
	.await;

	// 今の曲を止め、解決し直して、新しい曲として先頭から再生する
	assert_eq!(
		plays(&env.audio),
		vec![expected_request(&env, &a), expected_request(&env, &a)]
	);
	let second = track_for(&env.audio, &a).unwrap();
	assert_ne!(first, second);
	let stop = position_of(&env.audio, &AudioCall::Stop).unwrap();
	let replayed = env
		.audio
		.calls()
		.iter()
		.rposition(|c| matches!(c, AudioCall::Play(_)))
		.unwrap();
	assert!(stop < replayed);
	assert_eq!(
		env.resolver.resolve_calls(),
		vec![
			(a.clone(), Priority::Interactive),
			(a.clone(), Priority::Interactive)
		]
	);
	assert_eq!(env.playback()[0].item.video_id, a);
	assert!(env.history().is_empty());

	// 止めた曲の終了は無視し、作り直した曲の終了で次へ進む (キューが空なので退出する)
	env.audio
		.emit(AudioEvent::TrackEnded { track: first })
		.await;
	env.barrier().await;
	assert!(!disconnected(&env.audio));
	env.audio
		.emit(AudioEvent::TrackEnded { track: second })
		.await;
	wait_until("退出", || disconnected(&env.audio)).await;
	assert_eq!(env.history().len(), 1);
}

#[tokio::test]
async fn play_now_moves_current_to_next() {
	let env = Env::new();
	let (a, b, c) = (vid('a'), vid('b'), vid('c'));
	start_playlist(&env, &[&a, &b, &c]).await;
	let track_b = wait_preloaded(&env, &b).await;
	wait_until("3 曲目の先読み", || {
		env.resolver
			.resolve_calls()
			.contains(&(c.clone(), Priority::Prefetch))
	})
	.await;
	env.barrier().await;

	env.send(GuildCommand::PlayNow { index: 2 });
	wait_until("指定の曲の再生", || started(&env.audio, &c)).await;
	wait_until("指定の曲の通知", || {
		env.count_notice("play.now_playing") == 2
	})
	.await;

	// 今の曲を止めて、指定の曲を始める。今の曲はキューの先頭 (次の曲) に回る
	let stop = position_of(&env.audio, &AudioCall::Stop).unwrap();
	let play_c = env
		.audio
		.calls()
		.iter()
		.position(|call| matches!(call, AudioCall::Play(r) if r.video_id == c))
		.unwrap();
	assert!(stop < play_c);
	assert_eq!(
		plays(&env.audio),
		vec![expected_request(&env, &a), expected_request(&env, &c)]
	);
	assert_eq!(
		env.notice_args("play.now_playing"),
		Some(vec![("title", title(&c))])
	);
	assert_eq!(env.queue_ids(), vec![a.clone(), b.clone()]);
	assert_eq!(env.playback()[0].item.video_id, c);
	assert!(env.history().is_empty());

	// 次の曲でなくなった準備済みの曲は捨てる
	wait_until("準備済みの曲の破棄", || {
		env.audio
			.calls()
			.contains(&AudioCall::DropPreloaded(track_b))
	})
	.await;
}

#[tokio::test]
async fn play_now_out_of_range_notifies_error() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	start_playlist(&env, &[&a, &b]).await;

	env.send(GuildCommand::PlayNow { index: 0 });
	env.send(GuildCommand::PlayNow { index: 2 });
	wait_until("範囲外の通知", || {
		env.count_notice("error.index_out_of_range") == 2
	})
	.await;
	env.barrier().await;

	assert_eq!(
		env.notice_args("error.index_out_of_range"),
		Some(vec![("count", "1".to_string())])
	);
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &a)]);
	assert_eq!(stops(&env.audio), 0);
	assert_eq!(env.queue_ids(), vec![b]);
	assert_eq!(env.playback()[0].item.video_id, a);
}

#[tokio::test]
async fn clear_variants_remove_expected_items() {
	let env = Env::new();
	let ids: Vec<VideoId> = "abcdefg".chars().map(vid).collect();
	let refs: Vec<&VideoId> = ids.iter().collect();
	start_playlist(&env, &refs).await;
	let track_b = wait_preloaded(&env, &ids[1]).await;
	assert_eq!(env.queue_ids(), ids[1..].to_vec());

	// 範囲 (2 番から 3 番) を消す
	env.send(GuildCommand::Clear {
		range: ClearRange::Range { start: 2, end: 3 },
	});
	wait_until("範囲の削除", || env.count_notice("queue.cleared") == 1).await;
	assert_eq!(
		env.notice_args("queue.cleared"),
		Some(vec![("count", "2".to_string())])
	);
	assert_eq!(
		env.queue_ids(),
		vec![
			ids[1].clone(),
			ids[4].clone(),
			ids[5].clone(),
			ids[6].clone()
		]
	);

	// 先頭の 1 件を消す。準備済みだった曲は次の曲でなくなるので捨てる
	env.send(GuildCommand::Clear {
		range: ClearRange::First(1),
	});
	wait_until("先頭の削除", || env.count_notice("queue.cleared") == 2).await;
	assert_eq!(env.queue_ids(), ids[4..].to_vec());
	wait_until("準備済みの曲の破棄", || {
		env.audio
			.calls()
			.contains(&AudioCall::DropPreloaded(track_b))
	})
	.await;

	// 範囲外の番号は何も消さない
	env.send(GuildCommand::Clear {
		range: ClearRange::Range { start: 2, end: 4 },
	});
	env.send(GuildCommand::Clear {
		range: ClearRange::First(0),
	});
	wait_until("範囲外の通知", || {
		env.count_notice("error.clear_out_of_range") == 2
	})
	.await;
	assert!(!env.has_notice("error.index_out_of_range"));
	assert_eq!(env.queue_ids(), ids[4..].to_vec());

	// 全件を消す
	env.send(GuildCommand::Clear {
		range: ClearRange::All,
	});
	wait_until("全件の削除", || env.count_notice("queue.cleared") == 3).await;
	assert_eq!(
		env.notice_args("queue.cleared"),
		Some(vec![("count", "3".to_string())])
	);
	assert!(env.queue_ids().is_empty());

	env.send(GuildCommand::Clear {
		range: ClearRange::All,
	});
	wait_until("空のキューの通知", || {
		env.has_notice("error.queue_empty")
	})
	.await;
	env.barrier().await;

	// 今の曲は消さず、止めない
	assert_eq!(stops(&env.audio), 0);
	assert_eq!(env.playback()[0].item.video_id, ids[0]);
	assert!(!disconnected(&env.audio));
}

#[tokio::test]
async fn shuffle_keeps_current_track() {
	let env = Env::new();
	let ids: Vec<VideoId> = "abcdefghij".chars().map(vid).collect();
	let refs: Vec<&VideoId> = ids.iter().collect();
	start_playlist(&env, &refs).await;
	let track_b = wait_preloaded(&env, &ids[1]).await;

	env.send(GuildCommand::Shuffle);
	wait_until("シャッフルの通知", || {
		env.has_notice("queue.shuffled")
	})
	.await;
	env.barrier().await;

	let mut after = env.queue_ids();
	if after[0] != ids[1] {
		// 先頭が変わったなら、準備済みの曲は次の曲でなくなったので捨てる
		wait_until("準備済みの曲の破棄", || {
			env.audio
				.calls()
				.contains(&AudioCall::DropPreloaded(track_b))
		})
		.await;
	}
	after.sort_by(|x, y| x.0.cmp(&y.0));
	assert_eq!(after, ids[1..].to_vec());
	assert_eq!(env.playback()[0].item.video_id, ids[0]);
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &ids[0])]);
	assert_eq!(stops(&env.audio), 0);
}

#[tokio::test]
async fn set_volume_applies_and_persists() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	play_one(&env, &a).await;

	env.send(GuildCommand::SetVolume { percent: 150 });
	wait_until("音量の通知", || env.has_notice("control.volume")).await;
	assert!(env.audio.calls().contains(&AudioCall::SetVolume(1.5)));
	assert_eq!(saved_settings(&env).volume, 1.5);
	assert_eq!(
		env.notice_args("control.volume"),
		Some(vec![("percent", "150".to_string())])
	);

	// 範囲外の値は何も変えない
	env.send(GuildCommand::SetVolume { percent: 0 });
	env.send(GuildCommand::SetVolume { percent: 201 });
	wait_until("範囲外の通知", || {
		env.count_notice("error.volume_out_of_range") == 2
	})
	.await;
	env.barrier().await;
	assert_eq!(
		env.notice_args("error.volume_out_of_range"),
		Some(vec![("min", "1".to_string()), ("max", "200".to_string())])
	);
	assert_eq!(
		count_calls(&env.audio, |c| matches!(c, AudioCall::SetVolume(_))),
		1
	);
	assert_eq!(saved_settings(&env).volume, 1.5);

	// 次の曲は新しい音量で準備する
	env.set_videos(&[&b]);
	env.play(&watch_url(&b));
	wait_until("次の曲の準備", || !preloads(&env.audio).is_empty()).await;
	assert_eq!(preloads(&env.audio)[0].volume, 1.5);
}

#[tokio::test]
async fn toggle_loop_persists() {
	let env = Env::new();
	assert!(!saved_settings(&env).loop_enabled);

	env.send(GuildCommand::ToggleLoop);
	wait_until("ループの有効の通知", || {
		env.has_notice("control.loop_on")
	})
	.await;
	assert!(saved_settings(&env).loop_enabled);

	env.send(GuildCommand::ToggleLoop);
	wait_until("ループの無効の通知", || {
		env.has_notice("control.loop_off")
	})
	.await;
	assert!(!saved_settings(&env).loop_enabled);

	env.send(GuildCommand::ToggleLoop);
	wait_until("2 回目の有効の通知", || {
		env.count_notice("control.loop_on") == 2
	})
	.await;
	assert!(saved_settings(&env).loop_enabled);
	assert!(env.audio.calls().is_empty());

	// ループ中の ?skip は、飛ばした曲をキューの末尾へ戻す
	let (a, b) = (vid('a'), vid('b'));
	start_playlist(&env, &[&a, &b]).await;
	env.send(GuildCommand::Skip);
	wait_until("2 曲目の再生", || started(&env.audio, &b)).await;
	env.barrier().await;
	assert_eq!(env.queue_ids(), vec![a]);
}

#[tokio::test]
async fn move_keeps_playing() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	play_one(&env, &a).await;

	env.send_from(GuildCommand::Move, Some(other_voice()));
	wait_until("移動の通知", || env.has_notice("control.moved")).await;
	assert_eq!(
		env.notice_args("control.moved"),
		Some(vec![("channel", format!("<#{OTHER_VOICE}>"))])
	);
	assert!(env
		.audio
		.calls()
		.contains(&AudioCall::MoveTo(other_voice())));
	assert_eq!(env.playback()[0].voice_channel_id, other_voice());

	// 同じチャンネルへの移動と、ボイスチャンネルに居ない利用者の移動は何もしない
	env.send_from(GuildCommand::Move, Some(other_voice()));
	wait_until("同じチャンネルの通知", || {
		env.has_notice("error.same_channel")
	})
	.await;
	let before = env.count_notice("error.not_in_voice");
	env.send_from(GuildCommand::Move, None);
	wait_until("ボイスチャンネルに居ない通知", || {
		env.count_notice("error.not_in_voice") > before
	})
	.await;
	env.barrier().await;
	assert_eq!(
		count_calls(&env.audio, |c| matches!(c, AudioCall::MoveTo(_))),
		1
	);

	// 再生は止めずに続く。次の曲の再生状態も移動先のチャンネルで保存する
	assert_eq!(stops(&env.audio), 0);
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &a)]);
	env.set_videos(&[&b]);
	env.play(&watch_url(&b));
	wait_until("2 曲目の追加", || env.has_notice("play.queued")).await;
	end_track(&env, &a).await;
	wait_until("2 曲目の再生", || started(&env.audio, &b)).await;
	env.barrier().await;
	assert_eq!(env.playback()[0].item.video_id, b);
	assert_eq!(env.playback()[0].voice_channel_id, other_voice());
}

#[tokio::test(start_paused = true)]
async fn no_listeners_leaves_after_timeout() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	start_playlist(&env, &[&a, &b]).await;
	let timeout = alone_timeout(&env);

	env.listeners(0);
	env.barrier().await;
	tokio::time::advance(timeout - JUST_BEFORE).await;
	settle().await;
	assert!(!disconnected(&env.audio));

	tokio::time::advance(JUST_BEFORE).await;
	wait_until("自動退出", || disconnected(&env.audio)).await;
	wait_until("自動退出の通知", || {
		env.has_notice("control.auto_left")
	})
	.await;
	assert!(env.queue_ids().is_empty());
	assert!(env.playback().is_empty());
}

#[tokio::test(start_paused = true)]
async fn listener_returning_cancels_auto_leave() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	let timeout = alone_timeout(&env);

	env.listeners(0);
	env.barrier().await;
	tokio::time::advance(timeout - JUST_BEFORE).await;
	settle().await;
	env.listeners(1);
	env.barrier().await;
	tokio::time::advance(LONG_WAIT).await;
	settle().await;
	env.barrier().await;
	assert!(!disconnected(&env.audio));
	assert!(!env.has_notice("control.auto_left"));

	// 再び誰もいなくなったら、その時点から数え直す
	env.listeners(0);
	env.barrier().await;
	tokio::time::advance(timeout - JUST_BEFORE).await;
	settle().await;
	assert!(!disconnected(&env.audio));
	tokio::time::advance(JUST_BEFORE).await;
	wait_until("自動退出", || disconnected(&env.audio)).await;
	assert!(env.playback().is_empty());
}

#[tokio::test(start_paused = true)]
async fn zero_timeout_never_auto_leaves() {
	let env = Env::new();
	env.write(|c| settings::set_alone_timeout(c, guild(), 0))
		.await;
	let a = vid('a');
	play_one(&env, &a).await;

	env.listeners(0);
	env.barrier().await;
	tokio::time::advance(LONG_WAIT).await;
	settle().await;
	env.barrier().await;

	assert!(!disconnected(&env.audio));
	assert!(!env.has_notice("control.auto_left"));
	assert_eq!(env.playback()[0].item.video_id, a);
}

#[tokio::test]
async fn connect_failure_clears_queued_items() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_videos(&[&a, &b]);

	// 接続より先に問い合わせが終わり、曲がキューに入った後で接続に失敗する
	env.audio.hold_connect();
	env.play(&watch_url(&a));
	wait_until("接続中のキューへの追加", || {
		env.queue_ids() == vec![a.clone()]
	})
	.await;
	env.audio.fail_next(AudioOp::Connect, AudioError::Timeout);
	env.audio.release_connect();
	wait_until("接続の失敗の通知", || {
		env.has_notice("error.connect_failed")
	})
	.await;
	env.barrier().await;
	assert!(env.queue_ids().is_empty());
	assert!(env.playback().is_empty());

	// 次の ?p では、失敗したセッションの曲を再生しない
	play_one(&env, &b).await;
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &b)]);
	assert!(env.queue_ids().is_empty());
}

/// 接続の途中で `?leave` しても、遅れて終わった接続を切断し、Bot をチャンネルに残さないこと。
///
/// - 接続の処理は止めない (Songbird の参加は止めても続くため)。届いた接続は古いものとして切断する
#[tokio::test]
async fn leave_during_connect_disconnects_late_connection() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);
	env.audio.hold_connect();
	env.play(&watch_url(&a));
	wait_until("接続の開始", || connects(&env.audio) == 1).await;
	env.send(GuildCommand::Leave);
	wait_until("退出の通知", || env.has_notice("control.left")).await;
	env.barrier().await;
	assert_eq!(disconnects(&env.audio), 0);

	env.audio.release_connect();
	wait_until("遅れた接続の切断", || disconnects(&env.audio) == 1).await;
	env.barrier().await;
	assert!(plays(&env.audio).is_empty());
	assert!(env.queue_ids().is_empty());
}

#[tokio::test]
async fn playback_commands_require_same_voice_channel() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	start_playlist(&env, &[&a, &b]).await;
	let before = saved_settings(&env);

	// Bot と別のボイスチャンネル、またはボイスチャンネルに居ない利用者からは受け付けない
	let restricted = [
		GuildCommand::Pause,
		GuildCommand::Resume,
		GuildCommand::Skip,
		GuildCommand::Replay,
		GuildCommand::PlayNow { index: 1 },
		GuildCommand::Clear {
			range: ClearRange::All,
		},
		GuildCommand::Shuffle,
		GuildCommand::SetVolume { percent: 150 },
		GuildCommand::ToggleLoop,
		GuildCommand::Leave,
	];
	let count = restricted.len();
	for command in restricted {
		env.send_from(command, Some(other_voice()));
	}
	env.send_from(GuildCommand::Pause, None);
	wait_until("別のチャンネルの通知", || {
		env.count_notice("error.not_same_channel") == count + 1
	})
	.await;
	env.barrier().await;

	assert_eq!(plays(&env.audio), vec![expected_request(&env, &a)]);
	assert_eq!(stops(&env.audio), 0);
	assert!(!disconnected(&env.audio));
	assert!(!env
		.audio
		.calls()
		.iter()
		.any(|c| matches!(c, AudioCall::Pause | AudioCall::SetVolume(_))));
	assert_eq!(env.queue_ids(), vec![b.clone()]);
	assert_eq!(env.playback()[0].item.video_id, a);
	assert_eq!(saved_settings(&env), before);
	assert!(env.notices().iter().all(|(channel, _)| *channel == text()));

	// 同じボイスチャンネルからは受け付ける
	env.send(GuildCommand::Pause);
	wait_until("一時停止の通知", || env.has_notice("control.paused")).await;
}

#[tokio::test(start_paused = true)]
async fn move_clears_auto_leave_timer() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;

	// 聞き手が居なくなった後に、別のチャンネルへ移動する
	env.listeners(0);
	env.barrier().await;
	env.send_from(GuildCommand::Move, Some(other_voice()));
	wait_until("移動の通知", || env.has_notice("control.moved")).await;
	env.barrier().await;
	tokio::time::advance(LONG_WAIT).await;
	settle().await;
	env.barrier().await;

	// 移動前の時刻で退出しない
	assert!(!disconnected(&env.audio));
	assert!(!env.has_notice("control.auto_left"));
	assert_eq!(env.playback()[0].item.video_id, a);
}

#[tokio::test]
async fn leave_reports_save_failure() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	// 再生状態の削除を失敗させる
	env.write(|c| {
		c.execute_batch(
			"CREATE TRIGGER fail_playback_delete BEFORE DELETE ON playback_state
			 BEGIN SELECT RAISE(ABORT, 'test'); END;",
		)?;
		Ok(())
	})
	.await;

	env.send(GuildCommand::Leave);
	wait_until("保存の失敗の通知", || {
		env.has_notice("error.save_failed")
	})
	.await;
	env.barrier().await;

	// 切断は済ませ、成功の通知は送らない
	assert!(disconnected(&env.audio));
	assert!(!env.has_notice("control.left"));
}

#[tokio::test]
async fn leave_from_outside_is_rejected_while_connecting_or_recovering() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);

	// 接続中 (音声セッションがまだ無い) でも、セッションのボイスチャンネルと比べる
	env.audio.hold_connect();
	env.play(&watch_url(&a));
	wait_until("接続の開始", || connects(&env.audio) == 1).await;
	env.send_from(GuildCommand::Leave, Some(other_voice()));
	env.send_from(GuildCommand::Leave, None);
	env.send_from(GuildCommand::Skip, Some(other_voice()));
	wait_until("別のチャンネルの通知", || {
		env.count_notice("error.not_same_channel") == 3
	})
	.await;
	env.barrier().await;
	assert!(!env.has_notice("control.left"));
	env.audio.release_connect();
	wait_until("再生の開始", || started(&env.audio, &a)).await;
	env.barrier().await;

	// 回復中 (音声セッションが無い) も同じ
	env.audio.hold_connect();
	env.send(GuildCommand::Rejoin);
	wait_until("再接続の開始", || connects(&env.audio) == 2).await;
	env.barrier().await;
	env.send_from(GuildCommand::Leave, Some(other_voice()));
	env.send_from(GuildCommand::Leave, None);
	wait_until("別のチャンネルの通知", || {
		env.count_notice("error.not_same_channel") == 5
	})
	.await;
	env.barrier().await;
	assert!(!env.has_notice("control.left"));
	assert_eq!(env.playback()[0].item.video_id, a);
	env.audio.release_connect();
	wait_until("回復の後の再生", || plays(&env.audio).len() == 2).await;

	// 同じボイスチャンネルからは退出できる
	env.send(GuildCommand::Leave);
	wait_until("退出の通知", || env.has_notice("control.left")).await;
}

#[tokio::test(start_paused = true)]
async fn settings_changed_reloads_settings() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.write(|c| settings::set_volume(c, guild(), 0.25)).await;
	play_one(&env, &a).await;
	assert_eq!(plays(&env.audio)[0].volume, 0.25);

	// DB を直接書き換えても、通知までは古い設定を使う
	env.write(|c| settings::set_volume(c, guild(), 0.5)).await;
	env.handle()
		.try_send(GuildMessage::SettingsChanged)
		.unwrap();
	env.barrier().await;

	env.set_videos(&[&b]);
	env.play(&watch_url(&b));
	wait_until("次の曲の準備", || !preloads(&env.audio).is_empty()).await;
	assert_eq!(preloads(&env.audio)[0].volume, 0.5);
}

#[tokio::test(start_paused = true)]
async fn settings_changed_reschedules_alone_timer() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	let timeout = alone_timeout(&env);

	// 自動退出の秒数を延ばしておく (アクターは通知までキャッシュを使う)。
	// 時間を止めた試験では、タイマーを動かした後の書き込みの待ちで時計が進むため、先に書く
	let longer = u32::try_from(timeout.as_secs()).unwrap() * 2;
	env.write(move |c| settings::set_alone_timeout(c, guild(), longer))
		.await;
	env.listeners(0);
	env.barrier().await;
	tokio::time::advance(timeout - JUST_BEFORE).await;
	settle().await;
	env.handle()
		.try_send(GuildMessage::SettingsChanged)
		.unwrap();
	env.barrier().await;
	tokio::time::advance(timeout).await;
	settle().await;
	env.barrier().await;
	assert!(!disconnected(&env.audio));
	tokio::time::advance(timeout).await;
	wait_until("自動退出", || disconnected(&env.audio)).await;
}

#[tokio::test]
async fn send_existing_does_not_spawn() {
	let env = Env::new();
	let registry: &GuildRegistry = &env.registry;
	assert_eq!(
		registry.send_existing(guild(), GuildMessage::SettingsChanged),
		Err(SendError::NotRunning)
	);
	assert!(!registry.contains(guild()));

	// 動いているアクターには届く
	let a = vid('a');
	play_one(&env, &a).await;
	assert_eq!(
		registry.send_existing(guild(), GuildMessage::SettingsChanged),
		Ok(())
	);
}

#[tokio::test]
async fn lookup_while_paused_is_refused() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	let paused = || ResolveError::Paused {
		retry_at: tokio::time::Instant::now() + Duration::from_secs(60),
	};
	env.resolver.set_search("song", Err(paused()));
	env.resolver.set_result(&a, Err(paused()));
	env.resolver.set_playlist(
		PLAYLIST_URL,
		Err(ResolveError::Worker {
			code: ResolveErrorCode::RateLimited,
			message: "test".to_string(),
		}),
	);

	// 検索、動画 URL、プレイリストのどれも、利用制限の文言で断り、自動では取り直さない
	env.play("song");
	env.play(&watch_url(&a));
	env.play(PLAYLIST_URL);
	wait_until("3 件の断り", || {
		env.count_notice("error.rate_limited_lookup") == 3
	})
	.await;
	env.barrier().await;
	assert!(env.queue_ids().is_empty());
	assert!(!env.has_notice("error.lookup_failed"));
	assert_eq!(env.resolver.resolve_count(&a), 1);
	assert_eq!(env.resolver.playlist_calls().len(), 1);

	// それ以外の失敗は今の文言のまま
	env.resolver.set_result(&b, Err(ResolveError::Unavailable));
	env.play(&watch_url(&b));
	wait_until("取得の失敗", || env.has_notice("error.lookup_failed")).await;
	env.barrier().await;
	assert_eq!(env.count_notice("error.rate_limited_lookup"), 3);
	assert!(env.queue_ids().is_empty());
}

/// ワーカーのエラーを作る。
fn worker_error(code: ResolveErrorCode) -> ResolveError {
	ResolveError::Worker {
		code,
		message: "test".to_string(),
	}
}

#[tokio::test]
async fn lookup_failure_keys_by_kind_and_code() {
	use ResolveErrorCode::*;
	let env = Env::new();
	// (動画の文字, エラー, 期待するキー)。動画の URL の失敗
	let videos = [
		('a', worker_error(NotFound), "error.lookup_not_found"),
		('b', worker_error(Private), "error.lookup_private"),
		('c', worker_error(GeoBlocked), "error.lookup_geo_blocked"),
		(
			'd',
			worker_error(AgeRestricted),
			"error.lookup_age_restricted",
		),
		('e', worker_error(UnsupportedUrl), "error.unsupported_url"),
		('f', worker_error(RateLimited), "error.rate_limited_lookup"),
		('g', ResolveError::Timeout, "error.lookup_timeout"),
		('h', worker_error(Internal), "error.lookup_failed"),
		('i', ResolveError::WorkerDied, "error.lookup_failed"),
		('j', ResolveError::Unavailable, "error.lookup_failed"),
	];
	for (c, error, expected) in videos {
		let id = vid(c);
		env.resolver.set_result(&id, Err(error));
		let before = env.count_notice(expected);
		env.play(&watch_url(&id));
		wait_until(expected, || env.count_notice(expected) == before + 1).await;
	}

	// 検索語で結果が無いときは、検索用の文言
	env.resolver
		.set_search("nothing", Err(worker_error(NotFound)));
	env.play("nothing");
	wait_until("検索の失敗", || {
		env.has_notice("error.lookup_search_empty")
	})
	.await;

	// プレイリストが見つからない・非公開のときは、プレイリスト用の文言
	env.resolver
		.set_playlist(PLAYLIST_URL, Err(worker_error(Private)));
	env.play(PLAYLIST_URL);
	wait_until("プレイリストの失敗", || {
		env.has_notice("error.lookup_playlist_unavailable")
	})
	.await;
	env.barrier().await;
	assert!(env.queue_ids().is_empty());
	// 1 曲も取り出していないので、再生の終了は通知しない
	assert!(!env.has_notice("play.completed"));
}

#[tokio::test]
async fn play_denied_by_permission_skips_connect_and_explains() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);
	env.access.set(AccessVerdict::Denied(Denial::Missing(vec![
		VoicePermission::Connect,
	])));

	env.play(&watch_url(&a));
	wait_until("権限の不足の通知", || {
		env.has_notice("error.voice_no_permission")
	})
	.await;
	env.barrier().await;
	assert_eq!(
		env.notice_args("error.voice_no_permission"),
		Some(vec![("channel", format!("<#{VOICE}>"))])
	);
	assert_eq!(
		env.notice_lists("error.voice_no_permission"),
		Some(vec![("permissions", vec!["ui.perm_connect"])])
	);
	assert_eq!(env.access.calls(), vec![(guild(), voice())]);
	assert_eq!(connects(&env.audio), 0);
	assert!(!env.has_notice("error.connect_failed"));
	assert!(env.queue_ids().is_empty());
	assert!(env.playback().is_empty());
}

#[tokio::test]
async fn play_into_full_channel_explains() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);
	env.access.set(AccessVerdict::Denied(Denial::Full));

	env.play(&watch_url(&a));
	wait_until("満員の通知", || env.has_notice("error.voice_full")).await;
	env.barrier().await;
	assert_eq!(
		env.notice_args("error.voice_full"),
		Some(vec![("channel", format!("<#{VOICE}>"))])
	);
	assert_eq!(connects(&env.audio), 0);
	assert!(!env.has_notice("error.connect_failed"));
}

#[tokio::test]
async fn unknown_access_still_connects() {
	let env = Env::new();
	let a = vid('a');
	env.access.set(AccessVerdict::Unknown);
	play_one(&env, &a).await;
	assert_eq!(connects(&env.audio), 1);
	assert_eq!(env.access.calls(), vec![(guild(), voice())]);
}

#[tokio::test]
async fn move_denied_does_not_move() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.access.set(AccessVerdict::Denied(Denial::Missing(vec![
		VoicePermission::ViewChannel,
	])));

	env.send_from(GuildCommand::Move, Some(other_voice()));
	wait_until("権限の不足の通知", || {
		env.has_notice("error.voice_no_permission")
	})
	.await;
	env.barrier().await;
	assert_eq!(
		env.notice_args("error.voice_no_permission"),
		Some(vec![("channel", format!("<#{OTHER_VOICE}>"))])
	);
	assert_eq!(
		count_calls(&env.audio, |c| matches!(c, AudioCall::MoveTo(_))),
		0
	);
	assert!(!env.has_notice("control.moved"));
	assert_eq!(env.playback()[0].voice_channel_id, voice());
	assert_eq!(stops(&env.audio), 0);
}

#[tokio::test(start_paused = true)]
async fn move_check_hang_falls_back_to_moving() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.access.hang();

	// アクターは確認を待っている間、目印にも応答しないため、確認の呼び出しを目印にする
	env.send_from(GuildCommand::Move, Some(other_voice()));
	wait_until("移動先の確認", || env.access.calls().len() == 2).await;
	settle().await;
	assert!(!env.has_notice("control.moved"));
	let limit = watapachi_bot::audio::limits::VoiceLimits::new(&env.config.voice).access();
	tokio::time::advance(limit).await;
	wait_until("移動の通知", || env.has_notice("control.moved")).await;
	assert!(env
		.audio
		.calls()
		.contains(&AudioCall::MoveTo(other_voice())));
}
