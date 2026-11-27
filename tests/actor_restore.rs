//! 再生位置の保存、起動時の復元、終了の試験。
//! - 環境と待ちの補助は `support/actor_env.rs` にある。音声、解決、通知は偽実装、DB は一時ディレクトリの実 DB
//! - 「起きないこと」は目印 (`Env::barrier`) で、それより前のメッセージの処理を終えたことを確かめてから見る
//! - 時間を止めた試験では、試験側は tokio のタイマーを待たず、`tokio::time::advance` だけで時間を進める
//! - 保存の間隔、復元のずらし幅、終了の上限時間は config.example.toml の値を読む

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
use tokio::time::Instant;
use watapachi_bot::audio::access::{AccessVerdict, Denial, VoicePermission};
use watapachi_bot::audio::backend::{AudioError, AudioEvent, DisconnectKind, PlayRequest};
use watapachi_bot::db::playback::{self, PlaybackState};
use watapachi_bot::db::queue::{self, NewQueueItem};
use watapachi_bot::domain::{
	ChannelId, GuildId, Priority, ResolveError, ResolveErrorCode, UserId, VideoId,
};
use watapachi_bot::guild::message::{GuildCommand, GuildMessage, RestoreRequest};
use watapachi_bot::guild::restore::restore_all;

/// 時間を止めた試験で、時刻の直前を見るために引く時間。
const JUST_BEFORE: Duration = Duration::from_millis(1);
/// 保存されていた再生位置。
const SAVED_AT: Duration = Duration::from_secs(42);
/// 保存されていた通知先 (コマンドを送るテキストチャンネルとは別にする)。
const SAVED_TEXT: u64 = 11;
/// キューの上限 (試験の前準備で入れる曲数より多ければよい)。
const QUEUE_LIMIT: u32 = 100;

fn saved_text() -> ChannelId {
	ChannelId::new(SAVED_TEXT)
}

fn item(id: &VideoId) -> NewQueueItem {
	NewQueueItem {
		video_id: id.clone(),
		title: title(id),
		duration_s: Some(10),
		requester_id: UserId::new(USER),
	}
}

fn millis(position: Duration) -> u64 {
	position.as_millis() as u64
}

/// 保存されていた再生状態。ボイスチャンネルは `voice()`、通知先は `saved_text()`。
fn saved_state(guild: GuildId, id: &VideoId, position: Duration, paused: bool) -> PlaybackState {
	PlaybackState {
		guild_id: guild,
		voice_channel_id: voice(),
		text_channel_id: saved_text(),
		item: item(id),
		position_ms: millis(position),
		paused,
	}
}

/// 再生状態を DB に書く (前回の起動で保存されていたものとして)。
async fn save_state(env: &Env, state: PlaybackState) {
	env.write(move |c| playback::upsert(c, &state)).await;
}

/// ギルドのキューの末尾へ曲を入れる。
async fn enqueue(env: &Env, guild: GuildId, ids: &[&VideoId]) {
	let items: Vec<NewQueueItem> = ids.iter().map(|id| item(id)).collect();
	env.write(move |c| queue::append(c, guild, &items, QUEUE_LIMIT).map(|_| ()))
		.await;
}

fn stagger(env: &Env) -> Duration {
	Duration::from_millis(env.config.restore.stagger_ms)
}

/// 全ギルドの聞き手の人数を `listeners` として復元を実行する。
async fn restore(env: &Env, listeners: Option<u32>) {
	restore_all(
		&env.registry,
		&env.db.handles,
		move |_, _| listeners,
		stagger(env),
	)
	.await;
}

fn count_calls(audio: &FakeAudio, pred: impl Fn(&AudioCall) -> bool) -> usize {
	audio.calls().iter().filter(|c| pred(c)).count()
}

fn connects(audio: &FakeAudio) -> usize {
	count_calls(audio, |c| matches!(c, AudioCall::Connect { .. }))
}

fn pauses(audio: &FakeAudio) -> usize {
	count_calls(audio, |c| *c == AudioCall::Pause)
}

fn last_play(audio: &FakeAudio) -> PlayRequest {
	plays(audio).pop().unwrap()
}

/// 動画 URL の `?p` で 1 曲を再生し、再生の開始まで処理させる。
async fn play_one(env: &Env, id: &VideoId) {
	env.set_videos(&[id]);
	env.play(&watch_url(id));
	wait_until("再生の開始", || started(&env.audio, id)).await;
	env.barrier().await;
}

/// 保存されている再生位置と一時停止の状態。行が無ければ None。
fn saved_position(env: &Env) -> Option<(u64, bool)> {
	env.playback()
		.first()
		.map(|state| (state.position_ms, state.paused))
}

/// 保存された再生位置が `expected` になるまで待つ (書き込みは結果を待たずに行われるため)。
async fn wait_saved(env: &Env, expected: (u64, bool)) {
	wait_until("再生位置の保存", || {
		saved_position(env) == Some(expected)
	})
	.await;
}

/// 書き込みスレッドを止める。返す送信口へ送るか捨てると再開する。
/// - 止まったことを確かめてから戻る。以後の書き込み (アクターの書き込みを含む) は再開まで待つ
async fn block_writer(
	env: &Env,
) -> (
	std::sync::mpsc::Sender<()>,
	tokio::task::JoinHandle<Result<(), watapachi_bot::db::DbError>>,
) {
	let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
	let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
	let writer = env.db.handles.writer.clone();
	let blocker = tokio::spawn(async move {
		writer
			.run(move |_| {
				let _ = entered_tx.send(());
				let _ = release_rx.recv();
				Ok(())
			})
			.await
	});
	wait_until("書き込みスレッドの停止", || {
		entered_rx.try_recv().is_ok()
	})
	.await;
	(release_tx, blocker)
}

/// ギルドのキューの動画 ID を順に返す。
fn queue_of(env: &Env, guild: GuildId) -> Vec<VideoId> {
	queue::list(&env.conn(), guild, 0, QUEUE_LIMIT)
		.unwrap()
		.into_iter()
		.map(|q| q.item.video_id)
		.collect()
}

/// 今の曲の失敗を音声バックエンドから知らせる。
async fn fail_current(env: &Env, id: &VideoId) {
	let track = track_for(&env.audio, id).unwrap();
	env.audio
		.emit(AudioEvent::TrackFailed {
			track,
			error: AudioError::Failed("test".to_string()),
		})
		.await;
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

#[tokio::test(start_paused = true)]
async fn position_is_saved_periodically() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	let interval = Duration::from_secs(env.config.playback.position_save_interval_secs);

	// 間隔の直前までは保存しない (曲の切替で書いた先頭の位置のまま)
	env.audio.set_position(Duration::from_secs(7));
	tokio::time::advance(interval - JUST_BEFORE).await;
	settle().await;
	env.barrier().await;
	assert_eq!(saved_position(&env), Some((0, false)));

	// 間隔ごとに音声の位置を保存する
	tokio::time::advance(JUST_BEFORE).await;
	wait_saved(&env, (7_000, false)).await;
	env.audio.set_position(Duration::from_secs(12));
	tokio::time::advance(interval).await;
	wait_saved(&env, (12_000, false)).await;

	// 一時停止の間は定期的に保存しない
	env.audio.set_position(Duration::from_secs(15));
	env.send(GuildCommand::Pause);
	wait_saved(&env, (15_000, true)).await;
	env.audio.set_position(Duration::from_secs(99));
	tokio::time::advance(interval * 2).await;
	settle().await;
	env.barrier().await;
	assert_eq!(saved_position(&env), Some((15_000, true)));
}

#[tokio::test]
async fn pause_saves_position_immediately() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;

	env.audio.set_position(Duration::from_secs(13));
	env.send(GuildCommand::Pause);
	wait_saved(&env, (13_000, true)).await;

	env.send(GuildCommand::Resume);
	wait_saved(&env, (13_000, false)).await;
	assert!(env.has_notice("control.resumed"));
}

#[tokio::test]
async fn restore_seeks_to_saved_position() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);
	save_state(&env, saved_state(guild(), &a, SAVED_AT, false)).await;

	restore(&env, Some(1)).await;
	wait_until("復元の再生", || started(&env.audio, &a)).await;
	env.barrier().await;

	// 保存されていたボイスチャンネルへ接続し、解決し、保存した位置から再生する
	assert_eq!(
		env.audio.calls()[0],
		AudioCall::Connect {
			guild: guild(),
			channel: voice()
		}
	);
	assert_eq!(
		env.resolver.resolve_calls(),
		vec![(a.clone(), Priority::Interactive)]
	);
	let expected = PlayRequest {
		start_at: SAVED_AT,
		..expected_request(&env, &a)
	};
	assert_eq!(plays(&env.audio), vec![expected]);
	assert_eq!(pauses(&env.audio), 0);
	// 再生状態は残り、以後の操作も通る
	assert_eq!(env.playback()[0].item.video_id, a);
	env.send(GuildCommand::Pause);
	wait_until("一時停止の通知", || env.has_notice("control.paused")).await;
}

#[tokio::test]
async fn restore_keeps_paused_state() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);
	save_state(&env, saved_state(guild(), &a, SAVED_AT, true)).await;

	restore(&env, Some(1)).await;
	wait_until("一時停止の状態での復元", || {
		env.has_notice("play.restored_paused")
	})
	.await;
	env.barrier().await;

	// 一時停止のまま載せる (始めてから止めないので音が出ない)。再生状態の一時停止も残る
	assert!(last_play(&env.audio).paused);
	assert_eq!(pauses(&env.audio), 0);
	assert_eq!(last_play(&env.audio).start_at, SAVED_AT);
	assert_eq!(saved_position(&env), Some((millis(SAVED_AT), true)));
	assert!(env.has_notice("play.restored_paused"));
	assert!(!env.has_notice("play.restored"));

	// ?resume で再開できる (一時停止中として扱われている)
	// 偽の音声は `start_at` で位置を変えないため、シークした後の位置を指定しておく
	env.audio.set_position(SAVED_AT);
	env.send(GuildCommand::Resume);
	wait_until("再開の通知", || env.has_notice("control.resumed")).await;
	wait_saved(&env, (millis(SAVED_AT), false)).await;
}

#[tokio::test]
async fn restore_without_listeners_requeues_current_at_front() {
	let env = Env::new();
	let (a, b, c) = (vid('a'), vid('b'), vid('c'));
	save_state(&env, saved_state(guild(), &a, SAVED_AT, false)).await;
	enqueue(&env, guild(), &[&b, &c]).await;

	restore(&env, Some(0)).await;

	// 再参加しない。キューは残し、再生中だった曲を先頭へ戻し、再生状態を消す
	assert!(!env.registry.contains(guild()));
	assert_eq!(connects(&env.audio), 0);
	assert_eq!(env.queue_ids(), vec![a, b, c]);
	assert!(env.playback().is_empty());
	assert!(env.notices().is_empty());
}

#[tokio::test]
async fn restore_with_missing_channel_keeps_queue_and_clears_state() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	save_state(&env, saved_state(guild(), &a, SAVED_AT, true)).await;
	enqueue(&env, guild(), &[&b]).await;

	// チャンネルが無い (削除、権限の喪失)
	restore(&env, None).await;

	assert!(!env.registry.contains(guild()));
	assert_eq!(connects(&env.audio), 0);
	assert_eq!(env.queue_ids(), vec![a, b]);
	assert!(env.playback().is_empty());
}

#[tokio::test]
async fn restore_notifies_text_channel() {
	let env = Env::new();
	let a = vid('a');
	env.set_videos(&[&a]);
	save_state(&env, saved_state(guild(), &a, SAVED_AT, false)).await;

	restore(&env, Some(2)).await;
	wait_until("復元の通知", || env.has_notice("play.restored")).await;
	env.barrier().await;

	// 保存されていたテキストチャンネルへ、曲名を添えて 1 回だけ通知する (再生中の通知は送らない)
	let restored: Vec<_> = env
		.notices()
		.into_iter()
		.filter(|(_, n)| n.key == "play.restored")
		.collect();
	assert_eq!(restored.len(), 1);
	assert_eq!(restored[0].0, saved_text());
	assert_eq!(restored[0].1.args, vec![("title", title(&a))]);
	assert!(!env.has_notice("play.now_playing"));
	assert!(!env.has_notice("play.preparing"));
}

#[tokio::test(start_paused = true)]
async fn restore_is_staggered() {
	let env = Env::new();
	let guilds: Vec<GuildId> = (1..=3).map(GuildId::new).collect();
	let ids = [vid('a'), vid('b'), vid('c')];
	for (guild, id) in guilds.iter().zip(&ids) {
		env.set_videos(&[id]);
		save_state(&env, saved_state(*guild, id, SAVED_AT, false)).await;
	}
	let step = stagger(&env);
	let connected = || -> Vec<GuildId> {
		env.audio
			.calls()
			.into_iter()
			.filter_map(|c| match c {
				AudioCall::Connect { guild, .. } => Some(guild),
				_ => None,
			})
			.collect()
	};

	let check = async {
		for n in 1..=guilds.len() {
			wait_until("復元の接続", || connected().len() == n).await;
			assert_eq!(connected(), guilds[..n].to_vec());
			if n == guilds.len() {
				break;
			}
			// ずらし幅の直前までは次のギルドを始めない
			tokio::time::advance(step - JUST_BEFORE).await;
			settle().await;
			env.barrier().await;
			assert_eq!(connected().len(), n);
			tokio::time::advance(JUST_BEFORE).await;
		}
	};
	tokio::join!(restore(&env, Some(1)), check);
	for id in &ids {
		wait_until("各ギルドの再生", || started(&env.audio, id)).await;
	}
}

#[tokio::test]
async fn restore_resolve_failure_skips_to_next() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_videos(&[&b]);
	env.resolver.set_result(
		&a,
		Err(ResolveError::Worker {
			code: ResolveErrorCode::NotFound,
			message: "test".to_string(),
		}),
	);
	save_state(&env, saved_state(guild(), &a, SAVED_AT, true)).await;
	enqueue(&env, guild(), &[&b]).await;

	restore(&env, Some(1)).await;
	wait_until("次の曲の再生", || started(&env.audio, &b)).await;
	env.barrier().await;

	// 復元の曲は理由を添えて飛ばし、次の曲を先頭から通常どおり再生する
	assert!(!started(&env.audio, &a));
	assert!(env.has_notice("error.video_not_found"));
	assert_eq!(plays(&env.audio), vec![expected_request(&env, &b)]);
	assert_eq!(pauses(&env.audio), 0);
	assert!(env.has_notice("play.now_playing"));
	assert!(!env.has_notice("play.restored"));
	assert!(!env.has_notice("play.restored_paused"));
	let state = &env.playback()[0];
	assert_eq!(state.item.video_id, b);
	assert_eq!((state.position_ms, state.paused), (0, false));
	assert!(env.queue_ids().is_empty());
}

#[tokio::test]
async fn shutdown_saves_position_keeps_state_and_disconnects() {
	// 再生中
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.audio.set_position(Duration::from_secs(33));
	env.shutdown_all().await;
	wait_saved(&env, (33_000, false)).await;
	assert!(disconnected(&env.audio));
	assert_eq!(env.playback()[0].item.video_id, a);
	assert!(!env.registry.contains(guild()));

	// 一時停止中は一時停止のまま保存する
	let env = Env::new();
	play_one(&env, &a).await;
	env.audio.set_position(Duration::from_secs(8));
	env.send(GuildCommand::Pause);
	wait_saved(&env, (8_000, true)).await;
	env.audio.set_position(Duration::from_secs(9));
	env.shutdown_all().await;
	wait_saved(&env, (9_000, true)).await;
	assert!(disconnected(&env.audio));
}

#[tokio::test]
async fn restart_round_trip() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);
	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	env.barrier().await;
	env.audio.set_position(Duration::from_secs(25));

	// 終了して DB を閉じ、同じファイルを開き直す
	env.shutdown_all().await;
	let env = env.reopen().await;
	env.set_videos(&[&a, &b]);
	restore(&env, Some(1)).await;
	wait_until("復元の再生", || started(&env.audio, &a)).await;
	env.barrier().await;

	// 同じ曲を同じ位置から再生し、残りのキューも残っている
	let expected = PlayRequest {
		start_at: Duration::from_secs(25),
		..expected_request(&env, &a)
	};
	assert_eq!(plays(&env.audio), vec![expected]);
	assert_eq!(env.queue_ids(), vec![b.clone()]);
	assert_eq!(env.playback()[0].item.video_id, a);
	assert!(env.has_notice("play.restored"));
	end_track(&env, &a).await;
	wait_until("2 曲目の再生", || started(&env.audio, &b)).await;
}

#[tokio::test]
async fn restore_while_session_active_requeues_track() {
	let env = Env::new();
	let (a, b, c) = (vid('a'), vid('b'), vid('c'));
	env.set_videos(&[&a, &c]);
	play_one(&env, &b).await;
	enqueue(&env, guild(), &[&c]).await;

	// 再生中のアクターへ復元が届いたら、2 つ目のセッションを始めず、曲をキューの先頭へ戻す
	env.handle()
		.try_send(GuildMessage::Restore(RestoreRequest {
			state: saved_state(guild(), &a, SAVED_AT, false),
			listeners: 1,
		}))
		.unwrap();
	wait_until("キューの先頭への追加", || {
		env.queue_ids() == vec![a.clone(), c.clone()]
	})
	.await;
	env.barrier().await;
	assert_eq!(connects(&env.audio), 1);
	assert_eq!(plays(&env.audio).len(), 1);
	assert_eq!(env.playback()[0].item.video_id, b);
}

#[tokio::test]
async fn restore_connect_failure_keeps_queue() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_videos(&[&a, &b]);
	save_state(&env, saved_state(guild(), &a, SAVED_AT, false)).await;
	enqueue(&env, guild(), &[&b]).await;
	env.audio
		.fail_next(AudioOp::Connect, AudioError::Failed("test".to_string()));

	restore(&env, Some(1)).await;
	wait_until("復元の失敗の通知", || {
		env.has_notice("error.restore_failed")
	})
	.await;
	env.barrier().await;

	// キューを残し、中断した曲をキューの先頭へ戻し、再生状態を消す。
	// 通知は保存されていたテキストチャンネルへ
	let (channel, notice) = env
		.notices()
		.into_iter()
		.find(|(_, n)| n.key == "error.restore_failed")
		.unwrap();
	assert_eq!(channel, saved_text());
	assert_eq!(notice.args, vec![("prefix", env.config.bot.prefix.clone())]);
	assert!(!env.has_notice("error.connect_failed"));
	assert_eq!(env.queue_ids(), vec![a.clone(), b.clone()]);
	assert!(env.playback().is_empty());
	assert!(plays(&env.audio).is_empty());

	// 次の ?p で、戻した曲から再生する
	let c = vid('c');
	env.set_videos(&[&c]);
	env.play(&watch_url(&c));
	wait_until("戻した曲の再生", || started(&env.audio, &a)).await;
}

#[tokio::test]
async fn restore_keeps_state_when_mailbox_full() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	play_one(&env, &a).await;
	enqueue(&env, guild(), &[&b]).await;

	// アクターを書き込みの待ちで止め、メールボックスを満杯にする
	let (release_tx, blocker) = block_writer(&env).await;
	env.send(GuildCommand::ToggleLoop);
	settle().await;
	let handle = env.handle();
	let mut full = false;
	for _ in 0..=env.config.limits.actor_mailbox {
		if handle
			.try_send(GuildMessage::VoiceMembersChanged { listeners: 1 })
			.is_err()
		{
			full = true;
			break;
		}
	}
	assert!(full, "メールボックスが満杯にならない");

	// 送れなければ待たずに次へ進み、DB に触らない
	let finished = tokio::time::timeout(Duration::from_secs(1), restore(&env, Some(1)))
		.await
		.is_ok();
	release_tx.send(()).unwrap();
	blocker.await.unwrap().unwrap();
	wait_until("?loop の応答", || env.has_notice("control.loop_on")).await;
	// ここまでに送られた書き込みが終わったことを、書き込みスレッドの一巡で確かめる
	env.write(|_| Ok(())).await;
	assert!(finished, "復元が書き込みを待った");
	assert_eq!(env.queue_ids(), vec![b]);
	assert_eq!(env.playback()[0].item.video_id, a);
	assert_eq!(connects(&env.audio), 1);
}

#[tokio::test]
async fn restore_without_listeners_keeps_active_session_state() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	play_one(&env, &a).await;
	enqueue(&env, guild(), &[&b]).await;

	// 読み込むのは今のセッションの行。聞き手が 0 人でも、セッション中のアクターの行を消さない
	restore(&env, Some(0)).await;
	env.barrier().await;
	assert_eq!(env.playback()[0].item.video_id, a);
	// 今再生中の曲をキューへ重ねて入れない
	assert_eq!(env.queue_ids(), vec![b]);
	assert_eq!(connects(&env.audio), 1);
	assert_eq!(plays(&env.audio).len(), 1);
}

#[tokio::test]
async fn restore_without_listeners_to_idle_actor_requeues() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	save_state(&env, saved_state(guild(), &a, SAVED_AT, false)).await;
	enqueue(&env, guild(), &[&b]).await;
	// 待機中のアクターを作っておく
	env.barrier().await;
	assert!(env.registry.contains(guild()));

	// アクターが判断し、再参加せずに曲を先頭へ戻して行を消す
	restore(&env, None).await;
	wait_until("キューへの戻し", || {
		env.queue_ids() == vec![a.clone(), b.clone()]
	})
	.await;
	env.barrier().await;
	assert!(env.playback().is_empty());
	assert_eq!(connects(&env.audio), 0);
}

#[tokio::test(start_paused = true)]
async fn restore_keeps_state_changed_after_load() {
	let env = Env::new();
	let (first, second) = (GuildId::new(1), GuildId::new(2));
	let (a, b) = (vid('a'), vid('b'));
	save_state(&env, saved_state(first, &a, SAVED_AT, false)).await;
	save_state(&env, saved_state(second, &b, SAVED_AT, false)).await;
	let moved_ms = millis(SAVED_AT) + 5_000;

	let check = async {
		wait_until("1 つ目のギルドの戻し", || {
			queue_of(&env, first) == vec![a.clone()]
		})
		.await;
		// ずらしの待ちの間に、2 つ目のギルドの行が変わる (別の処理の位置の保存にあたる)。
		// 書き込みスレッドを待つと仮想時間が進むため、別の接続でその場で書く
		let c = rusqlite::Connection::open(&env.db_path).unwrap();
		c.busy_timeout(Duration::from_secs(5)).unwrap();
		c.execute(
			"UPDATE playback_state SET position_ms = ?1 WHERE guild_id = ?2",
			rusqlite::params![moved_ms as i64, second.get() as i64],
		)
		.unwrap();
		tokio::time::advance(stagger(&env)).await;
	};
	tokio::join!(restore(&env, None), check);

	// 読み込んだ後に変わった行は消さず、キューへも戻さない
	assert!(queue_of(&env, second).is_empty());
	let states = env.playback();
	assert_eq!(states.len(), 1);
	assert_eq!(states[0].guild_id, second);
	assert_eq!(states[0].position_ms, moved_ms);
}

#[tokio::test(start_paused = true)]
async fn shutdown_all_gives_up_on_hung_actor() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;

	// 書き込みスレッドを止め、アクターを書き込みの待ちで止める
	let (release_tx, blocker) = block_writer(&env).await;
	env.send(GuildCommand::ToggleLoop);
	settle().await;

	// 渡した期限で諦めて戻る。`shutdown.timeout_secs` より短い期限でも、それを超えて待たない
	// (後の DB の段の時間を残すため)
	let limit = Duration::from_secs(env.config.shutdown.timeout_secs) / 4;
	let begun = Instant::now();
	env.registry.shutdown_all(begun + limit).await;
	let elapsed = begun.elapsed();
	assert!(elapsed >= limit, "{elapsed:?}");
	assert!(elapsed < limit + Duration::from_secs(1), "{elapsed:?}");

	release_tx.send(()).unwrap();
	blocker.await.unwrap().unwrap();
}

#[tokio::test]
async fn pause_intent_survives_track_failure_and_recovery() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.send(GuildCommand::Pause);
	wait_until("一時停止の通知", || env.has_notice("control.paused")).await;
	env.audio.set_position(SAVED_AT);

	// 一時停止中の曲の失敗で、一時停止のまま作り直す
	fail_current(&env, &a).await;
	wait_until("作り直しの再生", || plays(&env.audio).len() == 2).await;
	env.barrier().await;
	assert!(last_play(&env.audio).paused);

	// 続く回復の後も一時停止のまま作り直す。一時停止の呼び出しは利用者の 1 回だけ
	lose_connection(&env).await;
	wait_rejoined(&env, 2, 3).await;
	env.barrier().await;
	assert!(last_play(&env.audio).paused);
	assert_eq!(pauses(&env.audio), 1);
	assert_eq!(last_play(&env.audio).start_at, SAVED_AT);
	assert_eq!(saved_position(&env), Some((millis(SAVED_AT), true)));
	env.send(GuildCommand::Resume);
	wait_until("再開の通知", || env.has_notice("control.resumed")).await;
}

#[tokio::test]
async fn pause_intent_survives_repeated_track_failure() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.send(GuildCommand::Pause);
	wait_until("一時停止の通知", || env.has_notice("control.paused")).await;

	// 一時停止中の曲の失敗で、一時停止のまま作り直す
	fail_current(&env, &a).await;
	wait_until("作り直しの再生", || plays(&env.audio).len() == 2).await;
	env.barrier().await;
	assert!(last_play(&env.audio).paused);

	// 作り直した曲が再び失敗しても、一時停止のまま作り直す (PipeInput)
	fail_current(&env, &a).await;
	wait_until("2 回目の作り直し", || plays(&env.audio).len() == 3).await;
	env.barrier().await;
	assert!(last_play(&env.audio).use_pipe);
	assert!(last_play(&env.audio).paused);
	assert_eq!(pauses(&env.audio), 1);
	env.send(GuildCommand::Resume);
	wait_until("再開の通知", || env.has_notice("control.resumed")).await;
}

#[tokio::test]
async fn pause_intent_survives_second_failure_before_start() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.send(GuildCommand::Pause);
	wait_until("一時停止の通知", || env.has_notice("control.paused")).await;
	env.audio.set_position(SAVED_AT);

	// 一時停止中に切断。再接続の後の解決を止めておく
	env.resolver.hold(&a);
	lose_connection(&env).await;
	wait_until("再接続の後の解決", || {
		connects(&env.audio) == 2 && env.resolver.resolve_count(&a) == 2
	})
	.await;
	env.barrier().await;

	// 作り直した曲が始まる前に、もう一度切断する
	lose_connection(&env).await;
	wait_until("2 回目の再接続の後の解決", || {
		connects(&env.audio) == 3 && env.resolver.resolve_count(&a) == 3
	})
	.await;
	env.barrier().await;
	assert_eq!(plays(&env.audio).len(), 1);

	// 一時停止のまま作り直す
	env.resolver.release(&a);
	wait_until("回復の後の作り直し", || {
		plays(&env.audio).len() == 2
	})
	.await;
	env.barrier().await;
	assert!(last_play(&env.audio).paused);
	assert_eq!(pauses(&env.audio), 1);
	assert_eq!(last_play(&env.audio).start_at, SAVED_AT);
	env.send(GuildCommand::Resume);
	wait_until("再開の通知", || env.has_notice("control.resumed")).await;
}

#[tokio::test]
async fn rejoin_while_paused_saves_unpaused_after_start() {
	let env = Env::new();
	let a = vid('a');
	play_one(&env, &a).await;
	env.audio.set_position(SAVED_AT);
	env.send(GuildCommand::Pause);
	wait_until("一時停止の通知", || env.has_notice("control.paused")).await;

	// ?rejoin は一時停止中でも再開し、再生を始めたら一時停止でない状態を保存する
	env.send(GuildCommand::Rejoin);
	wait_rejoined(&env, 2, 2).await;
	assert_eq!(pauses(&env.audio), 1);
	wait_saved(&env, (millis(SAVED_AT), false)).await;
}

#[tokio::test]
async fn pop_failure_clears_stale_playback_state() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_playlist(&[&a, &b]);
	env.play(PLAYLIST_URL);
	wait_until("1 曲目の再生", || started(&env.audio, &a)).await;
	env.barrier().await;
	// キューからの取り出しを失敗させる
	env.write(|c| {
		c.execute_batch(
			"CREATE TRIGGER fail_queue_delete BEFORE DELETE ON queue_items
			 BEGIN SELECT RAISE(ABORT, 'test'); END;",
		)?;
		Ok(())
	})
	.await;

	end_track(&env, &a).await;
	wait_until("退出", || disconnected(&env.audio)).await;
	env.barrier().await;

	// 終わった曲の再生状態を残さない (次の起動で再生し直さないため)。キューは残る
	assert!(env.playback().is_empty());
	assert_eq!(env.queue_ids(), vec![b]);
}

/// 復元の接続を待つ間の `?p` の曲は、黙ってキューへ入れず、追加を通知すること。
/// 接続の後は保存されていた曲を先に再生し、`?p` の曲はその次に再生する。
#[tokio::test]
async fn play_during_restore_connect_is_announced_as_queued() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_videos(&[&a, &b]);
	save_state(&env, saved_state(guild(), &a, SAVED_AT, false)).await;
	env.audio.hold_connect();

	restore(&env, Some(1)).await;
	wait_until("復元の接続の開始", || connects(&env.audio) == 1).await;
	env.play(&watch_url(&b));
	wait_until("接続中のキューへの追加", || {
		env.queue_ids() == vec![b.clone()]
	})
	.await;
	wait_until("追加の通知", || env.has_notice("play.queued")).await;
	assert_eq!(
		env.notice_args("play.queued"),
		Some(vec![("title", title(&b))])
	);

	env.audio.release_connect();
	wait_until("復元の再生", || started(&env.audio, &a)).await;
	env.barrier().await;
	assert!(!started(&env.audio, &b));
	assert_eq!(last_play(&env.audio).start_at, SAVED_AT);

	end_track(&env, &a).await;
	wait_until("?p の曲の再生", || started(&env.audio, &b)).await;
}

#[tokio::test]
async fn restore_denied_keeps_queue_and_uses_restore_notice() {
	let env = Env::new();
	let (a, b) = (vid('a'), vid('b'));
	env.set_videos(&[&a, &b]);
	save_state(&env, saved_state(guild(), &a, SAVED_AT, false)).await;
	enqueue(&env, guild(), &[&b]).await;
	env.access.set(AccessVerdict::Denied(Denial::Missing(vec![
		VoicePermission::Connect,
	])));

	restore(&env, Some(1)).await;
	wait_until("復元の失敗の通知", || {
		env.has_notice("error.restore_failed")
	})
	.await;
	env.barrier().await;
	assert!(!env.has_notice("error.voice_no_permission"));
	assert!(!env.has_notice("error.connect_failed"));
	assert!(env
		.audio
		.calls()
		.iter()
		.all(|c| !matches!(c, AudioCall::Connect { .. })));
	assert_eq!(env.queue_ids(), vec![a.clone(), b.clone()]);
	assert!(env.playback().is_empty());
}
