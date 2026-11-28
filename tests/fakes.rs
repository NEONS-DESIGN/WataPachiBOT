//! 音声と解決と通知の偽実装の試験。アクターの試験が頼る振る舞いを確かめる。

#[path = "support/fake_audio.rs"]
mod fake_audio;
#[path = "support/fake_notifier.rs"]
mod fake_notifier;
#[path = "support/fake_resolver.rs"]
mod fake_resolver;

use std::collections::BTreeMap;
use std::time::Duration;

use fake_audio::{AudioCall, AudioOp, FakeAudio, HEALTHY_DAVE_VERSION, HEALTHY_FRAMES_PER_SAMPLE};
use fake_notifier::FakeNotifier;
use fake_resolver::FakeResolver;
use tokio::sync::mpsc;
use watapachi_bot::audio::backend::{
	AudioBackend, AudioError, AudioEvent, DisconnectKind, PlayRequest, TrackId, VoiceHealth,
};
use watapachi_bot::domain::{
	ChannelId, GuildId, Priority, ResolveError, ResolveErrorCode, ResolvedStream, VideoId,
};
use watapachi_bot::guild::message::GuildMessage;
use watapachi_bot::guild::notifier::{Notice, Notifier};
use watapachi_bot::resolver::Resolver;

fn video(id: &str) -> VideoId {
	VideoId(id.to_string())
}

fn stream(id: &str) -> ResolvedStream {
	ResolvedStream {
		video_id: video(id),
		title: format!("title-{id}"),
		duration_s: Some(10),
		url: "http://localhost/audio".to_string(),
		http_headers: BTreeMap::new(),
		filesize: 100,
		format_id: "251".to_string(),
		expires_at: None,
		available_at: None,
		is_live: false,
		protocol: "https".to_string(),
	}
}

fn request(id: &str) -> PlayRequest {
	PlayRequest {
		video_id: video(id),
		stream: None,
		start_at: Duration::ZERO,
		volume: 1.0,
		use_pipe: false,
		paused: false,
	}
}

#[tokio::test]
async fn fake_audio_records_calls_and_emits_events() {
	let audio = FakeAudio::default();
	let guild = GuildId::new(1);
	let channel = ChannelId::new(2);
	let other = ChannelId::new(3);
	let (tx, mut rx) = mpsc::channel(8);

	let mut session = audio.connect(guild, channel, tx).await.expect("connect");
	let first = session.play(request("a")).await.expect("play");
	let second = session.preload(request("b")).await.expect("preload");
	assert_eq!((first, second), (TrackId(1), TrackId(2)));
	session.start_preloaded(second).await.expect("start");
	session.pause().await.expect("pause");
	session.resume().await.expect("resume");
	let moved = session.seek(Duration::from_secs(5)).await.expect("seek");
	assert_eq!(moved, Duration::from_secs(5));
	session.set_volume(0.5).await.expect("volume");
	assert_eq!(session.position().await, Ok(Duration::from_secs(5)));
	session.move_to(other).await.expect("move");
	session.stop().await.expect("stop");

	// fail_next は次の 1 回だけ失敗させる
	audio.fail_next(AudioOp::Pause, AudioError::Timeout);
	assert_eq!(session.pause().await, Err(AudioError::Timeout));
	assert_eq!(session.pause().await, Ok(()));
	// 別の操作には影響しない
	audio.fail_next(AudioOp::Play, AudioError::NotConnected);
	assert_eq!(session.resume().await, Ok(()));
	assert_eq!(
		session.play(request("c")).await,
		Err(AudioError::NotConnected)
	);

	session.disconnect().await.expect("disconnect");

	assert_eq!(
		audio.calls(),
		vec![
			AudioCall::Connect { guild, channel },
			AudioCall::Play(request("a")),
			AudioCall::Preload(request("b")),
			AudioCall::StartPreloaded(TrackId(2)),
			AudioCall::Pause,
			AudioCall::Resume,
			AudioCall::Seek(Duration::from_secs(5)),
			AudioCall::SetVolume(0.5),
			AudioCall::Position,
			AudioCall::MoveTo(other),
			AudioCall::Stop,
			AudioCall::Pause,
			AudioCall::Pause,
			AudioCall::Resume,
			AudioCall::Play(request("c")),
			AudioCall::Disconnect,
		]
	);

	// connect で渡した送信口へ、emit したイベントが順に届く
	audio.emit(AudioEvent::TrackStarted { track: first }).await;
	audio
		.emit(AudioEvent::Disconnected {
			reason: DisconnectKind::Lost,
		})
		.await;
	match rx.recv().await {
		Some(GuildMessage::Audio(AudioEvent::TrackStarted { track })) => assert_eq!(track, first),
		other => panic!("想定外のメッセージ: {other:?}"),
	}
	match rx.recv().await {
		Some(GuildMessage::Audio(AudioEvent::Disconnected { reason })) => {
			assert_eq!(reason, DisconnectKind::Lost)
		}
		other => panic!("想定外のメッセージ: {other:?}"),
	}
}

#[tokio::test]
async fn fake_resolver_can_hold_a_result_until_released() {
	let resolver = FakeResolver::default();
	let id = video("abc");
	resolver.set_result(&id, Ok(stream("abc")));
	resolver.hold(&id);

	let task = {
		let resolver = resolver.clone();
		let id = id.clone();
		tokio::spawn(async move { resolver.resolve(&id, Priority::Interactive).await })
	};
	// 現在のスレッドで動くランタイムなので、譲れば解決のタスクが保留で止まるところまで進む
	for _ in 0..10 {
		tokio::task::yield_now().await;
	}
	assert!(!task.is_finished(), "保留中は完了しない");

	resolver.release(&id);
	let result = task.await.expect("join");
	assert_eq!(result, Ok(stream("abc")));

	// 保留していない動画は即座に返り、未指定の動画は失敗する
	assert_eq!(
		resolver.resolve(&id, Priority::Prefetch).await,
		Ok(stream("abc"))
	);
	assert!(matches!(
		resolver.resolve(&video("zzz"), Priority::Prefetch).await,
		Err(ResolveError::Worker { .. })
	));
}

#[tokio::test]
async fn fake_notifier_records_notices_in_order() {
	let notifier = FakeNotifier::default();
	let channel = ChannelId::new(7);
	let first = Notice {
		key: "a",
		args: vec![("x", "1".to_string())],
		card: None,
		lists: vec![],
	};
	let second = Notice {
		key: "b",
		args: vec![],
		card: None,
		lists: vec![],
	};
	notifier.send(channel, first.clone()).await;
	notifier.send(channel, second.clone()).await;
	assert_eq!(notifier.sent(), vec![(channel, first), (channel, second)]);
}

#[tokio::test]
async fn fake_audio_health_and_position_can_be_set() {
	let audio = FakeAudio::default();
	let (tx, _rx) = mpsc::channel(8);
	let session = audio
		.connect(GuildId::new(1), ChannelId::new(2), tx)
		.await
		.expect("connect");

	// 既定は暗号化のフレームだけが増える
	let first = session.health();
	let second = session.health();
	assert_eq!(first.dave_version, Some(HEALTHY_DAVE_VERSION));
	assert_eq!(
		second.encrypted_frames - first.encrypted_frames,
		HEALTHY_FRAMES_PER_SAMPLE
	);
	assert_eq!(second.plaintext_frames, 0);

	// 増え方を変えると、累計にその分だけ足す
	audio.set_health_step(VoiceHealth {
		dave_version: None,
		encrypted_frames: 0,
		plaintext_frames: 7,
		withheld_frames: 0,
	});
	let third = session.health();
	assert_eq!(third.dave_version, None);
	assert_eq!(third.encrypted_frames, second.encrypted_frames);
	assert_eq!(third.plaintext_frames, 7);

	audio.set_position(Duration::from_secs(9));
	assert_eq!(session.position().await, Ok(Duration::from_secs(9)));
}

#[tokio::test]
async fn fake_resolver_returns_pushed_results_first() {
	let resolver = FakeResolver::default();
	let id = video("abc");
	let limited = || {
		Err(ResolveError::Worker {
			code: ResolveErrorCode::RateLimited,
			message: String::new(),
		})
	};
	resolver.set_result(&id, Ok(stream("abc")));
	resolver.push_results(&id, vec![limited(), limited()]);

	// 足した結果を resolve と refresh が呼ばれた順に返し、尽きたら指定の結果に戻る
	assert_eq!(
		resolver.resolve(&id, Priority::Interactive).await,
		limited()
	);
	assert_eq!(resolver.refresh(&id, Some("251")).await, limited());
	assert_eq!(
		resolver.resolve(&id, Priority::Interactive).await,
		Ok(stream("abc"))
	);
	assert_eq!(
		resolver.refresh_calls(),
		vec![(id.clone(), Some("251".to_string()))]
	);
	assert_eq!(resolver.resolve_count(&id), 3);
}
