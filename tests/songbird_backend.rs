//! SongbirdBackend の試験。
//!
//! - 接続の無い `Call::standalone` ではミキサーが曲を読み進めず、`TrackEvent` が届かない
//!   (2026-11-16 に確認)。そのため曲の再生と終わりのイベントは単体の変換
//!   (`ended_event`、`disconnect_kind`、`TrackGate`、`EventSink`) で確かめ、実機の確認は Discord で行う
//! - `TrackId` の払い出しと InputFactory の組み立ては、接続の無い Call でも確かめられる

#[path = "support/python.rs"]
mod python;
#[path = "support/wav.rs"]
mod wav;

use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use songbird::events::context_data::DisconnectReason;
use songbird::input::{Input, LiveInput};
use songbird::model::CloseCode as VoiceCloseCode;
use songbird::tracks::{PlayMode, ReadyState, TrackState};
use songbird::Call;
use tokio::sync::{mpsc, Mutex};
use twilight_model::id::Id;
use watapachi_bot::audio::backend::{AudioError, AudioEvent, DisconnectKind, PlayRequest, TrackId};
use watapachi_bot::audio::chunked::{
	ChunkConfig, ChunkError, ChunkFailure, UrlRefresher, EXPIRY_RECHECK_INTERVAL,
};
use watapachi_bot::audio::limits::{VoiceLimits, OUTER_MARGIN};
use watapachi_bot::audio::pipe::PipeSpec;
use watapachi_bot::audio::songbird_backend::{
	disconnect_kind, effective_position, ended_event, session_from_call, track_start_ready,
	CallOwners, EventSink, InputFactory, TrackGate,
};
use watapachi_bot::config::VoiceConfig;
use watapachi_bot::domain::{GuildId, ResolveError, ResolvedStream, VideoId};
use watapachi_bot::guild::message::GuildMessage;
use watapachi_bot::platform::job::Job;

/// イベントの送信を待つ上限。
const SEND_TIMEOUT: Duration = Duration::from_secs(2);
/// 届かないことを確かめるときに待つ時間。
const QUIET: Duration = Duration::from_millis(500);
/// 試験で使う動画 ID。
const VIDEO_ID: &str = "dQw4w9WgXcQ";

/// 取り直しを常に失敗させる偽の UrlRefresher。
struct NoRefresh;

#[async_trait]
impl UrlRefresher for NoRefresh {
	async fn refresh(&self, _: &VideoId, _: &str) -> Result<ResolvedStream, ResolveError> {
		Err(ResolveError::Unavailable)
	}
}

fn guild(id: u64) -> GuildId {
	Id::new(id)
}

fn voice() -> VoiceConfig {
	VoiceConfig {
		call_timeout_secs: 5,
		join_timeout_secs: 5,
	}
}

fn chunk_config() -> ChunkConfig {
	ChunkConfig {
		chunk_size: 64 * 1024,
		initial_chunk_size: 0,
		fetch_timeout: Duration::from_secs(5),
		refresh_timeout: Duration::from_secs(5),
		attempts: 2,
		refresh_margin: Duration::ZERO,
		available_wait_max: Duration::ZERO,
		expiry_recheck_interval: EXPIRY_RECHECK_INTERVAL,
	}
}

/// tests/fixtures 以下のファイルの絶対パス (文字列)。
fn fixture(name: &str) -> String {
	Path::new(env!("CARGO_MANIFEST_DIR"))
		.join("tests/fixtures")
		.join(name)
		.to_string_lossy()
		.into_owned()
}

/// 偽 yt-dlp を起動する InputFactory。
fn factory(watch_url: &str) -> Arc<InputFactory> {
	Arc::new(InputFactory {
		client: reqwest::Client::new(),
		refresher: Arc::new(NoRefresh),
		chunk: chunk_config(),
		pipe: Arc::new(ArcSwap::from_pointee(PipeSpec {
			python: python::test_python(),
			args: vec![fixture("fake_ytdlp.py")],
		})),
		job: Arc::new(Job::new().unwrap()),
		watch_url: watch_url.to_owned(),
	})
}

fn resolved(url: &str) -> ResolvedStream {
	ResolvedStream {
		video_id: VideoId(VIDEO_ID.to_owned()),
		title: "title".to_owned(),
		duration_s: Some(1),
		url: url.to_owned(),
		http_headers: BTreeMap::new(),
		filesize: 1024,
		format_id: "251".to_owned(),
		expires_at: None,
		available_at: None,
		is_live: false,
		protocol: "https".to_owned(),
	}
}

fn request(stream: Option<ResolvedStream>, use_pipe: bool) -> PlayRequest {
	PlayRequest {
		video_id: VideoId(VIDEO_ID.to_owned()),
		stream,
		start_at: Duration::ZERO,
		volume: 0.5,
		use_pipe,
		paused: false,
	}
}

/// 受信口から次のイベントを待つ。
async fn next_event(rx: &mut mpsc::Receiver<GuildMessage>) -> Option<AudioEvent> {
	match tokio::time::timeout(SEND_TIMEOUT, rx.recv()).await {
		Ok(Some(GuildMessage::Audio(event))) => Some(event),
		_ => None,
	}
}

#[test]
fn ended_event_uses_chunk_failure() {
	let failure = ChunkFailure::default();
	assert_eq!(
		ended_event(TrackId(1), Some(&failure)),
		AudioEvent::TrackEnded { track: TrackId(1) }
	);
	failure.record_for_test(ChunkError::Forbidden);
	assert!(matches!(
		ended_event(TrackId(1), Some(&failure)),
		AudioEvent::TrackFailed {
			track: TrackId(1),
			error: AudioError::Failed(_)
		}
	));
	assert_eq!(
		ended_event(TrackId(2), None),
		AudioEvent::TrackEnded { track: TrackId(2) }
	);
}

#[test]
fn disconnect_kind_mapping() {
	assert_eq!(
		disconnect_kind(Some(DisconnectReason::WsClosed(Some(
			VoiceCloseCode::Disconnected
		)))),
		DisconnectKind::Kicked
	);
	assert_eq!(
		disconnect_kind(Some(DisconnectReason::Requested)),
		DisconnectKind::Kicked
	);
	assert_eq!(
		disconnect_kind(Some(DisconnectReason::TimedOut)),
		DisconnectKind::Lost
	);
	assert_eq!(
		disconnect_kind(Some(DisconnectReason::WsClosed(None))),
		DisconnectKind::Lost
	);
	assert_eq!(disconnect_kind(None), DisconnectKind::Lost);
}

#[test]
fn track_gate_finishes_once() {
	let failure = ChunkFailure::default();
	let gate = TrackGate::new(TrackId(7), Some(failure.clone()));
	assert_eq!(
		gate.start(),
		Some(AudioEvent::TrackStarted { track: TrackId(7) })
	);
	assert_eq!(gate.start(), None, "開始は最初の 1 回だけ");
	failure.record_for_test(ChunkError::Timeout);
	assert!(matches!(
		gate.finish(None),
		Some(AudioEvent::TrackFailed {
			track: TrackId(7),
			..
		})
	));
	assert_eq!(gate.finish(None), None, "終わりは 1 回だけ");
	assert_eq!(
		gate.finish(Some("decode")),
		None,
		"終わった後の失敗も送らない"
	);
}

#[test]
fn track_gate_play_error_is_failed() {
	let gate = TrackGate::new(TrackId(3), Some(ChunkFailure::default()));
	assert!(matches!(
		gate.finish(Some("probe failed")),
		Some(AudioEvent::TrackFailed {
			track: TrackId(3),
			error: AudioError::Failed(_)
		})
	));
	assert_eq!(gate.finish(None), None);
}

#[test]
fn silenced_gate_sends_nothing() {
	let gate = TrackGate::new(TrackId(4), None);
	gate.silence();
	assert_eq!(gate.start(), None);
	assert_eq!(gate.finish(None), None);
}

/// 曲の状態を作る。
fn track_state(playing: PlayMode, ready: ReadyState) -> TrackState {
	TrackState {
		playing,
		ready,
		..TrackState::default()
	}
}

#[test]
fn track_start_waits_for_playable() {
	// 準備済みの曲の再生 (`TrackEvent::Play`) は入力の準備の前に届きうる。準備中は開始としない
	let preparing = track_state(PlayMode::Play, ReadyState::Preparing);
	assert!(!track_start_ready(Some(&preparing)));
	let uninitialised = track_state(PlayMode::Play, ReadyState::Uninitialised);
	assert!(!track_start_ready(Some(&uninitialised)));
	// 準備ができて再生中なら開始 (Play と Playable のどちらで受けても)
	let playable = track_state(PlayMode::Play, ReadyState::Playable);
	assert!(track_start_ready(Some(&playable)));
	// 一時停止のまま準備ができただけでは開始としない
	let paused = track_state(PlayMode::Pause, ReadyState::Playable);
	assert!(!track_start_ready(Some(&paused)));
	assert!(!track_start_ready(None));
}

#[test]
fn outer_voice_limits_cover_inner_steps() {
	// 外側 (アクター) の上限は、内側 (バックエンド) の時間切れと後始末の合計より長い
	let config = VoiceConfig {
		call_timeout_secs: 10,
		join_timeout_secs: 15,
	};
	let limits = VoiceLimits::new(&config);
	let call = Duration::from_secs(10);
	let join = Duration::from_secs(15);
	assert_eq!(limits.call(), call);
	assert_eq!(limits.join(), join);
	// 接続: 参加 → (登録 → 削除) または (時間切れ → 削除)
	assert_eq!(limits.connect(), join + call * 2 + OUTER_MARGIN);
	// 移動: 参加だけ
	assert_eq!(limits.move_to(), join + OUTER_MARGIN);
	assert!(limits.move_to() > join);
	// 切断: Call の停止 → 削除
	assert_eq!(limits.disconnect(), call * 2 + OUTER_MARGIN);
	assert!(OUTER_MARGIN > Duration::ZERO);
}

#[test]
fn effective_position_holds_pending_start() {
	let start = Duration::from_secs(90);
	assert_eq!(effective_position(Duration::ZERO, Some(start)), start);
	assert_eq!(
		effective_position(Duration::from_secs(95), Some(start)),
		Duration::from_secs(95)
	);
	assert_eq!(
		effective_position(Duration::from_secs(3), None),
		Duration::from_secs(3)
	);
}

#[test]
fn gate_position_waits_for_start_seek() {
	let gate = TrackGate::new(TrackId(5), None);
	let start = Duration::from_secs(90);
	gate.set_pending_start(start);
	// シークが終わるまで Songbird の位置は 0 付近。開始位置を返す
	assert_eq!(gate.observe_position(Duration::ZERO), start);
	assert_eq!(gate.observe_position(Duration::from_millis(400)), start);
	// 開始位置に達したらシークが終わったとみなし、以後は観測した位置をそのまま返す
	assert_eq!(
		gate.observe_position(Duration::from_secs(91)),
		Duration::from_secs(91)
	);
	assert_eq!(
		gate.observe_position(Duration::from_secs(10)),
		Duration::from_secs(10),
		"利用者が前へシークした後は 0 側の値も返す"
	);
}

#[test]
fn cleared_pending_start_returns_observed() {
	let gate = TrackGate::new(TrackId(6), None);
	gate.set_pending_start(Duration::from_secs(90));
	gate.clear_pending_start();
	assert_eq!(
		gate.observe_position(Duration::from_secs(2)),
		Duration::from_secs(2)
	);
}

#[tokio::test]
async fn newer_session_owns_the_call() {
	let owners = CallOwners::default();
	let old = owners.claim(guild(1)).await;
	let new = owners.claim(guild(1)).await;
	let other = owners.claim(guild(2)).await;
	assert_ne!(old, new);
	assert!(
		!owners.is_owner(guild(1), old).await,
		"古いセッションは持ち主でない"
	);
	assert!(owners.is_owner(guild(1), new).await);
	assert!(owners.is_owner(guild(2), other).await, "サーバーごとに別");
	assert!(
		!owners.release(guild(1), old).await,
		"古いセッションは手放せない"
	);
	assert!(owners.is_owner(guild(1), new).await);
	assert!(owners.release(guild(1), new).await);
	assert!(!owners.is_owner(guild(1), new).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn event_sink_closed_sends_nothing() {
	let (tx, mut rx) = mpsc::channel(4);
	let sink = EventSink::new(guild(1), tx, SEND_TIMEOUT);
	sink.emit(AudioEvent::Reconnected);
	assert_eq!(next_event(&mut rx).await, Some(AudioEvent::Reconnected));
	sink.close();
	assert!(!sink.is_open());
	sink.emit(AudioEvent::Disconnected {
		reason: DisconnectKind::Lost,
	});
	assert!(
		tokio::time::timeout(QUIET, rx.recv()).await.is_err(),
		"閉じた後は送らない"
	);
}

#[tokio::test(flavor = "multi_thread")]
async fn event_sink_waits_when_mailbox_full() {
	let (tx, mut rx) = mpsc::channel(1);
	let sink = EventSink::new(guild(1), tx, SEND_TIMEOUT);
	sink.emit(AudioEvent::TrackEnded { track: TrackId(1) });
	sink.emit(AudioEvent::TrackEnded { track: TrackId(2) });
	let mut got = Vec::new();
	for _ in 0..2 {
		got.push(next_event(&mut rx).await);
	}
	got.sort_by_key(|e| match e {
		Some(AudioEvent::TrackEnded { track }) => track.0,
		_ => u64::MAX,
	});
	assert_eq!(
		got,
		vec![
			Some(AudioEvent::TrackEnded { track: TrackId(1) }),
			Some(AudioEvent::TrackEnded { track: TrackId(2) }),
		],
		"満杯でも捨てずに待って送る"
	);
}

/// メールボックスが満杯になった後も、イベントが `emit` の順に届くこと。
///
/// - 満杯の間のイベントを別々のタスクで送ると、タスクの実行順で追い越しが起きる
/// - 受け取りで空きができた直後の `emit` も、待っている前のイベントを追い越さない
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_sink_keeps_order_when_mailbox_full() {
	const COUNT: u64 = 50;
	let (tx, mut rx) = mpsc::channel(1);
	let sink = EventSink::new(guild(1), tx, SEND_TIMEOUT);
	for id in 1..=COUNT / 2 {
		sink.emit(AudioEvent::TrackEnded { track: TrackId(id) });
	}
	let mut got = Vec::new();
	for id in COUNT / 2 + 1..=COUNT {
		got.push(next_event(&mut rx).await);
		sink.emit(AudioEvent::TrackEnded { track: TrackId(id) });
	}
	while got.len() < COUNT as usize {
		got.push(next_event(&mut rx).await);
	}
	let expected: Vec<_> = (1..=COUNT)
		.map(|id| Some(AudioEvent::TrackEnded { track: TrackId(id) }))
		.collect();
	assert_eq!(got, expected, "届く順序が送った順と違う");
	assert!(
		tokio::time::timeout(QUIET, rx.recv()).await.is_err(),
		"余分なイベントが届いた"
	);
}

/// 止めた曲は、Songbird が失敗を報告しても `TrackEnded` で終わること。
///
/// - 止めるときに取得を取り消すため、入力が読めなくなったことを Songbird が失敗と報告しうる
/// - 止める前に入力の失敗が記録されていれば `TrackFailed` のまま
#[test]
fn stopped_gate_ignores_play_error() {
	let gate = TrackGate::new(TrackId(11), Some(ChunkFailure::default()));
	gate.mark_stopped();
	assert_eq!(
		gate.finish(Some("parsing failed")),
		Some(AudioEvent::TrackEnded { track: TrackId(11) })
	);

	let failure = ChunkFailure::default();
	failure.record_for_test(ChunkError::Status(404));
	let gate = TrackGate::new(TrackId(12), Some(failure));
	gate.mark_stopped();
	assert!(matches!(
		gate.finish(Some("parsing failed")),
		Some(AudioEvent::TrackFailed {
			track: TrackId(12),
			..
		})
	));
}

/// シークは完了を待たずに、移る先の位置を返すこと。
///
/// - 入力の取得 (応答しない取得先) を待つと、呼び出しの上限を超えて切断として扱われる
#[tokio::test(flavor = "multi_thread")]
async fn seek_returns_without_waiting_for_fetch() {
	let inputs = factory("https://www.youtube.com/watch?v={id}");
	let (tx, _rx) = mpsc::channel(16);
	let call = Arc::new(Mutex::new(Call::standalone(
		guild(3),
		Id::<twilight_model::id::marker::UserMarker>::new(99),
	)));
	let mut session = session_from_call(
		guild(3),
		call,
		None,
		inputs,
		voice(),
		Arc::new(AtomicU64::new(1)),
		tx,
		SEND_TIMEOUT,
	);
	session
		.play(request(Some(resolved("http://127.0.0.1:9/never")), false))
		.await
		.unwrap();
	let target = Duration::from_secs(90);
	let moved = tokio::time::timeout(Duration::from_secs(2), session.seek(target))
		.await
		.expect("シークの完了を待った")
		.unwrap();
	assert_eq!(moved, target);
	session.disconnect().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn track_ids_unique_across_sessions() {
	let track_ids = Arc::new(AtomicU64::new(1));
	let inputs = factory("https://www.youtube.com/watch?v={id}");
	let (tx, _rx) = mpsc::channel(16);
	let mut ids = Vec::new();
	for g in [1u64, 2] {
		let call = Arc::new(Mutex::new(Call::standalone(
			guild(g),
			Id::<twilight_model::id::marker::UserMarker>::new(99),
		)));
		let mut session = session_from_call(
			guild(g),
			call,
			None,
			Arc::clone(&inputs),
			voice(),
			Arc::clone(&track_ids),
			tx.clone(),
			SEND_TIMEOUT,
		);
		for _ in 0..2 {
			let stream = resolved("http://127.0.0.1:9/never");
			ids.push(session.play(request(Some(stream), false)).await.unwrap());
		}
		session.disconnect().await.unwrap();
	}
	let mut unique = ids.clone();
	unique.sort_by_key(|t| t.0);
	unique.dedup();
	assert_eq!(unique.len(), ids.len(), "払い出しが重ならない: {ids:?}");
}

#[test]
fn factory_pipe_uses_watch_url() {
	let dir = tempfile::tempdir().unwrap();
	let bytes = wav::sine_wav(0.2);
	std::fs::write(dir.path().join(format!("{VIDEO_ID}.wav")), &bytes).unwrap();
	let encoded: String =
		url::form_urlencoded::byte_serialize(dir.path().to_string_lossy().as_bytes()).collect();
	let inputs = factory(&format!(
		"http://fixture.invalid/?file={encoded}/{{id}}.wav"
	));
	let built = inputs.build(&request(None, true)).unwrap();
	assert!(built.cancel.is_none(), "PipeInput には取り消しの口が無い");
	let failure = built.failure;
	let Input::Live(LiveInput::Raw(mut stream), None) = built.input else {
		panic!("Live(Raw) の入力であること");
	};
	let mut out = Vec::new();
	stream.input.read_to_end(&mut out).unwrap();
	assert_eq!(out, bytes);
	assert!(
		failure.get().is_none(),
		"正常に終わった子は失敗として記録しない"
	);
}

/// PipeInput の子が異常終了したら、標準出力の終わりで失敗を記録し、曲の終わりを `TrackFailed` にすること。
#[test]
fn factory_pipe_records_child_failure() {
	let dir = tempfile::tempdir().unwrap();
	let encoded: String =
		url::form_urlencoded::byte_serialize(dir.path().to_string_lossy().as_bytes()).collect();
	// 存在しないファイルを指す。偽 yt-dlp は何も書かずに終了コード 1 で終わる
	let inputs = factory(&format!(
		"http://fixture.invalid/?file={encoded}/missing-{{id}}.wav"
	));
	let built = inputs.build(&request(None, true)).unwrap();
	let failure = built.failure;
	let Input::Live(LiveInput::Raw(mut stream), None) = built.input else {
		panic!("Live(Raw) の入力であること");
	};
	let mut out = Vec::new();
	stream.input.read_to_end(&mut out).unwrap();
	assert!(out.is_empty());
	assert!(
		matches!(failure.get().as_deref(), Some(ChunkError::Pipe(_))),
		"異常終了が記録されていない: {:?}",
		failure.get()
	);
	assert!(matches!(
		ended_event(TrackId(9), Some(&failure)),
		AudioEvent::TrackFailed {
			track: TrackId(9),
			..
		}
	));
}

#[test]
fn factory_chunked_returns_failure() {
	let inputs = factory("https://www.youtube.com/watch?v={id}");
	let built = inputs
		.build(&request(Some(resolved("http://127.0.0.1:9/a")), false))
		.unwrap();
	assert!(matches!(built.input, Input::Lazy(_)));
	assert!(built.failure.get().is_none());
	let cancel = built.cancel.expect("ChunkedRequest の取り消しの口");
	assert!(!cancel.is_cancelled());
}

#[test]
fn factory_without_stream_fails() {
	let inputs = factory("https://www.youtube.com/watch?v={id}");
	assert!(matches!(
		inputs.build(&request(None, false)),
		Err(AudioError::Failed(_))
	));
}
