//! 試験用の音声バックエンド。呼び出しの記録、イベントの注入、失敗の注入ができる。
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{mpsc, watch};
use watapachi_bot::audio::backend::{
	AudioBackend, AudioError, AudioEvent, PlayRequest, TrackId, VoiceHealth, VoiceSession,
};
use watapachi_bot::domain::{ChannelId, GuildId};
use watapachi_bot::guild::message::GuildMessage;

/// `fail_next` で失敗させる操作の種類。`AudioBackend` / `VoiceSession` のメソッドに 1 対 1 で対応する。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AudioOp {
	Connect,
	Play,
	Preload,
	StartPreloaded,
	DropPreloaded,
	Stop,
	Pause,
	Resume,
	Seek,
	SetVolume,
	Position,
	MoveTo,
	Disconnect,
}

/// 記録された呼び出し。引数を持つ。
#[derive(Debug, Clone, PartialEq)]
pub enum AudioCall {
	Connect { guild: GuildId, channel: ChannelId },
	Play(PlayRequest),
	Preload(PlayRequest),
	StartPreloaded(TrackId),
	DropPreloaded(TrackId),
	Stop,
	Pause,
	Resume,
	Seek(Duration),
	SetVolume(f32),
	Position,
	MoveTo(ChannelId),
	Disconnect,
}

#[derive(Default)]
struct State {
	calls: Vec<AudioCall>,
	failures: HashMap<AudioOp, AudioError>,
	events: Option<mpsc::Sender<GuildMessage>>,
	next_track: u64,
	position: Duration,
	/// `connect` の保留。値が true になったら解放済み。
	connect_hold: Option<watch::Sender<bool>>,
	/// `disconnect` の保留。値が true になったら解放済み。
	disconnect_hold: Option<watch::Sender<bool>>,
	/// `health` が 1 回ごとに足す量。None なら暗号化のフレームだけが増える健全な既定の動き。
	health_step: Option<VoiceHealth>,
	/// `health` が最後に返した値 (累計)。
	health_total: VoiceHealth,
}

impl State {
	/// 呼び出しを記録し、`fail_next` が指定されていればそのエラーを一度だけ返す。
	fn record(&mut self, call: AudioCall, op: AudioOp) -> Result<(), AudioError> {
		self.calls.push(call);
		match self.failures.remove(&op) {
			Some(error) => Err(error),
			None => Ok(()),
		}
	}

	/// 次の `TrackId` を払い出す。
	fn alloc_track(&mut self) -> TrackId {
		self.next_track += 1;
		TrackId(self.next_track)
	}

	/// 累計に 1 回分を足して返す。
	fn next_health(&mut self) -> VoiceHealth {
		let step = self.health_step.clone().unwrap_or(VoiceHealth {
			dave_version: Some(HEALTHY_DAVE_VERSION),
			encrypted_frames: HEALTHY_FRAMES_PER_SAMPLE,
			plaintext_frames: 0,
			withheld_frames: 0,
		});
		self.health_total = VoiceHealth {
			dave_version: step.dave_version,
			encrypted_frames: self.health_total.encrypted_frames + step.encrypted_frames,
			plaintext_frames: self.health_total.plaintext_frames + step.plaintext_frames,
			withheld_frames: self.health_total.withheld_frames + step.withheld_frames,
		};
		self.health_total.clone()
	}
}

/// 既定の `health` が返す DAVE の版。
pub const HEALTHY_DAVE_VERSION: u16 = 1;
/// 既定の `health` が 1 回ごとに増やす暗号化のフレーム数 (1 秒の 20ms フレームに相当)。
pub const HEALTHY_FRAMES_PER_SAMPLE: u64 = 50;

/// 偽の `AudioBackend`。`clone` したものは状態を共有する。
///
/// - `connect` が返すセッションも同じ状態を共有する
/// - `TrackId` は 1 から連番で払い出す
/// - `position` は最後の `seek` または `set_position` の位置を返す (初期値は 0)
/// - `health` は呼ばれるたびに累計を増やして返す。既定は暗号化のフレームだけが増える健全な動き。
///   `set_health_step` で 1 回ごとの増え方を変えられる (全て 0 なら送信なし)
/// - `hold_connect` の後の `connect` は、呼び出しを記録してから `release_connect` まで完了しない。
///   `fail_next` の失敗は解放の後に判定する
///
/// 呼び出し方:
/// ```ignore
/// let audio = FakeAudio::default();
/// let session = audio.connect(guild, channel, tx).await?;
/// audio.fail_next(AudioOp::Play, AudioError::Timeout);
/// audio.emit(AudioEvent::Reconnected).await;
/// assert_eq!(audio.calls(), vec![...]);
/// ```
#[derive(Clone, Default)]
pub struct FakeAudio {
	state: Arc<Mutex<State>>,
}

impl FakeAudio {
	/// これまでの呼び出しを、呼ばれた順に返す。
	pub fn calls(&self) -> Vec<AudioCall> {
		self.state
			.lock()
			.map(|s| s.calls.clone())
			.unwrap_or_default()
	}

	/// 指定の操作の次の呼び出しを、一度だけ失敗させる。以降の呼び出しは成功する。
	pub fn fail_next(&self, op: AudioOp, error: AudioError) {
		if let Ok(mut s) = self.state.lock() {
			s.failures.insert(op, error);
		}
	}

	/// `position` が返す再生位置を指定する。
	pub fn set_position(&self, position: Duration) {
		if let Ok(mut s) = self.state.lock() {
			s.position = position;
		}
	}

	/// `health` が 1 回ごとに足す量を指定する。`dave_version` はそのまま返す値になる。
	pub fn set_health_step(&self, step: VoiceHealth) {
		if let Ok(mut s) = self.state.lock() {
			s.health_step = Some(step);
		}
	}

	/// 以後の `connect` を保留する。`release_connect` まで完了しない。
	pub fn hold_connect(&self) {
		if let Ok(mut s) = self.state.lock() {
			let (tx, _rx) = watch::channel(false);
			s.connect_hold = Some(tx);
		}
	}

	/// `connect` の保留を解く。待っていた `connect` は、その時点の `fail_next` の指定に従って完了する。
	pub fn release_connect(&self) {
		if let Ok(mut s) = self.state.lock() {
			if let Some(tx) = s.connect_hold.take() {
				tx.send_replace(true);
			}
		}
	}

	/// 以後の `disconnect` を保留する。`release_disconnect` まで完了しない (呼び出しは記録する)。
	pub fn hold_disconnect(&self) {
		if let Ok(mut s) = self.state.lock() {
			let (tx, _rx) = watch::channel(false);
			s.disconnect_hold = Some(tx);
		}
	}

	/// `disconnect` の保留を解く。
	pub fn release_disconnect(&self) {
		if let Ok(mut s) = self.state.lock() {
			if let Some(tx) = s.disconnect_hold.take() {
				tx.send_replace(true);
			}
		}
	}

	/// `connect` で受け取った送信口へ、イベントを `GuildMessage::Audio` として送る。
	///
	/// # Panics
	/// 試験用の補助のため、未接続または受信側が閉じていれば panic する。
	pub async fn emit(&self, event: AudioEvent) {
		let tx = self
			.state
			.lock()
			.ok()
			.and_then(|s| s.events.clone())
			.expect("emit は connect の後に呼ぶ");
		tx.send(GuildMessage::Audio(event))
			.await
			.expect("受信側が閉じている");
	}
}

#[async_trait]
impl AudioBackend for FakeAudio {
	async fn connect(
		&self,
		guild: GuildId,
		channel: ChannelId,
		events: mpsc::Sender<GuildMessage>,
	) -> Result<Box<dyn VoiceSession>, AudioError> {
		let mut hold = {
			let mut s = self.state.lock().map_err(|_| poisoned())?;
			s.calls.push(AudioCall::Connect { guild, channel });
			s.connect_hold.as_ref().map(|tx| tx.subscribe())
		};
		if let Some(rx) = hold.as_mut() {
			// 送信側が先に消えても (解放済み)、値は残るので待ちは解ける
			let _ = rx.wait_for(|released| *released).await;
		}
		{
			let mut s = self.state.lock().map_err(|_| poisoned())?;
			if let Some(error) = s.failures.remove(&AudioOp::Connect) {
				return Err(error);
			}
			s.events = Some(events);
		}
		Ok(Box::new(FakeSession {
			state: self.state.clone(),
		}))
	}
}

/// 偽のセッション。`FakeAudio` と状態を共有する。
struct FakeSession {
	state: Arc<Mutex<State>>,
}

/// ロックの汚染を `AudioError` に直す。
fn poisoned() -> AudioError {
	AudioError::Failed("FakeAudio の状態が壊れています".to_string())
}

impl FakeSession {
	/// 状態のロックを取って `f` を実行する。
	fn with<T>(
		&self,
		f: impl FnOnce(&mut State) -> Result<T, AudioError>,
	) -> Result<T, AudioError> {
		let mut s = self.state.lock().map_err(|_| poisoned())?;
		f(&mut s)
	}
}

#[async_trait]
impl VoiceSession for FakeSession {
	async fn play(&mut self, request: PlayRequest) -> Result<TrackId, AudioError> {
		self.with(|s| {
			s.record(AudioCall::Play(request), AudioOp::Play)?;
			Ok(s.alloc_track())
		})
	}

	async fn preload(&mut self, request: PlayRequest) -> Result<TrackId, AudioError> {
		self.with(|s| {
			s.record(AudioCall::Preload(request), AudioOp::Preload)?;
			Ok(s.alloc_track())
		})
	}

	async fn start_preloaded(&mut self, track: TrackId) -> Result<(), AudioError> {
		self.with(|s| s.record(AudioCall::StartPreloaded(track), AudioOp::StartPreloaded))
	}

	async fn drop_preloaded(&mut self, track: TrackId) -> Result<(), AudioError> {
		self.with(|s| s.record(AudioCall::DropPreloaded(track), AudioOp::DropPreloaded))
	}

	async fn stop(&mut self) -> Result<(), AudioError> {
		self.with(|s| s.record(AudioCall::Stop, AudioOp::Stop))
	}

	async fn pause(&mut self) -> Result<(), AudioError> {
		self.with(|s| s.record(AudioCall::Pause, AudioOp::Pause))
	}

	async fn resume(&mut self) -> Result<(), AudioError> {
		self.with(|s| s.record(AudioCall::Resume, AudioOp::Resume))
	}

	async fn seek(&mut self, position: Duration) -> Result<Duration, AudioError> {
		self.with(|s| {
			s.record(AudioCall::Seek(position), AudioOp::Seek)?;
			s.position = position;
			Ok(position)
		})
	}

	async fn set_volume(&mut self, volume: f32) -> Result<(), AudioError> {
		self.with(|s| s.record(AudioCall::SetVolume(volume), AudioOp::SetVolume))
	}

	async fn position(&self) -> Result<Duration, AudioError> {
		self.with(|s| {
			s.record(AudioCall::Position, AudioOp::Position)?;
			Ok(s.position)
		})
	}

	async fn move_to(&mut self, channel: ChannelId) -> Result<(), AudioError> {
		self.with(|s| s.record(AudioCall::MoveTo(channel), AudioOp::MoveTo))
	}

	async fn disconnect(self: Box<Self>) -> Result<(), AudioError> {
		let mut hold = self
			.state
			.lock()
			.ok()
			.and_then(|s| s.disconnect_hold.as_ref().map(|tx| tx.subscribe()));
		if let Some(rx) = hold.as_mut() {
			// 送信側が先に消えても (解放済み)、値は残るので待ちは解ける
			let _ = rx.wait_for(|released| *released).await;
		}
		self.with(|s| s.record(AudioCall::Disconnect, AudioOp::Disconnect))
	}

	fn health(&self) -> VoiceHealth {
		self.state
			.lock()
			.map(|mut s| s.next_health())
			.unwrap_or_default()
	}
}
