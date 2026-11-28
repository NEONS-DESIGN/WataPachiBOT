//! セッションからアクターへ送るイベントの組み立てと送り口。
//!
//! - `EventSink` はセッションごとの「開いている」印と、メールボックスが満杯のときの待ち行列を持つ
//! - `ended_event` と `disconnect_kind` は Songbird の状態からアクターへのイベントを決める (純粋)
//! - `is_left_channel` は、切断が移動の前のチャンネルの接続の終わりかを決める (純粋)

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use songbird::events::context_data::DisconnectReason;
use songbird::model::CloseCode as VoiceCloseCode;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::audio::backend::{AudioError, AudioEvent, DisconnectKind, TrackId};
use crate::audio::chunked::ChunkFailure;
use crate::domain::GuildId;
use crate::guild::message::GuildMessage;

/// チャンネルが分からないことを表す値 (`EventSink::voice_channel`)。
const UNKNOWN_CHANNEL: u64 = 0;

/// 切断が、移動の前のチャンネルの接続の終わりかを決める。
///
/// - `?move` で別のチャンネルへ移ると、Discord は前のチャンネルの音声の接続を 4022 (`CallTerminated`) で閉じる。
///   Songbird が新しい接続を始める前にこれを受けると、今の接続の切断として報告する
/// - 報告に付くチャンネルが、セッションの今のチャンネル (移動先) と違えば、前のチャンネルの終わりとみなす
/// - 今のチャンネルが分からなければ (試験の Call など) 前のチャンネルとはみなさない
///
/// # 引数
/// - `current`: セッションの今のチャンネル (`EventSink::voice_channel`)。0 は不明
/// - `disconnected`: 切断の報告に付いたチャンネル (`DisconnectData::channel_id`)
///
/// # 戻り値
/// 前のチャンネルの終わりなら true
///
/// # 例
/// `if is_left_channel(sink.voice_channel(), data.channel_id.get()) { /* 切断として扱わない */ }`
pub fn is_left_channel(current: u64, disconnected: u64) -> bool {
	current != UNKNOWN_CHANNEL && current != disconnected
}

/// 終わった曲のイベントを決める。
///
/// - `ChunkFailure` に失敗が記録されていれば `TrackFailed`、無ければ `TrackEnded`
///
/// # 引数
/// - `track`: 終わった曲
/// - `failure`: 入力 (`ChunkedRequest`、`PipeInput`) の失敗の記録。None は記録が無い入力
///
/// # 戻り値
/// アクターへ送るイベント
///
/// # 例
/// `let event = ended_event(track, failure.as_ref());`
pub fn ended_event(track: TrackId, failure: Option<&ChunkFailure>) -> AudioEvent {
	match failure.and_then(ChunkFailure::get) {
		Some(error) => AudioEvent::TrackFailed {
			track,
			error: AudioError::Failed(error.to_string()),
		},
		None => AudioEvent::TrackEnded { track },
	}
}

/// Songbird の切断の理由を、アクターに伝える切断の種類に変える。
///
/// - Discord に切られた (`WsClosed(Disconnected)`、4014) と、切断の要求 (`Requested`、
///   利用者による移動・追い出しを含む) は `Kicked`
/// - それ以外 (タイムアウト、通信の失敗、理由なし) は `Lost`
///
/// # 引数
/// - `reason`: `DisconnectData::reason`
///
/// # 戻り値
/// 切断の種類
///
/// # 例
/// `let kind = disconnect_kind(data.reason);`
pub fn disconnect_kind(reason: Option<DisconnectReason>) -> DisconnectKind {
	match reason {
		Some(DisconnectReason::WsClosed(Some(VoiceCloseCode::Disconnected)))
		| Some(DisconnectReason::Requested) => DisconnectKind::Kicked,
		_ => DisconnectKind::Lost,
	}
}

/// メールボックスが満杯の間に積んだイベント。`EventSink` の複製で共有する。
#[derive(Default)]
struct Backlog {
	/// 送る順に並べたイベント
	queue: VecDeque<AudioEvent>,
	/// 待ち行列を送るタスクが動いているか。true の間は `emit` もその場で送らずに積む
	draining: bool,
}

/// セッションからアクターへイベントを送る口。「開いている」印を持つ。
///
/// - 閉じた後は何も送らない。閉じるのは `disconnect` とセッションの破棄
/// - その場で送れなければ (メールボックスが満杯)、待ち行列に積み、別タスクが `send_timeout` を上限に
///   1 件ずつ待って送る (捨てない)。待ち行列がある間の後続も積むため、届く順序は `emit` の順のまま
/// - 上限を超えた、またはアクターが終わっていたら、種類と曲をログに残す
///
/// 呼び出し方:
/// ```ignore
/// let sink = EventSink::new(guild, events_tx, send_timeout);
/// sink.emit(AudioEvent::Reconnected);
/// sink.close();
/// ```
#[derive(Clone)]
pub struct EventSink {
	guild: GuildId,
	open: Arc<AtomicBool>,
	/// 音声の接続がつながっているか。`DriverDisconnect` で false、`DriverConnect` と `DriverReconnect` で true にする
	link_up: Arc<AtomicBool>,
	tx: mpsc::Sender<GuildMessage>,
	send_timeout: Duration,
	backlog: Arc<std::sync::Mutex<Backlog>>,
	/// セッションの今のボイスチャンネル (移動中は移動先)。0 は不明
	voice_channel: Arc<AtomicU64>,
	/// 前のチャンネルの終わりとして、アクターへ送らなかった切断があったか (移動に失敗したときに送り直す)
	held_disconnect: Arc<AtomicBool>,
}

impl EventSink {
	/// 開いた状態の送信口を作る。
	///
	/// # 引数
	/// - `guild`: ログに出すサーバー
	/// - `tx`: アクターのメールボックス
	/// - `send_timeout`: 1 件の送信を待つ上限
	///
	/// # 例
	/// `let sink = EventSink::new(guild, tx, Duration::from_secs(5));`
	pub fn new(guild: GuildId, tx: mpsc::Sender<GuildMessage>, send_timeout: Duration) -> Self {
		Self {
			guild,
			open: Arc::new(AtomicBool::new(true)),
			link_up: Arc::new(AtomicBool::new(true)),
			tx,
			send_timeout,
			backlog: Arc::new(std::sync::Mutex::new(Backlog::default())),
			voice_channel: Arc::new(AtomicU64::new(UNKNOWN_CHANNEL)),
			held_disconnect: Arc::new(AtomicBool::new(false)),
		}
	}

	/// セッションの今のボイスチャンネル。0 は不明。
	///
	/// # 例
	/// `let current = sink.voice_channel();`
	pub fn voice_channel(&self) -> u64 {
		self.voice_channel.load(Ordering::SeqCst)
	}

	/// セッションの今のボイスチャンネルを記録する。接続の直後と、移動を始める前と失敗した後に呼ぶ。
	///
	/// # 引数
	/// - `channel`: チャンネルの ID。0 は不明
	///
	/// # 例
	/// `sink.set_voice_channel(channel.get());`
	pub fn set_voice_channel(&self, channel: u64) {
		self.voice_channel.store(channel, Ordering::SeqCst);
	}

	/// 前のチャンネルの終わりとして、切断を送らなかったことを記録する。
	///
	/// # 例
	/// `sink.hold_disconnect();`
	pub fn hold_disconnect(&self) {
		self.held_disconnect.store(true, Ordering::SeqCst);
	}

	/// 送らなかった切断があったかを返し、記録を消す。
	///
	/// # 例
	/// `if sink.take_held_disconnect() { sink.emit(AudioEvent::Disconnected { reason: DisconnectKind::Lost }); }`
	pub fn take_held_disconnect(&self) -> bool {
		self.held_disconnect.swap(false, Ordering::SeqCst)
	}

	/// ログに出すサーバー。
	///
	/// # 例
	/// `tracing::warn!(guild = sink.guild().get(), "...");`
	pub(super) fn guild(&self) -> GuildId {
		self.guild
	}

	/// 音声の接続がつながっているか。
	///
	/// - 切れている間、Songbird は曲への命令 (`get_info` など) を処理しない (ミキサーが待機側へ移るため)。
	///   呼び出し側は待たずに諦める判断に使う
	///
	/// # 例
	/// `if !sink.link_up() { return Err(AudioError::NotConnected); }`
	pub fn link_up(&self) -> bool {
		self.link_up.load(Ordering::SeqCst)
	}

	/// 音声の接続の状態を記録する。
	///
	/// # 例
	/// `sink.set_link_up(false);`
	pub fn set_link_up(&self, up: bool) {
		self.link_up.store(up, Ordering::SeqCst);
	}

	/// 開いているか。
	///
	/// # 例
	/// `if sink.is_open() { ... }`
	pub fn is_open(&self) -> bool {
		self.open.load(Ordering::SeqCst)
	}

	/// 印を閉じる。以後のイベントは送らない。何度呼んでもよい。
	///
	/// # 例
	/// `sink.close();`
	pub fn close(&self) {
		self.open.store(false, Ordering::SeqCst);
	}

	/// イベントを送る。閉じていれば捨てる。
	///
	/// - 呼び出し元を待たせない。待ち行列が空ならその場で送り、メールボックスが満杯のときだけ
	///   待ち行列に積んで tokio のタスク (`drain`) に送らせる。ランタイムの外では送れずログに残す
	/// - 待ち行列があれば、その場で送れる状態でも後ろに積む (先に積んだイベントを追い越さない)
	///
	/// # 引数
	/// - `event`: 送るイベント
	///
	/// # 例
	/// `sink.emit(AudioEvent::TrackEnded { track });`
	pub fn emit(&self, event: AudioEvent) {
		if !self.is_open() {
			tracing::debug!(
				guild = self.guild.get(),
				?event,
				"閉じたセッションのイベントを捨てます"
			);
			return;
		}
		let mut backlog = self.lock_backlog();
		if backlog.draining {
			backlog.queue.push_back(event);
			return;
		}
		let event = match self.tx.try_send(GuildMessage::Audio(event)) {
			Ok(()) => return,
			Err(mpsc::error::TrySendError::Closed(_)) => {
				tracing::warn!(
					guild = self.guild.get(),
					"アクターが終了しているためイベントを捨てます"
				);
				return;
			}
			Err(mpsc::error::TrySendError::Full(GuildMessage::Audio(event))) => event,
			Err(mpsc::error::TrySendError::Full(_)) => return,
		};
		let Ok(runtime) = tokio::runtime::Handle::try_current() else {
			tracing::warn!(
				guild = self.guild.get(),
				?event,
				"ランタイムの外のためイベントを送れません"
			);
			return;
		};
		backlog.queue.push_back(event);
		backlog.draining = true;
		drop(backlog);
		runtime.spawn(self.clone().drain());
	}

	/// 待ち行列をロックする。毒されていても中身を使う (イベントを捨てないため)。
	///
	/// # 例
	/// `let mut backlog = self.lock_backlog();`
	fn lock_backlog(&self) -> std::sync::MutexGuard<'_, Backlog> {
		self.backlog
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
	}

	/// 待ち行列のイベントを先頭から 1 件ずつ送る。空になったら終わる。
	///
	/// - 1 件ごとに `send_timeout` を上限にメールボックスの空きを待つ (`reserve`)。空きを得てから
	///   先頭を取り出して送るため、待ちの間に積まれたイベントも順序を保つ
	/// - 上限を超えたイベントは捨ててログに残し、次へ進む
	/// - 閉じた、またはアクターが終わっていたら、残りを捨てて終わる
	///
	/// # 例
	/// `runtime.spawn(self.clone().drain());`
	async fn drain(self) {
		loop {
			let permit = if self.is_open() {
				Some(timeout(self.send_timeout, self.tx.reserve()).await)
			} else {
				None
			};
			let mut backlog = self.lock_backlog();
			let Some(event) = backlog.queue.pop_front() else {
				backlog.draining = false;
				return;
			};
			match permit {
				Some(Ok(Ok(permit))) if self.is_open() => permit.send(GuildMessage::Audio(event)),
				Some(Ok(Err(_))) => {
					tracing::warn!(
						guild = self.guild.get(),
						?event,
						dropped_after = backlog.queue.len(),
						"アクターが終了しているためイベントを捨てます"
					);
					backlog.queue.clear();
				}
				Some(Err(_)) => tracing::warn!(
					guild = self.guild.get(),
					?event,
					"アクターのメールボックスが空かないためイベントを捨てます"
				),
				_ => {
					tracing::debug!(
						guild = self.guild.get(),
						?event,
						dropped_after = backlog.queue.len(),
						"閉じたセッションのイベントを捨てます"
					);
					backlog.queue.clear();
				}
			}
			if backlog.queue.is_empty() {
				backlog.draining = false;
				return;
			}
		}
	}
}
