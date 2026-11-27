//! 音声処理を隠す trait と、その入出力の型。
//! - `AudioBackend` が接続の入口、`VoiceSession` が接続済みの音声セッション
//! - Songbird 実装の他に、Lavalink 実装や試験用の偽実装へ差し替えられるようにする
//! - Songbird の呼び出しはすべて `tokio::time::timeout` で包み、タイムアウトは切断として扱う (実装側の責務)

use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;

use super::access::Denial;
use crate::domain::{ChannelId, GuildId, ResolvedStream, VideoId};
use crate::guild::message::GuildMessage;

/// バックエンドが割り当てる曲の識別子。
///
/// `play` / `preload` の戻り値で払い出され、`AudioEvent` と `start_preloaded` で曲を指す。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TrackId(pub u64);

/// 音声バックエンドのエラー。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AudioError {
	/// 呼び出しがタイムアウトした。
	#[error("音声処理がタイムアウトしました")]
	Timeout,
	/// ボイスチャンネルへの接続に失敗した。
	#[error("ボイスチャンネルに接続できません: {0}")]
	Connect(String),
	/// 接続していない。
	#[error("ボイスチャンネルに接続していません")]
	NotConnected,
	/// 再生などの操作に失敗した。
	#[error("音声処理に失敗しました: {0}")]
	Failed(String),
	/// 参加の前の確認で、権限の不足か満員のため参加しなかった。
	#[error("ボイスチャンネルに入れません: {0}")]
	Denied(Denial),
}

/// 切断の種類。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectKind {
	/// 利用者に追い出された、またはチャンネルが消えた。再接続しない。
	Kicked,
	/// ネットワークの不調やタイムアウト。再接続を試みてよい。
	Lost,
}

/// バックエンドからアクターへ届くイベント。`GuildMessage::Audio` に包まれて送られる。
///
/// 実装が守る契約 (イベントには接続ごとの印が無く、アクターは `TrackId` と自分の状態だけで見分けるため):
/// - `TrackId` は接続 (セッション) をまたいでも重複させない。
///   崩れると、切断した古いセッションの終了・失敗のイベントを今の曲のものと取り違え、曲を飛ばしたり作り直したりする
/// - 1 つの曲の終わりには `TrackEnded` か `TrackFailed` のどちらか一方だけを送る。
///   崩れると、失敗した曲で次の曲へ進んだ後に、失敗の手順 (取り直し) が空振りする、または逆に作り直した曲を終わらせる
/// - アクターが自分で呼んだ `VoiceSession::disconnect` では `Disconnected` を送らない。
///   崩れると、回復の再接続の直後に古い切断が届き、回復を重ねて自動の回復の回数を無駄に使う
/// - `ChunkedRequest` と `PipeInput` で再生した曲が終わったら、バックエンドがその曲の `ChunkFailure` を見る。
///   失敗が記録されていれば `TrackEnded` ではなく `TrackFailed` を送る。
///   Songbird は入力の読み取りのエラーを曲の終端と区別しないため、アクターには見分けられない。
///   崩れると、取得に失敗した曲を最後まで再生したものとして次の曲へ進み、取り直しをしない
/// - イベントを黙って捨てない。アクターのメールボックス (`limits.actor_mailbox`) が満杯のときは、
///   `try_send` で失敗させて捨てるのではなく、上限時間付きの `send` (`tokio::time::timeout` で包む) で待つか、
///   やり直す。Songbird のイベントハンドラの中で待てない場合は、送信を別タスクへ渡して待つ。
///   上限を超えても届かなかったときは、捨てたイベントの種類と曲をログに残す。
///   崩れると、曲の終了を取りこぼしてアクターが準備中のまま止まる、切断に気づかず回復しない
///
/// SongbirdBackend の守り方: `TrackId` はバックエンド全体の通し番号から払い出し、
/// セッションごとに「開いている」印を持つ。イベントのハンドラは印が開いているときだけ送り、
/// `disconnect` (とセッションの破棄) は最初に印を閉じる。自分の切断の `Disconnected` と、
/// 古いセッションの曲のイベントはアクターへ届かない。
#[derive(Debug, Clone, PartialEq)]
pub enum AudioEvent {
	/// 曲の再生が始まった。アクターはログに残すだけ。
	TrackStarted { track: TrackId },
	/// 曲が最後まで再生された、または `stop` で止まった。失敗した曲には送らない (`TrackFailed` だけを送る)。
	/// `ChunkFailure` に失敗が記録された曲も失敗した曲として扱う。
	/// アクターは今の曲なら次の曲へ進む。
	TrackEnded { track: TrackId },
	/// 再生中の曲が失敗した (入力の内部の再試行を尽くした後)。同じ曲に `TrackEnded` を送らない。
	/// Songbird が終端として報告した曲でも、`ChunkFailure` に失敗が記録されていればこちらを送る。
	/// アクターは今の曲なら失敗時の手順 (取り直し → PipeInput → 飛ばす) に進む。
	TrackFailed { track: TrackId, error: AudioError },
	/// 接続が外から切れた。アクター自身の `VoiceSession::disconnect` では送らない。
	/// アクターは `Lost` なら回復、`Kicked` なら退出する。
	Disconnected { reason: DisconnectKind },
	/// 実装の内部で接続が自動で戻った。アクターはログに残すだけ。
	Reconnected,
}

/// 再生の要求。
#[derive(Debug, Clone, PartialEq)]
pub struct PlayRequest {
	pub video_id: VideoId,
	/// Songbird 実装が使う。Lavalink 実装は video_id だけで再生できる。
	pub stream: Option<ResolvedStream>,
	pub start_at: Duration,
	pub volume: f32,
	/// true なら PipeInput で再生する。
	pub use_pipe: bool,
	/// true なら一時停止のまま載せ、入力の準備だけを進める (`play` のみ。回復や復元で一時停止に戻す曲)。
	/// 始めてから一時停止にすると、一瞬音が出るため。開始のイベントは `resume` の後に届く
	pub paused: bool,
}

/// 音声接続の健全性。DAVE (E2EE) の状態とフレーム数を持つ。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VoiceHealth {
	pub dave_version: Option<u16>,
	pub encrypted_frames: u64,
	pub plaintext_frames: u64,
}

/// ボイスチャンネルへの接続を作る。
///
/// 呼び出し方:
/// ```ignore
/// let session = backend.connect(guild, channel, events_tx).await?;
/// ```
#[async_trait]
pub trait AudioBackend: Send + Sync {
	/// ボイスチャンネルへ接続する。
	///
	/// # 引数
	/// - `guild`: 接続するサーバー
	/// - `channel`: 接続するボイスチャンネル
	/// - `events`: 再生の終了や切断をアクターへ知らせる送信口。`GuildMessage::Audio` として送る
	///
	/// # 戻り値
	/// 接続済みのセッション
	///
	/// # エラー
	/// 接続の失敗、タイムアウト
	async fn connect(
		&self,
		guild: GuildId,
		channel: ChannelId,
		events: mpsc::Sender<GuildMessage>,
	) -> Result<Box<dyn VoiceSession>, AudioError>;
}

/// 接続済みの音声セッション。1 サーバーにつき 1 つで、アクターだけが操作する。
///
/// 呼び出し方:
/// ```ignore
/// let track = session.play(request).await?;
/// session.pause().await?;
/// ```
///
/// 全メソッドは、未接続なら `NotConnected`、時間切れなら `Timeout` を返してよい。
#[async_trait]
pub trait VoiceSession: Send {
	/// 今の曲を止めて、指定の曲を再生する。割り当てた `TrackId` を返す。
	async fn play(&mut self, request: PlayRequest) -> Result<TrackId, AudioError>;

	/// 次の曲を一時停止の状態で準備する。`start_preloaded` で再生を始める。
	async fn preload(&mut self, request: PlayRequest) -> Result<TrackId, AudioError>;

	/// `preload` で準備した曲の再生を始める。
	async fn start_preloaded(&mut self, track: TrackId) -> Result<(), AudioError>;

	/// `preload` で準備した曲を、再生せずに捨てる。
	///
	/// - 準備した曲が次の曲でなくなったとき (飛ばし、キューの削除・シャッフル、`?pnow`、退出) に呼ぶ
	/// - 再生中の曲には影響しない
	/// - すでに捨てた、または知らない `track` なら何もせず `Ok(())` を返してよい
	///
	/// # 引数
	/// - `track`: `preload` で得た識別子
	///
	/// # エラー
	/// 未接続なら `NotConnected`、時間切れなら `Timeout`、その他の失敗は `Failed`
	///
	/// # 呼び出し例
	/// ```ignore
	/// session.drop_preloaded(track).await?;
	/// ```
	async fn drop_preloaded(&mut self, track: TrackId) -> Result<(), AudioError>;

	/// 今の曲の再生を止める。`preload` で準備した曲は残す。
	async fn stop(&mut self) -> Result<(), AudioError>;

	/// 一時停止する。
	async fn pause(&mut self) -> Result<(), AudioError>;

	/// 一時停止を解除する。
	async fn resume(&mut self) -> Result<(), AudioError>;

	/// 再生位置を移す。戻り値は移る先の位置。
	///
	/// - 実装はシークの完了を待たずに返してよい。その場合、シークの失敗は曲の失敗 (`TrackFailed`) として届く
	async fn seek(&mut self, position: Duration) -> Result<Duration, AudioError>;

	/// 音量を設定する (1.0 が等倍)。今の曲と、`preload` で準備した曲の両方に反映する。
	async fn set_volume(&mut self, volume: f32) -> Result<(), AudioError>;

	/// 今の再生位置を得る。
	async fn position(&self) -> Result<Duration, AudioError>;

	/// 別のボイスチャンネルへ移動する。再生は続ける。
	async fn move_to(&mut self, channel: ChannelId) -> Result<(), AudioError>;

	/// 切断してセッションを消費する。
	///
	/// - この切断について `AudioEvent::Disconnected` を送らない。
	///   送ると、アクターは外からの切断と見分けられず、回復 (再接続) を重ねる
	async fn disconnect(self: Box<Self>) -> Result<(), AudioError>;

	/// 接続の健全性を同期的に返す。
	fn health(&self) -> VoiceHealth;
}
