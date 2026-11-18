//! 音声の呼び出しの上限時間。`voice.call_timeout_secs` と `voice.join_timeout_secs` から導く。
//! - 内側 (`SongbirdBackend`) は Songbird の 1 回の呼び出しを `call`、参加を `join` で打ち切り、
//!   失敗や時間切れのときは後始末 (Call の削除) まで行ってから戻る
//! - 外側 (アクター) の上限は、内側の時間切れと後始末の合計に `OUTER_MARGIN` を足した値にする。
//!   外側が先に切れると内側の後始末が走らず、Bot がボイスチャンネルに残るため
//! - 後始末の前の持ち主の記録のロックの待ち (`CallOwners`) は含めない。アクターは切断を待ってから接続を始めるため
//!
//! 呼び出し方:
//! ```ignore
//! let limits = VoiceLimits::new(&config.voice);
//! tokio::time::timeout(limits.connect(), audio.connect(guild, channel, events)).await;
//! ```

use std::time::Duration;

use crate::config::VoiceConfig;

/// 外側の上限に足す余裕。内側の時間切れの後の後始末が、外側の打ち切りより先に終わるように取る。
pub const OUTER_MARGIN: Duration = Duration::from_secs(2);

/// 音声の呼び出しの上限時間の組。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoiceLimits {
	call: Duration,
	join: Duration,
}

impl VoiceLimits {
	/// 設定から作る。
	///
	/// # 引数
	/// - `voice`: `[voice]` の設定
	///
	/// # 戻り値
	/// 上限時間の組
	///
	/// # 呼び出し例
	/// ```ignore
	/// let limits = VoiceLimits::new(&config.voice);
	/// ```
	pub fn new(voice: &VoiceConfig) -> Self {
		Self {
			call: Duration::from_secs(voice.call_timeout_secs),
			join: Duration::from_secs(voice.join_timeout_secs),
		}
	}

	/// Songbird の 1 回の呼び出しの上限 (`voice.call_timeout_secs`)。アクターの通常の呼び出しにも使う。
	///
	/// # 呼び出し例
	/// `bounded(limits.call(), call.lock()).await`
	pub fn call(&self) -> Duration {
		self.call
	}

	/// 参加 (`Songbird::join`) の上限 (`voice.join_timeout_secs`)。
	///
	/// # 呼び出し例
	/// `timeout(limits.join(), manager.join(guild, channel)).await`
	pub fn join(&self) -> Duration {
		self.join
	}

	/// アクターが `AudioBackend::connect` を待つ上限。
	///
	/// - 内側の最長: 参加 (`join`) → 接続のイベントの登録 (`call`) → 失敗した Call の削除 (`call`)
	///
	/// # 呼び出し例
	/// `timeout(limits.connect(), audio.connect(guild, channel, events)).await`
	pub fn connect(&self) -> Duration {
		self.join + self.call * 2 + OUTER_MARGIN
	}

	/// アクターが `VoiceSession::move_to` を待つ上限。
	///
	/// - 内側の最長: 参加 (`join`)
	///
	/// # 呼び出し例
	/// `timeout(limits.move_to(), voice.move_to(channel)).await`
	pub fn move_to(&self) -> Duration {
		self.join + OUTER_MARGIN
	}

	/// アクターが `VoiceSession::disconnect` を待つ上限。
	///
	/// - 内側の最長: Call の停止 (`call`) → Call の削除 (`call`)
	///
	/// # 呼び出し例
	/// `timeout(limits.disconnect(), session.disconnect()).await`
	pub fn disconnect(&self) -> Duration {
		self.call * 2 + OUTER_MARGIN
	}
}
