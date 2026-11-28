//! 音声の呼び出しの上限時間。`voice.call_timeout_secs`、`voice.join_timeout_secs`、
//! `voice.access_check_timeout_secs` から導く。
//! - 内側 (`SongbirdBackend`) は Songbird の 1 回の呼び出しを `call`、参加を `join` で打ち切り、
//!   失敗や時間切れのときは後始末 (Call の削除) まで行ってから戻る
//! - 外側 (アクター) の上限は、内側の時間切れと後始末の合計に `OUTER_MARGIN` を足した値にする。
//!   外側が先に切れると内側の後始末が走らず、Bot がボイスチャンネルに残るため
//! - 接続は、同じサーバーの前のセッションの切断 (`CallOwners` のロックを `call` × 2 まで持つ) を待ってから始まる。
//!   回復の切断の途中に `?leave` と `?p` が続くと、切断のタスクと新しい接続のタスクが並ぶため、その待ちも含める
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
	access: Duration,
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
			access: Duration::from_secs(voice.access_check_timeout_secs),
		}
	}

	/// アクターが参加の前の確認 (`VoiceAccess::check`) を待つ上限。
	///
	/// - 内側の最長: Bot の役職の取り直し (`voice.access_check_timeout_secs`)。その後のキャッシュでの判定は待たない
	/// - 過ぎたら確かめられなかったものとして参加を試す
	/// - `connect` には含めない。接続のタスクの全体は `access + connect` になる
	///
	/// # 呼び出し例
	/// `timeout(limits.access(), access.check(guild, channel)).await`
	pub fn access(&self) -> Duration {
		self.access + OUTER_MARGIN
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
	/// - 内側の最長: 前のセッションの切断の待ち (`call` × 2) → 参加 (`join`) → 接続のイベントの登録 (`call`)
	///   → 失敗した Call の削除 (`call`)
	///
	/// # 呼び出し例
	/// `timeout(limits.connect(), audio.connect(guild, channel, events)).await`
	pub fn connect(&self) -> Duration {
		self.join + self.call * 4 + OUTER_MARGIN
	}

	/// アクターが `VoiceSession::play` と `VoiceSession::preload` を待つ上限。
	///
	/// - 内側の最長: PipeInput の起動 (`call`) → Call のロック (`call`)
	/// - 外側が先に切れると、起動の時間切れ (`Failed`) が `Timeout` になり、切断として扱われるため
	///
	/// # 呼び出し例
	/// `timeout(limits.play(), voice.play(request)).await`
	pub fn play(&self) -> Duration {
		self.call * 2 + OUTER_MARGIN
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
