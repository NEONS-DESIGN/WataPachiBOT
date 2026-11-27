//! ボイスチャンネルへの参加の前の確認。
//! - `VoiceAccess` が確認役の trait。本番の実装は Gateway 側 (キャッシュと REST) に置く
//! - Discord は権限の無い参加や満員のチャンネルへの参加を黙って無視するため、参加の前に確かめて理由を伝える
//! - 確かめられないときは `Unknown` を返し、呼び出し側は参加を試す (確認が新しい失敗の原因にならないように)

use std::fmt;

use async_trait::async_trait;

use crate::domain::{ChannelId, GuildId};

/// 参加に要る権限のうち、足りないものを表す。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoicePermission {
	/// チャンネルを見る (`VIEW_CHANNEL`)
	ViewChannel,
	/// 接続 (`CONNECT`)
	Connect,
	/// 発言 (`SPEAK`)
	Speak,
}

impl VoicePermission {
	/// ログに出す名前 (Discord の API の権限名)。
	///
	/// # 呼び出し例
	/// ```ignore
	/// let name = VoicePermission::Connect.name(); // "CONNECT"
	/// ```
	pub fn name(self) -> &'static str {
		match self {
			VoicePermission::ViewChannel => "VIEW_CHANNEL",
			VoicePermission::Connect => "CONNECT",
			VoicePermission::Speak => "SPEAK",
		}
	}
}

/// 参加を断る理由。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Denial {
	/// 権限が足りない。足りない権限を `ViewChannel`・`Connect`・`Speak` の順に持つ
	Missing(Vec<VoicePermission>),
	/// 人数の上限に達していて、上限を越える権限 (`MOVE_MEMBERS`) も無い
	Full,
}

impl fmt::Display for Denial {
	/// ログ向けの表示。利用者向けの文言は `messages/*.toml` から作る。
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Denial::Missing(missing) => {
				let names: Vec<&str> = missing.iter().map(|p| p.name()).collect();
				write!(f, "権限がありません: {}", names.join(", "))
			}
			Denial::Full => f.write_str("チャンネルが満員です"),
		}
	}
}

/// 参加の前の確認の結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessVerdict {
	/// 参加してよい
	Allowed,
	/// 参加しても Discord に無視されるため、参加しない
	Denied(Denial),
	/// 確かめられなかった (キャッシュに情報が無いなど)。参加を試す
	Unknown,
}

/// 参加の前の確認役。
#[async_trait]
pub trait VoiceAccess: Send + Sync {
	/// Bot がボイスチャンネルに入れるかを確かめる。
	///
	/// - 実装は上限時間で打ち切り、確かめられなければ `Unknown` を返す
	///
	/// # 引数
	/// - `guild`: ギルド
	/// - `channel`: 入ろうとするボイスチャンネル
	///
	/// # 戻り値
	/// 確認の結果
	///
	/// # 呼び出し例
	/// ```ignore
	/// if let AccessVerdict::Denied(denial) = access.check(guild, channel).await { /* 断る */ }
	/// ```
	async fn check(&self, guild: GuildId, channel: ChannelId) -> AccessVerdict;
}
