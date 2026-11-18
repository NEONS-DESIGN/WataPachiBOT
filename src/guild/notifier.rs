//! 利用者への応答の trait。アクターは Discord を直接触らず、これを通して送る。

use async_trait::async_trait;

use crate::domain::{ChannelId, GuildId, MessageId, UserId, VideoId};

/// 利用者へ送る通知。文面は `messages/*.toml` のキーで指し、本文への差し込みを `args` に持つ。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
	pub key: &'static str,
	pub args: Vec<(&'static str, String)>,
	/// 曲のカード。`play.now_playing`・`play.queued`・`play.playlist_*` だけが持つ
	pub card: Option<Card>,
}

impl Notice {
	/// カードの無い通知を作る。
	///
	/// # 引数
	/// - `key`: `messages/*.toml` のキー
	/// - `args`: 本文への差し込み。無ければ空
	///
	/// # 呼び出し例
	/// ```ignore
	/// let notice = Notice::text("error.busy", Vec::new());
	/// ```
	pub fn text(key: &'static str, args: Vec<(&'static str, String)>) -> Self {
		Self {
			key,
			args,
			card: None,
		}
	}
}

/// 曲のカード。種類ごとに Embed の項目が違う。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Card {
	/// 再生を始めた曲 (`play.now_playing`)
	NowPlaying(TrackCard),
	/// キューに足した曲 (`play.queued`)
	Queued(TrackCard),
	/// キューに足したプレイリスト (`play.playlist_queued`・`play.playlist_truncated`)
	Playlist(PlaylistCard),
}

impl Card {
	/// カードを頼んだ人と、そのギルド。リクエストした人の名前とアイコンを引くのに使う。
	///
	/// # 呼び出し例
	/// ```ignore
	/// let (guild, user) = card.requester();
	/// ```
	pub fn requester(&self) -> (GuildId, UserId) {
		match self {
			Card::NowPlaying(track) | Card::Queued(track) => (track.guild, track.requester),
			Card::Playlist(list) => (list.guild, list.requester),
		}
	}

	/// 曲のカードの待機数。プレイリストのカードは None。
	///
	/// # 呼び出し例
	/// ```ignore
	/// let waiting = card.waiting();
	/// ```
	pub fn waiting(&self) -> Option<u32> {
		match self {
			Card::NowPlaying(track) | Card::Queued(track) => Some(track.waiting),
			Card::Playlist(_) => None,
		}
	}
}

/// プレイリストのカードの中身。名前と URL、追加数と断った数、1 曲目、頼んだ人。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaylistCard {
	pub guild: GuildId,
	/// プレイリストの名前。取れなければ None (`ui.unknown_title` を出す)
	pub title: Option<String>,
	/// プレイリストの URL (`?p` で受け取って組み立て直したもの)
	pub url: String,
	/// 1 曲目の動画。画像に使う。無ければ画像を付けない
	pub first_video: Option<VideoId>,
	pub added: u32,
	/// キューの上限のため足さなかった曲数
	pub rejected: u32,
	pub requester: UserId,
}

/// 1 曲のカードの中身。曲名と長さ、頼んだ人、待っている曲数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackCard {
	pub guild: GuildId,
	pub video_id: VideoId,
	pub title: String,
	pub duration_s: Option<u32>,
	pub requester: UserId,
	/// カードを作った時点のキューの曲数 (キューに追加のカードは追加した後の曲数)
	pub waiting: u32,
}

/// 送信済みのメッセージの場所。後から編集するために持つ。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageRef {
	pub channel: ChannelId,
	pub message: MessageId,
}

/// 通知の送り先。
///
/// 呼び出し方:
/// ```ignore
/// let sent = notifier.send(channel, notice).await;
/// if let Some(target) = sent {
/// 	notifier.edit(target, other_notice).await;
/// }
/// ```
#[async_trait]
pub trait Notifier: Send + Sync {
	/// 指定のテキストチャンネルへ通知を送る。
	///
	/// # 戻り値
	/// 送ったメッセージの場所。送信に失敗したら None (失敗は実装側で記録する)
	async fn send(&self, channel: ChannelId, notice: Notice) -> Option<MessageRef>;

	/// 送信済みのメッセージを通知の内容に書き換える。
	///
	/// # 戻り値
	/// 編集できたら true。失敗したら false (削除済みなど。呼び出し側は新しく送り直す)
	async fn edit(&self, target: MessageRef, notice: Notice) -> bool;
}
