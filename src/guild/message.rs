//! GuildActor が受け取るメッセージの型。

use std::fmt;

use tokio::sync::oneshot;

use crate::audio::backend::{AudioError, AudioEvent, VoiceSession};
use crate::db::playback::PlaybackState;
use crate::domain::{
	ChannelId, PlaylistMeta, ResolveError, ResolvedStream, TrackMeta, UserId, VideoId,
};

/// アクターのメールボックスに届くメッセージ。
#[derive(Debug)]
pub enum GuildMessage {
	/// 利用者のコマンド。応答先などは `context` から得る。
	Command {
		command: GuildCommand,
		context: CommandContext,
	},
	/// 音声バックエンドからのイベント。
	Audio(AudioEvent),
	/// 解決の完了。`generation` が古ければアクターが捨てる。
	Resolved {
		generation: u64,
		video_id: VideoId,
		result: Result<ResolvedStream, ResolveError>,
	},
	/// ボイスチャンネルの聞き手 (Bot 以外) の人数の変化。
	VoiceMembersChanged { listeners: u32 },
	/// ギルドの設定が `?setting` で書き換えられた。アクターはキャッシュした設定を捨て、次に使うときに読み直す。
	SettingsChanged,
	/// 定期処理の合図。
	Tick(TickKind),
	/// 起動時の再生状態の復元。
	Restore(RestoreRequest),
	/// 終了の要求。処理を終えたら `done` へ送る。
	Shutdown { done: oneshot::Sender<()> },
	/// `?p` の問い合わせ (動画の解決、検索、プレイリストの一覧) の完了。アクターが起動した処理から届く。
	/// `session` が今のセッションの番号と違えば、アクターが捨てる。
	QueryResolved {
		session: u64,
		context: CommandContext,
		/// 問い合わせの種類。失敗の文言を選ぶのに使う
		kind: LookupKind,
		result: Result<QueryResult, ResolveError>,
	},
	/// ボイスチャンネルへの接続の完了。`session` が今のセッションの番号と違えば、アクターが切断して捨てる。
	Connected {
		session: u64,
		result: Result<VoiceLink, AudioError>,
	},
	/// 先読みの解決の完了。曲の切替とは無関係に、動画 ID で結果を受け取る。
	/// `session` が今のセッションの番号と違えば、アクターが捨てる。
	Prefetched {
		session: u64,
		video_id: VideoId,
		result: Result<ResolvedStream, ResolveError>,
	},
}

/// `?p` の問い合わせの種類。失敗の通知の文言を選ぶのに使う。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupKind {
	/// 動画の URL (対応外の URL を含む)
	Video,
	/// 検索語
	Search,
	/// プレイリストの URL
	Playlist,
}

/// `?p` の問い合わせの結果。
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
	/// 動画 URL。解決済みのストリームを持ち、すぐ再生するならそのまま使う。
	Video(ResolvedStream),
	/// 検索の先頭の 1 件。
	Search(TrackMeta),
	/// プレイリストの一覧と、問い合わせた URL (カードのリンクに使う)。
	Playlist { url: String, meta: PlaylistMeta },
}

/// 接続済みの音声セッション。`GuildMessage` で運ぶための包み。
///
/// - `VoiceSession` は Debug を持たないため、ここで中身を出さない Debug を実装する
pub struct VoiceLink(pub Box<dyn VoiceSession>);

impl fmt::Debug for VoiceLink {
	/// 中身を出さずに型名だけを書く。
	///
	/// # 引数
	/// - `f`: 書き込み先
	///
	/// # 戻り値
	/// 書き込みの結果
	///
	/// # 呼び出し例
	/// ```ignore
	/// tracing::debug!(?link);
	/// ```
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("VoiceLink(..)")
	}
}

/// 利用者のコマンド。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuildCommand {
	Play {
		query: String,
	},
	Pause,
	Resume,
	Replay,
	Skip,
	/// キュー内の位置 (1 始まり) の曲をすぐ再生する。
	PlayNow {
		index: u32,
	},
	Clear {
		range: ClearRange,
	},
	ToggleLoop,
	Shuffle,
	/// 音量を百分率で設定する。
	SetVolume {
		percent: u16,
	},
	Move,
	Rejoin,
	Leave,
}

/// 定期処理の種類。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickKind {
	/// 再生位置の保存。
	Position,
	/// 音声接続の健全性の確認。
	Health,
	/// 一人きりのときの自動退出の判定。
	AloneTimeout,
}

/// `?clear [開始] [終了]` の範囲。番号は 1 始まりで、終了を含む。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClearRange {
	/// 全件。
	All,
	/// 先頭から指定の件数。
	First(u32),
	/// 開始から終了まで (両端を含む)。
	Range { start: u32, end: u32 },
}

/// コマンドの発行元。応答の宛先と、実行者のボイスチャンネルを持つ。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandContext {
	pub text_channel: ChannelId,
	pub user: UserId,
	/// 実行者が入っているボイスチャンネル。入っていなければ None。
	pub voice_channel: Option<ChannelId>,
}

/// 起動時の復元の要求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreRequest {
	/// 保存されていた再生状態。
	pub state: PlaybackState,
	/// 復元時点のボイスチャンネルの聞き手の人数。
	pub listeners: u32,
}
