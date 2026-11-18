//! 層をまたいで共有する型。
//! - 他モジュールへの依存を持たない（twilight-model の ID 型と tokio の Instant のみ使う）
//! - ワーカーのプロトコルで受け渡す型は serde の Serialize / Deserialize を実装する

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use twilight_model::id::marker::{ChannelMarker, GuildMarker, MessageMarker, UserMarker};
use twilight_model::id::Id;

/// サーバー ID。
pub type GuildId = Id<GuildMarker>;
/// チャンネル ID。
pub type ChannelId = Id<ChannelMarker>;
/// ユーザー ID。
pub type UserId = Id<UserMarker>;
/// メッセージ ID。
pub type MessageId = Id<MessageMarker>;

/// YouTube の動画 ID（11 文字の文字列）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VideoId(pub String);

/// 動画 1 件の軽量な情報（検索結果、プレイリストの要素）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrackMeta {
	pub video_id: VideoId,
	pub title: String,
	pub duration_s: Option<u32>,
	pub thumbnail: Option<String>,
}

/// プレイリストの情報。`title` はワーカーが取得できなかったとき None。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaylistMeta {
	pub title: Option<String>,
	pub entries: Vec<TrackMeta>,
}

/// 再生に使う音声ストリームの解決結果。
///
/// - `expires_at` は UNIX 秒。期限前に再解決する判断に使う。None は期限が不明で、
///   キャッシュせず、期限の確認を省いて 403 を受けたときの `refresh` に任せる
/// - `available_at` は UNIX 秒。YouTube は再生前の広告の秒数が過ぎるまで URL を 403 で拒むため、
///   この時刻までは取得を始めない。None は待たない
/// - `http_headers` はチャンク取得時にそのまま付ける
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedStream {
	pub video_id: VideoId,
	pub title: String,
	pub duration_s: Option<u32>,
	pub url: String,
	pub http_headers: BTreeMap<String, String>,
	pub filesize: u64,
	pub format_id: String,
	pub expires_at: Option<i64>,
	pub available_at: Option<i64>,
	pub is_live: bool,
	pub protocol: String,
}

/// ワーカーが返す解決エラーの分類。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolveErrorCode {
	NotFound,
	Private,
	GeoBlocked,
	AgeRestricted,
	RateLimited,
	UnsupportedUrl,
	Internal,
}

/// 解決処理全体のエラー。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResolveError {
	/// ワーカーがエラーコード付きで返した。
	#[error("ワーカーのエラー: {code:?}: {message}")]
	Worker {
		code: ResolveErrorCode,
		message: String,
	},
	/// 解決がタイムアウトした。
	#[error("解決がタイムアウトしました")]
	Timeout,
	/// 処理中にワーカーのプロセスが終了した。
	#[error("ワーカーが終了しました")]
	WorkerDied,
	/// 利用できるワーカーが無い。
	#[error("利用できるワーカーがありません")]
	Unavailable,
	/// 回数制限のため、プール全体で解決を止めている。retry_at を過ぎたらやり直してよい
	#[error("YouTube の利用制限のため解決を一時停止しています")]
	Paused { retry_at: tokio::time::Instant },
}

/// 解決要求の優先度。Interactive はユーザー操作、Prefetch は先読み。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Priority {
	Interactive,
	Prefetch,
}
