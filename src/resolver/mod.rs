//! 曲の解決。Python ワーカーとの通信を扱う。
//! - `Resolver` が上位層から見た解決の入口
//! - 実装は `cached::CachedResolver`。SQLite のキャッシュを見てから `WorkerPool` へ渡す

use async_trait::async_trait;

use crate::domain::{PlaylistMeta, Priority, ResolveError, ResolvedStream, TrackMeta, VideoId};

pub mod cached;
pub mod pool;
pub mod process;
pub mod protocol;

/// 動画の解決。再生経路とコマンド処理が使う。
///
/// 呼び出し方:
/// ```ignore
/// let stream = resolver.resolve(&video_id, Priority::Interactive).await?;
/// ```
#[async_trait]
pub trait Resolver: Send + Sync {
	/// 再生用のストリームを得る。キャッシュが使えればワーカーを呼ばない。
	///
	/// # 引数
	/// - `video_id`: 動画 ID
	/// - `priority`: 待ち行列の段
	///
	/// # 戻り値
	/// 解決結果。`filesize` は常に埋まっている (ライブなど長さの無いものは 0)
	///
	/// # エラー
	/// ワーカーの失敗、タイムアウト、ワーカーの不在、応答の不正
	async fn resolve(
		&self,
		video_id: &VideoId,
		priority: Priority,
	) -> Result<ResolvedStream, ResolveError>;

	/// キャッシュを使わずに取り直す。
	///
	/// # 引数
	/// - `video_id`: 動画 ID
	/// - `format_id`: Some なら同じ形式に固定する
	///
	/// # 戻り値
	/// 新しい解決結果。キャッシュも更新する
	///
	/// # エラー
	/// `resolve` と同じ
	async fn refresh(
		&self,
		video_id: &VideoId,
		format_id: Option<&str>,
	) -> Result<ResolvedStream, ResolveError>;

	/// 検索語から先頭の 1 件を得る。
	///
	/// # 引数
	/// - `query`: 検索語 (キャッシュの鍵には NFKC と小文字化をかけて使う)
	/// - `priority`: 待ち行列の段
	///
	/// # エラー
	/// ワーカーの失敗など。結果が 0 件なら `NotFound`
	async fn search(&self, query: &str, priority: Priority) -> Result<TrackMeta, ResolveError>;

	/// プレイリストの一覧を得る。
	///
	/// - ワーカーが持ち時間のうちに読み切れなかった長いプレイリストは、途中までの一覧になる
	///   (`limit` より少なくても全件とは限らない)
	///
	/// # 引数
	/// - `url`: プレイリストの URL
	/// - `limit`: 返す上限件数
	///
	/// # エラー
	/// ワーカーの失敗など
	async fn flat_playlist(&self, url: &str, limit: u32) -> Result<PlaylistMeta, ResolveError>;
}
