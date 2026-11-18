//! 試験用の解決器。動画 ID ごとに結果を指定し、完了を保留して後から解放できる。
#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::watch;
use watapachi_bot::domain::{
	PlaylistMeta, Priority, ResolveError, ResolvedStream, TrackMeta, VideoId,
};
use watapachi_bot::resolver::Resolver;

type ResolveResult = Result<ResolvedStream, ResolveError>;

#[derive(Default)]
struct State {
	results: HashMap<VideoId, ResolveResult>,
	/// 動画 ID ごとに、`resolve` と `refresh` が先に順に返す結果。尽きたら `results` を使う。
	sequences: HashMap<VideoId, VecDeque<ResolveResult>>,
	/// `refresh` の呼び出し (動画 ID、形式) を呼ばれた順に持つ。
	refresh_calls: Vec<(VideoId, Option<String>)>,
	/// 保留中の動画 ID。値が true になったら解放済み。
	holds: HashMap<VideoId, watch::Sender<bool>>,
	/// 検索語ごとの `search` の結果。
	searches: HashMap<String, Result<TrackMeta, ResolveError>>,
	/// URL ごとの `flat_playlist` の結果。
	playlists: HashMap<String, Result<PlaylistMeta, ResolveError>>,
	/// `resolve` の呼び出し (動画 ID、優先度) を呼ばれた順に持つ。
	resolve_calls: Vec<(VideoId, Priority)>,
	/// `flat_playlist` の呼び出し (URL、上限件数) を呼ばれた順に持つ。
	playlist_calls: Vec<(String, u32)>,
}

/// 結果を指定できる偽の `Resolver`。`clone` したものは状態を共有する。
///
/// - 結果が未指定の動画 ID は `NotFound` で失敗する
/// - `resolve` は呼ばれた時点 (保留で待つ前) に呼び出しを記録する
/// - `refresh` は `resolve` と同じ結果を返す。保留はしないが、呼び出しは `refresh_calls` に記録する
/// - `push_results` で指定した結果は、`resolve` と `refresh` が呼ばれた順に 1 つずつ先に返す
/// - `search` と `flat_playlist` は未指定なら `Unavailable` を返す
/// - `flat_playlist` は本物のワーカーと同じく、要素を `limit` 件までに切り詰めて返す
///
/// 呼び出し方:
/// ```ignore
/// let resolver = FakeResolver::default();
/// resolver.set_result(&id, Ok(stream));
/// resolver.hold(&id);
/// // resolve は release まで待つ
/// resolver.release(&id);
/// assert_eq!(resolver.resolve_calls(), vec![(id, Priority::Interactive)]);
/// ```
#[derive(Clone, Default)]
pub struct FakeResolver {
	state: Arc<Mutex<State>>,
}

impl FakeResolver {
	/// 動画 ID に対する結果を指定する。
	pub fn set_result(&self, video_id: &VideoId, result: ResolveResult) {
		if let Ok(mut s) = self.state.lock() {
			s.results.insert(video_id.clone(), result);
		}
	}

	/// 動画 ID に対して、`resolve` と `refresh` が順に返す結果を足す。尽きたら `set_result` の結果に戻る。
	pub fn push_results(&self, video_id: &VideoId, results: Vec<ResolveResult>) {
		if let Ok(mut s) = self.state.lock() {
			s.sequences
				.entry(video_id.clone())
				.or_default()
				.extend(results);
		}
	}

	/// これまでの `refresh` の呼び出しを、呼ばれた順に返す。
	pub fn refresh_calls(&self) -> Vec<(VideoId, Option<String>)> {
		self.state
			.lock()
			.map(|s| s.refresh_calls.clone())
			.unwrap_or_default()
	}

	/// 動画の `resolve` と `refresh` の呼び出しの合計。
	pub fn resolve_count(&self, video_id: &VideoId) -> usize {
		self.state
			.lock()
			.map(|s| {
				s.resolve_calls
					.iter()
					.filter(|(id, _)| id == video_id)
					.count() + s
					.refresh_calls
					.iter()
					.filter(|(id, _)| id == video_id)
					.count()
			})
			.unwrap_or_default()
	}

	/// 検索語に対する `search` の結果を指定する。
	pub fn set_search(&self, query: &str, result: Result<TrackMeta, ResolveError>) {
		if let Ok(mut s) = self.state.lock() {
			s.searches.insert(query.to_string(), result);
		}
	}

	/// URL に対する `flat_playlist` の結果を指定する。
	pub fn set_playlist(&self, url: &str, result: Result<PlaylistMeta, ResolveError>) {
		if let Ok(mut s) = self.state.lock() {
			s.playlists.insert(url.to_string(), result);
		}
	}

	/// これまでの `resolve` の呼び出しを、呼ばれた順に返す。
	pub fn resolve_calls(&self) -> Vec<(VideoId, Priority)> {
		self.state
			.lock()
			.map(|s| s.resolve_calls.clone())
			.unwrap_or_default()
	}

	/// これまでの `flat_playlist` の呼び出しを、呼ばれた順に返す。
	pub fn playlist_calls(&self) -> Vec<(String, u32)> {
		self.state
			.lock()
			.map(|s| s.playlist_calls.clone())
			.unwrap_or_default()
	}

	/// 動画 ID の `resolve` を保留する。`release` まで完了しない。
	pub fn hold(&self, video_id: &VideoId) {
		if let Ok(mut s) = self.state.lock() {
			let (tx, _rx) = watch::channel(false);
			s.holds.insert(video_id.clone(), tx);
		}
	}

	/// 保留を解く。待っていた `resolve` は指定済みの結果を返す。
	pub fn release(&self, video_id: &VideoId) {
		if let Ok(mut s) = self.state.lock() {
			if let Some(tx) = s.holds.remove(video_id) {
				tx.send_replace(true);
			}
		}
	}

	/// 指定済みの結果を返す。順に返す結果が残っていればそれを先に返す。未指定なら `NotFound`。
	fn lookup(&self, video_id: &VideoId) -> ResolveResult {
		let found = self.state.lock().ok().and_then(|mut s| {
			let queued = s
				.sequences
				.get_mut(video_id)
				.and_then(|queue| queue.pop_front());
			queued.or_else(|| s.results.get(video_id).cloned())
		});
		found.unwrap_or_else(|| {
			Err(ResolveError::Worker {
				code: watapachi_bot::domain::ResolveErrorCode::NotFound,
				message: "FakeResolver に結果が指定されていません".to_string(),
			})
		})
	}
}

#[async_trait]
impl Resolver for FakeResolver {
	async fn resolve(&self, video_id: &VideoId, priority: Priority) -> ResolveResult {
		let mut hold = self.state.lock().ok().and_then(|mut s| {
			s.resolve_calls.push((video_id.clone(), priority));
			s.holds.get(video_id).map(|tx| tx.subscribe())
		});
		if let Some(rx) = hold.as_mut() {
			// 送信側が先に消えても (release 済み)、値は残るので待ちは解ける
			let _ = rx.wait_for(|released| *released).await;
		}
		self.lookup(video_id)
	}

	async fn refresh(&self, video_id: &VideoId, format_id: Option<&str>) -> ResolveResult {
		if let Ok(mut s) = self.state.lock() {
			s.refresh_calls
				.push((video_id.clone(), format_id.map(str::to_owned)));
		}
		self.lookup(video_id)
	}

	async fn search(&self, query: &str, _priority: Priority) -> Result<TrackMeta, ResolveError> {
		self.state
			.lock()
			.ok()
			.and_then(|s| s.searches.get(query).cloned())
			.unwrap_or(Err(ResolveError::Unavailable))
	}

	async fn flat_playlist(&self, url: &str, limit: u32) -> Result<PlaylistMeta, ResolveError> {
		let found = self.state.lock().ok().and_then(|mut s| {
			s.playlist_calls.push((url.to_string(), limit));
			s.playlists.get(url).cloned()
		});
		let mut playlist = found.unwrap_or(Err(ResolveError::Unavailable))?;
		playlist.entries.truncate(limit as usize);
		Ok(playlist)
	}
}
