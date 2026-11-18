//! 先読みの状態。
//! - キューの先頭から `playback.prefetch_tracks` 曲を、1 曲ずつ順に解決する (`Priority::Prefetch`)
//! - 先頭の 1 曲は解決が済んだら `preload` で準備する。2 曲目以降は解決までにとどめる
//! - ここは状態と次の一手の判断だけを持つ。解決と `preload` の呼び出しはアクターが行う
//! - 呼び出し: `let plan = prefetcher.plan(&upcoming);` の後に、`plan.preload` を準備し `plan.resolve` を解決する

use std::collections::{HashMap, HashSet};

use crate::audio::backend::TrackId;
use crate::domain::{ResolveError, ResolvedStream, VideoId};

/// `plan` が返す次の一手。
#[derive(Debug, Default, PartialEq)]
pub(crate) struct PrefetchPlan {
	/// `preload` で準備する曲 (キューの先頭)。
	pub preload: Option<ResolvedStream>,
	/// 次に解決する曲。返した時点で解決中として記録する。
	pub resolve: Option<VideoId>,
}

/// 先読みの状態。
#[derive(Debug)]
pub(crate) struct Prefetcher {
	/// 先読みする曲数。
	depth: u32,
	/// 解決済みのストリーム。キューの先頭から外れた曲は `plan` で捨てる。
	resolved: HashMap<VideoId, ResolvedStream>,
	/// 先読みの解決に失敗した曲。再生するときに通常の優先度で解決し直す。
	failed: HashSet<VideoId>,
	/// 解決中の曲。同時に 1 曲まで。
	in_flight: Option<VideoId>,
	/// `preload` 済みの曲とその識別子、形式 (`format_id`)。
	preloaded: Option<(VideoId, TrackId, String)>,
	/// `preload` に失敗した曲。同じ曲で繰り返さない。
	preload_failed: Option<VideoId>,
}

impl Prefetcher {
	/// 空の状態を作る。
	///
	/// # 引数
	/// - `depth`: 先読みする曲数 (`playback.prefetch_tracks`)
	///
	/// # 戻り値
	/// 何も持たない `Prefetcher`
	///
	/// # 呼び出し例
	/// ```ignore
	/// let prefetcher = Prefetcher::new(config.playback.prefetch_tracks);
	/// ```
	pub fn new(depth: u32) -> Self {
		Self {
			depth,
			resolved: HashMap::new(),
			failed: HashSet::new(),
			in_flight: None,
			preloaded: None,
			preload_failed: None,
		}
	}

	/// 先読みする曲数。キューの先頭から何曲を読むかに使う。
	///
	/// # 戻り値
	/// 曲数
	///
	/// # 呼び出し例
	/// ```ignore
	/// let upcoming = queue::peek(c, guild, prefetcher.depth())?;
	/// ```
	pub fn depth(&self) -> u32 {
		self.depth
	}

	/// 解決済みのストリームを預ける (`?p` の動画 URL の解決結果など)。
	///
	/// - キューの先頭の範囲に無い曲は、次の `plan` で捨てる
	///
	/// # 引数
	/// - `stream`: 解決済みのストリーム
	///
	/// # 呼び出し例
	/// ```ignore
	/// prefetcher.stash(stream);
	/// ```
	pub fn stash(&mut self, stream: ResolvedStream) {
		self.failed.remove(&stream.video_id);
		self.resolved.insert(stream.video_id.clone(), stream);
	}

	/// 再生を始める曲の解決結果を取り出す。
	///
	/// # 引数
	/// - `video_id`: 再生を始める曲
	///
	/// # 戻り値
	/// 解決済みならそのストリーム。無ければ None
	///
	/// # 呼び出し例
	/// ```ignore
	/// if let Some(stream) = prefetcher.take_stream(&item.video_id) { ... }
	/// ```
	pub fn take_stream(&mut self, video_id: &VideoId) -> Option<ResolvedStream> {
		self.failed.remove(video_id);
		self.resolved.remove(video_id)
	}

	/// 再生を始める曲が `preload` 済みなら、その識別子と形式を取り出す。
	///
	/// - 違う曲の `preload` は残す
	///
	/// # 引数
	/// - `video_id`: 再生を始める曲
	///
	/// # 戻り値
	/// `preload` 済みならその識別子と形式 (`format_id`)。無ければ None
	///
	/// # 呼び出し例
	/// ```ignore
	/// if let Some((track, format_id)) = prefetcher.take_preloaded(&item.video_id) { ... }
	/// ```
	pub fn take_preloaded(&mut self, video_id: &VideoId) -> Option<(TrackId, String)> {
		match self.preloaded.take() {
			Some((id, track, format_id)) if id == *video_id => Some((track, format_id)),
			other => {
				self.preloaded = other;
				None
			}
		}
	}

	/// `preload` 済みの曲が次の曲でなくなっていれば、その記録を消して識別子を返す。
	///
	/// - 呼び出し側はこの識別子をバックエンドで捨てる (`drop_preloaded`)
	/// - `head` が None (次の曲が無い) なら、`preload` 済みの曲はすべて古い
	///
	/// # 引数
	/// - `head`: キューの先頭 (次の曲) の動画 ID
	///
	/// # 戻り値
	/// 古くなった `preload` の識別子。無ければ None
	///
	/// # 呼び出し例
	/// ```ignore
	/// if let Some(track) = prefetcher.take_stale_preload(upcoming.first()) { ... }
	/// ```
	pub fn take_stale_preload(&mut self, head: Option<&VideoId>) -> Option<TrackId> {
		match &self.preloaded {
			Some((id, track, _)) if Some(id) != head => {
				let track = *track;
				self.preloaded = None;
				Some(track)
			}
			_ => None,
		}
	}

	/// `preload` 済みの曲があるか。
	///
	/// # 戻り値
	/// あれば true
	///
	/// # 呼び出し例
	/// ```ignore
	/// if prefetcher.has_preloaded() { ... }
	/// ```
	pub fn has_preloaded(&self) -> bool {
		self.preloaded.is_some()
	}

	/// 先読みの解決結果を受け取る。
	///
	/// # 引数
	/// - `video_id`: 解決した曲
	/// - `result`: 解決結果
	///
	/// # 呼び出し例
	/// ```ignore
	/// prefetcher.complete(video_id, result);
	/// let plan = prefetcher.plan(&upcoming);
	/// ```
	pub fn complete(&mut self, video_id: VideoId, result: Result<ResolvedStream, ResolveError>) {
		if self.in_flight.as_ref() == Some(&video_id) {
			self.in_flight = None;
		}
		match result {
			Ok(stream) => {
				self.failed.remove(&video_id);
				self.resolved.insert(video_id, stream);
			}
			Err(_) => {
				self.failed.insert(video_id);
			}
		}
	}

	/// キューの先頭の並びから、次の一手を決める。
	///
	/// - 並びに無い曲の解決結果と失敗の記録を捨てる
	/// - 先頭の曲が解決済みで、まだ何も `preload` していなければ、`preload` を求める
	/// - 解決中の曲が無ければ、並びの順で最初の未解決の曲を解決中にして返す
	///
	/// # 引数
	/// - `upcoming`: キューの先頭から `depth` 曲の動画 ID (並び順)
	///
	/// # 戻り値
	/// 準備する曲と解決する曲。どちらも無ければ空の `PrefetchPlan`
	///
	/// # 呼び出し例
	/// ```ignore
	/// let plan = prefetcher.plan(&[next, after_next]);
	/// ```
	pub fn plan(&mut self, upcoming: &[VideoId]) -> PrefetchPlan {
		self.resolved.retain(|id, _| upcoming.contains(id));
		self.failed.retain(|id| upcoming.contains(id));
		let preload = upcoming.first().and_then(|head| {
			let blocked = self.preloaded.is_some() || self.preload_failed.as_ref() == Some(head);
			if blocked {
				None
			} else {
				self.resolved.get(head).cloned()
			}
		});
		let resolve = if self.in_flight.is_some() {
			None
		} else {
			upcoming
				.iter()
				.find(|id| !self.resolved.contains_key(*id) && !self.failed.contains(*id))
				.cloned()
		};
		if let Some(id) = &resolve {
			self.in_flight = Some(id.clone());
		}
		PrefetchPlan { preload, resolve }
	}

	/// `preload` の成功を記録する。
	///
	/// # 引数
	/// - `video_id`: 準備した曲
	/// - `track`: バックエンドが割り当てた識別子
	/// - `format_id`: 準備した形式 (曲の失敗の後の取り直しで同じ形式に固定するため)
	///
	/// # 呼び出し例
	/// ```ignore
	/// prefetcher.mark_preloaded(video_id, track, stream.format_id.clone());
	/// ```
	pub fn mark_preloaded(&mut self, video_id: VideoId, track: TrackId, format_id: String) {
		self.preloaded = Some((video_id, track, format_id));
	}

	/// `preload` の失敗を記録する。同じ曲では `preload` を求めなくなる。
	///
	/// # 引数
	/// - `video_id`: 準備に失敗した曲
	///
	/// # 呼び出し例
	/// ```ignore
	/// prefetcher.mark_preload_failed(video_id);
	/// ```
	pub fn mark_preload_failed(&mut self, video_id: VideoId) {
		self.preload_failed = Some(video_id);
	}

	/// すべて捨てる (退出したとき)。
	///
	/// # 呼び出し例
	/// ```ignore
	/// prefetcher.clear();
	/// ```
	pub fn clear(&mut self) {
		*self = Self::new(self.depth);
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use super::*;

	/// 試験で `preload` した形式。
	const FORMAT: &str = "251";

	fn id(c: char) -> VideoId {
		VideoId(c.to_string().repeat(11))
	}

	fn stream(c: char) -> ResolvedStream {
		ResolvedStream {
			video_id: id(c),
			title: c.to_string(),
			duration_s: None,
			url: String::new(),
			http_headers: BTreeMap::new(),
			filesize: 0,
			format_id: String::new(),
			expires_at: None,
			available_at: None,
			is_live: false,
			protocol: "https".to_string(),
		}
	}

	#[test]
	fn resolves_one_at_a_time_in_order_then_preloads_head() {
		let mut p = Prefetcher::new(2);
		let upcoming = [id('b'), id('c')];
		assert_eq!(p.plan(&upcoming).resolve, Some(id('b')));
		// 解決中は次を始めない
		assert_eq!(p.plan(&upcoming), PrefetchPlan::default());

		p.complete(id('b'), Ok(stream('b')));
		let plan = p.plan(&upcoming);
		assert_eq!(plan.preload, Some(stream('b')));
		assert_eq!(plan.resolve, Some(id('c')));
		p.mark_preloaded(id('b'), TrackId(5), FORMAT.to_owned());

		p.complete(id('c'), Ok(stream('c')));
		assert_eq!(p.plan(&upcoming), PrefetchPlan::default());
		assert_eq!(p.take_preloaded(&id('c')), None);
		assert_eq!(
			p.take_preloaded(&id('b')),
			Some((TrackId(5), FORMAT.to_owned()))
		);
	}

	#[test]
	fn failures_are_not_retried_and_dropped_items_are_forgotten() {
		let mut p = Prefetcher::new(2);
		assert_eq!(p.plan(&[id('b')]).resolve, Some(id('b')));
		p.complete(id('b'), Err(ResolveError::Timeout));
		assert_eq!(p.plan(&[id('b')]), PrefetchPlan::default());

		p.stash(stream('x'));
		p.plan(&[id('b')]);
		assert_eq!(p.take_stream(&id('x')), None);
	}

	#[test]
	fn stale_preload_is_taken_only_when_head_changes() {
		let mut p = Prefetcher::new(2);
		p.mark_preloaded(id('b'), TrackId(7), FORMAT.to_owned());
		assert_eq!(p.take_stale_preload(Some(&id('b'))), None);
		assert!(p.has_preloaded());
		assert_eq!(p.take_stale_preload(Some(&id('c'))), Some(TrackId(7)));
		assert!(!p.has_preloaded());
		assert_eq!(p.take_stale_preload(None), None);

		p.mark_preloaded(id('b'), TrackId(8), FORMAT.to_owned());
		assert_eq!(p.take_stale_preload(None), Some(TrackId(8)));
	}
}
