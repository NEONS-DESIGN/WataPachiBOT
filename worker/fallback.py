"""
予備設定 (解決の失敗後に 1 回だけ使う設定) の記憶と失敗の型。
- yt-dlp にも他のワーカーのモジュールにも依存しない (import しても起動を遅らせない)
- 予備設定の実行スレッド (BoundedRunner) と放置の管理は、時計の差し替えを含めて worker.py に置く

呼び出し例:
	memory = FallbackMemory(256)
	memory.remember("jNQXAC9IVRw")
	raise FallbackFailedError(first_error, fallback_error)
"""
import collections


class FallbackFailedError(Exception):
	"""
	解決が通常の設定でも予備設定でも失敗した。両方の理由を持つ。
	- 文言は通常の設定の理由を先頭に置く (エラーの分類に使うため)

	Example:
		raise FallbackFailedError(first_error, fallback_error)
	"""

	def __init__(self, first: Exception, fallback: Exception) -> None:
		"""Args: first は通常の設定の例外、fallback は予備設定の例外。Example: FallbackFailedError(first, second)"""
		super().__init__(f"{first} / fallback: {fallback}")
		self.first = first
		self.fallback = fallback


class FallbackMemory:
	"""
	予備設定でしか解決できなかった動画の ID を、上限付きで新しい順に覚える (LRU)。
	- 上限 (capacity) を超えたら、最も長く使っていない ID から忘れる
	- メインループのスレッドだけが使う (ロックを持たない)

	呼び出し例:
		memory = FallbackMemory(256)
		memory.remember("jNQXAC9IVRw")
		if memory.recall("jNQXAC9IVRw"): ...
	"""

	def __init__(self, capacity: int) -> None:
		"""Args: capacity は覚える上限の件数 (1 以上)。Example: FallbackMemory(256)"""
		self._capacity = max(capacity, 1)
		self._ids: collections.OrderedDict[str, None] = collections.OrderedDict()

	def __len__(self) -> int:
		"""Returns: 覚えている件数。Example: len(memory) -> 1"""
		return len(self._ids)

	def remember(self, video_id: str) -> None:
		"""
		動画 ID を覚える (既にあれば最新にする)。上限を超えたら最も古いものを忘れる。

		Args:
			video_id: 動画 ID
		Example:
			memory.remember("jNQXAC9IVRw")
		"""
		self._ids[video_id] = None
		self._ids.move_to_end(video_id)
		while len(self._ids) > self._capacity:
			self._ids.popitem(last=False)

	def recall(self, video_id: str | None) -> bool:
		"""
		動画 ID を覚えているかを返す。覚えていれば最新にする。

		Args:
			video_id: 動画 ID。None なら常に False
		Returns:
			覚えていれば True
		Example:
			memory.recall("jNQXAC9IVRw") -> True
		"""
		if video_id is None or video_id not in self._ids:
			return False
		self._ids.move_to_end(video_id)
		return True

	def forget(self, video_id: str | None) -> None:
		"""
		動画 ID を忘れる。覚えていなければ何もしない。

		Args:
			video_id: 動画 ID。None なら何もしない
		Example:
			memory.forget("jNQXAC9IVRw")
		"""
		if video_id is not None:
			self._ids.pop(video_id, None)
