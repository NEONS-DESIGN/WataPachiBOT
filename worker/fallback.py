"""
予備設定 (解決の失敗後に 1 回だけ使う設定) の記憶と失敗の型。
- yt-dlp にも他のワーカーのモジュールにも依存しない (import しても起動を遅らせない)
- 予備設定の実行スレッド (BoundedRunner) は runner.py、放置の管理と時計の差し替えは worker.py に置く

呼び出し例:
	memory = FallbackMemory(256)
	memory.remember("jNQXAC9IVRw")
	raise FallbackFailedError(first_error, fallback_error)
"""
import collections
import time
from collections.abc import Callable

# 予備設定へ直接回す動画を覚えておく秒数 (6 時間)。覚えた時点から数え、過ぎたら忘れて主経路から試し直す
# (主経路の not_found は一時的なことがある。直接回した予備設定が成功し続けると、忘れる機会が無くなるため)
FALLBACK_MEMORY_TTL_SECONDS = 6 * 60 * 60


class FallbackFailedError(Exception):
	"""
	解決が通常の設定でも予備設定でも失敗した。両方の理由を持つ。
	- 文言は通常の設定の理由を先頭に置く (エラーの分類に使うため)
	- first_unreliable が True なら、通常の設定の理由 (Cookie ありの not_found) は当てにならない

	Example:
		raise FallbackFailedError(first_error, fallback_error)
		raise FallbackFailedError(first_error, fallback_error, first_unreliable=True)
	"""

	def __init__(self, first: Exception, fallback: Exception, *, first_unreliable: bool = False) -> None:
		"""
		Args:
			first: 通常の設定の例外
			fallback: 予備設定の例外
			first_unreliable: 通常の設定の理由が当てにならないか (Cookie ありの not_found で取り直した場合に True)
		Example:
			FallbackFailedError(first, second)
		"""
		super().__init__(f"{first} / fallback: {fallback}")
		self.first = first
		self.fallback = fallback
		self.first_unreliable = first_unreliable


class FallbackMemory:
	"""
	予備設定でしか解決できなかった動画の ID を、上限と期限付きで新しい順に覚える (LRU)。
	- 上限 (capacity) を超えたら、最も長く使っていない ID から忘れる
	- 覚えてから ttl_seconds 秒たった ID は、無いものとして扱って取り除く (recall で期限は延びない)
	- メインループのスレッドだけが使う (ロックを持たない)

	呼び出し例:
		memory = FallbackMemory(256)
		memory.remember("jNQXAC9IVRw")
		if memory.recall("jNQXAC9IVRw"): ...
		FallbackMemory(256, ttl_seconds=60.0, clock=lambda: clock[0])  # 試験で時計を差し替える
	"""

	def __init__(
		self, capacity: int, ttl_seconds: float = FALLBACK_MEMORY_TTL_SECONDS,
		clock: Callable[[], float] = time.monotonic,
	) -> None:
		"""
		Args:
			capacity: 覚える上限の件数 (1 以上)
			ttl_seconds: 覚えておく秒数 (正の数)
			clock: 現在時刻 (秒) を返す関数。単調に増えるもの
		Example:
			FallbackMemory(256)
		"""
		self._capacity = max(capacity, 1)
		self._ttl_seconds = ttl_seconds
		self._clock = clock
		# 動画 ID と、覚えた時刻 (clock の値)
		self._ids: collections.OrderedDict[str, float] = collections.OrderedDict()

	def __len__(self) -> int:
		"""Returns: 期限内で覚えている件数 (期限切れは取り除く)。Example: len(memory) -> 1"""
		self._drop_expired()
		return len(self._ids)

	def _is_expired(self, remembered_at: float, now: float) -> bool:
		"""
		覚えた時刻から期限を過ぎたかを返す。

		Args:
			remembered_at: 覚えた時刻 (clock の値)
			now: 現在時刻 (clock の値)
		Returns:
			ttl_seconds 秒以上たっていれば True
		Example:
			self._is_expired(remembered_at, self._clock()) -> False
		"""
		return now - remembered_at >= self._ttl_seconds

	def _drop_expired(self) -> None:
		"""
		期限切れの ID をすべて取り除く。件数は上限 (capacity) までのため、全件を見ても軽い。

		Example:
			self._drop_expired()
		"""
		now = self._clock()
		expired = [video_id for video_id, remembered_at in self._ids.items() if self._is_expired(remembered_at, now)]
		for video_id in expired:
			del self._ids[video_id]

	def remember(self, video_id: str) -> None:
		"""
		動画 ID を覚える (既にあれば最新にし、期限も覚え直した時点から数え直す)。
		- 期限切れを先に取り除き、それでも上限を超えたら最も古いものを忘れる

		Args:
			video_id: 動画 ID
		Example:
			memory.remember("jNQXAC9IVRw")
		"""
		self._drop_expired()
		self._ids[video_id] = self._clock()
		self._ids.move_to_end(video_id)
		while len(self._ids) > self._capacity:
			self._ids.popitem(last=False)

	def recall(self, video_id: str | None) -> bool:
		"""
		動画 ID を期限内で覚えているかを返す。覚えていれば最新にする。期限切れなら忘れて False を返す。

		Args:
			video_id: 動画 ID。None なら常に False
		Returns:
			期限内で覚えていれば True
		Example:
			memory.recall("jNQXAC9IVRw") -> True
		"""
		if video_id is None:
			return False
		remembered_at = self._ids.get(video_id)
		if remembered_at is None:
			return False
		if self._is_expired(remembered_at, self._clock()):
			del self._ids[video_id]
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
