"""
処理を専用のデーモンスレッドで 1 件ずつ実行し、待ちを上限秒数で打ち切る実行器 (予備設定の取得に使う)。
- yt-dlp に依存しない。依存するワーカーのモジュールは request_budget だけ
- 放置の後に接続を張り直す処理と時計は、作る側 (worker.py) が渡す

呼び出し例:
	runner = BoundedRunner("stream-fallback", on_idle=reset_connections, idle_seconds=60.0)
	info = runner.run(lambda cache: make(cache).extract_info(url, download=False), 3.0)
"""
import queue
import threading
import time
from typing import Any, Callable

import request_budget


class FallbackTimeoutError(Exception):
	"""
	予備設定の取得が持ち時間の残りを超えた。取得は裏で続くが、結果は使わない。

	Example:
		raise FallbackTimeoutError(3.0)
	"""

	def __init__(self, limit: float) -> None:
		"""Args: limit は待った秒数。Example: FallbackTimeoutError(3.0)"""
		super().__init__(f"fallback timed out after {limit:.1f}s")
		self.limit = limit


class _FallbackJob:
	"""
	BoundedRunner へ渡す 1 件の処理と、その結果。BoundedRunner.run だけが作る。

	呼び出し例:
		job = _FallbackJob(action, request_budget.deadline())
	"""

	def __init__(self, action: Callable[[dict[str | None, Any]], Any], deadline: float | None) -> None:
		"""
		Args:
			action: 実行スレッドの持つインスタンスの辞書を受け取る処理
			deadline: 依頼元の要求の期限 (time.monotonic の値)。None なら期限なし
		Example:
			_FallbackJob(lambda cache: 1, None)
		"""
		self.action = action
		self.deadline = deadline
		self.done = threading.Event()
		self.result: Any = None
		self.error: BaseException | None = None


class BoundedRunner:
	"""
	処理を専用のデーモンスレッドで 1 件ずつ実行し、待ちを上限秒数で打ち切る。予備設定の取得に使う。
	- 処理には、実行スレッドだけが持つ辞書を渡す。YoutubeDL はその中に作り、スレッド間で共有しない
	- 打ち切ったら、そのスレッドと辞書 (中のインスタンス) を捨てる。次の実行で新しいスレッドを作る。
		yt-dlp の処理は外から止められないため、捨てたスレッドは処理を終えてから自分で終わる。結果は使わない
	- 処理の期限は run を呼んだときの request_budget の期限で、実行スレッドは処理の間それを使い続ける。
		打ち切られたスレッドは期限を過ぎているため、常駐 Deno などの待ちに入らない
	- on_idle を渡すと、前の処理の終わりから idle_seconds 以上あいた処理の前に、辞書の値の一覧を渡して呼ぶ
		(実行スレッドの中で呼ぶ。インスタンスの接続の張り直しに使う)
	- 呼び出しは 1 スレッド (メインループ) からに限る

	呼び出し例:
		runner = BoundedRunner("stream-fallback")
		info = runner.run(lambda cache: make(cache).extract_info(url, download=False), 3.0)
	"""

	def __init__(
		self,
		name: str,
		*,
		on_idle: Callable[[list[Any]], None] | None = None,
		idle_seconds: float = 0.0,
		clock: Callable[[], float] = time.monotonic,
	) -> None:
		"""
		スレッドは最初の run で作る。

		Args:
			name: 実行スレッドの名前
			on_idle: 放置の後の処理の前に、辞書の値の一覧を受け取って呼ぶ処理。None なら放置を判定しない
			idle_seconds: 放置とみなす秒数 (on_idle を渡したときだけ使う)
			clock: 現在時刻 (秒) を返す関数。試験で差し替える
		Example:
			BoundedRunner("stream-fallback")
			BoundedRunner("stream-fallback", on_idle=reset_connections, idle_seconds=60.0, clock=lambda: _now())
		"""
		self._name = name
		self._on_idle = on_idle
		self._idle_seconds = idle_seconds
		self._clock = clock
		self._tasks: queue.Queue[_FallbackJob | None] | None = None

	def _loop(self, tasks: queue.Queue[_FallbackJob | None], state: dict[str | None, Any]) -> None:
		"""
		実行スレッドの本体。None を受け取るまで処理を順に実行する。

		Args:
			tasks: 処理の待ち行列
			state: このスレッドだけが持つ辞書
		Example:
			threading.Thread(target=self._loop, args=(queue.Queue(), {}), daemon=True).start()
		"""
		last_used: float | None = None
		while True:
			job = tasks.get()
			if job is None:
				return
			# このスレッドのインスタンスの接続も、放置の後は張り直す (インスタンスはこのスレッドだけが使う)
			if self._on_idle is not None and last_used is not None and self._clock() - last_used >= self._idle_seconds:
				self._on_idle(list(state.values()))
			# 依頼元の要求の期限で固定する。メインループが次の要求で期限を変えても、このスレッドには届かない
			request_budget.adopt(job.deadline)
			try:
				job.result = job.action(state)
			except BaseException as error:  # 呼び出し側へ渡して送出させる。ここで握りつぶさない
				job.error = error
			finally:
				request_budget.adopt(None)
				last_used = self._clock()
				job.done.set()

	def run(self, action: Callable[[dict[str | None, Any]], Any], limit: float | None) -> Any:
		"""
		処理を実行スレッドで実行し、結果を返す。

		Args:
			action: 実行スレッドの辞書を受け取る処理
			limit: 待つ上限秒数。None なら終わるまで待つ
		Returns:
			action の戻り値
		Raises:
			FallbackTimeoutError: limit 秒以内に終わらなかった場合 (実行スレッドは捨てる)
			Exception: action が送出した例外
		Example:
			runner.run(lambda cache: 1, 1.0) -> 1
		"""
		tasks = self._queue()
		job = _FallbackJob(action, request_budget.deadline())
		tasks.put(job)
		if not job.done.wait(limit):
			# 今の処理を終えたら終わるよう伝え、このスレッドとインスタンスを二度と使わない
			tasks.put(None)
			self._tasks = None
			raise FallbackTimeoutError(limit if limit is not None else 0.0)
		if job.error is not None:
			raise job.error
		return job.result

	def submit(self, action: Callable[[dict[str | None, Any]], Any]) -> None:
		"""
		処理を実行スレッドへ投げ、終わりを待たずに戻る。起動時の準備に使う。
		- 期限なしで実行する。結果と例外は捨てるため、失敗は action の中で記録すること
		- 後から run で投げた処理は、この処理の後に実行される (run の待ちの上限にはこの処理の残りも含まれる)

		Args:
			action: 実行スレッドの辞書を受け取る処理
		Returns:
			なし
		Example:
			runner.submit(lambda cache: cache.setdefault(None, make()))
		"""
		self._queue().put(_FallbackJob(action, None))

	def _queue(self) -> queue.Queue[_FallbackJob | None]:
		"""
		実行スレッドの待ち行列を返す。スレッドが無ければ (初回、打ち切りの後) 作って開始する。

		Returns:
			処理の待ち行列
		Example:
			self._queue().put(job)
		"""
		if self._tasks is None:
			self._tasks = queue.Queue()
			thread = threading.Thread(target=self._loop, args=(self._tasks, {}), name=self._name, daemon=True)
			thread.start()
		return self._tasks
