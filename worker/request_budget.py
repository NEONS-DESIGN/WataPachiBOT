"""
1 要求の持ち時間 (Rust 側の resolve_timeout_secs から余裕を引いた秒数) の管理。
- Rust 側は --request-budget-secs で持ち時間を渡す。持ち時間を過ぎる前に失敗の応答を返し、
	Rust 側にワーカーを落とされないようにする (落とされると常駐 Deno の失敗の数も消え、同じ待ちを繰り返す)
- worker.py のメインループが要求ごとに begin / end を呼ぶ。予備設定 (worker.py) と常駐 Deno (resident_jsc.py) が
	残りの秒数で待ちを打ち切る
- 期限はスレッドごとに持つ (threading.local)。メインループの begin / end は、メインスレッドの期限だけを変える
- 予備設定の実行スレッドは、処理を受け取るときに依頼元の期限を adopt で写し、その処理の間は変えない。
	打ち切られたスレッドが、次の要求の期限や「期限なし」を読むことはない
- 期限が無い (引数の指定が無い、要求の外) スレッドでは打ち切らない

呼び出し例:
	request_budget.begin(28.0)
	try:
		timeout = request_budget.bound(10.0)
		job_deadline = request_budget.deadline()  # 別スレッドへ渡し、そこで adopt(job_deadline)
	finally:
		request_budget.end()
"""
import threading
import time

# スレッドごとの期限 (time.monotonic の値)。属性 deadline が無い、または None なら期限を設けない
_local = threading.local()


def begin(budget_seconds: float | None) -> None:
	"""
	このスレッドで要求の処理を始める。今から budget_seconds 秒後を期限にする。

	Args:
		budget_seconds: 持ち時間 (秒)。None なら期限を設けない
	Returns:
		なし
	Raises:
		なし
	Example:
		begin(28.0)
	"""
	adopt(None if budget_seconds is None else time.monotonic() + budget_seconds)


def end() -> None:
	"""
	このスレッドの要求の処理を終える。このスレッドの期限を消す (他のスレッドの期限は変えない)。

	Returns:
		なし
	Raises:
		なし
	Example:
		end()
	"""
	adopt(None)


def deadline() -> float | None:
	"""
	このスレッドの期限を返す。別スレッドへ処理を渡すときに、その処理の期限として渡す。

	Returns:
		期限 (time.monotonic の値)。期限が無ければ None
	Raises:
		なし
	Example:
		job_deadline = deadline()
	"""
	return getattr(_local, "deadline", None)


def adopt(value: float | None) -> None:
	"""
	このスレッドの期限を決める。予備設定の実行スレッドが、処理を始めるときに依頼元の期限を写すのに使う。

	Args:
		value: 期限 (time.monotonic の値)。None なら期限を設けない
	Returns:
		なし
	Raises:
		なし
	Example:
		adopt(job.deadline)
	"""
	_local.deadline = value


def remaining() -> float | None:
	"""
	このスレッドの残りの秒数を返す。

	Returns:
		残りの秒数 (過ぎていれば 0 以下)。期限が無ければ None
	Raises:
		なし
	Example:
		remaining() -> 12.5
	"""
	current = deadline()
	return None if current is None else current - time.monotonic()


def bound(seconds: float) -> float:
	"""
	待ちの上限を、このスレッドの残りの秒数で切り詰める。

	Args:
		seconds: 本来の上限秒数
	Returns:
		seconds と残りの秒数の小さい方 (過ぎていれば 0 以下)。期限が無ければ seconds
	Raises:
		なし
	Example:
		bound(30.0) -> 12.5
	"""
	left = remaining()
	return seconds if left is None else min(seconds, left)
