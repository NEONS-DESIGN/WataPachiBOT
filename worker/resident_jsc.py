"""
YouTube の JS チャレンジ (n / sig) を、ワーカーに常駐させた Deno で解く yt-dlp のプロバイダ。
- yt-dlp の標準の Deno プロバイダは解読のたびに Deno を起動し、解読スクリプトと前処理済みプレイヤー (約 4MB) を渡し直す。
	常駐させてプレイヤーを保持すると、2 回目以降は数 ms で済む
- import するとプロバイダが登録される。YoutubeDL の生成より前に import する (speedups.apply が行う)
- 失敗時は JsChallengeProviderError を送出し、yt-dlp が標準の Deno プロバイダで解き直す。Deno は次の解読で起動し直す
- 標準のプロバイダ (待ちに上限が無い) は、今の要求の持ち時間の残りが STANDARD_PROVIDER_MIN_SECONDS 未満なら使わせない
	(EJSBaseJCP.is_available を包む。包めなければ標準の動作のまま続ける)
- 解読スクリプトのエラーで解けなかったプレイヤーは期限つきで覚え、常駐 Deno にも標準のプロバイダにも解かせない
	(EJSBaseJCP._real_bulk_solve を包む。包めなければ標準のプロバイダは今までどおり解き直す)
- Deno の標準出力と標準エラーはパイプで受け取る。ワーカーのプロトコル用の標準出力には流れない
- yt-dlp の内部 API に依存する。import に失敗したら speedups が登録を諦め、標準の動作のまま続ける
- 各往復の上限は、今の要求の持ち時間の残り (request_budget) で切り詰める。Rust 側にワーカーごと落とされる前に
	このプロバイダが失敗を返し、失敗の数をワーカーの中に残す (落とされると数が消え、同じ待ちを繰り返す)
- ロックも残りの秒数を上限に取る。打ち切られた予備設定のスレッドがロックを持っていても、次の要求は待ち過ぎない
- 持ち時間切れ (ロックが取れない、切り詰めた上限で応答が来ない) は Deno の失敗として数えず、Deno も止めない。
	来なかった応答は次の往復の前に読み捨てる。本来の上限 (SOLVE_TIMEOUT_SECONDS など) までに答えなかった Deno だけを
	監視が止める (読み込みスレッドが数えた行の数で、その往復の応答が来たかを見分ける)

出典: NEONS-DESIGN/SatouSioBOT のコミット ed502a6 の module/resident_jsc.py を移植した
MIT License, Copyright (c) 2026 NEONS-DESIGN

呼び出し例:
	import resident_jsc  # 登録
	resident_jsc.warm_up(ydl, 10)
	resident_jsc.shutdown()
"""
import collections
import contextlib
import hashlib
import json
import pathlib
import queue
import subprocess
import sys
import threading
import time
import urllib.parse
from collections.abc import Callable, Iterator
from typing import Any

from yt_dlp.extractor.youtube.jsc._builtin.deno import DenoJCP
from yt_dlp.extractor.youtube.jsc._builtin.ejs import ScriptVariant
from yt_dlp.extractor.youtube.jsc.provider import (
	JsChallengeProvider,
	JsChallengeProviderError,
	JsChallengeProviderRejectedRequest,
	JsChallengeProviderResponse,
	JsChallengeRequest,
	JsChallengeResponse,
	JsChallengeType,
	NChallengeOutput,
	SigChallengeOutput,
	register_preference,
	register_provider,
)
from yt_dlp.utils import Popen

import request_budget
from player_cache import PLAYER_CACHE_KEY_PREFIX, player_cache_files

# Deno 側の処理 (このファイルと同じフォルダに置く)
SERVER_SCRIPT = pathlib.Path(__file__).with_name("resident_jsc.js")
# 応答を待つ上限秒数。超えたら Deno を終了して標準のプロバイダに任せる
# (スクリプトの評価やプレイヤーの読み込みを含む要求 / 保持済みのプレイヤーで解く要求)
SOLVE_TIMEOUT_SECONDS = 30
CACHED_SOLVE_TIMEOUT_SECONDS = 10
# 持ち時間の残りで切り詰めた 1 往復の上限がこれより短ければ、常駐 Deno を使わない
# (保持済みのプレイヤーでも解読に数十〜数百 ms かかる。間に合わない往復で Deno を落とすと、保持したプレイヤーを失う)
MIN_ROUND_TRIP_SECONDS = 0.5
# 強制終了の後、読み込みスレッドが終了を知らせるまで待つ余裕の秒数
READER_GRACE_SECONDS = 5
# 通信の失敗がこの回数続いたら、最後の失敗から FAILURE_RESET_SECONDS の間は常駐 Deno を使わない (標準の Deno プロバイダで解く)
MAX_CONSECUTIVE_FAILURES = 3
FAILURE_RESET_SECONDS = 600
# エラーの説明に添える Deno の標準エラーの行数と文字数
STDERR_TAIL_LINES = 20
STDERR_MESSAGE_LIMIT = 500
# エラーの説明に添える想定外の応答の文字数
RESPONSE_EXCERPT_CHARS = 200
# 標準の Deno プロバイダ (優先度 1000。継承しているためこのプロバイダにも加算される) より先に使わせるための加点
PREFERENCE_BONUS = 1000
# 常駐 Deno の起動オプション。プロンプト、外部取得、設定ファイル、npm を使わない (標準の Deno プロバイダの npm 無し版と同じ方針)
# --optimize-for-size は常駐中のメモリを抑える (解読の速さは変わらない)
DENO_OPTIONS = (
	"--no-prompt", "--no-remote", "--no-lock", "--node-modules-dir=none", "--no-config", "--no-npm", "--cached-only",
	"--v8-flags=--optimize-for-size",
)
# 常駐 Deno を起動したときに標準エラーへ出す記録 (後ろに pid を付ける)
STARTED_LOG = "[resident_jsc] deno started"
# 標準の JS チャレンジプロバイダ (Deno、Bun、Node、QuickJS) を使うのに要る、今の要求の持ち時間の最小の残り秒数
# (標準のプロバイダは解読のたびに JS ランタイムを起動してプレイヤーを渡し、その待ちに上限が無い。
#  常駐 Deno が持ち時間切れで諦めた後に標準のプロバイダが動くと、持ち時間を超えて Rust 側にワーカーごと落とされる。
#  開発機で標準の Deno の解読は約 1 秒 (2026-11-18)。本番機の遅さと抽出の残りの処理を見込んで余裕を持たせる)
STANDARD_PROVIDER_MIN_SECONDS = 5.0
# 標準のプロバイダの is_available を差し替えた印 (同じクラスを二重に包まないため)
BUDGET_GATE_MARK = "_watapachi_budget_gate"
# 解読スクリプトのエラーで解けなかったプレイヤーを覚える件数の上限と秒数
# (エラーはプレイヤーと解読スクリプトの組で決まり、解き直しても同じ。解読スクリプトは venv の入れ替え (ワーカーの起動し直し) でしか
#  変わらない。一時的な失敗を誤って覚えても、期限が過ぎれば解き直す)
BROKEN_PLAYER_MEMORY_SIZE = 16
BROKEN_PLAYER_TTL_SECONDS = 1800.0
# 標準のプロバイダの _real_bulk_solve を差し替えた印 (同じクラスを二重に包まないため)
BROKEN_PLAYER_GATE_MARK = "_watapachi_broken_player_gate"
# 解けなかったプレイヤーを覚えたときに標準エラーへ出す記録 (後ろに URL とエラーを付ける)
BROKEN_PLAYER_LOG = "[resident_jsc] broken player remembered"
# 覚えたプレイヤーの要求に返すエラーの説明 (後ろに URL を付ける)
BROKEN_PLAYER_MESSAGE = "解読に失敗したプレイヤーのため解きません"


class _LineCount:
	"""
	Deno の標準出力から受け取った行の数。読み込みスレッドだけが増やし、監視のタイマーが読む。
	- Deno は要求 1 件に 1 行で答えるため、n 件目の要求の応答が来たかは「数が n 以上か」で分かる

	Example:
		received = _LineCount()
	"""

	def __init__(self) -> None:
		"""0 から数える。Example: _LineCount().value -> 0"""
		self.value = 0


class _SolverError(JsChallengeProviderError):
	"""
	Deno は正常に応答したが、解読に失敗した (Deno を起動し直す必要はない)。

	Example:
		raise _SolverError("常駐 Deno での解読に失敗しました: ...")
	"""


class _BudgetExhaustedError(JsChallengeProviderError):
	"""
	今の要求の持ち時間の残りが足りない。Deno は止めず、通信の失敗としても数えない。

	Example:
		raise _BudgetExhaustedError("要求の持ち時間のうちに常駐 Deno の順番が来ませんでした")
	"""


class _BrokenPlayers:
	"""
	解読スクリプトのエラーで解けなかったプレイヤーの URL を、期限つきで上限の件数まで覚える。
	- 主経路、予備設定、Cookie なしの実行スレッドから使うため、ロックで守る
	- 上限を超えたら、覚えた時刻の古いものから忘れる

	Example:
		memory = _BrokenPlayers(BROKEN_PLAYER_MEMORY_SIZE, BROKEN_PLAYER_TTL_SECONDS)
		memory.remember(player_url)
		memory.recall(player_url) -> True
	"""

	def __init__(self, size: int, ttl: float, clock: Callable[[], float] = time.monotonic) -> None:
		"""
		Args:
			size: 覚える件数の上限
			ttl: 覚えておく秒数
			clock: 現在時刻 (秒) を返す関数。試験で差し替える
		Example:
			_BrokenPlayers(16, 1800.0)
		"""
		self._size = size
		self._ttl = ttl
		self._clock = clock
		self._lock = threading.Lock()
		self._expires: collections.OrderedDict[str, float] = collections.OrderedDict()

	def remember(self, player_url: str) -> None:
		"""
		プレイヤーを覚える。覚え済みなら期限を延ばす。

		Args:
			player_url: プレイヤーの URL
		Example:
			memory.remember(player_url)
		"""
		with self._lock:
			self._expires.pop(player_url, None)
			self._expires[player_url] = self._clock() + self._ttl
			while len(self._expires) > self._size:
				self._expires.popitem(last=False)

	def recall(self, player_url: str) -> bool:
		"""
		プレイヤーを覚えているかを返す。期限を過ぎていたら忘れる。

		Args:
			player_url: プレイヤーの URL
		Returns:
			期限の内に覚えていれば True
		Example:
			memory.recall(player_url) -> False
		"""
		with self._lock:
			expires = self._expires.get(player_url)
			if expires is None:
				return False
			if self._clock() >= expires:
				del self._expires[player_url]
				return False
			return True


def _broken_player_responses(requests: list[JsChallengeRequest]) -> Iterator[JsChallengeProviderResponse]:
	"""
	覚えたプレイヤーの要求に、解かずにエラーの応答を返す。

	Args:
		requests: 覚えたプレイヤーの要求
	Returns:
		要求ごとのエラーの応答を返すイテレータ (yt-dlp は次のプロバイダへ回し、最後は解読の失敗として警告する)
	Example:
		yield from _broken_player_responses(grouped_requests)
	"""
	for request in requests:
		# 2 番目の引数は expected (想定内の失敗として、yt-dlp に不具合の報告を促させない)
		error = JsChallengeProviderError(f"{BROKEN_PLAYER_MESSAGE}: {request.input.player_url}", True)
		yield JsChallengeProviderResponse(request, None, error)


def _round_trip_limit(default: float) -> float:
	"""
	1 往復の上限秒数を、今の要求の持ち時間の残りで切り詰める。

	Args:
		default: 本来の上限秒数
	Returns:
		上限秒数 (持ち時間が無ければ default)
	Raises:
		_BudgetExhaustedError: 切り詰めた上限が MIN_ROUND_TRIP_SECONDS 未満の場合
	Example:
		_round_trip_limit(SOLVE_TIMEOUT_SECONDS) -> 12.5
	"""
	limit = request_budget.bound(default)
	if limit < MIN_ROUND_TRIP_SECONDS:
		raise _BudgetExhaustedError(f"要求の持ち時間の残り ({max(limit, 0.0):.1f} 秒) が足りないため常駐 Deno を使いません")
	return limit


class _ResidentDeno:
	"""
	ワーカーに 1 つだけ常駐させる Deno プロセス。1 行 1 JSON で要求を送り、応答を 1 行受け取る。
	- 最初の要求で起動し、解読スクリプトを評価させる (スクリプトが変われば起動し直す)
	- 1 プロセスで扱うプレイヤーは 1 版だけにする (プレイヤーのコードが書き換えたグローバルな状態を別の版に持ち込まないため)
	- 標準出力と標準エラーはそれぞれ専用スレッドが読む。標準出力の終了 (EOF) は None で知らせ、標準エラーは末尾だけ残す
	- 通信の失敗が MAX_CONSECUTIVE_FAILURES 回続いたら、しばらく使わない (毎回タイムアウトまで待たせないため)
	- ワーカー内の全 YoutubeDL で共有する。要求はロックで直列にする。ロックは持ち時間の残りを上限に取る
	- 持ち時間切れで受け取らなかった応答は、監視のタイマーと一緒に _unanswered に残し、次の往復の前に読み捨てる

	呼び出し例:
		server = _ResidentDeno()
		response = server.request(command, env, script, {"type": "solve", ...})
	"""

	def __init__(self) -> None:
		"""起動はしない。最初の request で起動する。Example: _ResidentDeno()"""
		self._lock = threading.Lock()
		self._process: subprocess.Popen | None = None
		self._lines: queue.Queue[bytes | None] | None = None
		self._stderr: collections.deque[str] = collections.deque(maxlen=STDERR_TAIL_LINES)
		self._script_hash: str | None = None
		self._has_player = False
		self._failures = 0
		self._last_failure = 0.0
		# 持ち時間切れで受け取らなかった応答の監視のタイマー (送った順)。応答を読み捨てたら止める
		self._unanswered: list[threading.Timer] = []
		# 今の Deno へ送った要求の数と、受け取った行の数 (起動のたびに数え直す)
		self._sent = 0
		self._received = _LineCount()

	@property
	def pid(self) -> int | None:
		"""Returns: 稼働中の Deno のプロセス ID。起動していなければ None。Example: server.pid -> 1234"""
		process = self._process
		return process.pid if process is not None and process.poll() is None else None

	@property
	def available(self) -> bool:
		"""Returns: 通信の失敗が続いていない、または最後の失敗から FAILURE_RESET_SECONDS たっていれば True。Example: server.available -> True"""
		if self._failures >= MAX_CONSECUTIVE_FAILURES and time.monotonic() - self._last_failure >= FAILURE_RESET_SECONDS:
			self._failures = 0
		return self._failures < MAX_CONSECUTIVE_FAILURES

	def request(
		self,
		command: list[str],
		env: dict[str, str],
		script: str,
		message: dict[str, Any],
		*,
		new_player: bool = False,
		timeout: float | None = None,
	) -> dict[str, Any]:
		"""
		solve の message を送って応答を返す。必要なら起動して script を評価させる。

		Args:
			command: Deno の起動コマンド
			env: Deno の環境変数
			script: 解読スクリプト (yt-dlp-ejs の lib と core)
			message: solve の要求
			new_player: message でプレイヤーを渡すなら True。別の版を読み込み済みなら起動し直してから送る
			timeout: 各往復の上限秒数。None なら SOLVE_TIMEOUT_SECONDS か CACHED_SOLVE_TIMEOUT_SECONDS。
				どちらも今の要求の持ち時間の残りで切り詰める
		Returns:
			Deno の応答 (type が result か missing)
		Raises:
			_SolverError: 解読に失敗した場合 (Deno は残す)
			_BudgetExhaustedError: 持ち時間の残りが足りない場合 (Deno は残し、失敗として数えない)
			JsChallengeProviderError: 異常終了、タイムアウト、想定外の応答の場合 (Deno を終了し、次の要求で起動し直す)
		Example:
			server.request(command, env, script, {"type": "solve", "player_url": url, "requests": []})
		"""
		script_hash = hashlib.sha256(script.encode()).hexdigest()
		self._acquire()
		try:
			try:
				if self._unanswered and self.pid is None:
					# 打ち切った往復に本来の上限まで答えず監視が止めた (または応答の前に終了した)。本当の失敗として数える
					self._failures += 1
					self._last_failure = time.monotonic()
					# 同じ失敗を次の要求でも数えないよう、タイマーを止めて空にする
					for watchdog in self._unanswered:
						watchdog.cancel()
					self._unanswered.clear()
				if self.pid is None or self._script_hash != script_hash or (new_player and self._has_player):
					init_timeout = timeout or SOLVE_TIMEOUT_SECONDS
					init_limit = _round_trip_limit(init_timeout)
					self._start(command, env)
					try:
						self._exchange({"type": "init", "code": script}, ("ok",), init_limit, init_timeout)
					except _SolverError as error:
						# 評価できないスクリプトは何度送っても同じため、通信の失敗と同じく数えて止める
						raise self._failure(str(error)) from error
					self._script_hash = script_hash
				solve_timeout = timeout or (SOLVE_TIMEOUT_SECONDS if new_player else CACHED_SOLVE_TIMEOUT_SECONDS)
				solve_limit = _round_trip_limit(solve_timeout)
				if new_player:
					# 送った時点で保持済みとみなす。応答を待ち切れなくても Deno は読み込みうるため、
					# 次に別の版を送るときは起動し直す (1 プロセス 1 版を守る)
					self._has_player = True
				response = self._exchange(message, ("result", "missing"), solve_limit, solve_timeout)
			except (_SolverError, _BudgetExhaustedError):
				raise
			except JsChallengeProviderError:
				self._failures += 1
				self._last_failure = time.monotonic()
				self._stop()
				raise
			self._failures = 0
			return response
		finally:
			self._lock.release()

	def _acquire(self) -> None:
		"""
		ロックを取る。今の要求の持ち時間があれば、残りの秒数を上限に待つ。

		Raises:
			_BudgetExhaustedError: 残りの秒数のうちに取れなかった場合 (Deno の失敗としては数えない)
		Example:
			self._acquire()
		"""
		left = request_budget.remaining()
		if left is None:
			self._lock.acquire()
			return
		if left <= 0 or not self._lock.acquire(timeout=left):
			raise _BudgetExhaustedError("要求の持ち時間のうちに常駐 Deno の順番が来ませんでした")

	def stop(self) -> None:
		"""
		Deno を終了する。ワーカーの終了時に呼ぶ。

		Example:
			_server.stop()
		"""
		with self._lock:
			self._stop()

	def _start(self, command: list[str], env: dict[str, str]) -> None:
		"""
		Deno を起動し、標準出力と標準エラーを読むスレッドを開始する。

		Args:
			command: 起動コマンド
			env: 環境変数
		Raises:
			JsChallengeProviderError: 起動できない場合
		Example:
			self._start(command, env)
		"""
		self._stop()
		try:
			# 標準出力と標準エラーはパイプにする。ワーカーの fd 1 (標準エラーへ向け直し済み) を継がせない
			process = Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
		except OSError as error:
			raise JsChallengeProviderError(f"常駐 Deno を起動できませんでした: {error}") from error
		lines: queue.Queue[bytes | None] = queue.Queue()
		received = _LineCount()
		self._stderr.clear()
		threading.Thread(
			target=self._read_lines, args=(process, lines, received), name="resident-deno-stdout", daemon=True,
		).start()
		threading.Thread(target=self._read_stderr, args=(process,), name="resident-deno-stderr", daemon=True).start()
		self._process, self._lines = process, lines
		self._sent, self._received = 0, received
		print(f"{STARTED_LOG} (pid {process.pid})", file=sys.stderr, flush=True)

	@staticmethod
	def _read_lines(process: subprocess.Popen, lines: queue.Queue[bytes | None], received: _LineCount) -> None:
		"""
		Deno の標準出力を 1 行ずつ lines に入れ、受け取った行を数える。読み込みスレッドで実行し、終了時は必ず None を入れる。

		Args:
			process: Deno のプロセス
			lines: 応答の行を入れる待ち行列
			received: 受け取った行の数 (入れる前に増やす。監視が応答の有無を見るため)
		Example:
			threading.Thread(target=_ResidentDeno._read_lines, args=(process, lines, received), daemon=True).start()
		"""
		try:
			for line in process.stdout:
				received.value += 1
				lines.put(line)
		except (OSError, ValueError):
			# _stop がパイプを閉じたときに出る。終了は下の None で要求側へ知らせる
			pass
		finally:
			lines.put(None)

	def _read_stderr(self, process: subprocess.Popen) -> None:
		"""
		Deno の標準エラーを読み、末尾の STDERR_TAIL_LINES 行だけ残す (エラーの説明に添える)。

		Args:
			process: Deno のプロセス
		Example:
			threading.Thread(target=self._read_stderr, args=(process,), daemon=True).start()
		"""
		try:
			for line in process.stderr:
				self._stderr.append(line.decode(errors="replace").rstrip())
		except (OSError, ValueError):
			# _stop がパイプを閉じたときに出る。残す内容は説明用のため、読めなくなったら終える
			pass

	def _failure(self, reason: str) -> JsChallengeProviderError:
		"""
		通信の失敗を表す例外を作る。Deno の標準エラーの末尾を添える。

		Args:
			reason: 失敗の理由
		Returns:
			送出する例外
		Example:
			raise self._failure("常駐 Deno が起動していません")
		"""
		# 読み込みスレッドが append している最中に反復すると "deque mutated during iteration" になるため、複製を連結する
		if stderr := " / ".join(line for line in self._stderr.copy() if line):
			reason = f"{reason} (stderr: {stderr[-STDERR_MESSAGE_LIMIT:]})"
		return JsChallengeProviderError(reason)

	def _exchange(
		self, message: dict[str, Any], expected: tuple[str, ...], limit: float, timeout: float,
	) -> dict[str, Any]:
		"""
		1 件送って応答を 1 件受け取る。ロックを持って呼ぶ。
		- 前の往復で受け取らなかった応答があれば、先に読み捨てる
		- timeout 秒 (本来の上限) を超えても応答が来ていなければ Deno を強制終了して失敗にする
		- limit が timeout より短い (持ち時間で切り詰めた) 場合、limit 秒で応答を待つのをやめ、持ち時間切れにする。
			Deno は止めず、応答は次の往復の前に読み捨てる。強制終了の監視は timeout 秒のまま残すが、
			それまでに応答が来ていれば何もしない
		- 応答を待ち切れなかった場合を除き、どの経路で抜けても監視を止める

		Args:
			message: 送る要求
			expected: 受け付ける応答の type。どれでもなければ通信の失敗 (要求と応答の対応がずれるのを防ぐ)
			limit: 応答を待つ上限秒数 (持ち時間の残りで切り詰めた値)
			timeout: 本来の上限秒数
		Returns:
			応答の辞書
		Raises:
			_SolverError: Deno が error を返した場合
			_BudgetExhaustedError: limit 秒のうちに前の応答も含めて受け取れなかった場合 (失敗として数えない)
			JsChallengeProviderError: 送受信に失敗した場合
		Example:
			self._exchange({"type": "init", "code": script}, ("ok",), 10.0, SOLVE_TIMEOUT_SECONDS)
		"""
		process, lines = self._process, self._lines
		if process is None or lines is None or process.stdin is None:
			raise self._failure("常駐 Deno が起動していません")
		wait_until = time.monotonic() + limit
		self._drain(lines, wait_until)
		# 直列化の失敗 (TypeError など) は監視を始める前に出す
		payload = json.dumps(message).encode() + b"\n"
		truncated = limit < timeout
		timed_out = threading.Event()
		self._sent += 1
		sequence, received = self._sent, self._received

		def kill_on_timeout() -> None:
			"""この往復の応答が来ていなければ、時間切れの印を付けて Deno を強制終了する (Timer のスレッドで実行)。Example: threading.Timer(timeout, kill_on_timeout)"""
			if received.value >= sequence:
				return
			timed_out.set()
			process.kill()

		# 書き込み (4MB のプレイヤーはパイプの容量を超える) と読み込みのどちらで止まっても、強制終了で抜けられる
		watchdog = threading.Timer(timeout, kill_on_timeout)
		watchdog.daemon = True
		watchdog.start()
		keep_watchdog = False
		try:
			try:
				process.stdin.write(payload)
				process.stdin.flush()
				wait = max(wait_until - time.monotonic(), 0.0) if truncated else timeout + READER_GRACE_SECONDS
				line = lines.get(timeout=wait)
			except (OSError, ValueError) as error:
				if timed_out.is_set():
					raise self._failure(f"常駐 Deno が {timeout:g} 秒以内に応答しませんでした") from error
				raise self._failure(f"常駐 Deno への送信に失敗しました: {error}") from error
			except queue.Empty as error:
				if truncated and not timed_out.is_set():
					# 持ち時間切れ。Deno は本来の上限の内側なので止めない。応答は次の往復の前に読み捨てる
					keep_watchdog = True
					self._unanswered.append(watchdog)
					raise _BudgetExhaustedError(
						f"要求の持ち時間のうち ({limit:.1f} 秒) に常駐 Deno が応答しませんでした") from error
				raise self._failure("常駐 Deno の出力を読めなくなりました") from error
		finally:
			if not keep_watchdog:
				watchdog.cancel()
		if line is None:
			if timed_out.is_set():
				raise self._failure(f"常駐 Deno が {timeout:g} 秒以内に応答しませんでした")
			try:
				returncode = process.wait(timeout=READER_GRACE_SECONDS)
			except subprocess.TimeoutExpired:
				returncode = None
			raise self._failure(f"常駐 Deno が応答せずに終了しました (returncode: {returncode})")
		try:
			response = json.loads(line)
		except ValueError as error:
			raise self._failure(f"常駐 Deno の応答を読めませんでした: {line[:RESPONSE_EXCERPT_CHARS]!r}") from error
		response_type = response.get("type") if isinstance(response, dict) else None
		if response_type == "error":
			raise _SolverError(f"常駐 Deno での解読に失敗しました: {response.get('error')}")
		if response_type not in expected:
			raise self._failure(f"常駐 Deno から想定外の応答がありました: {line[:RESPONSE_EXCERPT_CHARS]!r}")
		return response

	def _drain(self, lines: queue.Queue[bytes | None], wait_until: float) -> None:
		"""
		前の往復で受け取らなかった応答を、送った順に読み捨てる。ロックを持って呼ぶ。

		Args:
			lines: Deno の応答の待ち行列
			wait_until: 待つ期限 (time.monotonic の値)
		Raises:
			_BudgetExhaustedError: 期限までに読み捨て終わらなかった場合 (失敗として数えない)
			JsChallengeProviderError: 応答の前に Deno が終了した場合 (本来の上限を超えて監視が止めた場合を含む)
		Example:
			self._drain(lines, time.monotonic() + 10)
		"""
		while self._unanswered:
			try:
				line = lines.get(timeout=max(wait_until - time.monotonic(), 0.0))
			except queue.Empty as error:
				raise _BudgetExhaustedError("前の往復の応答を待つ間に持ち時間を使い切りました") from error
			self._unanswered.pop(0).cancel()
			if line is None:
				raise self._failure("常駐 Deno が前の往復に応答せずに終了しました")

	def _stop(self) -> None:
		"""Deno を終了する。起動していなければ何もしない。ロックを持って呼ぶ。Example: self._stop()"""
		process, self._process, self._lines, self._script_hash = self._process, None, None, None
		self._has_player = False
		self._sent, self._received = 0, _LineCount()
		for watchdog in self._unanswered:
			watchdog.cancel()
		self._unanswered.clear()
		if process is None:
			return
		try:
			process.kill()
			process.wait(timeout=SOLVE_TIMEOUT_SECONDS)
		except (OSError, subprocess.TimeoutExpired) as error:
			# 終了済みなどで kill に失敗しても、パイプを閉じれば Deno は入力の終わりで終了する
			print(f"[resident_jsc] deno stop: {error}", file=sys.stderr, flush=True)
		for pipe in (process.stdin, process.stdout, process.stderr):
			if pipe is not None:
				with contextlib.suppress(OSError):
					pipe.close()


_server = _ResidentDeno()
# 解読スクリプトのエラーで解けなかったプレイヤー。常駐 Deno と標準のプロバイダの両方が引く
_broken_players = _BrokenPlayers(BROKEN_PLAYER_MEMORY_SIZE, BROKEN_PLAYER_TTL_SECONDS)


@register_provider
class ResidentDenoJCP(DenoJCP):
	"""
	常駐 Deno で解く JS チャレンジプロバイダ。Deno の検出、解読スクリプトの取得と検証は標準の Deno プロバイダのものを使う。
	yt-dlp が YoutubeIE ごとに生成する。直接は呼ばない。

	呼び出し例:
		import resident_jsc  # 読み込むと登録され、yt-dlp が JS チャレンジの解読に使う
	"""

	PROVIDER_NAME = "resident-deno"

	def close(self) -> None:
		"""常駐 Deno はワーカー内の全 YoutubeDL で共有するため、YoutubeDL の終了では止めない。Example: provider.close()"""

	def is_available(self) -> bool:
		"""Returns: Deno が使え、常駐 Deno の通信の失敗が続いていなければ True。Example: provider.is_available() -> True"""
		return _server.available and super().is_available()

	def _real_bulk_solve(self, requests: list[JsChallengeRequest]) -> Iterator[JsChallengeProviderResponse]:
		"""
		プレイヤーごとにまとめて常駐 Deno で解く。yt-dlp が呼ぶ。
		- 解読スクリプトのエラー (_SolverError) で解けなかったプレイヤーは覚えて、期限まで Deno に送らずにエラーの応答を返す
			(同じ失敗の繰り返しと、Deno が保持した別の版のプレイヤーの読み込み直しを省く)

		Args:
			requests: 解読の要求
		Returns:
			要求ごとの応答を返すイテレータ
		Raises:
			JsChallengeProviderRejectedRequest: 解読スクリプトが npm 版の場合
			JsChallengeProviderError: 常駐 Deno との通信に失敗した場合 (yt-dlp が標準のプロバイダで解き直す)
		Example:
			responses = list(provider._real_bulk_solve(requests))
		"""
		if self._lib_script.variant == ScriptVariant.DENO_NPM:
			# npm から読み込む版は import 文を含み、実行時に評価できない
			raise JsChallengeProviderRejectedRequest("npm 版の解読スクリプトは常駐 Deno では評価できません")
		grouped: dict[str, list[JsChallengeRequest]] = collections.defaultdict(list)
		for request in requests:
			grouped[request.input.player_url].append(request)
		for player_url, grouped_requests in grouped.items():
			if _broken_players.recall(player_url):
				yield from _broken_player_responses(grouped_requests)
				continue
			try:
				output = self._solve(player_url, grouped_requests)
			except _SolverError as error:
				_broken_players.remember(player_url)
				print(f"{BROKEN_PLAYER_LOG}: {player_url}: {str(error)[:STDERR_MESSAGE_LIMIT]}", file=sys.stderr, flush=True)
				raise
			responses = output.get("responses")
			if not isinstance(responses, list) or len(responses) != len(grouped_requests):
				raise JsChallengeProviderError(
					f"常駐 Deno の応答の件数が要求と一致しません: {str(output)[:RESPONSE_EXCERPT_CHARS]}")
			for request, response_data in zip(grouped_requests, responses, strict=True):
				data = response_data.get("data") if isinstance(response_data, dict) else None
				if not isinstance(data, dict) or response_data.get("type") != "result":
					error = response_data.get("error") if isinstance(response_data, dict) else response_data
					yield JsChallengeProviderResponse(
						request, None, JsChallengeProviderError(f"常駐 Deno での解読に失敗しました: {error}"))
					continue
				yield JsChallengeProviderResponse(request, JsChallengeResponse(request.type, (
					NChallengeOutput(data) if request.type is JsChallengeType.N else SigChallengeOutput(data)
				)))

	def _solve(
		self, player_url: str, requests: list[JsChallengeRequest], timeout: float | None = None,
	) -> dict[str, Any]:
		"""
		1 つのプレイヤーの要求を解く。まずプレイヤーを送らずに解かせ、Deno が保持していなければ、
		前処理済みプレイヤーのキャッシュか、取得したプレイヤーを送る。

		Args:
			player_url: プレイヤーの URL
			requests: このプレイヤーの要求。空なら読み込ませるだけ
			timeout: 各往復の上限秒数。None なら既定
		Returns:
			yt-dlp-ejs の jsc() の出力
		Raises:
			JsChallengeProviderError: 解読や通信に失敗した場合
		Example:
			provider._solve(player_url, [])
		"""
		message = {
			"type": "solve",
			"player_url": player_url,
			"requests": [{"type": request.type.value, "challenges": request.input.challenges} for request in requests],
		}
		output = self._request(message, timeout=timeout)
		if output["type"] != "missing":
			return output
		cache_key = f"{PLAYER_CACHE_KEY_PREFIX}{player_url}"
		if self._ENABLE_PREPROCESSED_PLAYER_CACHE and (preprocessed := self.ie.cache.load(self._CACHE_SECTION, cache_key)):
			return self._request({**message, "preprocessed_player": preprocessed}, new_player=True, timeout=timeout)
		video_id = next((request.video_id for request in requests), None)
		player = self._get_player(video_id, player_url)
		output = self._request(
			{**message, "player": player, "return_preprocessed": self._ENABLE_PREPROCESSED_PLAYER_CACHE},
			new_player=True,
			timeout=timeout,
		)
		if preprocessed := output.pop("preprocessed_player", None):
			self.ie.cache.store(self._CACHE_SECTION, cache_key, preprocessed)
		return output

	def _request(self, message: dict[str, Any], *, new_player: bool = False, timeout: float | None = None) -> dict[str, Any]:
		"""
		常駐 Deno に要求を送る。起動していなければ起動して解読スクリプトを評価させる。

		Args:
			message: solve の要求
			new_player: message でプレイヤーを渡すなら True
			timeout: 各往復の上限秒数。None なら既定
		Returns:
			Deno の応答
		Raises:
			JsChallengeProviderError: 解読や通信に失敗した場合
		Example:
			self._request({"type": "solve", "player_url": player_url, "requests": []})
		"""
		command = [self.runtime_info.path, "run", *DENO_OPTIONS, str(SERVER_SCRIPT)]
		# core の jsc を明示的にグローバルへ置く (宣言が var でなくなっても init で参照できるように)
		script = f"{self._lib_script.code}\nObject.assign(globalThis, lib);\n{self._core_script.code}\nglobalThis.jsc = jsc;\n"
		# 標準エラーをエラーの説明に添えるため、色付けの制御文字を出させない
		env = {**self._get_env_options(), "NO_COLOR": "1"}
		return _server.request(command, env, script, message, new_player=new_player, timeout=timeout)


def _standard_provider_allowed() -> bool:
	"""
	標準の JS チャレンジプロバイダを今使ってよいかを、今のスレッドの持ち時間の残りで決める。

	Returns:
		持ち時間が無い、または残りが STANDARD_PROVIDER_MIN_SECONDS 以上なら True
	Example:
		_standard_provider_allowed() -> True
	"""
	left = request_budget.remaining()
	return left is None or left >= STANDARD_PROVIDER_MIN_SECONDS


def _install_budget_gate() -> bool:
	"""
	標準の JS チャレンジプロバイダ (EJSBaseJCP の派生) の is_available を包み、持ち時間の残りが足りなければ False にする。
	- yt-dlp は常駐 Deno の失敗 (持ち時間切れを含む) の後、is_available が True の次のプロバイダで解き直す。
		標準のプロバイダの待ちには上限が無いため、残りが短いときは使わせない (JS チャレンジを解けないまま抽出を続けさせる)
	- 常駐 Deno のプロバイダ (ResidentDenoJCP) は対象にしない (自分で持ち時間を守るため)
	- yt-dlp の内部 API (EJSBaseJCP.is_available) に依存する。包めなければ例外を送出し、呼び出し側が標準の動作のまま続ける
	- 包んだ関数の中で想定外の例外が出たら、元の is_available の結果を使う
	- 二重に包まない (BUDGET_GATE_MARK の印で見分ける)

	Returns:
		包んだ (または包み済み) なら True
	Raises:
		ImportError / AttributeError: yt-dlp の内部構成が変わった場合
	Example:
		_install_budget_gate() -> True
	"""
	from yt_dlp.extractor.youtube.jsc._builtin.ejs import EJSBaseJCP

	original = EJSBaseJCP.__dict__["is_available"]
	if getattr(original, BUDGET_GATE_MARK, False):
		return True

	def is_available(self: Any) -> bool:
		"""
		持ち時間の残りが足りなければ False、そうでなければ元の is_available の結果を返す。yt-dlp が呼ぶ。

		Returns:
			このプロバイダを使ってよければ True
		Example:
			provider.is_available() -> False  # 残りが STANDARD_PROVIDER_MIN_SECONDS 未満のとき
		"""
		try:
			if not isinstance(self, ResidentDenoJCP) and not _standard_provider_allowed():
				return False
		except Exception as error:  # 判定の失敗は標準の動作 (元の結果) に戻す
			print(f"[resident_jsc] budget_gate: {type(error).__name__}: {error}", file=sys.stderr, flush=True)
		return original(self)

	setattr(is_available, BUDGET_GATE_MARK, True)
	EJSBaseJCP.is_available = is_available
	return True


try:
	BUDGET_GATE_INSTALLED = _install_budget_gate()
except Exception as _gate_error:  # 包めなくても常駐 Deno は使える。標準のプロバイダは上限なしのまま (従来の動作)
	BUDGET_GATE_INSTALLED = False
	print(f"[resident_jsc] budget_gate: {type(_gate_error).__name__}: {_gate_error}", file=sys.stderr, flush=True)


def _install_broken_player_gate() -> bool:
	"""
	標準の JS チャレンジプロバイダ (EJSBaseJCP の派生) の _real_bulk_solve を包み、常駐 Deno が解読に失敗して覚えた
	プレイヤーの要求を、ランタイムを起動せずにエラーの応答で返す。残りの要求だけを元の関数に渡す。
	- yt-dlp は常駐 Deno の失敗の後、標準の Deno で解き直す。解読スクリプトのエラーはランタイムを替えても同じため、起動の待ちを省く
	- 常駐 Deno のプロバイダ (ResidentDenoJCP) は _real_bulk_solve を自分で持つため、包んだ関数を通らない
	- yt-dlp の内部 API (EJSBaseJCP._real_bulk_solve) に依存する。包めなければ例外を送出し、呼び出し側が標準の動作のまま続ける
	- 二重に包まない (BROKEN_PLAYER_GATE_MARK の印で見分ける)

	Returns:
		包んだ (または包み済み) なら True
	Raises:
		ImportError / KeyError: yt-dlp の内部構成が変わった場合
	Example:
		_install_broken_player_gate() -> True
	"""
	from yt_dlp.extractor.youtube.jsc._builtin.ejs import EJSBaseJCP

	original = EJSBaseJCP.__dict__["_real_bulk_solve"]
	if getattr(original, BROKEN_PLAYER_GATE_MARK, False):
		return True

	def _real_bulk_solve(self: Any, requests: list[JsChallengeRequest]) -> Iterator[JsChallengeProviderResponse]:
		"""
		覚えたプレイヤーの要求にエラーの応答を返し、残りを元の関数で解く。yt-dlp が呼ぶ。

		Args:
			requests: 解読の要求
		Returns:
			要求ごとの応答を返すイテレータ
		Example:
			responses = list(provider._real_bulk_solve(requests))
		"""
		broken = [request for request in requests if _broken_players.recall(request.input.player_url)]
		yield from _broken_player_responses(broken)
		# 要求は値で比べると同じものがありうるため、同じオブジェクトかで分ける
		broken_ids = {id(request) for request in broken}
		remaining = [request for request in requests if id(request) not in broken_ids]
		if remaining:
			yield from original(self, remaining)

	setattr(_real_bulk_solve, BROKEN_PLAYER_GATE_MARK, True)
	EJSBaseJCP._real_bulk_solve = _real_bulk_solve
	return True


try:
	BROKEN_PLAYER_GATE_INSTALLED = _install_broken_player_gate()
except Exception as _broken_gate_error:  # 包めなくても常駐 Deno の記憶は使える。標準のプロバイダは今までどおり解き直す
	BROKEN_PLAYER_GATE_INSTALLED = False
	print(
		f"[resident_jsc] broken_player_gate: {type(_broken_gate_error).__name__}: {_broken_gate_error}",
		file=sys.stderr, flush=True,
	)


def _latest_cached_player_url(ydl: Any) -> str | None:
	"""
	yt-dlp のキャッシュにある前処理済みプレイヤーのうち、最も新しいもののプレイヤー URL を返す。

	Args:
		ydl: キャッシュの場所を持つ YoutubeDL
	Returns:
		プレイヤーの URL。キャッシュが無ければ None
	Example:
		_latest_cached_player_url(ydl) -> "https://www.youtube.com/s/player/<版>/player_es6.vflset/en_US/base.js"
	"""
	files = player_cache_files(ydl)
	if not files:
		return None
	# ファイル名は、キーを URL エンコードして % を , に置き換えたもの
	return urllib.parse.unquote(files[0].stem.replace(",", "%"))[len(PLAYER_CACHE_KEY_PREFIX):]


def warm_up(ydl: Any, timeout: float) -> bool:
	"""
	常駐 Deno を起動し、キャッシュにある最新のプレイヤーを読み込ませる (最初の解読の待ちを無くす)。
	- プレイヤーが更新されていれば、最初の解読で新しい版を読み込ませるだけなので、失敗しても抽出には影響しない
	- 失敗した場合は Deno を終了させる (_server が通信の失敗として扱う)

	Args:
		ydl: 解決用の YoutubeDL
		timeout: 各往復の上限秒数
	Returns:
		準備できたら True。プロバイダやキャッシュが無い、失敗した場合は False
	Raises:
		なし (失敗は標準エラーへ記録する)
	Example:
		warm_up(ydl, 10) -> True
	"""
	try:
		ie = ydl.get_info_extractor("Youtube")
		ie.initialize()
		provider = next(
			(provider for provider in ie._jsc_director.providers.values() if isinstance(provider, ResidentDenoJCP)), None)
		player_url = _latest_cached_player_url(ydl)
		if provider is None or player_url is None or not provider.is_available():
			return False
		provider._solve(player_url, [], timeout=timeout)
	except Exception as error:  # 準備の失敗は最初の解読で起動し直すだけ。起動は止めない
		print(f"[resident_jsc] warm_up: {type(error).__name__}: {error}", file=sys.stderr, flush=True)
		return False
	return True


def shutdown() -> None:
	"""
	常駐 Deno を終了する。ワーカーの終了時に呼ぶ。

	Example:
		shutdown()
	"""
	_server.stop()


@register_preference(ResidentDenoJCP)
def _preference(provider: JsChallengeProvider, requests: list[JsChallengeRequest]) -> int:
	"""
	常駐 Deno のプロバイダを標準の Deno プロバイダより優先させる。yt-dlp が呼ぶ。

	Args:
		provider: 対象のプロバイダ
		requests: 解読の要求
	Returns:
		加点
	Example:
		_preference(provider, requests) -> PREFERENCE_BONUS
	"""
	return PREFERENCE_BONUS
