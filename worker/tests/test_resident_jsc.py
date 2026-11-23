"""
常駐 Deno の JS チャレンジプロバイダ (resident_jsc.py) の試験。
- yt-dlp の内部 API は sys.modules に置いた偽のモジュールで差し替え、Deno の代わりに Python の子プロセスを使う
- network の印を付けた試験だけが実物の yt-dlp と Deno と YouTube を使う。既定の実行では除外する
- 実行: uv run --directory worker pytest tests/test_resident_jsc.py
- 実物での実行: uv run --directory worker --with "yt-dlp[default,curl-cffi]" --prerelease=allow pytest -m network
"""
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import types
from pathlib import Path
from typing import Any

import pytest

import request_budget
import speedups
from conftest import FakeDownloadError, install_fake_yt_dlp, wait_for
from runner import BoundedRunner, FallbackTimeoutError


ROOT = Path(__file__).resolve().parent.parent

# 実物の試験で解決する動画 (公開から長く残っている短い動画)
NETWORK_VIDEO_URL = "https://www.youtube.com/watch?v=jNQXAC9IVRw"
# 実物の試験で 1 回の応答を待つ上限秒数
NETWORK_RESPONSE_TIMEOUT_SECONDS = 120
# 標準の結果と一致を確かめる項目 (URL は署名が毎回変わるため比べない)
COMPARED_KEYS = ("id", "title", "duration", "format_id", "ext", "acodec", "protocol", "is_live")


class FakeProviderResponse:
	"""JsChallengeProviderResponse の代用。要求、解読の結果、エラーを持つ"""

	def __init__(self, request: Any, response: Any = None, error: Exception | None = None) -> None:
		"""Args: request は要求、response は解読の結果、error はエラー"""
		self.request = request
		self.response = response
		self.error = error


def import_resident_jsc(
	monkeypatch: pytest.MonkeyPatch, *, standard_available: bool | None = None, standard_solve: Any = None,
) -> types.ModuleType:
	"""
	偽の yt_dlp の内部 API を置いて resident_jsc を読み込む。Deno の代わりの子プロセスで _ResidentDeno を試すため。

	Args:
		monkeypatch: pytest の monkeypatch
		standard_available: None でなければ、偽の EJSBaseJCP に is_available (この値を返す) を持たせ、
			DenoJCP をその派生にする (標準のプロバイダの持ち時間の門を試すため)
		standard_solve: None でなければ、偽の EJSBaseJCP の _real_bulk_solve にし、DenoJCP をその派生にする
			(解読できなかったプレイヤーの門を試すため)
	Returns:
		読み込んだ resident_jsc モジュール
	Example:
		resident_jsc = import_resident_jsc(monkeypatch)
	"""
	fake = install_fake_yt_dlp(monkeypatch)
	deno_base: type = object
	if standard_available is not None:
		fake.EJSBaseJCP.is_available = lambda self: standard_available
		deno_base = fake.EJSBaseJCP
	if standard_solve is not None:
		fake.EJSBaseJCP._real_bulk_solve = standard_solve
		deno_base = fake.EJSBaseJCP

	class JsChallengeProviderError(Exception):
		"""JsChallengeProviderError の代用"""

	def identity(target: Any) -> Any:
		"""Returns: target をそのまま返す (登録の代用)"""
		return target

	modules = {
		"yt_dlp.extractor.youtube.jsc._builtin.deno": {"DenoJCP": type("DenoJCP", (deno_base,), {})},
		"yt_dlp.extractor.youtube.jsc.provider": {
			"JsChallengeProvider": object,
			"JsChallengeProviderError": JsChallengeProviderError,
			"JsChallengeProviderRejectedRequest": type("Rejected", (JsChallengeProviderError,), {}),
			"JsChallengeProviderResponse": FakeProviderResponse,
			"JsChallengeRequest": object,
			"JsChallengeResponse": object,
			"JsChallengeType": types.SimpleNamespace(N="n"),
			"NChallengeOutput": object,
			"SigChallengeOutput": object,
			"register_preference": lambda *providers: identity,
			"register_provider": identity,
		},
		"yt_dlp.utils": {"Popen": subprocess.Popen, "DownloadError": FakeDownloadError},
	}
	for name, attributes in modules.items():
		module = types.ModuleType(name)
		for key, value in attributes.items():
			setattr(module, key, value)
		monkeypatch.setitem(sys.modules, name, module)
	monkeypatch.setattr(
		sys.modules["yt_dlp.extractor.youtube.jsc._builtin.ejs"], "ScriptVariant",
		types.SimpleNamespace(DENO_NPM="deno_npm"), raising=False,
	)
	import resident_jsc

	return resident_jsc


# 要求を読んでも応答しない Deno の代わり
SILENT_DENO = [sys.executable, "-c", "import sys, time\nfor _ in sys.stdin:\n\ttime.sleep(60)"]
# 最初の要求を読んだら終了する Deno の代わり (本当の失敗)
EXITING_DENO = [sys.executable, "-c", "import sys\nsys.stdin.readline()"]
# init にすぐ答え、solve には要求の delay 秒の後に答える Deno の代わり
SLOW_DENO = [sys.executable, "-c", (
	"import json, sys, time\n"
	"for raw in sys.stdin:\n"
	"\tmessage = json.loads(raw)\n"
	"\tif message['type'] == 'init':\n"
	"\t\treply = {'type': 'ok'}\n"
	"\telse:\n"
	"\t\ttime.sleep(message.get('delay', 0))\n"
	"\t\treply = {'type': 'result', 'responses': []}\n"
	"\tsys.stdout.write(json.dumps(reply) + '\\n')\n"
	"\tsys.stdout.flush()\n"
)]


def solve(delay: float) -> dict[str, Any]:
	"""Args: delay は SLOW_DENO が答えるまでの秒数。Returns: solve の要求"""
	return {"type": "solve", "requests": [], "delay": delay}


def test_resident_deno_wait_is_bounded_by_the_budget(monkeypatch):
	"""常駐 Deno の往復の上限は持ち時間の残りで切り詰める。持ち時間切れは失敗として数えず、Deno も止めない"""
	resident_jsc = import_resident_jsc(monkeypatch)
	server = resident_jsc._ResidentDeno()
	budget = 1.0
	request_budget.begin(budget)
	started = time.monotonic()
	try:
		with pytest.raises(resident_jsc._BudgetExhaustedError):
			server.request(SILENT_DENO, dict(os.environ), "script", {"type": "solve", "requests": []})
		elapsed = time.monotonic() - started
		assert server.pid is not None
	finally:
		request_budget.end()
		server.stop()
	assert elapsed < resident_jsc.SOLVE_TIMEOUT_SECONDS / 2, elapsed
	assert server._failures == 0


def test_resident_deno_real_failure_is_counted(monkeypatch):
	"""持ち時間の内側の本当の失敗 (応答せずに終了) は、失敗として数えて Deno を止める"""
	resident_jsc = import_resident_jsc(monkeypatch)
	server = resident_jsc._ResidentDeno()
	request_budget.begin(10.0)
	try:
		with pytest.raises(resident_jsc.JsChallengeProviderError) as caught:
			server.request(EXITING_DENO, dict(os.environ), "script", {"type": "solve", "requests": []})
	finally:
		request_budget.end()
		server.stop()
	assert not isinstance(caught.value, resident_jsc._BudgetExhaustedError)
	assert server._failures == 1
	assert server.pid is None


def test_lock_held_by_another_thread_does_not_hold_up_the_next_request(monkeypatch):
	"""他のスレッドが常駐 Deno のロックを持っていても、次の要求は自分の持ち時間の内に諦める"""
	resident_jsc = import_resident_jsc(monkeypatch)
	server = resident_jsc._ResidentDeno()
	holding = threading.Event()
	release = threading.Event()

	def hold() -> None:
		"""ロックを release まで持つ (打ち切られたスレッドの代わり)"""
		with server._lock:
			holding.set()
			release.wait(10)

	holder = threading.Thread(target=hold, daemon=True)
	holder.start()
	holding.wait(5)
	budget = 1.0
	request_budget.begin(budget)
	started = time.monotonic()
	try:
		with pytest.raises(resident_jsc._BudgetExhaustedError):
			server.request(SILENT_DENO, dict(os.environ), "script", {"type": "solve", "requests": []})
		elapsed = time.monotonic() - started
	finally:
		request_budget.end()
		release.set()
		holder.join(5)
		server.stop()
	assert elapsed < budget * 3, elapsed
	assert server._failures == 0
	assert server.pid is None


def test_abandoned_thread_timeout_keeps_the_shared_deno(monkeypatch):
	"""打ち切った予備設定のスレッドの往復が持ち時間切れになっても、共有の Deno を止めず、次の要求は持ち時間の内に終わる"""
	resident_jsc = import_resident_jsc(monkeypatch)
	server = resident_jsc._ResidentDeno()
	env = dict(os.environ)
	try:
		# 期限の無い要求で起動しておく
		assert server.request(SLOW_DENO, env, "script", solve(0))["type"] == "result"
		pid = server.pid
		assert pid is not None

		# 予備設定のスレッドが 2 秒かかる往復の途中で打ち切られる
		runner = BoundedRunner("test-fallback")
		request_budget.begin(0.8)
		try:
			with pytest.raises(FallbackTimeoutError):
				runner.run(lambda state: server.request(SLOW_DENO, env, "script", solve(2.0)), request_budget.remaining())
		finally:
			request_budget.end()

		# 次の要求は、前の往復の応答を読み捨ててから自分の応答を受け取り、持ち時間の内に終わる
		budget = 5.0
		request_budget.begin(budget)
		started = time.monotonic()
		try:
			assert server.request(SLOW_DENO, env, "script", solve(0))["type"] == "result"
		finally:
			request_budget.end()
		assert time.monotonic() - started < budget
		assert server.pid == pid
		assert server._failures == 0
	finally:
		server.stop()


def test_resident_deno_is_not_used_without_budget(monkeypatch):
	"""持ち時間の残りが足りなければ Deno を起動せず、失敗としても数えない"""
	resident_jsc = import_resident_jsc(monkeypatch)
	server = resident_jsc._ResidentDeno()
	request_budget.begin(resident_jsc.MIN_ROUND_TRIP_SECONDS / 2)
	try:
		with pytest.raises(resident_jsc._BudgetExhaustedError):
			server.request(SILENT_DENO, dict(os.environ), "script", {"type": "solve", "requests": []})
	finally:
		request_budget.end()
		server.stop()
	assert server._failures == 0
	assert server.pid is None


def test_answered_round_trip_does_not_kill_the_deno_later(monkeypatch):
	"""持ち時間で打ち切った往復でも、Deno が本来の上限の内に答えていれば、上限を過ぎても Deno を止めない"""
	resident_jsc = import_resident_jsc(monkeypatch)
	server = resident_jsc._ResidentDeno()
	env = dict(os.environ)
	original_timeout = 2.0
	try:
		assert server.request(SLOW_DENO, env, "script", solve(0))["type"] == "result"
		pid = server.pid
		request_budget.begin(0.8)
		try:
			with pytest.raises(resident_jsc._BudgetExhaustedError):
				server.request(SLOW_DENO, env, "script", solve(1.0), timeout=original_timeout)
		finally:
			request_budget.end()
		# 1 秒後に答えた後、本来の上限 (2 秒) を十分に過ぎるまで次の要求を送らない
		time.sleep(original_timeout + 1.0)
		assert server.pid == pid
		assert server._failures == 0
		# 次の要求は前の応答を読み捨て、同じ Deno で答えを受け取る
		assert server.request(SLOW_DENO, env, "script", solve(0))["type"] == "result"
		assert server.pid == pid
	finally:
		server.stop()


def test_unanswering_deno_is_stopped_at_the_original_timeout(monkeypatch):
	"""打ち切った往復に本来の上限を過ぎても答えない Deno は、従来どおり止め、次の要求で失敗として数える"""
	resident_jsc = import_resident_jsc(monkeypatch)
	server = resident_jsc._ResidentDeno()
	env = dict(os.environ)
	original_timeout = 1.0
	try:
		assert server.request(SLOW_DENO, env, "script", solve(0))["type"] == "result"
		request_budget.begin(0.8)
		try:
			with pytest.raises(resident_jsc._BudgetExhaustedError):
				server.request(SLOW_DENO, env, "script", solve(60.0), timeout=original_timeout)
		finally:
			request_budget.end()
		wait_for(lambda: server.pid is None, original_timeout + 5.0)
		# 次の要求で失敗として数える (持ち時間を使い切った要求で確かめ、成功で数え直す前に見る)
		request_budget.begin(resident_jsc.MIN_ROUND_TRIP_SECONDS / 2)
		try:
			with pytest.raises(resident_jsc._BudgetExhaustedError):
				server.request(SLOW_DENO, env, "script", solve(0))
		finally:
			request_budget.end()
		assert server._failures == 1
		# その次は起動し直して答えを受け取る
		assert server.request(SLOW_DENO, env, "script", solve(0))["type"] == "result"
	finally:
		server.stop()


def test_watchdog_failure_is_counted_once(monkeypatch):
	"""監視が止めた後、持ち時間の足りない要求が 3 回続いても _failures は 1 だけ増え、_unanswered は空になる"""
	resident_jsc = import_resident_jsc(monkeypatch)
	server = resident_jsc._ResidentDeno()
	# 監視が Deno を止めた直後の状態 (pid は None、止めたタイマーが残っている)
	watchdog = threading.Timer(60.0, lambda: None)
	server._unanswered.append(watchdog)
	try:
		for _ in range(3):
			request_budget.begin(resident_jsc.MIN_ROUND_TRIP_SECONDS / 2)
			try:
				with pytest.raises(resident_jsc._BudgetExhaustedError):
					server.request(SILENT_DENO, dict(os.environ), "script", {"type": "solve", "requests": []})
			finally:
				request_budget.end()
	finally:
		server.stop()
	assert server._failures == 1
	assert server._unanswered == []


def test_player_cut_off_marks_has_player(monkeypatch):
	"""プレイヤーを送る往復を打ち切っても、Deno はプレイヤーを読み込みうるため保持済みとして扱い、別の版の前に起動し直す"""
	resident_jsc = import_resident_jsc(monkeypatch)
	server = resident_jsc._ResidentDeno()
	env = dict(os.environ)
	try:
		assert server.request(SLOW_DENO, env, "script", solve(0))["type"] == "result"
		pid = server.pid
		request_budget.begin(0.8)
		try:
			with pytest.raises(resident_jsc._BudgetExhaustedError):
				server.request(SLOW_DENO, env, "script", solve(1.0), new_player=True)
		finally:
			request_budget.end()
		assert server._has_player
		# 次のプレイヤーは、同じプロセスへ読み込まず起動し直してから送る
		assert server.request(SLOW_DENO, env, "script", solve(0), new_player=True)["type"] == "result"
		assert server.pid != pid
	finally:
		server.stop()


def test_unexpected_error_does_not_leave_the_watchdog(monkeypatch):
	"""想定外の例外 (直列化できない要求) で往復が終わっても、監視を残さず、後で Deno を止めない"""
	resident_jsc = import_resident_jsc(monkeypatch)
	server = resident_jsc._ResidentDeno()
	env = dict(os.environ)
	original_timeout = 1.0
	try:
		assert server.request(SLOW_DENO, env, "script", solve(0))["type"] == "result"
		pid = server.pid
		with pytest.raises(TypeError):
			server.request(SLOW_DENO, env, "script", {"type": "solve", "bad": object()}, timeout=original_timeout)
		time.sleep(original_timeout + 1.0)
		assert server.pid == pid
		assert server.request(SLOW_DENO, env, "script", solve(0))["type"] == "result"
	finally:
		server.stop()


def run_worker_resolve(args: list[str], url: str, stderr_path: Path) -> dict[str, Any]:
	"""
	実物のワーカーを起動して resolve を 1 回送り、終了まで待つ。

	Args:
		args: worker.py への引数
		url: 解決する URL
		stderr_path: ワーカーの標準エラーを書き出すファイル
	Returns:
		resolve の応答の辞書
	Raises:
		subprocess.TimeoutExpired: ワーカーが上限までに終了しない場合
	Example:
		run_worker_resolve(["--cachedir", "c"], NETWORK_VIDEO_URL, Path("e.log"))
	"""
	request = json.dumps({"id": 1, "op": "resolve", "url": url}).encode("utf-8") + b"\n"
	with stderr_path.open("wb") as stderr:
		completed = subprocess.run(
			[sys.executable, str(ROOT / "worker.py"), *args],
			input=request,
			stdout=subprocess.PIPE,
			stderr=stderr,
			env={**os.environ, "PYTHONUTF8": "1"},
			cwd=ROOT,
			timeout=NETWORK_RESPONSE_TIMEOUT_SECONDS,
			check=True,
		)
	# 標準出力はプロトコル専用。Deno や yt-dlp の出力が混ざっていれば 1 行にならない
	lines = completed.stdout.splitlines()
	assert len(lines) == 1, completed.stdout[:500]
	return json.loads(lines[0])


@pytest.mark.network
def test_resolve_with_resident_deno_matches_standard_result(tmp_path):
	"""常駐 Deno を使った解決が標準の解決と同じ結果になり、標準出力を汚さない"""
	pytest.importorskip("yt_dlp.extractor.youtube.jsc._builtin.deno")
	resident_jsc = pytest.importorskip("resident_jsc")
	if shutil.which(speedups.DENO_COMMAND) is None:
		pytest.skip("Deno が PATH に無い")

	# 標準: 高速化を適用せずに同じ worker.py の Worker で解決する
	standard_script = (
		"import json, sys; from worker import Worker, build_options; "
		f"print(json.dumps(Worker(build_options(sys.argv[1], None, None)).resolve({NETWORK_VIDEO_URL!r}, None)))"
	)
	standard = subprocess.run(
		[sys.executable, "-c", standard_script, str(tmp_path / "standard-cache")],
		stdout=subprocess.PIPE,
		stderr=subprocess.DEVNULL,
		cwd=ROOT,
		timeout=NETWORK_RESPONSE_TIMEOUT_SECONDS,
		check=True,
	)
	expected = json.loads(standard.stdout.decode("utf-8").splitlines()[-1])

	stderr_path = tmp_path / "worker.err"
	response = run_worker_resolve(["--cachedir", str(tmp_path / "cache")], NETWORK_VIDEO_URL, stderr_path)
	log = stderr_path.read_text(encoding="utf-8", errors="replace")
	assert response["ok"] is True, (response, log[-2000:])
	assert {key: response["result"][key] for key in COMPARED_KEYS} == {key: expected[key] for key in COMPARED_KEYS}
	assert '"resident_jsc": true' in log, log[-2000:]
	# 常駐 Deno が実際に起動して解いたこと
	assert resident_jsc.STARTED_LOG in log, log[-2000:]


def test_standard_providers_wait_for_enough_budget(monkeypatch):
	"""持ち時間の残りが STANDARD_PROVIDER_MIN_SECONDS 未満なら標準のプロバイダを使わせない。常駐 Deno は対象外"""
	resident_jsc = import_resident_jsc(monkeypatch, standard_available=True)
	assert resident_jsc.BUDGET_GATE_INSTALLED is True
	standard = sys.modules["yt_dlp.extractor.youtube.jsc._builtin.deno"].DenoJCP()
	resident = resident_jsc.ResidentDenoJCP()
	# 持ち時間の外 (期限なし) では元の結果のまま
	assert standard.is_available() is True
	request_budget.begin(resident_jsc.STANDARD_PROVIDER_MIN_SECONDS * 2)
	try:
		assert standard.is_available() is True
	finally:
		request_budget.end()
	request_budget.begin(resident_jsc.STANDARD_PROVIDER_MIN_SECONDS / 2)
	try:
		assert standard.is_available() is False
		assert resident.is_available() is True
	finally:
		request_budget.end()
	# 二重に包まない
	gate = sys.modules["yt_dlp.extractor.youtube.jsc._builtin.ejs"].EJSBaseJCP.is_available
	assert resident_jsc._install_budget_gate() is True
	assert sys.modules["yt_dlp.extractor.youtube.jsc._builtin.ejs"].EJSBaseJCP.is_available is gate


def test_standard_provider_gate_keeps_unavailable_result(monkeypatch):
	"""持ち時間が十分でも、元の is_available が False なら False のまま"""
	resident_jsc = import_resident_jsc(monkeypatch, standard_available=False)
	standard = sys.modules["yt_dlp.extractor.youtube.jsc._builtin.deno"].DenoJCP()
	request_budget.begin(resident_jsc.STANDARD_PROVIDER_MIN_SECONDS * 2)
	try:
		assert standard.is_available() is False
	finally:
		request_budget.end()


def test_missing_is_available_leaves_standard_behaviour(monkeypatch):
	"""yt-dlp の内部構成が違って包めなくても、resident_jsc の読み込みは失敗しない"""
	resident_jsc = import_resident_jsc(monkeypatch)
	assert resident_jsc.BUDGET_GATE_INSTALLED is False


BROKEN_PLAYER = "https://www.youtube.com/s/player/05a4ffc0/player_ias.vflset/en_US/base.js"
GOOD_PLAYER = "https://www.youtube.com/s/player/6b3d0364/player_ias.vflset/en_US/base.js"


def challenge(player_url: str, kind: str = "n") -> types.SimpleNamespace:
	"""Args: player_url はプレイヤーの URL、kind は種類。Returns: JsChallengeRequest の代用"""
	return types.SimpleNamespace(
		type=kind, video_id="vid", input=types.SimpleNamespace(player_url=player_url, challenges=["abc"]),
	)


def resident_provider(resident_jsc: types.ModuleType, solve: Any) -> Any:
	"""
	_solve を差し替えた常駐 Deno のプロバイダを作る (Deno を起動しない)。

	Args:
		resident_jsc: import_resident_jsc の戻り値
		solve: _solve の代わり。(player_url, requests) を受け取る
	Returns:
		ResidentDenoJCP のインスタンス
	"""
	provider = resident_jsc.ResidentDenoJCP()
	provider._lib_script = types.SimpleNamespace(variant="deno")
	provider._solve = lambda player_url, requests, timeout=None: solve(player_url, requests)
	return provider


def test_broken_players_expire_and_keep_a_limited_number(monkeypatch):
	"""覚えたプレイヤーは期限まで返し、上限を超えたら古いものから忘れる"""
	clock = [100.0]
	resident_jsc = import_resident_jsc(monkeypatch)

	memory = resident_jsc._BrokenPlayers(size=2, ttl=10.0, clock=lambda: clock[0])
	memory.remember("a")
	assert memory.recall("a") is True
	assert memory.recall("b") is False
	clock[0] += 10.0
	assert memory.recall("a") is False
	memory.remember("a")
	memory.remember("b")
	memory.remember("c")
	assert [memory.recall(url) for url in ("a", "b", "c")] == [False, True, True]


def test_solver_error_player_is_not_sent_to_deno_again(monkeypatch, capsys):
	"""解読スクリプトのエラーのプレイヤーを覚え、次からは Deno に送らずにエラーの応答を返す。他のプレイヤーは解く"""
	resident_jsc = import_resident_jsc(monkeypatch)
	sent: list[str] = []

	def solve(player_url: str, requests: list[Any]) -> dict[str, Any]:
		sent.append(player_url)
		if player_url == BROKEN_PLAYER:
			raise resident_jsc._SolverError("a.addOnDisposeCallback is not a function")
		return {"responses": [{"type": "error", "error": "per request"} for _ in requests]}

	provider = resident_provider(resident_jsc, solve)
	with pytest.raises(resident_jsc._SolverError):
		list(provider._real_bulk_solve([challenge(BROKEN_PLAYER)]))
	assert f"broken player remembered: {BROKEN_PLAYER}" in capsys.readouterr().err

	requests = [challenge(BROKEN_PLAYER, "n"), challenge(BROKEN_PLAYER, "sig"), challenge(GOOD_PLAYER)]
	responses = list(provider._real_bulk_solve(requests))
	assert sent == [BROKEN_PLAYER, GOOD_PLAYER]
	assert [response.request for response in responses] == requests
	assert all(isinstance(response.error, resident_jsc.JsChallengeProviderError) for response in responses)
	assert BROKEN_PLAYER in str(responses[0].error)
	# 記憶に当たったことは出さない (要求ごとに出ると多いため)
	assert "broken player" not in capsys.readouterr().err


@pytest.mark.parametrize("error_name", ["JsChallengeProviderError", "_BudgetExhaustedError"])
def test_other_failures_do_not_mark_the_player(monkeypatch, error_name):
	"""通信の失敗や持ち時間切れでは覚えず、次の要求でも Deno で解く"""
	resident_jsc = import_resident_jsc(monkeypatch)
	sent: list[str] = []

	def solve(player_url: str, requests: list[Any]) -> dict[str, Any]:
		sent.append(player_url)
		raise getattr(resident_jsc, error_name)("failed")

	provider = resident_provider(resident_jsc, solve)
	for _ in range(2):
		with pytest.raises(resident_jsc.JsChallengeProviderError):
			list(provider._real_bulk_solve([challenge(BROKEN_PLAYER)]))
	assert sent == [BROKEN_PLAYER, BROKEN_PLAYER]


def test_standard_providers_skip_remembered_players(monkeypatch):
	"""標準のプロバイダは、覚えたプレイヤーの要求をランタイムに渡さない。全部が覚えたものなら元の関数を呼ばない"""
	passed: list[list[Any]] = []

	def standard_solve(self: Any, requests: list[Any]) -> Any:
		passed.append(list(requests))
		yield from ()

	resident_jsc = import_resident_jsc(monkeypatch, standard_solve=standard_solve)
	assert resident_jsc.BROKEN_PLAYER_GATE_INSTALLED is True
	standard = sys.modules["yt_dlp.extractor.youtube.jsc._builtin.deno"].DenoJCP()
	resident_jsc._broken_players.remember(BROKEN_PLAYER)

	broken, good = challenge(BROKEN_PLAYER), challenge(GOOD_PLAYER)
	responses = list(standard._real_bulk_solve([broken, good]))
	assert passed == [[good]]
	assert [response.request for response in responses] == [broken]
	assert isinstance(responses[0].error, resident_jsc.JsChallengeProviderError)

	passed.clear()
	assert len(list(standard._real_bulk_solve([broken]))) == 1
	assert passed == []
	# 二重に包まない
	gate = sys.modules["yt_dlp.extractor.youtube.jsc._builtin.ejs"].EJSBaseJCP._real_bulk_solve
	assert resident_jsc._install_broken_player_gate() is True
	assert sys.modules["yt_dlp.extractor.youtube.jsc._builtin.ejs"].EJSBaseJCP._real_bulk_solve is gate


def test_missing_standard_solve_leaves_standard_behaviour(monkeypatch):
	"""yt-dlp の内部構成が違って標準のプロバイダを包めなくても、resident_jsc の読み込みは失敗しない"""
	resident_jsc = import_resident_jsc(monkeypatch)
	assert resident_jsc.BROKEN_PLAYER_GATE_INSTALLED is False


def test_player_cache_helpers_are_shared_without_a_cycle(monkeypatch):
	"""プレイヤーキャッシュの補助は player_cache にあり、resident_jsc は speedups を読み込まずに使う"""
	import player_cache

	assert speedups.player_cache_files is player_cache.player_cache_files
	assert speedups.PLAYER_CACHE_KEY_PREFIX == player_cache.PLAYER_CACHE_KEY_PREFIX
	resident_jsc = import_resident_jsc(monkeypatch)
	assert resident_jsc.player_cache_files is player_cache.player_cache_files
	assert not hasattr(resident_jsc, "speedups")
