"""
予備設定 (fallback.py、runner.py と worker.py の予備設定、Cookie を外した取り直し) の試験。
- yt-dlp の内部 API は sys.modules に置いた偽のモジュールで差し替え、同じプロセスの中で試す
- 実行: uv run --directory worker pytest tests/test_fallback.py
"""
import sys
import threading
import time
import types
from typing import Any

import pytest

import request_budget
import speedups
from conftest import FakeCookieLoadError, FakeDownloadError, install_fake_yt_dlp, wait_for
from runner import BoundedRunner, FallbackTimeoutError


def test_fallback_options_are_used_after_failure(monkeypatch, deno_on_path, capsys):
	"""解決に失敗したら予備設定で 1 回だけ取り直す"""
	fake = install_fake_yt_dlp(monkeypatch)
	from worker import DEFAULT_FORMAT, Worker, build_options

	config = speedups.SpeedupConfig(cookies="firefox", premium="auto")
	options = build_options("cache", "firefox", None)
	worker = Worker(options, speedups.stream_options(config, speedups.apply(config)))
	assert worker.fallback_ready is True

	result = worker.resolve("https://www.youtube.com/watch?v=x", None)
	assert result["id"] == "vid"
	first, second = fake.calls
	assert first["extractor_args"]["youtube"]["player_client"] == ["web_music"]
	# 予備設定は extractor_args を外し、Chrome の TLS 偽装を付ける。形式と Cookie は引き継ぐ
	assert "extractor_args" not in second
	assert second["impersonate"].client == "chrome"
	assert second["format"] == DEFAULT_FORMAT
	assert second["cookiesfrombrowser"] == ("firefox",)
	assert "fallback" in capsys.readouterr().err

	# 形式を固定した解決でも、同じ形式で予備設定を使う
	fake.calls.clear()
	worker.resolve("https://www.youtube.com/watch?v=x", "140")
	assert [call["format"] for call in fake.calls] == ["140", "140"]
	assert "impersonate" in fake.calls[1]


@pytest.mark.parametrize(
	"first,fallback,code",
	[
		# 通常の設定の理由で分類できなければ (internal)、予備設定の理由で分類する
		("HTTP Error 403: Forbidden", "Private video", "private"),
		("HTTP Error 403: Forbidden", "Sign in to confirm you're not a bot", "rate_limited"),
	],
)
def test_fallback_failure_reports_both_errors(monkeypatch, deno_on_path, first, fallback, code):
	"""予備設定でも失敗したら両方の理由を返し、通常の設定の理由を優先して分類する"""
	fake = install_fake_yt_dlp(monkeypatch)
	from worker import FallbackFailedError, Worker, build_options, error_response

	def always_fail(self: Any, url: str, download: bool = False) -> dict[str, Any]:
		"""Raises: 設定ごとに異なる FakeDownloadError"""
		raise FakeDownloadError(fallback if "impersonate" in self.params else first)

	monkeypatch.setattr(fake.YoutubeDL, "extract_info", always_fail)
	worker = Worker(build_options("cache", None, None))
	with pytest.raises(FallbackFailedError) as caught:
		worker.resolve("https://www.youtube.com/watch?v=x", None)
	response = error_response(1, caught.value)
	assert response["error"]["message"].startswith(first)
	assert fallback in response["error"]["message"]
	assert response["error"]["code"] == code


@pytest.mark.parametrize(
	"message",
	[
		"Private video",
		"Video unavailable",
		"Sign in to confirm your age",
		"This video is not available in your country",
		"The uploader has not made this video available in your country",
		"HTTP Error 429: Too Many Requests",
	],
)
def test_fallback_is_not_used_for_definite_failures(monkeypatch, deno_on_path, message):
	"""確定した失敗と単なる回数制限 (429) では予備設定で取り直さず、要求を増やさない"""
	fake = install_fake_yt_dlp(monkeypatch)
	from worker import Worker, build_options, error_response

	def fail(self: Any, url: str, download: bool = False) -> dict[str, Any]:
		"""Raises: 設定によらず同じ FakeDownloadError"""
		fake.calls.append(self.params)
		raise FakeDownloadError(message)

	monkeypatch.setattr(fake.YoutubeDL, "extract_info", fail)
	worker = Worker(build_options("cache", None, None))
	assert worker.fallback_ready is True
	with pytest.raises(FakeDownloadError) as caught:
		worker.resolve("https://www.youtube.com/watch?v=x", None)
	assert len(fake.calls) == 1
	assert "impersonate" not in fake.calls[0]
	assert error_response(1, caught.value)["error"]["code"] != "internal"


def test_fallback_is_used_for_bot_check(monkeypatch, deno_on_path):
	"""ボット判定 (rate_limited のうち "not a bot") では、TLS 偽装の予備設定で取り直す"""
	fake = install_fake_yt_dlp(monkeypatch)
	from worker import Worker, build_options

	def bot_check(self: Any, url: str, download: bool = False) -> dict[str, Any]:
		"""Returns: 予備設定なら固定の動画情報。Raises: 通常の設定はボット判定の FakeDownloadError"""
		fake.calls.append(self.params)
		if "impersonate" not in self.params:
			raise FakeDownloadError("Sign in to confirm you're not a bot. Use --cookies-from-browser")
		return {"id": "vid", "title": "title", "url": "https://example.invalid/a?expire=1", "format_id": "251"}

	monkeypatch.setattr(fake.YoutubeDL, "extract_info", bot_check)
	worker = Worker(build_options("cache", None, None))
	assert worker.resolve("https://www.youtube.com/watch?v=x", None)["id"] == "vid"
	assert len(fake.calls) == 2
	assert "impersonate" in fake.calls[1]


def install_first_fails(monkeypatch: pytest.MonkeyPatch, message: str) -> types.SimpleNamespace:
	"""
	通常の設定は message で失敗し、予備設定 (impersonate あり) は成功する偽の yt_dlp を置く。

	Args:
		monkeypatch: pytest の monkeypatch
		message: 通常の設定が送出する FakeDownloadError の文言
	Returns:
		install_fake_yt_dlp の戻り値
	"""
	fake = install_fake_yt_dlp(monkeypatch)

	def extract(self: Any, url: str, download: bool = False) -> dict[str, Any]:
		"""Returns: 予備設定なら固定の動画情報。Raises: 通常の設定は message の FakeDownloadError"""
		fake.calls.append(self.params)
		if "impersonate" not in self.params:
			raise FakeDownloadError(message)
		return {"id": "vid", "title": "title", "url": "https://example.invalid/a?expire=1", "format_id": "251"}

	monkeypatch.setattr(fake.YoutubeDL, "extract_info", extract)
	return fake


def test_not_found_with_cookies_uses_fallback(monkeypatch, deno_on_path):
	"""Cookie ありで not_found (web_music で Video unavailable) なら、予備設定で取り直す"""
	fake = install_first_fails(monkeypatch, "Video unavailable. This video is not available")
	from worker import Worker, build_options

	worker = Worker(build_options("cache", "firefox", None))
	result = worker.resolve("https://www.youtube.com/watch?v=x", None)
	assert result["id"] == "vid"
	# 予備設定で解決したことを応答で示す (更新の検査が主経路の失敗に気付けるようにするため)
	assert result["fallback"] is True
	assert len(fake.calls) == 2
	assert "impersonate" in fake.calls[1]


def test_not_found_without_cookies_skips_fallback(monkeypatch, deno_on_path):
	"""Cookie なしの not_found は確定した失敗として、予備設定で取り直さない"""
	fake = install_first_fails(monkeypatch, "Video unavailable. This video is not available")
	from worker import Worker, build_options

	worker = Worker(build_options("cache", None, None))
	with pytest.raises(FakeDownloadError):
		worker.resolve("https://www.youtube.com/watch?v=x", None)
	assert len(fake.calls) == 1


def test_private_with_cookies_skips_fallback(monkeypatch, deno_on_path):
	"""Cookie ありでも not_found 以外の確定した失敗 (private) は取り直さない"""
	fake = install_first_fails(monkeypatch, "Private video")
	from worker import Worker, build_options

	worker = Worker(build_options("cache", "firefox", None))
	with pytest.raises(FakeDownloadError):
		worker.resolve("https://www.youtube.com/watch?v=x", None)
	assert len(fake.calls) == 1


VIDEO_INFO = {"id": "vid", "title": "title", "url": "https://example.invalid/a?expire=1", "format_id": "251"}
JS_CHALLENGE_WARNING = "[youtube] x: n challenge solving failed: Some formats may be missing."
COOKIES_INVALID_WARNING = "[youtube] The provided YouTube account cookies are no longer valid."
FORMAT_UNAVAILABLE = "ERROR: [youtube] x: Requested format is not available. Use --list-formats"


def install_cookie_failure(
	monkeypatch: pytest.MonkeyPatch, warning: str | None, error: Exception, cookieless_error: Exception | None = None,
) -> types.SimpleNamespace:
	"""
	Cookie ありの取得は warning を出して error を送出し、Cookie なしの取得は成功する偽の yt_dlp を置く。

	Args:
		monkeypatch: pytest の monkeypatch
		warning: Cookie ありの取得で logger へ出す警告。None なら出さない
		error: Cookie ありの取得が送出する例外
		cookieless_error: Cookie なしの取得が送出する例外。None なら成功する
	Returns:
		install_fake_yt_dlp の戻り値 (calls に取得ごとの params が入る)
	"""
	fake = install_fake_yt_dlp(monkeypatch)

	def extract(self: Any, url: str, download: bool = False) -> dict[str, Any]:
		"""Returns: Cookie なしなら固定の動画情報。Raises: Cookie ありなら error"""
		fake.calls.append(self.params)
		if "cookiesfrombrowser" in self.params:
			if warning is not None:
				self.params["logger"].warning(warning)
			raise error
		if cookieless_error is not None:
			raise cookieless_error
		return dict(VIDEO_INFO)

	monkeypatch.setattr(fake.YoutubeDL, "extract_info", extract)
	return fake


@pytest.mark.parametrize(
	"warning,error",
	[
		# プレイヤーを解読できず、音声の形式が残らなかった
		(JS_CHALLENGE_WARNING, FakeDownloadError(FORMAT_UNAVAILABLE)),
		# Cookie が失効していた
		(COOKIES_INVALID_WARNING, FakeDownloadError("Sign in to confirm your age")),
		# Firefox の Cookie を読み込めなかった
		(None, FakeCookieLoadError("failed to load cookies")),
	],
)
def test_cookie_failure_retries_without_cookies(monkeypatch, deno_on_path, capsys, warning, error):
	"""Cookie 関連の失敗では、Cookie を外した予備設定で 1 回だけ取り直す"""
	fake = install_cookie_failure(monkeypatch, warning, error)
	from worker import Worker, build_options

	worker = Worker(build_options("cache", "firefox", None))
	result = worker.resolve("https://www.youtube.com/watch?v=x", None)
	assert result["id"] == "vid"
	assert result["fallback"] is True
	first, second = fake.calls
	assert first["cookiesfrombrowser"] == ("firefox",)
	assert "cookiesfrombrowser" not in second
	assert "impersonate" in second
	err = capsys.readouterr().err
	assert "cookieless: https://www.youtube.com/watch?v=x" in err
	assert "cookieless succeeded: https://www.youtube.com/watch?v=x" in err


def test_cookie_failure_is_retried_with_the_same_format(monkeypatch, deno_on_path):
	"""形式を固定した解決でも、同じ形式で Cookie なしの取り直しをする"""
	fake = install_cookie_failure(monkeypatch, JS_CHALLENGE_WARNING, FakeDownloadError(FORMAT_UNAVAILABLE))
	from worker import Worker, build_options

	worker = Worker(build_options("cache", "firefox", None))
	worker.resolve("https://www.youtube.com/watch?v=x", "140")
	assert [call["format"] for call in fake.calls] == ["140", "140"]
	assert "cookiesfrombrowser" not in fake.calls[1]


def test_js_challenge_failure_without_cookies_is_not_cookie_related(monkeypatch, deno_on_path, capsys):
	"""Cookie を使っていなければ Cookie 関連とせず、今までの予備設定で取り直す"""
	fake = install_fake_yt_dlp(monkeypatch)

	def extract(self: Any, url: str, download: bool = False) -> dict[str, Any]:
		"""Returns: 予備設定なら固定の動画情報。Raises: 通常の設定は形式の不足"""
		fake.calls.append(self.params)
		self.params["logger"].warning(JS_CHALLENGE_WARNING)
		if "impersonate" not in self.params:
			raise FakeDownloadError(FORMAT_UNAVAILABLE)
		return dict(VIDEO_INFO)

	monkeypatch.setattr(fake.YoutubeDL, "extract_info", extract)
	from worker import Worker, build_options

	worker = Worker(build_options("cache", None, None))
	assert worker.resolve("https://www.youtube.com/watch?v=x", None)["fallback"] is True
	assert len(fake.calls) == 2
	assert "cookieless" not in capsys.readouterr().err


def test_non_cookie_failure_with_cookies_keeps_cookies_in_fallback(monkeypatch, deno_on_path, capsys):
	"""Cookie を使っていても、Cookie 関連でない失敗は Cookie ありの予備設定で取り直す"""
	fake = install_first_fails(monkeypatch, "HTTP Error 403: Forbidden")
	from worker import Worker, build_options

	worker = Worker(build_options("cache", "firefox", None))
	worker.resolve("https://www.youtube.com/watch?v=x", None)
	assert fake.calls[1]["cookiesfrombrowser"] == ("firefox",)
	assert "cookieless" not in capsys.readouterr().err


def test_cookie_failure_with_rate_limit_is_not_retried(monkeypatch, deno_on_path):
	"""Cookie の失効の警告があっても、単なる回数制限では取り直さない"""
	fake = install_cookie_failure(
		monkeypatch, COOKIES_INVALID_WARNING, FakeDownloadError("HTTP Error 429: Too Many Requests"),
	)
	from worker import Worker, build_options

	worker = Worker(build_options("cache", "firefox", None))
	with pytest.raises(FakeDownloadError):
		worker.resolve("https://www.youtube.com/watch?v=x", None)
	assert len(fake.calls) == 1


def test_warning_without_failure_does_nothing(monkeypatch, deno_on_path):
	"""解読の警告が出ても取得が成功したら、取り直さない"""
	fake = install_fake_yt_dlp(monkeypatch)

	def extract(self: Any, url: str, download: bool = False) -> dict[str, Any]:
		"""Returns: 警告を出して固定の動画情報"""
		fake.calls.append(self.params)
		self.params["logger"].warning(JS_CHALLENGE_WARNING)
		return dict(VIDEO_INFO)

	monkeypatch.setattr(fake.YoutubeDL, "extract_info", extract)
	from worker import Worker, build_options

	worker = Worker(build_options("cache", "firefox", None))
	assert worker.resolve("https://www.youtube.com/watch?v=x", None)["fallback"] is False
	assert len(fake.calls) == 1


def test_cookieless_failure_reports_both_errors(monkeypatch, deno_on_path):
	"""Cookie なしでも失敗したら、両方の理由を返す"""
	install_cookie_failure(
		monkeypatch, JS_CHALLENGE_WARNING, FakeDownloadError(FORMAT_UNAVAILABLE),
		FakeDownloadError("Sign in to confirm you're not a bot"),
	)
	from worker import FallbackFailedError, Worker, build_options

	worker = Worker(build_options("cache", "firefox", None))
	with pytest.raises(FallbackFailedError) as caught:
		worker.resolve("https://www.youtube.com/watch?v=x", None)
	message = str(caught.value)
	assert message.startswith(FORMAT_UNAVAILABLE)
	assert "not a bot" in message


def test_cookieless_is_not_remembered(monkeypatch, deno_on_path):
	"""Cookie なしで解決できた動画も覚えず、次も Cookie ありの主経路から試す"""
	fake = install_cookie_failure(monkeypatch, JS_CHALLENGE_WARNING, FakeDownloadError(FORMAT_UNAVAILABLE))
	from worker import Worker, build_options

	worker = Worker(build_options("cache", "firefox", None))
	url = "https://www.youtube.com/watch?v=jNQXAC9IVRw"
	worker.resolve(url, None)
	fake.calls.clear()
	worker.resolve(url, None)
	assert fake.calls[0]["cookiesfrombrowser"] == ("firefox",)
	assert len(fake.calls) == 2


def test_cookieless_is_skipped_when_little_budget_remains(monkeypatch, deno_on_path, capsys):
	"""持ち時間の残りが足りなければ、Cookie なしでも取り直さない"""
	fake = install_cookie_failure(monkeypatch, JS_CHALLENGE_WARNING, FakeDownloadError(FORMAT_UNAVAILABLE))
	from worker import FALLBACK_MIN_SECONDS, Worker, build_options

	worker = Worker(build_options("cache", "firefox", None))
	request_budget.begin(FALLBACK_MIN_SECONDS - 1.0)
	try:
		with pytest.raises(FakeDownloadError):
			worker.resolve("https://www.youtube.com/watch?v=x", None)
	finally:
		request_budget.end()
	assert len(fake.calls) == 1
	assert "cookieless skipped" in capsys.readouterr().err


def test_worker_starts_without_cookie_module(monkeypatch, deno_on_path):
	"""yt_dlp.cookies が無い yt-dlp でも Worker を作れる (Cookie の読み込みの失敗は判定しないだけ)"""
	install_fake_yt_dlp(monkeypatch, internals=False)
	from worker import Worker, build_options

	worker = Worker(build_options("cache", "firefox", None))
	assert worker.fallback_ready is False


def resolve_with_info(monkeypatch: pytest.MonkeyPatch, extra: dict[str, Any]) -> dict[str, Any]:
	"""
	extract_info が基本の情報に extra を足して返す偽の yt_dlp で resolve する。

	Args:
		monkeypatch: pytest の monkeypatch
		extra: 基本の動画情報へ足す項目
	Returns:
		Worker.resolve の結果
	"""
	fake = install_fake_yt_dlp(monkeypatch)
	base = {"id": "vid", "title": "title", "url": "https://example.invalid/a?expire=1", "format_id": "251"}
	monkeypatch.setattr(fake.YoutubeDL, "extract_info", lambda self, url, download=False: {**base, **extra})
	from worker import Worker, build_options

	return Worker(build_options("cache", None, None)).resolve("https://www.youtube.com/watch?v=x", None)


def test_resolve_reports_no_fallback_when_first_attempt_succeeds(monkeypatch, deno_on_path):
	"""最初の取得で解決できたら fallback は False"""
	assert resolve_with_info(monkeypatch, {})["fallback"] is False


def test_resolve_returns_available_at(monkeypatch, deno_on_path):
	"""形式の available_at を UNIX 秒の整数で返す"""
	assert resolve_with_info(monkeypatch, {"available_at": 1793000123})["available_at"] == 1793000123


def test_resolve_available_at_prefers_audio_of_requested_formats(monkeypatch, deno_on_path):
	"""requested_formats があれば、その音声の形式の available_at を使う"""
	extra = {
		"available_at": 1,
		"requested_formats": [
			{"vcodec": "avc1", "acodec": "none", "available_at": 2},
			{"vcodec": "none", "acodec": "opus", "available_at": 1793000456.0},
		],
	}
	assert resolve_with_info(monkeypatch, extra)["available_at"] == 1793000456


def test_resolve_available_at_none_when_missing(monkeypatch, deno_on_path):
	"""available_at が無ければ None を返す"""
	assert resolve_with_info(monkeypatch, {})["available_at"] is None


def test_fallback_is_skipped_when_little_budget_remains(monkeypatch, deno_on_path, capsys):
	"""持ち時間の残りが FALLBACK_MIN_SECONDS 未満なら取り直さず、通常の設定の失敗を返す"""
	fake = install_fake_yt_dlp(monkeypatch)
	import worker as worker_module

	worker = worker_module.Worker(worker_module.build_options("cache", None, None))
	request_budget.begin(worker_module.FALLBACK_MIN_SECONDS / 2)
	try:
		with pytest.raises(FakeDownloadError):
			worker.resolve("https://www.youtube.com/watch?v=x", None)
	finally:
		request_budget.end()
	assert len(fake.calls) == 1
	assert "fallback skipped" in capsys.readouterr().err


def test_fallback_is_bounded_by_the_remaining_budget(monkeypatch, deno_on_path):
	"""予備設定の取得は持ち時間の残りで打ち切り、次の要求は新しい実行スレッドで取り直す"""
	fake = install_fake_yt_dlp(monkeypatch)
	import worker as worker_module

	budget = 1.0
	stall = 10.0
	# 試験を短くするため、取り直しに要る残りの秒数を持ち時間より短くする
	monkeypatch.setattr(worker_module, "FALLBACK_MIN_SECONDS", budget / 4)
	stalls = [stall]

	def slow_fallback(self: Any, url: str, download: bool = False) -> dict[str, Any]:
		"""Returns: 予備設定なら (1 回目は止まった後に) 固定の動画情報。Raises: 通常の設定は FakeDownloadError"""
		fake.calls.append(self.params)
		if "impersonate" not in self.params:
			raise FakeDownloadError("HTTP Error 403: Forbidden")
		if stalls:
			time.sleep(stalls.pop())
		return {"id": "vid", "title": "title", "url": "https://example.invalid/a?expire=1", "format_id": "251"}

	monkeypatch.setattr(fake.YoutubeDL, "extract_info", slow_fallback)
	worker = worker_module.Worker(worker_module.build_options("cache", None, None))
	request_budget.begin(budget)
	started = time.monotonic()
	try:
		with pytest.raises(worker_module.FallbackFailedError) as caught:
			worker.resolve("https://www.youtube.com/watch?v=x", None)
	finally:
		request_budget.end()
	elapsed = time.monotonic() - started
	assert elapsed < stall / 2, elapsed
	assert isinstance(caught.value.fallback, FallbackTimeoutError)
	assert worker_module.error_response(1, caught.value)["error"]["code"] == "internal"

	# 止まったスレッドを待たずに、次の要求の予備設定は終わる
	request_budget.begin(budget)
	try:
		assert worker.resolve("https://www.youtube.com/watch?v=x", None)["id"] == "vid"
	finally:
		request_budget.end()
	assert time.monotonic() - started < stall / 2


def test_runner_thread_keeps_the_deadline_of_its_job():
	"""打ち切った予備設定のスレッドは自分の処理の期限を持ち続け、次の要求の期限や期限なしを読まない"""
	runner = BoundedRunner("test-runner")
	release = threading.Event()
	seen: list[float | None] = []

	def action(state: dict[str | None, Any]) -> None:
		"""止められた後に、このスレッドから見た残りの秒数を記録する"""
		release.wait(10)
		seen.append(request_budget.remaining())

	request_budget.begin(0.2)
	try:
		with pytest.raises(FallbackTimeoutError):
			runner.run(action, request_budget.remaining())
	finally:
		request_budget.end()
	# 次の要求が始まってから、打ち切ったスレッドが期限を読む。
	# Event.wait は上限の少し前に戻ることがあるため、元の期限を確実に過ぎてから読ませる
	request_budget.begin(100.0)
	try:
		time.sleep(0.3)
		release.set()
		wait_for(lambda: bool(seen), 5.0)
	finally:
		request_budget.end()
	assert seen[0] is not None and seen[0] <= 0, seen


@pytest.mark.parametrize(
	"url,expected",
	[
		("https://www.youtube.com/watch?v=jNQXAC9IVRw", "jNQXAC9IVRw"),
		("https://music.youtube.com/watch?list=x&v=jNQXAC9IVRw&t=3", "jNQXAC9IVRw"),
		("https://youtu.be/jNQXAC9IVRw?t=1", "jNQXAC9IVRw"),
		("https://www.youtube.com/shorts/jNQXAC9IVRw", "jNQXAC9IVRw"),
		("https://www.youtube.com/live/jNQXAC9IVRw?feature=share", "jNQXAC9IVRw"),
		("https://www.youtube.com/embed/jNQXAC9IVRw", "jNQXAC9IVRw"),
		("https://www.youtube.com/watch?v=x", None),
		("https://www.youtube.com/watch", None),
		("https://www.youtube.com/playlist?list=PLx", None),
		("https://youtu.be/", None),
	],
)
def test_video_id_from_url(url, expected):
	"""動画 ID の形に合うものだけを URL から取り出す"""
	from worker import video_id_from_url

	assert video_id_from_url(url) == expected


def test_fallback_memory_forgets_the_least_recently_used():
	"""上限を超えたら最も長く使っていない ID から忘れる。recall は使った扱いにする"""
	from worker import FallbackMemory

	memory = FallbackMemory(2)
	memory.remember("a" * 11)
	memory.remember("b" * 11)
	assert memory.recall("a" * 11)
	memory.remember("c" * 11)
	assert len(memory) == 2
	assert memory.recall("a" * 11) and memory.recall("c" * 11)
	assert not memory.recall("b" * 11)
	assert not memory.recall(None)
	memory.forget("a" * 11)
	assert not memory.recall("a" * 11)


def test_fallback_memory_expires_entries():
	"""覚えてから ttl_seconds 秒たった ID は無いものとして扱い、取り除く。recall では期限は延びず、remember で延びる"""
	from fallback import FallbackMemory

	ttl = 100.0
	clock = [1000.0]
	memory = FallbackMemory(4, ttl_seconds=ttl, clock=lambda: clock[0])
	memory.remember("a" * 11)
	memory.remember("b" * 11)
	clock[0] += ttl - 1
	assert memory.recall("a" * 11)
	clock[0] += 1
	# recall で期限は延びない
	assert not memory.recall("a" * 11)
	assert len(memory) == 0
	# 覚え直すと期限も数え直す
	memory.remember("a" * 11)
	clock[0] += ttl - 1
	memory.remember("a" * 11)
	clock[0] += ttl - 1
	assert memory.recall("a" * 11)
	assert len(memory) == 1


def test_fallback_memory_drops_expired_before_evicting_live_entries():
	"""上限を超えるときは、期限切れを先に取り除き、期限内の ID を押し出さない"""
	from fallback import FallbackMemory

	ttl = 100.0
	clock = [1000.0]
	memory = FallbackMemory(2, ttl_seconds=ttl, clock=lambda: clock[0])
	memory.remember("a" * 11)
	clock[0] += ttl / 2
	memory.remember("b" * 11)
	clock[0] += ttl / 2 - 1
	# a を最近使ったものにする (LRU の順は b、a)
	assert memory.recall("a" * 11)
	clock[0] += 1
	# a だけが期限切れ。c を覚えると、最も長く使っていない b ではなく期限切れの a が消える
	memory.remember("c" * 11)
	assert len(memory) == 2
	assert memory.recall("b" * 11) and memory.recall("c" * 11)
	assert not memory.recall("a" * 11)


def test_fallback_memory_default_ttl_is_the_module_constant():
	"""期限の既定値はモジュールの定数"""
	import fallback

	clock = [0.0]
	memory = fallback.FallbackMemory(1, clock=lambda: clock[0])
	memory.remember("a" * 11)
	clock[0] += fallback.FALLBACK_MEMORY_TTL_SECONDS - 1
	assert memory.recall("a" * 11)
	clock[0] += 1
	assert not memory.recall("a" * 11)


# Cookie ありの主経路で not_found になる動画の URL
NOT_FOUND_VIDEO_URL = "https://www.youtube.com/watch?v=jNQXAC9IVRw"
NOT_FOUND_MESSAGE = "Video unavailable. This video is not available"


def test_not_found_video_goes_straight_to_fallback_next_time(monkeypatch, deno_on_path, capsys):
	"""Cookie ありの not_found から予備設定で解決できた動画は、次から主経路を飛ばして予備設定へ直接回す"""
	fake = install_first_fails(monkeypatch, NOT_FOUND_MESSAGE)
	from worker import Worker, build_options

	worker = Worker(build_options("cache", "firefox", None))
	assert worker.resolve(NOT_FOUND_VIDEO_URL, None)["fallback"] is True
	assert len(fake.calls) == 2
	fake.calls.clear()
	# 形式を固定した解決 (refresh) も、別の形の URL も、同じ動画なら直接回す
	for url, format_id in ((NOT_FOUND_VIDEO_URL, None), ("https://youtu.be/jNQXAC9IVRw", "251")):
		result = worker.resolve(url, format_id)
		assert result["fallback"] is True
	assert len(fake.calls) == 2
	assert all("impersonate" in call for call in fake.calls)
	assert [call["format"] for call in fake.calls][1] == "251"
	assert "fallback direct: jNQXAC9IVRw" in capsys.readouterr().err

	# 覚えていない動画は主経路から試す
	fake.calls.clear()
	worker.resolve("https://www.youtube.com/watch?v=dQw4w9WgXcQ", None)
	assert len(fake.calls) == 2
	assert "impersonate" not in fake.calls[0]


def test_bot_check_success_is_not_remembered(monkeypatch, deno_on_path):
	"""ボット判定から予備設定で解決できても覚えない (一時的なことが多く、速い主経路を使い続けるため)"""
	fake = install_first_fails(monkeypatch, "Sign in to confirm you're not a bot")
	from worker import Worker, build_options

	worker = Worker(build_options("cache", "firefox", None))
	worker.resolve(NOT_FOUND_VIDEO_URL, None)
	fake.calls.clear()
	worker.resolve(NOT_FOUND_VIDEO_URL, None)
	assert len(fake.calls) == 2
	assert "impersonate" not in fake.calls[0]


def test_failed_direct_fallback_forgets_the_video(monkeypatch, deno_on_path):
	"""直接回した予備設定が失敗したら、その失敗を返して動画を忘れ、次は主経路から試す"""
	fake = install_first_fails(monkeypatch, NOT_FOUND_MESSAGE)
	from worker import Worker, build_options, error_response

	worker = Worker(build_options("cache", "firefox", None))
	worker.resolve(NOT_FOUND_VIDEO_URL, None)

	def fail_everywhere(self: Any, url: str, download: bool = False) -> dict[str, Any]:
		"""Raises: 設定によらず Private video の FakeDownloadError"""
		fake.calls.append(self.params)
		raise FakeDownloadError("Private video")

	monkeypatch.setattr(fake.YoutubeDL, "extract_info", fail_everywhere)
	fake.calls.clear()
	with pytest.raises(FakeDownloadError) as caught:
		worker.resolve(NOT_FOUND_VIDEO_URL, None)
	assert error_response(1, caught.value)["error"]["code"] == "private"
	assert len(fake.calls) == 1 and "impersonate" in fake.calls[0]
	fake.calls.clear()
	with pytest.raises(FakeDownloadError):
		worker.resolve(NOT_FOUND_VIDEO_URL, None)
	assert "impersonate" not in fake.calls[0]


def test_remembered_video_expires_and_retries_main_path(monkeypatch, deno_on_path):
	"""覚えた動画も期限を過ぎたら直接回さず、主経路から試し直す (期限は worker の _now で数える)"""
	fake = install_first_fails(monkeypatch, NOT_FOUND_MESSAGE)
	import fallback
	import worker as worker_module

	clock = [1000.0]
	monkeypatch.setattr(worker_module, "_now", lambda: clock[0])
	worker = worker_module.Worker(worker_module.build_options("cache", "firefox", None))
	worker.resolve(NOT_FOUND_VIDEO_URL, None)
	fake.calls.clear()
	clock[0] += fallback.FALLBACK_MEMORY_TTL_SECONDS - 1
	worker.resolve(NOT_FOUND_VIDEO_URL, None)
	assert len(fake.calls) == 1 and "impersonate" in fake.calls[0]
	fake.calls.clear()
	clock[0] += 1
	worker.resolve(NOT_FOUND_VIDEO_URL, None)
	assert len(fake.calls) == 2
	assert "impersonate" not in fake.calls[0]


@pytest.mark.parametrize(
	"cookies,first,fallback,code",
	[
		# Cookie ありの not_found は当てにならないため、予備設定の具体的な理由を使う
		("firefox", NOT_FOUND_MESSAGE, "Private video", "private"),
		("firefox", NOT_FOUND_MESSAGE, "Sign in to confirm your age", "age_restricted"),
		("firefox", NOT_FOUND_MESSAGE, "Sign in to confirm you're not a bot", "rate_limited"),
		("firefox", NOT_FOUND_MESSAGE, "The uploader has not made this video available in your country", "geo_blocked"),
		# 予備設定の理由が具体的でなければ not_found のまま
		("firefox", NOT_FOUND_MESSAGE, "HTTP Error 403: Forbidden", "not_found"),
		("firefox", NOT_FOUND_MESSAGE, "Video unavailable", "not_found"),
		# not_found 以外の主経路の理由 (ボット判定) は今までどおり優先する
		("firefox", "Sign in to confirm you're not a bot", "Private video", "rate_limited"),
		(None, "Sign in to confirm you're not a bot", "Private video", "rate_limited"),
	],
)
def test_unreliable_not_found_prefers_specific_fallback_reason(monkeypatch, deno_on_path, cookies, first, fallback, code):
	"""両方失敗したとき、主経路が Cookie ありの not_found なら予備設定の具体的な理由で分類する"""
	fake = install_fake_yt_dlp(monkeypatch)
	from worker import FallbackFailedError, Worker, build_options, error_response

	def always_fail(self: Any, url: str, download: bool = False) -> dict[str, Any]:
		"""Raises: 設定ごとに異なる FakeDownloadError"""
		fake.calls.append(self.params)
		raise FakeDownloadError(fallback if "impersonate" in self.params else first)

	monkeypatch.setattr(fake.YoutubeDL, "extract_info", always_fail)
	worker = Worker(build_options("cache", cookies, None))
	with pytest.raises(FallbackFailedError) as caught:
		worker.resolve(NOT_FOUND_VIDEO_URL, None)
	assert len(fake.calls) == 2
	response = error_response(1, caught.value)
	assert response["error"]["code"] == code
	# 文言は主経路の理由を先頭に置いたまま
	assert response["error"]["message"].startswith(first)


def test_prepare_fallback_builds_the_instance_in_the_runner_thread(monkeypatch, deno_on_path, capsys):
	"""起動時の予備設定の準備は、実行スレッドの中でインスタンスを作って Cookie を読み、メインスレッドは待たない"""
	fake = install_fake_yt_dlp(monkeypatch)
	import worker as worker_module

	loaded: list[str] = []
	release = threading.Event()

	def cookiejar(self: Any) -> object:
		"""Cookie の読み込みの代わり。読んだスレッドの名前を記録し、release まで止まる"""
		loaded.append(threading.current_thread().name)
		release.wait(10)
		return object()

	monkeypatch.setattr(fake.YoutubeDL, "cookiejar", property(cookiejar), raising=False)
	worker = worker_module.Worker(worker_module.build_options("cache", "firefox", None))
	started = time.monotonic()
	assert worker.prepare_fallback() is True
	# 読み込みが止まっていても待たずに戻る
	assert time.monotonic() - started < 1.0
	wait_for(lambda: bool(loaded), 5.0)
	release.set()
	assert loaded == [worker_module.FALLBACK_THREAD_NAME]
	wait_for(lambda: "fallback_prepare: ok" in capsys.readouterr().err, 5.0)
	fake.calls.clear()
	# 最初の予備設定の要求は、準備したインスタンス (TLS 偽装付き) を使う
	assert worker.resolve("https://www.youtube.com/watch?v=x", None)["fallback"] is True


def test_prepare_fallback_is_skipped_without_fallback(monkeypatch, deno_on_path):
	"""予備設定を使えなければ準備しない"""
	install_fake_yt_dlp(monkeypatch)
	monkeypatch.delitem(sys.modules, "yt_dlp.networking.impersonate")
	import worker as worker_module

	worker = worker_module.Worker(worker_module.build_options("cache", None, None))
	assert worker.fallback_ready is False
	assert worker.prepare_fallback() is False
