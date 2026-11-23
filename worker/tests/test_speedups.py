"""
高速化 (speedups.py と worker.py の予備設定) の試験。
- yt-dlp の内部 API は sys.modules に置いた偽のモジュールで差し替え、同じプロセスの中で試す
- network の印を付けた試験だけが実物の yt-dlp と Deno と YouTube を使う。既定の実行では除外する
- 実行: uv run --directory worker pytest tests/test_speedups.py
- 実物での実行: uv run --directory worker --with "yt-dlp[default,curl-cffi]" --prerelease=allow pytest -m network
"""
import functools
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

ROOT = Path(__file__).resolve().parent.parent

# 実物の試験で解決する動画 (公開から長く残っている短い動画)
NETWORK_VIDEO_URL = "https://www.youtube.com/watch?v=jNQXAC9IVRw"
# 実物の試験で 1 回の応答を待つ上限秒数
NETWORK_RESPONSE_TIMEOUT_SECONDS = 120
# 標準の結果と一致を確かめる項目 (URL は署名が毎回変わるため比べない)
COMPARED_KEYS = ("id", "title", "duration", "format_id", "ext", "acodec", "protocol", "is_live")


class FakeDownloadError(Exception):
	"""yt_dlp.utils.DownloadError の代用"""


class FakeCookieLoadError(Exception):
	"""yt_dlp.cookies.CookieLoadError の代用"""


class FakeImpersonateTarget:
	"""yt_dlp.networking.impersonate.ImpersonateTarget の代用。from_str の引数を保持する"""

	def __init__(self, client: str) -> None:
		"""Args: client は偽装先の名前"""
		self.client = client

	@classmethod
	def from_str(cls, value: str) -> "FakeImpersonateTarget":
		"""Args: value は偽装先の名前。Returns: 生成したインスタンス"""
		return cls(value)


class FakeRequest:
	"""yt_dlp.networking.Request の代用"""

	def __init__(self, url: str, extensions: dict[str, Any] | None = None) -> None:
		"""Args: url は宛先、extensions は timeout などの追加設定"""
		self.url = url
		self.extensions = extensions or {}


def install_fake_yt_dlp(monkeypatch: pytest.MonkeyPatch, *, internals: bool = True) -> types.SimpleNamespace:
	"""
	偽の yt_dlp を sys.modules に置く。実物や過去の import の残りは試験の間だけ取り除く。

	Args:
		monkeypatch: pytest の monkeypatch
		internals: False なら最上位のモジュールだけを置き、内部 API の import を失敗させる
	Returns:
		偽のクラスを属性に持つ名前空間 (YoutubeIE、EJSBaseJCP、YoutubeDL、calls)
	Example:
		fake = install_fake_yt_dlp(monkeypatch)
	"""
	for name in list(sys.modules):
		if name == "yt_dlp" or name.startswith("yt_dlp.") or name == "resident_jsc":
			monkeypatch.delitem(sys.modules, name)

	calls: list[dict[str, Any]] = []

	class YoutubeDL:
		"""YoutubeDL の代用。impersonate の無い設定では解決に失敗する"""

		def __init__(self, params: dict[str, Any]) -> None:
			"""Args: params は YoutubeDL のオプション"""
			self.params = params

		def extract_info(self, url: str, download: bool = False) -> dict[str, Any]:
			"""Args: url は動画の URL。Returns: 固定の動画情報。Raises: FakeDownloadError"""
			calls.append(self.params)
			if "impersonate" not in self.params:
				raise FakeDownloadError("HTTP Error 403: Forbidden")
			return {"id": "vid", "title": "title", "url": "https://example.invalid/a?expire=1", "format_id": "251"}

	root = types.ModuleType("yt_dlp")
	root.__path__ = []
	root.YoutubeDL = YoutubeDL
	root.utils = types.SimpleNamespace(DownloadError=FakeDownloadError)
	root.version = types.SimpleNamespace(__version__="fake")
	monkeypatch.setitem(sys.modules, "yt_dlp", root)
	namespace = types.SimpleNamespace(YoutubeDL=YoutubeDL, calls=calls, YoutubeIE=None, EJSBaseJCP=None)
	if not internals:
		return namespace

	class YoutubeIE:
		"""YoutubeIE の代用。Premium の判定は常に False を返す"""

		def __init__(self, is_authenticated: bool) -> None:
			"""Args: is_authenticated はログイン済みか"""
			self.is_authenticated = is_authenticated

		def _is_premium_subscriber(self, initial_data: Any) -> bool:
			"""Returns: 常に False"""
			return False

	class EJSBaseJCP:
		"""EJSBaseJCP の代用"""

		_CACHE_SECTION = "challenge-solver"
		_ENABLE_PREPROCESSED_PLAYER_CACHE = False

	modules = {
		"yt_dlp.extractor": {},
		"yt_dlp.extractor.youtube": {"YoutubeIE": YoutubeIE},
		"yt_dlp.extractor.youtube.jsc": {},
		"yt_dlp.extractor.youtube.jsc._builtin": {},
		"yt_dlp.extractor.youtube.jsc._builtin.ejs": {"EJSBaseJCP": EJSBaseJCP},
		"yt_dlp.networking": {"Request": FakeRequest},
		"yt_dlp.networking.impersonate": {"ImpersonateTarget": FakeImpersonateTarget},
		"yt_dlp.cookies": {"CookieLoadError": FakeCookieLoadError},
	}
	for name, attributes in modules.items():
		module = types.ModuleType(name)
		module.__path__ = []
		for key, value in attributes.items():
			setattr(module, key, value)
		monkeypatch.setitem(sys.modules, name, module)
	namespace.YoutubeIE = YoutubeIE
	namespace.EJSBaseJCP = EJSBaseJCP
	return namespace


@pytest.fixture
def deno_on_path(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Deno が PATH にあるものとして扱い、常駐 Deno の登録まで進ませる"""
	monkeypatch.setattr(speedups.shutil, "which", lambda name: "C:/fake/deno.exe")


def test_missing_internal_api_returns_false_and_does_not_raise(monkeypatch, deno_on_path):
	"""yt-dlp の内部 API が無ければ各機能が False になり、例外を出さない"""
	install_fake_yt_dlp(monkeypatch, internals=False)
	config = speedups.SpeedupConfig(cookies="firefox", premium="yes")
	applied = speedups.apply(config)
	assert applied == {"player_cache": False, "premium_pin": False, "resident_jsc": False}
	# 固定に失敗したら initial_data を省かない (省くと Premium の高音質を選ばなくなる)
	args = speedups.stream_options(config, applied)["extractor_args"]["youtube"]
	assert "initial_data" not in args["player_skip"]


def test_each_speedup_failure_is_independent(monkeypatch, deno_on_path, tmp_path):
	"""1 つの機能の失敗が他の機能の適用を止めない"""
	fake = install_fake_yt_dlp(monkeypatch)

	class BrokenMeta(type):
		"""属性の書き換えで想定外の例外を出すメタクラス"""

		def __setattr__(cls, name: str, value: Any) -> None:
			"""Raises: RuntimeError を常に送出する"""
			raise RuntimeError("broken")

	class BrokenEJSBaseJCP(metaclass=BrokenMeta):
		"""プレイヤーキャッシュの有効化で例外を出す EJSBaseJCP"""

		_CACHE_SECTION = "challenge-solver"
		_ENABLE_PREPROCESSED_PLAYER_CACHE = False

	monkeypatch.setattr(sys.modules["yt_dlp.extractor.youtube.jsc._builtin.ejs"], "EJSBaseJCP", BrokenEJSBaseJCP)
	applied = speedups.apply(speedups.SpeedupConfig(cookies="firefox", premium="yes"))
	# プレイヤーキャッシュの例外と常駐 Deno の import の失敗が、Premium の固定を止めない
	assert applied == {"player_cache": False, "premium_pin": True, "resident_jsc": False}
	assert fake.YoutubeIE(is_authenticated=True)._is_premium_subscriber(None) is True

	# 起動時の準備も個別に失敗する。接続の失敗が古いプレイヤーキャッシュの掃除を止めない
	monkeypatch.setattr(sys.modules["yt_dlp.extractor.youtube.jsc._builtin.ejs"], "EJSBaseJCP", fake.EJSBaseJCP)
	cache_dir = tmp_path / "challenge-solver"
	cache_dir.mkdir()
	now = time.time()
	for age in range(4):
		path = cache_dir / f"player,3Ahttps,3A,2F,2Fv{age}.json"
		path.write_text("{}", encoding="utf-8")
		os.utime(path, (now - age * 60, now - age * 60))
	other = cache_dir / "lib.json"
	other.write_text("{}", encoding="utf-8")

	class OfflineYoutubeDL:
		"""接続に失敗し、Cookie の読み込みに成功する YoutubeDL の代用"""

		cookiejar = object()
		cache = types.SimpleNamespace(_get_root_dir=lambda: str(tmp_path))

		def urlopen(self, request: FakeRequest) -> Any:
			"""Raises: OSError を常に送出する"""
			raise OSError("offline")

	ydl = OfflineYoutubeDL()
	warmed = speedups.warm_up(ydl, ydl, speedups.SpeedupConfig(cookies="firefox", premium="yes"), applied)
	assert warmed == {"connection_warmup": False, "player_cache_prune": True, "resident_jsc_warmup": False}
	remaining = sorted(path.name for path in cache_dir.iterdir())
	assert remaining == ["lib.json", "player,3Ahttps,3A,2F,2Fv0.json", "player,3Ahttps,3A,2F,2Fv1.json"]


def test_premium_pin_is_skipped_on_auto(monkeypatch, deno_on_path):
	"""auto では Premium の判定を固定せず、初期データの取得も省かない"""
	fake = install_fake_yt_dlp(monkeypatch)
	original = fake.YoutubeIE._is_premium_subscriber
	auto = speedups.SpeedupConfig(cookies="firefox", premium="auto")
	applied = speedups.apply(auto)
	assert applied["premium_pin"] is False
	assert fake.YoutubeIE._is_premium_subscriber is original
	assert speedups.stream_options(auto, applied)["extractor_args"]["youtube"] == {
		"player_client": ["web_music"],
		"player_skip": ["webpage", "configs"],
	}

	pinned = speedups.SpeedupConfig(cookies="firefox", premium="no")
	applied = speedups.apply(pinned)
	assert applied["premium_pin"] is True
	assert fake.YoutubeIE(is_authenticated=True)._is_premium_subscriber({"x": 1}) is False
	assert speedups.stream_options(pinned, applied)["extractor_args"]["youtube"]["player_skip"] == [
		"webpage", "configs", "initial_data",
	]


def test_cookie_dependent_client_is_used_only_with_cookies(monkeypatch, deno_on_path):
	"""Cookie が無ければ web_music と player_skip を使わない"""
	install_fake_yt_dlp(monkeypatch)
	config = speedups.SpeedupConfig(cookies=None, premium="yes")
	applied = speedups.apply(config)
	# web_music と player_skip はログイン Cookie が前提のため、Cookie が無ければ何も足さない
	assert speedups.stream_options(config, applied) == {}


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
	assert isinstance(caught.value.fallback, worker_module.FallbackTimeoutError)
	assert worker_module.error_response(1, caught.value)["error"]["code"] == "internal"

	# 止まったスレッドを待たずに、次の要求の予備設定は終わる
	request_budget.begin(budget)
	try:
		assert worker.resolve("https://www.youtube.com/watch?v=x", None)["id"] == "vid"
	finally:
		request_budget.end()
	assert time.monotonic() - started < stall / 2


def import_resident_jsc(monkeypatch: pytest.MonkeyPatch, *, standard_available: bool | None = None) -> types.ModuleType:
	"""
	偽の yt_dlp の内部 API を置いて resident_jsc を読み込む。Deno の代わりの子プロセスで _ResidentDeno を試すため。

	Args:
		monkeypatch: pytest の monkeypatch
		standard_available: None でなければ、偽の EJSBaseJCP に is_available (この値を返す) を持たせ、
			DenoJCP をその派生にする (標準のプロバイダの持ち時間の門を試すため)
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
			"JsChallengeProviderResponse": object,
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


def wait_for(condition: Any, limit: float) -> None:
	"""
	条件が成り立つまで実時間で待つ。

	Args:
		condition: 引数の無い判定
		limit: 待つ上限秒数
	Raises:
		AssertionError: limit 秒以内に成り立たない場合
	"""
	deadline = time.monotonic() + limit
	while not condition():
		assert time.monotonic() < deadline, "待ち切れません"
		time.sleep(0.01)


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


def test_runner_thread_keeps_the_deadline_of_its_job():
	"""打ち切った予備設定のスレッドは自分の処理の期限を持ち続け、次の要求の期限や期限なしを読まない"""
	import worker as worker_module

	runner = worker_module.BoundedRunner("test-runner")
	release = threading.Event()
	seen: list[float | None] = []

	def action(state: dict[str | None, Any]) -> None:
		"""止められた後に、このスレッドから見た残りの秒数を記録する"""
		release.wait(10)
		seen.append(request_budget.remaining())

	request_budget.begin(0.2)
	try:
		with pytest.raises(worker_module.FallbackTimeoutError):
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
	import worker as worker_module

	server = resident_jsc._ResidentDeno()
	env = dict(os.environ)
	try:
		# 期限の無い要求で起動しておく
		assert server.request(SLOW_DENO, env, "script", solve(0))["type"] == "result"
		pid = server.pid
		assert pid is not None

		# 予備設定のスレッドが 2 秒かかる往復の途中で打ち切られる
		runner = worker_module.BoundedRunner("test-fallback")
		request_budget.begin(0.8)
		try:
			with pytest.raises(worker_module.FallbackTimeoutError):
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


class FakeDirector:
	"""YoutubeDL の _request_director の代用。close の回数を数え、fail なら close で送出する"""

	def __init__(self, fail: bool = False) -> None:
		"""Args: fail は close で例外を送出するか"""
		self.closed = 0
		self.fail = fail

	def close(self) -> None:
		"""閉じた回数を数える。Raises: RuntimeError (fail のとき)"""
		self.closed += 1
		if self.fail:
			raise RuntimeError("close failed")


def worker_with_clock(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, list[float]]:
	"""
	時計を差し替えた Worker を作る。返す list の先頭を書き換えると時刻が進む。

	Args:
		monkeypatch: pytest の monkeypatch
	Returns:
		(Worker、時刻を持つ 1 要素の list)
	"""
	fake = install_fake_yt_dlp(monkeypatch)
	base = {"id": "vid", "title": "title", "url": "https://example.invalid/a?expire=1", "format_id": "251"}
	monkeypatch.setattr(fake.YoutubeDL, "extract_info", lambda self, url, download=False: dict(base))
	import worker as worker_module

	clock = [1000.0]
	monkeypatch.setattr(worker_module, "_now", lambda: clock[0])
	return worker_module.Worker(worker_module.build_options("cache", None, None)), clock


RESOLVE_REQUEST = {"op": "resolve", "url": "https://www.youtube.com/watch?v=x"}


def test_idle_resets_connections_before_next_request(monkeypatch, deno_on_path):
	import worker as worker_module

	worker, clock = worker_with_clock(monkeypatch)
	director = FakeDirector()
	worker.stream.__dict__["_request_director"] = director
	worker.handle(RESOLVE_REQUEST)
	clock[0] += worker_module.IDLE_RESET_SECONDS - 1
	worker.handle(RESOLVE_REQUEST)
	assert director.closed == 0
	clock[0] += worker_module.IDLE_RESET_SECONDS
	worker.handle(RESOLVE_REQUEST)
	assert director.closed == 1
	assert "_request_director" not in worker.stream.__dict__


def test_ping_does_not_keep_connections_alive(monkeypatch, deno_on_path):
	import worker as worker_module

	worker, clock = worker_with_clock(monkeypatch)
	worker.handle(RESOLVE_REQUEST)
	director = FakeDirector()
	worker.flat.__dict__["_request_director"] = director
	for _ in range(3):
		clock[0] += worker_module.IDLE_RESET_SECONDS / 2
		worker.handle({"op": "ping"})
	worker.handle(RESOLVE_REQUEST)
	assert director.closed == 1


def test_first_request_without_warmup_does_not_reset(monkeypatch, deno_on_path):
	worker, clock = worker_with_clock(monkeypatch)
	director = FakeDirector()
	worker.stream.__dict__["_request_director"] = director
	clock[0] += 10_000
	worker.handle(RESOLVE_REQUEST)
	assert director.closed == 0


def test_warmup_counts_as_network_use(monkeypatch, deno_on_path):
	import worker as worker_module

	worker, clock = worker_with_clock(monkeypatch)
	director = FakeDirector()
	worker.stream.__dict__["_request_director"] = director
	# 起動時の接続の準備の後に記録する (main が warm_up の直後に呼ぶ)
	worker.mark_network_use()
	clock[0] += worker_module.IDLE_RESET_SECONDS - 1
	worker.handle(RESOLVE_REQUEST)
	assert director.closed == 0
	worker.stream.__dict__["_request_director"] = director
	clock[0] += worker_module.IDLE_RESET_SECONDS + 1
	worker.handle(RESOLVE_REQUEST)
	assert director.closed == 1


def test_main_marks_network_use_after_warmup():
	import inspect

	import worker as worker_module

	source = inspect.getsource(worker_module.main)
	assert source.index("speedups.warm_up(") < source.index("worker.mark_network_use()")


def test_reset_failure_does_not_break_the_request(monkeypatch, deno_on_path, capsys):
	import worker as worker_module

	worker, clock = worker_with_clock(monkeypatch)
	worker.handle(RESOLVE_REQUEST)
	worker.stream.__dict__["_request_director"] = FakeDirector(fail=True)
	clock[0] += worker_module.IDLE_RESET_SECONDS + 1
	assert worker.handle(RESOLVE_REQUEST)["id"] == "vid"
	assert "connection_reset" in capsys.readouterr().err


def test_fallback_thread_resets_its_own_instances(monkeypatch):
	import worker as worker_module

	clock = [1000.0]
	monkeypatch.setattr(worker_module, "_now", lambda: clock[0])
	runner = worker_module.BoundedRunner("test-idle")
	director = FakeDirector()

	def install(state: dict) -> int:
		"""実行スレッドの辞書に偽のインスタンスを置く"""
		instance = types.SimpleNamespace()
		instance.__dict__["_request_director"] = director
		state["x"] = instance
		return 0

	runner.run(install, 5.0)
	clock[0] += worker_module.IDLE_RESET_SECONDS + 1
	runner.run(lambda state: 0, 5.0)
	assert director.closed == 1


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


def install_warmup_recorder(
	monkeypatch: pytest.MonkeyPatch, fake: types.SimpleNamespace, fail: bool = False,
) -> list[tuple[str, float]]:
	"""
	偽の YoutubeDL に urlopen と cookiejar を足し、温めの要求を記録する。

	Args:
		monkeypatch: pytest の monkeypatch
		fake: install_fake_yt_dlp の戻り値
		fail: True なら urlopen で例外を送出する
	Returns:
		(URL、timeout) の記録の一覧 (呼ばれるたびに追記される)
	Example:
		opened = install_warmup_recorder(monkeypatch, fake)
	"""
	opened: list[tuple[str, float]] = []

	class Response:
		"""urlopen の応答の代用"""

		def __enter__(self) -> "Response":
			"""Returns: 自分"""
			return self

		def __exit__(self, *args: Any) -> None:
			"""何もしない"""

		def read(self) -> bytes:
			"""Returns: 空のバイト列"""
			return b""

	def urlopen(self: Any, request: FakeRequest) -> Response:
		"""Returns: 空の応答。Raises: OSError (fail のとき)"""
		opened.append((request.url, request.extensions["timeout"]))
		if fail:
			raise OSError("network down")
		return Response()

	monkeypatch.setattr(fake.YoutubeDL, "urlopen", urlopen, raising=False)
	monkeypatch.setattr(fake.YoutubeDL, "cookiejar", None, raising=False)
	return opened


def rewarm_worker(
	monkeypatch: pytest.MonkeyPatch, cookies: str | None, fail: bool = False,
) -> tuple[Any, list[float], list[tuple[str, float]]]:
	"""
	時計を差し替え、温めの要求を記録する Worker を作る。起動時の準備を済ませた状態にする。

	Args:
		monkeypatch: pytest の monkeypatch
		cookies: build_options に渡す Cookie の指定
		fail: True なら温めの要求が失敗する
	Returns:
		(Worker、時刻を持つ 1 要素の list、温めの要求の記録)
	Example:
		worker, clock, opened = rewarm_worker(monkeypatch, "firefox")
	"""
	fake = install_fake_yt_dlp(monkeypatch)
	base = {"id": "vid", "title": "title", "url": "https://example.invalid/a?expire=1", "format_id": "251"}
	monkeypatch.setattr(fake.YoutubeDL, "extract_info", lambda self, url, download=False: dict(base))
	opened = install_warmup_recorder(monkeypatch, fake, fail)
	import worker as worker_module

	clock = [1000.0]
	monkeypatch.setattr(worker_module, "_now", lambda: clock[0])
	worker = worker_module.Worker(worker_module.build_options("cache", cookies, None))
	worker.mark_network_use()
	return worker, clock, opened


def test_ping_after_idle_rewarms_after_the_response(monkeypatch, deno_on_path):
	"""放置の後の ping は通信せずに応答し、応答の後に接続を張り直して温める。温めた直後の要求では張り直さない"""
	import worker as worker_module

	worker, clock, opened = rewarm_worker(monkeypatch, "firefox")
	director = FakeDirector()
	worker.stream.__dict__["_request_director"] = director
	clock[0] += worker_module.IDLE_RESET_SECONDS + 1
	assert worker.handle({"op": "ping"})["pid"] == os.getpid()
	# 応答の前には通信しない
	assert opened == [] and director.closed == 0
	worker.after_response()
	assert director.closed == 1
	# Cookie ありは解決用の www と music、一覧用の www を温める
	assert [url for url, _ in opened] == [
		speedups.WARMUP_URL, speedups.WARMUP_MUSIC_URL, speedups.WARMUP_URL]
	assert all(0 < timeout <= speedups.WARMUP_TIMEOUT_SECONDS for _, timeout in opened)
	# 温めた時刻を前の通信として扱う
	worker.stream.__dict__["_request_director"] = director
	clock[0] += worker_module.IDLE_RESET_SECONDS - 1
	worker.handle(RESOLVE_REQUEST)
	assert director.closed == 1


def test_ping_without_idle_does_not_touch_the_network(monkeypatch, deno_on_path):
	"""放置の閾値に届かない ping では通信しない。閾値を超えたときだけ 1 回温める"""
	import worker as worker_module

	worker, clock, opened = rewarm_worker(monkeypatch, None)
	step = worker_module.IDLE_RESET_SECONDS / 3
	for _ in range(2):
		clock[0] += step
		worker.handle({"op": "ping"})
		worker.after_response()
	assert opened == []
	for _ in range(3):
		clock[0] += step
		worker.handle({"op": "ping"})
		worker.after_response()
	# Cookie なしは www を解決用と一覧用で 1 回ずつ。3 回の ping のうち温めたのは 1 回だけ
	assert len(opened) == 2


def test_long_idle_rewarms_once_until_next_request(monkeypatch, deno_on_path):
	"""放置が続いても温めるのは 1 回だけ。次の要求の後の放置では、また 1 回温める"""
	import worker as worker_module

	worker, clock, opened = rewarm_worker(monkeypatch, None)
	for _ in range(6):
		clock[0] += worker_module.IDLE_RESET_SECONDS + 1
		worker.handle({"op": "ping"})
		worker.after_response()
	# Cookie なしは www を解決用と一覧用で 1 回ずつ。放置の間、温め直さない
	assert len(opened) == 2
	worker.handle(RESOLVE_REQUEST)
	clock[0] += worker_module.IDLE_RESET_SECONDS + 1
	worker.handle({"op": "ping"})
	worker.after_response()
	assert len(opened) == 4


def test_failed_rewarm_breaks_nothing(monkeypatch, deno_on_path, capsys):
	"""温めの要求が失敗しても例外を出さず、次の要求は通常どおり処理し、ping のたびに温め直さない"""
	import worker as worker_module

	worker, clock, opened = rewarm_worker(monkeypatch, None, fail=True)
	clock[0] += worker_module.IDLE_RESET_SECONDS + 1
	worker.handle({"op": "ping"})
	worker.after_response()
	assert len(opened) == 2
	assert "idle_rewarm: partial" in capsys.readouterr().err
	clock[0] += worker_module.IDLE_RESET_SECONDS / 2
	worker.handle({"op": "ping"})
	worker.after_response()
	assert len(opened) == 2
	assert worker.handle(RESOLVE_REQUEST)["id"] == "vid"


def test_rewarm_respects_the_total_limit(monkeypatch, deno_on_path):
	"""温めの合計の上限を使い切ったら、残りの URL は飛ばす"""
	fake = install_fake_yt_dlp(monkeypatch)
	opened = install_warmup_recorder(monkeypatch, fake)
	ydl = fake.YoutubeDL({})
	assert speedups.warm_up_connections([(ydl, (speedups.WARMUP_URL, speedups.WARMUP_MUSIC_URL))], 0.0) is False
	assert opened == []
	assert speedups.warm_up_connections([(ydl, (speedups.WARMUP_URL,))], 1.0) is True
	assert 0 < opened[0][1] <= 1.0


def test_main_rewarms_after_writing_the_response():
	"""応答を書き出して flush してから、放置の後の温めを行う (Rust 側の死活確認を遅らせない)"""
	import inspect

	import worker as worker_module

	source = inspect.getsource(worker_module.main)
	assert source.index("out.flush()") < source.index("worker.after_response()")


def list_worker(monkeypatch: pytest.MonkeyPatch, results: dict[str, Any]) -> tuple[Any, list[dict[str, Any]]]:
	"""
	一覧の取得を記録する Worker を作る。extract_info は URL ごとに results の値を返す。

	Args:
		monkeypatch: pytest の monkeypatch
		results: URL と、その extract_info の戻り値の対応
	Returns:
		(Worker、extract_info の呼び出しの記録 (url、ie_key、process) の一覧)
	Example:
		worker, calls = list_worker(monkeypatch, {"https://www.youtube.com/playlist?list=x": {"entries": []}})
	"""
	fake = install_fake_yt_dlp(monkeypatch)
	calls: list[dict[str, Any]] = []

	def extract(self: Any, url: str, download: bool = False, ie_key: str | None = None, process: bool = True) -> Any:
		"""Returns: results の値。呼び出しを記録する"""
		calls.append({"url": url, "ie_key": ie_key, "process": process})
		return results[url]

	monkeypatch.setattr(fake.YoutubeDL, "extract_info", extract)
	import worker as worker_module

	return worker_module.Worker(worker_module.build_options("cache", None, None)), calls


def counted_entries(count: int, consumed: list[int], before_yield: Any = None) -> Any:
	"""
	要素を 1 件ずつ返すジェネレータ (yt-dlp のページごとの取得の代わり)。返した件数を consumed に記録する。

	Args:
		count: 返す件数
		consumed: 返した件数を入れる 1 要素の list
		before_yield: 各要素を返す直前 (読む側の next の中) に呼ぶ処理。返す件数を受け取る。None なら呼ばない
	Returns:
		要素のジェネレータ
	Example:
		entries = counted_entries(5, [0])
	"""
	for index in range(count):
		consumed[0] += 1
		if before_yield is not None:
			before_yield(consumed[0])
		yield {"id": f"id{index:09d}", "title": f"t{index}", "duration": index, "url": "x"}


PLAYLIST_URL = "https://www.youtube.com/playlist?list=PLabc"


def test_list_reads_entries_lazily_up_to_the_limit(monkeypatch, deno_on_path):
	"""一覧は yt-dlp に処理させずに 1 件ずつ読み、上限に達したら次を読まない (先のページを取らない)"""
	consumed = [0]
	worker, calls = list_worker(monkeypatch, {
		PLAYLIST_URL: {"_type": "playlist", "title": "list", "entries": counted_entries(10, consumed)},
	})
	result = worker.flat_playlist(PLAYLIST_URL, 3)
	assert [entry["id"] for entry in result["entries"]] == ["id000000000", "id000000001", "id000000002"]
	assert result["truncated"] is False and result["title"] == "list"
	assert consumed[0] == 3
	assert calls == [{"url": PLAYLIST_URL, "ie_key": None, "process": False}]


def test_list_is_truncated_when_the_budget_runs_low(monkeypatch, deno_on_path, capsys):
	"""持ち時間の残りが FLAT_LIST_RESERVE_SECONDS を下回ったら、次の要素を読まずに途中までの一覧を返す"""
	import worker as worker_module

	left = [100.0]
	monkeypatch.setattr(request_budget, "remaining", lambda: left[0])

	def spend(count: int) -> None:
		"""2 件目を返した後に、持ち時間の残りを足りなくする"""
		if count == 2:
			left[0] = worker_module.FLAT_LIST_RESERVE_SECONDS - 1

	consumed = [0]
	worker, _ = list_worker(monkeypatch, {
		PLAYLIST_URL: {"_type": "playlist", "title": "list", "entries": counted_entries(10, consumed, spend)},
	})
	result = worker.flat_playlist(PLAYLIST_URL, 100)
	assert [entry["id"] for entry in result["entries"]] == ["id000000000", "id000000001"]
	assert result["truncated"] is True
	assert consumed[0] == 2
	assert "list_truncated: 2 entries" in capsys.readouterr().err


def test_list_reads_the_first_entry_even_when_the_budget_is_low(monkeypatch, deno_on_path):
	"""最初のページは extract_info が取得済みのため、残りが足りなくても 1 件目までは読み、空の一覧にしない"""
	import worker as worker_module

	monkeypatch.setattr(request_budget, "remaining", lambda: worker_module.FLAT_LIST_RESERVE_SECONDS / 2)
	consumed = [0]
	worker, _ = list_worker(monkeypatch, {
		PLAYLIST_URL: {"_type": "playlist", "title": "list", "entries": counted_entries(10, consumed)},
	})
	result = worker.flat_playlist(PLAYLIST_URL, 100)
	assert [entry["id"] for entry in result["entries"]] == ["id000000000"]
	assert result["truncated"] is True
	assert consumed[0] == 1


def test_list_follows_redirects_with_a_limit(monkeypatch, deno_on_path):
	"""yt-dlp が返す転送はたどる。転送先の題名が無ければ転送元の題名を使い、転送が続きすぎたら失敗にする"""
	import worker as worker_module

	target = "https://www.youtube.com/playlist?list=PLtarget"
	worker, calls = list_worker(monkeypatch, {
		PLAYLIST_URL: {"_type": "url", "url": target, "ie_key": "YoutubeTab", "title": "origin"},
		target: {"_type": "playlist", "entries": [{"id": "a", "title": "A", "duration": 1}, None]},
	})
	result = worker.flat_playlist(PLAYLIST_URL, 10)
	assert [entry["id"] for entry in result["entries"]] == ["a"]
	assert result["title"] == "origin"
	assert calls[1] == {"url": target, "ie_key": "YoutubeTab", "process": False}

	loop = "https://www.youtube.com/playlist?list=PLloop"
	worker, calls = list_worker(monkeypatch, {loop: {"_type": "url", "url": loop}})
	with pytest.raises(ValueError):
		worker.flat_playlist(loop, 10)
	assert len(calls) == worker_module.MAX_LIST_REDIRECTS + 1


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


def test_pinned_instance_shares_the_loaded_cookies(monkeypatch, deno_on_path):
	"""形式を固定したインスタンスは、解決用のインスタンスが読み込んだ cookiejar を使い、Firefox の Cookie を読み直さない"""
	fake = install_fake_yt_dlp(monkeypatch)
	base = {"id": "vid", "title": "title", "url": "https://example.invalid/a?expire=1", "format_id": "251"}
	monkeypatch.setattr(fake.YoutubeDL, "extract_info", lambda self, url, download=False: dict(base))
	loads: list[object] = []

	def cookiejar(self: Any) -> object:
		"""Cookie の読み込みの代わり。読み込みのたびに新しい値を返す"""
		jar = object()
		loads.append(jar)
		return jar

	# yt-dlp と同じく cached_property にする (インスタンスの __dict__ に置いた値が優先される)
	cached = functools.cached_property(cookiejar)
	cached.__set_name__(fake.YoutubeDL, "cookiejar")
	monkeypatch.setattr(fake.YoutubeDL, "cookiejar", cached, raising=False)
	import worker as worker_module

	worker = worker_module.Worker(worker_module.build_options("cache", "firefox", None))
	jar = worker.stream.cookiejar
	worker.resolve("https://www.youtube.com/watch?v=x", "251")
	assert worker._pinned["251"].cookiejar is jar
	assert len(loads) == 1


def test_encode_response_logs_the_type_and_id_of_the_failure(capsys):
	"""直列化できない応答 (NaN) は internal の失敗応答に置き換え、型名と id を標準エラーへ記録する"""
	import worker as worker_module

	line = worker_module.encode_response({"id": 7, "ok": True, "result": {"duration": float("nan")}})
	response = json.loads(line)
	assert response == {"id": 7, "ok": False, "error": {"code": "internal", "message": "response could not be encoded"}}
	assert "encode_response: ValueError: id=7" in capsys.readouterr().err


def test_player_cache_helpers_are_shared_without_a_cycle(monkeypatch):
	"""プレイヤーキャッシュの補助は player_cache にあり、resident_jsc は speedups を読み込まずに使う"""
	import player_cache

	assert speedups.player_cache_files is player_cache.player_cache_files
	assert speedups.PLAYER_CACHE_KEY_PREFIX == player_cache.PLAYER_CACHE_KEY_PREFIX
	resident_jsc = import_resident_jsc(monkeypatch)
	assert resident_jsc.player_cache_files is player_cache.player_cache_files
	assert not hasattr(resident_jsc, "speedups")
