"""
試験の共通の部品。偽の yt_dlp、Deno を PATH にあるものとして扱う fixture、実時間で待つ補助。
- yt-dlp の内部 API は sys.modules に置いた偽のモジュールで差し替え、同じプロセスの中で試す
- 試験のモジュールは from conftest import ... で補助を読み込む (fixture は pytest が渡す)
"""
import functools
import sys
import time
import types
from typing import Any

import pytest

import speedups


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

		@functools.cached_property
		def _request_director(self) -> Any:
			"""実物と同じく cached_property にする (接続の張り直しの対象)。Returns: 新しい FakeDirector"""
			return FakeDirector()

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
