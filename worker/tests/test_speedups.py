"""
高速化 (speedups.py) の試験。
- yt-dlp の内部 API は sys.modules に置いた偽のモジュールで差し替え、同じプロセスの中で試す
- 実行: uv run --directory worker pytest tests/test_speedups.py
"""
import os
import sys
import time
import types
from typing import Any

import speedups
from conftest import FakeRequest, install_fake_yt_dlp


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


def test_premium_pin_accepts_any_arguments(monkeypatch, deno_on_path):
	"""固定した判定は引数の数と名前によらず同じ結果を返す (yt-dlp の引数の変更で TypeError にしない)"""
	fake = install_fake_yt_dlp(monkeypatch)
	speedups.apply(speedups.SpeedupConfig(cookies="firefox", premium="yes"))
	ie = fake.YoutubeIE(is_authenticated=True)
	assert ie._is_premium_subscriber() is True
	assert ie._is_premium_subscriber({"x": 1}) is True
	assert ie._is_premium_subscriber({"x": 1}, "extra", initial_data=None, webpage="") is True
	# ログインしていなければ yes でも Premium として扱わない
	assert fake.YoutubeIE(is_authenticated=False)._is_premium_subscriber(initial_data={}) is False


def test_cookie_dependent_client_is_used_only_with_cookies(monkeypatch, deno_on_path):
	"""Cookie が無ければ web_music と player_skip を使わない"""
	install_fake_yt_dlp(monkeypatch)
	config = speedups.SpeedupConfig(cookies=None, premium="yes")
	applied = speedups.apply(config)
	# web_music と player_skip はログイン Cookie が前提のため、Cookie が無ければ何も足さない
	assert speedups.stream_options(config, applied) == {}
