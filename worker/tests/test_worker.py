"""
ワーカーの試験。
- 要求と応答の試験は、スタブの yt_dlp を PYTHONPATH の先頭に置き、実プロセスとして起動する
- 接続の張り直し、放置の後の温め直し、一覧の読み方などは、偽の yt_dlp を sys.modules に置き、同じプロセスの中で試す
- ネットワークには触れない
- 実行: uv run --directory worker pytest
"""
import functools
import json
import os
import subprocess
import sys
import threading
import types
from pathlib import Path
from typing import Any

import pytest

import request_budget
import speedups
from conftest import FakeDirector, FakeRequest, install_fake_yt_dlp
from protocol import (
	COOKIE_FAILURE_INVALID, COOKIE_FAILURE_JS_CHALLENGE, ERROR_CODES, OPS, UnsupportedUrlError, classify_error,
	cookie_failure, is_bot_check, validate_url,
)
from worker import DEFAULT_PLAYLIST_LIMIT, StderrLogger, first_mix_video_id, mix_playlist_id

ROOT = Path(__file__).resolve().parent.parent
STUB = Path(__file__).resolve().parent / "stub"
PROTOCOL_JSON = ROOT.parent / "protocol" / "worker-protocol.json"


class WorkerProcess:
	"""子プロセスとして起動したワーカーへ要求を送る補助"""

	def __init__(self, cachedir: Path, extra_args: list[str] | None = None) -> None:
		"""Args: cachedir は --cachedir に渡すパス、extra_args は後ろに足す引数"""
		self.call_log = cachedir / "extract_calls.log"
		self.params_log = cachedir / "extract_params.log"
		env = {
			**os.environ, "PYTHONPATH": str(STUB), "PYTHONUTF8": "1",
			"STUB_CALL_LOG": str(self.call_log), "STUB_PARAMS_LOG": str(self.params_log),
		}
		self.proc = subprocess.Popen(
			[sys.executable, str(ROOT / "worker.py"), "--cachedir", str(cachedir), *(extra_args or [])],
			stdin=subprocess.PIPE,
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE,
			env=env,
			cwd=ROOT,
		)

	def send_raw(self, data: bytes) -> dict:
		"""
		生のバイト列を 1 行送り、応答を 1 行読む。

		Args:
			data: 改行を含まない送信内容
		Returns:
			応答の辞書
		Raises:
			AssertionError: 応答が JSON でない場合
		"""
		assert self.proc.stdin is not None and self.proc.stdout is not None
		self.proc.stdin.write(data + b"\n")
		self.proc.stdin.flush()
		line = self.proc.stdout.readline()
		assert line.endswith(b"\n"), line
		return json.loads(line.decode("utf-8"))

	def calls(self) -> list[str]:
		"""Returns: スタブの extract_info が受け取った URL の一覧"""
		if not self.call_log.exists():
			return []
		return self.call_log.read_text(encoding="utf-8").splitlines()

	def params(self) -> list[dict]:
		"""Returns: スタブの extract_info の呼び出しごとの URL と process"""
		if not self.params_log.exists():
			return []
		return [json.loads(line) for line in self.params_log.read_text(encoding="utf-8").splitlines()]

	def call(self, request: dict) -> dict:
		"""Args: request は要求の辞書。Returns: 応答の辞書"""
		return self.send_raw(json.dumps(request, ensure_ascii=False).encode("utf-8"))


@pytest.fixture
def worker(tmp_path):
	"""ワーカーを起動し、試験後に必ず終了させる"""
	instance = WorkerProcess(tmp_path)
	yield instance
	if instance.proc.poll() is None:
		instance.proc.kill()
	instance.proc.wait(timeout=10)
	for stream in (instance.proc.stdin, instance.proc.stdout, instance.proc.stderr):
		if stream:
			stream.close()


def test_ping_returns_pid(worker):
	response = worker.call({"id": 1, "op": "ping"})
	assert response["id"] == 1 and response["ok"] is True
	# Windows の venv はランチャー経由で起動するため Popen の pid とは一致しない
	pid = response["result"]["pid"]
	assert isinstance(pid, int) and pid > 0 and pid != os.getpid()


def test_stray_stdout_does_not_break_protocol(worker):
	response = worker.call({"id": 2, "op": "resolve", "url": "https://www.youtube.com/stray"})
	assert response["ok"] is True
	# 続けて ping しても全行が JSON のまま読める
	assert worker.call({"id": 3, "op": "ping"})["ok"] is True


def test_title_with_newline_and_emoji_stays_on_one_line(worker):
	response = worker.call({"id": 1, "op": "resolve", "url": "https://www.youtube.com/newline"})
	assert response["result"]["title"] == "line1\nline2 \U0001F3B5"


def test_invalid_json_returns_internal_error_and_continues(worker):
	response = worker.send_raw(b"{not json")
	assert response["ok"] is False and response["error"]["code"] == "internal"
	assert response["id"] is None
	assert worker.call({"id": 5, "op": "ping"})["ok"] is True


def test_unknown_op_returns_internal_error(worker):
	response = worker.call({"id": 7, "op": "pollute"})
	assert response["id"] == 7
	assert response["ok"] is False and response["error"]["code"] == "internal"


def test_format_id_is_applied(worker):
	default = worker.call({"id": 1, "op": "resolve", "url": "https://www.youtube.com/v", "format_id": None})
	assert default["result"]["format_id"] == "bestaudio[acodec=opus]/bestaudio/best"
	assert default["result"]["is_live"] is False
	assert default["result"]["expires_at"] == 1793000000
	pinned = worker.call({"id": 2, "op": "resolve", "url": "https://www.youtube.com/v", "format_id": "140"})
	assert pinned["result"]["format_id"] == "140"
	live = worker.call({"id": 3, "op": "resolve", "url": "https://www.youtube.com/live"})
	assert live["result"]["is_live"] is True


def test_filesize_is_exact_or_none(worker):
	exact = worker.call({"id": 1, "op": "resolve", "url": "https://www.youtube.com/v"})
	assert exact["result"]["filesize"] == 3946490
	# 概算しか無いときは概算を返さない
	approx = worker.call({"id": 2, "op": "resolve", "url": "https://www.youtube.com/approx"})
	assert approx["result"]["filesize"] is None


def test_flat_playlist_and_search_apply_limit(worker):
	listed = worker.call({"id": 1, "op": "flat_playlist", "url": "https://www.youtube.com/playlist", "limit": 2})
	assert [e["id"] for e in listed["result"]["entries"]] == ["a", "b"]
	# 持ち時間で途中までにしていない一覧は truncated が False
	assert listed["result"]["truncated"] is False
	found = worker.call({"id": 2, "op": "search", "query": "q", "limit": 1})
	assert [e["id"] for e in found["result"]["entries"]] == ["a"]


def test_flat_extraction_fetches_only_the_limit(worker):
	# 上限の件数までのページだけを取る (全ページを取ってから切り詰めない)。
	# yt-dlp には一覧を処理させず (process=False)、ワーカーが要素を上限まで読む
	listed = worker.call({"id": 1, "op": "flat_playlist", "url": "https://www.youtube.com/playlist?list=x", "limit": 2})
	found = worker.call({"id": 2, "op": "search", "query": "q", "limit": 1})
	full = worker.call({"id": 3, "op": "flat_playlist", "url": "https://www.youtube.com/playlist?list=y"})
	assert [len(r["result"]["entries"]) for r in (listed, found, full)] == [2, 1, 3]
	assert len(worker.params()) == 3
	assert all(entry["process"] is False for entry in worker.params())
	assert DEFAULT_PLAYLIST_LIMIT > 3


def test_download_error_is_classified(worker):
	response = worker.call({"id": 1, "op": "resolve", "url": "https://www.youtube.com/error:Private%20video"})
	assert response["ok"] is False and response["error"]["code"] == "private"


def test_accepts_request_budget(tmp_path):
	# Rust 側のプールが足す引数を受け付け、持ち時間の中の要求は通常どおり処理する
	instance = WorkerProcess(tmp_path, ["--request-budget-secs", "28"])
	try:
		assert instance.call({"id": 1, "op": "ping"})["ok"] is True
		assert instance.call({"id": 2, "op": "resolve", "url": "https://www.youtube.com/v"})["ok"] is True
	finally:
		instance.proc.kill()
		instance.proc.wait(timeout=10)
		for stream in (instance.proc.stdin, instance.proc.stdout, instance.proc.stderr):
			if stream:
				stream.close()


def test_exits_on_stdin_eof(worker):
	assert worker.call({"id": 1, "op": "ping"})["ok"] is True
	worker.proc.stdin.close()
	assert worker.proc.wait(timeout=10) == 0


@pytest.mark.parametrize(
	"message,code",
	[
		("Sign in to confirm your age", "age_restricted"),
		("Sign in to confirm you're not a bot", "rate_limited"),
		("HTTP Error 429: Too Many Requests", "rate_limited"),
		("This video is unavailable", "not_found"),
		("This video is not available", "not_found"),
		# 形式の指定に合う音声が無いのは動画の有無と関係が無い (予備設定で取り直す対象にする)
		("Requested format is not available. Use --list-formats for a list of available formats", "internal"),
		("Private video", "private"),
		("not available in your country", "geo_blocked"),
		# yt-dlp が YouTube の地域制限で送出する文言 (再生画面の文言そのまま)
		("ERROR: [youtube] abcdefghijk: The uploader has not made this video available in your country", "geo_blocked"),
		("This video is not available from your location due to geo restriction", "geo_blocked"),
		("boom", "internal"),
	],
)
def test_classify_error(message, code):
	assert classify_error(message) == code


@pytest.mark.parametrize(
	"message,expected",
	[
		("Sign in to confirm you're not a bot", True),
		("HTTP Error 429: Too Many Requests", False),
		("rate limit exceeded", False),
		("Private video", False),
	],
)
def test_is_bot_check(message, expected):
	# ボット判定は rate_limited に分類され、その中で "not a bot" の規則に当たるものだけ
	assert is_bot_check(message) is expected
	if expected:
		assert classify_error(message) == "rate_limited"


JS_WARNING = "[youtube] abcdefghijk: n challenge solving failed: Some formats may be missing."
SIG_WARNING = "[youtube] abcdefghijk: Signature solving failed: Some formats may be missing."
COOKIES_WARNING = "[youtube] The provided YouTube account cookies are no longer valid. They have likely been rotated"
FORMAT_ERROR = "ERROR: [youtube] abcdefghijk: Requested format is not available. Use --list-formats"


@pytest.mark.parametrize(
	"warnings,message,expected",
	[
		# 解読に失敗して音声の形式が取れなかった
		([JS_WARNING], FORMAT_ERROR, COOKIE_FAILURE_JS_CHALLENGE),
		([SIG_WARNING], FORMAT_ERROR, COOKIE_FAILURE_JS_CHALLENGE),
		# 解読の警告があっても、失敗の理由が形式でなければ Cookie 関連としない
		([JS_WARNING], "ERROR: Private video", None),
		# 解読の警告が無い形式の失敗 (形式の固定の取り直しなど) は Cookie 関連としない
		([], FORMAT_ERROR, None),
		# Cookie の失効の警告があれば、失敗の理由によらず Cookie 関連とする
		([COOKIES_WARNING], "Sign in to confirm your age", COOKIE_FAILURE_INVALID),
		([COOKIES_WARNING], "Sign in to confirm you're not a bot", COOKIE_FAILURE_INVALID),
		# ただし単なる回数制限では要求を増やさない
		([COOKIES_WARNING], "HTTP Error 429: Too Many Requests", None),
		# 正常なときにも出る警告は判定に使わない
		(["Unable to fetch GVS PO Token for web_music client: Missing required Data Sync ID"], FORMAT_ERROR, None),
	],
)
def test_cookie_failure(warnings, message, expected):
	assert cookie_failure(warnings, message) == expected


def test_logger_records_warnings_of_the_current_thread(capsys):
	logger = StderrLogger()
	with logger.recording() as warnings:
		logger.warning("a")
		# 打ち切られた予備設定のスレッドなど、別のスレッドの警告は記録しない
		other = threading.Thread(target=logger.warning, args=("b",))
		other.start()
		other.join()
		logger.info("i")
	logger.warning("c")
	assert warnings == ["a"]
	# 標準エラーへの出力は記録の有無によらず続ける
	err = capsys.readouterr().err
	assert "[warning] a" in err and "[warning] b" in err and "[warning] c" in err


def test_protocol_constants_match_protocol_json():
	definition = json.loads(PROTOCOL_JSON.read_text(encoding="utf-8"))
	assert list(OPS) == definition["ops"]
	assert list(ERROR_CODES) == definition["error_codes"]


def test_surrogate_title_returns_internal_error_and_keeps_serving(worker):
	response = worker.call({"id": 1, "op": "resolve", "url": "https://www.youtube.com/surrogate"})
	assert response["id"] == 1
	assert response["ok"] is False and response["error"]["code"] == "internal"
	assert worker.call({"id": 2, "op": "ping"})["ok"] is True


def test_nan_in_result_returns_internal_error_and_keeps_serving(worker):
	# NaN は JSON の外の表記になり、Rust 側が応答を読めずにワーカーを再起動するため、失敗応答に置き換える
	response = worker.call({"id": 1, "op": "resolve", "url": "https://www.youtube.com/nan-duration"})
	assert response["id"] == 1
	assert response["ok"] is False and response["error"]["code"] == "internal"
	assert worker.call({"id": 2, "op": "ping"})["ok"] is True


def test_resolve_of_playlist_result_is_an_error(worker):
	response = worker.call({"id": 1, "op": "resolve", "url": "https://www.youtube.com/playlist?list=x"})
	assert response["ok"] is False and response["error"]["code"] == "internal"


@pytest.mark.parametrize(
	("url", "expected"),
	[
		("https://music.youtube.com/playlist?list=RDATgxd7XfodHlwZV9wbGF5bGlzdA", "RDATgxd7XfodHlwZV9wbGF5bGlzdA"),
		("https://www.youtube.com/playlist?list=RDCLAK5uy_abc&si=x", "RDCLAK5uy_abc"),
		("https://www.youtube.com/watch?list=RDabc", "RDabc"),
		# 動画 ID があれば yt-dlp が watch ページから取れるため置き換えない
		("https://www.youtube.com/watch?v=dQw4w9WgXcQ&list=RDabc", None),
		("https://www.youtube.com/playlist?list=PLabc", None),
		("https://www.youtube.com/playlist", None),
	],
)
def test_mix_playlist_id(url, expected):
	assert mix_playlist_id(url) == expected


def test_first_mix_video_id_skips_non_video_items():
	response = {"contents": {"twoColumnWatchNextResults": {"playlist": {"playlist": {"contents": [
		{"automixPreviewVideoRenderer": {}},
		{"playlistPanelVideoRenderer": {"videoId": "dQw4w9WgXcQ"}},
	]}}}}}
	assert first_mix_video_id(response) == "dQw4w9WgXcQ"


@pytest.mark.parametrize(
	"response",
	[
		{},
		{"contents": None},
		{"contents": {"twoColumnWatchNextResults": {"playlist": {"playlist": {"contents": []}}}}},
		{"contents": {"twoColumnWatchNextResults": {"playlist": {"playlist": {"contents": "x"}}}}},
		# 動画 ID の形でない値は URL に入れない
		{"contents": {"twoColumnWatchNextResults": {"playlist": {"playlist": {"contents": [
			{"playlistPanelVideoRenderer": {"videoId": "bad id&x=1"}},
		]}}}}},
	],
)
def test_first_mix_video_id_returns_none_for_unexpected_shapes(response):
	assert first_mix_video_id(response) is None


def test_flat_playlist_of_mix_uses_watch_url_with_first_video(worker):
	response = worker.call({
		"id": 1, "op": "flat_playlist", "url": "https://music.youtube.com/playlist?list=RDmix", "limit": 10,
	})
	assert response["ok"] is True
	assert [e["id"] for e in response["result"]["entries"]] == ["a", "b", "c"]
	assert worker.calls() == ["https://www.youtube.com/watch?v=seedvideo01&list=RDmix"]


@pytest.mark.parametrize("playlist_id", ["RDfail", "RDempty"])
def test_flat_playlist_of_mix_falls_back_to_original_url(worker, playlist_id):
	# 先頭の曲を求められなければ、元の URL のまま yt-dlp に渡す (yt-dlp 本来のエラーになる)
	url = f"https://www.youtube.com/playlist?list={playlist_id}"
	response = worker.call({"id": 1, "op": "flat_playlist", "url": url, "limit": 10})
	assert response["ok"] is True
	assert worker.calls() == [url]


def test_flat_playlist_of_normal_list_is_not_rewritten(worker):
	url = "https://www.youtube.com/playlist?list=PLabc"
	worker.call({"id": 1, "op": "flat_playlist", "url": url, "limit": 10})
	assert worker.calls() == [url]


def test_large_response_is_written_completely(worker):
	response = worker.call({"id": 1, "op": "resolve", "url": "https://www.youtube.com/big"})
	assert response["ok"] is True and len(response["result"]["title"]) == 500_000


REJECTED_URLS = [
	"http://169.254.169.254/",
	"https://youtube.com.evil.example/watch?v=x",
	"https://evil.example/?u=youtube.com",
	"file:///c:/x",
	"https://youtube.com@evil.example/",
	"https://user:pw@www.youtube.com/watch?v=x",
	"https://www.youtube.com:8443/watch?v=x",
	"ftp://www.youtube.com/watch?v=x",
	"//www.youtube.com/watch?v=x",
	"www.youtube.com/watch?v=x",
	"",
]
ALLOWED_HOSTS = ["youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com", "youtu.be", "WWW.YouTube.com"]


@pytest.mark.parametrize("url", REJECTED_URLS)
def test_rejected_urls_never_reach_extract_info(worker, url):
	# スタブは extract_info に入ると "stray" を標準出力へ書く。拒否なら "error:" の分岐にも入らない
	for op in ("resolve", "flat_playlist"):
		response = worker.call({"id": 1, "op": op, "url": url})
		assert response["ok"] is False and response["error"]["code"] == "unsupported_url", (op, url)
	assert worker.calls() == []


def test_extract_info_is_called_for_allowed_url(worker):
	worker.call({"id": 1, "op": "resolve", "url": "https://youtu.be/x"})
	assert worker.calls() == ["https://youtu.be/x"]


@pytest.mark.parametrize("host", ALLOWED_HOSTS)
def test_allowed_hosts_pass(worker, host):
	for scheme in ("https", "http"):
		assert worker.call({"id": 1, "op": "resolve", "url": f"{scheme}://{host}/watch?v=x"})["ok"] is True
	assert worker.call({"id": 2, "op": "flat_playlist", "url": f"https://{host}/playlist?list=x", "limit": 1})["ok"] is True


def test_validate_url_raises_for_rejected():
	for url in REJECTED_URLS:
		with pytest.raises(UnsupportedUrlError):
			validate_url(url)


def test_allowed_extractors_fullmatch_youtube_family():
	"""yt-dlp は抽出器の名前全体に fullmatch で当てるため、YouTube 系の名前全体に一致させる"""
	from worker import ALLOWED_EXTRACTORS
	import re
	allowed = ["youtube", "youtube:search", "youtube:tab", "youtube:playlist", "YoutubeYtBe", "youtube:music:search_url"]
	for name in allowed:
		assert any(re.fullmatch(p, name) for p in ALLOWED_EXTRACTORS), name
	for name in ["Generic", "generic", "niconico", "soundcloud"]:
		assert not any(re.fullmatch(p, name) for p in ALLOWED_EXTRACTORS), name


class CachedDirectorHolder:
	"""_request_director を実物と同じく cached_property で持つ YoutubeDL の代用"""

	@functools.cached_property
	def _request_director(self) -> FakeDirector:
		"""Returns: 新しい FakeDirector"""
		return FakeDirector()


class InheritedDirectorHolder(CachedDirectorHolder):
	"""cached_property を親クラスから受け継ぐ YoutubeDL の代用 (MRO をたどることの確認用)"""


class PlainDirectorHolder:
	"""_request_director を通常の属性で持つ YoutubeDL の代用 (yt-dlp の将来の変更を想定する)"""

	def __init__(self, director: FakeDirector) -> None:
		"""Args: director はインスタンスの属性に置く接続"""
		self._request_director = director


def test_reset_closes_cached_property_director_including_inherited():
	"""cached_property の接続は、親クラスで定義されていても取り除いて閉じる"""
	import worker as worker_module

	for holder_type in (CachedDirectorHolder, InheritedDirectorHolder):
		instance = holder_type()
		director = FakeDirector()
		instance.__dict__["_request_director"] = director
		worker_module.reset_connections([instance])
		assert director.closed == 1
		assert "_request_director" not in instance.__dict__
		# 次の参照で作り直される
		assert isinstance(instance._request_director, FakeDirector)


def test_reset_skips_plain_attribute_director_and_logs_once(monkeypatch, capsys):
	"""接続が通常の属性なら取り除かずに飛ばし (以降の通信を壊さない)、型ごとに 1 回だけ記録する"""
	import worker as worker_module

	monkeypatch.setattr(worker_module, "_unsupported_director_types", set())
	director = FakeDirector()
	instance = PlainDirectorHolder(director)
	worker_module.reset_connections([instance])
	worker_module.reset_connections([PlainDirectorHolder(FakeDirector())])
	assert director.closed == 0
	assert instance._request_director is director
	assert capsys.readouterr().err.count("connection_reset: PlainDirectorHolder._request_director") == 1


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
	runner = worker_module.make_runner("test-idle")
	director = FakeDirector()

	def install(state: dict) -> int:
		"""実行スレッドの辞書に偽のインスタンスを置く"""
		instance = CachedDirectorHolder()
		instance.__dict__["_request_director"] = director
		state["x"] = instance
		return 0

	runner.run(install, 5.0)
	clock[0] += worker_module.IDLE_RESET_SECONDS + 1
	runner.run(lambda state: 0, 5.0)
	assert director.closed == 1


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
