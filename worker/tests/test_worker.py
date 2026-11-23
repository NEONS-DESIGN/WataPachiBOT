"""
ワーカーの試験。スタブの yt_dlp を PYTHONPATH の先頭に置き、実プロセスとして起動する。
- ネットワークには触れない
- 実行: uv run --directory worker pytest
"""
import json
import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest

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
