"""
yt-dlp 常駐ワーカー。
- 標準入力から JSON Lines の要求を受け、プロトコル専用の出力へ応答を返す
- 起動時に fd 1 を複製してプロトコル専用にし、fd 1 自体は標準エラーへ向ける
	(yt-dlp や Deno などの子プロセスが標準出力へ書いても通信が壊れないようにするため)
- 入出力はバイト列のまま UTF-8 で扱う。標準入力の EOF で終了する
- YoutubeDL はスレッド間で共有しない。解決用と一覧用の 2 インスタンスを持つ (形式の固定と予備設定の分は別に持つ)
- 予備設定のインスタンスは専用のスレッドの中で作って使う (持ち時間で待ちを打ち切るため)。
	起動の最後に、そのスレッドへインスタンスの作成と Cookie の読み込みを投げておく (最初の予備設定の待ちを減らす)
- 一覧 (flat_playlist、search) は要素を 1 件ずつ読み、持ち時間の残りが少なくなったら途中までの一覧を返す
	(長い再生リストのページの取得で持ち時間を超え、Rust 側にワーカーごと落とされないようにするため)
- 起動時に高速化 (speedups) を適用し、成否を標準エラーへ記録する。失敗しても標準の動作で続ける
- --request-budget-secs で 1 要求の持ち時間を受け取り、要求ごとに request_budget へ期限を置く。
	予備設定と常駐 Deno の待ちを残りの秒数に収め、Rust 側に打ち切られる前に失敗の応答を返す

呼び出し例:
	echo {"id":1,"op":"ping"} | python worker.py --cachedir .cache
	python worker.py --cachedir .cache --cookies firefox --premium auto --request-budget-secs 28
"""
import argparse
import contextlib
import functools
import json
import os
import queue
import re
import sys
import threading
import time
import urllib.parse
from collections.abc import Iterator
from typing import Any, BinaryIO, Callable

import request_budget
import speedups
# 予備設定の記憶と失敗の型は別のモジュールに置く。従来どおり worker からも参照できるようにする
from fallback import FallbackFailedError, FallbackMemory
from protocol import UnsupportedUrlError, classify_error, cookie_failure, is_bot_check, validate_url

# 既定の形式。opus を優先し、無ければ音声のみ、それも無ければ最良
DEFAULT_FORMAT = "bestaudio[acodec=opus]/bestaudio/best"

# yt-dlp の通信の無通信の上限 (秒)。1 回の読み取りで、この秒数のあいだ何も届かなければ失敗にする
# (要求全体の上限ではない。主経路が 1 回止まるとこの秒数を使うため、持ち時間 (28 秒) の半分未満にして、
#  止まった後でも予備設定で取り直す時間を残す)
SOCKET_TIMEOUT_SECONDS = 10

# 予備設定で取り直すのに要る最小の残り秒数。これより短ければ取り直さず、通常の設定の失敗を返す
# (ページ、プレイヤー、JS チャレンジの取得で数秒かかる。始めても持ち時間内に終わらず、打ち切るだけになる)
FALLBACK_MIN_SECONDS = 5.0

# 予備設定を実行するスレッドの名前
FALLBACK_THREAD_NAME = "stream-fallback"
# Cookie を外した予備設定を実行するスレッドの名前 (Cookie ありの予備設定とインスタンスを分ける)
COOKIELESS_THREAD_NAME = "stream-cookieless"
# Cookie を外した予備設定で取り除くオプション
COOKIE_OPTION_KEYS = ("cookiesfrombrowser", "cookiefile")
# Firefox の Cookie を読み込めなかったときの、Cookie 関連の失敗の理由の名前
COOKIE_FAILURE_LOAD = "cookie_load"

# 前の通信からこの秒数以上あいたら、次の要求の前に HTTP の接続を閉じて張り直す
# (使い回した接続は放置の間に経路の途中で切られ、次の要求が socket_timeout まで止まる。
#  開発機で 8 分の放置の後に 40 秒かかり、閉じてからなら 1.5 秒だった。PoC では 3.5 分の放置でも起きた)
# 張り直しの費用は TLS の接続の確立 1 回分だけなので、余裕を持って短くする
# ping でこの秒数以上の放置を見つけたら、応答を返した後に張り直して温めておく (次の要求で張り直しを払わない)
IDLE_RESET_SECONDS = 60.0

# 放置の後に ping の後で接続を温める、合計の上限秒数 (目安。DNS の解決などは切り詰められない)
# - 温めの間に届いた次の要求は、温め終えてから読む。その要求の持ち時間は読んだ時点から数えるため、
#   Rust 側の余裕 (resolve_timeout_secs と持ち時間の差、2 秒) より短くする
# - 1 URL ごとの上限は speedups.WARMUP_TIMEOUT_SECONDS で、合計の残りで切り詰める
IDLE_REWARM_LIMIT_SECONDS = 1.5

# Cookie ありの主経路で not_found になり、予備設定で解決できた動画を覚える件数 (古いものから忘れる)
# 覚えた動画は主経路を飛ばして予備設定へ直接回す (同じ動画で毎回 2 回解決するのを避ける)
FALLBACK_MEMORY_SIZE = 256

# YouTube の動画 ID の形 (11 文字)。予備設定へ直接回す動画を URL から見分けるのに使う
VIDEO_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{11}")
# パスの 2 番目が動画 ID になる YouTube のパスの接頭辞 (/shorts/<ID> など)
VIDEO_ID_PATH_PREFIXES = ("shorts", "live", "embed", "v", "e")
# 動画 ID をパスの先頭に持つ短縮 URL のホスト
SHORT_URL_HOSTS = ("youtu.be",)

# ミックス (YouTube Music のラジオなど、自動生成の再生リスト) の再生リスト ID の接頭辞
# 動画 ID の無い URL (playlist?list=RD...) は YouTube が "This playlist type is unviewable" で拒むため、
# 先頭の曲の動画 ID を付けた watch の URL に直してから一覧を取る (yt-dlp は watch ページからならミックスを取れる)
MIX_PLAYLIST_PREFIX = "RD"
# 先頭の曲を付けたミックスの URL の基底。後ろに v と list のクエリを付ける
MIX_WATCH_URL = "https://www.youtube.com/watch"
# ミックスの先頭の曲を求めるのに使う extractor と API (web クライアントの /next)
MIX_EXTRACTOR_NAME = "YoutubeTab"
MIX_NEXT_ENDPOINT = "next"
# /next の応答で、ミックスの曲の一覧がある場所
MIX_CONTENTS_PATH = ("contents", "twoColumnWatchNextResults", "playlist", "playlist", "contents")
# 曲の一覧の各要素で、動画を表す部品の名前と、その中の動画 ID のキー
MIX_VIDEO_RENDERER = "playlistPanelVideoRenderer"
MIX_VIDEO_ID_KEY = "videoId"

# 放置の判定と、予備設定へ直接回す動画の期限の判定に使う時計。試験で差し替える
_now: Callable[[], float] = time.monotonic

# YoutubeDL の HTTP の接続を持つ内部属性の名前 (yt-dlp では functools.cached_property)
REQUEST_DIRECTOR_ATTRIBUTE = "_request_director"
# 接続を持つ属性が cached_property でなかった型。記録を型ごとに 1 回にする (要求のたびに出さない)
_unsupported_director_types: set[type] = set()


def _director_is_cached_property(instance: Any) -> bool:
	"""
	インスタンスの型 (MRO をたどる) で、接続を持つ属性が functools.cached_property として定義されているかを返す。
	- cached_property なら、インスタンスの __dict__ から取り除くと次の参照で作り直される。
		それ以外 (通常の属性など) を取り除くと、以降の通信がすべて失敗するため取り除かない

	Args:
		instance: YoutubeDL のインスタンス
	Returns:
		最初に定義が見つかったクラスで cached_property なら True。定義が無い、または別の種類なら False
	Example:
		_director_is_cached_property(worker.stream) -> True
	"""
	for klass in type(instance).__mro__:
		definition = vars(klass).get(REQUEST_DIRECTOR_ATTRIBUTE)
		if definition is not None:
			return isinstance(definition, functools.cached_property)
	return False


def reset_connections(instances: list[Any]) -> None:
	"""
	YoutubeDL の HTTP の接続 (request director) を閉じる。次の通信で yt-dlp が作り直す。
	- YoutubeDL.close は JS チャレンジと PO Token の仕組みまで閉じるため使わず、接続だけを閉じる
	- yt-dlp の内部属性 (_request_director は cached_property) に依存する。失敗しても要求は続け、標準エラーへ記録する
	- 型の定義が cached_property でなければ閉じずに飛ばし、型ごとに 1 回だけ標準エラーへ記録する
		(yt-dlp の変更で通常の属性になった場合に取り除くと、以降の通信がすべて失敗するため)
	- 同じインスタンスが複数回含まれていても 1 回だけ閉じる

	Args:
		instances: YoutubeDL のインスタンス。呼び出したスレッドが持つものに限る (スレッド間で共有しないため)
	Returns:
		なし
	Example:
		reset_connections([worker.stream, worker.flat])
	"""
	seen: set[int] = set()
	for instance in instances:
		if id(instance) in seen:
			continue
		seen.add(id(instance))
		try:
			if not _director_is_cached_property(instance):
				instance_type = type(instance)
				if instance_type not in _unsupported_director_types:
					_unsupported_director_types.add(instance_type)
					speedups.log(
						f"connection_reset: {instance_type.__name__}.{REQUEST_DIRECTOR_ATTRIBUTE} が"
						" cached_property でないため閉じない"
					)
				continue
			director = instance.__dict__.pop(REQUEST_DIRECTOR_ATTRIBUTE, None)
			if director is not None:
				director.close()
		except Exception as error:  # 接続を閉じられなくても要求は続ける (古い接続で遅くなるだけ)
			speedups.log(f"connection_reset: {type(error).__name__}: {error}")

# yt-dlp に許可する extractor 名の正規表現。YouTube 系のみで generic を含めない (任意ホストへの接続を防ぐ)
# yt-dlp は名前全体に fullmatch で当てる (IE_NAME は youtube、youtube:search、youtube:tab、YoutubeYtBe など)
ALLOWED_EXTRACTORS = (r"(?i)youtube(:.*)?", r"(?i)youtubeytbe")

# 応答のエラーメッセージの最大文字数
MAX_ERROR_MESSAGE_CHARS = 500

# 通常の設定の理由が当てにならない (Cookie ありの not_found) とき、予備設定の理由を優先する分類
# (not_found より具体的な理由。internal と not_found は通常の設定の not_found のままにする)
SPECIFIC_FALLBACK_CODES: frozenset[str] = frozenset(("private", "age_restricted", "rate_limited", "geo_blocked"))

# flat_playlist と search の既定の件数
DEFAULT_PLAYLIST_LIMIT = 5000
DEFAULT_SEARCH_LIMIT = 1

# 一覧の次の要素を読む (次のページの取得を含みうる) のに要る、最小の残り秒数。これより短ければ途中までの一覧を返す
# (ページの取得は 1 回の読み取りが止まると SOCKET_TIMEOUT_SECONDS かかる。応答の直列化と書き出しの分を足す)
FLAT_LIST_RESERVE_SECONDS = SOCKET_TIMEOUT_SECONDS + 1.0
# 一覧の取得で、yt-dlp が返す転送 (url、url_transparent) をたどる上限の回数 (循環の防止)
MAX_LIST_REDIRECTS = 3
# 転送を表す yt-dlp の結果の種類
LIST_REDIRECT_TYPES = ("url", "url_transparent")

# 応答に含める項目 (formats などの巨大なデータは返さない)
RESOLVE_KEYS = (
	"id", "title", "duration", "thumbnail", "webpage_url", "url", "http_headers",
	"ext", "acodec", "protocol", "format_id", "abr",
)


class StderrLogger:
	"""
	yt-dlp のログをすべて標準エラーへ出すロガー。呼び出しは yt-dlp が行う。
	- recording の間は、そのスレッドで出た警告を記録する (Cookie 関連の失敗の判定に使う)
	- yt-dlp は only_once の警告も、logger があれば毎回 warning を呼ぶ

	呼び出し例:
		yt_dlp.YoutubeDL({"logger": StderrLogger()})
		with logger.recording() as warnings: ...
	"""

	def __init__(self) -> None:
		"""記録先はスレッドごとに持つ。Example: StderrLogger()"""
		self._local = threading.local()

	@contextlib.contextmanager
	def recording(self) -> Iterator[list[str]]:
		"""
		抜けるまでの間、呼び出したスレッドで出た警告を list に足す。入れ子にしない。
		- 打ち切られた予備設定のスレッドが後から出す警告は、別のスレッドのため混ざらない

		Returns:
			警告を足していく list (抜けた後も読める)
		Example:
			with logger.recording() as warnings:
				ydl.extract_info(url, download=False)
		"""
		warnings: list[str] = []
		self._local.warnings = warnings
		try:
			yield warnings
		finally:
			self._local.warnings = None

	def _write(self, level: str, message: str) -> None:
		"""1 行を標準エラーへ出す。Args: level はログレベル、message は本文。Example: self._write("info", "abc")"""
		print(f"[{level}] {message}", file=sys.stderr, flush=True)

	def debug(self, message: str) -> None:
		"""debug レベルは量が多いため捨てる。Args: message は本文。Example: logger.debug("abc")"""

	def info(self, message: str) -> None:
		"""info レベルを出す。Args: message は本文。Example: logger.info("abc")"""
		self._write("info", message)

	def warning(self, message: str) -> None:
		"""warning レベルを出し、記録中なら記録する。Args: message は本文。Example: logger.warning("abc")"""
		self._write("warning", message)
		warnings = getattr(self._local, "warnings", None)
		if warnings is not None:
			warnings.append(message)

	def error(self, message: str) -> None:
		"""error レベルを出す。Args: message は本文。Example: logger.error("abc")"""
		self._write("error", message)


def take_protocol_output() -> BinaryIO:
	"""
	fd 1 を複製してプロトコル専用の出力を作り、fd 1 を標準エラーへ向ける。

	Returns:
		プロトコル専用のバイナリ出力
	Raises:
		OSError: fd の複製に失敗した場合
	Example:
		out = take_protocol_output()
	"""
	protocol_fd = os.dup(1)
	os.dup2(2, 1)
	sys.stdout = sys.stderr
	# バッファ付きにする。raw な FileIO は大きな応答で部分書き込みになりうる (呼び出し側が flush する)
	return os.fdopen(protocol_fd, "wb")


def build_options(cachedir: str, cookies: str | None, user_agent: str | None) -> dict[str, Any]:
	"""
	全操作に共通の yt-dlp オプションを返す。

	Args:
		cachedir: キャッシュの固定パス
		cookies: Firefox の Cookie を使うなら "firefox"、使わないなら None
		user_agent: 指定する User-Agent、または None
	Returns:
		YoutubeDL に渡す辞書
	Raises:
		なし
	Example:
		build_options(".cache", None, None)["quiet"] -> True
	"""
	options: dict[str, Any] = {
		"quiet": True,
		"no_warnings": False,
		"noprogress": True,
		"color": "no_color",
		"logger": StderrLogger(),
		"cachedir": cachedir,
		"default_search": "ytsearch",
		"noplaylist": True,
		"socket_timeout": SOCKET_TIMEOUT_SECONDS,
		"allowed_extractors": list(ALLOWED_EXTRACTORS),
	}
	if cookies:
		options["cookiesfrombrowser"] = (cookies,)
	if user_agent:
		options["http_headers"] = {"User-Agent": user_agent}
	return options


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
	- 呼び出しは 1 スレッド (メインループ) からに限る

	呼び出し例:
		runner = BoundedRunner("stream-fallback")
		info = runner.run(lambda cache: make(cache).extract_info(url, download=False), 3.0)
	"""

	def __init__(self, name: str) -> None:
		"""Args: name は実行スレッドの名前。スレッドは最初の run で作る。Example: BoundedRunner("stream-fallback")"""
		self._name = name
		self._tasks: queue.Queue[_FallbackJob | None] | None = None

	@staticmethod
	def _loop(tasks: queue.Queue[_FallbackJob | None], state: dict[str | None, Any]) -> None:
		"""
		実行スレッドの本体。None を受け取るまで処理を順に実行する。

		Args:
			tasks: 処理の待ち行列
			state: このスレッドだけが持つ辞書
		Example:
			threading.Thread(target=BoundedRunner._loop, args=(queue.Queue(), {}), daemon=True).start()
		"""
		last_used: float | None = None
		while True:
			job = tasks.get()
			if job is None:
				return
			# このスレッドのインスタンスの接続も、放置の後は張り直す (インスタンスはこのスレッドだけが使う)
			if last_used is not None and _now() - last_used >= IDLE_RESET_SECONDS:
				reset_connections(list(state.values()))
			# 依頼元の要求の期限で固定する。メインループが次の要求で期限を変えても、このスレッドには届かない
			request_budget.adopt(job.deadline)
			try:
				job.result = job.action(state)
			except BaseException as error:  # 呼び出し側へ渡して送出させる。ここで握りつぶさない
				job.error = error
			finally:
				request_budget.adopt(None)
				last_used = _now()
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


def selected_available_at(info: dict[str, Any]) -> int | None:
	"""
	選ばれた形式の available_at (URL が使えるようになる UNIX 秒) を返す。
	- requested_formats があれば、その音声の形式 (vcodec が none、無ければ先頭) を見る。無ければ info 本体を見る

	Args:
		info: yt-dlp の情報の辞書
	Returns:
		UNIX 秒の整数。無い、または数値でなければ None
	Example:
		selected_available_at({"available_at": 1793000123}) -> 1793000123
	"""
	source = info
	requested = info.get("requested_formats")
	if requested:
		audio = [f for f in requested if isinstance(f, dict) and f.get("vcodec") in (None, "none")]
		candidates = audio or [f for f in requested if isinstance(f, dict)]
		if candidates:
			source = candidates[0]
	value = source.get("available_at")
	if isinstance(value, bool) or not isinstance(value, (int, float)):
		return None
	return int(value)


def video_id_from_url(url: str) -> str | None:
	"""
	動画の URL から動画 ID を取り出す。予備設定へ直接回す動画を見分けるのに使う。
	- watch?v=<ID>、youtu.be/<ID>、/shorts/<ID> などの形に対応する。ホストの検査は validate_url で済ませておくこと
	- ID の形 (VIDEO_ID_PATTERN) に合わなければ None (見分けられない URL は通常の経路で解決する)

	Args:
		url: 検査済みの動画の URL
	Returns:
		11 文字の動画 ID。取り出せなければ None
	Raises:
		なし
	Example:
		video_id_from_url("https://www.youtube.com/watch?v=jNQXAC9IVRw") -> "jNQXAC9IVRw"
		video_id_from_url("https://youtu.be/jNQXAC9IVRw?t=1") -> "jNQXAC9IVRw"
	"""
	try:
		parsed = urllib.parse.urlsplit(url)
		host = (parsed.hostname or "").lower()
	except ValueError:
		return None
	segments = [segment for segment in parsed.path.split("/") if segment]
	if host in SHORT_URL_HOSTS:
		candidate = segments[0] if segments else None
	elif segments[:1] == ["watch"]:
		candidate = next(iter(urllib.parse.parse_qs(parsed.query).get("v", [])), None)
	elif len(segments) >= 2 and segments[0] in VIDEO_ID_PATH_PREFIXES:
		candidate = segments[1]
	else:
		candidate = None
	if candidate is None or not VIDEO_ID_PATTERN.fullmatch(candidate):
		return None
	return candidate


def mix_playlist_id(url: str) -> str | None:
	"""
	動画 ID の無いミックスの URL なら、その再生リスト ID を返す。
	- list の値が MIX_PLAYLIST_PREFIX で始まり、v が無い URL だけが対象 (v があれば yt-dlp がそのまま取れる)
	- ホストの検査は validate_url で済ませておくこと

	Args:
		url: 検査済みの再生リストの URL
	Returns:
		ミックスの再生リスト ID。対象外なら None
	Raises:
		なし
	Example:
		mix_playlist_id("https://music.youtube.com/playlist?list=RDabc") -> "RDabc"
		mix_playlist_id("https://www.youtube.com/playlist?list=PLabc") -> None
	"""
	try:
		params = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
	except ValueError:
		return None
	if params.get("v"):
		return None
	playlist_id = next(iter(params.get("list", [])), "")
	return playlist_id if playlist_id.startswith(MIX_PLAYLIST_PREFIX) else None


def first_mix_video_id(response: Any) -> str | None:
	"""
	/next API の応答から、ミックスの先頭の曲の動画 ID を取り出す。
	- 曲の一覧 (MIX_CONTENTS_PATH) の中で、最初の MIX_VIDEO_RENDERER の動画 ID を返す (他の部品は飛ばす)
	- 応答の形が想定と違う、または動画 ID の形 (VIDEO_ID_PATTERN) でなければ None (URL に入れないため)

	Args:
		response: /next API の応答 (JSON を読んだもの)
	Returns:
		11 文字の動画 ID。取り出せなければ None
	Raises:
		なし
	Example:
		first_mix_video_id(worker.flat.get_info_extractor("YoutubeTab")._extract_response(...)) -> "TPbZ3IQZ-68"
	"""
	node: Any = response
	for key in MIX_CONTENTS_PATH:
		if not isinstance(node, dict):
			return None
		node = node.get(key)
	if not isinstance(node, list):
		return None
	for item in node:
		renderer = item.get(MIX_VIDEO_RENDERER) if isinstance(item, dict) else None
		if not isinstance(renderer, dict):
			continue
		video_id = renderer.get(MIX_VIDEO_ID_KEY)
		return video_id if isinstance(video_id, str) and VIDEO_ID_PATTERN.fullmatch(video_id) else None
	return None


def collect_entries(raw: Any, limit: int) -> tuple[list[dict[str, Any]], bool]:
	"""
	yt-dlp の一覧の要素を 1 件ずつ読み、id、title、duration だけの辞書にする。
	- 2 件目からは、要素を読む (次のページの取得を含みうる) 前に、このスレッドの持ち時間の残りを見る。
		FLAT_LIST_RESERVE_SECONDS 未満なら読むのをやめる。持ち時間が無ければ打ち切らない
	- 1 件目までは残りを見ずに読む。再生リストとミックスの最初のページは extract_info が取得済みで、
		検索は最初の要素で 1 ページ目を取るが、その時点では持ち時間がほぼ残っているため
	- 上限の件数に達したら次の要素を読まない。辞書でない要素 (None など) は数えずに飛ばす
	- 読み込みの途中の例外 (次のページの取得の失敗) はそのまま送出する

	Args:
		raw: yt-dlp の結果の entries (リスト、ジェネレータなどのイテラブル)。None なら空とみなす
		limit: 返す上限件数
	Returns:
		(要素の一覧, 持ち時間で途中までにしたら True) の組
	Raises:
		Exception: 要素の読み込みで yt-dlp が送出した例外
	Example:
		collect_entries([{"id": "a", "title": "A", "duration": 1}, None], 10) -> ([{"id": "a", ...}], False)
	"""
	entries: list[dict[str, Any]] = []
	if raw is None:
		return entries, False
	iterator = iter(raw)
	while len(entries) < limit:
		left = request_budget.remaining()
		if entries and left is not None and left < FLAT_LIST_RESERVE_SECONDS:
			return entries, True
		try:
			entry = next(iterator)
		except StopIteration:
			break
		if isinstance(entry, dict):
			entries.append({"id": entry.get("id"), "title": entry.get("title"), "duration": entry.get("duration")})
	return entries, False


class Worker:
	"""
	yt_dlp のインスタンスを保持して要求を処理する。スレッド間で共有しないこと。

	呼び出し例:
		worker = Worker(build_options(".cache", None, None))
		result = worker.handle({"op": "resolve", "url": url})
		worker.after_response()
	"""

	def __init__(self, options: dict[str, Any], stream_options: dict[str, Any] | None = None) -> None:
		"""
		インスタンスを生成する。予備設定のインスタンスも作り、作れなければ予備設定を使わない。

		Args:
			options: build_options の戻り値
			stream_options: 解決用だけに足すオプション (speedups.stream_options の戻り値)。None なら足さない
		Raises:
			ImportError: yt_dlp を読み込めない場合
		Example:
			Worker(build_options(".cache", None, None))
		"""
		import yt_dlp

		self._yt_dlp = yt_dlp
		self._stream_base = {**options, **(stream_options or {})}
		self.stream = yt_dlp.YoutubeDL({**self._stream_base, "format": DEFAULT_FORMAT})
		self.flat = yt_dlp.YoutubeDL({**options, "extract_flat": "in_playlist", "noplaylist": False})
		# format_id ごとの固定用インスタンス。同じ形式の再利用で生成コストを避ける。キーの None は既定の形式
		self._pinned: dict[str | None, Any] = {None: self.stream}
		# 最後に通信を伴う要求を処理した、または接続を温めた時刻 (_now の値)。放置の後に接続を張り直す判定に使う
		self._last_network_use: float | None = None
		# ping で放置を見つけ、応答の後に接続を張り直して温める予定があれば True
		self._rewarm_pending = False
		# 前の要求の後に、すでに一度温めたか。温めは放置 1 回につき 1 回にする
		# (温め直しを続けると、使われない間も YouTube へ定期的に通信し続けるため)
		self._rewarmed = False
		# 主経路を飛ばして予備設定へ直接回す動画。期限の判定は _now で行う (試験で時計を差し替えられるよう、呼ぶたびに引く)
		self._fallback_memory = FallbackMemory(FALLBACK_MEMORY_SIZE, clock=lambda: _now())
		# 予備設定のインスタンスは実行スレッドの中で作り、そのスレッドの辞書に形式ごとに持つ
		self._fallback_runner = BoundedRunner(FALLBACK_THREAD_NAME)
		self._fallback_base: dict[str, Any] | None = None
		try:
			fallback_base = speedups.fallback_options(self._stream_base)
			# TLS 偽装の対象が使えないと YoutubeDL の生成で失敗するため、ここで作って確かめる (作ったものは使わない)
			yt_dlp.YoutubeDL({**fallback_base, "format": DEFAULT_FORMAT})
			self._fallback_base = fallback_base
		except Exception as error:  # 予備設定が無くても通常の解決はできるため、記録して続ける
			speedups.log(f"stream_fallback: {type(error).__name__}: {error}")
		# Cookie 関連の失敗で使う、予備設定から Cookie を外した設定。Cookie を使っていなければ作らない
		self._cookieless_base: dict[str, Any] | None = None
		if self._fallback_base is not None and any(key in self._stream_base for key in COOKIE_OPTION_KEYS):
			self._cookieless_base = {
				key: value for key, value in self._fallback_base.items() if key not in COOKIE_OPTION_KEYS
			}
		self._cookieless_runner = BoundedRunner(COOKIELESS_THREAD_NAME)
		# Cookie の読み込みの失敗の型。古い yt-dlp に無ければ、その失敗は Cookie 関連として扱わない
		try:
			from yt_dlp.cookies import CookieLoadError
			self._cookie_load_error: type[BaseException] | None = CookieLoadError
		except ImportError:
			self._cookie_load_error = None
		self.yt_dlp_version = yt_dlp.version.__version__

	@property
	def fallback_ready(self) -> bool:
		"""Returns: 予備設定を使えるなら True。Example: worker.fallback_ready -> True"""
		return self._fallback_base is not None

	def prepare_fallback(self) -> bool:
		"""
		予備設定の実行スレッドへ、既定の形式のインスタンスの作成と Cookie の読み込みを投げる。終わりは待たない。
		- 起動の最後にメインスレッドから 1 回だけ呼ぶ。インスタンスは実行スレッドの中で作る (スレッド間で共有しない)
		- 予備設定を初めて使う要求で、TLS 偽装の準備と Firefox の Cookie の読み込みを待たないようにする
		- 失敗は実行スレッドが標準エラーへ記録する。最初の予備設定の要求で作り直すだけ

		Returns:
			投げたら True。予備設定を使えなければ False
		Example:
			applied["fallback_prepare"] = worker.prepare_fallback()
		"""
		base = self._fallback_base
		if base is None:
			return False
		self._fallback_runner.submit(lambda cache: self._prepare_fallback_instance(cache, base))
		return True

	def _prepare_fallback_instance(self, cache: dict[str | None, Any], base: dict[str, Any]) -> bool:
		"""
		予備設定の実行スレッドの中で、既定の形式のインスタンスを作り、Cookie を読み込ませる。
		- 例外は送出せず、標準エラーへ記録する (待つ呼び出し元がいないため)

		Args:
			cache: 実行スレッドの辞書
			base: 予備設定のオプション
		Returns:
			準備できたら True
		Example:
			self._fallback_runner.submit(lambda cache: self._prepare_fallback_instance(cache, base))
		"""
		started = time.perf_counter()
		try:
			# cookiejar は最初の参照で読み込まれる
			_ = self._instance_for(cache, base, None).cookiejar
		except Exception as error:  # 準備の失敗は最初の予備設定の要求で作り直すだけ
			speedups.log(f"fallback_prepare: {type(error).__name__}: {error}")
			return False
		speedups.log(f"fallback_prepare: ok in {time.perf_counter() - started:.2f}s")
		return True

	def _instance_for(self, cache: dict[str | None, Any], base: dict[str, Any], format_id: str | None) -> Any:
		"""
		形式に対応するインスタンスを cache から返す。無ければ base から作って入れる。

		Args:
			cache: 形式ごとのインスタンス
			base: 生成に使うオプション
			format_id: 固定する形式。None なら既定の形式
		Returns:
			YoutubeDL のインスタンス
		Example:
			self._instance_for(self._pinned, self._stream_base, "251")
		"""
		key = format_id or None
		if key not in cache:
			cache[key] = self._yt_dlp.YoutubeDL({**base, "format": key or DEFAULT_FORMAT})
		return cache[key]

	def _pinned_instance(self, format_id: str | None) -> Any:
		"""
		形式を固定した解決用のインスタンスを返す。無ければ作り、解決用 (stream) の Cookie を共有させる。
		- 新しいインスタンスは、最初の通信で Firefox の Cookie を読み直す (数百 ms)。形式の固定は再生中の URL の
			取り直しで使うため、読み込み済みの cookiejar を渡してその待ちを省く
		- cookiejar は yt-dlp の cached_property で、インスタンスの __dict__ に置くと読み込みが起きない (内部構成に依存する)
		- 共有するのはメインスレッドのインスタンスどうしに限る (cookiejar 自体はロックを持つ)
		- 共有に失敗したら記録して続ける (そのインスタンスが自分で読み込むだけ)
		- 形式は再生中の曲によって変わり、起動時に作っておくと起動が延びるため、作るのは最初の要求のとき

		Args:
			format_id: 固定する形式。None なら既定の形式 (stream そのもの)
		Returns:
			YoutubeDL のインスタンス
		Example:
			self._pinned_instance("251").extract_info(url, download=False)
		"""
		key = format_id or None
		if key in self._pinned:
			return self._pinned[key]
		instance = self._instance_for(self._pinned, self._stream_base, key)
		if self._stream_base.get("cookiesfrombrowser"):
			try:
				instance.__dict__["cookiejar"] = self.stream.cookiejar
			except Exception as error:  # 共有できなければ、そのインスタンスが最初の通信で読み込む
				speedups.log(f"pinned_cookies: {type(error).__name__}: {error}")
		return instance

	def _extract_stream(self, url: str, format_id: str | None) -> tuple[dict[str, Any], bool]:
		"""
		解決用の設定で取得する。失敗したら予備設定で 1 回だけ取り直す。予備設定を使ったことは標準エラーへ記録する。
		- 取り直すのは、最初の失敗が internal に分類されるとき、またはボット判定 (is_bot_check) のとき。
			Cookie を使っていて not_found のときも取り直す (web_music のみの解決で、実在する動画が
			Video unavailable になることがあるため)。Cookie なしの not_found は確定した失敗として取り直さない
			予備設定の TLS 偽装はボット判定を避けるためのものでもある。非公開、削除済み、年齢制限、地域制限は
			設定を変えても同じで、単なる回数制限 (HTTP 429 など) では要求を増やすと悪化するため取り直さない
		- 予備設定の取得は今の要求の持ち時間の残りで打ち切る。残りが FALLBACK_MIN_SECONDS 未満なら取り直さない
		- Cookie ありの not_found から予備設定で解決できた動画は FallbackMemory に覚え、次からは主経路を飛ばして
			予備設定へ直接回す (形式の指定によらない)。直接回した予備設定が失敗したら、その動画を忘れて失敗を返す
			(動画の状態が変わった可能性があるため、次の要求は主経路から試す)。
			覚えてから fallback.FALLBACK_MEMORY_TTL_SECONDS 秒たった動画も忘れ、主経路から試し直す
		- Cookie を使っていて、主経路の失敗が Cookie 関連 (protocol.cookie_failure、または Cookie の読み込みの失敗) なら、
			上の予備設定の代わりに、予備設定から Cookie を外した設定で 1 回だけ取り直す。判定には主経路の取得の間に
			yt-dlp が出した警告を使う。Cookie なしで解決できても覚えない (次の要求も Cookie ありから試す)

		Args:
			url: 検査済みの動画の URL
			format_id: 固定する形式。None なら DEFAULT_FORMAT
		Returns:
			yt-dlp の情報の辞書と、予備設定 (Cookie なしを含む) で取得したかどうかの組
		Raises:
			yt_dlp.utils.DownloadError: 失敗し、予備設定を使えない、使わない、または残りの秒数が足りない場合。
				直接回した予備設定が失敗した場合も、その例外をそのまま送出する
			yt_dlp.cookies.CookieLoadError: Cookie を読み込めず、Cookie なしで取り直せない (予備設定が無い、残りの秒数が足りない) 場合
			FallbackTimeoutError: 直接回した予備設定が持ち時間の残りを超えた場合
			FallbackFailedError: 予備設定でも失敗した、または持ち時間の残りを超えた場合
		Example:
			info, used_fallback = self._extract_stream(url, None)
		"""
		base = self._fallback_base
		video_id = video_id_from_url(url)
		if base is not None and self._fallback_memory.recall(video_id):
			limit = request_budget.remaining()
			if limit is None or limit >= FALLBACK_MIN_SECONDS:
				speedups.log(f"fallback direct: {video_id}")
				try:
					info = self._run_fallback(base, url, format_id, limit)
				except Exception as error:
					self._fallback_memory.forget(video_id)
					speedups.log(f"fallback direct failed: {video_id}: {str(error)[:MAX_ERROR_MESSAGE_CHARS]}")
					raise
				return info, True
		logger = self._stream_base.get("logger")
		recorder = logger.recording() if isinstance(logger, StderrLogger) else contextlib.nullcontext([])
		cookie_reason: str | None = None
		try:
			with recorder as recorded:
				return self._pinned_instance(format_id).extract_info(url, download=False), False
		except self._yt_dlp.utils.DownloadError as error:
			if self._cookieless_base is not None:
				cookie_reason = cookie_failure(recorded, str(error))
			if cookie_reason is None:
				return self._retry_with_fallback(url, format_id, video_id, error)
			first_error: Exception = error
		except Exception as error:
			# Cookie の読み込みの失敗は DownloadError に包まれずに届く。それ以外は今までどおり呼び出し元へ
			load_error = self._cookie_load_error
			if self._cookieless_base is None or load_error is None or not isinstance(error, load_error):
				raise
			cookie_reason = COOKIE_FAILURE_LOAD
			first_error = error
		return self._retry_without_cookies(url, format_id, first_error, cookie_reason)

	def _retry_without_cookies(
		self, url: str, format_id: str | None, first_error: Exception, cookie_reason: str,
	) -> tuple[dict[str, Any], bool]:
		"""
		Cookie 関連の失敗の後、予備設定から Cookie を外した設定で 1 回だけ取り直す。
		- 持ち時間の残りが FALLBACK_MIN_SECONDS 未満なら取り直さず、最初の失敗を送出する
		- 解決できても FallbackMemory には覚えない (次の要求も Cookie ありの主経路から試す)
		- _cookieless_base があるときだけ呼ぶ (無ければ first_error を送出する)

		Args:
			url: 検査済みの動画の URL
			format_id: 固定する形式。None なら DEFAULT_FORMAT
			first_error: 主経路の失敗
			cookie_reason: Cookie 関連の失敗の理由の名前 (標準エラーの記録に使う)
		Returns:
			yt-dlp の情報の辞書と True (予備設定で取得した) の組
		Raises:
			Exception: 残りの秒数が足りない場合は first_error をそのまま送出する
			FallbackFailedError: Cookie なしでも失敗した、または持ち時間の残りを超えた場合
		Example:
			return self._retry_without_cookies(url, None, error, "js_challenge")
		"""
		base = self._cookieless_base
		if base is None:
			raise first_error
		limit = request_budget.remaining()
		if limit is not None and limit < FALLBACK_MIN_SECONDS:
			speedups.log(f"cookieless skipped: {url}: {max(limit, 0.0):.1f}s left")
			raise first_error
		speedups.log(f"cookieless: {url}: {cookie_reason}: {str(first_error)[:MAX_ERROR_MESSAGE_CHARS]}")
		try:
			info = self._run_fallback(base, url, format_id, limit, self._cookieless_runner)
		except Exception as fallback_error:
			raise FallbackFailedError(first_error, fallback_error) from fallback_error
		speedups.log(f"cookieless succeeded: {url}")
		return info, True

	def _retry_with_fallback(
		self, url: str, format_id: str | None, video_id: str | None, first_error: Exception,
	) -> tuple[dict[str, Any], bool]:
		"""
		主経路の失敗 (Cookie 関連でないもの) の後、取り直す対象なら予備設定で 1 回だけ取り直す。
		- 取り直す条件と覚える条件は _extract_stream の説明のとおり

		Args:
			url: 検査済みの動画の URL
			format_id: 固定する形式。None なら DEFAULT_FORMAT
			video_id: URL から取り出した動画 ID。取り出せなければ None
			first_error: 主経路の DownloadError
		Returns:
			yt-dlp の情報の辞書と True (予備設定で取得した) の組
		Raises:
			yt_dlp.utils.DownloadError: 取り直す対象でない、予備設定を使えない、または残りの秒数が足りない場合
			FallbackFailedError: 予備設定でも失敗した、または持ち時間の残りを超えた場合
		Example:
			return self._retry_with_fallback(url, None, "jNQXAC9IVRw", error)
		"""
		base = self._fallback_base
		reason = str(first_error)
		code = classify_error(reason)
		not_found_with_cookies = code == "not_found" and bool(self._stream_base.get("cookiesfrombrowser"))
		retryable = code == "internal" or is_bot_check(reason) or not_found_with_cookies
		if base is None or not retryable:
			raise first_error
		limit = request_budget.remaining()
		if limit is not None and limit < FALLBACK_MIN_SECONDS:
			speedups.log(f"fallback skipped: {url}: {max(limit, 0.0):.1f}s left")
			raise first_error
		speedups.log(f"fallback: {url}: {str(first_error)[:MAX_ERROR_MESSAGE_CHARS]}")
		try:
			info = self._run_fallback(base, url, format_id, limit)
		except Exception as fallback_error:
			# Cookie ありの not_found は当てにならない (取り直す理由そのもの)。分類で予備設定の理由を優先させる
			raise FallbackFailedError(
				first_error, fallback_error, first_unreliable=not_found_with_cookies,
			) from fallback_error
		speedups.log(f"fallback succeeded: {url}")
		# ボット判定や internal は一時的なことが多いため覚えない (覚えると速い主経路を使わなくなる)
		if not_found_with_cookies:
			info_id = info.get("id") if isinstance(info, dict) else None
			if video_id is None and isinstance(info_id, str) and VIDEO_ID_PATTERN.fullmatch(info_id):
				video_id = info_id
			if video_id is not None:
				self._fallback_memory.remember(video_id)
		return info, True

	def _run_fallback(
		self, base: dict[str, Any], url: str, format_id: str | None, limit: float | None,
		runner: BoundedRunner | None = None,
	) -> dict[str, Any]:
		"""
		予備設定の実行スレッドで取得する。待ちは limit 秒で打ち切る。

		Args:
			base: 予備設定のオプション
			url: 検査済みの動画の URL
			format_id: 固定する形式。None なら DEFAULT_FORMAT
			limit: 待つ上限秒数 (今の要求の持ち時間の残り)。None なら終わるまで待つ
			runner: 実行するスレッド。None なら Cookie ありの予備設定のスレッド。
				インスタンスはスレッドごとに形式で持つため、base ごとに別のスレッドを渡す
		Returns:
			yt-dlp の情報の辞書
		Raises:
			FallbackTimeoutError: limit 秒以内に終わらなかった場合
			yt_dlp.utils.DownloadError: 取得に失敗した場合
		Example:
			info = self._run_fallback(self._fallback_base, url, None, request_budget.remaining())
		"""
		return (runner or self._fallback_runner).run(
			lambda cache: self._instance_for(cache, base, format_id).extract_info(url, download=False), limit,
		)

	def resolve(self, url: str, format_id: str | None) -> dict[str, Any]:
		"""
		1 曲を解決する。失敗したら予備設定で 1 回だけ取り直す。

		Args:
			url: 動画の URL
			format_id: 固定する形式。None なら DEFAULT_FORMAT
		Returns:
			RESOLVE_KEYS と filesize、expires_at、is_live、available_at、fallback を含む辞書。
			fallback は予備設定で解決したとき True (更新の検査が主経路の失敗を見分けるのに使う)
		Raises:
			yt_dlp.utils.DownloadError: 解決できず、予備設定を使えない場合
			FallbackFailedError: 予備設定でも解決できない場合
			UnsupportedUrlError: URL が YouTube のものでない場合
			ValueError: 結果が再生リストで url を持たない場合
		Example:
			worker.resolve("https://www.youtube.com/watch?v=jNQXAC9IVRw", None)["fallback"] -> False
		"""
		validate_url(url)
		info, used_fallback = self._extract_stream(url, format_id)
		if info.get("entries") is not None and not info.get("url"):
			raise ValueError("resolve に再生リストの URL が渡されました")
		result = {key: info.get(key) for key in RESOLVE_KEYS}
		# 概算 (filesize_approx) は使わない。Rust 側が byte 単位で Range を取るため、正確な値だけ返す
		# 正確な値が無ければ None を返し、Rust 側が Range 要求で全長を得る
		result["filesize"] = info.get("filesize") or None
		match = re.search(r"[?&/]expire[=/](\d+)", info.get("url") or "")
		result["expires_at"] = int(match.group(1)) if match else None
		result["is_live"] = bool(info.get("is_live"))
		result["available_at"] = selected_available_at(info)
		result["fallback"] = used_fallback
		return result

	def flat_playlist(self, url: str, limit: int) -> dict[str, Any]:
		"""
		プレイリストの ID 一覧を返す。

		Args:
			url: プレイリストの URL
			limit: 返す上限件数
		Returns:
			title、entries (id, title, duration)、truncated (持ち時間で途中までにしたら True) を含む辞書
		Raises:
			yt_dlp.utils.DownloadError: 取得できない場合
			UnsupportedUrlError: URL が YouTube のものでない場合
		Example:
			worker.flat_playlist("https://www.youtube.com/playlist?list=PL...", 100)["entries"]
		"""
		validate_url(url)
		return self._extract_list(self._mix_watch_url(url), limit)

	def _mix_watch_url(self, url: str) -> str:
		"""
		動画 ID の無いミックスの URL を、先頭の曲の動画 ID を付けた watch の URL に直す。
		- 先頭の曲は /next API で求める。yt-dlp の内部 API (_extract_response) に依存する
		- 対象外の URL、または先頭の曲を求められなければ url をそのまま返す (yt-dlp 本来のエラーになる)
		- 一覧用インスタンスを使う。メインスレッドだけが呼ぶこと
		- ミックスのときだけ通信が 1 往復増える

		Args:
			url: 検査済みの再生リストの URL
		Returns:
			一覧の取得に使う URL
		Raises:
			なし (失敗は標準エラーへ記録し、url を返す)
		Example:
			self._mix_watch_url("https://music.youtube.com/playlist?list=RDabc")
			-> "https://www.youtube.com/watch?v=TPbZ3IQZ-68&list=RDabc"
		"""
		playlist_id = mix_playlist_id(url)
		if playlist_id is None:
			return url
		try:
			extractor = self.flat.get_info_extractor(MIX_EXTRACTOR_NAME)
			response = extractor._extract_response(
				item_id=playlist_id, query={"playlistId": playlist_id},
				ep=MIX_NEXT_ENDPOINT, check_get_keys="contents",
			)
		except Exception as error:  # 通信の失敗や yt-dlp の内部の変更。元の URL で取らせる
			speedups.log(f"mix_first_video: {type(error).__name__}: {error}")
			return url
		video_id = first_mix_video_id(response)
		if video_id is None:
			speedups.log(f"mix_first_video: not found: {playlist_id}")
			return url
		return f"{MIX_WATCH_URL}?{urllib.parse.urlencode({'v': video_id, 'list': playlist_id})}"

	def _extract_list(self, target: str, limit: int) -> dict[str, Any]:
		"""
		一覧用インスタンスで取得して整形する。URL の検査は呼び出し側で済ませること。
		- yt-dlp には一覧を処理させず (process=False)、要素を 1 件ずつ読む。次のページの取得は要素を読むときに起きる
		- 要素を読む前に持ち時間の残りを見て、FLAT_LIST_RESERVE_SECONDS 未満なら読むのをやめ、
			途中までの一覧を truncated=True で返す (Rust 側は全件と扱わずにキャッシュする)
		- 上限の件数に達したら次の要素を読まない (上限より先のページを取得しない。
			全ページを取ってから切り詰めると長いプレイリストで遅い)
		- 一覧用インスタンスはメインスレッドだけが使う (共有しない)

		Args:
			target: URL または ytsearch 形式の文字列
			limit: 返す上限件数
		Returns:
			title、entries、truncated を含む辞書
		Raises:
			yt_dlp.utils.DownloadError: 取得できない場合
			ValueError: yt-dlp の結果が無い、または転送が MAX_LIST_REDIRECTS 回を超えた場合
		Example:
			self._extract_list("ytsearch1:abc", 1)
		"""
		info = self._extract_list_info(target)
		entries, truncated = collect_entries(info.get("entries"), limit)
		if truncated:
			speedups.log(f"list_truncated: {len(entries)} entries: {target}")
		return {"title": info.get("title"), "entries": entries, "truncated": truncated}

	def _extract_list_info(self, target: str) -> dict[str, Any]:
		"""
		一覧の情報を、要素を処理させずに取得する。yt-dlp が返す転送 (url、url_transparent) はたどる。
		- 転送先も一覧用インスタンスで取る。extractor は allowed_extractors の範囲に限られる (YouTube 以外へ行かない)
		- 転送先の題名が無ければ、転送元の題名を使う

		Args:
			target: URL または ytsearch 形式の文字列
		Returns:
			yt-dlp の結果の辞書 (entries は未処理のイテラブル)
		Raises:
			yt_dlp.utils.DownloadError: 取得できない場合
			ValueError: 結果が無い、または転送が MAX_LIST_REDIRECTS 回を超えた場合
		Example:
			info = self._extract_list_info("https://www.youtube.com/playlist?list=PLabc")
		"""
		info = self.flat.extract_info(target, download=False, process=False)
		for _ in range(MAX_LIST_REDIRECTS):
			if not isinstance(info, dict) or info.get("_type") not in LIST_REDIRECT_TYPES:
				break
			title = info.get("title")
			info = self.flat.extract_info(info.get("url"), download=False, ie_key=info.get("ie_key"), process=False)
			if isinstance(info, dict) and not info.get("title") and title:
				info["title"] = title
		if not isinstance(info, dict):
			raise ValueError("一覧の取得結果がありません")
		if info.get("_type") in LIST_REDIRECT_TYPES:
			raise ValueError("一覧の転送が多すぎます")
		return info

	def search(self, query: str, limit: int) -> dict[str, Any]:
		"""
		検索する。

		Args:
			query: 検索語
			limit: 取得件数
		Returns:
			flat_playlist と同じ形の辞書
		Raises:
			yt_dlp.utils.DownloadError: 取得できない場合
		Example:
			worker.search("never gonna give you up", 1)["entries"][0]["id"]
		"""
		# 検索は固定の接頭辞 ytsearch で始まる文字列を作るため、URL の検査は不要
		return self._extract_list(f"ytsearch{limit}:{query}", limit)

	def handle(self, request: dict[str, Any]) -> dict[str, Any]:
		"""
		1 件の要求を処理する。

		Args:
			request: op と各操作の引数を含む辞書
		Returns:
			result に入れる辞書
		Raises:
			ValueError: 未知の操作の場合
			KeyError: 必須の引数が無い場合
		Example:
			worker.handle({"op": "resolve", "url": url, "format_id": None})
		"""
		op = request.get("op")
		if op == "ping":
			# ping 自体は通信しない。放置を見つけたら印だけ付け、張り直しと温めは応答の後 (after_response) に行う
			# (Rust 側の死活確認を遅らせないため)
			if self._idle() and not self._rewarmed:
				self._rewarm_pending = True
			return {"pid": os.getpid(), "yt_dlp": self.yt_dlp_version}
		if self._idle():
			reset_connections(self._main_instances())
		try:
			return self._dispatch(op, request)
		finally:
			self._last_network_use = _now()
			self._rewarmed = False

	def _idle(self) -> bool:
		"""
		前の通信から IDLE_RESET_SECONDS 以上あいたかを返す。一度も通信していなければ False。

		Returns:
			放置の後なら True
		Example:
			if self._idle(): reset_connections(self._main_instances())
		"""
		return self._last_network_use is not None and _now() - self._last_network_use >= IDLE_RESET_SECONDS

	def _main_instances(self) -> list[Any]:
		"""
		メインスレッドが持つ YoutubeDL をすべて返す (形式の固定の分と一覧用)。予備設定の分は含めない。

		Returns:
			YoutubeDL のインスタンスの一覧
		Example:
			reset_connections(self._main_instances())
		"""
		return [*self._pinned.values(), self.flat]

	def after_response(self) -> None:
		"""
		応答を書き出した後にメインループが呼ぶ。ping で放置を見つけていたら、接続を張り直して温める。
		- 張り直すのは放置の判定を満たすときだけ。ping のたびに通信しない
		- 温めるのは前の要求の後に 1 回だけ。次の要求までは温め直さない
		- 温めは IDLE_REWARM_LIMIT_SECONDS 以内に収める。次の要求はメインループが読むまで待つため、温め終えてから処理する
		- 温めの成否によらず、終えた時刻を「前の通信」とする (温めた直後の要求で再び張り直さない。
			失敗しても次の要求は抽出時に接続し直すだけで、ping のたびに温め直さない)
		- 例外は送出しない (失敗は標準エラーへ記録し、次の要求は通常どおり処理する)

		Example:
			out.write(encode_response(response)); out.flush(); worker.after_response()
		"""
		if not self._rewarm_pending:
			return
		self._rewarm_pending = False
		if not self._idle():
			return
		started = _now()
		try:
			reset_connections(self._main_instances())
			targets = speedups.connection_targets(
				self.stream, self.flat, bool(self._stream_base.get("cookiesfrombrowser")))
			warmed = speedups.warm_up_connections(targets, IDLE_REWARM_LIMIT_SECONDS)
			speedups.log(f"idle_rewarm: {'ok' if warmed else 'partial'} in {_now() - started:.2f}s")
		except Exception as error:  # 温めの失敗は次の要求で接続し直すだけ。メインループは止めない
			speedups.log(f"idle_rewarm: {type(error).__name__}: {error}")
		finally:
			self._last_network_use = _now()
			self._rewarmed = True

	def mark_network_use(self) -> None:
		"""
		今を「最後に通信した時刻」として記録する。起動時の接続の準備 (speedups.warm_up) の直後に呼ぶ。
		- 準備で張った接続も放置の間に切られるため、最初の要求でも放置の判定に含める

		Example:
			worker.mark_network_use()
		"""
		self._last_network_use = _now()

	def _dispatch(self, op: Any, request: dict[str, Any]) -> dict[str, Any]:
		"""
		通信を伴う操作を実行する。

		Args:
			op: 操作の名前 (resolve、flat_playlist、search)
			request: 各操作の引数を含む辞書
		Returns:
			result に入れる辞書
		Raises:
			ValueError: 未知の操作の場合
			KeyError: 必須の引数が無い場合
		Example:
			self._dispatch("search", {"query": "abc"})
		"""
		if op == "resolve":
			return self.resolve(request["url"], request.get("format_id"))
		if op == "flat_playlist":
			return self.flat_playlist(request["url"], request.get("limit", DEFAULT_PLAYLIST_LIMIT))
		if op == "search":
			return self.search(request["query"], request.get("limit", DEFAULT_SEARCH_LIMIT))
		raise ValueError(f"unknown op: {op}")


def error_response(request_id: Any, error: Exception) -> dict[str, Any]:
	"""
	例外をプロトコルの失敗応答へ変換する。
	- 予備設定でも失敗した場合は通常の設定の理由で分類し、分類できなければ予備設定の理由で分類する
		(連結した文言のままだと、予備設定の理由が通常の設定の理由より先に一致することがあるため)
	- ただし通常の設定の理由が当てにならない (Cookie ありの not_found) ときは、予備設定の理由が
		SPECIFIC_FALLBACK_CODES のいずれかなら、そちらを使う

	Args:
		request_id: 要求の id。読み取れなかった場合は None
		error: 発生した例外
	Returns:
		ok=false の応答辞書
	Example:
		error_response(1, ValueError("x"))["error"]["code"] -> "internal"
	"""
	message = str(error)
	if isinstance(error, UnsupportedUrlError):
		code = "unsupported_url"
	elif isinstance(error, FallbackFailedError):
		code = classify_error(str(error.first))
		if code == "internal":
			code = classify_error(str(error.fallback))
		elif code == "not_found" and error.first_unreliable:
			fallback_code = classify_error(str(error.fallback))
			if fallback_code in SPECIFIC_FALLBACK_CODES:
				code = fallback_code
	else:
		code = classify_error(message)
	return {
		"id": request_id,
		"ok": False,
		"error": {"code": code, "message": message[:MAX_ERROR_MESSAGE_CHARS]},
	}


def encode_response(response: dict[str, Any]) -> bytes:
	"""
	応答を 1 行の UTF-8 (改行終端) にする。直列化に失敗したら internal の失敗応答へ置き換える。

	Args:
		response: 応答の辞書
	Returns:
		改行で終わるバイト列
	Raises:
		なし (lone surrogate、NaN や Infinity、直列化できない値は失敗応答にし、標準エラーへ記録する)
	Example:
		encode_response({"id": 1, "ok": True, "result": {}}) -> b'{"id": 1, "ok": true, "result": {}}' + 改行
	"""
	try:
		# NaN と Infinity は JSON の外の表記になり、Rust 側が応答を読めずにワーカーを再起動するため拒む
		return json.dumps(response, ensure_ascii=False, allow_nan=False).encode("utf-8") + b"\n"
	except (UnicodeEncodeError, TypeError, ValueError) as error:
		request_id = response.get("id")
		# 例外の文言には不正な文字や巨大な値が含まれうるため、型名と id だけを記録する
		speedups.log(f"encode_response: {type(error).__name__}: id={request_id!r}")
		# 失敗応答は固定文言にする。元の例外の文言に不正な文字が含まれうるため
		fallback = {
			"id": request_id if isinstance(request_id, int) else None,
			"ok": False,
			"error": {"code": "internal", "message": "response could not be encoded"},
		}
		return json.dumps(fallback).encode("utf-8") + b"\n"


def main() -> None:
	"""
	標準入力が閉じるまで要求を処理する。

	Raises:
		SystemExit: 引数が不正な場合 (argparse が送出する)
	Example:
		python worker.py --cachedir .cache --cookies firefox --premium auto --request-budget-secs 28
	"""
	parser = argparse.ArgumentParser()
	parser.add_argument("--cachedir", required=True)
	parser.add_argument("--cookies", default=None)
	parser.add_argument("--premium", choices=("auto", "yes", "no"), default="auto")
	parser.add_argument("--user-agent", default=None)
	# 1 要求の持ち時間 (秒)。Rust 側が resolve_timeout_secs から余裕を引いて渡す。省略すると打ち切らない
	parser.add_argument("--request-budget-secs", type=float, default=None)
	args = parser.parse_args()

	sys.stderr.reconfigure(encoding="utf-8", errors="backslashreplace")
	out = take_protocol_output()
	started = time.perf_counter()
	# 高速化はプロセス全体の差し替えを含むため、YoutubeDL の生成より前に適用する
	config = speedups.SpeedupConfig(cookies=args.cookies, premium=args.premium)
	applied = speedups.apply(config)
	worker = Worker(build_options(args.cachedir, args.cookies, args.user_agent), speedups.stream_options(config, applied))
	applied["stream_fallback"] = worker.fallback_ready
	applied.update(speedups.warm_up(worker.stream, worker.flat, config, applied))
	worker.mark_network_use()
	# 予備設定の準備は実行スレッドで進め、ここでは待たない (起動を延ばさない)
	applied["fallback_prepare"] = worker.prepare_fallback()
	speedups.log(json.dumps(applied))
	print(f"[worker] ready in {time.perf_counter() - started:.2f}s", file=sys.stderr, flush=True)

	try:
		for raw in sys.stdin.buffer:
			request_id = None
			# 持ち時間は要求を受け取った時点から数える
			request_budget.begin(args.request_budget_secs)
			try:
				request = json.loads(raw.decode("utf-8"))
				request_id = request.get("id")
				response = {"id": request_id, "ok": True, "result": worker.handle(request)}
			except Exception as error:  # 握りつぶさず、コード付きの応答として返す
				response = error_response(request_id, error)
			finally:
				request_budget.end()
			# json.dumps は改行を escape するため 1 行に収まる。大きな応答に備えて毎回 flush する
			out.write(encode_response(response))
			out.flush()
			# 応答を返してから、放置の後の接続の張り直しと温めを行う (次の要求はこの後に読む)
			worker.after_response()
	finally:
		speedups.shutdown(applied)


if __name__ == "__main__":
	main()
