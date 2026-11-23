"""
ワーカープロトコルの定数とエラー分類。
- 定義の正は protocol/worker-protocol.json。ここの定数は試験で一致を確かめる
- ワーカー本体 (worker.py) と試験から読み込む
"""
import re
from collections.abc import Sequence
from urllib.parse import urlparse

# 操作名。protocol.json の ops と同じ順序にする
OPS: tuple[str, ...] = ("resolve", "flat_playlist", "search", "ping")

# エラーコード。protocol.json の error_codes と同じ順序にする
ERROR_CODES: tuple[str, ...] = ("not_found", "private", "geo_blocked", "age_restricted", "rate_limited", "unsupported_url", "internal")

# ボット判定 ("Sign in to confirm you're not a bot") の文言。rate_limited の規則の一部で、
# 予備設定 (TLS 偽装) で取り直すかの判定 (is_bot_check) にも使う
BOT_CHECK_PATTERN = r"not a bot"

# 例外メッセージからエラーコードへの対応。上から順に判定する
ERROR_PATTERNS: tuple[tuple[str, str], ...] = (
	# 年齢制限の文言は "sign in to confirm your age" のため、レート制限より先に判定する
	("age_restricted", r"confirm your age|age.?restrict|inappropriate for some users"),
	("rate_limited", rf"rate.?limit|too many requests|http error 429|{BOT_CHECK_PATTERN}"),
	("private", r"private video|video is private"),
	# yt-dlp の YouTube の地域制限は "The uploader has not made this video available in your country"
	# (動画の再生画面の文言をそのまま使う)。既定の文言は "...not available from your location due to geo restriction"
	(
		"geo_blocked",
		r"not available in your country|not made this video available in your country"
		r"|not available from your location|geo.?restrict|blocked it in your country",
	),
	# 形式の指定に合う音声が無いのは動画の有無と関係が無い。not_found の "not available" より先に internal にする
	("internal", r"requested format is not available"),
	("not_found", r"video unavailable|video is unavailable|has been removed|does not exist|not available|http error 404|incomplete youtube id"),
)


def classify_error(message: str) -> str:
	"""
	例外メッセージをエラーコードへ分類する。

	Args:
		message: yt-dlp の例外メッセージ
	Returns:
		ERROR_CODES のいずれか。どれにも一致しなければ "internal"
	Raises:
		なし
	Example:
		classify_error("ERROR: Private video") -> "private"
	"""
	lowered = message.lower()
	for code, pattern in ERROR_PATTERNS:
		if re.search(pattern, lowered):
			return code
	return "internal"


def is_bot_check(message: str) -> bool:
	"""
	例外メッセージがボット判定か (rate_limited のうち BOT_CHECK_PATTERN に当たるもの)。
	- 単なる回数制限 (HTTP 429、too many requests) は含まない

	Args:
		message: yt-dlp の例外メッセージ
	Returns:
		rate_limited に分類され、ボット判定の文言を含むなら True
	Raises:
		なし
	Example:
		is_bot_check("Sign in to confirm you're not a bot") -> True
	"""
	return classify_error(message) == "rate_limited" and re.search(BOT_CHECK_PATTERN, message.lower()) is not None


# Cookie 関連の失敗の理由の名前 (標準エラーの記録に使う)
COOKIE_FAILURE_JS_CHALLENGE = "js_challenge"
COOKIE_FAILURE_INVALID = "cookies_invalid"
# JS チャレンジ (n / sig) の解読に失敗したときの yt-dlp の警告。Cookie のアカウントに配られたプレイヤーを
# 解読できないと出る (Cookie なしでは別のプレイヤーが配られ、解読できることがある)
JS_CHALLENGE_FAILURE_PATTERN = r"n challenge solving failed|signature solving failed"
# 解読に失敗して音声の形式が残らなかったときの yt-dlp のエラー
FORMAT_UNAVAILABLE_PATTERN = r"requested format is not available"
# Cookie の失効 (ブラウザでの入れ替え) を知らせる yt-dlp の警告
COOKIES_INVALID_PATTERN = r"account cookies are no longer valid"


def cookie_failure(warnings: Sequence[str], message: str) -> str | None:
	"""
	Cookie を使った取得の失敗が、Cookie が原因のものかを判定する。
	- 解読の失敗の警告があり、エラーが形式の不足なら COOKIE_FAILURE_JS_CHALLENGE
	- Cookie の失効の警告があれば COOKIE_FAILURE_INVALID。ただし単なる回数制限 (ボット判定でない rate_limited) は除く
	- Cookie を使っているかは呼び出し側で確かめること

	Args:
		warnings: その取得の間に yt-dlp が出した警告
		message: yt-dlp の例外メッセージ
	Returns:
		Cookie 関連なら理由の名前、違えば None
	Raises:
		なし
	Example:
		cookie_failure(["n challenge solving failed"], "Requested format is not available") -> "js_challenge"
	"""
	lowered = [warning.lower() for warning in warnings]
	reason = message.lower()
	if re.search(FORMAT_UNAVAILABLE_PATTERN, reason) and any(re.search(JS_CHALLENGE_FAILURE_PATTERN, w) for w in lowered):
		return COOKIE_FAILURE_JS_CHALLENGE
	if any(re.search(COOKIES_INVALID_PATTERN, w) for w in lowered):
		if classify_error(message) == "rate_limited" and not is_bot_check(message):
			return None
		return COOKIE_FAILURE_INVALID
	return None


# ワーカーが受け付ける URL のホスト。完全一致のみ (部分一致は youtube.com.evil.example を通すため使わない)
ALLOWED_HOSTS: frozenset[str] = frozenset(
	("youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com", "youtu.be")
)

# 受け付ける URL のスキーム
ALLOWED_SCHEMES: frozenset[str] = frozenset(("http", "https"))


class UnsupportedUrlError(ValueError):
	"""YouTube 以外の URL など、ワーカーが受け付けない URL。応答では unsupported_url になる"""


def validate_url(url: object) -> str:
	"""
	URL が YouTube のものか検査する。SSRF の防止が目的 (URL は利用者の入力で、ワーカーは Cookie を持つ)。

	Args:
		url: 要求に入っていた URL
	Returns:
		検査を通った URL (そのまま)
	Raises:
		UnsupportedUrlError: 文字列でない、スキームが http / https でない、
			ユーザー情報やポートを含む、ホストが ALLOWED_HOSTS に完全一致しない場合
	Example:
		validate_url("https://youtu.be/abc") -> "https://youtu.be/abc"
	"""
	if not isinstance(url, str) or not url or any(c.isspace() or ord(c) < 0x20 for c in url):
		raise UnsupportedUrlError("unsupported url")
	try:
		parsed = urlparse(url)
		host = parsed.hostname
		port = parsed.port
	except ValueError as error:
		raise UnsupportedUrlError("unsupported url") from error
	if parsed.scheme.lower() not in ALLOWED_SCHEMES or parsed.username is not None or parsed.password is not None:
		raise UnsupportedUrlError("unsupported url")
	if port is not None or host is None or host not in ALLOWED_HOSTS:
		raise UnsupportedUrlError("unsupported url")
	return url
