"""
yt_dlp の試験用スタブ。ネットワークに触れない。
- URL の文字列で振る舞いを切り替える
	- "stray": extract_info の中で fd 1 と print に書き込む
	- "newline": 改行と絵文字を含む題名を返す
	- "surrogate": 単独のサロゲートを含む題名を返す
	- "big": 50 万文字の題名を返す
	- "approx": filesize を持たず filesize_approx だけを返す
	- "nan-duration": duration に NaN (JSON にできない値) を返す
	- "/error:<URL エンコードした文言>": DownloadError を送出する
	- "&list=": watch ページのミックスとして一覧を返す
- get_info_extractor("YoutubeTab")._extract_response は /next API の代用。再生リスト ID で振る舞いを切り替える
	- "RDfail" で始まる: DownloadError を送出する
	- "RDempty" で始まる: 曲の無い応答を返す
	- それ以外: 先頭の曲が MIX_SEED_VIDEO_ID の応答を返す
- 環境変数 STUB_CALL_LOG のファイルへ extract_info が受け取った URL を追記する
- 環境変数 STUB_PARAMS_LOG のファイルへ、extract_info の呼び出し時の URL と process を JSON で 1 行ずつ追記する
- 返す format_id は YoutubeDL に渡された format の値 (format_id の適用を確かめるため)
"""
import json
import os
from urllib.parse import unquote
from types import SimpleNamespace

version = SimpleNamespace(__version__="0000.00.00-stub")

# /next API の代用が返すミックスの先頭の曲の動画 ID
MIX_SEED_VIDEO_ID = "seedvideo01"


class utils:
	"""yt_dlp.utils の代用"""

	class DownloadError(Exception):
		"""yt-dlp の取得失敗を表す例外"""


class YoutubeDL:
	"""YoutubeDL の代用。params に渡された辞書を保持する"""

	def __init__(self, params: dict) -> None:
		"""Args: params は YoutubeDL のオプション"""
		self.params = params

	def get_info_extractor(self, name: str) -> "_TabExtractor":
		"""Args: name は extractor の名前 (無視する)。Returns: /next API の代用を持つ extractor"""
		return _TabExtractor()

	def extract_info(self, url: str, download: bool = False, ie_key: str | None = None, process: bool = True) -> dict:
		"""
		URL に応じた固定の応答を返す。

		Args:
			url: 試験用の URL
			download: 無視する
			ie_key: 無視する
			process: 無視する (一覧の要素は常にリストで返す)
		Returns:
			動画またはプレイリストの辞書
		Raises:
			utils.DownloadError: URL が "error:" で始まる場合
		"""
		log_path = os.environ.get("STUB_CALL_LOG")
		if log_path:
			with open(log_path, "a", encoding="utf-8") as log:
				log.write(url + "\n")
		params_path = os.environ.get("STUB_PARAMS_LOG")
		if params_path:
			with open(params_path, "a", encoding="utf-8") as log:
				log.write(json.dumps({"url": url, "process": process}) + "\n")
		if "/error:" in url:
			raise utils.DownloadError(unquote(url.split("/error:", 1)[1]))
		if "stray" in url:
			print("stray-print")
			os.write(1, b"stray-fd1\n")
		if url.startswith("ytsearch") or "playlist" in url or "&list=" in url:
			return {
				"title": "list",
				"entries": [
					{"id": "a", "title": "A", "duration": 1},
					None,
					{"id": "b", "title": "B", "duration": 2},
					{"id": "c", "title": "C", "duration": 3},
				],
			}
		if "big" in url:
			title = "x" * 500_000
		elif "surrogate" in url:
			title = "a\ud800b"
		elif "newline" in url:
			title = "line1\nline2 \U0001F3B5"
		else:
			title = "title"
		return {
			"id": "vid",
			"title": title,
			"duration": float("nan") if "nan-duration" in url else 262,
			"thumbnail": "t",
			"webpage_url": url,
			"url": "https://example.invalid/a?expire=1793000000",
			"http_headers": {},
			"ext": "webm",
			"acodec": "opus",
			"protocol": "https",
			"format_id": self.params.get("format"),
			"abr": 130.0,
			"filesize": None if "approx" in url else 3946490,
			"filesize_approx": 999,
			"is_live": "live" in url,
		}


class _TabExtractor:
	"""YoutubeTab の extractor の代用。/next API の応答だけを返す"""

	def _extract_response(self, item_id: str, query: dict, ep: str, check_get_keys: str) -> dict:
		"""
		/next API の応答の代用を返す。

		Args:
			item_id: 再生リスト ID
			query: 送る問い合わせ (無視する)
			ep: API の名前 (無視する)
			check_get_keys: 応答に要るキー (無視する)
		Returns:
			web クライアントの /next の応答と同じ形の辞書
		Raises:
			utils.DownloadError: item_id が "RDfail" で始まる場合
		"""
		if item_id.startswith("RDfail"):
			raise utils.DownloadError("stub next failure")
		contents: list[dict] = []
		if not item_id.startswith("RDempty"):
			contents = [
				{"automixPreviewVideoRenderer": {}},
				{"playlistPanelVideoRenderer": {"videoId": MIX_SEED_VIDEO_ID}},
				{"playlistPanelVideoRenderer": {"videoId": "secondvid01"}},
			]
		return {"contents": {"twoColumnWatchNextResults": {"playlist": {"playlist": {"contents": contents}}}}}
