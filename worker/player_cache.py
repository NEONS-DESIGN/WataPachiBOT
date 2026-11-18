"""
yt-dlp の前処理済みプレイヤーのキャッシュの場所と名前の扱い。
- speedups (古い版の掃除) と resident_jsc (最新の版の読み込み) の両方が使う。
	どちらからも import できるよう、この 2 つを import しない
- yt-dlp の内部 API (EJSBaseJCP のキャッシュの区画名) は関数の中で import する。
	import するだけでは yt-dlp を読み込まない (ワーカーの起動を遅らせないため)

呼び出し例:
	files = player_cache_files(ydl)
	key = f"{PLAYER_CACHE_KEY_PREFIX}{player_url}"
"""
import pathlib
import urllib.parse
from typing import Any

# 前処理済みプレイヤーを yt-dlp のキャッシュに保存するキーの接頭辞 (キーは "player:<プレイヤーの URL>")
# yt-dlp の EJSBaseJCP が使うキーと同じ値 (出典の固定値)
PLAYER_CACHE_KEY_PREFIX = "player:"


def player_cache_files(ydl: Any) -> list[pathlib.Path]:
	"""
	yt-dlp のキャッシュにある前処理済みプレイヤーのファイルを、更新日時の新しい順に返す。
	- yt-dlp はキーを URL エンコードし、% を , に置き換えたファイル名で保存する

	Args:
		ydl: キャッシュの場所を持つ YoutubeDL
	Returns:
		ファイルの一覧。キャッシュが無い、読めない場合は空
	Raises:
		ImportError: yt-dlp の内部構成が変わった場合
		AttributeError: YoutubeDL にキャッシュの API が無い場合
	Example:
		player_cache_files(ydl)[0].name -> "player,3Ahttps,3A,2F,2F....json"
	"""
	from yt_dlp.extractor.youtube.jsc._builtin.ejs import EJSBaseJCP

	prefix = urllib.parse.quote(PLAYER_CACHE_KEY_PREFIX, safe="").replace("%", ",")
	cache_dir = pathlib.Path(ydl.cache._get_root_dir()) / EJSBaseJCP._CACHE_SECTION
	try:
		return sorted(cache_dir.glob(f"{prefix}*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
	except OSError:
		# 他のワーカーが一覧の途中で消した場合など。次の起動で掃除し直す
		return []
