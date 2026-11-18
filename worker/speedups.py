"""
yt-dlp の高速化。yt-dlp の内部 API に依存するため、どれが失敗しても標準の動作のまま続ける。
- apply: プロセス全体の差し替え (プレイヤーキャッシュ、Premium 判定の固定、常駐 Deno の登録)。YoutubeDL の生成より前に呼ぶ
- stream_options: 解決用の YoutubeDL に足すオプション (Cookie があるときだけ web_music と player_skip)
- fallback_options: 解決の失敗後に 1 回だけ使う予備設定 (extractor_args を外し、Chrome の TLS 偽装を付ける)
- warm_up: 起動時の準備 (Cookie の読み込み、接続の確立、古いプレイヤーキャッシュの掃除、常駐 Deno の準備)
- shutdown: 常駐 Deno を止める
- 各機能は独立に試し、例外は標準エラーへ記録して False を返す。標準出力には何も書かない

出典: NEONS-DESIGN/SatouSioBOT のコミット ed502a6 の module/extractor.py と module/options.py を移植した
MIT License, Copyright (c) 2026 NEONS-DESIGN

呼び出し例:
	config = SpeedupConfig(cookies="firefox", premium="auto")
	applied = apply(config)
	stream = YoutubeDL({**options, **stream_options(config, applied)})
	applied.update(warm_up(stream, flat, config, applied))
"""
import shutil
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable

# プレイヤーキャッシュの補助は resident_jsc と共有するため別のモジュールに置く。従来の名前でも参照できるようにする
from player_cache import PLAYER_CACHE_KEY_PREFIX, player_cache_files  # noqa: F401

# --premium の値。auto は yt-dlp の自動判定 (初期データの取得が要る) に任せる
PREMIUM_AUTO = "auto"
PREMIUM_YES = "yes"
PREMIUM_NO = "no"

# 常駐 Deno に使う実行ファイルの名前。yt-dlp の標準の Deno プロバイダと同じく PATH から探す
DENO_COMMAND = "deno"

# 前処理済みプレイヤーのキャッシュを残す版数 (1 版あたり約 4MB。YouTube がプレイヤーを更新するたびに増える。出典の固定値)
PLAYER_CACHE_KEEP = 2

# Cookie があるときの解決用の yt-dlp の設定 (ログイン Cookie が前提。webpage と configs の取得を省く。出典の固定値)
COOKIE_PLAYER_CLIENTS = ("web_music",)
COOKIE_PLAYER_SKIP = ("webpage", "configs")
# Premium の判定を固定したときに省く初期データ (/next API) を表す player_skip の値
SKIP_INITIAL_DATA = "initial_data"

# 予備設定の TLS 偽装の対象 (出典の固定値)
FALLBACK_IMPERSONATE = "chrome"

# 起動時に接続を確立しておく軽い URL。一覧用は www、解決用は Cookie があれば web_music の API も使う (出典の固定値)
WARMUP_URL = "https://www.youtube.com/generate_204"
WARMUP_MUSIC_URL = "https://music.youtube.com/generate_204"
# 接続の確立を待つ上限秒数。起動を待たせすぎないよう socket_timeout より短くする
WARMUP_TIMEOUT_SECONDS = 3
# 常駐 Deno の準備 (起動、スクリプトの評価、プレイヤーの読み込み) を待つ上限秒数
# 準備の間は標準入力を読まず、Rust 側は起動の ping への応答を worker.startup_timeout_secs (設定例では 60 秒) まで待つ。
# Cookie の読み込みと接続の確立も同じ待ちに入るため、その上限より十分短くする
RESIDENT_WARMUP_TIMEOUT_SECONDS = 10

# 標準エラーへ出す記録の接頭辞
LOG_PREFIX = "[speedups]"


@dataclass(frozen=True)
class SpeedupConfig:
	"""
	高速化の設定。Rust 側が config から作る CLI 引数をそのまま入れる。

	Attributes:
		cookies: Cookie を読むブラウザの名前 ("firefox" など)。使わないなら None
		premium: "yes" / "no" / "auto"
	Example:
		SpeedupConfig(cookies="firefox", premium="auto")
	"""

	cookies: str | None
	premium: str


def log(message: str) -> None:
	"""
	高速化の記録を標準エラーへ 1 行出す。

	Args:
		message: 本文
	Returns:
		なし
	Raises:
		なし
	Example:
		log("player_cache: 失敗")
	"""
	print(f"{LOG_PREFIX} {message}", file=sys.stderr, flush=True)


def _attempt(name: str, action: Callable[[], bool]) -> bool:
	"""
	1 つの高速化を試し、成否を返す。例外は記録して False にする (他の高速化を止めないため)。

	Args:
		name: 機能名 (記録に使う)
		action: 成功なら True、適用できなければ False を返す処理
	Returns:
		適用できたら True
	Raises:
		なし
	Example:
		_attempt("player_cache", _enable_player_cache)
	"""
	try:
		return bool(action())
	except Exception as error:  # yt-dlp の内部構成の変化などは標準の動作に戻すだけで、起動は止めない
		log(f"{name}: {type(error).__name__}: {error}")
		return False


def _enable_player_cache() -> bool:
	"""
	yt-dlp の前処理済みプレイヤーのキャッシュを有効にする。既定では解読のたびに Deno がプレイヤーを前処理し直す。
	- yt-dlp は古い版を消さないため、warm_up で掃除する

	Returns:
		差し替えたら True。差し替え先が無ければ False
	Raises:
		ImportError: yt-dlp の内部構成が変わった場合 (呼び出し側の _attempt が False にする)
	Example:
		_enable_player_cache() -> True
	"""
	from yt_dlp.extractor.youtube.jsc._builtin.ejs import EJSBaseJCP

	if not hasattr(EJSBaseJCP, "_ENABLE_PREPROCESSED_PLAYER_CACHE"):
		return False
	EJSBaseJCP._ENABLE_PREPROCESSED_PLAYER_CACHE = True
	return True


def _pin_premium_status(premium: str) -> bool:
	"""
	yt-dlp の YouTube Premium の判定を設定値で固定する。auto なら何もしない。
	- 固定すると判定に使う初期データ (/next API) が不要になり、stream_options が取得を省く
	- ログインしていなければ yes でも Premium として扱わない (yt-dlp の判定と同じ)

	Args:
		premium: "yes" / "no" / "auto"
	Returns:
		固定したら True。auto、解釈できない値、差し替え先が無い場合は False
	Raises:
		ImportError: yt-dlp の内部構成が変わった場合 (呼び出し側の _attempt が False にする)
	Example:
		_pin_premium_status("yes") -> True
	"""
	if premium not in (PREMIUM_YES, PREMIUM_NO):
		return False
	from yt_dlp.extractor.youtube import YoutubeIE

	if not callable(getattr(YoutubeIE, "_is_premium_subscriber", None)):
		return False
	is_premium = premium == PREMIUM_YES

	def _is_premium_subscriber(self: Any, initial_data: Any) -> bool:
		"""固定した判定。Args: initial_data は使わない。Returns: Premium として扱うなら True"""
		return is_premium and bool(self.is_authenticated)

	YoutubeIE._is_premium_subscriber = _is_premium_subscriber
	return True


def _register_resident_jsc() -> bool:
	"""
	常駐 Deno の JS チャレンジプロバイダを yt-dlp に登録する。YoutubeDL の生成より前に呼ぶ。
	- Deno が PATH に無ければ登録しない (標準のプロバイダも Deno を使えないため、yt-dlp の既定の動作に任せる)
	- resident_jsc の import でプロバイダが登録される

	Returns:
		登録したら True。Deno が無ければ False
	Raises:
		ImportError: yt-dlp の内部構成が変わった場合 (呼び出し側の _attempt が False にする)
	Example:
		_register_resident_jsc() -> True
	"""
	if shutil.which(DENO_COMMAND) is None:
		log(f"resident_jsc: {DENO_COMMAND} が PATH にありません")
		return False
	import resident_jsc  # noqa: F401  import で登録される

	return True


def apply(config: SpeedupConfig) -> dict[str, bool]:
	"""
	プロセス全体に効く高速化を適用する。YoutubeDL の生成より前に 1 回だけ呼ぶ。

	Args:
		config: 高速化の設定
	Returns:
		機能名と適用の成否 (player_cache、premium_pin、resident_jsc)
	Raises:
		なし (各機能の失敗は False にする)
	Example:
		apply(SpeedupConfig(cookies=None, premium="auto")) -> {"player_cache": True, "premium_pin": False, "resident_jsc": True}
	"""
	return {
		"player_cache": _attempt("player_cache", _enable_player_cache),
		"premium_pin": _attempt("premium_pin", lambda: _pin_premium_status(config.premium)),
		"resident_jsc": _attempt("resident_jsc", _register_resident_jsc),
	}


def stream_options(config: SpeedupConfig, applied: dict[str, bool]) -> dict[str, Any]:
	"""
	解決用の YoutubeDL に足すオプションを返す。
	- Cookie があるときだけ web_music を使い、webpage と configs の取得を省く (ログイン Cookie が前提)
	- Premium の判定を固定できたときだけ初期データの取得も省く。固定できなければ省かない
		(初期データが無いと Premium と判定されず、高音質の形式を選ばなくなるため)

	Args:
		config: 高速化の設定
		applied: apply の戻り値
	Returns:
		足すオプションの辞書。Cookie が無ければ空
	Raises:
		なし
	Example:
		stream_options(SpeedupConfig("firefox", "auto"), applied)["extractor_args"]["youtube"]["player_client"] -> ["web_music"]
	"""
	if not config.cookies:
		return {}
	player_skip = list(COOKIE_PLAYER_SKIP)
	if applied.get("premium_pin"):
		player_skip.append(SKIP_INITIAL_DATA)
	return {
		"extractor_args": {
			"youtube": {"player_client": list(COOKIE_PLAYER_CLIENTS), "player_skip": player_skip},
		},
	}


def fallback_options(options: dict[str, Any]) -> dict[str, Any]:
	"""
	解決に失敗した後に使う予備設定を返す。extractor_args を外し (yt-dlp の既定のクライアント)、Chrome の TLS 偽装を付ける。

	Args:
		options: 解決用のオプション
	Returns:
		予備設定の辞書。Cookie などの他の設定は引き継ぐ
	Raises:
		ImportError: yt-dlp に TLS 偽装の API が無い場合
	Example:
		fallback_options({"quiet": True, "extractor_args": {}}) -> {"quiet": True, "impersonate": ImpersonateTarget(...)}
	"""
	from yt_dlp.networking.impersonate import ImpersonateTarget

	result = {key: value for key, value in options.items() if key != "extractor_args"}
	result["impersonate"] = ImpersonateTarget.from_str(FALLBACK_IMPERSONATE)
	return result


def _prune_player_cache(ydl: Any) -> bool:
	"""
	前処理済みプレイヤーのキャッシュを、新しい PLAYER_CACHE_KEEP 版だけ残して消す。

	Args:
		ydl: キャッシュの場所を持つ YoutubeDL
	Returns:
		すべて消せたら True
	Raises:
		ImportError / AttributeError: player_cache_files と同じ (呼び出し側の _attempt が False にする)
	Example:
		_prune_player_cache(ydl) -> True
	"""
	succeeded = True
	for path in player_cache_files(ydl)[PLAYER_CACHE_KEEP:]:
		try:
			# 他のワーカーが同時に消した場合も成功とみなす
			path.unlink(missing_ok=True)
		except OSError as error:
			log(f"player_cache_prune: {path.name}: {error}")
			succeeded = False
	return succeeded


def connection_targets(stream: Any, flat: Any, cookies: bool) -> list[tuple[Any, tuple[str, ...]]]:
	"""
	接続を温める YoutubeDL と URL の組を返す。解決用は Cookie があれば web_music の API のホストも温める。

	Args:
		stream: 解決用の YoutubeDL
		flat: 一覧用の YoutubeDL
		cookies: Cookie を使うなら True
	Returns:
		warm_up_connections に渡す一覧
	Raises:
		なし
	Example:
		connection_targets(worker.stream, worker.flat, True)
	"""
	stream_urls = (WARMUP_URL, WARMUP_MUSIC_URL) if cookies else (WARMUP_URL,)
	return [(stream, stream_urls), (flat, (WARMUP_URL,))]


def warm_up_connections(targets: list[tuple[Any, tuple[str, ...]]], total_limit: float | None = None) -> bool:
	"""
	Cookie を読み込み、各 URL のホストへの接続を確立しておく。接続は YoutubeDL ごとに使い回される。
	- 失敗しても最初の要求で読み込み直し、接続し直すだけなので、記録して続ける
	- 1 URL ごとの上限は WARMUP_TIMEOUT_SECONDS。total_limit があれば合計の残りで切り詰め、使い切ったら残りの URL は飛ばす
	- YoutubeDL を作ったスレッドで呼ぶ (スレッド間で共有しないため)

	Args:
		targets: YoutubeDL と、接続しておく URL の組の一覧
		total_limit: 合計の上限秒数。None なら URL ごとの上限だけ
	Returns:
		すべて成功したら True (飛ばした URL があれば False)
	Raises:
		ImportError: yt-dlp に Request が無い場合 (起動時は呼び出し側の _attempt が False にする)
	Example:
		warm_up_connections([(ydl, (WARMUP_URL,))]) -> True
		warm_up_connections(connection_targets(stream, flat, True), 3.0) -> True
	"""
	from yt_dlp.networking import Request

	deadline = None if total_limit is None else time.monotonic() + total_limit
	succeeded = True
	for ydl, urls in targets:
		try:
			# cookiejar は最初の参照で読み込まれる。最初の要求の待ちを減らすため、ここで読ませる
			_ = ydl.cookiejar
		except Exception as error:  # Cookie の読み込みの失敗は抽出時に再試行される
			log(f"connection_warmup: cookie: {type(error).__name__}: {error}")
			succeeded = False
		for url in urls:
			timeout = WARMUP_TIMEOUT_SECONDS if deadline is None else min(WARMUP_TIMEOUT_SECONDS, deadline - time.monotonic())
			if timeout <= 0:
				log(f"connection_warmup: {url}: skipped (time limit)")
				succeeded = False
				continue
			try:
				with ydl.urlopen(Request(url, extensions={"timeout": timeout})) as response:
					response.read()
			except Exception as error:  # 接続の失敗は抽出時に接続し直すだけ
				log(f"connection_warmup: {url}: {type(error).__name__}: {error}")
				succeeded = False
	return succeeded


def _warm_up_resident_jsc(ydl: Any, applied: dict[str, bool]) -> bool:
	"""
	常駐 Deno を起動し、キャッシュにある最新のプレイヤーを読み込ませる。

	Args:
		ydl: 解決用の YoutubeDL
		applied: apply の戻り値
	Returns:
		準備できたら True。常駐 Deno が無い、キャッシュが無い場合は False
	Raises:
		なし (resident_jsc.warm_up が失敗を False にする)
	Example:
		_warm_up_resident_jsc(stream, applied) -> True
	"""
	if not applied.get("resident_jsc"):
		return False
	import resident_jsc

	return resident_jsc.warm_up(ydl, RESIDENT_WARMUP_TIMEOUT_SECONDS)


def warm_up(stream: Any, flat: Any, config: SpeedupConfig, applied: dict[str, bool]) -> dict[str, bool]:
	"""
	起動時の準備をする。最初の要求の待ちを減らす。YoutubeDL を作ったスレッドで呼ぶ。
	- 掃除は常駐 Deno の準備より前に行う (準備で読むのは最新の版のため、消す対象と重ならない)

	Args:
		stream: 解決用の YoutubeDL
		flat: 一覧用の YoutubeDL
		config: 高速化の設定
		applied: apply の戻り値
	Returns:
		機能名と成否 (connection_warmup、player_cache_prune、resident_jsc_warmup)
	Raises:
		なし (各機能の失敗は False にする)
	Example:
		warm_up(worker.stream, worker.flat, config, applied)["connection_warmup"] -> True
	"""
	return {
		"connection_warmup": _attempt(
			"connection_warmup", lambda: warm_up_connections(connection_targets(stream, flat, bool(config.cookies)))
		),
		"player_cache_prune": _attempt("player_cache_prune", lambda: _prune_player_cache(stream)),
		"resident_jsc_warmup": _attempt("resident_jsc_warmup", lambda: _warm_up_resident_jsc(stream, applied)),
	}


def shutdown(applied: dict[str, bool]) -> None:
	"""
	常駐 Deno を止める。ワーカーの終了時に呼ぶ。

	Args:
		applied: apply の戻り値
	Returns:
		なし
	Raises:
		なし (失敗は記録するだけ。プロセスの終了でパイプが閉じ、Deno も終了する)
	Example:
		shutdown(applied)
	"""
	if not applied.get("resident_jsc"):
		return

	def stop() -> bool:
		"""常駐 Deno を止める。Returns: 常に True"""
		import resident_jsc

		resident_jsc.shutdown()
		return True

	_attempt("shutdown", stop)
