"""
WataPachiBOT の通信を OS 側で優先させる設定を切り替えるツール (Windows 専用・要管理者権限)。

使い方 (通常は tools\\net_priority.bat から呼ぶ。バッチが管理者への昇格と Python の選択を行う):
	python tools\\net_priority.py on      優先設定を有効にする
	python tools\\net_priority.py off     有効にする前の状態に戻す
	python tools\\net_priority.py status  現在の設定を表示する
	python tools\\net_priority.py check   Bot の通信に DSCP が付いているかを実際のパケットで確かめる
	引数なしで起動するとメニューを表示する。

有効にすると次を行う。
	- Bot 本体 (bin\\watapachi-bot.exe) とワーカーの python の通信に、QoS ポリシーで DSCP を付ける
	- ドメイン不参加の PC でも DSCP が付くよう Tcpip\\QoS の "Do not use NLA" を設定する
	- Windows Update などの配信の最適化 (Delivery Optimization) が使う帯域を制限する
DSCP はルーター側が対応していれば優先されるが、配信の最適化の帯域制限は PC の中で完結するため確実に効く。
旧 Bot (SatouSioBOT) の同名のツールの設定が残っていれば、on と off の最初に引き継ぐ (こちらの設定を優先する)。
標準ライブラリのみで動くため、ワーカーの環境の python でも、別の Python でも実行できる。
OS の設定の読み書きに使う PowerShell は、pwsh (7 系) があればそれを、無ければ Windows PowerShell 5.1 を使う。
"""

import argparse
import ctypes
import ipaddress
import json
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import time
import tomllib
import winreg
from collections import Counter
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent.parent
BOT_EXE_RELATIVE = Path("bin") / "watapachi-bot.exe"

# 設定ファイルと、ワーカーの環境の既定の場所 (config.example.toml の [paths] と同じ値)
CONFIG_FILE_NAME = "config.toml"
DEFAULT_WORKER_ENVS = "worker/envs"
DEFAULT_WORKER_PYTHON = "worker/.venv/Scripts/python.exe"
CURRENT_FILE_NAME = "current.json"
VENV_PYTHON_RELATIVE = Path("Scripts") / "python.exe"
PYVENV_CFG_NAME = "pyvenv.cfg"

PROGRAM_DATA = Path(os.environ.get("ProgramData", r"C:\ProgramData"))
STATE_FILE = PROGRAM_DATA / "WataPachiBOT" / "net_priority.json"
STATE_VERSION = 1
POLICY_PREFIX = "WataPachiBOT-"

# 旧 Bot (SatouSioBOT) の同名のツール。退避ファイルの形式とレジストリの項目は同じ
OLD_STATE_FILE = PROGRAM_DATA / "SatouSioBOT" / "net_priority.json"
OLD_POLICY_PREFIX = "SatouSioBOT-"
BROKEN_SUFFIX = ".broken"

# EF (Expedited Forwarding): 音声通話向けの最優先クラス
DSCP_BOT = 46
# AF41: 動画・音声ストリーミング向けのクラス
DSCP_WORKER = 34

QOS_REG_KEY = r"SYSTEM\CurrentControlSet\Services\Tcpip\QoS"
NLA_VALUE_NAME = "Do not use NLA"
NLA_VALUE = "1"

DO_REG_KEY = r"SOFTWARE\Policies\Microsoft\Windows\DeliveryOptimization"
# Get-DOConfig の項目名と表示名 (サービスが実際に使っている値)
DO_EFFECTIVE_FIELDS = {
	"DownBackLimitPct": "裏のダウンロードの上限",
	"DownloadForegroundLimitPct": "手動のダウンロードの上限",
}
# 回線の空き帯域に対する上限 (%)。0 は制限なし (OS の自動調整)
DO_LIMITS = {
	"DOPercentageMaxBackgroundBandwidth": 10,
	"DOPercentageMaxForegroundBandwidth": 50,
}

# PowerShell 7 (Store 版は %LOCALAPPDATA%\Microsoft\WindowsApps の別名) を優先し、無ければ Windows PowerShell 5.1 を使う
PWSH_NAME = "pwsh"
WINDOWS_POWERSHELL = "powershell"
PS_TIMEOUT_SEC = 120
CHECK_SECONDS = 5
CAPTURE_BYTES = 96

PROTO_TCP = 6
PROTO_UDP = 17
ETHERTYPE_VLAN = 0x8100
ETHERTYPE_IPV4 = 0x0800
ETHERTYPE_IPV6 = 0x86DD
ETHERTYPE_OFFSET = 12
IPV4_MIN_HEADER_LEN = 20
VLAN_TAG_LEN = 4
IPV6_HEADER_LEN = 40
PCAPNG_SHB = 0x0A0D0D0A
PCAPNG_EPB = 6
PCAPNG_BOM_LE = b"\x4d\x3c\x2b\x1a"
PCAPNG_EPB_DATA_OFFSET = 28
PCAPNG_EPB_CAPLEN_OFFSET = 20
PCAPNG_MIN_BLOCK_LEN = 12

REG_VIEW = winreg.KEY_WOW64_64KEY
# レジストリの型ごとに winreg が返す Python の型
REG_PY_TYPES = {winreg.REG_SZ: str, winreg.REG_DWORD: int}
# PowerShell が単一引用符として扱う文字 (' と U+2018 から U+201B)
PS_SINGLE_QUOTES = ("'", "\u2018", "\u2019", "\u201a", "\u201b")


class ToolError(Exception):
	"""利用者に理由を伝えて処理を止めるための例外。"""


# ---------------------------------------------------------------------------
# 共通
# ---------------------------------------------------------------------------


def _decode(raw: bytes) -> str:
	"""
	外部コマンドの出力を文字列にする。

	- UTF-8 で読めなければ cp932 で読む (読めない文字は置き換える)

	Args:
		raw: 外部コマンドの出力。
	Returns:
		文字列。
	Example:
		text = _decode(result.stdout)
	"""
	try:
		return raw.decode("utf-8")
	except UnicodeDecodeError:
		return raw.decode("cp932", errors="replace")


def _ps_quote(text: str) -> str:
	"""
	PowerShell の単一引用符の文字列にする。

	- PowerShell は ‘ ’ ‚ ‛ も単一引用符として扱うため、それらも重ねる

	Args:
		text: 引用する文字列。
	Returns:
		引用符で囲んだ文字列。
	Example:
		_ps_quote("C:\\a'b")  # -> "'C:\\a''b'"
	"""
	for quote in PS_SINGLE_QUOTES:
		text = text.replace(quote, quote * 2)
	return "'" + text + "'"


def powershell_exe() -> str:
	"""
	スクリプトの実行に使う PowerShell を返す。

	- PATH に pwsh (PowerShell 7) があればそれを、無ければ Windows PowerShell 5.1 を使う
	- Store 版の pwsh は、利用者の WindowsApps にある実行の別名で見つかる

	Returns:
		pwsh の絶対パス、または WINDOWS_POWERSHELL。
	Example:
		subprocess.run([powershell_exe(), "-NoProfile", "-Command", "1"])
	"""
	return shutil.which(PWSH_NAME) or WINDOWS_POWERSHELL


def run_ps(script: str) -> str:
	"""
	PowerShell でスクリプトを実行して標準出力を返す。

	- powershell_exe() の PowerShell で実行する
	- エラーで止まる設定 ($ErrorActionPreference = 'Stop') で実行し、PS_TIMEOUT_SEC 秒で打ち切る

	Args:
		script: 実行するスクリプト。
	Returns:
		標準出力。
	Raises:
		ToolError: PowerShell が無い、時間切れ、終了コードが 0 以外のとき。
	Example:
		run_ps("Remove-NetQosPolicy -Name 'x' -Confirm:$false")
	"""
	command = (
		"[Console]::OutputEncoding = [Text.Encoding]::UTF8; "
		"$ErrorActionPreference = 'Stop'; "
		"$ProgressPreference = 'SilentlyContinue'; "
		+ script
	)
	try:
		result = subprocess.run(
			[powershell_exe(), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", command],
			capture_output=True,
			timeout=PS_TIMEOUT_SEC,
		)
	except FileNotFoundError as e:
		raise ToolError("PowerShell が見つかりません。") from e
	except subprocess.TimeoutExpired as e:
		raise ToolError(f"PowerShell の処理が {PS_TIMEOUT_SEC} 秒で終わりませんでした。") from e
	out = _decode(result.stdout)
	if result.returncode != 0:
		detail = _decode(result.stderr).strip() or out.strip()
		raise ToolError(f"PowerShell の実行に失敗しました: {detail}")
	return out


def run_ps_json(script: str) -> list[dict]:
	"""
	PowerShell の結果を JSON の配列で受け取る。

	Args:
		script: オブジェクトの列を出力するスクリプト。
	Returns:
		オブジェクトごとの辞書のリスト。出力が無ければ空。
	Raises:
		ToolError: 実行の失敗、出力を JSON として読めないとき。
	Example:
		rows = run_ps_json("Get-NetIPAddress | Select-Object IPAddress")
	"""
	out = run_ps(f"ConvertTo-Json -Compress -Depth 3 -InputObject @({script})").strip()
	if not out:
		return []
	try:
		data = json.loads(out)
	except ValueError as e:
		raise ToolError(f"PowerShell の出力を読み取れませんでした: {out[:200]}") from e
	return data if isinstance(data, list) else [data]


def run_cmd(args: list[str]) -> tuple[int, str]:
	"""
	外部コマンドを実行する。

	Args:
		args: コマンドと引数。
	Returns:
		(終了コード, 標準出力と標準エラーをつないだ文字列)。
	Raises:
		ToolError: コマンドが無い、PS_TIMEOUT_SEC 秒で終わらないとき。
	Example:
		code, out = run_cmd(["pktmon", "stop"])
	"""
	try:
		result = subprocess.run(args, capture_output=True, timeout=PS_TIMEOUT_SEC)
	except FileNotFoundError as e:
		raise ToolError(f"{args[0]} が見つかりません。") from e
	except subprocess.TimeoutExpired as e:
		raise ToolError(f"{args[0]} の処理が {PS_TIMEOUT_SEC} 秒で終わりませんでした。") from e
	return result.returncode, (_decode(result.stdout) + _decode(result.stderr)).strip()


def is_admin() -> bool:
	"""
	管理者権限で動いているかを返す。

	Returns:
		管理者なら True。判定できなければ False。
	Example:
		if not is_admin(): ...
	"""
	try:
		return bool(ctypes.windll.shell32.IsUserAnAdmin())
	except (AttributeError, OSError):
		return False


def require_admin() -> None:
	"""
	管理者権限が無ければ止める。

	Raises:
		ToolError: 管理者権限が無いとき。
	Example:
		require_admin()
	"""
	if not is_admin():
		raise ToolError("管理者権限が必要です。net_priority.bat から実行するか、管理者として起動してください。")


# ---------------------------------------------------------------------------
# 対象プログラムの特定
# ---------------------------------------------------------------------------


def bot_exe_path(repo: Path = REPO_DIR) -> Path:
	"""
	Bot 本体の exe (bin\\watapachi-bot.exe) の実体を返す。

	Args:
		repo: リポジトリの直下。
	Returns:
		exe の絶対パス。
	Raises:
		ToolError: exe が無いとき (何も変えずに止めるため)。
	Example:
		exe = bot_exe_path()
	"""
	exe = repo / BOT_EXE_RELATIVE
	if not exe.is_file():
		raise ToolError(f"Bot の実行ファイルがありません: {exe}")
	return exe.resolve()


def _repo_path(repo: Path, text: str) -> Path:
	"""
	設定の相対パスを、リポジトリの直下からのパスにする。

	- Bot は作業ディレクトリ (リポジトリの直下) からの相対で読むため、それに合わせる

	Args:
		repo: リポジトリの直下。
		text: 設定のパス。絶対パスならそのまま使う。
	Returns:
		パス。
	Example:
		_repo_path(REPO_DIR, "worker/envs")
	"""
	path = Path(text)
	return path if path.is_absolute() else repo / path


def _load_paths_config(repo: Path) -> dict:
	"""
	config.toml の [paths] を読む。

	- ファイルが無ければ空の辞書を返す (既定値を使う)
	- 読めない・壊れているときは注意を表示して空の辞書を返す

	Args:
		repo: リポジトリの直下。
	Returns:
		[paths] の辞書。
	Example:
		paths = _load_paths_config(REPO_DIR)
	"""
	config = repo / CONFIG_FILE_NAME
	try:
		with config.open("rb") as file:
			data = tomllib.load(file)
	except FileNotFoundError:
		return {}
	except (OSError, tomllib.TOMLDecodeError) as e:
		print(f"[注意] {config} を読めないため、既定の場所でワーカーの環境を探します。({e})")
		return {}
	paths = data.get("paths")
	return paths if isinstance(paths, dict) else {}


def _worker_launcher_candidates(repo: Path) -> list[Path]:
	"""
	Bot が起動時に選ぶ順で、ワーカーの python (venv の Scripts\\python.exe) の候補を返す。

	- 順番は Bot と同じ：current.json の python → その previous の環境 → 設定の paths.worker_python
	- previous は置き場の直下の名前のときだけ使う (Bot と同じ)
	- current.json が無い・壊れているときは、その分の候補を飛ばす

	Args:
		repo: リポジトリの直下。
	Returns:
		候補のパスのリスト (存在するかは調べない)。
	Example:
		candidates = _worker_launcher_candidates(REPO_DIR)
	"""
	paths = _load_paths_config(repo)
	envs_text = paths.get("worker_envs", DEFAULT_WORKER_ENVS)
	envs = _repo_path(repo, envs_text if isinstance(envs_text, str) else DEFAULT_WORKER_ENVS)
	candidates: list[Path] = []
	try:
		record = json.loads((envs / CURRENT_FILE_NAME).read_text(encoding="utf-8-sig"))
	except (OSError, ValueError):
		record = None
	if isinstance(record, dict):
		python = record.get("python")
		if isinstance(python, str) and python:
			candidates.append(_repo_path(repo, python))
		previous = record.get("previous")
		if isinstance(previous, str) and previous and Path(previous).name == previous and previous not in (".", ".."):
			candidates.append(envs / previous / VENV_PYTHON_RELATIVE)
	worker_python = paths.get("worker_python", DEFAULT_WORKER_PYTHON)
	candidates.append(_repo_path(repo, worker_python if isinstance(worker_python, str) else DEFAULT_WORKER_PYTHON))
	return candidates


def _venv_base_python(launcher: Path) -> Path:
	"""
	venv の python.exe から、実際に通信する python.exe の実体を返す。

	- venv の Scripts\\python.exe は本体を子として起動する中継役のため、pyvenv.cfg の home にある本体を返す
	- pyvenv.cfg が無い・home の python.exe が無いときは、venv でない Python とみなして launcher 自身を返す
	- シンボリックリンクだと実行中のプロセスのパスと一致しないため、実体にする

	Args:
		launcher: 存在する python.exe のパス。
	Returns:
		python.exe の実体の絶対パス。
	Example:
		base = _venv_base_python(Path("worker/envs/x/Scripts/python.exe"))
	"""
	cfg = launcher.parent.parent / PYVENV_CFG_NAME
	try:
		lines = cfg.read_text(encoding="utf-8", errors="replace").splitlines()
	except OSError:
		return launcher.resolve()
	for line in lines:
		key, sep, value = line.partition("=")
		if sep and key.strip().lower() == "home":
			candidate = Path(value.strip()) / "python.exe"
			if candidate.is_file():
				return candidate.resolve()
	return launcher.resolve()


def worker_base_python(repo: Path = REPO_DIR) -> Path | None:
	"""
	ワーカー (曲の解決と PipeInput) の通信を行う python.exe の実体を返す。

	- Bot が起動時に選ぶ python と同じものを選び、その venv の本体を返す
	- Bot が次に起動したときに選ぶものなので、更新で環境が替わった直後は、動いているものと違うことがある

	Args:
		repo: リポジトリの直下。
	Returns:
		python.exe の実体の絶対パス。候補がどれも無ければ None。
	Example:
		python = worker_base_python()
	"""
	for launcher in _worker_launcher_candidates(repo):
		if launcher.is_file():
			return _venv_base_python(launcher)
	return None


def policy_targets(repo: Path = REPO_DIR) -> list[tuple[str, Path, int]]:
	"""
	作成する QoS ポリシーの一覧を返す。

	- ワーカーの python が見つからなければ注意を表示し、Bot 本体だけを返す

	Args:
		repo: リポジトリの直下。
	Returns:
		(ポリシー名, 対象の exe, DSCP) のリスト。
	Raises:
		ToolError: Bot 本体の exe が無いとき。
	Example:
		for name, exe, dscp in policy_targets(): ...
	"""
	targets = [(POLICY_PREFIX + "bot", bot_exe_path(repo), DSCP_BOT)]
	worker = worker_base_python(repo)
	if worker:
		targets.append((POLICY_PREFIX + "worker", worker, DSCP_WORKER))
	else:
		print("[注意] ワーカーの python が見つからないため、ワーカーのポリシーは作りません。(update-worker で環境を作ってから on をやり直してください)")
	return targets


# ---------------------------------------------------------------------------
# レジストリ
# ---------------------------------------------------------------------------


def reg_read(key: str, name: str) -> tuple[bool, int | None, object]:
	"""
	HKLM の値を読む。

	Args:
		key: キーのパス。
		name: 値の名前。
	Returns:
		(存在するか, 型, 値)。無ければ (False, None, None)。
	Example:
		exists, value_type, value = reg_read(QOS_REG_KEY, NLA_VALUE_NAME)
	"""
	try:
		with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key, 0, winreg.KEY_READ | REG_VIEW) as handle:
			value, value_type = winreg.QueryValueEx(handle, name)
			return True, value_type, value
	except FileNotFoundError:
		return False, None, None


def reg_write(key: str, name: str, value_type: int, value: object) -> None:
	"""
	HKLM に値を書く。キーが無ければ作る。

	Args:
		key: キーのパス。
		name: 値の名前。
		value_type: winreg.REG_SZ などの型。
		value: 値。
	Raises:
		OSError: 書き込めないとき。
	Example:
		reg_write(QOS_REG_KEY, NLA_VALUE_NAME, winreg.REG_SZ, "1")
	"""
	with winreg.CreateKeyEx(winreg.HKEY_LOCAL_MACHINE, key, 0, winreg.KEY_SET_VALUE | REG_VIEW) as handle:
		winreg.SetValueEx(handle, name, 0, value_type, value)


def reg_delete(key: str, name: str) -> None:
	"""
	HKLM の値を消す。無ければ何もしない。

	Args:
		key: キーのパス。
		name: 値の名前。
	Raises:
		OSError: 消せないとき (無いときを除く)。
	Example:
		reg_delete(QOS_REG_KEY, NLA_VALUE_NAME)
	"""
	try:
		with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key, 0, winreg.KEY_SET_VALUE | REG_VIEW) as handle:
			winreg.DeleteValue(handle, name)
	except FileNotFoundError:
		pass


def desired_registry() -> list[tuple[str, str, int, object]]:
	"""
	有効にするときに書き込む値の一覧を返す。

	Returns:
		(キー, 名前, 型, 値) のリスト。
	Example:
		for key, name, value_type, value in desired_registry(): ...
	"""
	items: list[tuple[str, str, int, object]] = [(QOS_REG_KEY, NLA_VALUE_NAME, winreg.REG_SZ, NLA_VALUE)]
	for name, percent in DO_LIMITS.items():
		items.append((DO_REG_KEY, name, winreg.REG_DWORD, percent))
	return items


# ---------------------------------------------------------------------------
# 退避ファイル
# ---------------------------------------------------------------------------


def _valid_state(data: object) -> bool:
	"""
	退避ファイルの中身の形式が正しいかを返す。

	Args:
		data: JSON として読んだ中身。
	Returns:
		version が STATE_VERSION で、registry がリストなら True。
	Example:
		if _valid_state(json.loads(text)): ...
	"""
	return isinstance(data, dict) and data.get("version") == STATE_VERSION and isinstance(data.get("registry"), list)


def _write_state(path: Path, state: dict) -> None:
	"""
	退避ファイルを書く。

	- 書き込みの途中で止まっても壊れたファイルが残らないよう、一時ファイルに書いてから置き換える

	Args:
		path: 退避ファイルのパス。親のディレクトリが無ければ作る。
		state: 書き込む中身。
	Raises:
		OSError: 書き込めないとき。
	Example:
		_write_state(STATE_FILE, state)
	"""
	path.parent.mkdir(parents=True, exist_ok=True)
	temp = path.with_suffix(".tmp")
	temp.write_text(json.dumps(state, ensure_ascii=False, indent="\t"), encoding="utf-8")
	os.replace(temp, path)


def load_state(path: Path = STATE_FILE) -> dict | None:
	"""
	有効にする前の値の退避を読む。

	- 壊れている・形式が違うときは、今の値 (このツールが書いた値かもしれない) で上書きしないよう止める
	- メモ帳などで編集されて BOM が付いていても読む

	Args:
		path: 退避ファイルのパス。
	Returns:
		退避の中身。ファイルが無ければ None。
	Raises:
		ToolError: 読めない、形式が違うとき。
	Example:
		state = load_state()
	"""
	try:
		data = json.loads(path.read_text(encoding="utf-8-sig"))
	except FileNotFoundError:
		return None
	except (OSError, ValueError) as e:
		raise ToolError(f"退避ファイル {path} を読めません。中身を確認し、不要なら削除してから実行してください。({e})") from e
	if not _valid_state(data):
		raise ToolError(f"退避ファイル {path} の形式が違います。中身を確認し、不要なら削除してから実行してください。")
	return data


def save_state_if_missing() -> None:
	"""
	初回の有効化のときだけ、書き換える前の値を退避する。

	- 有効化を繰り返しても、最初の元の値を保つ

	Raises:
		ToolError: 退避ファイルが壊れている、値が想定外の型で設定済みのとき。
		OSError: 書き込めないとき。
	Example:
		save_state_if_missing()
	"""
	if load_state(STATE_FILE) is not None:
		return
	registry = []
	for key, name, expected_type, _value in desired_registry():
		exists, value_type, value = reg_read(key, name)
		if exists and value_type != expected_type:
			raise ToolError(f"{key}\\{name} が想定外の型 ({value_type}) で設定済みのため、変更せずに中止しました。")
		registry.append({"key": key, "name": name, "exists": exists, "type": value_type, "value": value})
	state = {"version": STATE_VERSION, "created": datetime.now().isoformat(timespec="seconds"), "registry": registry}
	_write_state(STATE_FILE, state)


def adopt_old_state(old: Path = OLD_STATE_FILE, new: Path = STATE_FILE) -> list[str]:
	"""
	旧ツール (SatouSioBOT) の退避ファイルを引き継ぐ。

	- 新ツールの退避が無ければ、旧ツールの退避を新ツールの退避にする (旧ツールが書き換える前の、本当の元の値のため)
	- 新ツールの退避があれば、それを使い続ける
	- どちらでも旧ツールの退避は消す。後から旧ツールの off を実行しても、何も変えずに終わるようにするため
	- 旧ツールの退避が読めない・形式が違うときは、名前に .broken を付けて退ける (既にあれば上書き)
	- ファイルの操作だけを行い、QoS ポリシーとレジストリには触らない

	Args:
		old: 旧ツールの退避ファイル。
		new: 新ツールの退避ファイル。
	Returns:
		利用者に表示する行のリスト。旧ツールの退避が無ければ空。
	Raises:
		ToolError: 新ツールの退避ファイルが壊れているとき (旧ツールの退避は変えない)。
		OSError: ファイルを書けない、消せないとき。
	Example:
		for line in adopt_old_state(): print(line)
	"""
	if not old.exists():
		return []
	# 新ツールの退避が壊れていれば、旧ツールの退避に触る前に止める
	new_state = load_state(new)
	try:
		data = json.loads(old.read_text(encoding="utf-8-sig"))
	except (OSError, ValueError):
		data = None
	if not _valid_state(data):
		broken = old.with_name(old.name + BROKEN_SUFFIX)
		os.replace(old, broken)
		return [f"[注意] 旧ツールの退避ファイルを読めないため、{broken} に退けました。旧ツールを使う前の値は分からなくなりました。"]
	if new_state is None:
		_write_state(new, data)
		old.unlink()
		return ["[OK] 旧ツールの退避ファイルを引き継ぎました。(off で旧ツールを使う前の値に戻ります)"]
	old.unlink()
	return ["[OK] 旧ツールの退避ファイルを削除しました。(このツールの退避ファイルを使います)"]


# ---------------------------------------------------------------------------
# QoS ポリシー
# ---------------------------------------------------------------------------


def list_policies(prefix: str = POLICY_PREFIX) -> list[dict]:
	"""
	名前が prefix で始まる QoS ポリシーの一覧を返す。

	Args:
		prefix: ポリシー名の接頭辞。旧ツールのものは OLD_POLICY_PREFIX。
	Returns:
		ポリシーごとの辞書 (Name、AppPathName、DSCPAction など) のリスト。
	Raises:
		ToolError: PowerShell の実行に失敗したとき。
	Example:
		policies = list_policies()
	"""
	return run_ps_json(
		f"Get-NetQosPolicy | Where-Object {{ $_.Name -like {_ps_quote(prefix + '*')} }} "
		"| Select-Object Name, AppPathName, DSCPAction, NetworkProfile, IPProtocol"
	)


def remove_policies(prefix: str = POLICY_PREFIX) -> int:
	"""
	名前が prefix で始まる QoS ポリシーを消す。

	Args:
		prefix: ポリシー名の接頭辞。
	Returns:
		消した件数。
	Raises:
		ToolError: PowerShell の実行に失敗したとき。
	Example:
		removed = remove_policies(OLD_POLICY_PREFIX)
	"""
	policies = list_policies(prefix)
	for policy in policies:
		run_ps(f"Remove-NetQosPolicy -Name {_ps_quote(policy['Name'])} -Confirm:$false")
	return len(policies)


def create_policies(targets: list[tuple[str, Path, int]]) -> None:
	"""
	QoS ポリシーを作る。全ネットワーク・TCP と UDP の両方が対象。

	Args:
		targets: policy_targets() の戻り値。
	Raises:
		ToolError: PowerShell の実行に失敗したとき。
	Example:
		create_policies(policy_targets())
	"""
	for name, exe, dscp in targets:
		run_ps(
			f"New-NetQosPolicy -Name {_ps_quote(name)} -AppPathNameMatchCondition {_ps_quote(str(exe))} "
			f"-IPProtocolMatchCondition Both -NetworkProfile All -DSCPAction {dscp} | Out-Null"
		)


def take_over_old_tool() -> int:
	"""
	旧ツール (SatouSioBOT) の設定を引き継ぐ。on と off の最初に呼ぶ。

	- 最初に新ツールの退避を読む (壊れていれば、旧ツールの設定も含めて何も変えずに止めるため)
	- 退避ファイルを引き継いでから、旧ツールの QoS ポリシーを消す (新ツールのポリシーと二重に付けないため)
	- 戻り値が 1 以上で、引き継いだ後も新ツールの退避が無ければ、旧ツールは退避なしで有効だった (呼び出し側で扱う)

	Returns:
		消した旧ツールのポリシーの件数。
	Raises:
		ToolError: 新ツールの退避ファイルが壊れている、PowerShell の実行に失敗したとき。
		OSError: ファイルの操作に失敗したとき。
	Example:
		old_removed = take_over_old_tool()
	"""
	load_state(STATE_FILE)
	for line in adopt_old_state(OLD_STATE_FILE, STATE_FILE):
		print(line)
	removed = remove_policies(OLD_POLICY_PREFIX)
	if removed:
		print(f"[OK] 旧ツールの QoS ポリシーを {removed} 件削除しました。")
	return removed


def clear_tool_values() -> None:
	"""
	レジストリのうち、このツールの設定値と同じ値だけを消す (他で設定された値は残す)。

	- 退避が無く、元の値が分からないときに使う (旧ツールの off の、退避が無いときと同じ扱い)

	Raises:
		OSError: レジストリの操作に失敗したとき。
	Example:
		clear_tool_values()
	"""
	for key, name, _type, value in desired_registry():
		exists, _cur_type, current = reg_read(key, name)
		if exists and current == value:
			reg_delete(key, name)


# ---------------------------------------------------------------------------
# 配信の最適化
# ---------------------------------------------------------------------------


def show_delivery_optimization() -> None:
	"""
	配信の最適化サービスが実際に使っている帯域の上限を表示する。

	- ポリシーはサービスの再起動なしで読み込まれる
	- 取得に失敗しても止めずに、その旨を表示する

	Example:
		show_delivery_optimization()
	"""
	try:
		rows = run_ps_json(f"Get-DOConfig | Select-Object {', '.join(DO_EFFECTIVE_FIELDS)}")
	except ToolError as e:
		print(f"  実際の値を取得できませんでした: {e}")
		return
	for row in rows:
		for field, label in DO_EFFECTIVE_FIELDS.items():
			percent = row.get(field)
			shown = "制限なし (自動調整)" if percent in (0, None) else f"{percent}%"
			print(f"  {label}: {shown}")


# ---------------------------------------------------------------------------
# 操作
# ---------------------------------------------------------------------------


def warn_if_pacer_disabled() -> None:
	"""
	DSCP の付与に要る QoS パケット スケジューラーが、接続中のアダプターで無効なら知らせる。

	- 確認できなくても止めずに、その旨を表示する

	Example:
		warn_if_pacer_disabled()
	"""
	try:
		rows = run_ps_json(
			"Get-NetAdapter -Physical | Where-Object Status -eq 'Up' "
			"| Get-NetAdapterBinding -ComponentID ms_pacer | Select-Object Name, Enabled"
		)
	except ToolError as e:
		print(f"[注意] QoS パケット スケジューラーの状態を確認できませんでした: {e}")
		return
	for row in rows:
		if not row.get("Enabled"):
			print(
				f"[注意] {row.get('Name')} で QoS パケット スケジューラーが無効のため DSCP が付きません。"
				"アダプターのプロパティで有効にしてください。(切り替え時に通信が一瞬切れます)"
			)


def enable() -> None:
	"""
	優先設定を有効にする。

	- 対象の exe が無ければ、旧ツールの引き継ぎも含めて何も変えずに止める
	- Bot の再起動は要らない (再生中の通信にもすぐ反映される)

	Raises:
		ToolError: 管理者でない、対象の exe が無い、退避ファイルが壊れている、PowerShell の失敗。
		OSError: レジストリやファイルの操作に失敗したとき。
	Example:
		enable()
	"""
	require_admin()
	targets = policy_targets()
	old_removed = take_over_old_tool()
	if old_removed and load_state(STATE_FILE) is None:
		# 旧ツールが退避なしで有効だった。今の値は旧ツールが書いた値のため、元の値として退避しない
		clear_tool_values()
		print("[注意] 旧ツールを使う前の値が分からないため、このツールの設定値と同じ値は、元は無かったものとして扱います。")
	save_state_if_missing()

	remove_policies()
	create_policies(targets)
	for name, exe, dscp in targets:
		print(f"[OK] QoS ポリシー {name}: {exe} の通信に DSCP {dscp} を付けます。")

	for key, name, value_type, value in desired_registry():
		reg_write(key, name, value_type, value)
	print(f"[OK] ドメイン不参加でも DSCP が付くよう {NLA_VALUE_NAME} を設定しました。")
	limits = " / ".join(f"{name}={percent}%" for name, percent in DO_LIMITS.items())
	print(f"[OK] 配信の最適化の帯域を制限しました。({limits})")
	warn_if_pacer_disabled()

	print("")
	print("有効にしました。Bot の再起動は不要で、再生中の通信にもすぐ反映されます。")
	print("反映を確かめるには再生中に check を実行してください。")


def disable() -> None:
	"""
	有効にする前の状態に戻す。

	- 旧ツールの設定が残っていれば、先に引き継いでから戻す (旧ツールを使う前の値に戻る)
	- 退避ファイルは一般ユーザーも書ける場所にあるため、このツールが扱う値・型に一致するものだけ戻す

	Raises:
		ToolError: 管理者でない、退避ファイルが壊れている、PowerShell の失敗。
		OSError: レジストリの操作に失敗したとき。
	Example:
		disable()
	"""
	require_admin()
	old_removed = take_over_old_tool()
	state = load_state(STATE_FILE)
	new_removed = remove_policies()
	print(f"[OK] QoS ポリシーを {new_removed} 件削除しました。")

	# 旧ツールのポリシーだけが残っていた (旧ツールが退避なしで有効だった) ときも、値を消すため数に含める
	if state is None and new_removed + old_removed == 0:
		# 有効化されていない状態で消すと、元から設定されていた値 (例: Do not use NLA) まで消してしまうため何もしない
		print("")
		print("すでに無効です。(変更はありません)")
		return
	if state is not None:
		allowed = {(key, name): value_type for key, name, value_type, _value in desired_registry()}
		for item in state.get("registry", []):
			if not isinstance(item, dict):
				continue
			key, name = item.get("key"), item.get("name")
			expected_type = allowed.get((key, name))
			if expected_type is None:
				print(f"[注意] 退避ファイルに対象外の項目があったため無視しました: {key}\\{name}")
				continue
			if not item.get("exists"):
				reg_delete(key, name)
			elif item.get("type") == expected_type and isinstance(item.get("value"), REG_PY_TYPES[expected_type]):
				reg_write(key, name, expected_type, item["value"])
			else:
				print(f"[注意] 退避された {name} の型が想定外のため戻さず削除しました。")
				reg_delete(key, name)
		print("[OK] レジストリを有効化前の値に戻しました。")
	else:
		clear_tool_values()
		print("[OK] 退避ファイルが無いため、このツールの設定値と同じものだけ削除しました。")

	try:
		STATE_FILE.unlink(missing_ok=True)
	except OSError as e:
		print(f"[注意] 退避ファイルを削除できませんでした: {e}")
	print("")
	print("無効にしました。(元の状態に戻しました)")


def status() -> None:
	"""
	現在の設定を表示する。何も変えない。

	- 旧ツールの設定が残っていれば注意を表示する
	- 管理者でなくても動く (一部の項目は正しく読めないことがある)

	Raises:
		ToolError: PowerShell の実行に失敗したとき。
	Example:
		status()
	"""
	print("=== QoS ポリシー ===")
	policies = list_policies()
	try:
		targets_now = {name: exe for name, exe, _dscp in policy_targets()}
	except ToolError as e:
		print(f"  [注意] 対象プログラムを特定できません: {e}")
		targets_now = {}
	stale = False
	if policies:
		for policy in policies:
			exe = Path(policy.get("AppPathName") or "")
			if not exe.is_file():
				mark = "  (このパスのファイルがありません。on をやり直してください)"
				stale = True
			elif policy["Name"] in targets_now and str(targets_now[policy["Name"]]).lower() != str(exe).lower():
				mark = f"  (現在の対象 {targets_now[policy['Name']]} と違います。on をやり直してください)"
				stale = True
			else:
				mark = ""
			print(f"  {policy['Name']}: DSCP {policy.get('DSCPAction')}  {exe}{mark}")
	else:
		print("  なし")

	print("=== レジストリ ===")
	matched = 0
	desired = desired_registry()
	for key, name, _type, value in desired:
		exists, _cur_type, current = reg_read(key, name)
		if exists and current == value:
			matched += 1
		shown = current if exists else "未設定"
		print(f"  {name} = {shown}")

	print("=== 配信の最適化 (実際の値) ===")
	show_delivery_optimization()

	print("=== 対象プログラム ===")
	for name, exe in targets_now.items():
		print(f"  {name.removeprefix(POLICY_PREFIX)}: {exe}")

	old_policies = list_policies(OLD_POLICY_PREFIX)
	if old_policies or OLD_STATE_FILE.exists():
		print("=== 旧ツール (SatouSioBOT) ===")
		for policy in old_policies:
			print(f"  {policy['Name']}: DSCP {policy.get('DSCPAction')}  {policy.get('AppPathName')}")
		if OLD_STATE_FILE.exists():
			print(f"  退避ファイル: {OLD_STATE_FILE}")
		print("[注意] 旧ツールの設定が残っています。on か off を実行すると、このツールが引き継ぎます。")

	warn_if_pacer_disabled()

	# レジストリの値は他で設定済みのこともあるため、無効の判定はこのツールのポリシーと退避ファイルで行う
	has_state = STATE_FILE.is_file()
	if policies and matched == len(desired) and not stale:
		overall = "有効"
	elif not policies and not has_state:
		overall = "無効"
	else:
		overall = "一部だけ有効 (on か off をやり直してください)"
	print("")
	print(f"状態: {overall}")
	if not is_admin():
		print("(管理者権限が無いため、一部の項目は正しく読めていない可能性があります)")


# ---------------------------------------------------------------------------
# 動作確認 (パケットキャプチャ)
# ---------------------------------------------------------------------------


def _iter_pcapng_frames(data: bytes) -> Iterator[bytes]:
	"""
	pcapng の Enhanced Packet Block から、フレームの中身を順に返す。

	- 途中で切れたブロックがあれば、そこで止める

	Args:
		data: pcapng のバイト列。
	Returns:
		フレームのバイト列を返すイテレーター。
	Example:
		for frame in _iter_pcapng_frames(data): ...
	"""
	pos = 0
	endian = "<"
	size = len(data)
	while pos + PCAPNG_MIN_BLOCK_LEN <= size:
		block_type = struct.unpack_from(endian + "I", data, pos)[0]
		if block_type == PCAPNG_SHB:
			endian = "<" if data[pos + 8:pos + 12] == PCAPNG_BOM_LE else ">"
		block_len = struct.unpack_from(endian + "I", data, pos + 4)[0]
		if block_len < PCAPNG_MIN_BLOCK_LEN or pos + block_len > size:
			break
		if block_type == PCAPNG_EPB and block_len >= PCAPNG_EPB_DATA_OFFSET:
			cap_len = struct.unpack_from(endian + "I", data, pos + PCAPNG_EPB_CAPLEN_OFFSET)[0]
			start = pos + PCAPNG_EPB_DATA_OFFSET
			yield data[start:min(start + cap_len, pos + block_len)]
		pos += block_len


def _parse_frame(frame: bytes) -> tuple[int, bytes, int, int] | None:
	"""
	Ethernet フレームから送信元と DSCP を取り出す。

	Args:
		frame: Ethernet フレーム (VLAN タグ付きも可)。
	Returns:
		(プロトコル番号, 送信元 IP, 送信元ポート, DSCP)。TCP・UDP 以外や短すぎるものは None。
	Example:
		parsed = _parse_frame(frame)
	"""
	offset = ETHERTYPE_OFFSET
	if len(frame) < offset + 2:
		return None
	ethertype = struct.unpack_from("!H", frame, offset)[0]
	if ethertype == ETHERTYPE_VLAN:
		offset += VLAN_TAG_LEN
		if len(frame) < offset + 2:
			return None
		ethertype = struct.unpack_from("!H", frame, offset)[0]
	ip = frame[offset + 2:]
	if ethertype == ETHERTYPE_IPV4 and len(ip) >= IPV4_MIN_HEADER_LEN:
		header_len = (ip[0] & 0x0F) * 4
		dscp = ip[1] >> 2
		proto = ip[9]
		src = bytes(ip[12:16])
		l4 = ip[header_len:]
	elif ethertype == ETHERTYPE_IPV6 and len(ip) >= IPV6_HEADER_LEN:
		traffic_class = ((ip[0] & 0x0F) << 4) | (ip[1] >> 4)
		dscp = traffic_class >> 2
		proto = ip[6]
		src = bytes(ip[8:24])
		l4 = ip[IPV6_HEADER_LEN:]
	else:
		return None
	if proto not in (PROTO_TCP, PROTO_UDP) or len(l4) < 2:
		return None
	return proto, src, struct.unpack_from("!H", l4, 0)[0], dscp


def _local_addresses() -> set[bytes]:
	"""
	この PC の IP アドレスの集合を返す。受信したパケットを送信として数えないために使う。

	Returns:
		アドレスのバイト列の集合。
	Raises:
		ToolError: PowerShell の実行に失敗したとき。
	Example:
		local = _local_addresses()
	"""
	rows = run_ps_json("Get-NetIPAddress | Select-Object IPAddress")
	addresses = set()
	for row in rows:
		text = str(row.get("IPAddress") or "").split("%")[0]
		try:
			addresses.add(ipaddress.ip_address(text).packed)
		except ValueError:
			continue
	return addresses


def _bot_sockets(targets: list[tuple[str, Path, int]]) -> dict[tuple[int, int], str]:
	"""
	対象のプログラムが使っているローカルのポートを返す。

	Args:
		targets: policy_targets() の戻り値。
	Returns:
		{(プロトコル番号, ポート): ポリシー名}。
	Raises:
		ToolError: PowerShell の実行に失敗したとき。
	Example:
		sockets = _bot_sockets(policy_targets())
	"""
	paths = ", ".join(_ps_quote(str(exe)) for _name, exe, _dscp in targets)
	rows = run_ps_json(
		f"$paths = @({paths}); "
		"$procs = Get-CimInstance Win32_Process | Where-Object { $_.ExecutablePath -and ($paths -contains $_.ExecutablePath) }; "
		"$map = @{}; foreach ($p in $procs) { $map[[int]$p.ProcessId] = $p.ExecutablePath }; "
		"@(Get-NetUDPEndpoint -ErrorAction SilentlyContinue | Where-Object { $map.ContainsKey([int]$_.OwningProcess) } "
		"| ForEach-Object { [pscustomobject]@{ Proto = 17; Port = [int]$_.LocalPort; Path = $map[[int]$_.OwningProcess] } }) + "
		"@(Get-NetTCPConnection -State Established -ErrorAction SilentlyContinue | Where-Object { $map.ContainsKey([int]$_.OwningProcess) } "
		"| ForEach-Object { [pscustomobject]@{ Proto = 6; Port = [int]$_.LocalPort; Path = $map[[int]$_.OwningProcess] } })"
	)
	by_path = {str(exe).lower(): name for name, exe, _dscp in targets}
	return {(int(r["Proto"]), int(r["Port"])): by_path.get(str(r["Path"]).lower(), "?") for r in rows}


def _capture(seconds: int) -> bytes:
	"""
	pktmon で NIC を通るパケットを記録する。

	- 前回が強制終了などで記録中のまま残っていると開始できないため、先に止める

	Args:
		seconds: 記録する秒数。
	Returns:
		pcapng のバイト列。
	Raises:
		ToolError: pktmon を開始できない、変換に失敗したとき。
	Example:
		data = _capture(CHECK_SECONDS)
	"""
	run_cmd(["pktmon", "stop"])
	with tempfile.TemporaryDirectory(prefix="net_priority_") as work:
		etl = Path(work) / "capture.etl"
		pcap = Path(work) / "capture.pcapng"
		code, out = run_cmd(["pktmon", "start", "--capture", "--comp", "nics", "--pkt-size", str(CAPTURE_BYTES), "--file-name", str(etl)])
		if code != 0:
			raise ToolError(f"pktmon を開始できませんでした: {out}")
		print(f"{seconds} 秒間パケットを記録しています...")
		try:
			time.sleep(seconds)
		finally:
			run_cmd(["pktmon", "stop"])
		code, out = run_cmd(["pktmon", "etl2pcap", str(etl), "--out", str(pcap)])
		if code != 0 or not pcap.is_file():
			raise ToolError(f"記録の変換に失敗しました: {out}")
		return pcap.read_bytes()


def check() -> int:
	"""
	数秒間パケットを記録し、Bot の送信パケットに付いている DSCP を集計して表示する。

	Returns:
		記録できた送信パケットがすべて期待どおりなら 0、そうでなければ 1。
	Raises:
		ToolError: 管理者でない、Bot の通信が見つからない、記録に失敗したとき。
	Example:
		code = check()
	"""
	require_admin()
	targets = policy_targets()
	sockets = _bot_sockets(targets)
	if not sockets:
		raise ToolError("Bot の通信が見つかりません。Bot が起動していて、曲を再生している間に実行してください。")
	local = _local_addresses()
	data = _capture(CHECK_SECONDS)

	counts: dict[str, Counter] = {name: Counter() for name, _exe, _dscp in targets}
	for frame in _iter_pcapng_frames(data):
		parsed = _parse_frame(frame)
		if parsed is None:
			continue
		proto, src, src_port, dscp = parsed
		if src not in local:
			continue
		owner = sockets.get((proto, src_port))
		if owner in counts:
			counts[owner][dscp] += 1

	print("")
	if not any(counts.values()):
		print("Bot の送信パケットを 1 件も記録できませんでした。曲を再生している間にもう一度実行してください。")
		return 1
	ok = True
	for name, _exe, expected in targets:
		counter = counts[name]
		total = sum(counter.values())
		if total == 0:
			print(f"  {name}: 送信パケットなし (この間は通信していませんでした)")
			continue
		detail = ", ".join(f"DSCP {d}: {n} 件" for d, n in sorted(counter.items()))
		marked = counter.get(expected, 0)
		verdict = "OK" if marked == total else "一部未適用" if marked else "未適用"
		if marked != total:
			ok = False
		print(f"  {name}: {detail}  -> [{verdict}] (期待値 DSCP {expected})")
	print("")
	if ok:
		print("記録できた通信には期待どおりの DSCP が付いていました。")
		return 0
	print("DSCP が付いていない通信があります。on を実行済みなら、有効化より前に開いた通信の残りの可能性があるため、少し待ってからもう一度確かめてください。")
	return 1


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

ACTIONS = {
	"on": (enable, "有効にする (Bot の通信を優先)"),
	"off": (disable, "無効にする (元に戻す)"),
	"status": (status, "状態を表示"),
	"check": (check, "動作確認 (再生中に実行)"),
}


def run_action(name: str) -> int:
	"""
	操作を実行し、エラーを表示に変える。

	Args:
		name: ACTIONS のキー。
	Returns:
		終了コード (成功は 0)。
	Example:
		sys.exit(run_action("status"))
	"""
	func, _label = ACTIONS[name]
	try:
		return func() or 0
	except ToolError as e:
		print(f"[エラー] {e}")
	except OSError as e:
		print(f"[エラー] OS の操作に失敗しました: {e}")
	except KeyboardInterrupt:
		print("[中断] 途中で中断しました。status で状態を確認してください。")
	except Exception as e:
		print(f"[エラー] 想定外のエラーが発生しました: {type(e).__name__}: {e}")
	return 1


def menu() -> int:
	"""
	対話メニューを表示する。終了を選ぶか、入力が終わるまで繰り返す。

	Returns:
		終了コード (常に 0)。
	Example:
		sys.exit(menu())
	"""
	keys = list(ACTIONS)
	while True:
		print("")
		print("=== WataPachiBOT ネットワーク優先設定 ===")
		for index, key in enumerate(keys, 1):
			print(f"  {index}. {ACTIONS[key][1]}")
		print("  0. 終了")
		try:
			choice = input("番号を入力してください: ").strip()
		except (EOFError, KeyboardInterrupt):
			return 0
		if choice == "0":
			return 0
		if choice.isdigit() and 1 <= int(choice) <= len(keys):
			print("")
			run_action(keys[int(choice) - 1])
		else:
			print("番号が正しくありません。")


def main() -> int:
	"""
	コマンドラインの引数を解釈して実行する。

	Returns:
		終了コード。
	Example:
		sys.exit(main())
	"""
	if sys.platform != "win32":
		print("このツールは Windows 専用です。")
		return 1
	for stream in (sys.stdout, sys.stderr):
		try:
			stream.reconfigure(errors="replace")
		except AttributeError:
			pass
	parser = argparse.ArgumentParser(description="WataPachiBOT の通信を OS 側で優先させる設定を切り替えます。")
	parser.add_argument("action", nargs="?", choices=list(ACTIONS), help="省略するとメニューを表示")
	args = parser.parse_args()
	if args.action is None:
		return menu()
	return run_action(args.action)


if __name__ == "__main__":
	sys.exit(main())
