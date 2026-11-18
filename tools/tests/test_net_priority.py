"""
tools/net_priority.py のうち、ファイルだけを扱う処理の試験。

- 対象のプログラムの特定 (bot の exe、ワーカーの python の実体)
- 旧ツール (SatouSioBOT) の退避ファイルの引き継ぎ
- QoS ポリシーとレジストリを変える処理は、管理者の権限と OS の状態を要するため対象にしない (開発機で手で確かめる)

実行例 (リポジトリの直下から):
	worker\\.venv\\Scripts\\python.exe -m pytest tools\\tests -p no:cacheprovider -q
"""

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

TOOL_PATH = Path(__file__).resolve().parent.parent / "net_priority.py"
VALID_STATE = {"version": 1, "created": "2026-01-01T00:00:00", "registry": []}


def _load_tool() -> ModuleType:
	"""
	tools/net_priority.py をモジュールとして読み込む。

	tools はパッケージではないため、ファイルのパスから読む。

	Returns:
		読み込んだモジュール。
	Raises:
		ImportError: ファイルを読み込めないとき。
	Example:
		np = _load_tool()
	"""
	spec = importlib.util.spec_from_file_location("net_priority", TOOL_PATH)
	if spec is None or spec.loader is None:
		raise ImportError(f"{TOOL_PATH} を読み込めません")
	module = importlib.util.module_from_spec(spec)
	spec.loader.exec_module(module)
	return module


np = _load_tool()


def _make_venv(venv_dir: Path, home: Path) -> Path:
	"""
	偽の venv (Scripts\\python.exe と pyvenv.cfg) と、home の python.exe を作る。

	Args:
		venv_dir: venv のディレクトリ。
		home: pyvenv.cfg の home に書くディレクトリ。python.exe も作る。
	Returns:
		venv の Scripts\\python.exe のパス。
	Example:
		launcher = _make_venv(tmp_path / "worker" / ".venv", tmp_path / "base")
	"""
	scripts = venv_dir / "Scripts"
	scripts.mkdir(parents=True)
	launcher = scripts / "python.exe"
	launcher.write_bytes(b"")
	(venv_dir / "pyvenv.cfg").write_text(f"home = {home}\nversion_info = 3.14.0\n", encoding="utf-8")
	home.mkdir(parents=True, exist_ok=True)
	(home / "python.exe").write_bytes(b"")
	return launcher


def _write_current(envs: Path, record: dict) -> None:
	"""
	環境の記録 current.json を書く。

	Args:
		envs: 環境の置き場。無ければ作る。
		record: 書き込む内容。
	Example:
		_write_current(tmp_path / "worker" / "envs", {"python": "..."})
	"""
	envs.mkdir(parents=True, exist_ok=True)
	(envs / "current.json").write_text(json.dumps(record), encoding="utf-8")


def _base(path: Path) -> Path:
	"""home の python.exe の実体のパスを返す。"""
	return (path / "python.exe").resolve()


# ---------------------------------------------------------------------------
# 対象のプログラムの特定
# ---------------------------------------------------------------------------


def test_worker_python_from_current_json(tmp_path: Path) -> None:
	repo = tmp_path / "repo"
	envs = repo / "worker" / "envs"
	_make_venv(envs / "e1", tmp_path / "base1")
	_write_current(envs, {"name": "e1", "python": "worker/envs\\e1\\Scripts\\python.exe"})
	assert np.worker_base_python(repo) == _base(tmp_path / "base1")


def test_worker_python_uses_previous_when_current_python_missing(tmp_path: Path) -> None:
	repo = tmp_path / "repo"
	envs = repo / "worker" / "envs"
	_make_venv(envs / "old", tmp_path / "base-old")
	_write_current(envs, {"name": "e2", "python": "worker/envs/e2/Scripts/python.exe", "previous": "old"})
	assert np.worker_base_python(repo) == _base(tmp_path / "base-old")


def test_worker_python_ignores_previous_with_separator(tmp_path: Path) -> None:
	repo = tmp_path / "repo"
	envs = repo / "worker" / "envs"
	_make_venv(repo / "elsewhere", tmp_path / "base-x")
	_write_current(envs, {"python": "worker/envs/e2/Scripts/python.exe", "previous": "../../elsewhere"})
	assert np.worker_base_python(repo) is None


def test_worker_python_falls_back_to_worker_python(tmp_path: Path) -> None:
	repo = tmp_path / "repo"
	envs = repo / "worker" / "envs"
	envs.mkdir(parents=True)
	(envs / "current.json").write_text("{broken", encoding="utf-8")
	_make_venv(repo / "worker" / ".venv", tmp_path / "base2")
	assert np.worker_base_python(repo) == _base(tmp_path / "base2")


def test_worker_python_uses_config_paths(tmp_path: Path) -> None:
	repo = tmp_path / "repo"
	repo.mkdir()
	(repo / "config.toml").write_text(
		'[paths]\nworker_envs = "other/envs"\nworker_python = "nope/python.exe"\n', encoding="utf-8"
	)
	envs = repo / "other" / "envs"
	_make_venv(envs / "e3", tmp_path / "base3")
	_write_current(envs, {"python": "other/envs/e3/Scripts/python.exe"})
	assert np.worker_base_python(repo) == _base(tmp_path / "base3")


def test_worker_python_broken_config_uses_defaults(tmp_path: Path) -> None:
	repo = tmp_path / "repo"
	repo.mkdir()
	(repo / "config.toml").write_text("[paths\nbroken", encoding="utf-8")
	_make_venv(repo / "worker" / ".venv", tmp_path / "base4")
	assert np.worker_base_python(repo) == _base(tmp_path / "base4")


def test_worker_python_none_when_missing(tmp_path: Path) -> None:
	repo = tmp_path / "repo"
	repo.mkdir()
	assert np.worker_base_python(repo) is None


def test_bot_exe_missing_raises(tmp_path: Path) -> None:
	with pytest.raises(np.ToolError):
		np.bot_exe_path(tmp_path)


def test_policy_targets_without_worker_keeps_bot(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
	exe = tmp_path / "bin" / "watapachi-bot.exe"
	exe.parent.mkdir()
	exe.write_bytes(b"")
	targets = np.policy_targets(tmp_path)
	assert targets == [(np.POLICY_PREFIX + "bot", exe.resolve(), np.DSCP_BOT)]
	assert "注意" in capsys.readouterr().out


def test_policy_targets_with_worker(tmp_path: Path) -> None:
	exe = tmp_path / "bin" / "watapachi-bot.exe"
	exe.parent.mkdir()
	exe.write_bytes(b"")
	_make_venv(tmp_path / "worker" / ".venv", tmp_path / "base5")
	targets = np.policy_targets(tmp_path)
	assert targets[1] == (np.POLICY_PREFIX + "worker", _base(tmp_path / "base5"), np.DSCP_WORKER)


# ---------------------------------------------------------------------------
# PowerShell の選択
# ---------------------------------------------------------------------------


def test_powershell_prefers_pwsh(monkeypatch: pytest.MonkeyPatch) -> None:
	monkeypatch.setattr(np.shutil, "which", lambda name: r"C:\x\pwsh.exe" if name == "pwsh" else None)
	assert np.powershell_exe() == r"C:\x\pwsh.exe"


def test_powershell_falls_back_to_windows_powershell(monkeypatch: pytest.MonkeyPatch) -> None:
	monkeypatch.setattr(np.shutil, "which", lambda name: None)
	assert np.powershell_exe() == np.WINDOWS_POWERSHELL


# ---------------------------------------------------------------------------
# 旧ツールの退避ファイルの引き継ぎ
# ---------------------------------------------------------------------------


def _paths(tmp_path: Path) -> tuple[Path, Path]:
	"""(旧ツールの退避ファイル, 新ツールの退避ファイル) のパスを返す。どちらも作らない。"""
	return tmp_path / "SatouSioBOT" / "net_priority.json", tmp_path / "WataPachiBOT" / "net_priority.json"


def test_adopt_moves_old_state_when_new_missing(tmp_path: Path) -> None:
	old, new = _paths(tmp_path)
	old.parent.mkdir()
	old.write_text(json.dumps(VALID_STATE), encoding="utf-8")
	lines = np.adopt_old_state(old, new)
	assert json.loads(new.read_text(encoding="utf-8")) == VALID_STATE
	assert not old.exists()
	assert lines


def test_adopt_keeps_new_state_when_both_exist(tmp_path: Path) -> None:
	old, new = _paths(tmp_path)
	old.parent.mkdir()
	new.parent.mkdir()
	old.write_text(json.dumps({**VALID_STATE, "created": "old"}), encoding="utf-8")
	new.write_text(json.dumps(VALID_STATE), encoding="utf-8")
	np.adopt_old_state(old, new)
	assert json.loads(new.read_text(encoding="utf-8")) == VALID_STATE
	assert not old.exists()


def test_adopt_renames_broken_old_state(tmp_path: Path) -> None:
	old, new = _paths(tmp_path)
	old.parent.mkdir()
	old.write_text("{broken", encoding="utf-8")
	# 前回の .broken が残っていても上書きする
	old.with_name(old.name + ".broken").write_text("previous", encoding="utf-8")
	lines = np.adopt_old_state(old, new)
	assert not old.exists()
	assert old.with_name(old.name + ".broken").read_text(encoding="utf-8") == "{broken"
	assert not new.exists()
	assert any("注意" in line for line in lines)


def test_adopt_renames_old_state_with_wrong_format(tmp_path: Path) -> None:
	old, new = _paths(tmp_path)
	old.parent.mkdir()
	old.write_text(json.dumps({**VALID_STATE, "version": 2}), encoding="utf-8")
	np.adopt_old_state(old, new)
	assert old.with_name(old.name + ".broken").exists()
	assert not new.exists()


def test_adopt_noop_without_old_state(tmp_path: Path) -> None:
	old, new = _paths(tmp_path)
	assert np.adopt_old_state(old, new) == []
	assert not new.exists()


# ---------------------------------------------------------------------------
# 旧ツールが退避なしで有効だったときの on / off (OS の操作は偽物に差し替える)
# ---------------------------------------------------------------------------


class FakeSystem:
	"""
	レジストリと QoS ポリシーの偽物。on / off の分岐を管理者の権限なしで試すために使う。

	- registry: {(キー, 名前): (型, 値)}
	- policies: {接頭辞: 件数}。remove_policies で 0 になる

	Example:
		fake = FakeSystem.install(monkeypatch, tmp_path, policies={np.OLD_POLICY_PREFIX: 1})
	"""

	def __init__(self, policies: dict[str, int]) -> None:
		"""
		Args:
			policies: 接頭辞ごとの、今あるポリシーの件数。
		"""
		self.registry: dict[tuple[str, str], tuple[int, object]] = {}
		self.policies = dict(policies)

	@classmethod
	def install(cls, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, policies: dict[str, int]) -> "FakeSystem":
		"""
		偽物を作り、net_priority の OS の操作と退避ファイルの場所を差し替える。

		- レジストリには、このツールが書く値 (旧ツールが on のままの状態) を入れておく

		Args:
			monkeypatch: pytest の monkeypatch。
			tmp_path: 退避ファイルを置く一時ディレクトリ。
			policies: 接頭辞ごとの、今あるポリシーの件数。
		Returns:
			差し替えた偽物。
		"""
		fake = cls(policies)
		for key, name, value_type, value in np.desired_registry():
			fake.registry[(key, name)] = (value_type, value)
		old, new = _paths(tmp_path)
		monkeypatch.setattr(np, "OLD_STATE_FILE", old)
		monkeypatch.setattr(np, "STATE_FILE", new)
		monkeypatch.setattr(np, "require_admin", lambda: None)
		monkeypatch.setattr(np, "reg_read", fake.read)
		monkeypatch.setattr(np, "reg_write", fake.write)
		monkeypatch.setattr(np, "reg_delete", fake.delete)
		monkeypatch.setattr(np, "remove_policies", fake.remove_policies)
		monkeypatch.setattr(np, "create_policies", lambda targets: None)
		monkeypatch.setattr(np, "warn_if_pacer_disabled", lambda: None)
		monkeypatch.setattr(np, "policy_targets", lambda: [(np.POLICY_PREFIX + "bot", tmp_path / "bot.exe", np.DSCP_BOT)])
		return fake

	def read(self, key: str, name: str) -> tuple[bool, int | None, object]:
		"""reg_read の偽物。"""
		if (key, name) in self.registry:
			value_type, value = self.registry[(key, name)]
			return True, value_type, value
		return False, None, None

	def write(self, key: str, name: str, value_type: int, value: object) -> None:
		"""reg_write の偽物。"""
		self.registry[(key, name)] = (value_type, value)

	def delete(self, key: str, name: str) -> None:
		"""reg_delete の偽物。"""
		self.registry.pop((key, name), None)

	def remove_policies(self, prefix: str = np.POLICY_PREFIX) -> int:
		"""remove_policies の偽物。消した件数を返す。"""
		return self.policies.pop(prefix, 0)


def test_off_after_old_tool_without_state_clears_tool_values(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	fake = FakeSystem.install(monkeypatch, tmp_path, policies={np.OLD_POLICY_PREFIX: 2})
	np.disable()
	assert fake.registry == {}
	assert fake.policies == {}


def test_off_after_old_tool_with_broken_state_clears_tool_values(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	fake = FakeSystem.install(monkeypatch, tmp_path, policies={np.OLD_POLICY_PREFIX: 1})
	np.OLD_STATE_FILE.parent.mkdir()
	np.OLD_STATE_FILE.write_text("{broken", encoding="utf-8")
	np.disable()
	assert fake.registry == {}


def test_on_after_old_tool_without_state_records_values_as_absent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	FakeSystem.install(monkeypatch, tmp_path, policies={np.OLD_POLICY_PREFIX: 1})
	np.enable()
	state = json.loads(np.STATE_FILE.read_text(encoding="utf-8"))
	assert [item["exists"] for item in state["registry"]] == [False] * len(np.desired_registry())


def test_takeover_with_broken_new_state_changes_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	fake = FakeSystem.install(monkeypatch, tmp_path, policies={np.OLD_POLICY_PREFIX: 1})
	np.STATE_FILE.parent.mkdir()
	np.STATE_FILE.write_text("{broken", encoding="utf-8")
	with pytest.raises(np.ToolError):
		np.disable()
	assert fake.policies == {np.OLD_POLICY_PREFIX: 1}


def test_off_without_any_tool_state_keeps_preexisting_values(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	fake = FakeSystem.install(monkeypatch, tmp_path, policies={})
	np.disable()
	assert len(fake.registry) == len(np.desired_registry())
