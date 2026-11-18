//! 試験で起動する Python の実行ファイルを決める。
//!
//! - 環境変数 `WATAPACHI_TEST_PYTHON` があれば、そのパスをそのまま使う (venv の python を試すときなど)
//! - 無ければ PATH の `python` に `sys.executable` を尋ね、ランチャーを通らない実体のパスを使う
//! - PATH の `python` が Python install manager のランチャー (`WindowsApps\python.exe`) だと、
//!   孫が試験の Job から抜けて残り、「終了しない」と誤判定する (開発機で確認)。
//!   原因はランチャーが子を入れる独自の Job の設定と推測しているが、未確認
//! - `sys.executable` が実体でなくエイリアスを返す環境 (Microsoft Store 版など) では、
//!   環境変数で実体のパスを指定する
//! - 結果は試験バイナリの中で 1 回だけ求めて使い回す
//!
//! 呼び出し方:
//! ```ignore
//! #[path = "support/python.rs"]
//! mod python;
//! let child = tokio::process::Command::new(python::test_python()).arg("-c").arg("pass").spawn();
//! ```

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::OnceLock;

/// 試験で使う Python を指定する環境変数の名前。
pub const TEST_PYTHON_ENV: &str = "WATAPACHI_TEST_PYTHON";

/// 実体のパスを尋ねるときに起動するコマンド。
const PATH_PYTHON: &str = "python";

/// 試験で起動する Python の実行ファイルのパスを返す。
///
/// - 戻り値: `WATAPACHI_TEST_PYTHON` の値、または PATH の `python` の `sys.executable`
/// - 失敗時: `python` を起動できない、または空の結果なら panic する (試験の前提が崩れているため)
///
/// # 例
/// `let python = test_python();`
pub fn test_python() -> PathBuf {
	static PYTHON: OnceLock<PathBuf> = OnceLock::new();
	PYTHON.get_or_init(resolve).clone()
}

/// 環境変数か `sys.executable` から Python のパスを求める。`test_python` からだけ呼ぶ。
fn resolve() -> PathBuf {
	if let Some(path) = std::env::var_os(TEST_PYTHON_ENV).filter(|v| !v.is_empty()) {
		return PathBuf::from(path);
	}
	let output = Command::new(PATH_PYTHON)
		.args(["-c", "import sys; print(sys.executable)"])
		.stdin(Stdio::null())
		.output()
		.expect("PATH の python を起動できること");
	assert!(
		output.status.success(),
		"sys.executable の取得に失敗: {}",
		String::from_utf8_lossy(&output.stderr)
	);
	let path = String::from_utf8_lossy(&output.stdout).trim().to_owned();
	assert!(!path.is_empty(), "sys.executable が空");
	PathBuf::from(path)
}
