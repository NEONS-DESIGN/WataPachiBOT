//! ワーカーの環境 (uv の venv) の記録と、起動時に使う python の選択。
//!
//! - 環境は `<root>/<名前>/` に置く。名前は作った時刻 (`%Y%m%d-%H%M%S`)
//! - 今使う環境は `<root>/current.json` に記録する。書き換えは一時ファイルを経由して置き換える
//! - 片付けは「使用中かを調べ、使用中でなければ名前を `<名前>.deleting` に変えてから消す」
//! - 使用中の判定は、環境の中の exe / pyd / dll を書き込みで開けるかで行う。
//!   Windows は動いている exe や読み込み中の pyd を含むディレクトリでも名前の変更を許すため、名前の変更では判定できない
//! - 起動時の python は `current.json` の環境 → その `previous` の環境 → 設定の `paths.worker_python` の順に、
//!   実在するものを選ぶ。`current.json` が無い、壊れているときは `paths.worker_python` で起動する
//!
//! 呼び出し方:
//! ```no_run
//! use std::path::Path;
//! use watapachi_bot::update::envs::{select_python, EnvStore};
//! let store = EnvStore::new("worker/envs");
//! let (python, source) = select_python(&store, Path::new("worker/.venv/Scripts/python.exe"));
//! ```

use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use windows_sys::Win32::Foundation::ERROR_SHARING_VIOLATION;

/// 現在の環境を記録するファイル名。
pub const CURRENT_FILE: &str = "current.json";
/// 消す途中の環境に付ける接尾辞。
pub const DELETING_SUFFIX: &str = ".deleting";
/// `current.json` を書くときの一時ファイルの接尾辞。
const TMP_SUFFIX: &str = ".tmp";
/// 環境の名前の時刻の書式。
const NAME_FORMAT: &str = "%Y%m%d-%H%M%S";
/// 環境の中の python.exe の相対パス。
pub const PYTHON_RELATIVE: [&str; 2] = ["Scripts", "python.exe"];
/// 使用中かを調べるファイルの拡張子 (小文字で比べる)。動いている exe と読み込み中の DLL は書き込みで開けない。
const PROBE_EXTENSIONS: [&str; 3] = ["exe", "pyd", "dll"];

/// 1 つの環境の記録。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvRecord {
	/// 環境の名前 (`<root>` 直下のディレクトリ名)
	pub name: String,
	/// その環境の python.exe。例: `worker/envs/20261118-040000/Scripts/python.exe`
	pub python: PathBuf,
	/// 入っている yt-dlp のバージョン
	pub yt_dlp_version: String,
	/// 作った時刻 (UNIX 秒)
	pub created_at: i64,
	/// 直前に使っていた環境の名前
	pub previous: Option<String>,
}

/// 環境の記録の操作の失敗。
#[derive(Debug, thiserror::Error)]
pub enum EnvError {
	/// ファイルやディレクトリの操作に失敗した
	#[error("環境の操作に失敗しました: {0}")]
	Io(#[from] io::Error),
	/// 環境の中のファイルを使っているプロセスがある。値は使用中と分かったファイル
	#[error("環境が使用中です: {}", .0.display())]
	InUse(PathBuf),
	/// `current.json` を読めない
	#[error("current.json が壊れています: {0}")]
	Corrupt(String),
}

/// 片付けの結果。
#[derive(Debug, Default, PartialEq, Eq)]
pub struct CleanupReport {
	/// 消した名前 (名前順)
	pub removed: Vec<String>,
	/// 使用中のため、または名前を変えられず、または消しきれず、残した名前 (名前順)
	pub kept_busy: Vec<String>,
}

/// 環境の置き場 (`paths.worker_envs`) への操作。
#[derive(Debug, Clone)]
pub struct EnvStore {
	root: PathBuf,
	/// 片付けで消さない python (設定の `paths.worker_python`)。置き場の下の環境を指していれば、その環境を残す
	kept_python: Option<PathBuf>,
}

impl EnvStore {
	/// 置き場を指定して作る。ディレクトリは作らない。
	///
	/// # 引数
	/// - `root`: 環境の置き場 (`paths.worker_envs`)
	///
	/// # 例
	/// `let store = EnvStore::new("worker/envs");`
	pub fn new(root: impl Into<PathBuf>) -> Self {
		Self {
			root: root.into(),
			kept_python: None,
		}
	}

	/// 片付けで消さない python を指定する (予備の `paths.worker_python`)。
	///
	/// - その python が置き場の下の環境にあれば、`cleanup` はその環境を current / previous と同じく残す
	/// - 置き場の外を指していれば何も変わらない
	///
	/// # 引数
	/// - `python`: 設定の `paths.worker_python`
	///
	/// # 戻り値
	/// 指定を足した置き場
	///
	/// # 例
	/// `let store = EnvStore::new(&config.paths.worker_envs).keep_python(&config.paths.worker_python);`
	pub fn keep_python(mut self, python: impl Into<PathBuf>) -> Self {
		self.kept_python = Some(python.into());
		self
	}

	/// python のパスが置き場の直下のどの環境に属するか。
	///
	/// - まず字面で `root` を取り除いて調べ、だめなら両方を正規化 (`canonicalize`) して調べる。
	///   相対と絶対の違い、大文字と小文字の違いは正規化で吸収する (実在しないパスは字面だけで調べる)
	///
	/// # 引数
	/// - `python`: 調べる python.exe のパス
	///
	/// # 戻り値
	/// 環境の名前。置き場の外、または置き場の直下のファイルなら `None`
	///
	/// # 例
	/// `let name = store.env_name_of(Path::new("worker/envs/20261118-040000/Scripts/python.exe"));`
	fn env_name_of(&self, python: &Path) -> Option<String> {
		let first_dir = |relative: &Path| {
			let mut components = relative.components();
			match (components.next(), components.next()) {
				(Some(std::path::Component::Normal(name)), Some(_)) => {
					name.to_str().map(str::to_owned)
				}
				_ => None,
			}
		};
		if let Some(name) = python.strip_prefix(&self.root).ok().and_then(first_dir) {
			return Some(name);
		}
		let root = fs::canonicalize(&self.root).ok()?;
		let python = fs::canonicalize(python).ok()?;
		python.strip_prefix(&root).ok().and_then(first_dir)
	}

	/// `current.json` のパス。
	fn current_path(&self) -> PathBuf {
		self.root.join(CURRENT_FILE)
	}

	/// `current.json` を読む。
	///
	/// # 戻り値
	/// 記録。ファイルが無ければ `None`
	///
	/// # エラー
	/// - 読み取りに失敗した場合は `EnvError::Io`
	/// - JSON として読めない場合は `EnvError::Corrupt`
	///
	/// # 例
	/// `let current = store.read_current()?;`
	pub fn read_current(&self) -> Result<Option<EnvRecord>, EnvError> {
		let bytes = match fs::read(self.current_path()) {
			Ok(bytes) => bytes,
			Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
			Err(error) => return Err(error.into()),
		};
		serde_json::from_slice(&bytes)
			.map(Some)
			.map_err(|error| EnvError::Corrupt(error.to_string()))
	}

	/// `current.json` を書く。
	///
	/// - 置き場が無ければ作る
	/// - 一時ファイルに書いてから `std::fs::rename` で置き換える (途中で止まっても古い記録が残る)
	/// - 置き換えの前に一時ファイルを `sync_all` でディスクへ書き出す (電源断で中身の無い記録が残らないように)
	///
	/// # 引数
	/// - `record`: 書く記録
	///
	/// # エラー
	/// 書き込みか置き換えに失敗した場合は `EnvError::Io`。失敗時は一時ファイルを消す
	///
	/// # 例
	/// `store.write_current(&record)?;`
	pub fn write_current(&self, record: &EnvRecord) -> Result<(), EnvError> {
		fs::create_dir_all(&self.root)?;
		let json = serde_json::to_vec_pretty(record)
			.map_err(|error| EnvError::Io(io::Error::new(io::ErrorKind::InvalidData, error)))?;
		let tmp = self.root.join(format!("{CURRENT_FILE}{TMP_SUFFIX}"));
		let result = write_synced(&tmp, &json).and_then(|()| fs::rename(&tmp, self.current_path()));
		if result.is_err() {
			// 残った一時ファイルは次回の書き込みで上書きされるが、置いたままにしない
			let _ = fs::remove_file(&tmp);
		}
		Ok(result?)
	}

	/// 新しい環境の名前とディレクトリを決める。
	///
	/// - 名前は `now` の `%Y%m%d-%H%M%S`。同じ名前が既にあれば `-2`、`-3` と足す
	/// - 置き場は作るが、環境のディレクトリ自体は作らない (uv venv が作る)
	///
	/// # 引数
	/// - `now`: 現在の現地時刻
	///
	/// # 戻り値
	/// (名前, 環境のディレクトリ)
	///
	/// # エラー
	/// 置き場を作れない場合は `EnvError::Io`
	///
	/// # 例
	/// `let (name, dir) = store.allocate(jiff::ZonedDateTime::now().datetime())?;`
	pub fn allocate(&self, now: jiff::civil::DateTime) -> Result<(String, PathBuf), EnvError> {
		fs::create_dir_all(&self.root)?;
		let base = now.strftime(NAME_FORMAT).to_string();
		let mut name = base.clone();
		let mut serial = 1_u32;
		while self.taken(&name) {
			serial += 1;
			name = format!("{base}-{serial}");
		}
		let dir = self.root.join(&name);
		Ok((name, dir))
	}

	/// 名前が使用済みか (消す途中の残骸も使用済みとみなす)。
	fn taken(&self, name: &str) -> bool {
		self.root.join(name).exists() || self.root.join(deleting_name(name)).exists()
	}

	/// 環境の python.exe のパス。
	///
	/// # 引数
	/// - `name`: 環境の名前
	///
	/// # 戻り値
	/// `<root>/<name>/Scripts/python.exe`
	///
	/// # 例
	/// `let python = store.python_of("20261118-040000");`
	pub fn python_of(&self, name: &str) -> PathBuf {
		let mut path = self.root.join(name);
		path.extend(PYTHON_RELATIVE);
		path
	}

	/// 環境を 1 つ消す。
	///
	/// - 使用中か (`ensure_idle`) を先に調べ、使用中なら名前も変えずに何も消さない
	/// - 使用中でなければ名前を `<name>.deleting` に変えてから中身を消す
	/// - 前回の消し残しの `<name>.deleting` があれば、同じ判定をしてから先に消す (使用中か消せなければ失敗)
	/// - 判定の後に使われ始めた場合は、名前の変更か削除の失敗で止まる (途中まで消えることはありうる)
	///
	/// # 引数
	/// - `name`: 環境の名前
	///
	/// # エラー
	/// - 環境か消し残しが使用中の場合は `EnvError::InUse`
	/// - 調べられない、名前を変えられない、消しきれなかった場合は `EnvError::Io`
	///
	/// # 例
	/// `store.discard("20261118-040000")?;`
	pub fn discard(&self, name: &str) -> Result<(), EnvError> {
		let from = self.root.join(name);
		let to = self.root.join(deleting_name(name));
		if to.exists() {
			remove_if_idle(&to)?;
		}
		ensure_idle(&from)?;
		fs::rename(&from, &to)?;
		fs::remove_dir_all(&to)?;
		Ok(())
	}

	/// `current` と `previous` 以外の環境と、`.deleting` の残骸を消す。
	///
	/// - `keep_python` で指定した python が置き場の下の環境にあれば、その環境も残す
	///   (current と previous の python が無いときの最後の予備のため)
	/// - `current.json` が無ければ何もしない (どれが現役か分からないため)
	/// - `root` 直下のディレクトリだけを見る。`current.json` と一時ファイルは触らない
	/// - 使用中の環境 (`ensure_idle` で判定) は名前も変えずに `kept_busy` に入れる。途中まで消すことはしない
	/// - 名前を変えられない、消しきれない環境も `kept_busy` に入れる
	/// - `.deleting` の残骸も同じ判定をしてから消す。使用中か消せなければ `kept_busy` に入れ、次回に回す
	///
	/// # 戻り値
	/// 片付けの結果
	///
	/// # エラー
	/// `current.json` を読めない (壊れている) 場合、置き場を列挙できない場合
	///
	/// # 例
	/// `let report = store.cleanup()?;`
	pub fn cleanup(&self) -> Result<CleanupReport, EnvError> {
		let Some(current) = self.read_current()? else {
			return Ok(CleanupReport::default());
		};
		let fallback = self
			.kept_python
			.as_deref()
			.and_then(|python| self.env_name_of(python));
		let mut keep: BTreeSet<&str> = BTreeSet::new();
		keep.insert(current.name.as_str());
		if let Some(previous) = &current.previous {
			keep.insert(previous.as_str());
		}
		if let Some(fallback) = &fallback {
			keep.insert(fallback.as_str());
		}

		let mut residues = BTreeSet::new();
		let mut envs = BTreeSet::new();
		for entry in fs::read_dir(&self.root)? {
			let entry = entry?;
			if !entry.file_type()?.is_dir() {
				continue;
			}
			let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
				continue;
			};
			if name.ends_with(DELETING_SUFFIX) {
				residues.insert(name);
			} else if !keep.contains(name.as_str()) {
				envs.insert(name);
			}
		}

		let mut report = CleanupReport::default();
		// 残骸を先に消す。同じ名前の .deleting が残っていると、名前の変更が衝突するため
		for name in residues {
			match remove_if_idle(&self.root.join(&name)) {
				Ok(()) => report.removed.push(name),
				Err(_) => report.kept_busy.push(name),
			}
		}
		for name in envs {
			match self.discard(&name) {
				Ok(()) => report.removed.push(name),
				Err(_) => report.kept_busy.push(name),
			}
		}
		report.removed.sort();
		report.kept_busy.sort();
		Ok(report)
	}
}

/// 消す途中の名前。
fn deleting_name(name: &str) -> String {
	format!("{name}{DELETING_SUFFIX}")
}

/// ファイルを作り直して書き、ディスクへ書き出してから閉じる。
///
/// # 引数
/// - `path`: 書くファイル (あれば上書きする)
/// - `bytes`: 中身
///
/// # エラー
/// 作成、書き込み、書き出しのどれかの失敗
///
/// # 例
/// `write_synced(&tmp, b"{}")?;`
fn write_synced(path: &Path, bytes: &[u8]) -> io::Result<()> {
	let mut file = fs::File::create(path)?;
	file.write_all(bytes)?;
	file.sync_all()
}

/// 使用中でなければディレクトリを消す。
///
/// # 引数
/// - `dir`: 消すディレクトリ (`.deleting` の残骸)
///
/// # エラー
/// 使用中なら `EnvError::InUse`。調べられない、消しきれない場合は `EnvError::Io`
///
/// # 例
/// `remove_if_idle(&root.join("20261118-040000.deleting"))?;`
fn remove_if_idle(dir: &Path) -> Result<(), EnvError> {
	ensure_idle(dir)?;
	fs::remove_dir_all(dir)?;
	Ok(())
}

/// 環境 (またはその残骸) を使っているプロセスが無いことを確かめる。
///
/// # 引数
/// - `dir`: 環境のディレクトリ
///
/// # エラー
/// 使用中のファイルがあれば `EnvError::InUse`。`dir` を列挙できなければ `EnvError::Io`
///
/// # 例
/// `ensure_idle(&root.join("20261118-040000"))?;`
fn ensure_idle(dir: &Path) -> Result<(), EnvError> {
	match find_busy_file(dir)? {
		Some(path) => Err(EnvError::InUse(path)),
		None => Ok(()),
	}
}

/// ディレクトリの下から、使用中のファイルを 1 つ探す。
///
/// - 拡張子が `PROBE_EXTENSIONS` のファイルを再帰的に探し、書き込みで開けるかを試す。開くだけで中身は変えない
/// - 共有違反 (`ERROR_SHARING_VIOLATION`) で開けなければ使用中とする。
///   動いている exe (環境の python.exe など) と、読み込み中の pyd / dll がこれに当たる (開発機で確認)
/// - それ以外の理由で開けない (途中で消えた、読み取り専用など) ファイルは使用中とみなさない
/// - 途中で消えたサブディレクトリは飛ばす。シンボリックリンクとジャンクションはたどらない
/// - 調べた後に使われ始めることは防げない
///
/// # 引数
/// - `dir`: 調べるディレクトリ
///
/// # 戻り値
/// 使用中と分かった最初のファイル。無ければ `None`
///
/// # エラー
/// `dir` 自体を列挙できない場合 (無い場合を含む)、サブディレクトリの列挙が消えた以外の理由で失敗した場合
///
/// # 例
/// `if let Some(path) = find_busy_file(&env_dir)? { /* 使用中 */ }`
fn find_busy_file(dir: &Path) -> io::Result<Option<PathBuf>> {
	let mut pending = vec![dir.to_path_buf()];
	let mut is_root = true;
	while let Some(current) = pending.pop() {
		let entries = match fs::read_dir(&current) {
			Ok(entries) => entries,
			Err(error) if !is_root && error.kind() == io::ErrorKind::NotFound => continue,
			Err(error) => return Err(error),
		};
		is_root = false;
		for entry in entries {
			let entry = entry?;
			let file_type = entry.file_type()?;
			let path = entry.path();
			if file_type.is_dir() {
				pending.push(path);
			} else if file_type.is_file() && has_probe_extension(&path) && is_busy(&path) {
				return Ok(Some(path));
			}
		}
	}
	Ok(None)
}

/// 拡張子が `PROBE_EXTENSIONS` のどれかか (大文字小文字を区別しない)。
///
/// # 引数
/// - `path`: ファイルのパス
///
/// # 戻り値
/// 調べる対象なら true
///
/// # 例
/// `assert!(has_probe_extension(Path::new("Lib/_ssl.PYD")));`
fn has_probe_extension(path: &Path) -> bool {
	path.extension()
		.and_then(|extension| extension.to_str())
		.is_some_and(|extension| {
			PROBE_EXTENSIONS
				.iter()
				.any(|probe| extension.eq_ignore_ascii_case(probe))
		})
}

/// ファイルを書き込みで開き、共有違反で開けなければ使用中とする。開けたらすぐ閉じる。
///
/// - 作成も切り詰めもしない (`write(true)` だけを付ける)
///
/// # 引数
/// - `path`: 調べるファイル
///
/// # 戻り値
/// 共有違反で開けなければ true。開けた、または他の理由で開けなければ false
///
/// # 例
/// `let busy = is_busy(&python);`
fn is_busy(path: &Path) -> bool {
	match fs::OpenOptions::new().write(true).open(path) {
		Ok(_) => false,
		Err(error) => error
			.raw_os_error()
			.is_some_and(|code| u32::try_from(code).ok() == Some(ERROR_SHARING_VIOLATION)),
	}
}

/// 起動時に使う python を選ぶ。
///
/// - `current.json` が読めて、指す python が実在すれば、それを使う
/// - 指す python が無ければ、記録の `previous` の環境の python (`python_of`) が実在すれば、それを使う。
///   `previous` が置き場の直下の名前でなければ使わない
/// - それ以外 (記録なし、壊れている、どちらの python も無い) は `fallback` を使い、理由を返す
///
/// # 引数
/// - `store`: 環境の置き場
/// - `fallback`: 設定の `paths.worker_python`
///
/// # 戻り値
/// (使う python, どれを使ったか)
///
/// # 例
/// `let (python, source) = select_python(&store, &config.paths.worker_python);`
pub fn select_python(store: &EnvStore, fallback: &Path) -> (PathBuf, PythonSource) {
	let fall = |reason| (fallback.to_path_buf(), PythonSource::Fallback(reason));
	let record = match store.read_current() {
		Ok(Some(record)) => record,
		Ok(None) => return fall(FallbackReason::NoRecord),
		Err(_) => return fall(FallbackReason::Corrupt),
	};
	if record.python.is_file() {
		return (record.python, PythonSource::Current(record.name));
	}
	let previous = record
		.previous
		.filter(|name| is_plain_name(name))
		.map(|name| (store.python_of(&name), name))
		.filter(|(python, _)| python.is_file());
	match previous {
		Some((python, name)) => (
			python,
			PythonSource::Previous {
				name,
				current: record.name,
			},
		),
		None => fall(FallbackReason::MissingPython),
	}
}

/// 置き場の直下の 1 つの名前か (区切りや `..` を含まないか)。
///
/// # 引数
/// - `name`: 環境の名前
///
/// # 戻り値
/// 1 つの通常の名前なら true
///
/// # 例
/// `assert!(!is_plain_name("../x"));`
fn is_plain_name(name: &str) -> bool {
	let mut components = Path::new(name).components();
	matches!(
		(components.next(), components.next()),
		(Some(std::path::Component::Normal(_)), None)
	)
}

/// 起動時にどの python を使ったか。
#[derive(Debug, PartialEq, Eq)]
pub enum PythonSource {
	/// `current.json` の環境 (名前)
	Current(String),
	/// `current.json` の環境の python が無いため、直前の環境を使った
	Previous {
		/// 使った直前の環境の名前
		name: String,
		/// python が無かった `current.json` の環境の名前
		current: String,
	},
	/// 設定の `paths.worker_python` (current と previous を使わなかった理由)
	Fallback(FallbackReason),
}

/// `current.json` の環境を使わなかった理由。
#[derive(Debug, PartialEq, Eq)]
pub enum FallbackReason {
	/// `current.json` が無い
	NoRecord,
	/// `current.json` を読めない
	Corrupt,
	/// 記録が指す python も、直前の環境 (`previous`) の python も無い
	MissingPython,
}
