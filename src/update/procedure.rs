//! yt-dlp の更新の手順。新しい環境 (venv) を作り、版を比べ、検査し、動いているワーカーを切り替え、記録する。
//!
//! - 環境の作成・版の読み取り・検査は `EnvTools`、動いているワーカーの切り替えは `WorkerSwitch` が担う
//! - 切り替えが `None` のとき (`update-worker`) は入れ替えずに記録だけ書き換える
//! - 作成・版の読み取り・検査・切り替えのどれかで失敗したら `current.json` は変えない。
//!   新しい環境は消す (切り替えで元へ戻せない台があるときだけ残す)
//! - `current.json` の読み書き、環境の名前の確保、消去、片付けはファイル操作 (fsync を含む) のため
//!   `spawn_blocking` で行い、非同期のスレッドを塞がない
//! - `run` を記録の前で打ち切れば (終了、Ctrl+C) `current.json` は書き換わらない。作りかけの環境は次回の片付けで消える。
//!   記録の書き込みが始まった後に打ち切っても、書き込みは最後まで行われる (一時ファイルからの置き換えなので壊れない)
//!
//! 呼び出し方:
//! ```no_run
//! # use std::sync::Arc;
//! # use watapachi_bot::update::envs::EnvStore;
//! # use watapachi_bot::update::procedure::{EnvTools, Updater};
//! # async fn demo(tools: Arc<dyn EnvTools>) {
//! let updater = Updater::new(EnvStore::new("worker/envs"), tools, true);
//! let outcome = updater.run(None, jiff::ZonedDateTime::now()).await;
//! # }
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;

use crate::resolver::pool::RollingRestartError;
use crate::update::envs::{EnvError, EnvRecord, EnvStore};

/// 更新の手順の失敗。
#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
	/// 環境の記録・ディレクトリの操作に失敗した
	#[error(transparent)]
	Env(#[from] EnvError),
	/// 外部コマンド (uv、python) の起動か実行に失敗した。`detail` は出力の末尾など
	#[error("{what} に失敗しました: {detail}")]
	Command { what: String, detail: String },
	/// 上限時間を超えた (内容は何が超えたか)
	#[error("{0} が上限時間を超えました")]
	Timeout(String),
	/// 新しい環境の検査 (起動、ping、解決) に失敗した
	#[error("新しい環境の検査に失敗しました: {0}")]
	Check(String),
}

/// 更新の手順のどの段で失敗したか。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateStage {
	/// 新しい環境の作成 (名前の確保、uv)
	Create,
	/// 版の読み取り
	Version,
	/// 新しい環境の検査
	Check,
	/// ワーカーの入れ替え
	Switch,
	/// `current.json` の書き換え
	Record,
}

impl UpdateStage {
	/// ログに出す段の名前。
	pub fn label(self) -> &'static str {
		match self {
			Self::Create => "create",
			Self::Version => "version",
			Self::Check => "check",
			Self::Switch => "switch",
			Self::Record => "record",
		}
	}
}

/// 1 回の更新の結果。
#[derive(Debug, PartialEq, Eq)]
pub enum UpdateOutcome {
	/// 新しい環境へ切り替えて記録した
	Updated { version: String },
	/// 版が同じで更新しなかった。`recycled` は同じ環境でワーカーを入れ替えたか
	Unchanged { version: String, recycled: bool },
	/// 途中で失敗した
	Failed { stage: UpdateStage, message: String },
	/// 入れ替えの途中で終了した
	Stopped,
}

/// 環境の作成と検査。本物は `UvTools`、試験は偽物に置き換える。
#[async_trait]
pub trait EnvTools: Send + Sync {
	/// venv を作り、パッケージを入れる。
	///
	/// # 引数
	/// - `dir`: 作る環境のディレクトリ (まだ無い)
	///
	/// # エラー
	/// コマンドの失敗、上限時間の超過
	async fn create(&self, dir: &Path) -> Result<(), UpdateError>;

	/// `<python> -m yt_dlp --version` の 1 行目。
	///
	/// # 引数
	/// - `python`: 環境の python.exe
	///
	/// # エラー
	/// コマンドの失敗、上限時間の超過、出力が空
	async fn yt_dlp_version(&self, python: &Path) -> Result<String, UpdateError>;

	/// 新しい環境で `WorkerProcess` を起動し、ping と `check_video` の resolve を確かめる。
	///
	/// # 引数
	/// - `python`: 環境の python.exe
	///
	/// # エラー
	/// 起動・ping・解決のどれかの失敗 (`UpdateError::Check`)、上限時間の超過
	async fn check(&self, python: &Path) -> Result<(), UpdateError>;
}

/// 動いているワーカーの切り替え。本物は `app::LiveSwitch`。
#[async_trait]
pub trait WorkerSwitch: Send + Sync {
	/// ワーカーを入れ替える。
	///
	/// - `Some` なら新しい python へ、`None` なら同じ環境で入れ替える
	/// - 成功したら、再生の入力 (PipeInput) の起動方法も新しい python へ差し替える
	///
	/// # 引数
	/// - `python`: 新しい環境の python.exe
	///
	/// # エラー
	/// `RollingRestartError` (入れ替えの失敗、終了による中断)
	async fn switch(&self, python: Option<PathBuf>) -> Result<(), RollingRestartError>;
}

/// yt-dlp の更新を 1 回行う。
pub struct Updater {
	store: EnvStore,
	tools: Arc<dyn EnvTools>,
	recycle: bool,
}

impl Updater {
	/// 更新の手順を作る。
	///
	/// # 引数
	/// - `store`: 環境の置き場
	/// - `tools`: 環境の作成と検査
	/// - `recycle`: 版が同じでもワーカーを入れ替えるか (`update.recycle_workers_daily`)
	///
	/// # 例
	/// `let updater = Updater::new(store, Arc::new(tools), config.update.recycle_workers_daily);`
	pub fn new(store: EnvStore, tools: Arc<dyn EnvTools>, recycle: bool) -> Self {
		Self {
			store,
			tools,
			recycle,
		}
	}

	/// 更新を 1 回行い、結果を 1 行でログに出す (成功は info、失敗は error)。
	///
	/// - 手順: 新しい環境の作成、版の比較、検査、切り替え、記録、片付け
	/// - 作成・版の読み取り・検査の失敗: 新しい環境を消す。`current.json` は変えず、片付けもしない
	/// - 版が `current` と同じ: 新しい環境を消し、片付ける。`recycle` かつ `switch` があれば同じ環境で入れ替える (失敗はログだけ)
	/// - 切り替えの失敗: `not_restored` が空なら新しい環境を消し、空でなければ (新しい環境で動く台が残りうる) 消さない。どちらも `current.json` は変えず、片付けもしない
	/// - 終了で中断 (`Stopped`): 何も消さず、`current.json` も変えない
	/// - 成功 (`switch` が `None` なら検査まで成功): `current.json` を新しい環境に書き、前の環境を `previous` にして片付ける
	/// - `current.json` が無い、壊れている、指す python が無いときは版を比べずに進み、成功したら書き直す
	///   (`previous` は `None`。python の無い環境は戻り先に使えないため残さず、片付けで消える)
	/// - 打ち切られても (Future の破棄) `current.json` は書き換わらない
	///
	/// # 引数
	/// - `switch`: 動いているワーカーの切り替え。`None` なら入れ替えずに記録だけ書き換える
	/// - `now`: 現在の現地時刻 (環境の名前と `created_at` に使う)
	///
	/// # 戻り値
	/// 結果 `UpdateOutcome`
	///
	/// # 例
	/// `let outcome = updater.run(Some(&live), jiff::ZonedDateTime::now()).await;`
	pub async fn run(
		&self,
		switch: Option<&dyn WorkerSwitch>,
		now: jiff::ZonedDateTime,
	) -> UpdateOutcome {
		let started = Instant::now();
		let outcome = self.execute(switch, now).await;
		log_outcome(&outcome, started);
		outcome
	}

	/// `run` の本体。ログは出さない。
	async fn execute(
		&self,
		switch: Option<&dyn WorkerSwitch>,
		now: jiff::ZonedDateTime,
	) -> UpdateOutcome {
		// python の有無もファイル操作なので、記録の読み取りと一緒にブロッキング用のスレッドで調べる
		let read = self
			.blocking(|store| {
				Ok(store.read_current()?.map(|record| {
					let has_python = record.python.is_file();
					(record, has_python)
				}))
			})
			.await;
		let current = match read {
			// 起動時の select_python と同じ判定。python の無い記録は直すために無いものとして扱う
			Ok(Some((record, false))) => {
				tracing::warn!(
					env = %record.name,
					python = %record.python.display(),
					"current.json の指す python が無いため、無いものとして更新します"
				);
				None
			}
			Ok(Some((record, true))) => Some(record),
			Ok(None) => None,
			Err(EnvError::Corrupt(reason)) => {
				tracing::warn!(%reason, "current.json が壊れているため、無いものとして更新します");
				None
			}
			Err(error) => return failed(UpdateStage::Create, &error),
		};
		let started_at = now.datetime();
		let (name, dir) = match self.blocking(move |store| store.allocate(started_at)).await {
			Ok(allocated) => allocated,
			Err(error) => return failed(UpdateStage::Create, &error),
		};
		let python = self.store.python_of(&name);

		if let Err(error) = self.tools.create(&dir).await {
			self.discard(&name).await;
			return failed(UpdateStage::Create, &error);
		}
		let version = match self.tools.yt_dlp_version(&python).await {
			Ok(version) => version,
			Err(error) => {
				self.discard(&name).await;
				return failed(UpdateStage::Version, &error);
			}
		};

		if current
			.as_ref()
			.is_some_and(|record| record.yt_dlp_version == version)
		{
			self.discard(&name).await;
			let recycled = self.recycle_in_place(switch).await;
			self.cleanup().await;
			return UpdateOutcome::Unchanged { version, recycled };
		}

		if let Err(error) = self.tools.check(&python).await {
			self.discard(&name).await;
			return failed(UpdateStage::Check, &error);
		}

		if let Some(switch) = switch {
			match switch.switch(Some(python.clone())).await {
				Ok(()) => {}
				Err(RollingRestartError::Stopped) => return UpdateOutcome::Stopped,
				Err(error) => {
					// 元の環境へ戻せなかった台が無ければ、新しい環境を使う台は無い
					let restored = matches!(
						&error,
						RollingRestartError::Failed { not_restored, .. } if not_restored.is_empty()
					);
					if restored {
						self.discard(&name).await;
					} else {
						tracing::error!(
							env = %name,
							%error,
							"元の環境へ戻せないワーカーがあるため、新しい環境を残します"
						);
					}
					return failed(UpdateStage::Switch, &error);
				}
			}
		}

		let record = EnvRecord {
			name,
			python,
			yt_dlp_version: version.clone(),
			created_at: now.timestamp().as_second(),
			previous: current.map(|record| record.name),
		};
		if let Err(error) = self
			.blocking(move |store| store.write_current(&record))
			.await
		{
			return failed(UpdateStage::Record, &error);
		}
		self.cleanup().await;
		UpdateOutcome::Updated { version }
	}

	/// 版が同じときに、同じ環境でワーカーを入れ替える。
	///
	/// # 戻り値
	/// 入れ替えた (成功した) か。`recycle` が偽、切り替えが無い、失敗のときは false
	async fn recycle_in_place(&self, switch: Option<&dyn WorkerSwitch>) -> bool {
		let Some(switch) = switch.filter(|_| self.recycle) else {
			return false;
		};
		match switch.switch(None).await {
			Ok(()) => true,
			Err(error) => {
				tracing::warn!(%error, "同じ環境でのワーカーの入れ替えに失敗しました");
				false
			}
		}
	}

	/// 置き場へのファイル操作を、ブロッキング用のスレッドで行う。
	///
	/// - 置き場の複製を渡す (`EnvStore` はパスだけを持つ)
	/// - Future を破棄しても、始まった操作は最後まで行われる
	///
	/// # 引数
	/// - `f`: 置き場を受け取る操作
	///
	/// # 戻り値
	/// `f` の戻り値
	///
	/// # エラー
	/// `f` のエラー。ブロッキングのタスクが異常終了したら `EnvError::Io` (`ErrorKind::Other`)
	///
	/// # 例
	/// `let current = self.blocking(|store| store.read_current()).await?;`
	async fn blocking<T, F>(&self, f: F) -> Result<T, EnvError>
	where
		T: Send + 'static,
		F: FnOnce(&EnvStore) -> Result<T, EnvError> + Send + 'static,
	{
		let store = self.store.clone();
		tokio::task::spawn_blocking(move || f(&store))
			.await
			.map_err(|error| EnvError::Io(std::io::Error::other(error.to_string())))?
	}

	/// 新しい環境を消す。失敗はログだけにする (次回の片付けで消し直す)。
	async fn discard(&self, name: &str) {
		let store = self.store.clone();
		let target = name.to_owned();
		match tokio::task::spawn_blocking(move || store.discard(&target)).await {
			Ok(Ok(())) => {}
			Ok(Err(error)) => tracing::warn!(env = %name, %error, "新しい環境を消せませんでした"),
			Err(error) => {
				tracing::warn!(env = %name, %error, "新しい環境を消す処理が異常終了しました")
			}
		}
	}

	/// 古い環境を片付ける。失敗と使用中で残ったものはログだけにする。
	async fn cleanup(&self) {
		let store = self.store.clone();
		match tokio::task::spawn_blocking(move || store.cleanup()).await {
			Ok(Ok(report)) => {
				if !report.removed.is_empty() {
					tracing::info!(removed = ?report.removed, "古い環境を消しました");
				}
				if !report.kept_busy.is_empty() {
					tracing::warn!(kept = ?report.kept_busy, "使用中などで消せない環境を残しました。次回に消します");
				}
			}
			Ok(Err(error)) => tracing::warn!(%error, "環境の片付けに失敗しました"),
			Err(error) => tracing::warn!(%error, "環境の片付けが異常終了しました"),
		}
	}
}

/// 失敗の結果を作る。
fn failed(stage: UpdateStage, error: &dyn std::fmt::Display) -> UpdateOutcome {
	UpdateOutcome::Failed {
		stage,
		message: error.to_string(),
	}
}

/// 結果を 1 行でログに出す。成功は info、失敗は error。
///
/// # 引数
/// - `outcome`: 結果
/// - `started`: 開始時刻 (かかった時間を出す)
fn log_outcome(outcome: &UpdateOutcome, started: Instant) {
	let elapsed_secs = started.elapsed().as_secs_f64();
	match outcome {
		UpdateOutcome::Updated { version } => {
			tracing::info!(%version, elapsed_secs, "yt-dlp を更新しました");
		}
		UpdateOutcome::Unchanged { version, recycled } => {
			tracing::info!(%version, recycled, elapsed_secs, "yt-dlp は更新なしです");
		}
		UpdateOutcome::Failed { stage, message } => {
			tracing::error!(stage = stage.label(), %message, elapsed_secs, "yt-dlp の更新に失敗しました");
		}
		UpdateOutcome::Stopped => {
			tracing::info!(elapsed_secs, "終了のため yt-dlp の更新を中断しました");
		}
	}
}
