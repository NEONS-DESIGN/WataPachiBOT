//! ワーカー 1 台のプロセスと、その標準入出力での 1 往復。
//! - 1 台は同時に 1 件だけ処理する。`call` は `&mut self` で直列になる
//! - 標準エラーは起動時に作るタスクが読み続ける。読まないとパイプが詰まりワーカーが止まる
//! - venv の python.exe はランチャーで、実体は子プロセスになる。そのためプロセスごとに
//!   Job を持ち、終了時はこの Job を閉じてツリー全体を落とす
//! - 呼び出し元 (`pool.rs`) は `call` が `Rejected` 以外を返したらこの台を使い続けない

use std::io;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::task::JoinHandle;

use crate::domain::ResolveErrorCode;
use crate::platform::job::Job;
use crate::resolver::protocol::{parse_response, WorkerRequest, WorkerResponse};

/// コンソールを持たせない。Ctrl+C がワーカーへ直接届くと、Bot が順に止める前にワーカーが先に終わる。
const CREATE_NO_WINDOW: u32 = 0x0800_0000;
/// 強制終了の後、プロセスの回収を待つ上限。Job で落としているため通常はすぐ終わる。
const KILL_WAIT: Duration = Duration::from_secs(5);
/// 終了後に標準エラーの残りを読み切る上限。異常終了の原因を記録に残すために待つ。
const STDERR_FLUSH_WAIT: Duration = Duration::from_secs(1);
/// 標準エラーの 1 レコードの上限。超えた行は分割して記録し、メモリの使用量を抑える。
const STDERR_RECORD_BYTES: u64 = 8 * 1024;
/// 標準出力の読み取りバッファ。Windows の Tokio は子プロセスのパイプを 1 回の読み取りごとに
/// ブロッキング用のスレッドへ渡すため、既定の 8KB では大きな応答 (10MB) で往復が千回を超え、
/// 負荷が高いと 30 秒を超えた。Tokio の 1 回の読み取りの上限 (2MB) 以下で大きく取る。
const STDOUT_BUFFER_BYTES: usize = 1024 * 1024;

/// ワーカーの起動方法。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerSpec {
	/// Python の実行ファイル。venv の更新で切り替わる。
	pub python: PathBuf,
	/// worker.py のパス。
	pub script: PathBuf,
	/// スクリプトへの引数。`--request-budget-secs` はプールが足すため含めない。
	pub args: Vec<String>,
}

/// `WorkerProcess::call` の失敗。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CallError {
	/// ワーカーがエラーコード付きで返した。ワーカーは正常で、続けて使える。
	#[error("ワーカーのエラー: {code:?}: {message}")]
	Rejected {
		code: ResolveErrorCode,
		message: String,
	},
	/// 上限までに応答が無かった。
	#[error("ワーカーの応答がタイムアウトしました")]
	Timeout,
	/// 標準入出力が閉じた。プロセスの終了を意味する。
	#[error("ワーカーが終了しました")]
	Died,
	/// 応答の形が不正、または送信する要求を作れない。
	#[error("ワーカーとの通信が不正です: {0}")]
	Protocol(String),
}

/// 起動済みのワーカー 1 台。
///
/// - 破棄するとプロセスツリーごと終了する。回収まで待つなら `close` か `kill` を呼ぶ
#[derive(Debug)]
pub struct WorkerProcess {
	child: Child,
	stdin: Option<ChildStdin>,
	stdout: BufReader<ChildStdout>,
	stderr_task: Option<JoinHandle<()>>,
	/// このワーカー専用の Job。閉じるとプロセスツリーが終了する。
	tree: Option<Job>,
	next_id: u64,
}

impl WorkerProcess {
	/// ワーカーを起動して Job に入れる。
	///
	/// - `PYTHONUTF8=1` を設定する
	/// - プール全体の Job の内側に、このワーカー専用の Job を入れ子にする
	/// - Tokio のランタイムの中で呼ぶこと
	///
	/// # 引数
	/// - `spec`: 起動方法
	/// - `parent`: プール全体の Job
	///
	/// # 戻り値
	/// 起動済みのワーカー。ワーカーの初期化の完了は待たない
	///
	/// # エラー
	/// 実行ファイルが無い場合、Job への割り当てに失敗した場合
	///
	/// # 例
	/// `let mut worker = WorkerProcess::spawn(&spec, &job)?;`
	pub fn spawn(spec: &WorkerSpec, parent: &Job) -> io::Result<Self> {
		let tree = Job::new()?;
		let mut child = Command::new(&spec.python)
			.arg(&spec.script)
			.args(&spec.args)
			.env("PYTHONUTF8", "1")
			.stdin(Stdio::piped())
			.stdout(Stdio::piped())
			.stderr(Stdio::piped())
			.kill_on_drop(true)
			.creation_flags(CREATE_NO_WINDOW)
			.spawn()?;
		// ランチャーが実体を起動する前に入れるため、他の処理を挟まない
		parent.assign(&child)?;
		tree.assign(&child)?;

		let stdin = child.stdin.take().ok_or_else(|| missing_pipe("stdin"))?;
		let stdout = child.stdout.take().ok_or_else(|| missing_pipe("stdout"))?;
		let stderr = child.stderr.take().ok_or_else(|| missing_pipe("stderr"))?;
		let stderr_task = tokio::spawn(drain_stderr(stderr, child.id()));
		Ok(Self {
			child,
			stdin: Some(stdin),
			stdout: BufReader::with_capacity(STDOUT_BUFFER_BYTES, stdout),
			stderr_task: Some(stderr_task),
			tree: Some(tree),
			next_id: 0,
		})
	}

	/// 要求を 1 件送り、応答を待つ。
	///
	/// - 送信も含めて `limit` で打ち切る
	/// - 打ち切ると応答が途中で残るため、`Rejected` 以外の後はこの台を使わないこと
	///
	/// # 引数
	/// - `request`: 要求
	/// - `limit`: 応答を待つ上限
	///
	/// # 戻り値
	/// 成功応答の `result`
	///
	/// # エラー
	/// - ワーカーが失敗を返した: `CallError::Rejected`
	/// - 上限を超えた: `CallError::Timeout`
	/// - 標準入出力が閉じた: `CallError::Died`
	/// - 応答が解釈できない、id が合わない: `CallError::Protocol`
	///
	/// # 例
	/// `let pong = worker.call(&WorkerRequest::Ping, Duration::from_secs(30)).await?;`
	pub async fn call(
		&mut self,
		request: &WorkerRequest,
		limit: Duration,
	) -> Result<Value, CallError> {
		self.next_id += 1;
		let id = self.next_id;
		let mut line = request
			.to_line(id)
			.map_err(|error| CallError::Protocol(error.to_string()))?;
		line.push('\n');
		match tokio::time::timeout(limit, self.exchange(&line, id)).await {
			Ok(result) => result,
			Err(_) => Err(CallError::Timeout),
		}
	}

	/// 1 行送って 1 行受け取る。`call` から呼ぶ。
	///
	/// # 引数
	/// - `line`: 改行まで含む要求
	/// - `id`: 要求の番号
	///
	/// # 戻り値
	/// 成功応答の `result`
	///
	/// # エラー
	/// `call` と同じ (`Timeout` を除く)
	///
	/// # 例
	/// `self.exchange("{...}\n", 1).await?;`
	async fn exchange(&mut self, line: &str, id: u64) -> Result<Value, CallError> {
		let stdin = self.stdin.as_mut().ok_or(CallError::Died)?;
		stdin
			.write_all(line.as_bytes())
			.await
			.map_err(|_| CallError::Died)?;
		stdin.flush().await.map_err(|_| CallError::Died)?;

		// 長さの制限を設けない。プレイリストの応答は大きくなる
		let mut response = String::new();
		match self.stdout.read_line(&mut response).await {
			Ok(0) => return Err(CallError::Died),
			Ok(_) => {}
			Err(error) if error.kind() == io::ErrorKind::InvalidData => {
				return Err(CallError::Protocol(error.to_string()));
			}
			Err(_) => return Err(CallError::Died),
		}
		let parsed = parse_response(response.trim_end())
			.map_err(|error| CallError::Protocol(error.to_string()))?;
		match parsed {
			WorkerResponse::Success {
				id: Some(answered),
				result,
			} if answered == id => Ok(result),
			// 要求を解釈できなかった場合のワーカーの失敗応答は id が null になる
			WorkerResponse::Failure {
				id: answered,
				code,
				message,
			} if answered.is_none() || answered == Some(id) => Err(CallError::Rejected {
				code: code.into(),
				message,
			}),
			_ => Err(CallError::Protocol(format!(
				"要求 {id} と応答の id が一致しません"
			))),
		}
	}

	/// 要求が無いときに標準出力を見張る。EOF か、頼んでいない出力が来ると戻る。
	///
	/// - キャンセルしてよい。読んだ内容は `BufReader` に残る
	///
	/// # 戻り値
	/// 読める量。0 は EOF
	///
	/// # エラー
	/// 読み取りに失敗した場合
	///
	/// # 例
	/// `let readable = worker.wait_for_output().await?;`
	pub async fn wait_for_output(&mut self) -> io::Result<usize> {
		Ok(self.stdout.fill_buf().await?.len())
	}

	/// 標準入力を閉じて終了を待つ。残れば強制終了する。
	///
	/// # 引数
	/// - `grace`: 自発的な終了を待つ上限
	///
	/// # 例
	/// `worker.close(Duration::from_secs(5)).await;`
	pub async fn close(mut self, grace: Duration) {
		drop(self.stdin.take());
		if tokio::time::timeout(grace, self.child.wait())
			.await
			.is_err()
		{
			tracing::warn!(
				pid = self.child.id(),
				"ワーカーが終了しないため強制終了します"
			);
		}
		self.kill().await;
	}

	/// プロセスツリーごと強制終了して回収する。
	///
	/// - 終了済みでも呼んでよい。残った子孫を落とすため `close` の最後でも呼ぶ
	///
	/// # 例
	/// `worker.kill().await;`
	pub async fn kill(mut self) {
		drop(self.stdin.take());
		// 最後のハンドルを閉じると KILL_ON_JOB_CLOSE で子孫まで終了する
		drop(self.tree.take());
		// Job への割り当てが効いていない場合の保険。終了済みならエラーになるが無害
		let _ = self.child.start_kill();
		if tokio::time::timeout(KILL_WAIT, self.child.wait())
			.await
			.is_err()
		{
			tracing::error!(pid = self.child.id(), "ワーカーの終了を確認できません");
		}
		if let Some(mut task) = self.stderr_task.take() {
			if tokio::time::timeout(STDERR_FLUSH_WAIT, &mut task)
				.await
				.is_err()
			{
				task.abort();
			}
		}
	}
}

impl Drop for WorkerProcess {
	/// `close` と `kill` を経ずに破棄された場合に標準エラーのタスクを止める。
	///
	/// - プロセスは `tree` の破棄と `kill_on_drop` で終了する
	fn drop(&mut self) {
		if let Some(task) = self.stderr_task.take() {
			task.abort();
		}
	}
}

/// パイプを取得できない場合のエラーを作る。
///
/// # 引数
/// - `name`: パイプの名前
///
/// # 例
/// `child.stdin.take().ok_or_else(|| missing_pipe("stdin"))?;`
fn missing_pipe(name: &str) -> io::Error {
	io::Error::other(format!("ワーカーの {name} を取得できません"))
}

/// 標準エラーを EOF まで読み、tracing へ流す。
///
/// - 不正な UTF-8 でも止まらないようバイト列で読む
/// - 改行の無い大量の出力でもメモリが増えないよう、上限ごとに区切って記録する
///
/// # 引数
/// - `stderr`: ワーカーの標準エラー
/// - `pid`: 記録に付ける PID
///
/// # 例
/// `tokio::spawn(drain_stderr(stderr, child.id()));`
async fn drain_stderr(stderr: ChildStderr, pid: Option<u32>) {
	let mut reader = BufReader::new(stderr);
	let mut record = Vec::new();
	loop {
		record.clear();
		let mut limited = (&mut reader).take(STDERR_RECORD_BYTES);
		match limited.read_until(b'\n', &mut record).await {
			Ok(0) => break,
			Ok(_) => {
				let text = String::from_utf8_lossy(&record);
				tracing::info!(pid, "worker: {}", text.trim_end());
			}
			Err(error) => {
				tracing::warn!(%error, pid, "ワーカーの標準エラーを読めません");
				break;
			}
		}
	}
}
