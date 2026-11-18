//! 外からの停止の命令。動いている Bot に、Ctrl+C と同じ終了処理をさせる。
//! - Bot は起動時に名前付きのイベント (手動リセット) を作り、専用のスレッドで待つ (`StopListener`)
//! - `watapachi-bot stop` はイベントをシグナルにし、二重起動の防止のミューテックスが無くなるまで調べる (`request_stop`)
//! - ミューテックスは誰も所有しない (作るだけ) ため、待つことはできない。名前で開けるかどうかで、Bot が動いているかを知る
//! - 手動リセットのため、Bot が待ちを始める前に届いた命令も、待ちを始めた時点で受ける
//! - `Global\` の名前はセッションをまたいで効く (SSH のセッションから、デスクトップで動く Bot へ届く)。
//!   試験では `Local\` の名前を使う
//!
//! 呼び出し方:
//! ```no_run
//! use std::time::Duration;
//! use watapachi_bot::platform::single_instance::MUTEX_NAME;
//! use watapachi_bot::platform::stop_request::{request_stop, StopListener, STOP_EVENT_NAME};
//! # async fn example() -> anyhow::Result<()> {
//! // Bot の側
//! let mut listener = StopListener::create(STOP_EVENT_NAME)?;
//! listener.requested().await;
//! // 命令を送る側
//! let outcome = request_stop(STOP_EVENT_NAME, MUTEX_NAME, Duration::from_secs(25))?;
//! # Ok(())
//! # }
//! ```

use std::io;
use std::ptr;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use windows_sys::Win32::Foundation::{
	CloseHandle, DuplicateHandle, GetLastError, DUPLICATE_SAME_ACCESS, ERROR_ACCESS_DENIED,
	ERROR_ALREADY_EXISTS, ERROR_FILE_NOT_FOUND, HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::System::Threading::{
	CreateEventW, GetCurrentProcess, OpenEventW, OpenMutexW, ResetEvent, SetEvent,
	WaitForSingleObject, EVENT_MODIFY_STATE, SYNCHRONIZATION_SYNCHRONIZE,
};

/// 停止の命令のイベント名。製品名の固定値。設定にしない。
pub const STOP_EVENT_NAME: &str = "Global\\WataPachiBOT-Stop";

/// 待ちのスレッドが、受け手の破棄を確かめる間隔 (ミリ秒)。
/// 受け手を破棄してから、スレッドが終わってイベントを閉じるまでの最大の遅れになる。
const WAIT_POLL_MS: u32 = 500;
/// `request_stop` がミューテックスの有無を調べる間隔。
const EXIT_POLL: Duration = Duration::from_millis(200);
/// 待ちのスレッドの名前。
const THREAD_NAME: &str = "stop-request";

/// 停止の命令の結果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopOutcome {
	/// 命令を送り、Bot が終わった
	Stopped,
	/// Bot が動いていなかった (イベントもミューテックスも無い)
	NotRunning,
}

/// 停止の命令の失敗。
#[derive(Debug, thiserror::Error)]
pub enum StopRequestError {
	/// ミューテックスはあるが、停止の命令を待っていない
	/// (旧版の Bot、起動の直後、待ちの準備に失敗した Bot、update-worker の実行中)
	#[error(
		"Bot か update-worker が動いていますが、停止の命令を待っていません (旧版の Bot、起動の直後、update-worker の実行中など)"
	)]
	Unsupported,
	/// 上限までに Bot が終わらなかった
	#[error("上限の時間までに Bot が終わりませんでした")]
	Timeout,
	/// OS がイベントかミューテックスの操作を拒否した
	#[error("停止の命令を送れません: {0}")]
	Os(#[from] io::Error),
}

/// Bot の側の、停止の命令の受け口。破棄すると待ちのスレッドが終わり、イベントを閉じる。
///
/// - 受け口自身もイベントのハンドルを持つ。命令を受けた後 (終了処理の間) もイベントが残り、
///   もう一度送られた `stop` が「対応していない」と誤らずに、終わりを待てるようにするため
#[derive(Debug)]
pub struct StopListener {
	/// 待ちのスレッドからの知らせ。結果を受けた後は None
	receiver: Option<oneshot::Receiver<()>>,
	/// イベントのハンドル (待ちのスレッドのものとは別)。破棄で閉じる
	_event: OwnedHandle,
}

/// 1 度だけ閉じるハンドル。破棄で閉じる。
#[derive(Debug)]
struct OwnedHandle(HANDLE);

// イベントのハンドルは待つ・閉じるだけで、どのスレッドから扱っても安全。
unsafe impl Send for OwnedHandle {}
unsafe impl Sync for OwnedHandle {}

impl Drop for OwnedHandle {
	/// ハンドルを閉じる。
	fn drop(&mut self) {
		// SAFETY: self.0 は作成か複製で得た有効なハンドルで、ここで 1 度だけ閉じる
		unsafe { CloseHandle(self.0) };
	}
}

impl StopListener {
	/// 名前付きのイベントを作り、待ちのスレッドを始める。
	///
	/// - 手動リセット、初めは非シグナルで作る。既にあれば (前回の命令の残り) 非シグナルに戻す
	///
	/// # 引数
	/// - `name`: イベント名 (通常は `STOP_EVENT_NAME`)
	///
	/// # 戻り値
	/// 受け口
	///
	/// # エラー
	/// OS がイベントの作成かリセットを拒否した、スレッドを作れない
	///
	/// # 例
	/// `let mut listener = StopListener::create(STOP_EVENT_NAME)?;`
	pub fn create(name: &str) -> io::Result<StopListener> {
		let wide = to_wide(name);
		// SAFETY: wide は NUL 終端の有効な UTF-16。セキュリティ属性は既定 (null)
		let handle = unsafe { CreateEventW(ptr::null(), 1, 0, wide.as_ptr()) };
		if handle.is_null() {
			return Err(io::Error::last_os_error());
		}
		// SAFETY: 直前の API 呼び出しのエラーを読む。間に他の Win32 呼び出しを挟まない
		let existed = unsafe { GetLastError() } == ERROR_ALREADY_EXISTS;
		let event = OwnedHandle(handle);
		// SAFETY: event.0 は上で得た有効なハンドル
		if existed && unsafe { ResetEvent(event.0) } == 0 {
			return Err(io::Error::last_os_error());
		}
		let for_thread = duplicate(&event)?;
		let (sender, receiver) = oneshot::channel();
		// スレッドを作れなかったときは、クロージャごと複製のハンドルが破棄されて閉じる
		std::thread::Builder::new()
			.name(THREAD_NAME.to_string())
			.spawn(move || wait_for_signal(for_thread, sender))?;
		Ok(StopListener {
			receiver: Some(receiver),
			_event: event,
		})
	}

	/// 停止の命令を待つ。
	///
	/// - 1 度結果を返した後は、永遠に待つ (`select!` の枝を条件で止めずに呼んでもよい)
	///
	/// # 戻り値
	/// 命令を受けたら Some(())。待ちのスレッドが命令を受けずに止まった (OS の待ちの失敗) ら None
	///
	/// # 例
	/// `if listener.requested().await.is_some() { /* 終了する */ }`
	pub async fn requested(&mut self) -> Option<()> {
		match self.receiver.as_mut() {
			Some(receiver) => {
				let result = receiver.await.ok();
				self.receiver = None;
				result
			}
			None => std::future::pending().await,
		}
	}
}

/// 同じプロセスの中でハンドルを複製する。
///
/// # 引数
/// - `handle`: 複製するハンドル
///
/// # 戻り値
/// 同じアクセス権の別のハンドル
///
/// # エラー
/// OS が複製を拒否した
///
/// # 例
/// `let for_thread = duplicate(&event)?;`
fn duplicate(handle: &OwnedHandle) -> io::Result<OwnedHandle> {
	let mut copy: HANDLE = ptr::null_mut();
	// SAFETY: 自プロセスの疑似ハンドルと有効なハンドルを渡し、結果は copy に書かれる
	let ok = unsafe {
		let process = GetCurrentProcess();
		DuplicateHandle(
			process,
			handle.0,
			process,
			&mut copy,
			0,
			0,
			DUPLICATE_SAME_ACCESS,
		)
	};
	if ok == 0 {
		return Err(io::Error::last_os_error());
	}
	Ok(OwnedHandle(copy))
}

/// 待ちのスレッドの本体。シグナルを受けたら知らせて終わる。受け手が破棄されたら知らせずに終わる。
///
/// - 終わるときに自分のハンドル (複製) を閉じる。イベント自体は受け口のハンドルが残す
/// - OS の待ちが失敗したら、知らせずに終わる (受け手は None を受ける)
///
/// # 引数
/// - `handle`: 待つイベント
/// - `sender`: 受け手への知らせ
///
/// # 例
/// `std::thread::spawn(move || wait_for_signal(owned, sender));`
fn wait_for_signal(handle: OwnedHandle, sender: oneshot::Sender<()>) {
	loop {
		if sender.is_closed() {
			break;
		}
		// SAFETY: handle.0 はこのスレッドが持つ有効なハンドル
		let result = unsafe { WaitForSingleObject(handle.0, WAIT_POLL_MS) };
		match result {
			WAIT_OBJECT_0 => {
				// 受け手が既に破棄されていれば、送信の失敗は無視してよい
				let _ = sender.send(());
				break;
			}
			WAIT_TIMEOUT => {}
			_ => {
				tracing::error!(
					error = %io::Error::last_os_error(),
					"停止の命令の待ちが失敗しました"
				);
				break;
			}
		}
	}
	// handle はここで破棄されて閉じる
}

/// 動いている Bot に停止の命令を送り、終わるまで待つ。
///
/// - イベントが無ければ、ミューテックスの有無で「動いていない」と「対応していない」を見分ける
/// - イベントをシグナルにした後、`EXIT_POLL` ごとにミューテックスを開いてみて、無くなれば終わったとみなす。
///   開いたハンドルはすぐ閉じる (開いたままだと、次に起動する Bot が「既に起動している」で失敗するため)
///
/// # 引数
/// - `event_name`: イベント名 (通常は `STOP_EVENT_NAME`)
/// - `mutex_name`: 二重起動の防止のミューテックス名 (通常は `MUTEX_NAME`)
/// - `timeout`: 終わるまで待つ上限
///
/// # 戻り値
/// 終わったら `Stopped`、動いていなければ `NotRunning`
///
/// # エラー
/// - イベントが無く、ミューテックスがある: `Unsupported`
/// - 上限までに終わらない: `Timeout`
/// - OS がイベントかミューテックスの操作を拒否した: `Os`
///
/// # 例
/// `let outcome = request_stop(STOP_EVENT_NAME, MUTEX_NAME, Duration::from_secs(25))?;`
pub fn request_stop(
	event_name: &str,
	mutex_name: &str,
	timeout: Duration,
) -> Result<StopOutcome, StopRequestError> {
	let started = Instant::now();
	let wide = to_wide(event_name);
	// SAFETY: wide は NUL 終端の有効な UTF-16
	let event = unsafe { OpenEventW(EVENT_MODIFY_STATE, 0, wide.as_ptr()) };
	if event.is_null() {
		let error = io::Error::last_os_error();
		if error.raw_os_error() != Some(ERROR_FILE_NOT_FOUND as i32) {
			return Err(StopRequestError::Os(error));
		}
		return if mutex_exists(mutex_name)? {
			Err(StopRequestError::Unsupported)
		} else {
			Ok(StopOutcome::NotRunning)
		};
	}
	// SAFETY: event は上で得た有効なハンドル
	let set = unsafe { SetEvent(event) };
	let set_error = (set == 0).then(io::Error::last_os_error);
	// SAFETY: event は有効で、ここで 1 度だけ閉じる
	unsafe { CloseHandle(event) };
	if let Some(error) = set_error {
		return Err(StopRequestError::Os(error));
	}
	loop {
		if !mutex_exists(mutex_name)? {
			return Ok(StopOutcome::Stopped);
		}
		let elapsed = started.elapsed();
		if elapsed >= timeout {
			return Err(StopRequestError::Timeout);
		}
		std::thread::sleep(EXIT_POLL.min(timeout - elapsed));
	}
}

/// 名前付きミューテックスがあるかを、開いてすぐ閉じて調べる。
///
/// - 開く権利が無い (`ERROR_ACCESS_DENIED`) ときも、あるとみなす
///
/// # 引数
/// - `name`: ミューテックス名
///
/// # 戻り値
/// あれば true
///
/// # エラー
/// 無い、権利が無い以外の理由で開けない
///
/// # 例
/// `if mutex_exists(MUTEX_NAME)? { /* 動いている */ }`
fn mutex_exists(name: &str) -> io::Result<bool> {
	let wide = to_wide(name);
	// SAFETY: wide は NUL 終端の有効な UTF-16
	let handle = unsafe { OpenMutexW(SYNCHRONIZATION_SYNCHRONIZE, 0, wide.as_ptr()) };
	if !handle.is_null() {
		// SAFETY: handle は上で得た有効なハンドルで、ここで 1 度だけ閉じる
		unsafe { CloseHandle(handle) };
		return Ok(true);
	}
	let error = io::Error::last_os_error();
	match error.raw_os_error() {
		Some(code) if code == ERROR_FILE_NOT_FOUND as i32 => Ok(false),
		Some(code) if code == ERROR_ACCESS_DENIED as i32 => Ok(true),
		_ => Err(error),
	}
}

/// 文字列を NUL 終端の UTF-16 にする。
///
/// # 引数
/// - `text`: 変える文字列
///
/// # 戻り値
/// 末尾に 0 を足した UTF-16 の列
///
/// # 例
/// `let wide = to_wide("Local\\Example");`
fn to_wide(text: &str) -> Vec<u16> {
	text.encode_utf16().chain(std::iter::once(0)).collect()
}
