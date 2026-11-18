//! 二重起動の防止。名前付きミューテックスを保持している間、同名の 2 つ目の取得は失敗する。
//! - ハンドルを閉じる (Drop) か、プロセスが終了すると OS が解放する
//! - `Global\` の名前はセッションをまたいで効く (サービスとコンソールの二重起動も防ぐ)。
//!   権限の都合で作れない環境の試験では `Local\` の名前を使う
//!
//! 呼び出し方:
//! ```no_run
//! use watapachi_bot::platform::single_instance::{SingleInstance, MUTEX_NAME};
//! let _instance = SingleInstance::acquire(MUTEX_NAME).expect("already running");
//! // 値が生きている間、2 つ目の起動は AlreadyRunning で失敗する
//! ```

use std::io;
use std::ptr;

use windows_sys::Win32::Foundation::{CloseHandle, GetLastError, ERROR_ALREADY_EXISTS, HANDLE};
use windows_sys::Win32::System::Threading::CreateMutexW;

/// 製品名の固定値。設定にしない。
pub const MUTEX_NAME: &str = "Global\\WataPachiBOT";

/// 既に起動していたために終えたときの、実行ファイルの終了コード。
/// - 起動スクリプト (tools/start.ps1 の `$ExitCodeAlreadyRunning`) はこの値で起動し直しをやめる。値を変えるときは両方を変える
/// - 0 (正常)、1 (失敗)、2 (使い方の誤り) と重ねない
pub const EXIT_CODE_ALREADY_RUNNING: u8 = 3;

/// エラーの連なり (`context` で包んだ元を含む) に、二重起動 (`AlreadyRunning`) があるか。
///
/// - 包む側が `anyhow!("...")` で文字列にしていると型が失われ、見つけられない。
///   `anyhow::Error::new(SingleInstanceError::AlreadyRunning)` のように型のまま渡すこと
///
/// # 引数
/// - `error`: 調べるエラー
///
/// # 戻り値
/// 二重起動なら true
///
/// # 例
/// `let code = if is_already_running(&error) { EXIT_CODE_ALREADY_RUNNING } else { 1 };`
pub fn is_already_running(error: &anyhow::Error) -> bool {
	error.chain().any(|cause| {
		matches!(
			cause.downcast_ref::<SingleInstanceError>(),
			Some(SingleInstanceError::AlreadyRunning)
		)
	})
}

/// 二重起動の防止で起きるエラー。
#[derive(Debug, thiserror::Error)]
pub enum SingleInstanceError {
	/// 同名のミューテックスが既にある (別の Bot が動いている)。
	#[error("既に起動しています")]
	AlreadyRunning,
	/// OS がミューテックスの作成を拒否した。
	#[error("ミューテックスを作れません: {0}")]
	Os(#[from] io::Error),
}

/// 名前付きミューテックスのハンドル。破棄するとミューテックスを手放す。
#[derive(Debug)]
pub struct SingleInstance {
	handle: HANDLE,
}

// ミューテックスのハンドルは保持するだけでスレッド間で動かして安全。
unsafe impl Send for SingleInstance {}
unsafe impl Sync for SingleInstance {}

impl SingleInstance {
	/// 名前付きミューテックスを作る。既にあれば `AlreadyRunning`。
	///
	/// # 引数
	/// - `name`: ミューテックス名 (通常は `MUTEX_NAME`)
	///
	/// # 戻り値
	/// 保持している間ミューテックスを占有する値
	///
	/// # エラー
	/// - 同名のミューテックスが既にある場合は `AlreadyRunning`
	/// - OS が作成を拒否した場合は `Os`
	///
	/// # 例
	/// `let guard = SingleInstance::acquire("Local\\Example")?;`
	pub fn acquire(name: &str) -> Result<SingleInstance, SingleInstanceError> {
		let wide: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
		// SAFETY: wide は NUL 終端の有効な UTF-16。セキュリティ属性は既定 (null)
		let handle = unsafe { CreateMutexW(ptr::null(), 0, wide.as_ptr()) };
		if handle.is_null() {
			return Err(SingleInstanceError::Os(io::Error::last_os_error()));
		}
		// SAFETY: 直前の API 呼び出しのエラーを読む。間に他の Win32 呼び出しを挟まない
		let already = unsafe { GetLastError() } == ERROR_ALREADY_EXISTS;
		if already {
			// 既存のミューテックスを開いただけなので、自分のハンドルは閉じる
			// SAFETY: handle は上で得た有効なハンドル
			unsafe { CloseHandle(handle) };
			return Err(SingleInstanceError::AlreadyRunning);
		}
		Ok(SingleInstance { handle })
	}
}

impl Drop for SingleInstance {
	/// ハンドルを閉じてミューテックスを手放す。
	fn drop(&mut self) {
		// SAFETY: handle は acquire で得た有効なハンドルで、ここで 1 度だけ閉じる
		unsafe { CloseHandle(self.handle) };
	}
}
