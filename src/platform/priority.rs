//! 自分のプロセスの優先度を通常へ戻す。
//!
//! - タスクスケジューラの既定の優先度 (7) で起動すると、優先度クラスは BELOW_NORMAL、
//!   メモリ優先度は LOW になる。CPU が混むとコマンドの応答と音声の処理が後回しになる
//! - 優先度クラスが IDLE か BELOW_NORMAL のプロセスが作る子プロセスは、同じクラスを引き継ぐ。
//!   ワーカー (python、Deno) を起動する前に NORMAL へ上げれば、子も NORMAL で起動する
//! - NORMAL 以上 (ABOVE_NORMAL、HIGH など) は変えない。下げることはしない
//! - I/O 優先度は公開された API で戻せないため扱わない (タスクの登録で優先度 4 を指定して避ける)
//!
//! 呼び出し方:
//! ```no_run
//! use watapachi_bot::platform::priority;
//! let report = priority::raise_to_normal();
//! let raised_from = report.class.ok().flatten();
//! ```

use std::ffi::c_void;
use std::io;

use windows_sys::Win32::System::Threading::{
	GetCurrentProcess, GetPriorityClass, GetProcessInformation, ProcessMemoryPriority,
	SetPriorityClass, SetProcessInformation, ABOVE_NORMAL_PRIORITY_CLASS,
	BELOW_NORMAL_PRIORITY_CLASS, HIGH_PRIORITY_CLASS, IDLE_PRIORITY_CLASS,
	MEMORY_PRIORITY_INFORMATION, MEMORY_PRIORITY_NORMAL, NORMAL_PRIORITY_CLASS,
	REALTIME_PRIORITY_CLASS,
};

/// `MEMORY_PRIORITY_INFORMATION` の大きさ (API に渡すバイト数)。
const MEMORY_PRIORITY_INFO_SIZE: u32 = std::mem::size_of::<MEMORY_PRIORITY_INFORMATION>() as u32;

/// 優先度の調整の結果。部分ごとに成否を持つ (片方の失敗で他方を止めない)。
#[derive(Debug)]
pub struct PriorityReport {
	/// 優先度クラス。`Ok(Some(元のクラス))` なら NORMAL へ上げた、`Ok(None)` なら変えなかった
	pub class: io::Result<Option<u32>>,
	/// メモリ優先度。`Ok(Some(元の値))` なら NORMAL へ上げた、`Ok(None)` なら変えなかった
	pub memory: io::Result<Option<u32>>,
}

/// 優先度クラスを NORMAL へ上げるべきか (IDLE か BELOW_NORMAL か)。
///
/// # 引数
/// - `class`: `GetPriorityClass` の値
///
/// # 戻り値
/// 上げるべきなら true。NORMAL 以上と、知らない値は false
///
/// # 例
/// `assert!(priority::class_needs_raise(BELOW_NORMAL_PRIORITY_CLASS));`
pub fn class_needs_raise(class: u32) -> bool {
	class == IDLE_PRIORITY_CLASS || class == BELOW_NORMAL_PRIORITY_CLASS
}

/// メモリ優先度を NORMAL へ上げるべきか (NORMAL 未満か)。
///
/// # 引数
/// - `memory_priority`: `MEMORY_PRIORITY_INFORMATION.MemoryPriority` の値
///
/// # 戻り値
/// NORMAL 未満なら true
///
/// # 例
/// `assert!(priority::memory_needs_raise(MEMORY_PRIORITY_LOW));`
pub fn memory_needs_raise(memory_priority: u32) -> bool {
	memory_priority < MEMORY_PRIORITY_NORMAL
}

/// 自分のプロセスの優先度クラスとメモリ優先度を、低ければ NORMAL へ上げる。
///
/// - ワーカーなどの子プロセスを起動する前に呼ぶ (子は起動時の親のクラスを引き継ぐため)
/// - 失敗しても何も巻き戻さない。結果を見てログに残し、起動は続ける
///
/// # 戻り値
/// 優先度クラスとメモリ優先度それぞれの結果
///
/// # 例
/// `let report = priority::raise_to_normal();`
pub fn raise_to_normal() -> PriorityReport {
	PriorityReport {
		class: raise_class(),
		memory: raise_memory(),
	}
}

/// 優先度クラスを、IDLE か BELOW_NORMAL なら NORMAL へ上げる。
///
/// # 戻り値
/// 上げたら元のクラス、変えなかったら None
///
/// # エラー
/// クラスを読めない、または変えられないときの OS のエラー
///
/// # 例
/// `let raised_from = raise_class()?;`
fn raise_class() -> io::Result<Option<u32>> {
	// SAFETY: GetCurrentProcess は閉じる必要の無い疑似ハンドルを返す。引数は無い
	let process = unsafe { GetCurrentProcess() };
	// SAFETY: process は自分のプロセスの有効な疑似ハンドル
	let class = unsafe { GetPriorityClass(process) };
	if class == 0 {
		return Err(io::Error::last_os_error());
	}
	if !class_needs_raise(class) {
		return Ok(None);
	}
	// SAFETY: process は自分のプロセスの有効な疑似ハンドル。NORMAL へ上げるのに特権は要らない
	if unsafe { SetPriorityClass(process, NORMAL_PRIORITY_CLASS) } == 0 {
		return Err(io::Error::last_os_error());
	}
	Ok(Some(class))
}

/// メモリ優先度を、NORMAL 未満なら NORMAL へ上げる。
///
/// # 戻り値
/// 上げたら元の値、変えなかったら None
///
/// # エラー
/// 値を読めない、または変えられないときの OS のエラー
///
/// # 例
/// `let raised_from = raise_memory()?;`
fn raise_memory() -> io::Result<Option<u32>> {
	// SAFETY: GetCurrentProcess は閉じる必要の無い疑似ハンドルを返す。引数は無い
	let process = unsafe { GetCurrentProcess() };
	let mut info = MEMORY_PRIORITY_INFORMATION { MemoryPriority: 0 };
	// SAFETY: info は MEMORY_PRIORITY_INFORMATION そのもので、大きさを正しく渡している
	let read = unsafe {
		GetProcessInformation(
			process,
			ProcessMemoryPriority,
			(&mut info as *mut MEMORY_PRIORITY_INFORMATION).cast::<c_void>(),
			MEMORY_PRIORITY_INFO_SIZE,
		)
	};
	if read == 0 {
		return Err(io::Error::last_os_error());
	}
	let before = info.MemoryPriority;
	if !memory_needs_raise(before) {
		return Ok(None);
	}
	let normal = MEMORY_PRIORITY_INFORMATION {
		MemoryPriority: MEMORY_PRIORITY_NORMAL,
	};
	// SAFETY: normal は MEMORY_PRIORITY_INFORMATION そのもので、大きさを正しく渡している
	let written = unsafe {
		SetProcessInformation(
			process,
			ProcessMemoryPriority,
			(&normal as *const MEMORY_PRIORITY_INFORMATION).cast::<c_void>(),
			MEMORY_PRIORITY_INFO_SIZE,
		)
	};
	if written == 0 {
		return Err(io::Error::last_os_error());
	}
	Ok(Some(before))
}

/// 優先度クラスの値をログ向けの名前にする。
///
/// # 引数
/// - `class`: `GetPriorityClass` の値
///
/// # 戻り値
/// `IDLE` などの名前。知らない値は `UNKNOWN`
///
/// # 例
/// `assert_eq!(priority::class_name(BELOW_NORMAL_PRIORITY_CLASS), "BELOW_NORMAL");`
pub fn class_name(class: u32) -> &'static str {
	match class {
		IDLE_PRIORITY_CLASS => "IDLE",
		BELOW_NORMAL_PRIORITY_CLASS => "BELOW_NORMAL",
		NORMAL_PRIORITY_CLASS => "NORMAL",
		ABOVE_NORMAL_PRIORITY_CLASS => "ABOVE_NORMAL",
		HIGH_PRIORITY_CLASS => "HIGH",
		REALTIME_PRIORITY_CLASS => "REALTIME",
		_ => "UNKNOWN",
	}
}
