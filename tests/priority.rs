//! プロセスの優先度を通常へ戻す処理の試験。
//!
//! - 試験のプロセス自身の優先度を下げてから戻す。統合試験はファイルごとに別のプロセスなので、他の試験へ影響しない
//! - 優先度を変える試験は 1 つにまとめる (並行に走る試験どうしで取り合わないように)

use watapachi_bot::platform::priority;
use windows_sys::Win32::System::Threading::{
	GetCurrentProcess, GetPriorityClass, ProcessMemoryPriority, SetPriorityClass,
	SetProcessInformation, ABOVE_NORMAL_PRIORITY_CLASS, BELOW_NORMAL_PRIORITY_CLASS,
	IDLE_PRIORITY_CLASS, MEMORY_PRIORITY_INFORMATION, MEMORY_PRIORITY_LOW, NORMAL_PRIORITY_CLASS,
};

/// 自分のプロセスのメモリ優先度を変える。
fn set_memory_priority(value: u32) {
	let info = MEMORY_PRIORITY_INFORMATION {
		MemoryPriority: value,
	};
	// SAFETY: info は MEMORY_PRIORITY_INFORMATION そのもので、大きさを正しく渡している
	let ok = unsafe {
		SetProcessInformation(
			GetCurrentProcess(),
			ProcessMemoryPriority,
			(&info as *const MEMORY_PRIORITY_INFORMATION).cast(),
			std::mem::size_of::<MEMORY_PRIORITY_INFORMATION>() as u32,
		)
	};
	assert_ne!(ok, 0, "メモリ優先度を変えられない");
}

/// 自分のプロセスの優先度クラスを読む。
fn current_class() -> u32 {
	// SAFETY: 自分のプロセスの疑似ハンドルを渡す
	unsafe { GetPriorityClass(GetCurrentProcess()) }
}

/// 自分のプロセスの優先度クラスを変える。
fn set_class(class: u32) {
	// SAFETY: 自分のプロセスの疑似ハンドルを渡す
	let ok = unsafe { SetPriorityClass(GetCurrentProcess(), class) };
	assert_ne!(ok, 0, "優先度クラスを変えられない");
}

#[test]
fn only_low_classes_need_raise() {
	assert!(priority::class_needs_raise(IDLE_PRIORITY_CLASS));
	assert!(priority::class_needs_raise(BELOW_NORMAL_PRIORITY_CLASS));
	assert!(!priority::class_needs_raise(NORMAL_PRIORITY_CLASS));
	assert!(!priority::class_needs_raise(ABOVE_NORMAL_PRIORITY_CLASS));
	assert_eq!(
		priority::class_name(BELOW_NORMAL_PRIORITY_CLASS),
		"BELOW_NORMAL"
	);
}

#[test]
fn raises_low_class_and_keeps_higher_one() {
	// タスクスケジューラの既定 (優先度 7) と同じクラスとメモリ優先度にしてから戻す
	set_class(BELOW_NORMAL_PRIORITY_CLASS);
	set_memory_priority(MEMORY_PRIORITY_LOW);
	let report = priority::raise_to_normal();
	assert_eq!(report.class.unwrap(), Some(BELOW_NORMAL_PRIORITY_CLASS));
	assert_eq!(report.memory.unwrap(), Some(MEMORY_PRIORITY_LOW));
	assert_eq!(current_class(), NORMAL_PRIORITY_CLASS);

	// 2 回目は何もしない
	let report = priority::raise_to_normal();
	assert_eq!(report.class.unwrap(), None);
	assert_eq!(report.memory.unwrap(), None);

	// NORMAL より高いクラスは下げない
	set_class(ABOVE_NORMAL_PRIORITY_CLASS);
	assert_eq!(priority::raise_to_normal().class.unwrap(), None);
	assert_eq!(current_class(), ABOVE_NORMAL_PRIORITY_CLASS);
	set_class(NORMAL_PRIORITY_CLASS);
}
