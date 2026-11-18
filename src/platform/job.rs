//! Windows の Job Object。子プロセスの寿命を Bot に紐付ける。
//! - `KILL_ON_JOB_CLOSE` を付ける。最後のハンドルが閉じると、所属する全プロセスが終了する
//! - Bot が異常終了しても OS がハンドルを閉じるため、ワーカーと Deno が残らない
//! - Job は入れ子にできる。プール全体の Job の下にワーカーごとの Job を置くと、
//!   ワーカーごとの Job の破棄でそのプロセスツリーだけを終了できる
//!
//! 呼び出し方:
//! ```no_run
//! # fn main() -> std::io::Result<()> {
//! # let child: tokio::process::Child = todo!();
//! use watapachi_bot::platform::job::Job;
//! let job = Job::new()?;
//! job.assign(&child)?;
//! drop(job); // child とその子孫が終了する
//! # Ok(())
//! # }
//! ```

use std::io;

use std::os::windows::io::AsRawHandle;

use tokio::process::Child;
use win32job::{ExtendedLimitInfo, Job as Win32Job};

/// `KILL_ON_JOB_CLOSE` 付きの Job Object。破棄すると所属する全プロセスが終了する。
#[derive(Debug)]
pub struct Job {
	inner: Win32Job,
}

impl Job {
	/// 無名の Job を作る。
	///
	/// # 戻り値
	/// `KILL_ON_JOB_CLOSE` を設定した Job
	///
	/// # エラー
	/// OS が Job の作成か設定を拒否した場合
	///
	/// # 例
	/// `let job = Job::new()?;`
	pub fn new() -> io::Result<Self> {
		let mut info = ExtendedLimitInfo::new();
		info.limit_kill_on_job_close();
		let inner = Win32Job::create_with_limit_info(&info).map_err(io::Error::other)?;
		Ok(Self { inner })
	}

	/// 起動済みの子プロセスを Job に入れる。
	///
	/// - 以後にそのプロセスが作る子孫も同じ Job に入る
	/// - すでに別の Job に入っていれば、この Job はその内側に入れ子になる
	/// - 起動から呼び出しまでに子が作った孫は入らない。起動直後に呼ぶこと
	///
	/// # 引数
	/// - `child`: 対象のプロセス
	///
	/// # エラー
	/// プロセスのハンドルが無い (回収済み) 場合、OS が割り当てを拒否した場合
	///
	/// # 例
	/// `job.assign(&child)?;`
	pub fn assign(&self, child: &Child) -> io::Result<()> {
		let handle = child
			.raw_handle()
			.ok_or_else(|| io::Error::other("プロセスのハンドルがありません"))?;
		self.inner
			.assign_process(handle as isize)
			.map_err(io::Error::other)
	}

	/// 標準ライブラリの `Child` を Job に入れる (Songbird の `ChildContainer` が `std::process::Child` を取るため)。
	///
	/// - 動作と注意は `assign` と同じ。起動直後に呼ぶこと
	///
	/// # 引数
	/// - `child`: 対象のプロセス
	///
	/// # エラー
	/// OS が割り当てを拒否した場合
	///
	/// # 例
	/// `job.assign_std(&child)?;`
	pub fn assign_std(&self, child: &std::process::Child) -> io::Result<()> {
		self.inner
			.assign_process(child.as_raw_handle() as isize)
			.map_err(io::Error::other)
	}
}
