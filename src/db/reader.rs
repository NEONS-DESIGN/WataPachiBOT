//! 読み取り専用の接続プール。
//! - 接続は `read_connections` 本。セマフォで同時利用数を制限し、`spawn_blocking` で実行する
//! - 書き込みスレッドが処理中でも WAL のおかげで読める

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};
use tokio::sync::Semaphore;

use super::{DbError, BUSY_TIMEOUT_MS, STATEMENT_CACHE_CAPACITY};

struct Pool {
	connections: Mutex<Vec<Connection>>,
	permits: Arc<Semaphore>,
}

/// 読み取り専用の接続プール。Clone 可。
#[derive(Clone)]
pub struct DbReader {
	pool: Arc<Pool>,
}

impl DbReader {
	/// 読み取り専用の接続を `count` 本開く。
	///
	/// # 引数
	/// - `path`: 移行済みの DB ファイル
	/// - `count`: 接続数（1 以上。0 のときは 1 にする）
	///
	/// # エラー
	/// - 接続を開けなかったとき
	pub(super) fn open(path: &Path, count: usize) -> Result<Self, DbError> {
		let count = count.max(1);
		let mut connections = Vec::with_capacity(count);
		for _ in 0..count {
			let conn = Connection::open_with_flags(
				path,
				OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
			)?;
			conn.busy_timeout(Duration::from_millis(BUSY_TIMEOUT_MS))?;
			conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
			connections.push(conn);
		}
		Ok(Self {
			pool: Arc::new(Pool {
				connections: Mutex::new(connections),
				permits: Arc::new(Semaphore::new(count)),
			}),
		})
	}

	/// プールの接続で読み取り処理を実行する。
	///
	/// # 引数
	/// - `f`: 接続を受け取る処理。ブロッキング用スレッドで動く
	///
	/// # 戻り値
	/// `f` の戻り値。
	///
	/// # エラー
	/// - `f` が返したエラー
	/// - 接続が空いていない、または `f` が panic したとき `ReaderClosed`
	///
	/// # 呼び出し例
	/// ```ignore
	/// let n: i64 = reader.read(|c| Ok(c.query_row("SELECT count(*) FROM history", [], |r| r.get(0))?)).await?;
	/// ```
	pub async fn read<T: Send + 'static>(
		&self,
		f: impl FnOnce(&Connection) -> Result<T, DbError> + Send + 'static,
	) -> Result<T, DbError> {
		// 許可は接続を返し終えるまで持つ。future が破棄されても貸し出し中に解放されないよう、
		// 所有版を取って spawn_blocking の中へ移す（pop から spawn までに await は無い）
		let permit = Arc::clone(&self.pool.permits)
			.acquire_owned()
			.await
			.map_err(|_| DbError::ReaderClosed)?;
		let conn = {
			let mut guard = self
				.pool
				.connections
				.lock()
				.map_err(|_| DbError::ReaderClosed)?;
			guard.pop().ok_or(DbError::ReaderClosed)?
		};
		let pool = Arc::clone(&self.pool);
		let joined = tokio::task::spawn_blocking(move || {
			// panic しても接続をプールへ戻せるように捕まえる
			let result = catch_unwind(AssertUnwindSafe(|| f(&conn)));
			if let Ok(mut guard) = pool.connections.lock() {
				guard.push(conn);
			}
			drop(permit);
			result
		})
		.await;
		match joined {
			Ok(Ok(result)) => result,
			_ => Err(DbError::ReaderClosed),
		}
	}
}
