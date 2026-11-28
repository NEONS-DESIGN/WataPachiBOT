//! SQLite の基盤。
//! - 書き込みは専用スレッド（DbWriter）、読み取りは読み取り専用接続のプール（DbReader）
//! - 起動時は `Database::open` で移行を適用して両方を作る。終了時は `Database::close`

pub mod cache;
pub mod history;
mod migrate;
pub mod playback;
pub mod queue;
mod reader;
pub mod settings;
mod writer;

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::DbConfig;

pub use reader::DbReader;
pub use writer::DbWriter;
// 呼び出し側が型名を書けるように再公開する
pub use rusqlite::Connection;

/// 他の接続が書き込み中のときに待つ上限。ホットパスの値ではなく SQLite の内部待機なので定数にする
pub(crate) const BUSY_TIMEOUT_MS: u64 = 5000;

/// 接続ごとに保持する準備済みの文の数 (`prepare_cached` の LRU の容量)。
/// - rusqlite の既定は 16 で、操作関数の文の種類 (40 前後) より少なく、追い出しと準備のし直しが起きる
/// - 文 1 つは数 KB 程度のため、種類の数に余裕を持たせた値にする
pub(crate) const STATEMENT_CACHE_CAPACITY: usize = 128;
/// チェックポイントの後に WAL のファイルを縮める大きさ (`journal_size_limit`)。
/// 期限切れの掃除などで大きく伸びた WAL が、終了の TRUNCATE まで残らないようにする
pub(crate) const JOURNAL_SIZE_LIMIT_BYTES: i64 = 64 * 1024 * 1024;

/// 現在の UNIX 秒。時計が異常なら 0。
///
/// - 操作関数が `added_at` などを記録するときに使う
pub(crate) fn now_secs() -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|d| d.as_secs() as i64)
		.unwrap_or(0)
}

/// DB の整数列から ID を作る。
///
/// # 引数
/// - `value`: 列の値（Discord の ID は u64 を i64 に詰めて保存している）
///
/// # エラー
/// - 0 のとき（ID として不正な値が DB に入っている）
pub(crate) fn id_from_sql<T>(value: i64) -> rusqlite::Result<twilight_model::id::Id<T>> {
	twilight_model::id::Id::new_checked(value as u64).ok_or_else(|| {
		rusqlite::Error::FromSqlConversionFailure(
			0,
			rusqlite::types::Type::Integer,
			"ID が 0 です".into(),
		)
	})
}

/// DB 層のエラー。
///
/// - 書き込み・読み取り・移行のすべてがこの型を返す
#[derive(Debug, thiserror::Error)]
pub enum DbError {
	/// SQLite のエラー。
	#[error("SQLite のエラー: {0}")]
	Sqlite(#[from] rusqlite::Error),
	/// 書き込みスレッドが終了している、または処理中に異常終了した。
	#[error("DB の書き込みスレッドが利用できません")]
	WriterClosed,
	/// 読み取り接続のプールが閉じている、または処理が異常終了した。
	#[error("DB の読み取り接続が利用できません")]
	ReaderClosed,
	/// コミットに失敗した。同じバッチの成功済みの命令もすべてこの結果になる。
	#[error("DB のコミットに失敗しました: {0}")]
	Commit(String),
	/// 移行に失敗した、または DB が新しすぎる。
	#[error("DB の移行に失敗しました: {0}")]
	Migration(String),
	/// スレッドの起動などの入出力エラー。
	#[error("DB の入出力エラー: {0}")]
	Io(#[from] std::io::Error),
}

/// 書き込みと読み取りのハンドルの組。Clone して各所へ配る。
#[derive(Clone)]
pub struct DbHandles {
	pub writer: DbWriter,
	pub reader: DbReader,
}

/// DB 全体の所有者。`close` で終了処理をする。
pub struct Database {
	pub handles: DbHandles,
}

impl Database {
	/// DB を開き、移行を適用し、書き込みスレッドと読み取りプールを用意する。
	///
	/// # 引数
	/// - `path`: DB ファイルのパス（無ければ作る。親ディレクトリは存在すること）
	/// - `config`: `[db]` の設定
	///
	/// # 戻り値
	/// 使用可能な `Database`。
	///
	/// # エラー
	/// - 接続、PRAGMA、移行、スレッド起動のいずれかに失敗したとき
	/// - 既存 DB の `user_version` が知っている移行より新しいとき
	///
	/// # 呼び出し例
	/// ```ignore
	/// let db = Database::open(Path::new("bot.db"), &config.db)?;
	/// let writer = db.handles.writer.clone();
	/// ```
	pub fn open(path: &Path, config: &DbConfig) -> Result<Self, DbError> {
		// 書き込み接続を先に作り、移行を終えてから読み取り接続を開く
		let writer_conn = writer::open_writer_connection(path)?;
		migrate::apply(&writer_conn)?;
		let writer = DbWriter::spawn(writer_conn, config.batch_size, config.writer_queue)?;
		let reader = DbReader::open(path, config.read_connections)?;
		Ok(Self {
			handles: DbHandles { writer, reader },
		})
	}

	/// 未処理の書き込みをすべて実行して閉じる。
	///
	/// # 戻り値
	/// 書き込みスレッドの終了後に `Ok(())`。
	///
	/// # エラー
	/// - 書き込みスレッドがすでに終了していたとき
	///
	/// # 呼び出し例
	/// ```ignore
	/// db.close().await?;
	/// ```
	pub async fn close(self) -> Result<(), DbError> {
		self.handles.writer.close().await
	}
}
