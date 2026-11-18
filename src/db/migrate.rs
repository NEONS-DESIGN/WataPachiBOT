//! スキーマの移行。
//! - `PRAGMA user_version` が適用済みの番号。`MIGRATIONS` を先頭から順に適用する
//! - 1 つの移行は 1 トランザクション。途中で失敗したら何も残さない

use rusqlite::Connection;

use super::DbError;

/// 適用する移行。添字 + 1 が適用後の user_version になる。末尾にだけ足すこと。
const MIGRATIONS: &[&str] = &[
	include_str!("../../migrations/0001_init.sql"),
	include_str!("../../migrations/0002_playlist_fetch_limit.sql"),
	include_str!("../../migrations/0003_available_at.sql"),
	include_str!("../../migrations/0004_history_index.sql"),
	include_str!("../../migrations/0005_lang.sql"),
];

/// 未適用の移行をすべて適用する。
///
/// # 引数
/// - `conn`: 書き込み可能な接続
///
/// # エラー
/// - DB の user_version が既知の移行数より大きいとき
/// - SQL の実行に失敗したとき
///
/// # 呼び出し例
/// ```ignore
/// migrate::apply(&conn)?; // 2 回呼んでも結果は同じ
/// ```
pub(super) fn apply(conn: &Connection) -> Result<(), DbError> {
	let current: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
	let known = MIGRATIONS.len() as i64;
	if current > known {
		return Err(DbError::Migration(format!(
			"DB の版 {current} がこの Bot の対応 {known} より新しい"
		)));
	}
	for (index, sql) in MIGRATIONS.iter().enumerate().skip(current.max(0) as usize) {
		let tx = conn.unchecked_transaction()?;
		tx.execute_batch(sql)?;
		tx.pragma_update(None, "user_version", (index + 1) as i64)?;
		tx.commit()?;
	}
	Ok(())
}
