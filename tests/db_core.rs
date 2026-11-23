//! DB 基盤の統合テスト。
//! - 移行、WAL、バッチ内の失敗の独立、close での書き切り、位置更新の集約、読み取りの並行

use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

use tempfile::TempDir;
use watapachi_bot::config::DbConfig;
use watapachi_bot::db::{Database, DbError};
use watapachi_bot::domain::GuildId;

/// テスト用の DB 設定。
fn config() -> DbConfig {
	DbConfig {
		batch_size: 256,
		read_connections: 4,
		writer_queue: 4096,
	}
}

/// 一時ディレクトリに DB を開く。ディレクトリは呼び出し側で保持する。
fn open_in(dir: &TempDir) -> Database {
	Database::open(&dir.path().join("test.db"), &config()).unwrap()
}

/// DB ファイルを開き直して user_version を読む。
fn user_version(path: &Path) -> i64 {
	let conn = rusqlite::Connection::open(path).unwrap();
	conn.pragma_query_value(None, "user_version", |r| r.get(0))
		.unwrap()
}

#[tokio::test]
async fn migration_sets_user_version_and_is_idempotent() {
	let dir = tempfile::tempdir().unwrap();
	let path = dir.path().join("test.db");
	let db = Database::open(&path, &config()).unwrap();
	db.close().await.unwrap();
	assert_eq!(user_version(&path), 6);

	// 2 回目の open でも版とデータは変わらない
	let db = Database::open(&path, &config()).unwrap();
	let tables: i64 = db
		.handles
		.reader
		.read(|c| {
			Ok(c.query_row(
				"SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'queue_items'",
				[],
				|r| r.get(0),
			)?)
		})
		.await
		.unwrap();
	assert_eq!(tables, 1);
	db.close().await.unwrap();
	assert_eq!(user_version(&path), 6);
}

#[tokio::test]
async fn wal_mode_is_enabled() {
	let dir = tempfile::tempdir().unwrap();
	let db = open_in(&dir);
	let mode: String = db
		.handles
		.reader
		.read(|c| Ok(c.query_row("PRAGMA journal_mode", [], |r| r.get(0))?))
		.await
		.unwrap();
	assert_eq!(mode.to_lowercase(), "wal");
	db.close().await.unwrap();
}

#[tokio::test]
async fn failed_command_does_not_roll_back_others_in_batch() {
	let dir = tempfile::tempdir().unwrap();
	let db = open_in(&dir);
	let writer = db.handles.writer.clone();
	writer
		.run(|c| Ok(c.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY)")?))
		.await
		.unwrap();

	// 書き込みスレッドを止めている間に 3 件を溜め、同じバッチで処理させる
	let (release_tx, release_rx) = mpsc::channel::<()>();
	let blocker = {
		let w = writer.clone();
		tokio::spawn(async move {
			w.run(move |_| {
				let _ = release_rx.recv_timeout(Duration::from_secs(10));
				Ok(())
			})
			.await
		})
	};
	tokio::time::sleep(Duration::from_millis(100)).await;

	let w1 = writer.clone();
	let ok1 = tokio::spawn(async move {
		w1.run(|c| Ok(c.execute("INSERT INTO t (id) VALUES (1)", [])?))
			.await
	});
	// 途中で書いてから失敗する命令。書いた分も巻き戻る
	let w2 = writer.clone();
	let bad = tokio::spawn(async move {
		w2.run(|c| {
			c.execute("INSERT INTO t (id) VALUES (2)", [])?;
			c.execute("INSERT INTO t (id) VALUES (1)", [])?; // 主キー重複
			Ok(())
		})
		.await
	});
	let w3 = writer.clone();
	let ok3 = tokio::spawn(async move {
		w3.run(|c| Ok(c.execute("INSERT INTO t (id) VALUES (3)", [])?))
			.await
	});
	tokio::time::sleep(Duration::from_millis(100)).await;
	release_tx.send(()).unwrap();

	blocker.await.unwrap().unwrap();
	ok1.await.unwrap().unwrap();
	assert!(matches!(bad.await.unwrap(), Err(DbError::Sqlite(_))));
	ok3.await.unwrap().unwrap();

	let ids: Vec<i64> = db
		.handles
		.reader
		.read(|c| {
			let mut stmt = c.prepare("SELECT id FROM t ORDER BY id")?;
			let rows = stmt.query_map([], |r| r.get(0))?;
			Ok(rows.collect::<Result<Vec<i64>, _>>()?)
		})
		.await
		.unwrap();
	assert_eq!(ids, vec![1, 3]);
	db.close().await.unwrap();
}

/// 書き込みスレッドの 1 トランザクションの時間の上限 (writer.rs の TRANSACTION_TIME_LIMIT) より十分長い時間。
const LONGER_THAN_TRANSACTION_LIMIT: Duration = Duration::from_millis(200);

#[tokio::test]
async fn slow_command_result_does_not_wait_for_later_commands_in_batch() {
	let dir = tempfile::tempdir().unwrap();
	let db = open_in(&dir);
	let writer = db.handles.writer.clone();
	writer
		.run(|c| Ok(c.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY)")?))
		.await
		.unwrap();

	// 書き込みスレッドを止めている間に 2 件を溜め、同じバッチに拾わせる
	let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
	let (release_tx, release_rx) = mpsc::channel::<()>();
	let w = writer.clone();
	let blocker = tokio::spawn(async move {
		w.run(move |_| {
			let _ = started_tx.send(());
			let _ = release_rx.recv_timeout(Duration::from_secs(10));
			Ok(())
		})
		.await
	});
	started_rx.await.unwrap();

	// 先頭: 上限より長くかかる命令。後ろ: 試験が合図するまで終わらない命令
	let w1 = writer.clone();
	let slow = tokio::spawn(async move {
		w1.run(|c| {
			c.execute("INSERT INTO t (id) VALUES (1)", [])?;
			std::thread::sleep(LONGER_THAN_TRANSACTION_LIMIT);
			Ok(())
		})
		.await
	});
	let (later_tx, later_rx) = mpsc::channel::<()>();
	let w2 = writer.clone();
	let later = tokio::spawn(async move {
		w2.run(move |c| {
			let _ = later_rx.recv_timeout(Duration::from_secs(10));
			Ok(c.execute("INSERT INTO t (id) VALUES (2)", [])?)
		})
		.await
	});
	tokio::time::sleep(Duration::from_millis(100)).await;
	release_tx.send(()).unwrap();
	blocker.await.unwrap().unwrap();

	// 先頭の結果は後ろの命令を待たずに返り、その時点でコミット済み (読み取りから見える)
	tokio::time::timeout(Duration::from_secs(5), slow)
		.await
		.expect("先頭の結果が後ろの命令の終わりを待った")
		.unwrap()
		.unwrap();
	let seen: i64 = db
		.handles
		.reader
		.read(|c| Ok(c.query_row("SELECT count(*) FROM t", [], |r| r.get(0))?))
		.await
		.unwrap();
	assert_eq!(seen, 1);

	later_tx.send(()).unwrap();
	assert_eq!(later.await.unwrap().unwrap(), 1);
	db.close().await.unwrap();
}

#[tokio::test]
async fn close_checkpoints_wal_while_reader_is_open() {
	let dir = tempfile::tempdir().unwrap();
	let path = dir.path().join("test.db");
	let db = Database::open(&path, &config()).unwrap();
	let reader = db.handles.reader.clone();
	db.handles
		.writer
		.run(|c| {
			c.execute_batch("CREATE TABLE t (v TEXT)")?;
			let mut stmt = c.prepare("INSERT INTO t (v) VALUES (?1)")?;
			for i in 0..1000 {
				stmt.execute([format!("row {i}")])?;
			}
			Ok(())
		})
		.await
		.unwrap();
	// 読み取り接続を使ったことがあり、開いたままの状態にする
	let n: i64 = reader
		.read(|c| Ok(c.query_row("SELECT count(*) FROM t", [], |r| r.get(0))?))
		.await
		.unwrap();
	assert_eq!(n, 1000);
	let wal = dir.path().join("test.db-wal");
	assert!(fs_len(&wal) > 0, "書き込みの後は WAL に中身があるはず");

	let started = std::time::Instant::now();
	db.close().await.unwrap();
	// 終了処理の上限 (既定 20 秒) に比べて十分短い
	assert!(started.elapsed() < Duration::from_secs(2));
	// 読み取り接続が開いたままでも、TRUNCATE のチェックポイントで WAL は空になる
	assert_eq!(fs_len(&wal), 0);
	// 読み取り接続はまだ使え、本体から同じ内容が読める
	let n: i64 = reader
		.read(|c| Ok(c.query_row("SELECT count(*) FROM t", [], |r| r.get(0))?))
		.await
		.unwrap();
	assert_eq!(n, 1000);
}

/// ファイルの大きさ (バイト)。無ければ 0。
fn fs_len(path: &Path) -> u64 {
	std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

#[tokio::test]
async fn close_flushes_pending_writes() {
	let dir = tempfile::tempdir().unwrap();
	let path = dir.path().join("test.db");
	let db = Database::open(&path, &config()).unwrap();
	let writer = db.handles.writer.clone();
	writer
		.run(|c| Ok(c.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY)")?))
		.await
		.unwrap();

	// 書き込みスレッドを止めて 1000 件をキューに溜め、解除と同時に close する
	let (release_tx, release_rx) = mpsc::channel::<()>();
	let w = writer.clone();
	let blocker = tokio::spawn(async move {
		w.run(move |_| {
			let _ = release_rx.recv_timeout(Duration::from_secs(10));
			Ok(())
		})
		.await
	});
	let mut tasks = Vec::new();
	for i in 0..1000i64 {
		let w = writer.clone();
		tasks.push(tokio::spawn(async move {
			w.run(move |c| Ok(c.execute("INSERT INTO t (id) VALUES (?1)", [i])?))
				.await
		}));
	}
	// 全タスクが送信を終えるまで待つ（キュー容量 4096 > 1001 なので送信は待たされない）
	tokio::time::sleep(Duration::from_millis(300)).await;
	release_tx.send(()).unwrap();
	db.close().await.unwrap();
	blocker.await.unwrap().unwrap();
	for task in tasks {
		task.await.unwrap().unwrap();
	}

	let db = Database::open(&path, &config()).unwrap();
	let count: i64 = db
		.handles
		.reader
		.read(|c| Ok(c.query_row("SELECT count(*) FROM t", [], |r| r.get(0))?))
		.await
		.unwrap();
	assert_eq!(count, 1000);
	db.close().await.unwrap();
}

#[tokio::test]
async fn positions_for_same_guild_are_coalesced() {
	let dir = tempfile::tempdir().unwrap();
	let db = open_in(&dir);
	let writer = db.handles.writer.clone();
	let guild = GuildId::new(42);
	writer
		.run(|c| {
			Ok(c.execute(
				"INSERT INTO playback_state (guild_id, voice_channel_id, text_channel_id, video_id, title, requester_id, updated_at)
				 VALUES (42, 1, 2, 'abc', 't', 3, 0)",
				[],
			)
			.map(|_| ())?)
		})
		.await
		.unwrap();

	// 実際の UPDATE 回数を数えるトリガー
	writer
		.run(|c| {
			Ok(c.execute_batch(
				"CREATE TABLE upd_log (n INTEGER);
				 CREATE TRIGGER upd_count AFTER UPDATE ON playback_state
				 BEGIN INSERT INTO upd_log VALUES (1); END;",
			)?)
		})
		.await
		.unwrap();

	// 書き込みスレッドを止めて複数回の更新を溜める
	let (release_tx, release_rx) = mpsc::channel::<()>();
	let w = writer.clone();
	let blocker = tokio::spawn(async move {
		w.run(move |_| {
			let _ = release_rx.recv_timeout(Duration::from_secs(10));
			Ok(())
		})
		.await
	});
	tokio::time::sleep(Duration::from_millis(100)).await;
	for pos in [1000u64, 2000, 3000] {
		writer.save_position(guild, pos, false).await.unwrap();
	}
	writer.save_position(guild, 4000, true).await.unwrap();
	release_tx.send(()).unwrap();
	blocker.await.unwrap().unwrap();

	// 後続の run が返った時点で位置更新は処理済み（FIFO）
	let (pos, paused): (i64, i64) = writer
		.run(|c| {
			Ok(c.query_row(
				"SELECT position_ms, paused FROM playback_state WHERE guild_id = 42",
				[],
				|r| Ok((r.get(0)?, r.get(1)?)),
			)?)
		})
		.await
		.unwrap();
	assert_eq!((pos, paused), (4000, 1));
	let updates: i64 = writer
		.run(|c| Ok(c.query_row("SELECT count(*) FROM upd_log", [], |r| r.get(0))?))
		.await
		.unwrap();
	assert_eq!(updates, 1, "同じギルドの更新は 1 回にまとまるはず");
	db.close().await.unwrap();
}

#[tokio::test]
async fn reads_run_while_writer_is_busy() {
	let dir = tempfile::tempdir().unwrap();
	let db = open_in(&dir);
	let (release_tx, release_rx) = mpsc::channel::<()>();
	let w = db.handles.writer.clone();
	let blocker = tokio::spawn(async move {
		w.run(move |c| {
			c.execute(
				"INSERT INTO bot_admins (guild_id, user_id) VALUES (1, 1)",
				[],
			)?;
			let _ = release_rx.recv_timeout(Duration::from_secs(10));
			Ok(())
		})
		.await
	});
	tokio::time::sleep(Duration::from_millis(100)).await;

	// 書き込みが未コミットで止まっていても、読み取りは待たされない
	let count: i64 = tokio::time::timeout(
		Duration::from_secs(3),
		db.handles
			.reader
			.read(|c| Ok(c.query_row("SELECT count(*) FROM bot_admins", [], |r| r.get(0))?)),
	)
	.await
	.expect("読み取りが書き込みに塞がれた")
	.unwrap();
	assert_eq!(count, 0);

	release_tx.send(()).unwrap();
	blocker.await.unwrap().unwrap();
	db.close().await.unwrap();
}

#[tokio::test]
async fn run_result_is_visible_to_reader_after_return() {
	let dir = tempfile::tempdir().unwrap();
	let db = open_in(&dir);
	for i in 0..50i64 {
		db.handles
			.writer
			.run(move |c| {
				Ok(c.execute("INSERT INTO bot_admins VALUES (?1, 1)", [i])
					.map(|_| ())?)
			})
			.await
			.unwrap();
		let n: i64 = db
			.handles
			.reader
			.read(|c| Ok(c.query_row("SELECT count(*) FROM bot_admins", [], |r| r.get(0))?))
			.await
			.unwrap();
		assert_eq!(n, i + 1);
	}
	db.close().await.unwrap();
}

#[tokio::test]
async fn cancelled_read_does_not_break_pool() {
	let dir = tempfile::tempdir().unwrap();
	let single = DbConfig {
		read_connections: 1,
		..config()
	};
	let db = Database::open(&dir.path().join("test.db"), &single).unwrap();
	let reader = db.handles.reader.clone();
	// 接続を借りたまま future を破棄する
	let slow = reader.read(|_| {
		std::thread::sleep(Duration::from_millis(300));
		Ok(())
	});
	assert!(tokio::time::timeout(Duration::from_millis(50), slow)
		.await
		.is_err());
	// 前の読み取りが終わるまで待たされるが、失敗はしない
	let v: i64 = reader
		.read(|c| Ok(c.query_row("SELECT 1", [], |r| r.get(0))?))
		.await
		.unwrap();
	assert_eq!(v, 1);
	db.close().await.unwrap();
}
