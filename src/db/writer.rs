//! 書き込み専用スレッド。
//! - 専用の OS スレッドが mpsc から命令を受け、最大 `batch_size` 件をまとめて処理する
//! - まとめた命令は 1 トランザクションで処理する。ただし実行の経過が `TRANSACTION_TIME_LIMIT` を超えたら
//!   そこでコミットして結果を返し、残りは次のトランザクションで続ける (先頭の軽い命令が後ろの重い命令を待たない)
//! - 命令ごとに SAVEPOINT で囲み、1 件の失敗が他へ波及しないようにする
//! - 再生位置の更新は同じギルドの未処理分を 1 件にまとめ、結果を待たない
//! - 終了時は WAL のチェックポイント (TRUNCATE) をしてから接続を閉じる

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rusqlite::Connection;
use tokio::sync::{mpsc, oneshot};

use super::{now_secs, DbError, BUSY_TIMEOUT_MS, STATEMENT_CACHE_CAPACITY};
use crate::domain::GuildId;

/// 1 トランザクションで命令を実行し続ける時間の上限。
/// - 超えた時点の命令までをコミットして結果を返す。命令の途中では切らない
/// - WAL と synchronous = NORMAL ではコミットに fsync が無く安いため、短く取って応答を優先する
const TRANSACTION_TIME_LIMIT: Duration = Duration::from_millis(20);

/// 終了時のチェックポイントで、読み取り中の接続を待つ上限。
/// - 超えたらチェックポイントを諦めて閉じる (WAL は次回の起動で取り込まれる)
/// - 終了処理の上限 (`shutdown.timeout_secs`) に収まるよう、通常の待ち (`BUSY_TIMEOUT_MS`) より短くする
const SHUTDOWN_CHECKPOINT_BUSY_TIMEOUT: Duration = Duration::from_millis(1000);

/// 結果の通知処理。コミット後に呼ぶ。引数はコミット側のエラー（成功なら None）。
type Deliver = Box<dyn FnOnce(Option<String>) + Send>;

/// スレッド内で実行する処理。
/// - 戻り値の bool が false なら SAVEPOINT を巻き戻す
/// - 呼び出し側への結果通知は Deliver に分け、コミットの成否が分かってから呼ぶ
type Job = Box<dyn FnOnce(&Connection) -> (bool, Deliver) + Send>;

enum Command {
	/// 任意の処理。結果は処理側が oneshot で返す。
	Run(Job),
	/// そのギルドの保留中の再生位置を書く。
	SavePosition(GuildId),
	/// ここまでの命令をすべて処理して終了する。
	Shutdown(oneshot::Sender<()>),
}

struct Inner {
	tx: mpsc::Sender<Command>,
	/// ギルドごとの最新の再生位置（位置 ms、一時停止）。命令がキューにある間だけ保持する
	pending_positions: Mutex<HashMap<GuildId, (u64, bool)>>,
	thread: Mutex<Option<JoinHandle<()>>>,
}

/// 書き込みスレッドへのハンドル。Clone 可。
#[derive(Clone)]
pub struct DbWriter {
	inner: Arc<Inner>,
}

/// 書き込み用の接続を開き、PRAGMA を設定する。
///
/// # 引数
/// - `path`: DB ファイルのパス
///
/// # エラー
/// - 接続または PRAGMA の設定に失敗したとき
pub(super) fn open_writer_connection(path: &Path) -> Result<Connection, DbError> {
	let conn = Connection::open(path)?;
	conn.busy_timeout(Duration::from_millis(BUSY_TIMEOUT_MS))?;
	// journal_mode は結果の行を返すので値を読み捨てる
	let mode =
		conn.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))?;
	if !mode.eq_ignore_ascii_case("wal") {
		return Err(DbError::Migration(format!(
			"WAL モードにできません（journal_mode = {mode}）"
		)));
	}
	conn.pragma_update(None, "synchronous", "NORMAL")?;
	conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
	Ok(conn)
}

impl DbWriter {
	/// 書き込みスレッドを起動する。
	///
	/// # 引数
	/// - `conn`: 移行済みの書き込み接続（スレッドへ移す）
	/// - `batch_size`: 1 トランザクションの最大件数（0 のときは 1）
	/// - `queue`: mpsc の容量（0 のときは 1）
	///
	/// # エラー
	/// - スレッドを起動できなかったとき
	pub(super) fn spawn(
		conn: Connection,
		batch_size: usize,
		queue: usize,
	) -> Result<Self, DbError> {
		let (tx, rx) = mpsc::channel(queue.max(1));
		let inner = Arc::new(Inner {
			tx,
			pending_positions: Mutex::new(HashMap::new()),
			thread: Mutex::new(None),
		});
		// スレッドは Inner を弱参照で持つ。全ハンドルが落ちたら送信側が閉じてスレッドが終わる
		let weak = Arc::downgrade(&inner);
		let handle = std::thread::Builder::new()
			.name("db-writer".to_string())
			.spawn(move || writer_loop(conn, rx, batch_size.max(1), weak))?;
		if let Ok(mut slot) = inner.thread.lock() {
			*slot = Some(handle);
		}
		Ok(Self { inner })
	}

	/// 書き込み接続で処理を実行し、結果を待つ。
	///
	/// # 引数
	/// - `f`: 接続を受け取る処理。SAVEPOINT 内で動く。`Err` を返すとその処理だけ巻き戻る
	///
	/// # 戻り値
	/// `f` の戻り値。
	///
	/// # エラー
	/// - `f` が返したエラー
	/// - スレッドが終了している、または `f` が panic したとき `WriterClosed`
	///
	/// # 呼び出し例
	/// ```ignore
	/// writer.run(|c| { c.execute("DELETE FROM history", [])?; Ok(()) }).await?;
	/// ```
	pub async fn run<T: Send + 'static>(
		&self,
		f: impl FnOnce(&Connection) -> Result<T, DbError> + Send + 'static,
	) -> Result<T, DbError> {
		let (result_tx, result_rx) = oneshot::channel();
		let job: Job = Box::new(move |conn| {
			let result = f(conn);
			let ok = result.is_ok();
			let deliver: Deliver = Box::new(move |commit_error| {
				// コミットに失敗したなら成功の結果は返さない
				let result = match (result, commit_error) {
					(Ok(_), Some(message)) => Err(DbError::Commit(message)),
					(result, _) => result,
				};
				// 受け手が先に諦めていても書き込み自体は有効のまま
				let _ = result_tx.send(result);
			});
			(ok, deliver)
		});
		self.inner
			.tx
			.send(Command::Run(job))
			.await
			.map_err(|_| DbError::WriterClosed)?;
		result_rx.await.map_err(|_| DbError::WriterClosed)?
	}

	/// 再生位置を保存する。結果は待たない。
	///
	/// # 引数
	/// - `guild`: 対象のギルド
	/// - `position_ms`: 再生位置（ミリ秒）
	/// - `paused`: 一時停止中か
	///
	/// # 戻り値
	/// キューに入れた時点で `Ok(())`。
	///
	/// # エラー
	/// - スレッドが終了しているとき `WriterClosed`
	///
	/// # 注意
	/// - 同じギルドの未処理分は最新の値 1 件にまとまる
	/// - `playback_state` に行が無いギルドでは何も起きない
	///
	/// # 呼び出し例
	/// ```ignore
	/// writer.save_position(guild, 12_000, false).await?;
	/// ```
	pub async fn save_position(
		&self,
		guild: GuildId,
		position_ms: u64,
		paused: bool,
	) -> Result<(), DbError> {
		let newly_pending = {
			let mut map = self
				.inner
				.pending_positions
				.lock()
				.map_err(|_| DbError::WriterClosed)?;
			map.insert(guild, (position_ms, paused)).is_none()
		};
		// すでに命令がキューにあれば、スレッドが最新値を拾うので送らない
		if newly_pending
			&& self
				.inner
				.tx
				.send(Command::SavePosition(guild))
				.await
				.is_err()
		{
			if let Ok(mut map) = self.inner.pending_positions.lock() {
				map.remove(&guild);
			}
			return Err(DbError::WriterClosed);
		}
		Ok(())
	}

	/// 未処理の命令をすべて処理させ、スレッドの終了を待つ。
	///
	/// - スレッドは命令を書き切った後、WAL のチェックポイント (TRUNCATE) をしてから接続を閉じる。
	///   読み取り接続が開いたままでも、読み取り中でなければ WAL の中身は本体へ移り、WAL は空になる
	/// - 読み取り中の接続が `SHUTDOWN_CHECKPOINT_BUSY_TIMEOUT` を過ぎても終わらなければ、チェックポイントを諦めて閉じる
	///
	/// # エラー
	/// - すでに終了しているとき `WriterClosed`
	///
	/// # 呼び出し例
	/// ```ignore
	/// writer.close().await?;
	/// ```
	pub(super) async fn close(&self) -> Result<(), DbError> {
		let (ack_tx, ack_rx) = oneshot::channel();
		self.inner
			.tx
			.send(Command::Shutdown(ack_tx))
			.await
			.map_err(|_| DbError::WriterClosed)?;
		ack_rx.await.map_err(|_| DbError::WriterClosed)?;
		// 確認はチェックポイントと接続の破棄の後に届く。ここではスレッドの後始末を待つ
		let handle = self.inner.thread.lock().ok().and_then(|mut s| s.take());
		if let Some(handle) = handle {
			let _ = tokio::task::spawn_blocking(move || handle.join()).await;
		}
		Ok(())
	}
}

/// 書き込みスレッドの本体。
///
/// - 1 件を待って受け、続けて取れる分を `batch_size` まで集める
/// - 集めた命令を先頭から 1 トランザクションで実行し、経過が `TRANSACTION_TIME_LIMIT` を超えたら
///   そこまでをコミットして結果を返し、残りは新しいトランザクションで続ける
/// - Shutdown を受けたらそこまでを書き切り、チェックポイントと接続の破棄の後に確認を返して終わる
/// - 全ハンドルが落ちて受信が閉じたときも、チェックポイントをしてから終わる
fn writer_loop(
	mut conn: Connection,
	mut rx: mpsc::Receiver<Command>,
	batch_size: usize,
	inner: Weak<Inner>,
) {
	let mut shutdown_ack: Option<oneshot::Sender<()>> = None;
	while shutdown_ack.is_none() {
		let Some(first) = rx.blocking_recv() else {
			break;
		};
		let mut batch = vec![first];
		while batch.len() < batch_size && !matches!(batch.last(), Some(Command::Shutdown(_))) {
			match rx.try_recv() {
				Ok(cmd) => batch.push(cmd),
				Err(_) => break,
			}
		}
		let mut pending = batch.into_iter().peekable();
		while pending.peek().is_some() {
			let tx = match conn.transaction() {
				Ok(tx) => tx,
				Err(error) => {
					// 開始できないときは残りの命令を捨てる。呼び出し側へは WriterClosed が返る
					tracing::error!(%error, "DB のトランザクションを開始できません");
					for cmd in pending.by_ref() {
						discard_command(cmd, &inner, &mut shutdown_ack);
					}
					break;
				}
			};
			let started = Instant::now();
			let mut deliveries: Vec<Deliver> = Vec::new();
			for cmd in pending.by_ref() {
				execute_command(&tx, cmd, &inner, &mut shutdown_ack, &mut deliveries);
				if started.elapsed() >= TRANSACTION_TIME_LIMIT {
					break;
				}
			}
			// 結果はコミットの後で通知する。呼び出し側が戻った時点で読み取りから見えること、
			// コミット失敗を成功として返さないことを保証する
			let commit_error = match tx.commit() {
				Ok(()) => None,
				Err(error) => {
					tracing::error!(%error, "DB のコミットに失敗しました");
					Some(error.to_string())
				}
			};
			for deliver in deliveries {
				deliver(commit_error.clone());
			}
		}
	}
	checkpoint_before_close(&conn);
	// 接続を先に閉じてから確認を返す。呼び出し側が開き直しても競合しない
	drop(conn);
	if let Some(ack) = shutdown_ack {
		let _ = ack.send(());
	}
}

/// 1 件の命令をトランザクションの中で実行する。
///
/// # 引数
/// - `tx`: 実行中のトランザクション
/// - `cmd`: 命令
/// - `inner`: 保留中の再生位置の持ち主
/// - `shutdown_ack`: Shutdown を受けたら確認の送り先を入れる
/// - `deliveries`: コミット後に呼ぶ結果の通知処理を足す
///
/// # 呼び出し例
/// `execute_command(&tx, cmd, &inner, &mut shutdown_ack, &mut deliveries);`
fn execute_command(
	tx: &Connection,
	cmd: Command,
	inner: &Weak<Inner>,
	shutdown_ack: &mut Option<oneshot::Sender<()>>,
	deliveries: &mut Vec<Deliver>,
) {
	match cmd {
		Command::Run(job) => deliveries.extend(run_in_savepoint(tx, job)),
		Command::SavePosition(guild) => {
			let latest = inner
				.upgrade()
				.and_then(|i| i.pending_positions.lock().ok()?.remove(&guild));
			if let Some((position_ms, paused)) = latest {
				deliveries.extend(run_in_savepoint(
					tx,
					Box::new(move |c| {
						let ok = write_position(c, guild, position_ms, paused);
						(ok, Box::new(|_| {}) as Deliver)
					}),
				));
			}
		}
		Command::Shutdown(ack) => *shutdown_ack = Some(ack),
	}
}

/// トランザクションを開始できなかった命令を捨てる。
///
/// - Run は通知処理ごと捨てる (呼び出し側には WriterClosed が返る)
/// - 位置更新は保留を消す。残すとそのギルドの位置が以後保存されなくなる
///
/// # 引数
/// - `cmd`: 捨てる命令
/// - `inner`: 保留中の再生位置の持ち主
/// - `shutdown_ack`: Shutdown なら確認の送り先を入れる (終了は続ける)
///
/// # 呼び出し例
/// `discard_command(cmd, &inner, &mut shutdown_ack);`
fn discard_command(
	cmd: Command,
	inner: &Weak<Inner>,
	shutdown_ack: &mut Option<oneshot::Sender<()>>,
) {
	match cmd {
		Command::Shutdown(ack) => *shutdown_ack = Some(ack),
		Command::SavePosition(guild) => forget_pending(inner, guild),
		Command::Run(_) => {}
	}
}

/// 終了の前に WAL の中身を本体へ移し、WAL を空にする (`PRAGMA wal_checkpoint(TRUNCATE)`)。
///
/// - 読み取り接続が開いたままでも、読み取り中でなければ最後まで移せる
/// - 読み取り中の接続は `SHUTDOWN_CHECKPOINT_BUSY_TIMEOUT` まで待つ。移しきれなければ warn を残して続ける
/// - 失敗しても終了は止めない (WAL は次回の起動で取り込まれる)
///
/// # 引数
/// - `conn`: 書き込み接続 (この後で破棄する)
///
/// # 呼び出し例
/// `checkpoint_before_close(&conn);`
fn checkpoint_before_close(conn: &Connection) {
	if let Err(error) = conn.busy_timeout(SHUTDOWN_CHECKPOINT_BUSY_TIMEOUT) {
		tracing::warn!(%error, "チェックポイントの待ち時間を設定できません");
	}
	let started = Instant::now();
	// 結果の行は (busy, WAL のページ数, 移したページ数)。busy = 1 は読み取り中の接続があり移しきれなかった
	let result = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
		Ok((
			row.get::<_, i64>(0)?,
			row.get::<_, i64>(1)?,
			row.get::<_, i64>(2)?,
		))
	});
	let elapsed_ms = started.elapsed().as_millis() as u64;
	match result {
		Ok((0, log_pages, moved_pages)) => {
			tracing::debug!(
				log_pages,
				moved_pages,
				elapsed_ms,
				"WAL のチェックポイントを終えました"
			)
		}
		Ok((_, log_pages, moved_pages)) => tracing::warn!(
			log_pages,
			moved_pages,
			elapsed_ms,
			"読み取り中の接続があり、WAL を移しきれませんでした。次回の起動で取り込みます"
		),
		Err(error) => tracing::warn!(%error, elapsed_ms, "WAL のチェックポイントに失敗しました"),
	}
}

/// 再生位置を `playback_state` へ書く。成功なら true。
fn write_position(conn: &Connection, guild: GuildId, position_ms: u64, paused: bool) -> bool {
	let result = conn
		.prepare_cached(
			"UPDATE playback_state SET position_ms = ?1, paused = ?2, updated_at = ?3 WHERE guild_id = ?4",
		)
		.and_then(|mut statement| {
			statement.execute(rusqlite::params![
				position_ms as i64,
				paused,
				now_secs(),
				guild.get() as i64
			])
		});
	match result {
		Ok(_) => true,
		Err(error) => {
			tracing::warn!(%error, guild = guild.get(), "再生位置を保存できません");
			false
		}
	}
}

/// 保留中の再生位置を捨てる。
fn forget_pending(inner: &Weak<Inner>, guild: GuildId) {
	if let Some(inner) = inner.upgrade() {
		if let Ok(mut map) = inner.pending_positions.lock() {
			map.remove(&guild);
		}
	}
}

/// 1 件の処理を SAVEPOINT で囲んで実行する。
///
/// - 失敗または panic のときはその処理だけ巻き戻す
/// - 戻り値は結果の通知処理。panic、SAVEPOINT の開始失敗、RELEASE 失敗では None
///   （呼び出し側は通知を受けられず WriterClosed になる）
fn run_in_savepoint(conn: &Connection, job: Job) -> Option<Deliver> {
	if conn.execute_batch("SAVEPOINT cmd").is_err() {
		return None;
	}
	let outcome = catch_unwind(AssertUnwindSafe(|| job(conn))).ok();
	let ok = matches!(outcome, Some((true, _)));
	if ok {
		match conn.execute_batch("RELEASE cmd") {
			Ok(()) => return outcome.map(|(_, deliver)| deliver),
			Err(error) => tracing::error!(%error, "SAVEPOINT の解放に失敗しました"),
		}
	}
	if let Err(error) = conn.execute_batch("ROLLBACK TO cmd; RELEASE cmd") {
		tracing::error!(%error, "SAVEPOINT の巻き戻しに失敗しました");
	}
	// 失敗した処理の結果（Err）はそのまま通知する。RELEASE 失敗で巻き戻した場合は捨てる
	if ok {
		None
	} else {
		outcome.map(|(_, deliver)| deliver)
	}
}
