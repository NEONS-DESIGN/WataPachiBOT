//! ワーカーのプール。2 段の待ち行列で要求を割り当て、ワーカーの死活を管理する。
//!
//! 構成:
//! - 管理タスク 1 本 (`Manager`): 待ち行列と各ワーカーの状態を持つ。await は受信だけで、
//!   送信はすべて無制限チャネルのため、管理タスクは他のタスクを待たず、相互待ちにならない
//! - 実行タスクがワーカーごとに 1 本 (`Slot`): `WorkerProcess` を占有し、起動、要求の実行、
//!   死活確認、再起動を行う。応答は呼び出し元へ直接返す
//! - 共有するロックは一時停止の状態 (`PauseGate`) だけ。短い読み書きのみで、await をまたがない
//!
//! 一時停止 (`rate_limited`):
//! - `resolve`・`flat_playlist`・`search` が `rate_limited` を受けたら、プール全体で解決を止める。
//!   長さは `worker.rate_limit_backoff_secs` から倍々で、上限は `worker.rate_limit_backoff_max_secs`
//! - 停止中に届いた要求と、停止中に待ち行列に残っていた要求は、ワーカーへ送らずに
//!   `ResolveError::Paused` で返す。ping は止めない
//! - 停止中に届いた `rate_limited` (停止の前に送った要求の応答) は数えない。
//!   ping 以外の要求が成功すると、倍々の段階を初めに戻す
//!
//! 起動の確認:
//! - 起動 (再起動、入れ替えを含む) したワーカーは、`worker.startup_timeout_secs` 以内に ping に答えてから
//!   受付に入る。ワーカーは準備 (Cookie の読み込み、常駐 Deno の準備) を終えるまで標準入力を読まないため、
//!   準備の途中で要求を送ると `resolve_timeout_secs` を超え、落として起動し直すことを繰り返す
//! - 答えなければ起動の失敗として扱い、待ち時間を置いて起動し直す。確認の間、要求は待ち行列で待つ
//! - ワーカーには 1 要求の持ち時間 (`--request-budget-secs`) を渡す。値は `resolve_timeout_secs` から
//!   `REQUEST_BUDGET_MARGIN_SECS` を引いたもの。ワーカーは予備設定と常駐 Deno の待ちをこの範囲に収め、
//!   Rust 側に打ち切られる前に失敗の応答を返す
//!
//! 要求が必ず終わることの根拠:
//! - 実行中の要求は、応答、タイムアウト、死亡のいずれかで終わる。プロセスの操作はすべて上限付き
//! - 待ち行列の要求は、起動できるワーカーが無くなると `Unavailable` で、一時停止が始まると
//!   `Paused` で終わる
//! - Prefetch は、予約を残して実行できる台数が無くなると `Unavailable` で終わる
//! - 終了時は待ち行列を空にする。行き先を失った要求は応答の送信側が破棄され、
//!   `submit` が `Unavailable` を返す
//!
//! 呼び出し方:
//! ```no_run
//! # use std::sync::Arc;
//! # use watapachi_bot::{config::WorkerConfig, platform::job::Job, resolver::process::WorkerSpec};
//! # async fn example(spec: WorkerSpec, config: WorkerConfig, job: Arc<Job>
//! # ) -> Result<(), watapachi_bot::domain::ResolveError> {
//! use watapachi_bot::domain::Priority;
//! use watapachi_bot::resolver::pool::{WorkerClient, WorkerPool};
//! use watapachi_bot::resolver::protocol::WorkerRequest;
//!
//! let pool = WorkerPool::start(spec, &config, job);
//! let pong = pool.submit(WorkerRequest::Ping, Priority::Interactive).await?;
//! pool.shutdown().await;
//! # Ok(())
//! # }
//! ```

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, MutexGuard, PoisonError};
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio::time::{Instant, MissedTickBehavior};

use crate::config::WorkerConfig;
use crate::domain::{Priority, ResolveError, ResolveErrorCode};
use crate::platform::job::Job;
use crate::resolver::process::{CallError, WorkerProcess, WorkerSpec};
use crate::resolver::protocol::WorkerRequest;

/// 再起動の待ち時間の初期値。運用で変える理由が無いため固定値にし、設定にしない。
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// 再起動の待ち時間の上限。起動に失敗し続ける環境で再試行を詰めすぎないための固定値で、設定にしない。
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// 標準入力を閉じた後、自発的な終了を待つ上限。`shutdown.timeout_secs` の内側に収める。
const EXIT_GRACE: Duration = Duration::from_secs(5);
/// ワーカーへ 1 要求の持ち時間 (秒) を渡す引数の名前。worker.py が受け取る。
const REQUEST_BUDGET_ARG: &str = "--request-budget-secs";
/// `resolve_timeout_secs` から差し引いて、ワーカーへ渡す 1 要求の持ち時間を作る余裕 (秒)。
///
/// - ワーカーが持ち時間を使い切って失敗の応答を書いてから、Rust 側が受け取るまでの時間に充てる
///   (応答の直列化とパイプの転送、Windows でのスレッドの切り替え。大きな応答は 1 秒近くかかった)
/// - 余裕が無いと、ワーカーが応答する直前に Rust 側が打ち切ってワーカーを落とす。落とすと
///   ワーカー内の常駐 Deno の失敗の数も消え、同じ待ちを繰り返す
/// - `worker.resolve_timeout_secs` はこの値より大きいことを設定の検証で求める
pub const REQUEST_BUDGET_MARGIN_SECS: u64 = 2;

/// 要求への応答。
type Reply = Result<Value, ResolveError>;
/// 入れ替え 1 台分の結果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplaceOutcome {
	/// 新しいワーカーが ping に答えた。または入れ替える必要が無かった。
	Done,
	/// 起動か ping に失敗した。
	Failed,
	/// 全体の終了で中断した。
	Stopped,
}

/// 1 台の入れ替えの依頼。
struct ReplaceRequest {
	ack: oneshot::Sender<ReplaceOutcome>,
	/// 入れ替えの目的の世代。この世代の起動方法で動いている台は入れ替えずに `Done` を返す。
	///
	/// - 入れ替えの途中で死亡やタイムアウトで再起動した台は、すでに新しい起動方法で動いている。
	///   もう一度入れ替えると、要求を受けられない時間と Python の起動の費用が無駄になる
	/// - 巻き戻しでは元の世代を指定し、元の起動方法で動いている台を再起動しない
	target: u64,
}

impl ReplaceRequest {
	/// 結果を返す。依頼元が取り下げていれば何もしない。
	///
	/// # 引数
	/// - `outcome`: 結果
	///
	/// # 例
	/// `request.answer(ReplaceOutcome::Done);`
	fn answer(self, outcome: ReplaceOutcome) {
		let _ = self.ack.send(outcome);
	}
}

/// 世代番号付きの起動方法。ワーカーがどの起動方法で動いているかを見分ける。
#[derive(Debug)]
struct VersionedSpec {
	spec: WorkerSpec,
	generation: u64,
}

/// `rolling_restart` の失敗。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RollingRestartError {
	/// 新しい起動方法のワーカーが起動か ping に失敗した。全台を元の起動方法へ戻そうとした。
	///
	/// - `not_restored` が空なら全台が元の環境で動いている。新しい環境は消してよい
	/// - 空でなければ、その台が新しい環境のまま残った可能性がある。新しい環境を消してはならない
	#[error(
		"ワーカー {slot} を新しい環境で動かせません。元の環境へ戻せなかった台: {not_restored:?}"
	)]
	Failed {
		slot: usize,
		not_restored: Vec<usize>,
	},
	/// 全体の終了で中断した。
	#[error("終了中のため入れ替えを中断しました")]
	Stopped,
}

/// ワーカーへ要求を投げる窓口。`CachedResolver` はこの trait に依存する。
#[async_trait]
pub trait WorkerClient: Send + Sync {
	/// 要求を実行して `result` を返す。
	///
	/// # 引数
	/// - `request`: ワーカーへの要求
	/// - `priority`: 待ち行列の段
	///
	/// # 戻り値
	/// 成功応答の `result`
	///
	/// # エラー
	/// - ワーカーが失敗を返した: `ResolveError::Worker`
	/// - 応答が不正だった: `ResolveError::Worker` (`Internal`)。その台は再起動する
	/// - 再試行の後も上限を超えた: `ResolveError::Timeout`
	/// - 再試行の後もワーカーが死んだ: `ResolveError::WorkerDied`
	/// - 実行できるワーカーが無い、終了中: `ResolveError::Unavailable`
	/// - 回数制限で一時停止中 (ping を除く): `ResolveError::Paused`
	///
	/// # 例
	/// `let value = client.submit(WorkerRequest::Ping, Priority::Interactive).await?;`
	async fn submit(&self, request: WorkerRequest, priority: Priority) -> Reply;
}

/// 待ち行列を流れる要求。
struct Task {
	request: WorkerRequest,
	priority: Priority,
	reply: oneshot::Sender<Reply>,
	/// 死亡かタイムアウトによる再試行を使い切ったか。
	retried: bool,
	/// 再試行で避けるワーカー。
	avoid: Option<usize>,
}

/// 管理タスクへの通知。
enum Event {
	/// 新しい要求。
	Submit(Task),
	/// ワーカーが次の要求を受けられる。
	Ready(usize),
	/// ワーカーが死に、再起動を待っている。
	Down(usize),
	/// ワーカーを起動できなかった。
	SpawnFailed(usize),
	/// 要求を待ち行列の先頭へ戻す。
	Requeue(Task),
	/// 実行タスクが終わった。
	SlotExited(usize),
	/// 空いたらワーカーを入れ替える。
	Replace {
		slot: usize,
		request: ReplaceRequest,
	},
	/// 待機中のワーカーが ping を送りたい。空いていれば許可する。
	PingDue(usize),
	/// 全体を終了する。完了の通知先が無い場合は `None`。
	Shutdown(Option<oneshot::Sender<()>>),
}

/// 実行タスクへの指示。
enum SlotCommand {
	/// 要求を実行する。
	Run(Task),
	/// 標準入力を閉じて入れ替える。
	Replace(ReplaceRequest),
	/// ping を送る。管理タスクはこの間ワーカーを Busy として扱う。
	Ping,
}

/// 管理タスクから見たワーカーの状態。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotState {
	/// 起動前か再起動の待機中。間もなく `Idle` になる見込みがある。
	Starting,
	/// 要求を受けられる。
	Idle,
	/// 要求か ping を実行中。
	Busy,
	/// 入れ替え中。
	Replacing,
	/// 直近の起動に失敗した。再試行は続くが、回復の見込みを持たない。
	Failed,
	/// 実行タスクが終わった。
	Exited,
}

impl SlotState {
	/// 要求を処理できる見込みがあるか。
	///
	/// # 戻り値
	/// `Failed` と `Exited` 以外で真
	///
	/// # 例
	/// `assert!(SlotState::Starting.can_serve());`
	fn can_serve(self) -> bool {
		!matches!(self, SlotState::Failed | SlotState::Exited)
	}
}

/// 管理タスクが持つ 1 台分の情報。
struct SlotEntry {
	state: SlotState,
	commands: mpsc::UnboundedSender<SlotCommand>,
	/// 実行中の要求が終わるのを待っている入れ替え。
	pending_replace: Option<ReplaceRequest>,
}

/// 待ち行列とワーカーの状態を持つ管理タスク。
struct Manager {
	slots: Vec<SlotEntry>,
	interactive: VecDeque<Task>,
	prefetch: VecDeque<Task>,
	reserved: usize,
	stopping: bool,
	stop_signal: watch::Sender<bool>,
	shutdown_waiters: Vec<oneshot::Sender<()>>,
	pause: Arc<PauseGate>,
}

impl Manager {
	/// 全ての実行タスクが終わるまで通知を処理する。
	///
	/// # 引数
	/// - `events`: 通知の受信側
	///
	/// # 例
	/// `tokio::spawn(manager.run(receiver));`
	async fn run(mut self, mut events: mpsc::UnboundedReceiver<Event>) {
		while let Some(event) = events.recv().await {
			self.handle(event);
			self.pump();
			if self.stopping
				&& self
					.slots
					.iter()
					.all(|slot| slot.state == SlotState::Exited)
			{
				break;
			}
		}
		for waiter in self.shutdown_waiters {
			let _ = waiter.send(());
		}
	}

	/// 通知 1 件で状態を更新する。
	///
	/// # 引数
	/// - `event`: 通知
	///
	/// # 例
	/// `self.handle(Event::Ready(0));`
	fn handle(&mut self, event: Event) {
		match event {
			Event::Submit(task) => self.enqueue(task, false),
			Event::Requeue(task) => self.enqueue(task, true),
			Event::Ready(slot) => self.on_ready(slot),
			Event::Down(slot) => self.slots[slot].state = SlotState::Starting,
			Event::SpawnFailed(slot) => {
				let entry = &mut self.slots[slot];
				entry.state = SlotState::Failed;
				if let Some(request) = entry.pending_replace.take() {
					request.answer(ReplaceOutcome::Failed);
				}
			}
			Event::SlotExited(slot) => {
				let entry = &mut self.slots[slot];
				entry.state = SlotState::Exited;
				if let Some(request) = entry.pending_replace.take() {
					request.answer(ReplaceOutcome::Stopped);
				}
			}
			Event::Replace { slot, request } => self.on_replace(slot, request),
			Event::PingDue(slot) => {
				let entry = &mut self.slots[slot];
				// 空いていない (要求を送った後など) なら見送る。次の間隔で再び届く
				if !self.stopping && entry.state == SlotState::Idle {
					entry.state = SlotState::Busy;
					let _ = entry.commands.send(SlotCommand::Ping);
				}
			}
			Event::Shutdown(waiter) => self.on_shutdown(waiter),
		}
	}

	/// 要求を待ち行列へ入れる。終了中なら失敗を返す。
	///
	/// # 引数
	/// - `task`: 要求
	/// - `front`: 先頭へ入れるか。再試行は先に来た要求なので先頭へ戻す
	///
	/// # 例
	/// `self.enqueue(task, false);`
	fn enqueue(&mut self, task: Task, front: bool) {
		if self.stopping {
			let _ = task.reply.send(Err(ResolveError::Unavailable));
			return;
		}
		let queue = self.queue_mut(task.priority);
		if front {
			queue.push_front(task);
		} else {
			queue.push_back(task);
		}
	}

	/// ワーカーが空いた。入れ替えが待っていれば先に行う。
	///
	/// # 引数
	/// - `slot`: ワーカーの番号
	///
	/// # 例
	/// `self.on_ready(0);`
	fn on_ready(&mut self, slot: usize) {
		let entry = &mut self.slots[slot];
		match entry.pending_replace.take() {
			Some(request) => {
				entry.state = SlotState::Replacing;
				let _ = entry.commands.send(SlotCommand::Replace(request));
			}
			None => entry.state = SlotState::Idle,
		}
	}

	/// 入れ替えの依頼を受ける。処理中なら新規の割り当てを止めて完了を待つ。
	///
	/// # 引数
	/// - `slot`: ワーカーの番号
	/// - `request`: 依頼
	///
	/// # 例
	/// `self.on_replace(0, request);`
	fn on_replace(&mut self, slot: usize, request: ReplaceRequest) {
		let stopping = self.stopping;
		let entry = &mut self.slots[slot];
		if stopping || entry.state == SlotState::Exited {
			request.answer(ReplaceOutcome::Stopped);
		} else if entry.state == SlotState::Idle {
			entry.state = SlotState::Replacing;
			let _ = entry.commands.send(SlotCommand::Replace(request));
		} else {
			// Busy と Starting は Ready で、Failed は次の Ready か SpawnFailed で決着する
			entry.pending_replace = Some(request);
		}
	}

	/// 終了を始める。待ち行列を空にし、実行タスクへ合図する。
	///
	/// # 引数
	/// - `waiter`: 完了の通知先
	///
	/// # 例
	/// `self.on_shutdown(None);`
	fn on_shutdown(&mut self, waiter: Option<oneshot::Sender<()>>) {
		self.stopping = true;
		self.shutdown_waiters.extend(waiter);
		// 受信側 (実行タスク) が全滅していると失敗するが、その場合は合図が要らない
		let _ = self.stop_signal.send(true);
		fail_all(&mut self.interactive);
		fail_all(&mut self.prefetch);
		for entry in &mut self.slots {
			if let Some(request) = entry.pending_replace.take() {
				request.answer(ReplaceOutcome::Stopped);
			}
		}
	}

	/// 実行できない要求を落とし、実行できる要求を空きワーカーへ割り当てる。
	///
	/// - Interactive を先に割り当てるため、Prefetch が追い越すことはない
	/// - 呼び出し元が取り下げた要求は実行しない
	/// - 一時停止中は ping 以外の要求を、空きを待たずに `Paused` で返す
	///
	/// # 例
	/// `self.pump();`
	fn pump(&mut self) {
		if self.stopping {
			return;
		}
		self.interactive.retain(|task| !task.reply.is_closed());
		self.prefetch.retain(|task| !task.reply.is_closed());
		if let Some(retry_at) = self.pause.paused_until() {
			reject_paused(&mut self.interactive, retry_at);
			reject_paused(&mut self.prefetch, retry_at);
		}

		let capacity = self
			.slots
			.iter()
			.filter(|slot| slot.state.can_serve())
			.count();
		if capacity == 0 {
			fail_all(&mut self.interactive);
		}
		if capacity <= self.reserved {
			fail_all(&mut self.prefetch);
		}
		self.dispatch(Priority::Interactive);
		self.dispatch(Priority::Prefetch);
	}

	/// 優先度に対応する待ち行列 (読み取り用)。
	///
	/// # 引数
	/// - `priority`: 優先度
	///
	/// # 戻り値
	/// その優先度の待ち行列
	///
	/// # 例
	/// `let next = self.queue(Priority::Interactive).front();`
	fn queue(&self, priority: Priority) -> &VecDeque<Task> {
		match priority {
			Priority::Interactive => &self.interactive,
			Priority::Prefetch => &self.prefetch,
		}
	}

	/// 優先度に対応する待ち行列 (書き換え用)。
	///
	/// # 引数
	/// - `priority`: 優先度
	///
	/// # 戻り値
	/// その優先度の待ち行列
	///
	/// # 例
	/// `self.queue_mut(task.priority).push_back(task);`
	fn queue_mut(&mut self, priority: Priority) -> &mut VecDeque<Task> {
		match priority {
			Priority::Interactive => &mut self.interactive,
			Priority::Prefetch => &mut self.prefetch,
		}
	}

	/// 片方の待ち行列から、前の要求から順に割り当てる。
	///
	/// - Prefetch は割り当て前の空きが予約数を超えるときだけ実行する
	/// - 一時停止中に取り出した ping 以外の要求は、ワーカーへ送らずに `Paused` で返す。
	///   `pump` の確認の後に別の台が停止を始めた場合に当たる
	///
	/// # 引数
	/// - `priority`: 対象の待ち行列
	///
	/// # 例
	/// `self.dispatch(Priority::Interactive);`
	fn dispatch(&mut self, priority: Priority) {
		let keep_free = match priority {
			Priority::Interactive => 0,
			Priority::Prefetch => self.reserved,
		};
		while self.idle_count() > keep_free {
			let Some(task) = self.queue(priority).front() else {
				break;
			};
			let Some(slot) = self.pick_slot(task) else {
				// 空いている台が無い。ループの条件で起こらないが、念のため止める
				break;
			};
			if let Some(task) = self.queue_mut(priority).pop_front() {
				if let Some(retry_at) = paused_for(&self.pause, &task.request) {
					let _ = task.reply.send(Err(ResolveError::Paused { retry_at }));
					continue;
				}
				let entry = &mut self.slots[slot];
				entry.state = SlotState::Busy;
				// 実行タスクが消えていると要求ごと破棄され、submit は Unavailable を返す
				let _ = entry.commands.send(SlotCommand::Run(task));
			}
		}
	}

	/// 空いているワーカーの数を返す。
	///
	/// # 例
	/// `let idle = self.idle_count();`
	fn idle_count(&self) -> usize {
		self.slots
			.iter()
			.filter(|slot| slot.state == SlotState::Idle)
			.count()
	}

	/// 要求を任せるワーカーを選ぶ。
	///
	/// - 再試行の要求は、空いている別のワーカーを優先する
	/// - 別の台が空いていなければ、失敗した台でも空いていれば使う。
	///   失敗した台は死亡かタイムアウトで再起動しており、`Idle` なら新しいプロセスで動いている。
	///   他の台が実行中のときに、その終わり (最大で `resolve_timeout_secs`) を待たせないため
	///
	/// # 引数
	/// - `task`: 要求
	///
	/// # 戻り値
	/// ワーカーの番号。空いている台が無い場合は `None`
	///
	/// # 例
	/// `let slot = self.pick_slot(&task);`
	fn pick_slot(&self, task: &Task) -> Option<usize> {
		let is_idle = |index: usize| self.slots[index].state == SlotState::Idle;
		(0..self.slots.len())
			.filter(|index| Some(*index) != task.avoid)
			.find(|index| is_idle(*index))
			.or_else(|| task.avoid.filter(|avoid| is_idle(*avoid)))
	}
}

/// 待ち行列の要求をすべて `Unavailable` で終える。
///
/// # 引数
/// - `queue`: 対象の待ち行列
///
/// # 例
/// `fail_all(&mut self.prefetch);`
fn fail_all(queue: &mut VecDeque<Task>) {
	for task in queue.drain(..) {
		let _ = task.reply.send(Err(ResolveError::Unavailable));
	}
}

/// 待ち行列のうち ping 以外の要求を `Paused` で終える。ping は順序を保って残す。
///
/// # 引数
/// - `queue`: 対象の待ち行列
/// - `retry_at`: 再開の時刻
///
/// # 例
/// `reject_paused(&mut self.interactive, retry_at);`
fn reject_paused(queue: &mut VecDeque<Task>, retry_at: Instant) {
	for task in std::mem::take(queue) {
		if is_pausable(&task.request) {
			let _ = task.reply.send(Err(ResolveError::Paused { retry_at }));
		} else {
			queue.push_back(task);
		}
	}
}

/// 一時停止の対象の要求か。
///
/// - YouTube へ問い合わせる `resolve`・`flat_playlist`・`search` が対象。死活確認の ping は対象外
///
/// # 引数
/// - `request`: 要求
///
/// # 戻り値
/// 対象なら真
///
/// # 例
/// `assert!(!is_pausable(&WorkerRequest::Ping));`
fn is_pausable(request: &WorkerRequest) -> bool {
	!matches!(request, WorkerRequest::Ping)
}

/// タイムアウトした要求を別の台で再試行するか。
///
/// - `flat_playlist` は再試行しない。タイムアウトの多くは長いプレイリストのページの取得で、
///   別の台でも同じだけかかる。再試行すると 2 台とも上限まで待って落とすことになる
/// - 他の要求は再試行する (1 台の一時的な不調で終わることが多い)
///
/// # 引数
/// - `request`: タイムアウトした要求
///
/// # 戻り値
/// 再試行するなら真
///
/// # 例
/// `assert!(!retries_after_timeout(&WorkerRequest::FlatPlaylist { url, limit: 200 }));`
fn retries_after_timeout(request: &WorkerRequest) -> bool {
	!matches!(request, WorkerRequest::FlatPlaylist { .. })
}

/// 要求を一時停止で止めるべきなら、再開の時刻を返す。
///
/// # 引数
/// - `pause`: 一時停止の状態
/// - `request`: 要求
///
/// # 戻り値
/// 止めるなら再開の時刻。送ってよいなら `None`
///
/// # 例
/// `if let Some(retry_at) = paused_for(&self.pause, &task.request) { /* Paused で返す */ }`
fn paused_for(pause: &PauseGate, request: &WorkerRequest) -> Option<Instant> {
	if is_pausable(request) {
		pause.paused_until()
	} else {
		None
	}
}

/// 一時停止の長さ (純粋)。`min(base × 2^(strikes-1), max)`。
///
/// - `strikes` は 1 以上。0 は 1 と同じに扱う
/// - 桁があふれる大きさは `max` にする
///
/// # 引数
/// - `strikes`: 成功を挟まずに続いた一時停止の回数 (今回を含む)
/// - `base`: 1 回目の長さ (`worker.rate_limit_backoff_secs`)
/// - `max`: 上限 (`worker.rate_limit_backoff_max_secs`)
///
/// # 戻り値
/// 一時停止の長さ
///
/// # 例
/// ```
/// use std::time::Duration;
/// use watapachi_bot::resolver::pool::pause_duration;
///
/// let (base, max) = (Duration::from_secs(5), Duration::from_secs(60));
/// assert_eq!(pause_duration(2, base, max), Duration::from_secs(10));
/// ```
pub fn pause_duration(strikes: u32, base: Duration, max: Duration) -> Duration {
	2u32.checked_pow(strikes.saturating_sub(1))
		.and_then(|factor| base.checked_mul(factor))
		.map_or(max, |length| length.min(max))
}

/// 一時停止の状態の中身。
#[derive(Debug, Default)]
struct PauseState {
	/// 再開の時刻。過ぎていれば停止していない。
	paused_until: Option<Instant>,
	/// 成功を挟まずに続いた一時停止の回数。
	strikes: u32,
}

/// プール全体の一時停止。`WorkerPool`、管理タスク、実行タスクで共有する。
///
/// - ロックは短い読み書きだけで、await をまたがない
/// - ロックの保持中にパニックしたタスクがあっても、値を取り出して続ける。
///   各フィールドの更新は単独の代入で、壊れた値は残らない
struct PauseGate {
	state: std::sync::Mutex<PauseState>,
	base: Duration,
	max: Duration,
}

impl PauseGate {
	/// 停止していない状態を作る。
	///
	/// # 引数
	/// - `config`: `[worker]` の設定。`rate_limit_backoff_secs` と `rate_limit_backoff_max_secs` を使う
	///
	/// # 戻り値
	/// 停止していない状態
	///
	/// # 例
	/// `let pause = Arc::new(PauseGate::new(config));`
	fn new(config: &WorkerConfig) -> Self {
		Self {
			state: std::sync::Mutex::new(PauseState::default()),
			base: Duration::from_secs(config.rate_limit_backoff_secs),
			max: Duration::from_secs(config.rate_limit_backoff_max_secs),
		}
	}

	/// 状態のロックを取る。
	///
	/// # 戻り値
	/// ロックの保持者。await をまたいで持たないこと
	///
	/// # 例
	/// `self.lock().strikes = 0;`
	fn lock(&self) -> MutexGuard<'_, PauseState> {
		self.state.lock().unwrap_or_else(PoisonError::into_inner)
	}

	/// 一時停止中なら再開の時刻を返す。
	///
	/// # 戻り値
	/// 再開の時刻が今より後なら `Some`
	///
	/// # 例
	/// `let until = pause.paused_until();`
	fn paused_until(&self) -> Option<Instant> {
		let now = Instant::now();
		self.lock().paused_until.filter(|until| *until > now)
	}

	/// `rate_limited` を受けた。停止中でなければ段階を 1 つ進めて停止を始める。
	///
	/// - 停止中に届いたもの (停止の前に別の台へ送った要求の応答) は何も変えない
	/// - 時刻が表せないほど長い停止は、`base` の長さで代える
	///
	/// # 例
	/// `self.pause.on_rate_limited();`
	fn on_rate_limited(&self) {
		let now = Instant::now();
		let mut state = self.lock();
		if state.paused_until.is_some_and(|until| until > now) {
			return;
		}
		let strikes = state.strikes.saturating_add(1);
		let length = pause_duration(strikes, self.base, self.max);
		let until = now
			.checked_add(length)
			.or_else(|| now.checked_add(self.base));
		let Some(until) = until else {
			drop(state);
			tracing::error!(strikes, "一時停止の再開の時刻を表せないため、停止しません");
			return;
		};
		state.strikes = strikes;
		state.paused_until = Some(until);
		drop(state);
		tracing::warn!(
			strikes,
			pause_secs = until.duration_since(now).as_secs(),
			"YouTube の利用制限を受けたため、解決を一時停止します"
		);
	}

	/// ping 以外の要求が成功した。段階を初めに戻す。再開の時刻は変えず、期限で明ける。
	///
	/// # 例
	/// `self.pause.on_success();`
	fn on_success(&self) {
		self.lock().strikes = 0;
	}
}

/// 再起動の待ち時間。失敗が続くと倍々に伸びる。
#[derive(Debug)]
struct Backoff {
	next: Duration,
}

impl Backoff {
	/// 初期値の待ち時間を作る。
	///
	/// # 例
	/// `let mut backoff = Backoff::new();`
	fn new() -> Self {
		Self {
			next: INITIAL_BACKOFF,
		}
	}

	/// 今回の待ち時間を返し、次回の分を伸ばす。
	///
	/// # 戻り値
	/// 今回の待ち時間
	///
	/// # 例
	/// `let delay = backoff.advance();`
	fn advance(&mut self) -> Duration {
		let current = self.next;
		self.next = (current * 2).min(MAX_BACKOFF);
		current
	}

	/// 初期値へ戻す。ワーカーが応答したときに呼ぶ。
	///
	/// # 例
	/// `backoff.reset();`
	fn reset(&mut self) {
		self.next = INITIAL_BACKOFF;
	}
}

/// `serve` が終わった理由。
enum Served {
	/// 全体の終了。`force` なら処理中の要求を打ち切ったので待たずに落とす。
	Stop { force: bool },
	/// ワーカーが使えなくなった。
	Failed,
	/// 入れ替えの指示を受けた。
	Replace(ReplaceRequest),
}

/// 全体の終了の合図を待つ。
///
/// - 送信側が消えた場合も終了として扱う
/// - `wait_for` の戻り値は読み取りロックを持つため、ここで捨てて await をまたがせない
///
/// # 引数
/// - `signal`: 合図の受信側
///
/// # 例
/// `stopped(&mut self.stop_signal).await;`
async fn stopped(signal: &mut watch::Receiver<bool>) {
	let _ = signal.wait_for(|stop| *stop).await;
}

/// 実行タスクの終了を、パニックの場合も含めて管理タスクへ伝える。
struct ExitNotice {
	index: usize,
	events: mpsc::UnboundedSender<Event>,
}

impl Drop for ExitNotice {
	/// 終了を通知する。管理タスクが先に消えていれば何もしない。
	fn drop(&mut self) {
		let _ = self.events.send(Event::SlotExited(self.index));
	}
}

/// ワーカー 1 台を担当する実行タスク。
struct Slot {
	index: usize,
	events: mpsc::UnboundedSender<Event>,
	commands: mpsc::UnboundedReceiver<SlotCommand>,
	stop_signal: watch::Receiver<bool>,
	current: Arc<ArcSwap<VersionedSpec>>,
	job: Arc<Job>,
	timeout: Duration,
	/// 起動したワーカーが最初の ping に答えるまでの上限 (`worker.startup_timeout_secs`)。
	startup_timeout: Duration,
	/// ワーカーへ渡す 1 要求の持ち時間 (秒)。
	request_budget_secs: u64,
	ping_interval: Duration,
	backoff: Backoff,
	/// 次の起動の確認後に完了を返す入れ替え。
	pending_replace: Option<ReplaceRequest>,
	/// 動いているワーカーの起動方法の世代。
	generation: Option<u64>,
	pause: Arc<PauseGate>,
}

impl Slot {
	/// 全体の終了までワーカーの起動と実行を繰り返す。
	///
	/// # 例
	/// `tokio::spawn(slot.run());`
	async fn run(mut self) {
		let _notice = ExitNotice {
			index: self.index,
			events: self.events.clone(),
		};
		let mut delay = None;
		loop {
			if let Some(delay) = delay.take() {
				if !self.wait_backoff(delay).await {
					return;
				}
			}
			// 入れ替えで閉じた直後などに終了が始まっていれば、新しく起動しない。
			// 値はすぐ写し取り、読み取りロックを await へ持ち越さない
			let stop = *self.stop_signal.borrow();
			if stop {
				return;
			}
			let Some(mut process) = self.start_process() else {
				delay = Some(self.backoff.advance());
				continue;
			};
			let request = self.pending_replace.take();
			// 準備を終えて ping に答えるまで受付に入れない。この間、要求は待ち行列で待つ
			match self.verify(&mut process).await {
				Verified::Answered => {}
				Verified::Stopped => {
					process.kill().await;
					return;
				}
				Verified::NoAnswer => {
					// 落としてから返す。返った後に依頼元が環境を消しても、使用中のファイルが無い
					process.kill().await;
					if request.is_some() {
						self.notify(Event::Down(self.index));
					} else {
						// 起動の失敗と同じに扱う。動く台が無ければ待ち行列の要求を待たせ続けない
						self.notify(Event::SpawnFailed(self.index));
					}
					answer(request, ReplaceOutcome::Failed);
					delay = Some(self.backoff.advance());
					continue;
				}
			}
			self.notify(Event::Ready(self.index));
			// Ready の後に返す。次の台の入れ替えが始まる時点でこの台が受付に戻っている
			answer(request, ReplaceOutcome::Done);

			match self.serve(&mut process).await {
				Served::Stop { force: true } => {
					process.kill().await;
					return;
				}
				Served::Stop { force: false } => {
					process.close(EXIT_GRACE).await;
					return;
				}
				Served::Failed => {
					// 落とす前に Down を送る。強制終了の待ち (最大で数秒) の間に、待機中だった台へ
					// 新しい要求を割り当てさせないため (割り当てられた要求は wait_backoff が戻すが、その分遅れる)
					self.notify(Event::Down(self.index));
					process.kill().await;
					delay = Some(self.backoff.advance());
				}
				Served::Replace(request) => {
					process.close(EXIT_GRACE).await;
					self.pending_replace = Some(request);
				}
			}
		}
	}

	/// 管理タスクへ通知する。
	///
	/// - 管理タスクが消えているのは終了後だけなので、失敗は無視する
	///
	/// # 引数
	/// - `event`: 通知
	///
	/// # 例
	/// `self.notify(Event::Ready(self.index));`
	fn notify(&self, event: Event) {
		let _ = self.events.send(event);
	}

	/// 現在の起動方法でワーカーを起動する。
	///
	/// - 起動方法の引数の後ろに、1 要求の持ち時間 (`--request-budget-secs`) を足す
	///
	/// # 戻り値
	/// 起動したワーカー。失敗すると通知を済ませて `None` を返す
	///
	/// # 例
	/// `let process = self.start_process();`
	fn start_process(&mut self) -> Option<WorkerProcess> {
		let current = self.current.load_full();
		let mut spec = current.spec.clone();
		spec.args.extend([
			REQUEST_BUDGET_ARG.to_owned(),
			self.request_budget_secs.to_string(),
		]);
		match WorkerProcess::spawn(&spec, &self.job) {
			Ok(process) => {
				self.generation = Some(current.generation);
				Some(process)
			}
			Err(error) => {
				tracing::error!(%error, python = %current.spec.python.display(), "ワーカーを起動できません");
				self.generation = None;
				self.notify(Event::SpawnFailed(self.index));
				answer(self.pending_replace.take(), ReplaceOutcome::Failed);
				None
			}
		}
	}

	/// 再起動の待ち時間を過ごす。
	///
	/// - この間に届いた要求は待ち行列へ戻す。管理タスクが `Down` を受ける前に送った分が来る
	/// - 入れ替えの指示は次の起動の確認後に完了を返す
	///
	/// # 引数
	/// - `delay`: 待ち時間
	///
	/// # 戻り値
	/// 待ち終えたら真。全体の終了で中断したら偽
	///
	/// # 例
	/// `if !self.wait_backoff(delay).await { return; }`
	async fn wait_backoff(&mut self, delay: Duration) -> bool {
		let sleep = tokio::time::sleep(delay);
		tokio::pin!(sleep);
		loop {
			tokio::select! {
				biased;
				_ = stopped(&mut self.stop_signal) => return false,
				command = self.commands.recv() => match command {
					None => return false,
					Some(SlotCommand::Run(task)) => self.notify(Event::Requeue(task)),
					// 次の起動は現在の起動方法を使うため、起動の確認後に完了を返す
					Some(SlotCommand::Replace(request)) => {
						answer(self.pending_replace.replace(request), ReplaceOutcome::Failed);
					}
					// 管理タスクが Down を受ける前に許可した分。ping する相手がいない
					Some(SlotCommand::Ping) => {}
				},
				_ = &mut sleep => return true,
			}
		}
	}

	/// 起動したワーカーが `startup_timeout` 以内に ping に答えることを確かめる。
	///
	/// - 起動、再起動、入れ替えのすべてで、受付に入る前に呼ぶ
	///
	/// # 引数
	/// - `process`: 対象のワーカー
	///
	/// # 戻り値
	/// 確認の結果
	///
	/// # 例
	/// `let verified = self.verify(&mut process).await;`
	async fn verify(&mut self, process: &mut WorkerProcess) -> Verified {
		let limit = self.startup_timeout;
		match self.call(process, &WorkerRequest::Ping, limit).await {
			None => Verified::Stopped,
			Some(Ok(_)) => {
				self.backoff.reset();
				Verified::Answered
			}
			Some(Err(error)) => {
				tracing::error!(slot = self.index, %error, "起動したワーカーが ping に答えません");
				Verified::NoAnswer
			}
		}
	}

	/// 全体の終了を見張りながら要求を 1 件実行する。
	///
	/// # 引数
	/// - `process`: 対象のワーカー
	/// - `request`: 要求
	/// - `limit`: 応答を待つ上限
	///
	/// # 戻り値
	/// 実行の結果。全体の終了で打ち切ったら `None`
	///
	/// # 例
	/// `let result = self.call(process, &WorkerRequest::Ping, self.timeout).await;`
	async fn call(
		&mut self,
		process: &mut WorkerProcess,
		request: &WorkerRequest,
		limit: Duration,
	) -> Option<Result<Value, CallError>> {
		tokio::select! {
			biased;
			_ = stopped(&mut self.stop_signal) => None,
			result = process.call(request, limit) => Some(result),
		}
	}

	/// ワーカーが使える間、指示の実行と死活確認を続ける。
	///
	/// # 引数
	/// - `process`: 対象のワーカー
	///
	/// # 戻り値
	/// 終わった理由
	///
	/// # 例
	/// `let served = self.serve(&mut process).await;`
	async fn serve(&mut self, process: &mut WorkerProcess) -> Served {
		let mut ping =
			tokio::time::interval_at(Instant::now() + self.ping_interval, self.ping_interval);
		ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
		loop {
			tokio::select! {
				biased;
				_ = stopped(&mut self.stop_signal) => return Served::Stop { force: false },
				command = self.commands.recv() => match command {
					None => return Served::Stop { force: false },
					Some(SlotCommand::Replace(request)) => {
						if self.generation == Some(request.target) {
							// すでに目的の世代の起動方法で動いている (入れ替えの途中で再起動した台など)
							self.notify(Event::Ready(self.index));
							request.answer(ReplaceOutcome::Done);
						} else {
							return Served::Replace(request);
						}
					}
					Some(SlotCommand::Run(task)) => {
						if let Some(served) = self.run_task(process, task).await {
							return served;
						}
						ping.reset();
					}
					Some(SlotCommand::Ping) => {
						let limit = self.timeout;
						match self.call(process, &WorkerRequest::Ping, limit).await {
							None => return Served::Stop { force: true },
							Some(Ok(_)) | Some(Err(CallError::Rejected { .. })) => {
								self.backoff.reset();
								self.notify(Event::Ready(self.index));
							}
							Some(Err(error)) => {
								tracing::warn!(%error, "ワーカーが ping に答えません");
								return Served::Failed;
							}
						}
					}
				},
				output = process.wait_for_output() => {
					match output {
						Ok(0) => tracing::warn!("待機中のワーカーが終了しました"),
						Ok(_) => tracing::warn!("ワーカーが要求の無い出力をしました"),
						Err(error) => tracing::warn!(%error, "ワーカーの標準出力を読めません"),
					}
					return Served::Failed;
				}
				// 自分で ping を始めない。空いていれば管理タスクが Busy にしてから Ping を返す。
				// こうすると ping の間に要求が割り当てられず、利用者を待たせない
				_ = ping.tick() => self.notify(Event::PingDue(self.index)),
			}
		}
	}

	/// 要求を 1 件実行し、結果を呼び出し元へ返す。
	///
	/// - 死亡とタイムアウトは 1 回だけ別の台で再試行する (一時的な不調で利用者の操作を失敗させないため)。
	///   不正な応答は再試行しない。同じ要求で別の台まで再起動させないため
	/// - `flat_playlist` のタイムアウトは再試行しない。長いプレイリストは別の台でも同じだけかかり、
	///   2 台とも落として利用者を 2 倍待たせるため
	/// - ping 以外の `rate_limited` で一時停止を始め、成功で段階を戻す。呼び出し元へ返す前と
	///   `Ready` の前に更新し、続く要求と待ち行列の要求が必ず停止を見るようにする
	///
	/// # 引数
	/// - `process`: 対象のワーカー
	/// - `task`: 要求
	///
	/// # 戻り値
	/// ワーカーを続けて使えるなら `None`。使えないなら終わる理由
	///
	/// # 例
	/// `let served = self.run_task(process, task).await;`
	async fn run_task(&mut self, process: &mut WorkerProcess, task: Task) -> Option<Served> {
		let limit = self.timeout;
		let Some(result) = self.call(process, &task.request, limit).await else {
			let _ = task.reply.send(Err(ResolveError::Unavailable));
			return Some(Served::Stop { force: true });
		};
		let pausable = is_pausable(&task.request);
		match result {
			Ok(value) => {
				if pausable {
					self.pause.on_success();
				}
				let _ = task.reply.send(Ok(value));
			}
			Err(CallError::Rejected { code, message }) => {
				if pausable && code == ResolveErrorCode::RateLimited {
					self.pause.on_rate_limited();
				}
				let _ = task.reply.send(Err(ResolveError::Worker { code, message }));
			}
			Err(CallError::Timeout) => {
				tracing::warn!(slot = self.index, "要求がタイムアウトしました");
				if retries_after_timeout(&task.request) {
					self.retry_elsewhere(task, ResolveError::Timeout);
				} else {
					let _ = task.reply.send(Err(ResolveError::Timeout));
				}
				return Some(Served::Failed);
			}
			Err(CallError::Died) => {
				tracing::warn!(slot = self.index, "処理中にワーカーが死にました");
				self.retry_elsewhere(task, ResolveError::WorkerDied);
				return Some(Served::Failed);
			}
			Err(CallError::Protocol(message)) => {
				tracing::warn!(slot = self.index, %message, "ワーカーの応答が不正です");
				let _ = task.reply.send(Err(ResolveError::Worker {
					code: ResolveErrorCode::Internal,
					message,
				}));
				return Some(Served::Failed);
			}
		}
		self.backoff.reset();
		self.notify(Event::Ready(self.index));
		None
	}

	/// 死亡かタイムアウトで終わった要求を、1 回だけ別のワーカーへ回す。
	///
	/// - 再試行を使い切っていれば `error` を返す
	/// - 再試行は待ち行列の先頭へ戻し、この台を避ける。別の台が空いていなければ、再起動したこの台で行う
	/// - 再試行の回数は死亡とタイムアウトで共通に数える (1 回の要求につき 1 回)
	///
	/// # 引数
	/// - `task`: 失敗した要求
	/// - `error`: 再試行を使い切っていたときに返すエラー
	///
	/// # 例
	/// `self.retry_elsewhere(task, ResolveError::Timeout);`
	fn retry_elsewhere(&self, mut task: Task, error: ResolveError) {
		if task.retried {
			let _ = task.reply.send(Err(error));
		} else {
			task.retried = true;
			task.avoid = Some(self.index);
			self.notify(Event::Requeue(task));
		}
	}
}

/// 入れ替えたワーカーの確認の結果。
enum Verified {
	/// ping に答えた。
	Answered,
	/// 答えなかった。
	NoAnswer,
	/// 全体の終了で打ち切った。
	Stopped,
}

/// 依頼があれば入れ替えの結果を返す。
///
/// # 引数
/// - `request`: 依頼
/// - `outcome`: 結果
///
/// # 例
/// `answer(self.pending_replace.take(), ReplaceOutcome::Failed);`
fn answer(request: Option<ReplaceRequest>, outcome: ReplaceOutcome) {
	if let Some(request) = request {
		request.answer(outcome);
	}
}

/// ワーカーへ渡す 1 要求の持ち時間 (秒) を求める。
///
/// - `resolve_timeout_secs` から `REQUEST_BUDGET_MARGIN_SECS` を引く
/// - 設定の検証を経ない呼び出し (試験) で余裕より短い上限が来ても、1 秒は残す
///
/// # 引数
/// - `resolve_timeout_secs`: Rust 側が応答を待つ上限 (秒)
///
/// # 戻り値
/// 持ち時間 (秒)
///
/// # 例
/// `assert_eq!(request_budget_secs(30), 28);`
fn request_budget_secs(resolve_timeout_secs: u64) -> u64 {
	resolve_timeout_secs
		.saturating_sub(REQUEST_BUDGET_MARGIN_SECS)
		.max(1)
}

/// ワーカーのプール。
///
/// - 破棄すると `shutdown` と同じ終了を始める (完了は待たない)
pub struct WorkerPool {
	events: mpsc::UnboundedSender<Event>,
	current: Arc<ArcSwap<VersionedSpec>>,
	/// 次に使う起動方法の世代。
	next_generation: AtomicU64,
	worker_count: usize,
	/// 入れ替えを 1 本に絞る。await をまたいで持つが、他の処理はこのロックを取らず、
	/// 完了の通知は全体の終了でも必ず返るため止まらない。
	rolling: Mutex<()>,
	pause: Arc<PauseGate>,
}

impl WorkerPool {
	/// ワーカーの起動を始める。完了は待たない。
	///
	/// - Tokio のランタイムの中で呼ぶこと
	/// - 起動できないワーカー、`startup_timeout_secs` 以内に ping に答えないワーカーは、
	///   バックオフしながら起動し直す
	/// - 起動方法の引数の後ろに `--request-budget-secs` を足す。`spec.args` には含めないこと
	///
	/// # 引数
	/// - `spec`: ワーカーの起動方法
	/// - `config`: `[worker]` の設定
	/// - `job`: 全ワーカーを入れる Job
	///
	/// # 戻り値
	/// プール
	///
	/// # 例
	/// `let pool = WorkerPool::start(spec, &config.worker, job);`
	pub fn start(spec: WorkerSpec, config: &WorkerConfig, job: Arc<Job>) -> Self {
		let (events, receiver) = mpsc::unbounded_channel();
		let (stop_sender, stop_receiver) = watch::channel(false);
		let current = Arc::new(ArcSwap::from_pointee(VersionedSpec {
			spec,
			generation: 0,
		}));
		let worker_count = config.worker_count as usize;
		// 0 は interval が受け付けない。設定の検証を経ない呼び出しでも落とさない
		let ping_interval = Duration::from_secs(config.ping_interval_secs.max(1));
		let startup_timeout = Duration::from_secs(config.startup_timeout_secs);
		let request_budget_secs = request_budget_secs(config.resolve_timeout_secs);
		let pause = Arc::new(PauseGate::new(config));

		let mut slots = Vec::with_capacity(worker_count);
		for index in 0..worker_count {
			let (commands, command_receiver) = mpsc::unbounded_channel();
			slots.push(SlotEntry {
				state: SlotState::Starting,
				commands,
				pending_replace: None,
			});
			let slot = Slot {
				index,
				events: events.clone(),
				commands: command_receiver,
				stop_signal: stop_receiver.clone(),
				current: current.clone(),
				job: job.clone(),
				timeout: Duration::from_secs(config.resolve_timeout_secs),
				startup_timeout,
				request_budget_secs,
				ping_interval,
				backoff: Backoff::new(),
				pending_replace: None,
				generation: None,
				pause: pause.clone(),
			};
			tokio::spawn(slot.run());
		}
		let manager = Manager {
			slots,
			interactive: VecDeque::new(),
			prefetch: VecDeque::new(),
			reserved: config.prefetch_reserved_workers as usize,
			stopping: false,
			stop_signal: stop_sender,
			shutdown_waiters: Vec::new(),
			pause: pause.clone(),
		};
		tokio::spawn(manager.run(receiver));
		Self {
			events,
			current,
			next_generation: AtomicU64::new(1),
			worker_count,
			rolling: Mutex::new(()),
			pause,
		}
	}

	/// 回数制限で一時停止中なら、再開の時刻を返す。
	///
	/// # 戻り値
	/// 再開の時刻。停止していなければ `None`
	///
	/// # 例
	/// `if let Some(retry_at) = pool.paused_until() { tokio::time::sleep_until(retry_at).await; }`
	pub fn paused_until(&self) -> Option<Instant> {
		self.pause.paused_until()
	}

	/// 回数制限で一時停止中か。
	///
	/// # 戻り値
	/// 停止中なら真
	///
	/// # 例
	/// `if pool.is_paused() { /* 更新を見送る */ }`
	pub fn is_paused(&self) -> bool {
		self.paused_until().is_some()
	}

	/// ワーカーを 1 台ずつ入れ替える。実行中の要求は失敗させない。
	///
	/// - 1 台ごとに新規の割り当てを止め、処理の完了を待ち、標準入力を閉じて起動し直す
	/// - 新しいワーカーが ping に答えてから次の台へ進む
	/// - 入れ替えの途中で死亡やタイムアウトにより再起動し、すでに新しい起動方法で動いている台は入れ替えない
	/// - 失敗すると残りの台を入れ替えず、起動方法を元に戻し、新しい起動方法で動いている台を
	///   同じ手順で元に戻す (新しい環境を消してよいかを呼び出し元が判断できるようにするため)。
	///   全台が元に戻れば `Failed { not_restored: [] }` を返し、新しい環境は消してよい
	/// - 成功した後は全台が新しい環境で動くため、古い環境を消してよい
	/// - 途中で future を破棄すると元へ戻らない。完了まで待つこと
	///
	/// # 引数
	/// - `spec`: 切り替え先の起動方法。`None` なら同じ環境で再起動する
	///
	/// # エラー
	/// - 起動か ping に失敗した: `RollingRestartError::Failed`
	/// - 終了中: `RollingRestartError::Stopped`
	///
	/// # 例
	/// `pool.rolling_restart(Some(new_spec)).await?;`
	pub async fn rolling_restart(
		&self,
		spec: Option<WorkerSpec>,
	) -> Result<(), RollingRestartError> {
		let _rolling = self.rolling.lock().await;
		let previous = self.current.load_full();
		let target = self.next_generation.fetch_add(1, Ordering::Relaxed);
		let next = VersionedSpec {
			spec: spec.unwrap_or_else(|| previous.spec.clone()),
			generation: target,
		};
		self.current.store(Arc::new(next));
		for slot in 0..self.worker_count {
			match self.replace_slot(slot, target).await {
				ReplaceOutcome::Done => {}
				ReplaceOutcome::Stopped => {
					self.current.store(previous);
					return Err(RollingRestartError::Stopped);
				}
				ReplaceOutcome::Failed => {
					tracing::error!(slot, "入れ替えに失敗したため元の環境へ戻します");
					let keep = previous.generation;
					self.current.store(previous);
					return Err(self.roll_back(slot, keep).await);
				}
			}
		}
		Ok(())
	}

	/// 元の世代で動いていない台を、元の起動方法へ入れ替える。
	///
	/// - 入れ替え済みの台に加え、入れ替え中に再起動して新しい起動方法になった台も戻す
	/// - 元の世代で動いている台は再起動しない
	///
	/// # 引数
	/// - `failed`: 失敗した台の番号
	/// - `keep`: 元の起動方法の世代
	///
	/// # 戻り値
	/// 呼び出し元へ返すエラー
	///
	/// # 例
	/// `return Err(self.roll_back(slot, previous.generation).await);`
	async fn roll_back(&self, failed: usize, keep: u64) -> RollingRestartError {
		let mut not_restored = Vec::new();
		for slot in 0..self.worker_count {
			match self.replace_slot(slot, keep).await {
				ReplaceOutcome::Done => {}
				ReplaceOutcome::Failed => not_restored.push(slot),
				ReplaceOutcome::Stopped => return RollingRestartError::Stopped,
			}
		}
		if !not_restored.is_empty() {
			tracing::error!(?not_restored, "元の環境へ戻せない台があります");
		}
		RollingRestartError::Failed {
			slot: failed,
			not_restored,
		}
	}

	/// 1 台の入れ替えを依頼して完了を待つ。
	///
	/// # 引数
	/// - `slot`: ワーカーの番号
	/// - `target`: 目的の世代。この世代で動いていれば入れ替えずに `Done` になる
	///
	/// # 戻り値
	/// 結果。管理タスクが終わっていれば `Stopped`
	///
	/// # 例
	/// `let outcome = self.replace_slot(0, target).await;`
	async fn replace_slot(&self, slot: usize, target: u64) -> ReplaceOutcome {
		let (ack, done) = oneshot::channel();
		let request = ReplaceRequest { ack, target };
		if self.events.send(Event::Replace { slot, request }).is_err() {
			return ReplaceOutcome::Stopped;
		}
		done.await.unwrap_or(ReplaceOutcome::Stopped)
	}

	/// 全体を終了する。全ワーカーのプロセスが回収されてから戻る。
	///
	/// - 待ち行列と実行中の要求は `Unavailable` で終わる
	/// - 待機中のワーカーは標準入力を閉じて終了を待ち、残れば強制終了する
	/// - 何度呼んでもよい。上限は `EXIT_GRACE` と強制終了の待ちの合計
	///
	/// # 例
	/// `pool.shutdown().await;`
	pub async fn shutdown(&self) {
		let (waiter, done) = oneshot::channel();
		if self.events.send(Event::Shutdown(Some(waiter))).is_ok() {
			// 管理タスクが先に終わると送信側ごと破棄されるが、その時点で終了済み
			let _ = done.await;
		}
	}
}

#[async_trait]
impl WorkerClient for WorkerPool {
	async fn submit(&self, request: WorkerRequest, priority: Priority) -> Reply {
		// 一時停止中は管理タスクへ送らない。ping は止めない
		if let Some(retry_at) = paused_for(&self.pause, &request) {
			return Err(ResolveError::Paused { retry_at });
		}
		let (reply, answer) = oneshot::channel();
		let task = Task {
			request,
			priority,
			reply,
			retried: false,
			avoid: None,
		};
		self.events
			.send(Event::Submit(task))
			.map_err(|_| ResolveError::Unavailable)?;
		// 送信側の破棄は、実行タスクの消失か全体の終了を意味する
		answer.await.unwrap_or(Err(ResolveError::Unavailable))
	}
}

impl Drop for WorkerPool {
	/// 終了を始める。完了は待てないため、回収は実行タスクに任せる。
	fn drop(&mut self) {
		let _ = self.events.send(Event::Shutdown(None));
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn request_budget_leaves_the_margin() {
		assert_eq!(request_budget_secs(30), 30 - REQUEST_BUDGET_MARGIN_SECS);
		assert_eq!(request_budget_secs(1), 1);
	}

	#[test]
	fn only_flat_playlist_skips_the_retry_after_timeout() {
		let flat = WorkerRequest::FlatPlaylist {
			url: "https://www.youtube.com/playlist?list=x".to_owned(),
			limit: 200,
		};
		assert!(!retries_after_timeout(&flat));
		let resolve = WorkerRequest::Resolve {
			url: "https://www.youtube.com/watch?v=x".to_owned(),
			format_id: None,
		};
		assert!(retries_after_timeout(&resolve));
		let search = WorkerRequest::Search {
			query: "q".to_owned(),
			limit: 1,
		};
		assert!(retries_after_timeout(&search));
		assert!(retries_after_timeout(&WorkerRequest::Ping));
	}

	#[test]
	fn backoff_doubles_up_to_the_limit_and_resets() {
		let mut backoff = Backoff::new();
		let delays: Vec<u64> = (0..7).map(|_| backoff.advance().as_secs()).collect();
		assert_eq!(delays, vec![1, 2, 4, 8, 16, 30, 30]);
		backoff.reset();
		assert_eq!(backoff.advance(), Duration::from_secs(1));
	}
}
