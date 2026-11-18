//! ページ送りのボタンを灰色にする期限の記録。
//! - 旧 Bot (discord.py の `View(timeout=120)`) と同じく、最後の操作から `ui.pagination_timeout_secs` で灰色にする
//! - メッセージごとに期限と今のボタンを持つ。押されたら `touch` で期限を延ばし、ボタンの無いページにしたら `forget` で消す
//! - 期限のタスクは `touch` が新しい記録を作ったときだけ起動し、`due` が `Wait` を返す間は待ち直す
//! - 灰色にする編集の送信中に `touch` / `forget` が来たら、送信の後の `settle` が戻すボタンを返す
//!   (編集とボタンの押下の応答の順序は保証されないため、灰色の編集が後に届いても新しいボタンに戻す)
//! - 記録はメモリだけに持つ。再起動で消えた記録のボタンは、押されたときに発行時刻で判定する
//! - 呼び出し: `if timers.touch(target, components, deadline) { /* 期限のタスクを起動する */ }`

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use tokio::time::Instant;
use twilight_model::channel::message::component::Component;

use crate::domain::MessageId;
use crate::guild::notifier::MessageRef;

/// 記録の段階。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
	/// 期限を待っている
	Armed,
	/// 期限のタスクがボタンの編集を送っている
	Sending {
		/// 送信中に `touch` / `forget` が来て、送っているボタンが古くなった
		stale: bool,
		/// 送信の後に記録を残すか (`touch` なら残し、灰色にする編集か `forget` なら消す)
		keep: bool,
	},
}

/// 1 通のメッセージの記録。
#[derive(Debug)]
struct Entry {
	target: MessageRef,
	deadline: Instant,
	components: Vec<Component>,
	phase: Phase,
}

/// 期限の確認の結果。
#[derive(Debug)]
pub enum Due {
	/// まだ期限前。この時刻まで待ち直す
	Wait(Instant),
	/// 期限が来た。このボタンを灰色にして送り、送った後に `settle` を呼ぶ
	Expired {
		target: MessageRef,
		components: Vec<Component>,
	},
	/// 記録が無い (灰色にした後、`forget` の後、または作っていない)
	Gone,
}

/// 編集を送った後の扱い (`settle` の結果)。
#[derive(Debug, PartialEq)]
pub enum Settled {
	/// 送ったもので確定した。記録は消した。期限のタスクを終える
	Done,
	/// 送信中にボタンが変わった。このボタンを送り直し、送った後にもう一度 `settle` を呼ぶ
	Resend(Vec<Component>),
	/// 送ったボタンのまま、この時刻まで待ち直す
	Wait(Instant),
}

/// ページ送りのボタンの期限の記録。
///
/// - ロックは記録の読み書きの間だけ持ち、待つ間や Discord への送信の間は持たない
///
/// 呼び出し方:
/// ```ignore
/// let timers = PageTimers::new();
/// if timers.touch(target, components, Instant::now() + timeout) { spawn_expiry(target.message); }
/// // 期限のタスク: due が Expired なら送信し、settle が Resend の間は送り直す
/// ```
#[derive(Debug, Default)]
pub struct PageTimers {
	entries: Mutex<HashMap<MessageId, Entry>>,
}

impl PageTimers {
	/// 空の記録を作る。
	///
	/// # 呼び出し例
	/// ```ignore
	/// let timers = PageTimers::new();
	/// ```
	pub fn new() -> Self {
		Self::default()
	}

	/// 記録のロックを取る。前の持ち主が panic していても中身を使う (記録が壊れる処理は無いため)。
	fn lock(&self) -> MutexGuard<'_, HashMap<MessageId, Entry>> {
		self.entries.lock().unwrap_or_else(PoisonError::into_inner)
	}

	/// メッセージの期限と今のボタンを記録する。あれば差し替える (期限を延ばす)。
	///
	/// - 期限のタスクが編集を送っている間なら、送った後に `settle` がこのボタンを返す
	///
	/// # 引数
	/// - `target`: ボタンを付けたメッセージ
	/// - `components`: 今のボタン (期限に灰色にする元)
	/// - `deadline`: 灰色にする時刻
	///
	/// # 戻り値
	/// 新しい記録を作ったら true (呼び出し側が期限のタスクを起動する)。差し替えたら false
	///
	/// # 呼び出し例
	/// ```ignore
	/// let created = timers.touch(target, reply.components.clone(), Instant::now() + timeout);
	/// ```
	pub fn touch(&self, target: MessageRef, components: Vec<Component>, deadline: Instant) -> bool {
		let mut entries = self.lock();
		match entries.get_mut(&target.message) {
			Some(entry) => {
				entry.deadline = deadline;
				entry.components = components;
				if let Phase::Sending { .. } = entry.phase {
					entry.phase = Phase::Sending {
						stale: true,
						keep: true,
					};
				}
				false
			}
			None => {
				let entry = Entry {
					target,
					deadline,
					components,
					phase: Phase::Armed,
				};
				entries.insert(target.message, entry);
				true
			}
		}
	}

	/// メッセージの記録を消す (ボタンの無いページに書き換えたとき)。記録が無ければ何もしない。
	///
	/// - 期限のタスクが編集を送っている間なら、送った後に `settle` がボタンを外すよう返す
	///
	/// # 引数
	/// - `message`: メッセージ
	///
	/// # 呼び出し例
	/// ```ignore
	/// timers.forget(target.message);
	/// ```
	pub fn forget(&self, message: MessageId) {
		let mut entries = self.lock();
		let Some(entry) = entries.get_mut(&message) else {
			return;
		};
		match entry.phase {
			Phase::Armed => {
				entries.remove(&message);
			}
			Phase::Sending { .. } => {
				entry.components = Vec::new();
				entry.phase = Phase::Sending {
					stale: true,
					keep: false,
				};
			}
		}
	}

	/// 期限が来たかを確かめる。来ていれば送信中にして、灰色にする元のボタンを返す。
	///
	/// # 引数
	/// - `message`: メッセージ
	/// - `now`: 今の時刻
	///
	/// # 戻り値
	/// 待ち直す時刻、灰色にする元のボタン、または記録が無いこと
	///
	/// # 呼び出し例
	/// ```ignore
	/// match timers.due(message, Instant::now()) { Due::Wait(at) => sleep_until(at).await, .. }
	/// ```
	pub fn due(&self, message: MessageId, now: Instant) -> Due {
		let mut entries = self.lock();
		let Some(entry) = entries.get_mut(&message) else {
			return Due::Gone;
		};
		if entry.deadline > now {
			return Due::Wait(entry.deadline);
		}
		entry.phase = Phase::Sending {
			stale: false,
			keep: false,
		};
		Due::Expired {
			target: entry.target,
			components: entry.components.clone(),
		}
	}

	/// 期限のタスクが編集を送り終えた。送信中の変化を見て、次にすることを返す。
	///
	/// - 変化が無く、灰色にした (または `forget` の後に外した) なら記録を消して `Done`
	/// - 変化が無く、`touch` の後のボタンを送り直したなら、待ちに戻して `Wait`
	/// - 送信中に `touch` / `forget` が来たなら、送信中のまま今のボタンを `Resend` で返す
	///
	/// # 引数
	/// - `message`: メッセージ
	///
	/// # 戻り値
	/// 次にすること
	///
	/// # 呼び出し例
	/// ```ignore
	/// match timers.settle(target.message) { Settled::Resend(c) => { /* 送り直す */ } _ => {} }
	/// ```
	pub fn settle(&self, message: MessageId) -> Settled {
		let mut entries = self.lock();
		let Some(entry) = entries.get_mut(&message) else {
			return Settled::Done;
		};
		match entry.phase {
			Phase::Armed => Settled::Wait(entry.deadline),
			Phase::Sending { stale: true, keep } => {
				entry.phase = Phase::Sending { stale: false, keep };
				Settled::Resend(entry.components.clone())
			}
			Phase::Sending {
				stale: false,
				keep: true,
			} => {
				entry.phase = Phase::Armed;
				Settled::Wait(entry.deadline)
			}
			Phase::Sending {
				stale: false,
				keep: false,
			} => {
				entries.remove(&message);
				Settled::Done
			}
		}
	}
}
