//! 試験用の通知先。送信と編集を順に記録する。
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use watapachi_bot::domain::{ChannelId, MessageId};
use watapachi_bot::guild::notifier::{Card, MessageRef, Notice, Notifier};

/// 記録の 1 件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Logged {
	/// 新しく送った。`id` は払い出したメッセージ ID
	Sent { id: u64, key: &'static str },
	/// 編集した。`target` は編集先のメッセージ ID
	Edited { target: u64, key: &'static str },
}

/// 内部の記録。
#[derive(Default)]
struct Inner {
	/// 送信と、成功した編集の通知 (送られた順)
	sent: Vec<(ChannelId, Notice)>,
	log: Vec<Logged>,
	/// (メッセージ ID, 最後に書かれた通知)
	cards: Vec<(u64, Notice)>,
}

/// 通知を記録する偽の `Notifier`。`clone` したものは記録を共有する。
///
/// - `send` は 1 から順にメッセージ ID を払い出す
/// - `fail_edits(true)` の間、`edit` は記録せずに false を返す
///
/// 呼び出し方:
/// ```ignore
/// let notifier = FakeNotifier::default();
/// notifier.send(channel, notice).await;
/// assert_eq!(notifier.sent().len(), 1);
/// ```
#[derive(Clone, Default)]
pub struct FakeNotifier {
	inner: Arc<Mutex<Inner>>,
	next_id: Arc<AtomicU64>,
	fail_edits: Arc<AtomicBool>,
}

impl FakeNotifier {
	/// これまでに送られた通知 (編集で書き換えた内容を含む) を、送られた順に返す。
	pub fn sent(&self) -> Vec<(ChannelId, Notice)> {
		self.inner
			.lock()
			.map(|i| i.sent.clone())
			.unwrap_or_default()
	}

	/// 送信と編集の記録を順に返す。
	pub fn log(&self) -> Vec<Logged> {
		self.inner.lock().map(|i| i.log.clone()).unwrap_or_default()
	}

	/// 指定のメッセージに最後に書かれたカード。
	pub fn card_of(&self, id: u64) -> Option<Card> {
		let inner = self.inner.lock().ok()?;
		inner
			.cards
			.iter()
			.rev()
			.find(|(i, n)| *i == id && n.card.is_some())
			.and_then(|(_, n)| n.card.clone())
	}

	/// 編集を失敗させるか切り替える。
	pub fn fail_edits(&self, fail: bool) {
		self.fail_edits.store(fail, Ordering::SeqCst);
	}
}

#[async_trait]
impl Notifier for FakeNotifier {
	async fn send(&self, channel: ChannelId, notice: Notice) -> Option<MessageRef> {
		let id = self.next_id.fetch_add(1, Ordering::SeqCst) + 1;
		if let Ok(mut inner) = self.inner.lock() {
			inner.log.push(Logged::Sent {
				id,
				key: notice.key,
			});
			inner.cards.push((id, notice.clone()));
			inner.sent.push((channel, notice));
		}
		Some(MessageRef {
			channel,
			message: MessageId::new(id),
		})
	}

	async fn edit(&self, target: MessageRef, notice: Notice) -> bool {
		if self.fail_edits.load(Ordering::SeqCst) {
			return false;
		}
		if let Ok(mut inner) = self.inner.lock() {
			inner.log.push(Logged::Edited {
				target: target.message.get(),
				key: notice.key,
			});
			inner.cards.push((target.message.get(), notice.clone()));
			inner.sent.push((target.channel, notice));
		}
		true
	}
}
