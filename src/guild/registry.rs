//! アクターの登録簿。
//! - ギルドごとのアクターを使われた時点で作り、終了したものを消す
//! - 送信は `GuildHandle::try_send` で待たずに行う。メールボックスが満杯なら `GuildBusy` を返し、
//!   呼び出し側 (Dispatcher) を止めない
//! - 終了済みのアクターへ送ったときは、作り直して 1 回だけ送り直す
//! - 終了処理は `shutdown_all`。各アクターへ `Shutdown` を送り、呼び出し側が渡す期限まで待つ
//! - 呼び出し: `registry.get_or_spawn(guild).try_send(message)?;`

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::actor;
use super::message::GuildMessage;
use super::notifier::Notifier;
use crate::audio::access::VoiceAccess;
use crate::audio::backend::AudioBackend;
use crate::config::Config;
use crate::db::DbHandles;
use crate::domain::GuildId;
use crate::resolver::Resolver;

/// アクターが使う外部の部品。Clone して各アクターへ配る。
#[derive(Clone)]
pub struct ActorDeps {
	pub audio: Arc<dyn AudioBackend>,
	/// 参加の前の確認役 (権限と満員)
	pub access: Arc<dyn VoiceAccess>,
	pub resolver: Arc<dyn Resolver>,
	pub notifier: Arc<dyn Notifier>,
	pub db: DbHandles,
	pub config: Arc<Config>,
}

/// アクターのメールボックスが満杯で受け取れない。終了処理中も同じエラーを返す。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("サーバーの処理が混み合っています")]
pub struct GuildBusy;

/// `GuildRegistry::send_existing` が送れなかった理由。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SendError {
	/// アクターが動いていない (作らずに諦めた)。
	#[error("アクターが動いていません")]
	NotRunning,
	/// メールボックスが満杯。
	#[error("サーバーの処理が混み合っています")]
	Busy,
}

/// 1 ギルドのアクターへの送信口。Clone 可。
#[derive(Clone)]
pub struct GuildHandle {
	guild: GuildId,
	tx: mpsc::Sender<GuildMessage>,
	registry: Weak<RegistryInner>,
}

impl GuildHandle {
	/// メッセージを待たずに送る。
	///
	/// - アクターが終了していたら、登録簿から作り直して 1 回だけ送り直す
	///
	/// # 引数
	/// - `message`: 送るメッセージ
	///
	/// # 戻り値
	/// 受け付けたら `Ok(())`
	///
	/// # エラー
	/// - メールボックスが満杯のとき、または終了処理中のとき `GuildBusy`。メッセージは捨てる
	///
	/// # 呼び出し例
	/// ```ignore
	/// if handle.try_send(message).is_err() { /* 混雑中と返す */ }
	/// ```
	pub fn try_send(&self, message: GuildMessage) -> Result<(), GuildBusy> {
		match self.tx.try_send(message) {
			Ok(()) => Ok(()),
			Err(mpsc::error::TrySendError::Full(_)) => Err(GuildBusy),
			Err(mpsc::error::TrySendError::Closed(message)) => {
				let tx = self
					.registry
					.upgrade()
					.and_then(|registry| registry.sender(self.guild))
					.ok_or(GuildBusy)?;
				tx.try_send(message).map_err(|_| GuildBusy)
			}
		}
	}
}

/// ギルドごとのアクターの登録簿。
pub struct GuildRegistry {
	inner: Arc<RegistryInner>,
}

/// 登録簿の中身。アクターは弱参照で持ち、終了時に自分の登録を消す。
pub(crate) struct RegistryInner {
	deps: ActorDeps,
	actors: Mutex<HashMap<GuildId, Entry>>,
	/// アクターの通し番号。古いアクターが新しいアクターの登録を消さないように使う。
	next_id: AtomicU64,
	shutting_down: AtomicBool,
}

/// 登録の 1 件。
struct Entry {
	id: u64,
	tx: mpsc::Sender<GuildMessage>,
}

impl GuildRegistry {
	/// 空の登録簿を作る。
	///
	/// # 引数
	/// - `deps`: アクターへ配る部品
	///
	/// # 戻り値
	/// アクターを持たない登録簿
	///
	/// # 呼び出し例
	/// ```ignore
	/// let registry = GuildRegistry::new(deps);
	/// ```
	pub fn new(deps: ActorDeps) -> Self {
		Self {
			inner: Arc::new(RegistryInner {
				deps,
				actors: Mutex::new(HashMap::new()),
				next_id: AtomicU64::new(0),
				shutting_down: AtomicBool::new(false),
			}),
		}
	}

	/// ギルドのアクターへの送信口を返す。動いていなければ作る。
	///
	/// - tokio のランタイムの中で呼ぶ (アクターを `tokio::spawn` するため)
	/// - 終了処理の後は、送ると常に `GuildBusy` になる送信口を返す
	///
	/// # 引数
	/// - `guild`: 対象のギルド
	///
	/// # 戻り値
	/// 送信口
	///
	/// # 呼び出し例
	/// ```ignore
	/// registry.get_or_spawn(guild).try_send(message)?;
	/// ```
	pub fn get_or_spawn(&self, guild: GuildId) -> GuildHandle {
		let tx = self.inner.sender(guild).unwrap_or_else(|| {
			// 受信側をすぐ捨てて、閉じた送信口を作る
			let (tx, _rx) = mpsc::channel(1);
			tx
		});
		GuildHandle {
			guild,
			tx,
			registry: Arc::downgrade(&self.inner),
		}
	}

	/// 動いているアクターだけへメッセージを送る。無ければ作らない。
	///
	/// - 設定の変更の通知など、アクターが無ければ意味のないメッセージに使う
	///
	/// # 引数
	/// - `guild`: 対象のギルド
	/// - `message`: 送るメッセージ
	///
	/// # 戻り値
	/// 受け付けたら `Ok(())`
	///
	/// # エラー
	/// - アクターが無い、または終了済みなら `SendError::NotRunning`
	/// - メールボックスが満杯なら `SendError::Busy` (メッセージは捨てる)
	///
	/// # 呼び出し例
	/// ```ignore
	/// let _ = registry.send_existing(guild, GuildMessage::SettingsChanged);
	/// ```
	pub fn send_existing(&self, guild: GuildId, message: GuildMessage) -> Result<(), SendError> {
		let tx = {
			let actors = self.inner.lock();
			match actors.get(&guild) {
				Some(entry) if !entry.tx.is_closed() => entry.tx.clone(),
				_ => return Err(SendError::NotRunning),
			}
		};
		tx.try_send(message).map_err(|error| match error {
			mpsc::error::TrySendError::Full(_) => SendError::Busy,
			mpsc::error::TrySendError::Closed(_) => SendError::NotRunning,
		})
	}

	/// ギルドのアクターが動いているか。
	///
	/// # 引数
	/// - `guild`: 対象のギルド
	///
	/// # 戻り値
	/// 登録があり、メールボックスが開いていれば true
	///
	/// # 呼び出し例
	/// ```ignore
	/// if registry.contains(guild) { ... }
	/// ```
	pub fn contains(&self, guild: GuildId) -> bool {
		self.inner
			.lock()
			.get(&guild)
			.is_some_and(|entry| !entry.tx.is_closed())
	}

	/// すべてのアクターを終了させる。
	///
	/// - 以後は新しいアクターを作らない
	/// - 各アクターへ `Shutdown` を送り (アクターは再生位置を保存して切断する)、応答を待つ
	/// - `deadline` を過ぎたら、応答の無いギルドをログに残して諦めて戻る。
	///   メールボックスが満杯で送れないアクターや、処理の途中で止まったアクターがあっても、期限を超えて待たない
	/// - 独自の上限は持たない。終了の後の段 (DB) の時間を残すのは呼び出し側が期限で決める
	///
	/// # 引数
	/// - `deadline`: 待つ期限
	///
	/// # 呼び出し例
	/// ```ignore
	/// registry.shutdown_all(Instant::now() + Duration::from_secs(15)).await;
	/// ```
	pub async fn shutdown_all(&self, deadline: Instant) {
		self.inner.shutting_down.store(true, Ordering::SeqCst);
		let targets: Vec<(GuildId, mpsc::Sender<GuildMessage>)> = self
			.inner
			.lock()
			.iter()
			.map(|(guild, entry)| (*guild, entry.tx.clone()))
			.collect();
		let mut pending: HashSet<GuildId> = targets.iter().map(|(guild, _)| *guild).collect();
		let mut waits = JoinSet::new();
		for (guild, tx) in targets {
			waits.spawn(async move {
				let (done, finished) = oneshot::channel();
				if tx.send(GuildMessage::Shutdown { done }).await.is_ok() {
					// 送信側が捨てられても (アクターの異常終了) 待ちは解ける
					let _ = finished.await;
				}
				guild
			});
		}
		let all = async {
			while let Some(joined) = waits.join_next().await {
				if let Ok(guild) = joined {
					pending.remove(&guild);
				}
			}
		};
		if tokio::time::timeout_at(deadline, all).await.is_err() {
			let guilds: Vec<u64> = pending.iter().map(|guild| guild.get()).collect();
			tracing::warn!(
				?guilds,
				"終了の応答が無いアクターがあります。待たずに終了処理を続けます"
			);
		}
		// 待ちの処理は `waits` を捨てるときに止まる
		self.inner.lock().clear();
	}
}

impl RegistryInner {
	/// 登録の表をロックする。汚染されていても中身は壊れていないので使い続ける。
	///
	/// # 戻り値
	/// ロックのガード
	///
	/// # 呼び出し例
	/// ```ignore
	/// let actors = self.lock();
	/// ```
	fn lock(&self) -> MutexGuard<'_, HashMap<GuildId, Entry>> {
		self.actors.lock().unwrap_or_else(PoisonError::into_inner)
	}

	/// 動いているアクターの送信口を返す。無ければ作る。
	///
	/// # 引数
	/// - `guild`: 対象のギルド
	///
	/// # 戻り値
	/// 送信口。終了処理中なら None
	///
	/// # 呼び出し例
	/// ```ignore
	/// let tx = inner.sender(guild)?;
	/// ```
	fn sender(self: &Arc<Self>, guild: GuildId) -> Option<mpsc::Sender<GuildMessage>> {
		let mut actors = self.lock();
		if self.shutting_down.load(Ordering::SeqCst) {
			return None;
		}
		match actors.get(&guild) {
			Some(entry) if !entry.tx.is_closed() => Some(entry.tx.clone()),
			_ => Some(self.spawn_locked(&mut actors, guild)),
		}
	}

	/// アクターを作って登録する。表のロックを持ったまま呼ぶ。
	///
	/// # 引数
	/// - `actors`: ロック済みの表
	/// - `guild`: 対象のギルド
	///
	/// # 戻り値
	/// 新しいアクターの送信口
	///
	/// # 呼び出し例
	/// ```ignore
	/// let tx = self.spawn_locked(&mut actors, guild);
	/// ```
	fn spawn_locked(
		self: &Arc<Self>,
		actors: &mut HashMap<GuildId, Entry>,
		guild: GuildId,
	) -> mpsc::Sender<GuildMessage> {
		let id = self.next_id.fetch_add(1, Ordering::Relaxed);
		let tx = actor::spawn(guild, id, self.deps.clone(), Arc::downgrade(self));
		actors.insert(guild, Entry { id, tx: tx.clone() });
		tx
	}

	/// 終了するアクターが呼ぶ。登録を消し、受信口を閉じる。
	///
	/// - 閉じる前に届いていたメッセージは、終了処理中でなければ新しいアクターへ引き継ぐ
	/// - 閉じた後に送られたメッセージは、送信側が `try_send` の中で作り直したアクターへ送る
	///   (表のロックを持って閉じるため、引き継ぎより先に届くことはない)
	///
	/// # 引数
	/// - `guild`: 対象のギルド
	/// - `id`: 終了するアクターの通し番号
	/// - `rx`: 終了するアクターの受信口
	///
	/// # 呼び出し例
	/// ```ignore
	/// registry.retire(guild, id, &mut rx);
	/// ```
	pub(crate) fn retire(
		self: &Arc<Self>,
		guild: GuildId,
		id: u64,
		rx: &mut mpsc::Receiver<GuildMessage>,
	) {
		let mut actors = self.lock();
		if actors.get(&guild).is_some_and(|entry| entry.id == id) {
			actors.remove(&guild);
		}
		rx.close();
		let mut left = Vec::new();
		while let Ok(message) = rx.try_recv() {
			left.push(message);
		}
		if left.is_empty() || self.shutting_down.load(Ordering::SeqCst) {
			// 終了処理中は捨てる。`Shutdown` の応答口も捨てられ、待つ側はそれで解ける
			return;
		}
		let tx = match actors.get(&guild) {
			Some(entry) if !entry.tx.is_closed() => entry.tx.clone(),
			_ => self.spawn_locked(&mut actors, guild),
		};
		for message in left {
			if tx.try_send(message).is_err() {
				tracing::warn!(
					guild = guild.get(),
					"終了したアクターのメッセージを引き継げませんでした"
				);
			}
		}
	}
}
