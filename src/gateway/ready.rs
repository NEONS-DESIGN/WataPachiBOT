//! 起動の準備の門。`Ready` の後、全ギルドの `GuildCreate` が揃うか期限を過ぎたら 1 回だけ開く。
//! - 時刻は引数で受け取る純粋な状態機械。タイマーは持たない (期限の確認は呼び出し側が `poll` する)
//! - 開いた後の `Ready` (再接続) は何もしない。復元は起動の 1 回だけ行う
//! - Dispatcher は `Ready` を別タスクで門に渡し、`GuildCreate` はその場で門に渡すため、
//!   `Ready` の直後に届いた `GuildCreate` が `Ready` より先に門へ届きうる。
//!   `Ready` の前に届いたギルドは覚えておき、`Ready` の待つギルドから差し引く

use std::collections::HashSet;
use std::time::{Duration, Instant};

use crate::domain::GuildId;

/// 起動の準備の門。
///
/// 呼び出し方:
/// ```ignore
/// let mut gate = ReadyGate::new(Duration::from_secs(config.gateway.ready_wait_secs));
/// gate.on_ready(ready.guilds.iter().map(|g| g.id), Instant::now());
/// gate.on_guild_create(guild_id);
/// if gate.poll(Instant::now()) { /* 復元して受付を始める */ }
/// ```
#[derive(Debug)]
pub struct ReadyGate {
	/// `Ready` の後、全ギルドを待つ上限
	wait: Duration,
	/// まだ `GuildCreate` が来ていないギルド
	pending: HashSet<GuildId>,
	/// `Ready` より先に処理された `GuildCreate` のギルド。`Ready` を受けたら空にする
	early: HashSet<GuildId>,
	/// 待ちの期限。`Ready` を受けるまで None
	deadline: Option<Instant>,
	/// 一度開いたら true。以後は開かない
	opened: bool,
}

impl ReadyGate {
	/// 閉じた門を作る。
	///
	/// # 引数
	/// - `wait`: `Ready` の後、全ギルドを待つ上限 (`gateway.ready_wait_secs`)
	///
	/// # 呼び出し例
	/// ```ignore
	/// let gate = ReadyGate::new(Duration::from_secs(30));
	/// ```
	pub fn new(wait: Duration) -> Self {
		Self {
			wait,
			pending: HashSet::new(),
			early: HashSet::new(),
			deadline: None,
			opened: false,
		}
	}

	/// `Ready` を受けた。待つギルドと期限を決める。
	///
	/// - 2 回目以降 (再接続) は何もしない。待っている途中でも、待つギルドと期限を変えない
	/// - 先に処理された `GuildCreate` のギルドは待たない
	///
	/// # 引数
	/// - `guilds`: `Ready` に載っていたギルド
	/// - `now`: 受けた時刻
	///
	/// # 呼び出し例
	/// ```ignore
	/// gate.on_ready(ready.guilds.iter().map(|g| g.id), Instant::now());
	/// ```
	pub fn on_ready(&mut self, guilds: impl IntoIterator<Item = GuildId>, now: Instant) {
		if self.opened || self.deadline.is_some() {
			return;
		}
		let early = std::mem::take(&mut self.early);
		self.pending = guilds
			.into_iter()
			.filter(|guild| !early.contains(guild))
			.collect();
		self.deadline = Some(now + self.wait);
	}

	/// `GuildCreate` を受けた。待っているギルドから外す。
	///
	/// - `Ready` の前に処理されたものは覚えておき、`Ready` の待つギルドから差し引く
	/// - `Ready` の後の、待っていないギルド (新しく招かれた) は無視する
	///
	/// # 引数
	/// - `guild`: 届いたギルド (利用できないギルドも含む)
	///
	/// # 呼び出し例
	/// ```ignore
	/// gate.on_guild_create(guild.id());
	/// ```
	pub fn on_guild_create(&mut self, guild: GuildId) {
		if self.opened {
			return;
		}
		if self.deadline.is_none() {
			self.early.insert(guild);
			return;
		}
		self.pending.remove(&guild);
	}

	/// 開くかを確かめる。
	///
	/// # 引数
	/// - `now`: 今の時刻
	///
	/// # 戻り値
	/// `Ready` の後、全ギルドが揃ったか期限を過ぎたとき、最初の 1 回だけ true。
	/// `Ready` の前と、開いた後は false
	///
	/// # 呼び出し例
	/// ```ignore
	/// if gate.poll(Instant::now()) { restore().await; }
	/// ```
	pub fn poll(&mut self, now: Instant) -> bool {
		if self.opened {
			return false;
		}
		let Some(deadline) = self.deadline else {
			return false;
		};
		if self.pending.is_empty() || now >= deadline {
			self.opened = true;
			self.pending.clear();
			return true;
		}
		false
	}
}
