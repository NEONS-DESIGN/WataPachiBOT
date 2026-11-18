//! 失敗時の手順の記録と判断。I/O を持たない。
//! - `TrackAttempts`: 今の曲の失敗の回数。曲の失敗の手順 (取り直し → PipeInput → 飛ばす) と、解決のやり直しを決める
//! - `RetryPolicy`: 解決のやり直しの回数と待ち時間 (`worker.max_retries`、`worker.rate_limit_backoff_*`)
//! - `RejoinLimiter`: 自動の再接続の回数の制限 (直近 `health.auto_rejoin_window_secs` に `health.auto_rejoin_max` 回)
//! - `PositionClock`: 音声から位置を得られないときに使う、経過時間からの再生位置の見積もり
//! - 時刻は呼び出し側が渡す (tokio の仮想時間の試験でも同じに動く)
//! - 呼び出し: アクター (`supervise.rs`) が今の曲 (`CurrentTrack`) とアクター自身に持たせて使う

use std::collections::VecDeque;
use std::time::Duration;

use tokio::time::Instant;

use crate::config::{HealthConfig, WorkerConfig};
use crate::domain::{ResolveError, ResolveErrorCode};

/// 曲の失敗 (`AudioEvent::TrackFailed`) の後にとる手順。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailureStep {
	/// 1 回目: 同じ形式で取り直し、今の位置から再開する。
	Reresolve,
	/// 2 回目: PipeInput で先頭から再生し、利用者に通知する。
	Pipe,
	/// 3 回目以降: 曲を飛ばして通知する。
	Skip,
}

/// 解決を諦めて曲を飛ばす理由。通知の文面を選ぶのに使う。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipReason {
	NotFound,
	Private,
	GeoBlocked,
	AgeRestricted,
	/// YouTube 以外の URL など、ワーカーが受け付けない曲。
	UnsupportedUrl,
	/// やり直しの上限に達した (理由を特定しない)。取得の不調 (ワーカーの停止、通信の障害) を含む。
	Other,
}

impl SkipReason {
	/// 曲そのものに原因がある飛ばしか。
	///
	/// - `not_found` `private` `geo_blocked` `age_restricted` `unsupported_url` は、その曲を何度取っても同じ結果になる
	/// - `Other` は取得の不調でも起きる。続くときは曲を飛ばし続けず再生を止める (アクターの `skip_streak`)
	///
	/// # 戻り値
	/// 曲に固有の理由なら true
	///
	/// # 呼び出し例
	/// ```ignore
	/// let counted = !reason.is_track_specific();
	/// ```
	pub fn is_track_specific(self) -> bool {
		!matches!(self, SkipReason::Other)
	}
}

/// 解決の失敗の後にとる動き。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResolveAction {
	/// すぐにやり直す (`internal`)。
	RetryNow,
	/// 待ってからやり直す (`rate_limited`、プールの一時停止)。`notify` が true なら待機中を知らせる (1 回の待ちにつき 1 回)。
	RetryAfter { delay: Duration, notify: bool },
	/// 曲を飛ばす。
	Skip(SkipReason),
	/// rate_limited で回数を使い切った。飛ばさずに再生を止める
	Halt,
}

/// 解決のやり直しの方針。設定から作る。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RetryPolicy {
	/// やり直しの上限回数 (`worker.max_retries`)。最初の解決は含まない。
	pub max_retries: u32,
	/// `rate_limited` の最初の待ち時間。
	pub backoff_base: Duration,
	/// `rate_limited` の待ち時間の上限。
	pub backoff_max: Duration,
}

impl RetryPolicy {
	/// `[worker]` の設定から作る。
	///
	/// # 引数
	/// - `config`: `[worker]` の設定
	///
	/// # 戻り値
	/// やり直しの方針
	///
	/// # 呼び出し例
	/// ```ignore
	/// let policy = RetryPolicy::from_config(&config.worker);
	/// ```
	pub fn from_config(config: &WorkerConfig) -> Self {
		Self {
			max_retries: config.max_retries,
			backoff_base: Duration::from_secs(config.rate_limit_backoff_secs),
			backoff_max: Duration::from_secs(config.rate_limit_backoff_max_secs),
		}
	}

	/// n 回目のやり直しまでの待ち時間。最初の待ち時間から倍々に増やし、上限で止める。
	///
	/// # 引数
	/// - `retry`: 何回目のやり直しか (1 始まり。0 は 1 として扱う)
	///
	/// # 戻り値
	/// 待ち時間
	///
	/// # 呼び出し例
	/// ```ignore
	/// let delay = policy.backoff(2); // 既定値なら 10 秒
	/// ```
	pub fn backoff(&self, retry: u32) -> Duration {
		let doublings = retry.saturating_sub(1);
		// 2 の累乗が u32 を超える回数では、どのみち上限に張り付く
		let factor = 2u32.checked_pow(doublings).unwrap_or(u32::MAX);
		self.backoff_base
			.checked_mul(factor)
			.unwrap_or(self.backoff_max)
			.min(self.backoff_max)
	}
}

/// 今の曲の失敗の記録。今の曲を差し替えると作り直す (曲ごとに数える)。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TrackAttempts {
	/// 再生中の失敗 (`TrackFailed`) の回数。
	failures: u32,
	/// 解決のやり直しの回数。解決に成功したら 0 に戻す。
	resolve_retries: u32,
	/// 今の待ち (`rate_limited`、プールの一時停止) について、待機中を知らせたか。解決に成功したら戻す。
	rate_limit_notified: bool,
}

impl TrackAttempts {
	/// 曲の失敗を 1 回数え、次の手順を返す。
	///
	/// # 戻り値
	/// 1 回目は `Reresolve`、2 回目は `Pipe`、3 回目以降は `Skip`
	///
	/// # 呼び出し例
	/// ```ignore
	/// match current.attempts.on_track_failed() { FailureStep::Reresolve => ..., ... }
	/// ```
	pub fn on_track_failed(&mut self) -> FailureStep {
		self.failures = self.failures.saturating_add(1);
		match self.failures {
			1 => FailureStep::Reresolve,
			2 => FailureStep::Pipe,
			_ => FailureStep::Skip,
		}
	}

	/// 解決の失敗を受け、次の動きを返す。
	///
	/// - `not_found` `private` `geo_blocked` `age_restricted`: やり直さずに飛ばす (理由付き)
	/// - `internal`: `max_retries` 回まですぐにやり直し、その後は飛ばす
	/// - `rate_limited`: `max_retries` 回まで倍々の待ち時間の後にやり直し、その後は飛ばさずに止める (`Halt`)。
	///   待機中の通知は、成功するまでに 1 回だけ求める
	/// - `Paused` (プールの一時停止): 回数を増やさず、`retry_at` まで待ってからやり直す。
	///   待機中の通知は `rate_limited` と同じ印を使い、成功するまでに 1 回だけ求める
	/// - タイムアウト、ワーカーの停止、ワーカーの不在: 一時的な失敗として、`max_retries` 回まで
	///   `rate_limited` と同じ倍々の待ち時間の後にやり直し、その後は飛ばす。待機中の通知は求めない
	///   (プールは起動できるワーカーが無いとすぐ `Unavailable` を返すため、すぐにやり直すとキューを使い切る)
	/// - `unsupported_url`: やり直さずに飛ばす
	/// - やり直しの回数は `internal`、`rate_limited`、一時的な失敗で共通に数える。
	///   回数を使い切ったときの失敗が `rate_limited` なら止め、それ以外なら飛ばす
	///
	/// # 引数
	/// - `error`: 解決のエラー
	/// - `policy`: やり直しの方針
	/// - `now`: 今の時刻 (`Paused` の残り時間を求める)
	///
	/// # 戻り値
	/// 次の動き
	///
	/// # 呼び出し例
	/// ```ignore
	/// match current.attempts.on_resolve_error(&error, &policy, Instant::now()) { ResolveAction::RetryNow => ..., ... }
	/// ```
	pub fn on_resolve_error(
		&mut self,
		error: &ResolveError,
		policy: &RetryPolicy,
		now: Instant,
	) -> ResolveAction {
		let code = match error {
			ResolveError::Worker { code, .. } => *code,
			ResolveError::Paused { retry_at } => {
				return ResolveAction::RetryAfter {
					delay: retry_at.saturating_duration_since(now),
					notify: self.take_rate_limit_notice(),
				};
			}
			ResolveError::Timeout | ResolveError::WorkerDied | ResolveError::Unavailable => {
				if self.resolve_retries >= policy.max_retries {
					return ResolveAction::Skip(SkipReason::Other);
				}
				self.resolve_retries += 1;
				return ResolveAction::RetryAfter {
					delay: policy.backoff(self.resolve_retries),
					notify: false,
				};
			}
		};
		let retryable = matches!(
			code,
			ResolveErrorCode::Internal | ResolveErrorCode::RateLimited
		);
		if retryable && self.resolve_retries >= policy.max_retries {
			return match code {
				ResolveErrorCode::RateLimited => ResolveAction::Halt,
				_ => ResolveAction::Skip(SkipReason::Other),
			};
		}
		match code {
			ResolveErrorCode::NotFound => ResolveAction::Skip(SkipReason::NotFound),
			ResolveErrorCode::Private => ResolveAction::Skip(SkipReason::Private),
			ResolveErrorCode::GeoBlocked => ResolveAction::Skip(SkipReason::GeoBlocked),
			ResolveErrorCode::AgeRestricted => ResolveAction::Skip(SkipReason::AgeRestricted),
			ResolveErrorCode::UnsupportedUrl => ResolveAction::Skip(SkipReason::UnsupportedUrl),
			ResolveErrorCode::Internal => {
				self.resolve_retries += 1;
				ResolveAction::RetryNow
			}
			ResolveErrorCode::RateLimited => {
				self.resolve_retries += 1;
				ResolveAction::RetryAfter {
					delay: policy.backoff(self.resolve_retries),
					notify: self.take_rate_limit_notice(),
				}
			}
		}
	}

	/// 待機中の通知を求めるかを決め、求めたら印を立てる (`rate_limited` と `Paused` で共有する)。
	///
	/// # 戻り値
	/// 成功してからまだ知らせていなければ true
	///
	/// # 呼び出し例
	/// ```ignore
	/// let notify = self.take_rate_limit_notice();
	/// ```
	fn take_rate_limit_notice(&mut self) -> bool {
		let notify = !self.rate_limit_notified;
		self.rate_limit_notified = true;
		notify
	}

	/// 解決の成功を記録する。やり直しの回数と待機中の通知の記録を戻す (曲の失敗の回数は残す)。
	///
	/// # 呼び出し例
	/// ```ignore
	/// current.attempts.on_resolved();
	/// ```
	pub fn on_resolved(&mut self) {
		self.resolve_retries = 0;
		self.rate_limit_notified = false;
	}
}

/// 自動の再接続の回数の制限。直近の期間 (スライドする窓) の回数を数える。
#[derive(Debug, Clone)]
pub(crate) struct RejoinLimiter {
	max: u32,
	window: Duration,
	/// 期間内の再接続の時刻 (古い順)。
	recent: VecDeque<Instant>,
}

impl RejoinLimiter {
	/// `[health]` の設定から作る。
	///
	/// # 引数
	/// - `config`: `[health]` の設定 (`auto_rejoin_max`、`auto_rejoin_window_secs`)
	///
	/// # 戻り値
	/// 記録の無い制限
	///
	/// # 呼び出し例
	/// ```ignore
	/// let limiter = RejoinLimiter::new(&config.health);
	/// ```
	pub fn new(config: &HealthConfig) -> Self {
		Self {
			max: config.auto_rejoin_max,
			window: Duration::from_secs(config.auto_rejoin_window_secs),
			recent: VecDeque::new(),
		}
	}

	/// 自動の再接続をしてよいか確かめ、してよければ記録する。
	///
	/// - 期間を過ぎた記録は捨てる (時刻 `t` の記録は `t + window` から数えない)
	/// - `auto_rejoin_max` が 0 なら常に断る
	///
	/// # 引数
	/// - `now`: 今の時刻
	///
	/// # 戻り値
	/// 期間内の回数が上限未満なら記録して true。上限に達していれば false
	///
	/// # 呼び出し例
	/// ```ignore
	/// if !self.limiter.try_acquire(Instant::now()) { suggest_rejoin(); }
	/// ```
	pub fn try_acquire(&mut self, now: Instant) -> bool {
		while let Some(oldest) = self.recent.front() {
			if now.saturating_duration_since(*oldest) >= self.window {
				self.recent.pop_front();
			} else {
				break;
			}
		}
		if self.recent.len() >= self.max as usize {
			return false;
		}
		self.recent.push_back(now);
		true
	}
}

/// 再生位置の見積もり。再生を始めた位置と、再生していた時間から求める。
///
/// - 音声から位置を得られないとき (呼び出しの失敗、時間切れ) の代わりに使う
/// - 一時停止の間は進めない
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct PositionClock {
	/// 最後に止めた (または始めた) ときの位置。
	base: Duration,
	/// 再生中ならその起点の時刻。止まっていれば None。
	running_since: Option<Instant>,
}

impl PositionClock {
	/// 止まった状態で位置を決める (再生の前、再開の位置を決めたとき)。
	///
	/// # 引数
	/// - `position`: 位置
	///
	/// # 呼び出し例
	/// ```ignore
	/// clock.set(saved);
	/// ```
	pub fn set(&mut self, position: Duration) {
		self.base = position;
		self.running_since = None;
	}

	/// 今の位置から進め始める (再生の開始、再開)。進めている途中なら何もしない。
	///
	/// # 引数
	/// - `now`: 今の時刻
	///
	/// # 呼び出し例
	/// ```ignore
	/// clock.run(Instant::now());
	/// ```
	pub fn run(&mut self, now: Instant) {
		if self.running_since.is_none() {
			self.running_since = Some(now);
		}
	}

	/// 進めるのを止める (一時停止)。止まっていれば何もしない。
	///
	/// # 引数
	/// - `now`: 今の時刻
	///
	/// # 呼び出し例
	/// ```ignore
	/// clock.halt(Instant::now());
	/// ```
	pub fn halt(&mut self, now: Instant) {
		if let Some(since) = self.running_since.take() {
			self.base += now.saturating_duration_since(since);
		}
	}

	/// 今の位置の見積もり。
	///
	/// # 引数
	/// - `now`: 今の時刻
	///
	/// # 戻り値
	/// 位置
	///
	/// # 呼び出し例
	/// ```ignore
	/// let position = clock.position(Instant::now());
	/// ```
	pub fn position(&self, now: Instant) -> Duration {
		match self.running_since {
			Some(since) => self.base + now.saturating_duration_since(since),
			None => self.base,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn policy() -> RetryPolicy {
		RetryPolicy {
			max_retries: 3,
			backoff_base: Duration::from_secs(5),
			backoff_max: Duration::from_secs(60),
		}
	}

	fn worker(code: ResolveErrorCode) -> ResolveError {
		ResolveError::Worker {
			code,
			message: String::new(),
		}
	}

	#[test]
	fn backoff_doubles_and_is_capped() {
		let p = policy();
		let secs: Vec<u64> = (1..=6).map(|n| p.backoff(n).as_secs()).collect();
		assert_eq!(secs, vec![5, 10, 20, 40, 60, 60]);
		assert_eq!(p.backoff(0), Duration::from_secs(5));
		assert_eq!(p.backoff(u32::MAX), Duration::from_secs(60));
	}

	#[test]
	fn failure_steps_follow_the_order() {
		let mut a = TrackAttempts::default();
		assert_eq!(a.on_track_failed(), FailureStep::Reresolve);
		a.on_resolved();
		assert_eq!(a.on_track_failed(), FailureStep::Pipe);
		assert_eq!(a.on_track_failed(), FailureStep::Skip);
		assert_eq!(a.on_track_failed(), FailureStep::Skip);
	}

	#[test]
	fn resolve_errors_map_to_actions() {
		let p = policy();
		let now = Instant::now();
		let mut a = TrackAttempts::default();
		for (code, reason) in [
			(ResolveErrorCode::NotFound, SkipReason::NotFound),
			(ResolveErrorCode::Private, SkipReason::Private),
			(ResolveErrorCode::GeoBlocked, SkipReason::GeoBlocked),
			(ResolveErrorCode::AgeRestricted, SkipReason::AgeRestricted),
			(ResolveErrorCode::UnsupportedUrl, SkipReason::UnsupportedUrl),
		] {
			assert_eq!(
				a.on_resolve_error(&worker(code), &p, now),
				ResolveAction::Skip(reason)
			);
			assert!(reason.is_track_specific(), "{reason:?}");
		}
		assert!(!SkipReason::Other.is_track_specific());
		for _ in 0..p.max_retries {
			assert_eq!(
				a.on_resolve_error(&worker(ResolveErrorCode::Internal), &p, now),
				ResolveAction::RetryNow
			);
		}
		assert_eq!(
			a.on_resolve_error(&worker(ResolveErrorCode::Internal), &p, now),
			ResolveAction::Skip(SkipReason::Other)
		);
	}

	#[test]
	fn rate_limited_backs_off_notifies_once_then_halts() {
		let p = policy();
		let now = Instant::now();
		let mut a = TrackAttempts::default();
		let limited = worker(ResolveErrorCode::RateLimited);
		let actions: Vec<ResolveAction> = (0..4)
			.map(|_| a.on_resolve_error(&limited, &p, now))
			.collect();
		assert_eq!(
			actions,
			vec![
				ResolveAction::RetryAfter {
					delay: Duration::from_secs(5),
					notify: true
				},
				ResolveAction::RetryAfter {
					delay: Duration::from_secs(10),
					notify: false
				},
				ResolveAction::RetryAfter {
					delay: Duration::from_secs(20),
					notify: false
				},
				ResolveAction::Halt,
			]
		);
		// 成功の後は数え直し、待機中も改めて知らせる
		a.on_resolved();
		assert_eq!(
			a.on_resolve_error(&limited, &p, now),
			ResolveAction::RetryAfter {
				delay: Duration::from_secs(5),
				notify: true
			}
		);
	}

	#[test]
	fn transient_errors_back_off_without_notice_then_skip() {
		let p = policy();
		let now = Instant::now();
		for error in [
			ResolveError::Timeout,
			ResolveError::WorkerDied,
			ResolveError::Unavailable,
		] {
			let mut a = TrackAttempts::default();
			let actions: Vec<ResolveAction> = (0..4)
				.map(|_| a.on_resolve_error(&error, &p, now))
				.collect();
			let waits: Vec<ResolveAction> = [5, 10, 20]
				.into_iter()
				.map(|secs| ResolveAction::RetryAfter {
					delay: Duration::from_secs(secs),
					notify: false,
				})
				.chain([ResolveAction::Skip(SkipReason::Other)])
				.collect();
			assert_eq!(actions, waits, "{error:?}");
			// 成功の後は数え直す
			a.on_resolved();
			assert_eq!(
				a.on_resolve_error(&error, &p, now),
				ResolveAction::RetryAfter {
					delay: Duration::from_secs(5),
					notify: false
				}
			);
		}
	}

	fn limited() -> ResolveError {
		worker(ResolveErrorCode::RateLimited)
	}

	fn paused_for(now: Instant, secs: u64) -> ResolveError {
		ResolveError::Paused {
			retry_at: now + Duration::from_secs(secs),
		}
	}

	#[test]
	fn paused_waits_without_counting() {
		let p = policy();
		let now = Instant::now();
		let error = paused_for(now, 7);
		let mut a = TrackAttempts::default();
		for _ in 0..10 {
			assert!(matches!(
				a.on_resolve_error(&error, &p, now),
				ResolveAction::RetryAfter { delay, .. } if delay == Duration::from_secs(7)
			));
		}
		// 10 回待っても、続く rate_limited は 1 回目として扱う (Halt にならない)
		assert!(matches!(
			a.on_resolve_error(&limited(), &p, now),
			ResolveAction::RetryAfter { delay, .. } if delay == p.backoff(1)
		));
		// 期限を過ぎた Paused は待たずにやり直す
		assert_eq!(
			a.on_resolve_error(&paused_for(now, 0), &p, now + Duration::from_secs(1)),
			ResolveAction::RetryAfter {
				delay: Duration::ZERO,
				notify: false
			}
		);
	}

	#[test]
	fn paused_notifies_once() {
		let p = policy();
		let now = Instant::now();
		let mut a = TrackAttempts::default();
		let notifies: Vec<bool> = (0..3)
			.map(|_| match a.on_resolve_error(&paused_for(now, 5), &p, now) {
				ResolveAction::RetryAfter { notify, .. } => notify,
				other => panic!("{other:?}"),
			})
			.collect();
		assert_eq!(notifies, vec![true, false, false]);
		// rate_limited の通知と印を共有する
		assert_eq!(
			a.on_resolve_error(&limited(), &p, now),
			ResolveAction::RetryAfter {
				delay: p.backoff(1),
				notify: false
			}
		);
		let mut b = TrackAttempts::default();
		assert!(matches!(
			b.on_resolve_error(&limited(), &p, now),
			ResolveAction::RetryAfter { notify: true, .. }
		));
		assert!(matches!(
			b.on_resolve_error(&paused_for(now, 5), &p, now),
			ResolveAction::RetryAfter { notify: false, .. }
		));
		// 成功の後は改めて知らせる
		b.on_resolved();
		assert!(matches!(
			b.on_resolve_error(&paused_for(now, 5), &p, now),
			ResolveAction::RetryAfter { notify: true, .. }
		));
	}

	#[test]
	fn rate_limited_exhaustion_halts() {
		let p = policy();
		let now = Instant::now();
		let mut a = TrackAttempts::default();
		let actions: Vec<ResolveAction> = (0..=p.max_retries)
			.map(|_| a.on_resolve_error(&limited(), &p, now))
			.collect();
		let (last, waits) = actions.split_last().unwrap();
		assert!(waits
			.iter()
			.all(|action| matches!(action, ResolveAction::RetryAfter { .. })));
		assert_eq!(*last, ResolveAction::Halt);

		// 回数を使い切ったときの失敗が rate_limited 以外なら、今と同じく飛ばす
		let mut b = TrackAttempts::default();
		for _ in 0..p.max_retries {
			b.on_resolve_error(&limited(), &p, now);
		}
		assert_eq!(
			b.on_resolve_error(&ResolveError::Timeout, &p, now),
			ResolveAction::Skip(SkipReason::Other)
		);
	}

	#[test]
	fn internal_exhaustion_still_skips() {
		let p = policy();
		let now = Instant::now();
		let mut a = TrackAttempts::default();
		let internal = worker(ResolveErrorCode::Internal);
		let last = (0..=p.max_retries)
			.map(|_| a.on_resolve_error(&internal, &p, now))
			.last();
		assert_eq!(last, Some(ResolveAction::Skip(SkipReason::Other)));
	}

	#[test]
	fn rejoin_limiter_uses_a_sliding_window() {
		let config = HealthConfig {
			check_interval_secs: 1,
			dave_grace_secs: 5,
			dave_plaintext_tolerance_frames: 50,
			unhealthy_checks: 3,
			auto_rejoin_max: 3,
			auto_rejoin_window_secs: 600,
		};
		let mut limiter = RejoinLimiter::new(&config);
		let start = Instant::now();
		let at = |secs: u64| start + Duration::from_secs(secs);
		assert!(limiter.try_acquire(at(0)));
		assert!(limiter.try_acquire(at(100)));
		assert!(limiter.try_acquire(at(200)));
		assert!(!limiter.try_acquire(at(599)));
		// 最初の記録だけが期間を過ぎた
		assert!(limiter.try_acquire(at(600)));
		assert!(!limiter.try_acquire(at(699)));
		assert!(limiter.try_acquire(at(700)));

		let mut never = RejoinLimiter::new(&HealthConfig {
			auto_rejoin_max: 0,
			..config
		});
		assert!(!never.try_acquire(at(0)));
	}

	#[test]
	fn position_clock_stops_while_halted() {
		let start = Instant::now();
		let at = |secs: u64| start + Duration::from_secs(secs);
		let mut clock = PositionClock::default();
		clock.set(Duration::from_secs(30));
		assert_eq!(clock.position(at(10)), Duration::from_secs(30));
		clock.run(at(0));
		assert_eq!(clock.position(at(10)), Duration::from_secs(40));
		clock.halt(at(10));
		assert_eq!(clock.position(at(100)), Duration::from_secs(40));
		clock.run(at(100));
		clock.run(at(105));
		assert_eq!(clock.position(at(110)), Duration::from_secs(50));
	}
}
