//! GuildActor の失敗時の手順と、音声接続の監視と回復。
//! - `actor.rs` の子のモジュール。アクターの状態を直接扱う
//! - 解決の失敗: エラーコードごとに飛ばす、すぐやり直す、待ってからやり直す (`recovery::TrackAttempts`)。
//!   待ちはアクターの `select!` のタイマー (`retry_at`) で、アクターを止めない
//! - 曲に固有でない理由 (取得の不調) の飛ばしが `playback.skip_streak_limit` 回続いたら、飛ばさずに再生を止める。
//!   今の曲をキューの先頭へ戻し、キューを残して退出し、1 回だけ案内する (取得できない間にキューを使い切らないため)
//! - `rate_limited` で解決のやり直しを使い切ったときも、同じ手順で飛ばさずに止める (案内の文言だけが違う)。
//!   プールの一時停止 (`Paused`) は回数を使わずに待つ
//! - 曲の失敗 (`TrackFailed`): 1 回目は同じ形式で取り直して今の位置から、2 回目は PipeInput で先頭から、3 回目は飛ばす
//! - 健全性: 再生中に `health.check_interval_secs` ごとに標本を取り、`HealthJudge` で判定する。
//!   猶予は今の曲の開始 (`TrackStarted`) を受けた時刻から数える (`?move` の後は移動した時刻から数え直す)
//! - 回復: Recovering に入り、位置を保存し、切断して同じボイスチャンネルへ接続し直し、保存した位置から再生する。
//!   切断と再接続は 1 つの別タスクで順に行い、アクターは待たない
//!   再接続に失敗したら (自動の回復と `?rejoin` のどちらも)、今の曲をキューの先頭へ戻し、キューを残して退出する。
//!   自動の回復 (送信の異常、`Disconnected(Lost)`、音声の呼び出しの時間切れ) は回数を制限し、
//!   超えたら一時停止して `?rejoin` を案内する。`?rejoin` は制限を受けない
//! - `Disconnected(Kicked)` は `?leave` と同じくセッションを終える
//! - 呼び出し: アクターの `handle`、`run` のタイマー、`on_connected`、`on_resolved` から呼ばれる

use std::time::Duration;

use tokio::time::Instant;

use super::{GuildActor, Next, RetryWait};
use crate::audio::backend::{AudioError, DisconnectKind, TrackId, VoiceSession};
use crate::config::Config;
use crate::domain::{ChannelId, ResolveError};
use crate::guild::health::{HealthJudge, Verdict};
use crate::guild::notices::{keys, plain, titled_text, with_args};
use crate::guild::recovery::{FailureStep, RejoinLimiter, ResolveAction, RetryPolicy, SkipReason};
use crate::guild::state::PlayerState;

/// 回復を始めるきっかけ。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Trigger {
	/// 自動の回復。回数の制限を受ける。値はログに出す原因。
	Auto(&'static str),
	/// `?rejoin`。回数の制限を受けない。値は応答先。
	Manual(ChannelId),
}

/// 自動の回復の原因 (ログ用)。
const CAUSE_TIMEOUT: &str = "音声の呼び出しの時間切れ";
const CAUSE_LOST: &str = "音声の接続の切断";
const CAUSE_UNHEALTHY: &str = "音声の送信の異常";

/// 再生を止めた理由 (ログ用)。
const HALT_FETCH_FAILURES: &str = "曲の取得の失敗が続いた";
const HALT_RATE_LIMITED: &str = "YouTube の利用制限で解決のやり直しを使い切った";

/// 失敗と健全性の監視の状態。アクターが 1 つ持つ。
pub(super) struct Supervision {
	/// 健全性の判定。接続、再接続、曲の開始、`?move` のたびに数え直す。
	pub(super) judge: HealthJudge,
	/// 自動の再接続の回数の制限。アクターの寿命の間 (セッションをまたいで) 数える。
	pub(super) limiter: RejoinLimiter,
	/// 解決のやり直しの方針。
	pub(super) policy: RetryPolicy,
	/// 健全性の確認の間隔。
	pub(super) check_interval: Duration,
	/// 今の曲の開始 (`AudioEvent::TrackStarted`) を受けた時刻。健全性の猶予の起点。
	/// None の間 (接続の直後、曲を始めてから開始が届くまで) は健全性を判定しない。
	/// 入力の準備 (広告の秒数の待ちを含む) の間は送信が無く、送信なしと取り違えるため
	pub(super) started_at: Option<Instant>,
	/// 次に健全性を確かめる時刻。接続している間だけ持つ。
	pub(super) health_at: Option<Instant>,
	/// `rate_limited` の後に解決をやり直す時刻と、そのときの曲の世代。
	pub(super) retry_at: Option<(Instant, u64)>,
	/// `?rejoin` の応答先。手動の回復の間だけ持つ。
	pub(super) rejoin_reply: Option<ChannelId>,
	/// 音声の呼び出しが時間切れになり、セッションが壊れたとみなしているか。ループの先頭で回復を始める合図。
	pub(super) timed_out: bool,
	/// 自動の回復の上限で回復を諦めたセッションを、応答しないものとして扱っているか。
	/// 立っている間は位置を音声に問い合わせず見積もりを使う。音声の呼び出しが成功するか、接続し直すと消す
	pub(super) broken: bool,
	/// 曲に固有でない理由で曲を飛ばした連続の回数。曲の再生が始まるか、セッションが終わると 0 に戻す。
	pub(super) skip_streak: u32,
	/// `skip_streak` がこの回数に達したら再生を止める (`playback.skip_streak_limit`)。
	pub(super) skip_streak_limit: u32,
}

impl Supervision {
	/// 設定から作る。
	///
	/// # 引数
	/// - `config`: 設定 (`[health]`、`[worker]`、`[playback]` を使う)
	///
	/// # 戻り値
	/// 何も記録していない状態
	///
	/// # 呼び出し例
	/// ```ignore
	/// let sup = Supervision::new(&deps.config);
	/// ```
	pub(super) fn new(config: &Config) -> Self {
		Self {
			judge: HealthJudge::new(&config.health),
			limiter: RejoinLimiter::new(&config.health),
			policy: RetryPolicy::from_config(&config.worker),
			check_interval: Duration::from_secs(config.health.check_interval_secs),
			started_at: None,
			health_at: None,
			retry_at: None,
			rejoin_reply: None,
			timed_out: false,
			broken: false,
			skip_streak: 0,
			skip_streak_limit: config.playback.skip_streak_limit,
		}
	}

	/// セッションの終わり (Idle に戻るとき) に、セッションに属する記録を捨てる。再接続の制限の記録は残す。
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.sup.end_session();
	/// ```
	pub(super) fn end_session(&mut self) {
		self.judge.reset();
		self.started_at = None;
		self.health_at = None;
		self.retry_at = None;
		self.rejoin_reply = None;
		self.timed_out = false;
		self.broken = false;
		self.skip_streak = 0;
	}
}

/// 解決を諦めた理由の通知のキー。
///
/// # 引数
/// - `reason`: 理由
///
/// # 戻り値
/// 曲名を差し込む通知のキー
///
/// # 呼び出し例
/// ```ignore
/// self.skip_current(skip_notice(SkipReason::Private));
/// ```
fn skip_notice(reason: SkipReason) -> &'static str {
	match reason {
		SkipReason::NotFound => keys::VIDEO_NOT_FOUND,
		SkipReason::Private => keys::VIDEO_PRIVATE,
		SkipReason::GeoBlocked => keys::VIDEO_GEO_BLOCKED,
		SkipReason::AgeRestricted => keys::VIDEO_AGE_RESTRICTED,
		SkipReason::UnsupportedUrl | SkipReason::Other => keys::TRACK_SKIPPED,
	}
}

impl GuildActor {
	/// 前の処理で音声の呼び出しが時間切れになっていたら、回復を始める (時間切れは切断として扱う)。
	///
	/// - 回復を始められなかった (未接続、回復中、上限) 場合も記録は消し、繰り返さない
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.recover_if_timed_out().await; // run のループの先頭
	/// ```
	pub(super) async fn recover_if_timed_out(&mut self) {
		if !self.sup.timed_out {
			return;
		}
		self.begin_recovery(Trigger::Auto(CAUSE_TIMEOUT)).await;
		self.sup.timed_out = false;
	}

	/// 新しい音声セッションを持つ。健全性の監視を最初から始める。
	///
	/// # 引数
	/// - `voice`: 接続済みのセッション
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.attach_voice(voice);
	/// ```
	pub(super) fn attach_voice(&mut self, voice: Box<dyn VoiceSession>) {
		let now = Instant::now();
		self.voice = Some(voice);
		self.sup.timed_out = false;
		self.sup.broken = false;
		self.sup.judge.reset();
		self.sup.started_at = None;
		self.sup.health_at = Some(now + self.sup.check_interval);
	}

	/// 解決の失敗を受け、エラーコードごとにやり直すか飛ばすかを決める。
	///
	/// - `internal` はすぐにやり直す。`rate_limited` は待ってからやり直し、最初の待ちで待機中を通知する。
	///   プールの一時停止 (`Paused`) は回数を使わずに `retry_at` まで待つ (通知は `rate_limited` と共通で 1 回)。
	///   タイムアウト、ワーカーの停止や不在は、通知せずに待ってからやり直す
	/// - やり直しの上限、またはやり直さない失敗なら、理由を添えて曲を飛ばす。
	///   曲に固有でない理由の飛ばしが続いていれば、飛ばさずに再生を止める (`skip_or_halt`)
	/// - `rate_limited` で上限に達したら、飛ばさずに再生を止めて `error.rate_limited_stopped` を 1 回送る。
	///   `skip_streak` は数えない
	/// - `age_restricted` は運用者向けに Cookie の確認を促すログも出す
	///
	/// # 引数
	/// - `error`: 今の曲の解決のエラー
	///
	/// # 戻り値
	/// やり直す、または再生を止めたなら `Next::Stay`。曲を飛ばしたなら `Next::Advance`
	///
	/// # 呼び出し例
	/// ```ignore
	/// let next = self.on_resolve_failed(&error).await; self.advance_if(next).await;
	/// ```
	pub(super) async fn on_resolve_failed(&mut self, error: &ResolveError) -> Next {
		let policy = self.sup.policy;
		let Some(current) = self.current.as_mut() else {
			return Next::Advance;
		};
		let action = current
			.attempts
			.on_resolve_error(error, &policy, Instant::now());
		let video_id = current.item.video_id.clone();
		let title = current.item.title.clone();
		let generation = current.generation;
		tracing::warn!(guild = self.guild.get(), video_id = %video_id.0, %error, ?action, "曲を解決できません");
		match action {
			ResolveAction::RetryNow => {
				self.spawn_resolve(video_id);
				Next::Stay
			}
			ResolveAction::RetryAfter { delay, notify } => {
				if let (true, Some(channels)) = (notify, self.channels) {
					self.notify(channels.text, titled_text(keys::RATE_LIMITED, &title));
				}
				self.sup.retry_at = Some((Instant::now() + delay, generation));
				Next::Stay
			}
			ResolveAction::Skip(reason) => {
				if reason == SkipReason::AgeRestricted {
					tracing::warn!(
						guild = self.guild.get(),
						video_id = %video_id.0,
						"年齢制限の動画を再生できません。worker.cookies_browser の Cookie を確認してください"
					);
				}
				self.skip_or_halt(skip_notice(reason), reason.is_track_specific())
					.await
			}
			ResolveAction::Halt => {
				self.halt_keeping_queue(keys::RATE_LIMITED_STOPPED, HALT_RATE_LIMITED)
					.await;
				Next::Stay
			}
		}
	}

	/// 今の曲を飛ばす。曲に固有でない理由の飛ばしは数え、続いていれば飛ばさずに再生を止める。
	///
	/// - `track_specific` が true の飛ばしは数えない (連続も途切れさせない)
	/// - 数えた回数が `playback.skip_streak_limit` に達したら、今の曲は飛ばさない。キューの先頭へ戻し、
	///   キューを残して退出し、`?p` での再開を 1 回だけ案内する (`halt_keeping_queue`)
	/// - 連続の回数は曲の再生が始まると 0 に戻す (`on_started`)
	/// - 曲に固有でない理由で飛ばした曲は、終えた曲と同じく記録する (`record_finished`。
	///   再生を始めていれば履歴へ、ループ中はキューの末尾へ戻す)。曲が悪いとは限らないため、ループから消さない。
	///   曲に固有の理由 (非公開、削除済み、ライブ配信など) で飛ばした曲は記録せず、ループからも消す
	///
	/// # 引数
	/// - `key`: 飛ばしたときの通知のキー (曲名を差し込む)
	/// - `track_specific`: 曲そのものに原因がある飛ばしか
	///
	/// # 戻り値
	/// 曲を飛ばしたら `Next::Advance` (呼び出し側は次の曲へ進む)。再生を止めたら `Next::Stay`
	///
	/// # 呼び出し例
	/// ```ignore
	/// let next = self.skip_or_halt(keys::TRACK_SKIPPED, false).await; self.advance_if(next).await;
	/// ```
	pub(super) async fn skip_or_halt(&mut self, key: &'static str, track_specific: bool) -> Next {
		if !track_specific {
			self.sup.skip_streak = self.sup.skip_streak.saturating_add(1);
			if self.sup.skip_streak >= self.sup.skip_streak_limit {
				self.halt_keeping_queue(keys::FETCH_STOPPED, HALT_FETCH_FAILURES)
					.await;
				return Next::Stay;
			}
			let finished = self.current.take();
			self.skip_current(key, finished.as_ref());
			if let Some(finished) = finished {
				self.record_finished(finished).await;
			}
			return Next::Advance;
		}
		let current = self.current.take();
		self.skip_current(key, current.as_ref());
		Next::Advance
	}

	/// 今の曲を飛ばさずに再生を止める (取得の不調が続いた、`rate_limited` で回数を使い切った)。
	///
	/// - 今の曲をキューの先頭へ戻し、`playback_state` を消し、キューを残して退出する (`leave_keeping_queue`)
	/// - 案内は 1 回だけ送る (`{prefix}` を差し込む)。書き込みに失敗したら案内の代わりに `error.save_failed` を送る
	/// - `skip_streak` は変えない (退出でセッションの記録ごと 0 に戻る)
	///
	/// # 引数
	/// - `key`: 案内のキー (`{prefix}` を差し込む)
	/// - `reason`: ログに出す止めた理由
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.halt_keeping_queue(keys::FETCH_STOPPED, HALT_FETCH_FAILURES).await;
	/// ```
	async fn halt_keeping_queue(&mut self, key: &'static str, reason: &'static str) {
		tracing::warn!(
			guild = self.guild.get(),
			streak = self.sup.skip_streak,
			reason,
			"再生を止めてキューを残します"
		);
		let channels = self.channels;
		let saved = self.leave_keeping_queue().await;
		let Some(channels) = channels else {
			return;
		};
		let notice = self.resume_notice(key, &saved);
		self.notify(channels.text, notice);
	}

	/// `rate_limited` の待ちが明けた。今の曲が同じで、まだ再生を始めていなければ解決をやり直す。
	///
	/// # 呼び出し例
	/// ```ignore
	/// () = wait_deadline(retry) => self.on_retry_due(),
	/// ```
	pub(super) fn on_retry_due(&mut self) {
		let Some((_, generation)) = self.sup.retry_at.take() else {
			return;
		};
		let video_id = self
			.current
			.as_ref()
			.filter(|current| current.generation == generation && current.is_preparing())
			.map(|current| current.item.video_id.clone());
		match video_id {
			Some(video_id) => self.spawn_resolve(video_id),
			None => tracing::debug!(
				guild = self.guild.get(),
				generation,
				"曲が変わったため解決のやり直しを取り消します"
			),
		}
	}

	/// 曲の失敗を受ける。
	///
	/// - 今の曲でなければ無視する
	/// - 1 回目: 今の位置 (音声から得られなければ見積もり) を覚え、同じ形式で取り直して再開する
	/// - 2 回目: PipeInput で先頭から再生し直し、その旨を通知する
	/// - 3 回目: 曲を飛ばして通知し、次の曲へ進む。曲に固有でない飛ばしとして数え、
	///   続いていれば飛ばさずに再生を止める (`skip_or_halt`)
	///
	/// # 引数
	/// - `track`: 失敗した曲
	/// - `error`: バックエンドのエラー
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_track_failed(track, error).await;
	/// ```
	pub(super) async fn on_track_failed(&mut self, track: TrackId, error: AudioError) {
		let Some(current) = self
			.current
			.as_mut()
			.filter(|current| current.track == Some(track))
		else {
			tracing::debug!(
				guild = self.guild.get(),
				track = track.0,
				"今の曲ではない失敗を無視します"
			);
			return;
		};
		let step = current.attempts.on_track_failed();
		let title = current.item.title.clone();
		tracing::warn!(guild = self.guild.get(), %error, ?step, "再生中の曲が失敗しました");
		match step {
			FailureStep::Reresolve => {
				let position = self.current_position().await.unwrap_or_default();
				self.restart_current(position, false);
			}
			FailureStep::Pipe => {
				if let Some(channels) = self.channels {
					self.notify(channels.text, titled_text(keys::PIPE_FALLBACK, &title));
				}
				self.restart_current(Duration::ZERO, true);
			}
			FailureStep::Skip => {
				let next = self.skip_or_halt(keys::TRACK_SKIPPED, false).await;
				self.advance_if(next).await;
			}
		}
	}

	/// 今の曲を、取り直してから指定の位置で再生し直す (曲の失敗の後)。
	///
	/// - 一時停止中だった曲は、再生を始めてすぐ一時停止に戻す。前の作り直しの一時停止の予定も残す
	///   (作り直した曲が始まる前に再び失敗しても、一時停止に戻すため)
	///
	/// # 引数
	/// - `start_at`: 再生を始める位置
	/// - `use_pipe`: PipeInput で再生するか
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.restart_current(position, false);
	/// ```
	fn restart_current(&mut self, start_at: Duration, use_pipe: bool) {
		// 再生を始めた曲の作り直しなので、解決のやり直しの待ちは無い。残っていても前の世代の待ちとして捨てる
		let generation = self.advance_generation(RetryWait::Discard);
		let paused = self.state == PlayerState::Paused;
		let Some(current) = self.current.as_mut() else {
			return;
		};
		current.restart(generation, start_at, use_pipe, true);
		// 一時停止中の曲は、作り直した後も一時停止に戻す (予定を上書きせずに足す)
		current.pause_on_start |= paused;
		let video_id = current.item.video_id.clone();
		self.state = PlayerState::Preparing;
		self.spawn_resolve(video_id);
	}

	/// 今の曲の再生位置を得る。
	///
	/// - 再生を始める前なら、次に始める位置
	/// - 自動の回復を諦めたセッション (`Supervision::broken`) には問い合わせず、経過時間からの見積もりを返す
	///   (応答しないセッションの呼び出しで `voice.call_timeout_secs` まで待たないため)
	/// - 音声から得られなければ (失敗、時間切れ、壊れたセッション)、経過時間からの見積もり
	///
	/// # 戻り値
	/// 位置。今の曲が無ければ None
	///
	/// # 呼び出し例
	/// ```ignore
	/// let position = self.current_position().await;
	/// ```
	pub(super) async fn current_position(&mut self) -> Option<Duration> {
		let current = self.current.as_ref()?;
		if current.is_preparing() {
			return Some(current.start_at);
		}
		let estimate = current.clock.position(Instant::now());
		if self.sup.broken {
			return Some(estimate);
		}
		match self.call_voice(|voice| voice.position()).await {
			Ok(position) => Some(position),
			Err(error) => {
				tracing::warn!(guild = self.guild.get(), %error, ?estimate, "再生位置を得られません。見積もりを使います");
				Some(estimate)
			}
		}
	}

	/// 音声接続の切断を受ける。
	///
	/// - `Lost`: 自動の回復を始める
	/// - `Kicked`: `?leave` と同じくキューと `playback_state` を消して退出し、通知する。
	///   Idle と回復中 (切断した古いセッションのイベント) は無視する
	///
	/// # 引数
	/// - `reason`: 切断の種類
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_disconnected(reason).await;
	/// ```
	pub(super) async fn on_disconnected(&mut self, reason: DisconnectKind) {
		match reason {
			DisconnectKind::Lost => self.begin_recovery(Trigger::Auto(CAUSE_LOST)).await,
			DisconnectKind::Kicked => {
				let Some(channels) = self
					.channels
					.filter(|_| !matches!(self.state, PlayerState::Idle | PlayerState::Recovering))
				else {
					tracing::debug!(guild = self.guild.get(), state = ?self.state, "切断のイベントを無視します");
					return;
				};
				tracing::info!(
					guild = self.guild.get(),
					"ボイスチャンネルから切断されたため退出します"
				);
				let key = match self.leave_and_clear().await {
					Ok(()) => keys::KICKED,
					Err(_) => keys::SAVE_FAILED,
				};
				self.notify(channels.text, plain(key));
			}
		}
	}

	/// 健全性の確認の時刻になった。次の時刻を決めてから確かめる。
	///
	/// # 呼び出し例
	/// ```ignore
	/// () = wait_deadline(health) => self.on_health_due().await,
	/// ```
	pub(super) async fn on_health_due(&mut self) {
		self.sup.health_at = self
			.voice
			.is_some()
			.then(|| Instant::now() + self.sup.check_interval);
		self.check_health().await;
	}

	/// 健全性を 1 回確かめる。異常なら自動の回復を始める。
	///
	/// - 再生中 (Playing) だけ標本を取る。それ以外では基準と回数を捨てる (一時停止や曲の切替の間は送信が無いため)
	/// - 猶予は今の曲の開始 (`TrackStarted`) を受けた時刻から数える。まだ受けていなければ判定せず、
	///   基準と回数を捨てる (入力の準備中は送信が無いため)
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.check_health().await;
	/// ```
	pub(super) async fn check_health(&mut self) {
		let sample = match (self.state, self.voice.as_ref()) {
			(PlayerState::Playing, Some(voice)) => voice.health(),
			_ => {
				self.sup.judge.reset();
				return;
			}
		};
		let Some(started_at) = self.sup.started_at else {
			self.sup.judge.reset();
			return;
		};
		let since = Instant::now().saturating_duration_since(started_at);
		// 送信の状態を追うための記録 (毎秒出るため debug)。無音の原因を調べるときに使う
		tracing::debug!(
			guild = self.guild.get(),
			listeners = self.listeners,
			dave_version = ?sample.dave_version,
			encrypted = sample.encrypted_frames,
			plaintext = sample.plaintext_frames,
			withheld = sample.withheld_frames,
			"音声の送信の標本"
		);
		if let Verdict::Unhealthy(kind) = self.sup.judge.observe(&sample, since, self.listeners) {
			tracing::warn!(
				guild = self.guild.get(),
				?kind,
				?sample,
				"音声の送信の異常を検知しました"
			);
			self.sup.judge.reset();
			self.begin_recovery(Trigger::Auto(CAUSE_UNHEALTHY)).await;
		}
	}

	/// 回復の手順を始める。
	///
	/// - 回復中なら何もしない (`?rejoin` には回復中を通知する)。再接続を二重に始めない
	/// - 接続していなければ何もしない (`?rejoin` には未接続を通知する)
	/// - 自動の回復は回数の制限を受け、超えたら一時停止して `?rejoin` を案内する
	/// - Recovering に入り、今の位置を保存し、切断と同じボイスチャンネルへの接続を 1 つの別タスクで順に行う
	///   (アクターは切断を待たない)。接続の結果は `Connected` で届き、`on_rejoined` か `on_rejoin_failed` へ進む
	/// - 自動の回復を諦めたセッション (`Supervision::broken`) からは、位置を問い合わせずに見積もりを使う
	/// - 今の曲は世代を進めて未開始に戻す (解決中の結果や古いセッションのイベントを捨てる)
	/// - 位置は一時停止中かどうか (一時停止に戻す予定を含む) と一緒に保存する。
	///   自動の回復では一時停止に戻す予定を残し (上書きせずに足す)、一時停止中だった曲は再接続の後も一時停止に戻す。
	///   `?rejoin` は一時停止中でも続きから再生する。再生を始めたら一時停止でない状態を保存し直す
	/// - `rate_limited` の待ちは捨てない。今の曲の世代に合わせて残し、再接続の後に残りを待つ
	///
	/// # 引数
	/// - `trigger`: きっかけ
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.begin_recovery(Trigger::Manual(reply)).await;
	/// ```
	pub(super) async fn begin_recovery(&mut self, trigger: Trigger) {
		let manual = match trigger {
			Trigger::Manual(reply) => Some(reply),
			Trigger::Auto(_) => None,
		};
		if self.state == PlayerState::Recovering {
			match manual {
				Some(reply) => self.notify(reply, plain(keys::RECOVERING)),
				None => tracing::debug!(guild = self.guild.get(), "回復中のため回復を重ねません"),
			}
			return;
		}
		let Some(channels) = self.channels.filter(|_| self.voice.is_some()) else {
			match manual {
				Some(reply) => self.notify(reply, plain(keys::NOT_CONNECTED)),
				None => tracing::debug!(guild = self.guild.get(), "接続していないため回復しません"),
			}
			return;
		};
		if let Trigger::Auto(cause) = trigger {
			if !self.sup.limiter.try_acquire(Instant::now()) {
				self.suspend_for_rejoin(channels.text, cause).await;
				return;
			}
			tracing::warn!(guild = self.guild.get(), cause, "音声の接続を回復します");
		} else {
			tracing::info!(guild = self.guild.get(), "?rejoin で音声の接続を回復します");
		}
		let position = self.current_position().await;
		let paused = self.persisted_paused();
		self.state = PlayerState::Recovering;
		self.sup.rejoin_reply = manual;
		self.sup.health_at = None;
		self.sup.started_at = None;
		self.slow = None;
		if let Some(position) = position {
			self.persist_position(position, paused).await;
			// 解決のやり直しの待ちは、世代を進めた今の曲の待ちとして残す
			let generation = self.advance_generation(RetryWait::Carry);
			if let Some(current) = self.current.as_mut() {
				current.restart(generation, position, false, false);
				if manual.is_some() {
					current.pause_on_start = false;
				} else {
					current.pause_on_start |= paused;
				}
			}
		}
		// 準備済みの次の曲は切断で消えるため、記録だけ捨てる (再接続の後に準備し直す)
		let _ = self.prefetch.take_stale_preload(None);
		self.sup.timed_out = false;
		let previous = self.voice.take();
		self.spawn_connect(channels.voice, previous);
	}

	/// 自動の回復の上限に達した。再生中なら一時停止し、`?rejoin` を案内する。
	///
	/// - 一時停止の呼び出しに失敗しても Paused にする (健全性の確認を止め、案内を繰り返さない)
	/// - 一時停止にしたら、位置 (見積もり) と一時停止の状態を保存する
	/// - セッションを応答しないものとして記録する (`Supervision::broken`)。`?rejoin` で位置を問い合わせて待たないため
	///
	/// # 引数
	/// - `text`: 案内の送り先
	/// - `cause`: 回復のきっかけ (ログ用)
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.suspend_for_rejoin(channels.text, cause).await;
	/// ```
	async fn suspend_for_rejoin(&mut self, text: ChannelId, cause: &'static str) {
		tracing::warn!(
			guild = self.guild.get(),
			cause,
			"自動の回復の上限に達したため一時停止します"
		);
		if self.state == PlayerState::Playing {
			if let Err(error) = self.call_voice(|voice| voice.pause()).await {
				tracing::warn!(guild = self.guild.get(), %error, "一時停止できません");
			}
			self.state = PlayerState::Paused;
			let now = Instant::now();
			let estimate = self.current.as_mut().map(|current| {
				current.clock.halt(now);
				current.clock.position(now)
			});
			// 壊れたセッションへ位置を問い合わせず、見積もりを保存する
			if let Some(position) = estimate {
				self.persist_position(position, true).await;
			}
		}
		// 壊れたセッションのまま、利用者の ?rejoin を待つ。時間切れの記録で回復を繰り返さない
		self.sup.timed_out = false;
		self.sup.broken = true;
		let prefix = self.deps.config.bot.prefix.clone();
		self.notify(
			text,
			with_args(keys::REJOIN_LIMIT, [(keys::ARG_PREFIX, prefix)]),
		);
	}

	/// 回復の再接続ができた。今の曲を保存した位置から再生し直す (今の曲が無ければ次の曲へ)。
	///
	/// - `?rejoin` なら再接続を通知する。自動の回復は通知しない
	/// - `rate_limited` の待ちが残っていれば、解決はその時刻まで待つ (`on_retry_due` が始める)
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_rejoined().await;
	/// ```
	pub(super) async fn on_rejoined(&mut self) {
		tracing::info!(guild = self.guild.get(), "ボイスチャンネルに再接続しました");
		self.state = PlayerState::Preparing;
		if let Some(reply) = self.sup.rejoin_reply.take() {
			self.notify(reply, plain(keys::REJOINED));
		}
		let video_id = self
			.current
			.as_ref()
			.map(|current| current.item.video_id.clone());
		match video_id {
			Some(_) if self.sup.retry_at.is_some() => tracing::debug!(
				guild = self.guild.get(),
				"解決のやり直しの待ちが残っているため、時刻まで待ちます"
			),
			Some(video_id) => self.spawn_resolve(video_id),
			None => self.start_next().await,
		}
	}

	/// 一時停止中だった曲を作り直して (または一時停止の状態で復元して) 再生を始めた直後に、一時停止に戻す。
	///
	/// - 成功したら Paused にし、位置の見積もりを止め、一時停止に戻す予定を消す。通知はしない
	/// - 失敗したら再生中のまま (ログのみ)。予定は残し、次の作り直しや回復の後に改めて一時停止に戻す。
	///   時間切れなら次のループで回復に入る
	///
	/// # 呼び出し例
	/// ```ignore
	/// if pause { self.pause_after_restart().await; }
	/// ```
	pub(super) async fn pause_after_restart(&mut self) {
		match self.call_voice(|voice| voice.pause()).await {
			Ok(()) => self.mark_paused_after_restart(),
			Err(error) => {
				tracing::warn!(guild = self.guild.get(), %error, "作り直した曲を一時停止に戻せません");
			}
		}
	}

	/// 一時停止に戻した (または一時停止のまま載せた) 曲を Paused として記録する。音声は呼ばない。
	///
	/// - Paused にし、位置の見積もりを止め、一時停止に戻す予定を消す。通知はしない
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.mark_paused_after_restart();
	/// ```
	pub(super) fn mark_paused_after_restart(&mut self) {
		self.state = PlayerState::Paused;
		if let Some(current) = self.current.as_mut() {
			current.clock.halt(Instant::now());
			current.pause_on_start = false;
		}
	}

	/// 回復の再接続に失敗した (自動の回復と `?rejoin` のどちらも)。キューを残して退出し、`?p` での再開を案内する。
	///
	/// - 今の曲をキューの先頭へ戻し、`playback_state` を消す (`leave_keeping_queue`)。保存した位置は使わない
	/// - 案内は `?rejoin` の応答先、無ければセッションのテキストチャンネルへ送る。
	///   書き込みに失敗したら案内の代わりに `error.save_failed` を送る
	///
	/// # 引数
	/// - `error`: 接続のエラー
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_rejoin_failed(error).await;
	/// ```
	pub(super) async fn on_rejoin_failed(&mut self, error: AudioError) {
		tracing::warn!(guild = self.guild.get(), %error, "ボイスチャンネルに再接続できません");
		let reply = self
			.sup
			.rejoin_reply
			.take()
			.or(self.channels.map(|channels| channels.text));
		let saved = self.leave_keeping_queue().await;
		let notice = self.resume_notice(keys::REJOIN_FAILED, &saved);
		if let Some(reply) = reply {
			self.notify(reply, notice);
		}
	}
}
