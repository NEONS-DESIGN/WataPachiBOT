//! 音声接続の健全性の判定。I/O を持たない。
//! - アクターが `health.check_interval_secs` ごとに `VoiceSession::health()` の標本を渡し、前回との差分で判定する
//! - しきい値はすべて `HealthConfig` から読む (コードに値を書かない)
//! - 猶予の起点 (`since_play`) は呼び出し側が決める。アクターは今の曲の開始 (`TrackStarted`) を受けた時刻から数え、
//!   `?move` で移動したときは移動した時刻から数え直す
//! - `dave_version` が None または 0 のときは DAVE が無効とみなし、未暗号化の確認を飛ばして送信なしだけを判定する。
//!   実機の試験で確かめるまでの暫定の扱い
//! - 聴者 (Bot 以外の参加者) が 0 人の間は、正常でも鍵が無く全フレームを送らない (Songbird が捨てて数える) ため、
//!   未暗号化と送らなかったの確認を飛ばして送信なしだけを判定する
//! - 呼び出し:
//!   ```ignore
//!   let mut judge = HealthJudge::new(&config.health);
//!   if let Verdict::Unhealthy(kind) = judge.observe(&voice.health(), since_started, listeners) { ... }
//!   judge.reset(); // 接続、再接続、曲の開始、移動のとき
//!   ```

use std::time::Duration;

use crate::audio::backend::VoiceHealth;
use crate::config::HealthConfig;

/// 1 回の判定の結果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
	/// 猶予中 (猶予の起点から `health.dave_grace_secs` 以内)。判定しない。
	Grace,
	/// 正常。異常の条件が続いていても回数に達していなければこれを返す。
	Healthy,
	/// 異常の条件が `health.unhealthy_checks` 回続いた。
	Unhealthy(UnhealthyKind),
}

/// 異常の種類。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnhealthyKind {
	/// 未暗号化の送信の増分が許容を超えた (DAVE が有効なのに未暗号化で送った)。
	Plaintext,
	/// 送らなかったフレームの増分が許容を超えた (聴者がいるのに DAVE の鍵が無い)。
	Withheld,
	/// 再生中なのに暗号化も未暗号化も送らなかったも増えない。
	NoFrames,
}

/// 標本の差分から健全性を判定する。標本の基準と、異常が続いた回数を持つ。
#[derive(Debug, Clone)]
pub struct HealthJudge {
	grace: Duration,
	tolerance: u64,
	threshold: u32,
	/// 前回の標本 (差分の基準)。無ければ次の標本を基準にするだけ。
	last: Option<VoiceHealth>,
	/// 未暗号化か送らなかったの増分が許容を超えた回数 (連続)。どちらも暗号化して届けられていないため、まとめて数える。
	unprotected_streak: u32,
	/// 送信が無かった回数 (連続)。
	silent_streak: u32,
}

impl HealthJudge {
	/// 設定のしきい値で作る。
	///
	/// # 引数
	/// - `config`: `[health]` の設定 (`dave_grace_secs`、`dave_plaintext_tolerance_frames`、`unhealthy_checks`)
	///
	/// # 戻り値
	/// 基準も回数も持たない判定器
	///
	/// # 呼び出し例
	/// ```ignore
	/// let judge = HealthJudge::new(&config.health);
	/// ```
	pub fn new(config: &HealthConfig) -> Self {
		Self {
			grace: Duration::from_secs(config.dave_grace_secs),
			tolerance: config.dave_plaintext_tolerance_frames,
			threshold: config.unhealthy_checks.max(1),
			last: None,
			unprotected_streak: 0,
			silent_streak: 0,
		}
	}

	/// 基準と回数を捨てる。再接続したとき (フレームの累計が 0 から数え直されるとき) と、
	/// 再生していない間 (一時停止、曲の切替) に呼ぶ。
	///
	/// # 呼び出し例
	/// ```ignore
	/// judge.reset();
	/// ```
	pub fn reset(&mut self) {
		self.last = None;
		self.unprotected_streak = 0;
		self.silent_streak = 0;
	}

	/// 標本を 1 つ判定する。
	///
	/// - `since_play` が猶予以内なら `Grace`。標本は基準として覚え、回数は数え直す
	/// - 基準が無ければ標本を基準にして `Healthy` (差分が取れないため判定しない)
	/// - DAVE が有効で聴者が 1 人以上なら、未暗号化か送らなかったの増分が許容を超えた回数を数える
	///   (種類は未暗号化が超えていれば `Plaintext`、そうでなければ `Withheld`)。
	///   聴者が 0 人の間は数えず、`unprotected_streak` を 0 に戻す
	/// - 暗号化・未暗号化・送らなかったの増分がすべて 0 なら、送信なしの回数を数える
	/// - それ以外 (送信がある) なら両方の回数を数え直す
	/// - どちらかの回数が `unhealthy_checks` に達したら `Unhealthy`
	///
	/// # 引数
	/// - `sample`: `VoiceSession::health()` の値 (接続からの累計)
	/// - `since_play`: 猶予の起点 (アクターでは今の曲の `TrackStarted` を受けた時刻) からの経過時間
	/// - `listeners`: ボイスチャンネルの聴者 (Bot 以外) の人数
	///
	/// # 戻り値
	/// 判定の結果
	///
	/// # 呼び出し例
	/// ```ignore
	/// if let Verdict::Unhealthy(kind) = judge.observe(&voice.health(), now - started_at, listeners) { recover(kind); }
	/// ```
	pub fn observe(
		&mut self,
		sample: &VoiceHealth,
		since_play: Duration,
		listeners: u32,
	) -> Verdict {
		let previous = self.last.replace(sample.clone());
		if since_play <= self.grace {
			self.unprotected_streak = 0;
			self.silent_streak = 0;
			return Verdict::Grace;
		}
		let Some(previous) = previous else {
			return Verdict::Healthy;
		};
		// 累計が巻き戻った (別の接続の値) 場合は増分 0 として扱う
		let encrypted = sample
			.encrypted_frames
			.saturating_sub(previous.encrypted_frames);
		let plaintext = sample
			.plaintext_frames
			.saturating_sub(previous.plaintext_frames);
		let withheld = sample
			.withheld_frames
			.saturating_sub(previous.withheld_frames);
		let dave_active = sample.dave_version.is_some_and(|version| version > 0);
		let judged = dave_active && listeners > 0;
		let unprotected = if !judged {
			None
		} else if plaintext > self.tolerance {
			Some(UnhealthyKind::Plaintext)
		} else if withheld > self.tolerance {
			Some(UnhealthyKind::Withheld)
		} else {
			None
		};
		if !judged {
			// 判定しない (聴者がいなければ正常でも鍵が無く、全フレームを送らない)
			self.unprotected_streak = 0;
		}
		if let Some(kind) = unprotected {
			self.silent_streak = 0;
			self.unprotected_streak += 1;
			if self.unprotected_streak >= self.threshold {
				return Verdict::Unhealthy(kind);
			}
		} else if encrypted == 0 && plaintext == 0 && withheld == 0 {
			self.unprotected_streak = 0;
			self.silent_streak += 1;
			if self.silent_streak >= self.threshold {
				return Verdict::Unhealthy(UnhealthyKind::NoFrames);
			}
		} else {
			self.unprotected_streak = 0;
			self.silent_streak = 0;
		}
		Verdict::Healthy
	}
}
