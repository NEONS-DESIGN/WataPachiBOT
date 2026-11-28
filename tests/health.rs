//! 音声接続の健全性の判定 (`HealthJudge`) の試験。
//! - しきい値は config.example.toml の `[health]` を使う (コードに値を書かない)
//! - 標本は累計の値。`Feed` が 1 回ごとの増分を足して累計を作る

use std::path::PathBuf;
use std::time::Duration;

use watapachi_bot::audio::backend::VoiceHealth;
use watapachi_bot::config::{Config, HealthConfig};
use watapachi_bot::guild::health::{HealthJudge, UnhealthyKind, Verdict};

/// DAVE が有効なときの版。
const DAVE: Option<u16> = Some(1);
/// 健全な 1 回分の暗号化のフレーム数。
const FRAMES: u64 = 50;
/// 聴者 (Bot 以外の参加者) が 1 人以上いる状態の人数。
const LISTENERS: u32 = 1;

fn health_config() -> HealthConfig {
	let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
	Config::load(&root.join("config.example.toml"))
		.unwrap()
		.health
}

/// 猶予の終わった後の経過時間。呼ぶたびに 1 確認分だけ進める。
struct Feed {
	judge: HealthJudge,
	total: VoiceHealth,
	since: Duration,
	interval: Duration,
	/// 判定に渡す聴者の人数。
	listeners: u32,
}

impl Feed {
	/// 猶予の最後の標本 (基準) を与えた状態で作る。
	fn after_grace(config: &HealthConfig, dave_version: Option<u16>) -> Self {
		let mut judge = HealthJudge::new(config);
		let total = VoiceHealth {
			dave_version,
			encrypted_frames: 0,
			plaintext_frames: 0,
			withheld_frames: 0,
		};
		let grace = Duration::from_secs(config.dave_grace_secs);
		assert_eq!(judge.observe(&total, grace, LISTENERS), Verdict::Grace);
		Self {
			judge,
			total,
			since: grace,
			interval: Duration::from_secs(config.check_interval_secs),
			listeners: LISTENERS,
		}
	}

	/// 増分を足した標本を判定させる。
	fn step(&mut self, encrypted: u64, plaintext: u64) -> Verdict {
		self.step_with_withheld(encrypted, plaintext, 0)
	}

	/// 送らなかったフレームの増分も足した標本を判定させる。
	fn step_with_withheld(&mut self, encrypted: u64, plaintext: u64, withheld: u64) -> Verdict {
		self.total.encrypted_frames += encrypted;
		self.total.plaintext_frames += plaintext;
		self.total.withheld_frames += withheld;
		self.since += self.interval;
		self.judge.observe(&self.total, self.since, self.listeners)
	}
}

#[test]
fn within_grace_is_grace() {
	let config = health_config();
	let mut judge = HealthJudge::new(&config);
	let grace = Duration::from_secs(config.dave_grace_secs);
	let interval = Duration::from_secs(config.check_interval_secs);

	// 猶予の間は、送信が無くても未暗号化が多くても判定しない
	let mut since = Duration::ZERO;
	let mut plaintext = 0;
	while since <= grace {
		plaintext += config.dave_plaintext_tolerance_frames * 10;
		let sample = VoiceHealth {
			dave_version: DAVE,
			encrypted_frames: 0,
			plaintext_frames: plaintext,
			withheld_frames: 0,
		};
		assert_eq!(judge.observe(&sample, since, LISTENERS), Verdict::Grace);
		since += interval;
	}
}

#[test]
fn encrypted_frames_are_healthy() {
	let config = health_config();
	let mut feed = Feed::after_grace(&config, DAVE);
	for _ in 0..config.unhealthy_checks * 3 {
		assert_eq!(feed.step(FRAMES, 0), Verdict::Healthy);
	}
}

#[test]
fn plaintext_within_tolerance_is_healthy() {
	let config = health_config();
	let mut feed = Feed::after_grace(&config, DAVE);
	// 許容 (50 フレーム) ちょうどまでは正常
	for _ in 0..config.unhealthy_checks * 3 {
		assert_eq!(
			feed.step(FRAMES, config.dave_plaintext_tolerance_frames),
			Verdict::Healthy
		);
	}
}

#[test]
fn plaintext_over_tolerance_three_times_is_unhealthy() {
	let config = health_config();
	let mut feed = Feed::after_grace(&config, DAVE);
	let over = config.dave_plaintext_tolerance_frames + 1;
	for _ in 1..config.unhealthy_checks {
		assert_eq!(feed.step(FRAMES, over), Verdict::Healthy);
	}
	assert_eq!(
		feed.step(FRAMES, over),
		Verdict::Unhealthy(UnhealthyKind::Plaintext)
	);
}

#[test]
fn one_healthy_sample_resets_the_count() {
	let config = health_config();
	let mut feed = Feed::after_grace(&config, DAVE);
	let over = config.dave_plaintext_tolerance_frames + 1;
	for _ in 1..config.unhealthy_checks {
		assert_eq!(feed.step(FRAMES, over), Verdict::Healthy);
	}
	// 1 回の正常で数え直す
	assert_eq!(feed.step(FRAMES, 0), Verdict::Healthy);
	for _ in 1..config.unhealthy_checks {
		assert_eq!(feed.step(FRAMES, over), Verdict::Healthy);
	}
	assert_eq!(
		feed.step(FRAMES, over),
		Verdict::Unhealthy(UnhealthyKind::Plaintext)
	);

	// 送信なしも同じく、1 回の正常で数え直す
	let mut feed = Feed::after_grace(&config, DAVE);
	for _ in 1..config.unhealthy_checks {
		assert_eq!(feed.step(0, 0), Verdict::Healthy);
	}
	assert_eq!(feed.step(FRAMES, 0), Verdict::Healthy);
	for _ in 1..config.unhealthy_checks {
		assert_eq!(feed.step(0, 0), Verdict::Healthy);
	}
	assert_eq!(feed.step(0, 0), Verdict::Unhealthy(UnhealthyKind::NoFrames));
}

#[test]
fn no_frames_three_times_is_unhealthy() {
	let config = health_config();
	let mut feed = Feed::after_grace(&config, DAVE);
	for _ in 1..config.unhealthy_checks {
		assert_eq!(feed.step(0, 0), Verdict::Healthy);
	}
	assert_eq!(feed.step(0, 0), Verdict::Unhealthy(UnhealthyKind::NoFrames));
}

#[test]
fn without_dave_only_no_frames_is_judged() {
	let config = health_config();
	let over = config.dave_plaintext_tolerance_frames * 10;
	// DAVE が無効 (None、0) なら、未暗号化の送信は正常な送信として数える (暫定の扱い)
	for dave_version in [None, Some(0)] {
		let mut feed = Feed::after_grace(&config, dave_version);
		for _ in 0..config.unhealthy_checks * 3 {
			assert_eq!(feed.step(0, over), Verdict::Healthy);
		}
		for _ in 1..config.unhealthy_checks {
			assert_eq!(feed.step(0, 0), Verdict::Healthy);
		}
		assert_eq!(feed.step(0, 0), Verdict::Unhealthy(UnhealthyKind::NoFrames));
	}
}

#[test]
fn reset_forgets_the_baseline_and_the_count() {
	let config = health_config();
	let mut feed = Feed::after_grace(&config, DAVE);
	for _ in 1..config.unhealthy_checks {
		assert_eq!(feed.step(0, 0), Verdict::Healthy);
	}
	feed.judge.reset();
	// 基準が無いので最初の標本は基準になるだけ。数え直すので、上限の回数までは正常
	for _ in 0..config.unhealthy_checks {
		assert_eq!(feed.step(0, 0), Verdict::Healthy);
	}
	assert_eq!(feed.step(0, 0), Verdict::Unhealthy(UnhealthyKind::NoFrames));
}

#[test]
fn plaintext_ignored_without_listeners() {
	let config = health_config();
	let mut feed = Feed::after_grace(&config, DAVE);
	feed.listeners = 0;
	let over = config.dave_plaintext_tolerance_frames + FRAMES;
	for _ in 0..config.unhealthy_checks * 3 {
		assert_eq!(feed.step(0, over), Verdict::Healthy);
	}
}

#[test]
fn plaintext_counted_after_listener_joins() {
	let config = health_config();
	let mut feed = Feed::after_grace(&config, DAVE);
	let over = config.dave_plaintext_tolerance_frames + FRAMES;
	// 聴者がいない間の未暗号化は数えない
	feed.listeners = 0;
	for _ in 0..config.unhealthy_checks * 2 {
		assert_eq!(feed.step(0, over), Verdict::Healthy);
	}
	// 聴者が来てから unhealthy_checks 回で異常
	feed.listeners = 1;
	for _ in 1..config.unhealthy_checks {
		assert_eq!(feed.step(0, over), Verdict::Healthy);
	}
	assert_eq!(
		feed.step(0, over),
		Verdict::Unhealthy(UnhealthyKind::Plaintext)
	);
}

#[test]
fn no_frames_detected_without_listeners() {
	let config = health_config();
	let mut feed = Feed::after_grace(&config, DAVE);
	feed.listeners = 0;
	for _ in 1..config.unhealthy_checks {
		assert_eq!(feed.step(0, 0), Verdict::Healthy);
	}
	assert_eq!(feed.step(0, 0), Verdict::Unhealthy(UnhealthyKind::NoFrames));
}

#[test]
fn withheld_ignored_without_listeners() {
	let config = health_config();
	let mut feed = Feed::after_grace(&config, DAVE);
	feed.listeners = 0;
	// 一人の間は鍵が無く、送らなかったフレームだけが増える。送信なしとも見なさない
	for _ in 0..config.unhealthy_checks * 3 {
		assert_eq!(feed.step_with_withheld(0, 0, FRAMES), Verdict::Healthy);
	}
}

#[test]
fn withheld_over_tolerance_with_listeners_is_unhealthy() {
	let config = health_config();
	let mut feed = Feed::after_grace(&config, DAVE);
	for _ in 1..config.unhealthy_checks {
		assert_eq!(feed.step_with_withheld(0, 0, FRAMES), Verdict::Healthy);
	}
	assert_eq!(
		feed.step_with_withheld(0, 0, FRAMES),
		Verdict::Unhealthy(UnhealthyKind::Withheld)
	);
}

#[test]
fn withheld_within_tolerance_is_healthy() {
	let config = health_config();
	let mut feed = Feed::after_grace(&config, DAVE);
	let within = config.dave_plaintext_tolerance_frames;
	for _ in 0..config.unhealthy_checks * 2 {
		assert_eq!(feed.step_with_withheld(FRAMES, 0, within), Verdict::Healthy);
	}
}

#[test]
fn all_three_zero_is_no_frames() {
	let config = health_config();
	let mut feed = Feed::after_grace(&config, DAVE);
	feed.listeners = 0;
	for _ in 1..config.unhealthy_checks {
		assert_eq!(feed.step_with_withheld(0, 0, 0), Verdict::Healthy);
	}
	assert_eq!(
		feed.step_with_withheld(0, 0, 0),
		Verdict::Unhealthy(UnhealthyKind::NoFrames)
	);
}

#[test]
fn withheld_rewind_is_zero_delta() {
	let config = health_config();
	let mut feed = Feed::after_grace(&config, DAVE);
	feed.total.withheld_frames = FRAMES * 10;
	assert_eq!(feed.step(FRAMES, 0), Verdict::Healthy);
	// 別の接続の累計 (巻き戻り) は増分 0 として扱い、異常に数えない
	for _ in 0..config.unhealthy_checks * 2 {
		feed.total.withheld_frames = 0;
		assert_eq!(feed.step(FRAMES, 0), Verdict::Healthy);
	}
}
