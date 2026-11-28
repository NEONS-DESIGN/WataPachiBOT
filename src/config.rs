//! 設定ファイル（config.toml）の読み込みと検証。
//! - 節と項目の名前は config.example.toml と同じ
//! - 未知のキーは `deny_unknown_fields` で拒否する（綴り間違いを黙って無視しない）
//! - 既定値は config.example.toml が持つ。コードに既定値を重ねて持たない

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::audio::limits::{VoiceLimits, OUTER_MARGIN};
use crate::lang::Lang;

pub use crate::error::ConfigError;

/// URL の雛形で動画 ID に置き換える部分。
const UI_ID_PLACEHOLDER: &str = "{id}";
/// `shutdown.timeout_secs` の上限 (秒)。巨大な値で終了の期限 (`Instant + Duration`) があふれないようにする
pub const MAX_SHUTDOWN_TIMEOUT_SECS: u64 = 600;
/// 時間の設定 (秒) の上限 (7 日)。巨大な値で `Instant + Duration` や `Duration` の掛け算があふれて panic しないようにする。
/// `shutdown.timeout_secs` は別の上限、キャッシュの保持期間 (`cache.*`) は対象外
pub const MAX_DURATION_SECS: u64 = 7 * 24 * 60 * 60;
/// `restore.stagger_ms` の上限 (ミリ秒)。復元を終えるまでコマンドの受付を始めないため、
/// 誤った大きな値で起動後に長くコマンドを受け付けない状態にならないようにする
pub const MAX_RESTORE_STAGGER_MS: u64 = 5_000;
/// `worker.worker_count` の上限。1 台は数十 MB の Python のため、誤設定で数を作りすぎないようにする
pub const MAX_WORKER_COUNT: u32 = 16;
/// `db.read_connections` の上限。誤設定で接続を開きすぎてハンドルを使い切らないようにする
pub const MAX_READ_CONNECTIONS: usize = 64;
/// 待ち行列の容量 (`db.writer_queue`、`limits.actor_mailbox`) の上限。
/// tokio のチャネルとセマフォは巨大な容量で panic するため、それより十分に小さく取る
pub const MAX_CHANNEL_CAPACITY: usize = 1_000_000;
/// `playback.initial_chunk_size_bytes` を省略したときの値 (256KiB)。
/// 開発機の計測で、Songbird の入力の解析が先頭から数 KB しか読まず、64KiB では読み戻しで取り直しが起きたため、この大きさにする。
pub const DEFAULT_INITIAL_CHUNK_SIZE_BYTES: u64 = 256 * 1024;

/// `playback.initial_chunk_size_bytes` の省略時の値を返す (serde の既定値)。
fn default_initial_chunk_size() -> u64 {
	DEFAULT_INITIAL_CHUNK_SIZE_BYTES
}

/// `voice.access_check_timeout_secs` の省略時の値 (秒)。
pub const DEFAULT_ACCESS_CHECK_TIMEOUT_SECS: u64 = 3;

/// `voice.access_check_timeout_secs` の省略時の値を返す (serde の既定値)。
fn default_access_check_timeout() -> u64 {
	DEFAULT_ACCESS_CHECK_TIMEOUT_SECS
}

/// `worker.ping_timeout_secs` の省略時の値 (秒)。
/// ping は数 ms で返り、応答の後の温め直し (1.5 秒以内) を待っても十分に収まる長さにする
pub const DEFAULT_PING_TIMEOUT_SECS: u64 = 10;

/// `worker.ping_timeout_secs` の省略時の値を返す (serde の既定値)。
fn default_ping_timeout() -> u64 {
	DEFAULT_PING_TIMEOUT_SECS
}

/// `ui.user_avatar_url` の既定値 (Discord の CDN)。
pub const DEFAULT_USER_AVATAR_URL: &str = "https://cdn.discordapp.com/avatars/{user}/{hash}.png";
/// `ui.member_avatar_url` の既定値 (Discord の CDN)。
pub const DEFAULT_MEMBER_AVATAR_URL: &str =
	"https://cdn.discordapp.com/guilds/{guild}/users/{user}/avatars/{hash}.png";
/// `ui.default_avatar_url` の既定値 (Discord の CDN)。
pub const DEFAULT_DEFAULT_AVATAR_URL: &str = "https://cdn.discordapp.com/embed/avatars/{index}.png";

/// serde の既定値。`DEFAULT_USER_AVATAR_URL` を返す。
fn default_user_avatar_url() -> String {
	DEFAULT_USER_AVATAR_URL.to_owned()
}

/// serde の既定値。`DEFAULT_MEMBER_AVATAR_URL` を返す。
fn default_member_avatar_url() -> String {
	DEFAULT_MEMBER_AVATAR_URL.to_owned()
}

/// serde の既定値。`DEFAULT_DEFAULT_AVATAR_URL` を返す。
fn default_default_avatar_url() -> String {
	DEFAULT_DEFAULT_AVATAR_URL.to_owned()
}

/// `bot.activity_name` の既定値。`{prefix}` は接頭辞に置き換える。
pub const DEFAULT_ACTIVITY_NAME: &str = "{prefix}help";

/// serde の既定値。`DEFAULT_ACTIVITY_NAME` を返す。
fn default_activity_name() -> String {
	DEFAULT_ACTIVITY_NAME.to_owned()
}

/// serde の既定値。`defaults.lang` を省略したときは日本語。
fn default_lang() -> Lang {
	Lang::Ja
}

/// 設定全体。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
	pub bot: BotConfig,
	pub paths: PathsConfig,
	pub defaults: DefaultsConfig,
	pub limits: LimitsConfig,
	pub worker: WorkerConfig,
	pub update: UpdateConfig,
	pub playback: PlaybackConfig,
	pub health: HealthConfig,
	pub voice: VoiceConfig,
	pub cache: CacheConfig,
	pub db: DbConfig,
	pub shutdown: ShutdownConfig,
	pub restore: RestoreConfig,
	pub ui: UiConfig,
	pub gateway: GatewayConfig,
}

/// `[bot]` コマンドの接頭辞とログ。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BotConfig {
	pub prefix: String,
	pub log_level: String,
	pub log_retention_days: u32,
	/// Bot のステータス（プレイ中）に出す名前。`{prefix}` は接頭辞に置き換える。省略時は `DEFAULT_ACTIVITY_NAME`
	#[serde(default = "default_activity_name")]
	pub activity_name: String,
}

/// `[paths]` 実行時に使うパス。相対パスは実行ディレクトリ基準。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathsConfig {
	pub database: PathBuf,
	pub logs: PathBuf,
	pub ytdlp_cache: PathBuf,
	pub worker_envs: PathBuf,
	/// ワーカーと PipeInput が起動する python の実行ファイル。uv の venv の python.exe を指す
	/// (Python install manager のエイリアスを通すと孫が Job から抜ける)。
	pub worker_python: PathBuf,
	/// ワーカーのスクリプト (worker.py)。
	pub worker_script: PathBuf,
	/// uv の実行ファイル。PATH から探す名前でもよい。
	pub uv: PathBuf,
}

/// `[defaults]` サーバーごとの設定の初期値。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultsConfig {
	pub volume: f32,
	pub queue_limit: u32,
	pub playlist_limit: u32,
	pub alone_timeout_secs: u32,
	/// 文面の言語。省略時は日本語
	#[serde(default = "default_lang")]
	pub lang: Lang,
}

/// `[limits]` ユーザーが変更できる値の上限と、アクターの制限。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
	pub queue_limit_max: u32,
	pub playlist_limit_max: u32,
	pub alone_timeout_max_secs: u32,
	pub actor_mailbox: usize,
	pub actor_idle_timeout_secs: u64,
}

/// `[worker]` Python 常駐ワーカー。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
	pub worker_count: u32,
	pub prefetch_reserved_workers: u32,
	/// Rust 側が 1 要求の応答を待つ上限 (秒)。ワーカーへはここから余裕を引いた持ち時間を渡す。
	pub resolve_timeout_secs: u64,
	/// 起動 (再起動) したワーカーが最初の ping に答えるまでの上限 (秒)。超えたら起動の失敗として扱う。
	pub startup_timeout_secs: u64,
	pub ping_interval_secs: u64,
	/// 待機中のワーカーへの ping の応答を待つ上限 (秒)。超えたら止まったワーカーとして再起動する。
	/// `resolve_timeout_secs` より長ければ `resolve_timeout_secs` を使う。
	/// 省略時は `DEFAULT_PING_TIMEOUT_SECS` (この項目より前に作った `config.toml` を読めるようにする)
	#[serde(default = "default_ping_timeout")]
	pub ping_timeout_secs: u64,
	pub max_retries: u32,
	/// `rate_limited` で解決をやり直すまでの最初の待ち時間 (秒)。やり直すたびに倍にする。
	pub rate_limit_backoff_secs: u64,
	/// `rate_limited` の待ち時間の上限 (秒)。
	pub rate_limit_backoff_max_secs: u64,
	/// 空文字で Cookie を使わない。
	pub cookies_browser: String,
	/// `yes` / `no` / `auto`。
	pub youtube_premium: String,
	pub user_agent: String,
}

/// `[update]` yt-dlp の自動更新。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateConfig {
	/// `HH:MM`。空文字で更新しない。
	pub time: String,
	pub recycle_workers_daily: bool,
	/// 新しい venv に入れるパッケージ (空白区切り。`uv pip install` に渡す)。
	pub packages: String,
	/// 新しい venv の Python の版 (`uv venv --python` に渡す)。
	pub python: String,
	/// 更新の検査で解決する動画の ID。
	pub check_video: String,
	/// uv の 1 コマンドの上限 (秒)。
	pub install_timeout_secs: u64,
	/// 新しい環境の起動と検査の上限 (秒)。
	pub check_timeout_secs: u64,
}

/// `[playback]` チャンク取得と先読み。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlaybackConfig {
	pub chunk_size_bytes: u64,
	/// 曲の始めとシークの直後に、読み手が待つ取得の大きさ (バイト)。`chunk_size_bytes` 以上なら同じ大きさ。
	/// 省略時は `DEFAULT_INITIAL_CHUNK_SIZE_BYTES` (この項目より前に作った `config.toml` を読めるようにする)
	#[serde(default = "default_initial_chunk_size")]
	pub initial_chunk_size_bytes: u64,
	pub chunk_fetch_timeout_secs: u64,
	pub chunk_fetch_attempts: u32,
	pub url_refresh_margin_secs: u64,
	/// 解決結果の `available_at` まで最初の取得を待つ時間の上限 (秒)。0 は待たない。
	pub available_wait_max_secs: u64,
	pub prefetch_tracks: u32,
	pub slow_notice_secs: u64,
	pub position_save_interval_secs: u64,
	/// 曲に固有でない理由 (取得の不調) で曲を飛ばすのがこの回数続いたら、飛ばさずに再生を止める。
	/// 曲の再生が始まると数え直す。
	pub skip_streak_limit: u32,
	/// PipeInput が yt-dlp の `-f` に渡す形式の指定。
	pub pipe_format: String,
}

/// `[health]` 音声接続の監視。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthConfig {
	pub check_interval_secs: u64,
	pub dave_grace_secs: u64,
	pub dave_plaintext_tolerance_frames: u64,
	pub unhealthy_checks: u32,
	pub auto_rejoin_max: u32,
	pub auto_rejoin_window_secs: u64,
}

/// `[voice]` Songbird 呼び出しのタイムアウト。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoiceConfig {
	pub call_timeout_secs: u64,
	pub join_timeout_secs: u64,
	/// 参加の前の権限の確認で、Bot の役職を REST で取り直す上限 (秒)。
	/// 省略時は `DEFAULT_ACCESS_CHECK_TIMEOUT_SECS` (この項目より前に作った `config.toml` を読めるようにする)
	#[serde(default = "default_access_check_timeout")]
	pub access_check_timeout_secs: u64,
}

/// `[cache]` キャッシュの保持期間。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
	pub playlist_ttl_secs: u64,
	pub search_days: u32,
	pub track_days: u32,
	pub history_days: u32,
}

/// `[db]` SQLite。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DbConfig {
	pub batch_size: usize,
	pub read_connections: usize,
	pub writer_queue: usize,
}

/// `[shutdown]` 終了処理。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShutdownConfig {
	pub timeout_secs: u64,
}

/// `[restore]` 起動時の復元。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreConfig {
	pub stagger_ms: u64,
}

/// `[ui]` Embed とページ送り。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UiConfig {
	/// 動画のサムネイルの URL の雛形。`{id}` を動画 ID に置き換える。
	pub thumbnail_url: String,
	/// 動画のページの URL の雛形。`{id}` を動画 ID に置き換える。
	pub watch_url: String,
	/// ページ送りのボタンを受け付ける期間 (秒)。発行から過ぎたボタンは動かさない。
	pub pagination_timeout_secs: u64,
	/// ユーザーのアバターの URL の雛形。`{user}` と `{hash}` を置き換える。
	/// 省略時は `DEFAULT_USER_AVATAR_URL` (この項目より前に作った `config.toml` を読めるようにする)
	#[serde(default = "default_user_avatar_url")]
	pub user_avatar_url: String,
	/// サーバーのアバターの URL の雛形。`{guild}`、`{user}`、`{hash}` を置き換える。省略時は既定値
	#[serde(default = "default_member_avatar_url")]
	pub member_avatar_url: String,
	/// 既定のアバターの URL の雛形。`{index}` を置き換える。省略時は既定値
	#[serde(default = "default_default_avatar_url")]
	pub default_avatar_url: String,
}

/// `[gateway]` Discord の Gateway の受信。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayConfig {
	/// `Ready` の後、全ギルドの `GuildCreate` を待つ上限 (秒)。過ぎたら揃わなくても復元とコマンドの受付を始める。
	pub ready_wait_secs: u64,
	/// ボイスの状態の変化をアクターへ届けるまで、混雑時にやり直す上限 (秒)。
	pub event_send_timeout_secs: u64,
}

impl Config {
	/// 設定ファイルを読み込んで検証する。
	///
	/// # 引数
	/// - `path`: config.toml のパス
	///
	/// # 戻り値
	/// 検証済みの `Config`。
	///
	/// # エラー
	/// - 読めない: `ConfigError::Read`
	/// - 構文エラー、未知のキー、型の不一致、キーの欠落: `ConfigError::Parse`
	/// - 範囲の不正: `ConfigError::Invalid`
	///
	/// # 例
	/// ```no_run
	/// # use std::path::Path;
	/// # use watapachi_bot::config::Config;
	/// let config = Config::load(Path::new("config.toml"))?;
	/// # Ok::<(), watapachi_bot::config::ConfigError>(())
	/// ```
	pub fn load(path: &Path) -> Result<Config, ConfigError> {
		let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
			path: path.to_path_buf(),
			source,
		})?;
		let config: Config = toml::from_str(&text).map_err(|source| ConfigError::Parse {
			path: path.to_path_buf(),
			source: Box::new(source),
		})?;
		config.validate()?;
		Ok(config)
	}

	/// 値の範囲を検査する。
	///
	/// - 最初に見つかった不正を返す
	/// - 0 を許さない項目は、ループの停止やゼロ割りを避けるために拒否する
	///
	/// # エラー
	/// `ConfigError::Invalid { key, reason }`。`key` は `節.項目`。
	///
	/// # 例
	/// ```ignore
	/// config.validate()?;
	/// ```
	pub fn validate(&self) -> Result<(), ConfigError> {
		let bot = &self.bot;
		check(!bot.prefix.is_empty(), "bot.prefix", "空にできません")?;
		check(
			!bot.activity_name.trim().is_empty(),
			"bot.activity_name",
			"空にできません",
		)?;
		check(
			matches!(
				bot.log_level.as_str(),
				"error" | "warn" | "info" | "debug" | "trace"
			),
			"bot.log_level",
			"error / warn / info / debug / trace のいずれか",
		)?;

		let d = &self.defaults;
		let l = &self.limits;
		check(
			d.volume.is_finite() && (0.0..=1.0).contains(&d.volume),
			"defaults.volume",
			"0.0 から 1.0 の範囲",
		)?;
		check(
			d.queue_limit >= 1 && d.queue_limit <= l.queue_limit_max,
			"defaults.queue_limit",
			"1 以上 limits.queue_limit_max 以下",
		)?;
		check(
			d.playlist_limit >= 1 && d.playlist_limit <= l.playlist_limit_max,
			"defaults.playlist_limit",
			"1 以上 limits.playlist_limit_max 以下",
		)?;
		check(
			d.alone_timeout_secs <= l.alone_timeout_max_secs,
			"defaults.alone_timeout_secs",
			"limits.alone_timeout_max_secs 以下",
		)?;
		check(
			(1..=MAX_CHANNEL_CAPACITY).contains(&l.actor_mailbox),
			"limits.actor_mailbox",
			&format!("1 以上 {MAX_CHANNEL_CAPACITY} 以下"),
		)?;
		check_secs(
			l.actor_idle_timeout_secs,
			1,
			"limits.actor_idle_timeout_secs",
		)?;

		let w = &self.worker;
		check(
			(1..=MAX_WORKER_COUNT).contains(&w.worker_count),
			"worker.worker_count",
			&format!("1 以上 {MAX_WORKER_COUNT} 以下"),
		)?;
		check(
			w.prefetch_reserved_workers < w.worker_count,
			"worker.prefetch_reserved_workers",
			"worker.worker_count 未満（先読み専用を除いた対話用が 1 つ以上要る）",
		)?;
		let margin = crate::resolver::pool::REQUEST_BUDGET_MARGIN_SECS;
		check(
			w.resolve_timeout_secs > margin && w.resolve_timeout_secs <= MAX_DURATION_SECS,
			"worker.resolve_timeout_secs",
			&format!(
				"{} 以上 {MAX_DURATION_SECS} 以下（ワーカーの持ち時間はここから {margin} 秒を引いた値）",
				margin + 1
			),
		)?;
		check_secs(w.startup_timeout_secs, 1, "worker.startup_timeout_secs")?;
		check_secs(w.ping_interval_secs, 1, "worker.ping_interval_secs")?;
		check_secs(w.ping_timeout_secs, 1, "worker.ping_timeout_secs")?;
		check_secs(
			w.rate_limit_backoff_secs,
			1,
			"worker.rate_limit_backoff_secs",
		)?;
		check(
			w.rate_limit_backoff_max_secs >= w.rate_limit_backoff_secs
				&& w.rate_limit_backoff_max_secs <= MAX_DURATION_SECS,
			"worker.rate_limit_backoff_max_secs",
			&format!("worker.rate_limit_backoff_secs 以上 {MAX_DURATION_SECS} 以下"),
		)?;
		check(
			matches!(w.youtube_premium.as_str(), "yes" | "no" | "auto"),
			"worker.youtube_premium",
			"yes / no / auto のいずれか",
		)?;

		check(
			self.update.time.is_empty() || is_hhmm(&self.update.time),
			"update.time",
			"空、または HH:MM 形式",
		)?;
		let u = &self.update;
		check(
			!self.paths.uv.as_os_str().is_empty(),
			"paths.uv",
			"空にできません",
		)?;
		check(
			!u.packages.trim().is_empty(),
			"update.packages",
			"空にできません",
		)?;
		check(
			!u.python.trim().is_empty(),
			"update.python",
			"空にできません",
		)?;
		check(
			!u.check_video.trim().is_empty(),
			"update.check_video",
			"空にできません",
		)?;
		check_secs(u.install_timeout_secs, 1, "update.install_timeout_secs")?;
		check_secs(u.check_timeout_secs, 1, "update.check_timeout_secs")?;

		let p = &self.playback;
		check(
			p.chunk_size_bytes >= 1,
			"playback.chunk_size_bytes",
			"1 以上",
		)?;
		check(
			p.initial_chunk_size_bytes >= 1,
			"playback.initial_chunk_size_bytes",
			"1 以上",
		)?;
		check_secs(
			p.chunk_fetch_timeout_secs,
			1,
			"playback.chunk_fetch_timeout_secs",
		)?;
		check(
			p.chunk_fetch_attempts >= 1,
			"playback.chunk_fetch_attempts",
			"1 以上",
		)?;
		check_secs(
			p.url_refresh_margin_secs,
			0,
			"playback.url_refresh_margin_secs",
		)?;
		check_secs(
			p.available_wait_max_secs,
			0,
			"playback.available_wait_max_secs",
		)?;
		check_secs(p.slow_notice_secs, 0, "playback.slow_notice_secs")?;
		check_secs(
			p.position_save_interval_secs,
			1,
			"playback.position_save_interval_secs",
		)?;
		check(
			!p.pipe_format.is_empty(),
			"playback.pipe_format",
			"空にできません",
		)?;
		check(
			p.skip_streak_limit >= 1,
			"playback.skip_streak_limit",
			"1 以上",
		)?;

		let h = &self.health;
		check_secs(h.check_interval_secs, 1, "health.check_interval_secs")?;
		check_secs(h.dave_grace_secs, 0, "health.dave_grace_secs")?;
		check(h.unhealthy_checks >= 1, "health.unhealthy_checks", "1 以上")?;
		check_secs(
			h.auto_rejoin_window_secs,
			1,
			"health.auto_rejoin_window_secs",
		)?;

		let v = &self.voice;
		check_secs(v.call_timeout_secs, 1, "voice.call_timeout_secs")?;
		check_secs(v.join_timeout_secs, 1, "voice.join_timeout_secs")?;
		check_secs(
			v.access_check_timeout_secs,
			1,
			"voice.access_check_timeout_secs",
		)?;
		// アクターは待機の時間を過ぎると、起動中の処理 (接続を含む) を捨てて終わる。
		// 接続のタスク (参加の前の確認と接続) の上限より短いと、`?stop` の後などに残った接続の処理が後始末の途中で止まる
		let limits = VoiceLimits::new(v);
		let connect = limits.access() + limits.connect();
		check(
			Duration::from_secs(l.actor_idle_timeout_secs) > connect,
			"limits.actor_idle_timeout_secs",
			&format!(
				"音声の接続を待つ上限 ({} 秒 = voice.access_check_timeout_secs + voice.join_timeout_secs + voice.call_timeout_secs × 4 + {} 秒 × 2) より長い値",
				connect.as_secs(),
				OUTER_MARGIN.as_secs()
			),
		)?;

		let db = &self.db;
		check(db.batch_size >= 1, "db.batch_size", "1 以上")?;
		check(
			(1..=MAX_READ_CONNECTIONS).contains(&db.read_connections),
			"db.read_connections",
			&format!("1 以上 {MAX_READ_CONNECTIONS} 以下"),
		)?;
		check(
			(1..=MAX_CHANNEL_CAPACITY).contains(&db.writer_queue),
			"db.writer_queue",
			&format!("1 以上 {MAX_CHANNEL_CAPACITY} 以下"),
		)?;

		check(
			(1..=MAX_SHUTDOWN_TIMEOUT_SECS).contains(&self.shutdown.timeout_secs),
			"shutdown.timeout_secs",
			&format!("1 以上 {MAX_SHUTDOWN_TIMEOUT_SECS} 以下"),
		)?;
		check(
			self.restore.stagger_ms <= MAX_RESTORE_STAGGER_MS,
			"restore.stagger_ms",
			&format!("{MAX_RESTORE_STAGGER_MS} 以下"),
		)?;

		let ui = &self.ui;
		check(
			ui.thumbnail_url.contains(UI_ID_PLACEHOLDER),
			"ui.thumbnail_url",
			"{id} を含む URL の雛形",
		)?;
		check(
			ui.watch_url.contains(UI_ID_PLACEHOLDER),
			"ui.watch_url",
			"{id} を含む URL の雛形",
		)?;
		check_secs(ui.pagination_timeout_secs, 1, "ui.pagination_timeout_secs")?;

		let g = &self.gateway;
		check_secs(g.ready_wait_secs, 1, "gateway.ready_wait_secs")?;
		check_secs(
			g.event_send_timeout_secs,
			1,
			"gateway.event_send_timeout_secs",
		)?;
		Ok(())
	}
}

/// 時間の設定 (秒) が `min` 以上 `MAX_DURATION_SECS` 以下でなければ `ConfigError::Invalid` を返す。
///
/// # 引数
/// - `value`: 設定の値
/// - `min`: 下限 (0 を許すなら 0)
/// - `key`: `節.項目`
///
/// # エラー
/// 範囲外のとき `ConfigError::Invalid`。
///
/// # 例
/// `check_secs(g.ready_wait_secs, 1, "gateway.ready_wait_secs")?;`
fn check_secs(value: u64, min: u64, key: &str) -> Result<(), ConfigError> {
	check(
		(min..=MAX_DURATION_SECS).contains(&value),
		key,
		&format!("{min} 以上 {MAX_DURATION_SECS} 以下"),
	)
}

/// 条件が偽なら `ConfigError::Invalid` を返す。
///
/// # 引数
/// - `ok`: 検査の結果
/// - `key`: `節.項目`
/// - `reason`: 満たすべき条件の説明
///
/// # エラー
/// `ok` が偽のとき `ConfigError::Invalid`。
fn check(ok: bool, key: &str, reason: &str) -> Result<(), ConfigError> {
	if ok {
		Ok(())
	} else {
		Err(ConfigError::Invalid {
			key: key.to_owned(),
			reason: reason.to_owned(),
		})
	}
}

/// `HH:MM`（00:00 から 23:59）か判定する。
///
/// # 引数
/// - `s`: 判定する文字列
///
/// # 戻り値
/// 形式と範囲が正しければ true。
fn is_hhmm(s: &str) -> bool {
	let Some((h, m)) = s.split_once(':') else {
		return false;
	};
	let two_digits = |t: &str| t.len() == 2 && t.bytes().all(|b| b.is_ascii_digit());
	two_digits(h)
		&& two_digits(m)
		&& h.parse::<u32>().is_ok_and(|h| h < 24)
		&& m.parse::<u32>().is_ok_and(|m| m < 60)
}
