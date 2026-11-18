//! 起動と終了の順序制御。
//! - 起動: 暗号プロバイダ → `.env` と設定とメッセージ → ログ → 二重起動の防止 → Job → DB → ワーカー →
//!   Discord (HTTP、Shard、Songbird、キャッシュ) → バックエンドと登録簿と Dispatcher → Shard の受信 →
//!   Scheduler
//! - 復元は Dispatcher が `Ready` の後に行う。ここでは呼ばない
//! - 終了: 受付の停止と Scheduler への停止の合図 → アクター → DB → ワーカー → Scheduler の終わりを待つ →
//!   Shard。全体で `shutdown.timeout_secs` を上限とし、各段は残りの時間で打ち切って次の段へ進む。
//!   アクターの段は `DB_STAGE_RESERVE` を残して打ち切る
//!   (終了時の位置の保存は DbWriter に積むだけのため、DB の段で書き出せないと失われる)
//! - Scheduler は入れ替えの途中なら、ワーカーの段 (プールの終了) で入れ替えが `Stopped` で終わるまで戻らない。
//!   そのため Scheduler の終わりはワーカーの段の後に待つ
//! - Ctrl+C、コンソールの窓を閉じる操作 (`CTRL_CLOSE_EVENT`)、サービスの停止 (`RunMode::Service`)、
//!   外からの停止の命令 (`watapachi-bot stop`)、Shard の再接続しない終わり (`ShardExit::Fatal`) で終了する。
//!   最後のものはエラーとして返す
//! - サービスでは Ctrl+C と窓を閉じる操作を待たず、停止の合図と停止の命令だけを待つ
//! - 停止の命令の受け口を作れなくても起動は続ける (命令が使えないだけ)
//! - 窓を閉じたときは Windows が約 5 秒で強制終了するため、全体の期限を `CONSOLE_CLOSE_BUDGET` に縮める
//! - トークンはログにもエラーの文言にも出さない
//! - 相対パスはカレントディレクトリ基準 (実行ファイルの場所ではない)
//!
//! 呼び出し方:
//! ```ignore
//! runtime.block_on(app::run(AppPaths::standard()))?;
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context as _};
use arc_swap::ArcSwap;
use songbird::shards::TwilightMap;
use songbird::Songbird;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use twilight_gateway::{ConfigBuilder, Shard, ShardId};
use twilight_http::Client;

use crate::audio::backend::AudioBackend;
use crate::audio::chunked::{ChunkConfig, UrlRefresher};
use crate::audio::pipe::PipeSpec;
use crate::audio::songbird_backend::{InputFactory, SongbirdBackend};
use crate::config::Config;
use crate::db::{settings, Database};
use crate::gateway::dispatcher::{Dispatcher, DispatcherParts};
use crate::gateway::notifier::DiscordNotifier;
use crate::gateway::shard::{new_cache, presence, run_shard, ShardExit, INTENTS};
use crate::guild::notifier::Notifier;
use crate::guild::registry::{ActorDeps, GuildRegistry};
use crate::lang::GuildLangs;
use crate::logging;
use crate::messages::MessageCatalog;
use crate::platform::job::Job;
use crate::platform::single_instance::{SingleInstance, SingleInstanceError, MUTEX_NAME};
use crate::platform::stop_request::{StopListener, STOP_EVENT_NAME};
use crate::resolver::cached::{CachedResolver, HttpStreamLength};
use crate::resolver::pool::RollingRestartError;
use crate::resolver::pool::{WorkerClient, WorkerPool};
use crate::resolver::process::WorkerSpec;
use crate::resolver::Resolver;
use crate::scheduler::{self, SchedulerDeps};
use crate::update::envs::{select_python, EnvStore, FallbackReason, PythonSource};
use crate::update::procedure::{UpdateOutcome, Updater, WorkerSwitch};
use crate::update::tools::UvTools;

/// `.env` でトークンを置くキー。
const TOKEN_KEY: &str = "DISCORD_TOKEN";
/// `.env` のコメント行の先頭。
const ENV_COMMENT: char = '#';
/// UTF-8 の BOM。メモ帳で保存した `.env` の先頭に付くことがある。
const BOM: char = '\u{feff}';
/// 現在のユーザー (Bot 自身) の取得を待つ上限。twilight-http の 1 要求の既定の上限 (10 秒) より長く取り、
/// HTTP クライアント側の失敗を先に受け取る。
const CURRENT_USER_TIMEOUT: Duration = Duration::from_secs(15);

/// ワーカーへの引数の名前。worker.py の argparse と揃える。
const ARG_CACHEDIR: &str = "--cachedir";
const ARG_PREMIUM: &str = "--premium";
const ARG_COOKIES: &str = "--cookies";
const ARG_USER_AGENT: &str = "--user-agent";

/// 終了でアクターの段が使い切らずに DB の段へ残す時間。全体が短いときは半分までに縮める。
/// DbWriter に積んだ書き込み (終了時の位置の保存を含む) を書き出して閉じる時間に充てる。
pub const DB_STAGE_RESERVE: Duration = Duration::from_secs(5);

/// コンソールの窓を閉じたときの終了全体の上限。Windows は `CTRL_CLOSE_EVENT` の約 5 秒後に
/// プロセスを強制終了するため、それより短く取り、アクターと DB の段を間に合わせる。
const CONSOLE_CLOSE_BUDGET: Duration = Duration::from_secs(4);

/// Songbird のミキサースレッド 1 本に載せる通話の数。
/// - ミキサーは再生中の入力を同期で読み、チャンク入力の取得が止まると読みもその場で止まる
/// - 既定の 16 では全サーバーの通話が 1 本に載り、1 サーバーの停滞が他のサーバーの音と操作まで止める
/// - 1 にしてサーバーごとにスレッドを分ける。同時再生は数件なので、スレッドが増える負担は小さい
const MIXERS_PER_THREAD: std::num::NonZeroUsize = std::num::NonZeroUsize::MIN;

/// 終了の段の名前 (ログ用)。
const STAGE_ACTORS: &str = "アクターの終了";
const STAGE_DATABASE: &str = "DB の終了";
const STAGE_WORKERS: &str = "ワーカーの終了";
const STAGE_SCHEDULER: &str = "Scheduler の終了";
const STAGE_SHARD: &str = "Gateway の切断";

/// 設定ファイルの場所 (作業ディレクトリ基準)。
pub const CONFIG_PATH: &str = "config.toml";
/// メッセージ表のディレクトリ (作業ディレクトリ基準)。言語ごとに `{code}.toml` を置く。
pub const MESSAGES_DIR: &str = "messages";
/// トークンを置く `.env` の場所 (作業ディレクトリ基準)。
pub const ENV_PATH: &str = ".env";

/// 起動に使うファイルの場所。相対パスはカレントディレクトリ基準。
#[derive(Debug, Clone)]
pub struct AppPaths {
	/// config.toml
	pub config: PathBuf,
	/// messages (ja.toml・en.toml を置くディレクトリ)
	pub messages: PathBuf,
	/// `.env` (`DISCORD_TOKEN`)
	pub env: PathBuf,
}

impl AppPaths {
	/// 作業ディレクトリ直下の標準の場所を返す。
	///
	/// # 戻り値
	/// `config.toml`、`messages`、`.env` を指す `AppPaths`
	///
	/// # 例
	/// `let paths = AppPaths::standard();`
	pub fn standard() -> Self {
		Self {
			config: PathBuf::from(CONFIG_PATH),
			messages: PathBuf::from(MESSAGES_DIR),
			env: PathBuf::from(ENV_PATH),
		}
	}
}

/// 実行の形。終了のきっかけの待ち方が変わる。
pub enum RunMode {
	/// コンソールで動かす。Ctrl+C と窓を閉じる操作で終了する
	Console,
	/// Windows サービスとして動かす。`stop` が true になったら終了順を始める。
	/// Ctrl+C と窓を閉じる操作は待たない
	Service {
		/// サービス制御マネージャーからの停止の合図
		stop: watch::Receiver<bool>,
	},
}

/// 動いている部品のうち、終了の順序で止めるもの。`shutdown` に渡す。
pub struct RunningParts {
	dispatcher: Arc<Dispatcher>,
	registry: Arc<GuildRegistry>,
	database: Database,
	pool: Arc<WorkerPool>,
	/// Scheduler への停止の合図
	scheduler_stop: watch::Sender<bool>,
	/// Scheduler のタスク
	scheduler_task: JoinHandle<()>,
	/// Shard の受信ループへの終了の合図
	shard_stop: watch::Sender<bool>,
	/// Shard の受信ループ。すでに終わっていれば None
	shard_task: Option<JoinHandle<ShardExit>>,
}

/// 終了のきっかけ。
enum StopReason {
	/// Ctrl+C を受けた
	Signal,
	/// コンソールの窓が閉じられた (`CTRL_CLOSE_EVENT`)
	ConsoleClosed,
	/// サービス制御マネージャーから停止か OS の終了の合図を受けた
	ServiceStop,
	/// 外からの停止の命令 (`watapachi-bot stop`) を受けた
	Requested,
	/// Shard の受信ループが先に終わった
	ShardEnded(Result<ShardExit, tokio::task::JoinError>),
}

/// コンソールで動かす。`run_with(paths, RunMode::Console)` と同じ。
///
/// # 引数
/// - `paths`: 設定、メッセージ、`.env` の場所
///
/// # エラー
/// `run_with` と同じ
///
/// # 例
/// `app::run(AppPaths::standard()).await?;`
pub async fn run(paths: AppPaths) -> anyhow::Result<()> {
	run_with(paths, RunMode::Console).await
}

/// 決まった起動順で立ち上げ、終了の合図を待って終了順で止める。
///
/// - `.env`、設定、メッセージのどれかが不正なら、ログ・ミューテックス・ワーカーを作る前に止まる
/// - Shard が再接続しない形で終わったときも終了順で止め、エラーを返す
///
/// # 引数
/// - `paths`: 設定、メッセージ、`.env` の場所
/// - `mode`: コンソールかサービスか。終了の合図の待ち方が変わる
///
/// # 戻り値
/// Ctrl+C かサービスの停止で終えたら `Ok(())`
///
/// # エラー
/// - トークンが無い、設定かメッセージが不正、ログを初期化できない、既に起動している、
///   Job か DB を作れない、Discord の利用者情報を得られない
/// - Shard が再接続しない形で終わった (トークンの誤り、許されていない Intent など)
///
/// # 例
/// ```ignore
/// app::run_with(AppPaths::standard(), RunMode::Console).await?;
/// ```
pub async fn run_with(paths: AppPaths, mode: RunMode) -> anyhow::Result<()> {
	// 失敗は既に入っている (別の箇所が先に入れた) 場合だけのため無視する
	let _ = rustls::crypto::ring::default_provider().install_default();

	let token = read_token(&paths.env)?;
	let config = Config::load(&paths.config)
		.with_context(|| format!("設定を読めません: {}", paths.config.display()))?;
	let catalog = MessageCatalog::load(&paths.messages)
		.with_context(|| format!("メッセージを読めません: {}", paths.messages.display()))?;

	let _log_guard = logging::init(&config.bot, &config.paths.logs)
		.with_context(|| format!("ログを初期化できません: {}", config.paths.logs.display()))?;
	logging::install_panic_hook();
	tracing::info!(version = env!("CARGO_PKG_VERSION"), "起動します");

	let result = run_logged(token, Arc::new(config), Arc::new(catalog), mode).await;
	match &result {
		Ok(()) => tracing::info!("終了しました"),
		Err(error) => tracing::error!("異常終了します: {error:#}"),
	}
	result
}

/// ログの初期化の後の起動と、終了までの待ち。
///
/// # 引数
/// - `token`: Discord のトークン
/// - `config`: 検証済みの設定
/// - `catalog`: 言語ごとのメッセージ表
/// - `mode`: 終了の合図の待ち方
///
/// # エラー
/// `run_with` の「ログの初期化の後」のエラー
///
/// # 例
/// `run_logged(token, Arc::new(config), Arc::new(catalog), RunMode::Console).await?;`
async fn run_logged(
	token: String,
	config: Arc<Config>,
	catalog: Arc<MessageCatalog>,
	mut mode: RunMode,
) -> anyhow::Result<()> {
	let _instance = SingleInstance::acquire(MUTEX_NAME).map_err(|error| match error {
		// 型のまま渡す。main が二重起動を見分けて専用の終了コードを返すため
		SingleInstanceError::AlreadyRunning => {
			anyhow::Error::new(SingleInstanceError::AlreadyRunning)
		}
		other => anyhow!(other).context("二重起動の確認に失敗しました"),
	})?;
	// 二重起動の防止の後に作る (`stop` はミューテックスの有無で Bot の終わりを知るため)
	let mut stop_request = match StopListener::create(STOP_EVENT_NAME) {
		Ok(listener) => Some(listener),
		Err(error) => {
			tracing::error!(%error, "停止の命令を待てません。watapachi-bot stop は使えません");
			None
		}
	};
	let job = Arc::new(Job::new().context("Job Object を作れません")?);

	ensure_parent_dir(&config.paths.database)?;
	let database = Database::open(&config.paths.database, &config.db)
		.with_context(|| format!("DB を開けません: {}", config.paths.database.display()))?;

	let (python, python_origin) = choose_python(&config);
	check_worker_files(&config, &python, python_origin);
	let pipe = Arc::new(ArcSwap::from_pointee(PipeSpec::from_config(
		&config, &python,
	)));
	let pool = Arc::new(WorkerPool::start(
		worker_spec(&config, &python),
		&config.worker,
		Arc::clone(&job),
	));

	let deadline_after = Duration::from_secs(config.shutdown.timeout_secs);
	let parts = match connect(&token, &config, &catalog, &job, &database, &pool, &pipe).await {
		Ok(connected) => connected,
		Err(error) => {
			// 起動の途中で止める。始めたワーカーと DB は終了の手順で閉じる
			let deadline = Instant::now() + deadline_after;
			stage(STAGE_DATABASE, deadline, close_database(database)).await;
			stage(STAGE_WORKERS, deadline, pool.shutdown()).await;
			return Err(error);
		}
	};
	let Connected {
		dispatcher,
		registry,
		shard_stop,
		mut shard_task,
	} = parts;
	let (scheduler_stop, scheduler_task) =
		start_scheduler(&config, &job, &database, &pool, &pipe, &python);
	match mode {
		RunMode::Console => tracing::info!("起動しました。Ctrl+C で終了します"),
		RunMode::Service { .. } => tracing::info!("起動しました。サービスの停止で終了します"),
	}

	let reason = wait_for_stop(&mut mode, &mut stop_request, &mut shard_task).await;
	let (shard_task, budget) = match reason {
		StopReason::Signal | StopReason::ServiceStop | StopReason::Requested => {
			(Some(shard_task), deadline_after)
		}
		StopReason::ConsoleClosed => (Some(shard_task), deadline_after.min(CONSOLE_CLOSE_BUDGET)),
		StopReason::ShardEnded(_) => (None, deadline_after),
	};
	let deadline = Instant::now() + budget;
	let running = RunningParts {
		dispatcher,
		registry,
		database,
		pool,
		scheduler_stop,
		scheduler_task,
		shard_stop,
		shard_task,
	};
	shutdown(running, deadline).await;

	match reason {
		StopReason::Signal
		| StopReason::ConsoleClosed
		| StopReason::ServiceStop
		| StopReason::Requested => Ok(()),
		StopReason::ShardEnded(Ok(ShardExit::Fatal { close_code })) => {
			tracing::error!(?close_code, "Gateway の接続が再接続しない形で終わりました");
			Err(anyhow!(
				"Gateway の接続が再接続しない形で終わりました (閉じる理由のコード: {close_code:?})"
			))
		}
		StopReason::ShardEnded(Ok(ShardExit::Shutdown)) => {
			Err(anyhow!("終了の合図の前に Gateway の受信が終わりました"))
		}
		StopReason::ShardEnded(Err(error)) => {
			Err(anyhow!(error).context("Gateway の受信のタスクが異常終了しました"))
		}
	}
}

/// Discord への接続の後に動いている部品。
struct Connected {
	dispatcher: Arc<Dispatcher>,
	registry: Arc<GuildRegistry>,
	shard_stop: watch::Sender<bool>,
	shard_task: JoinHandle<ShardExit>,
}

/// Discord に接続し、再生と振り分けの部品を作って Shard の受信を始める (起動順の 8〜9)。
///
/// # 引数
/// - `token`: Discord のトークン
/// - `config`: 設定
/// - `catalog`: 言語ごとのメッセージ表
/// - `job`: PipeInput の子を入れる Job
/// - `database`: 開いた DB
/// - `pool`: 起動を始めたワーカーのプール
/// - `pipe`: PipeInput の起動方法 (更新で入れ替わる)
///
/// # 戻り値
/// 終了の順序で止める部品
///
/// # エラー
/// HTTP クライアントを作れない、言語の設定を DB から読めない、Bot 自身の利用者情報を上限時間内に得られない
///
/// # 例
/// `let connected = connect(&token, &config, &catalog, &job, &database, &pool, &pipe).await?;`
async fn connect(
	token: &str,
	config: &Arc<Config>,
	catalog: &Arc<MessageCatalog>,
	job: &Arc<Job>,
	database: &Database,
	pool: &Arc<WorkerPool>,
	pipe: &Arc<ArcSwap<PipeSpec>>,
) -> anyhow::Result<Connected> {
	let lang_entries = database
		.handles
		.reader
		.read(settings::load_langs)
		.await
		.context("言語の設定を読めません")?;
	tracing::info!(guilds = lang_entries.len(), "言語の設定を読みました");
	let langs = Arc::new(GuildLangs::new(config.defaults.lang, lang_entries));

	// チャンクの取得と全長の取得で 1 つを共有し、googlevideo への TLS 接続を使い回す。
	// 上限は要求ごとに付ける。Client::new は TLS の初期化の失敗で panic するため builder で作る
	let media_http = reqwest::Client::builder()
		.build()
		.context("動画の取得に使う HTTP クライアントを作れません")?;
	let length = HttpStreamLength::new(
		media_http.clone(),
		Duration::from_secs(config.playback.chunk_fetch_timeout_secs),
	);
	let resolver = Arc::new(CachedResolver::new(
		Arc::clone(pool) as Arc<dyn WorkerClient>,
		database.handles.clone(),
		Arc::new(length),
		config.cache.clone(),
		config.playback.clone(),
	));

	let http = Arc::new(Client::new(token.to_owned()));
	let current_user = async {
		let response = http
			.current_user()
			.await
			.context("Discord から Bot の利用者情報を得られません")?;
		response
			.model()
			.await
			.context("Discord の利用者情報の応答を読めません")
	};
	let bot_id = tokio::time::timeout(CURRENT_USER_TIMEOUT, current_user)
		.await
		.map_err(|_| {
			anyhow!("Discord から Bot の利用者情報を得られません (上限時間を過ぎました)")
		})??
		.id;
	tracing::info!(bot_id = bot_id.get(), "Discord の利用者情報を得ました");

	let shard_config = ConfigBuilder::new(token.to_owned(), INTENTS)
		.presence(presence(&config.bot)?)
		.build();
	let shard = Shard::with_config(ShardId::ONE, shard_config);
	let senders = HashMap::from([(ShardId::ONE.number(), shard.sender())]);
	let songbird = Arc::new(Songbird::twilight_from_config(
		Arc::new(TwilightMap::new(senders)),
		bot_id,
		songbird_config(),
	));
	let cache = Arc::new(new_cache());

	let inputs = Arc::new(InputFactory {
		client: media_http,
		refresher: Arc::clone(&resolver) as Arc<dyn UrlRefresher>,
		chunk: ChunkConfig::from_config(config),
		pipe: Arc::clone(pipe),
		job: Arc::clone(job),
		watch_url: config.ui.watch_url.clone(),
	});
	let audio = Arc::new(SongbirdBackend::new(
		Arc::clone(&songbird),
		inputs,
		config.voice.clone(),
		Duration::from_secs(config.gateway.event_send_timeout_secs),
	));
	let notifier = Arc::new(DiscordNotifier::new(
		Arc::clone(&http),
		Arc::clone(&cache),
		Arc::clone(catalog),
		Arc::clone(&langs),
		config.ui.clone(),
		config.bot.prefix.clone(),
	));
	let registry = Arc::new(GuildRegistry::new(ActorDeps {
		audio: audio as Arc<dyn AudioBackend>,
		resolver: resolver as Arc<dyn Resolver>,
		notifier: Arc::clone(&notifier) as Arc<dyn Notifier>,
		db: database.handles.clone(),
		config: Arc::clone(config),
	}));
	let dispatcher = Dispatcher::new(DispatcherParts {
		http,
		cache: Arc::clone(&cache),
		notifier,
		registry: Arc::clone(&registry),
		db: database.handles.clone(),
		config: Arc::clone(config),
		catalog: Arc::clone(catalog),
		langs,
		bot_id,
	});

	let (shard_stop, stop_receiver) = watch::channel(false);
	let shard_task = tokio::spawn(run_shard(
		shard,
		cache,
		songbird,
		Duration::from_secs(config.voice.call_timeout_secs),
		Arc::clone(&dispatcher),
		stop_receiver,
	));
	Ok(Connected {
		dispatcher,
		registry,
		shard_stop,
		shard_task,
	})
}

/// Songbird の設定を作る。ミキサーのスケジューラを `MIXERS_PER_THREAD` 件ごとのスレッドにする。
///
/// - 呼ぶたびに新しいスケジューラ (スレッドの管理役) を起動するため、`connect` で 1 回だけ呼ぶ
/// - tokio のランタイムの中で呼ぶこと
///
/// # 戻り値
/// `Songbird::twilight_from_config` に渡す設定
///
/// # 例
/// `Songbird::twilight_from_config(map, bot_id, songbird_config())`
fn songbird_config() -> songbird::Config {
	let mut scheduler = songbird::driver::SchedulerConfig::default();
	scheduler.strategy = songbird::driver::SchedulerMode::MaxPerThread(MIXERS_PER_THREAD);
	songbird::Config::default().scheduler(songbird::driver::Scheduler::new(scheduler))
}

/// Scheduler を起動する (起動順の 9)。
///
/// - 更新の手順は `paths.worker_envs` の環境を使い、入れ替えは `LiveSwitch` (プールと `pipe` を共有) で行う
/// - `update.time` が空なら掃除だけを行う
/// - 時計は `jiff::ZonedDateTime::now` (OS のローカル時刻)
/// - 起動から `STARTUP_UPDATE_HOLD` の間は、更新の窓の中でも毎日の処理を始めない
///
/// # 引数
/// - `config`: 設定
/// - `job`: uv と検査の子を入れる Job
/// - `database`: 開いた DB
/// - `pool`: ワーカーのプール
/// - `pipe`: PipeInput の起動方法 (`InputFactory` と共有)
/// - `python`: 起動時に選んだ python.exe (ワーカーの起動指定の雛形に使う)
///
/// # 戻り値
/// 停止の合図の送り手と、Scheduler のタスク
///
/// # 例
/// `let (scheduler_stop, scheduler_task) = start_scheduler(&config, &job, &database, &pool, &pipe, &python);`
fn start_scheduler(
	config: &Config,
	job: &Arc<Job>,
	database: &Database,
	pool: &Arc<WorkerPool>,
	pipe: &Arc<ArcSwap<PipeSpec>>,
	python: &Path,
) -> (watch::Sender<bool>, JoinHandle<()>) {
	let template = worker_spec(config, python);
	let tools = UvTools::from_config(config, template.clone(), Arc::clone(job));
	let updater = Updater::new(
		EnvStore::new(&config.paths.worker_envs).keep_python(&config.paths.worker_python),
		Arc::new(tools),
		config.update.recycle_workers_daily,
	);
	let switch = LiveSwitch {
		pool: Arc::clone(pool),
		template,
		pipe: Arc::clone(pipe),
	};
	let deps = SchedulerDeps {
		writer: database.handles.writer.clone(),
		cache: config.cache.clone(),
		update_at: scheduler::parse_time(&config.update.time),
		updater: Arc::new(updater),
		switch: Arc::new(switch),
		pool: Arc::clone(pool),
		clock: Arc::new(jiff::ZonedDateTime::now),
		startup_hold: scheduler::STARTUP_UPDATE_HOLD,
	};
	let (stop, stop_receiver) = watch::channel(false);
	(stop, scheduler::spawn(deps, stop_receiver))
}

/// 外からの停止の命令と、実行の形に応じた終了のきっかけを待つ。
///
/// - 停止の命令の待ちが止まった (OS の待ちの失敗) ときはエラーのログを残し、残りだけを待つ
///
/// # 引数
/// - `mode`: コンソールなら Ctrl+C と窓を閉じる操作、サービスなら停止の合図を待つ
/// - `stop_request`: 停止の命令の受け口。None なら待たない
/// - `shard_task`: Shard の受信ループ。終わった場合は以後 poll しないこと
///
/// # 戻り値
/// 終了のきっかけ
///
/// # 例
/// `let reason = wait_for_stop(&mut mode, &mut stop_request, &mut shard_task).await;`
async fn wait_for_stop(
	mode: &mut RunMode,
	stop_request: &mut Option<StopListener>,
	shard_task: &mut JoinHandle<ShardExit>,
) -> StopReason {
	let Some(listener) = stop_request else {
		return wait_for_mode_stop(mode, shard_task).await;
	};
	tokio::select! {
		received = listener.requested() => match received {
			Some(()) => {
				tracing::info!("停止の命令を受けました");
				StopReason::Requested
			}
			None => {
				tracing::error!("停止の命令の待ちが止まりました。watapachi-bot stop は使えません");
				wait_for_mode_stop(mode, shard_task).await
			}
		},
		reason = wait_for_mode_stop(mode, shard_task) => reason,
	}
}

/// 実行の形に応じて、終了のきっかけを待つ。
///
/// # 引数
/// - `mode`: コンソールなら Ctrl+C と窓を閉じる操作、サービスなら停止の合図を待つ
/// - `shard_task`: Shard の受信ループ。終わった場合は以後 poll しないこと
///
/// # 戻り値
/// 終了のきっかけ
///
/// # 例
/// `let reason = wait_for_mode_stop(&mut mode, &mut shard_task).await;`
async fn wait_for_mode_stop(
	mode: &mut RunMode,
	shard_task: &mut JoinHandle<ShardExit>,
) -> StopReason {
	match mode {
		RunMode::Console => wait_for_console_stop(shard_task).await,
		RunMode::Service { stop } => wait_for_service_stop(stop, shard_task).await,
	}
}

/// サービスの停止の合図、Shard の受信ループの終わりのどちらかを待つ。
///
/// - 合図の送り手が先に破棄された場合も、停止として扱う (サービスの本体が先に終わった)
///
/// # 引数
/// - `stop`: true を受けたら停止する。既に true なら直ちに戻る
/// - `shard_task`: Shard の受信ループ。終わった場合は以後 poll しないこと
///
/// # 戻り値
/// 終了のきっかけ
///
/// # 例
/// `let reason = wait_for_service_stop(&mut stop, &mut shard_task).await;`
async fn wait_for_service_stop(
	stop: &mut watch::Receiver<bool>,
	shard_task: &mut JoinHandle<ShardExit>,
) -> StopReason {
	tokio::select! {
		result = stop.wait_for(|stopped| *stopped) => {
			if result.is_err() {
				tracing::warn!("サービスの停止の合図の送り手が先に破棄されました");
			}
			tracing::info!("サービスの停止を受けました");
			StopReason::ServiceStop
		}
		joined = &mut *shard_task => StopReason::ShardEnded(joined),
	}
}

/// Ctrl+C、コンソールの窓を閉じる操作、Shard の受信ループの終わりのどれかを待つ。
///
/// - Ctrl+C か窓を閉じる操作を待てない (登録に失敗した) ときはエラーのログを残し、残りだけを待つ
/// - 窓を閉じる操作 (`CTRL_CLOSE_EVENT`) を受けた後、Windows は約 5 秒でプロセスを強制終了する。
///   tokio は通知の後に通知のスレッドを止めて戻さないため、その間は終了処理を進められる
///
/// # 引数
/// - `shard_task`: Shard の受信ループ。終わった場合は以後 poll しないこと
///
/// # 戻り値
/// 終了のきっかけ
///
/// # 例
/// `let reason = wait_for_console_stop(&mut shard_task).await;`
async fn wait_for_console_stop(shard_task: &mut JoinHandle<ShardExit>) -> StopReason {
	let mut signal_available = true;
	let mut console_close = match tokio::signal::windows::ctrl_close() {
		Ok(listener) => Some(listener),
		Err(error) => {
			tracing::error!(%error, "コンソールの窓を閉じる操作を待てません");
			None
		}
	};
	loop {
		tokio::select! {
			closed = recv_console_close(&mut console_close), if console_close.is_some() => match closed {
				Some(()) => {
					tracing::info!("コンソールの窓が閉じられました。短い期限で終了します");
					return StopReason::ConsoleClosed;
				}
				None => {
					tracing::error!("コンソールの窓を閉じる操作の通知が止まりました");
					console_close = None;
				}
			},
			signal = tokio::signal::ctrl_c(), if signal_available => match signal {
				Ok(()) => {
					tracing::info!("Ctrl+C を受けました");
					return StopReason::Signal;
				}
				Err(error) => {
					tracing::error!(%error, "Ctrl+C を待てません。Gateway の終わりだけを待ちます");
					signal_available = false;
				}
			},
			joined = &mut *shard_task => return StopReason::ShardEnded(joined),
		}
	}
}

/// コンソールの窓を閉じる操作を 1 回待つ。
///
/// # 引数
/// - `listener`: 登録した受け口。None なら永遠に待つ (`select!` の枝は条件で止める)
///
/// # 戻り値
/// 受けたら Some(())、通知が止まったら None
///
/// # 例
/// `recv_console_close(&mut console_close).await`
async fn recv_console_close(
	listener: &mut Option<tokio::signal::windows::CtrlClose>,
) -> Option<()> {
	match listener {
		Some(listener) => listener.recv().await,
		None => std::future::pending().await,
	}
}

/// 終了の各段を、全体で `shutdown.timeout_secs` の期限の中で順に行う。
///
/// - 順序: 受付の停止と Scheduler への停止の合図 → 全アクターの `Shutdown` → DB を空にして閉じる →
///   ワーカーの終了 → Scheduler の終わりを待つ → Shard を閉じる (合図を送り、受信ループの終わりを待つ)
/// - アクターの段は `actors_deadline` の期限で打ち切り、DB の段に `DB_STAGE_RESERVE` を残す
/// - 各段は `deadline` までの残りの時間で打ち切り、超えたら警告して次の段へ進む。
///   期限を過ぎた後の段も、合図を送るなどの即時の処理は行う
/// - Scheduler は合図で uv の段を捨てて戻るが、入れ替えの途中ならプールの終了まで戻らない。
///   ワーカーの段より前に待つと止まるため、ワーカーの段の後に残りの時間で待ち、過ぎたら打ち切る
/// - Shard の受信ループが期限までに終わらなければ打ち切る
///
/// # 引数
/// - `parts`: 動いている部品
/// - `deadline`: 全体の期限
///
/// # 例
/// `shutdown(parts, Instant::now() + Duration::from_secs(config.shutdown.timeout_secs)).await;`
pub async fn shutdown(parts: RunningParts, deadline: Instant) {
	let RunningParts {
		dispatcher,
		registry,
		database,
		pool,
		scheduler_stop,
		scheduler_task,
		shard_stop,
		shard_task,
	} = parts;
	tracing::info!("終了処理を始めます");
	dispatcher.stop_accepting();
	// Scheduler が先に終わっていれば、送信の失敗は無視してよい
	let _ = scheduler_stop.send(true);
	// アクターの段は自分の期限で戻る。外側の `deadline` は万一戻らないときの打ち切り
	let actors_until = actors_deadline(Instant::now(), deadline);
	stage(STAGE_ACTORS, deadline, registry.shutdown_all(actors_until)).await;
	stage(STAGE_DATABASE, deadline, close_database(database)).await;
	stage(STAGE_WORKERS, deadline, pool.shutdown()).await;
	join_or_abort(STAGE_SCHEDULER, deadline, scheduler_task).await;

	// 受信ループが既に終わっていれば、送信の失敗は無視してよい
	let _ = shard_stop.send(true);
	if let Some(task) = shard_task {
		if let Some(exit) = join_or_abort(STAGE_SHARD, deadline, task).await {
			tracing::info!(?exit, "Gateway を閉じました");
		}
	}
	tracing::info!("終了処理を終えました");
}

/// 終了の段として、タスクの終わりを期限まで待つ。過ぎたら警告して打ち切る (`abort`)。
///
/// - タスクが異常終了 (panic、打ち切り) していたら警告して None を返す
///
/// # 引数
/// - `name`: 段の名前 (ログ用)
/// - `deadline`: 全体の期限
/// - `task`: 待つタスク
///
/// # 戻り値
/// 期限内に正常に終われば Some(タスクの戻り値)、それ以外は None
///
/// # 例
/// `join_or_abort(STAGE_SCHEDULER, deadline, scheduler_task).await;`
pub async fn join_or_abort<T>(name: &str, deadline: Instant, mut task: JoinHandle<T>) -> Option<T> {
	match tokio::time::timeout_at(deadline, &mut task).await {
		Ok(Ok(output)) => Some(output),
		Ok(Err(error)) => {
			tracing::warn!(stage = name, %error, "終了の段のタスクが異常終了しました");
			None
		}
		Err(_) => {
			tracing::warn!(
				stage = name,
				"終了の段が上限時間を過ぎたため、待たずに打ち切ります"
			);
			task.abort();
			None
		}
	}
}

/// 終了でアクターの段を打ち切る期限を決める (純粋)。
///
/// - 残りの時間から `DB_STAGE_RESERVE` を差し引いた時刻。残りが短ければ、残りの半分を DB の段に残す
/// - 期限を過ぎていれば `now` を返す
///
/// # 引数
/// - `now`: 今の時刻
/// - `deadline`: 終了全体の期限
///
/// # 戻り値
/// アクターの段の期限 (`now` 以上 `deadline` 以下)
///
/// # 例
/// `registry.shutdown_all(actors_deadline(Instant::now(), deadline)).await;`
pub fn actors_deadline(now: Instant, deadline: Instant) -> Instant {
	let remaining = deadline.saturating_duration_since(now);
	let reserve = DB_STAGE_RESERVE.min(remaining / 2);
	now + (remaining - reserve)
}

/// 終了の 1 段を期限まで待つ。過ぎたら警告して戻る。
///
/// # 引数
/// - `name`: 段の名前 (ログ用)
/// - `deadline`: 全体の期限
/// - `future`: 段の処理
///
/// # 戻り値
/// 期限内に終われば Some(結果)、過ぎたら None
///
/// # 例
/// `stage(STAGE_WORKERS, deadline, pool.shutdown()).await;`
async fn stage<F: Future>(name: &str, deadline: Instant, future: F) -> Option<F::Output> {
	match tokio::time::timeout_at(deadline, future).await {
		Ok(output) => Some(output),
		Err(_) => {
			tracing::warn!(
				stage = name,
				"終了の段が上限時間を過ぎたため、待たずに次へ進みます"
			);
			None
		}
	}
}

/// DB を閉じ、失敗はログに残す。
///
/// # 引数
/// - `database`: 閉じる DB
///
/// # 例
/// `close_database(database).await;`
async fn close_database(database: Database) {
	if let Err(error) = database.close().await {
		tracing::warn!(%error, "DB を閉じられませんでした");
	}
}

/// 起動中の Bot の切り替え。ワーカーのプールと、`InputFactory` と共有する `PipeSpec` を持つ。
///
/// - 更新の手順 (`Updater::run`) の `WorkerSwitch` として使う
/// - `pipe` は起動時に作った `Arc<ArcSwap<PipeSpec>>` と同じものを渡す
///
/// # 例
/// `let live = LiveSwitch { pool, template: worker_spec(&config, &python), pipe };`
pub struct LiveSwitch {
	/// ワーカーのプール
	pub pool: Arc<WorkerPool>,
	/// 起動指定の雛形。python だけ差し替える
	pub template: WorkerSpec,
	/// PipeInput の起動方法 (`InputFactory` と共有)
	pub pipe: Arc<ArcSwap<PipeSpec>>,
}

#[async_trait::async_trait]
impl WorkerSwitch for LiveSwitch {
	/// ワーカーを入れ替える。
	///
	/// - `Some` なら雛形の python を差し替えた起動指定で `rolling_restart` し、成功したら `pipe` の python も差し替える
	/// - `None` なら同じ環境で入れ替える (`pipe` は変えない)
	///
	/// # 引数
	/// - `python`: 新しい環境の python.exe
	///
	/// # エラー
	/// `WorkerPool::rolling_restart` の失敗。失敗時は `pipe` を変えない
	async fn switch(&self, python: Option<PathBuf>) -> Result<(), RollingRestartError> {
		let spec = python.as_ref().map(|python| WorkerSpec {
			python: python.clone(),
			..self.template.clone()
		});
		self.pool.rolling_restart(spec).await?;
		if let Some(python) = python {
			let mut next = PipeSpec::clone(&self.pipe.load());
			next.python = python;
			self.pipe.store(Arc::new(next));
		}
		Ok(())
	}
}

/// `update-worker` で Bot が動いていたときの文言。
pub const UPDATE_WORKER_BOT_RUNNING: &str = "Bot の起動中は使えません。止めてから実行してください";
/// 更新の結果の表示: 更新した。
const OUTCOME_UPDATED: &str = "更新しました: yt-dlp";
/// 更新の結果の表示: 同じ版だった。
const OUTCOME_UNCHANGED: &str = "更新はありません: yt-dlp";
/// 更新の結果の表示: 失敗した。
const OUTCOME_FAILED: &str = "失敗しました";
/// 更新の結果の表示: 途中で終了した。
const OUTCOME_STOPPED: &str = "途中で終了しました。現在の環境は変わっていません";

/// Bot を止めている間にワーカーの環境を作る (`watapachi-bot update-worker`)。
///
/// - 設定を読み、ログを初期化し、二重起動の防止の mutex を取る (Bot の起動中は取れず Err)
/// - 入れ替え先なしで `Updater::run` を実行する (検査まで成功したら `current.json` を書く)
/// - 最初の環境を作る用途にも使う
///
/// # 引数
/// - `paths`: 設定とメッセージの場所
///
/// # 戻り値
/// 更新の結果。`Failed` と `Stopped` を終了コードに変えるのは呼び出し側
///
/// # エラー
/// 設定の読み込み、ログの初期化、Job の作成の失敗。Bot が起動中のとき
///
/// # 例
/// `let outcome = update_worker(AppPaths::standard()).await?;`
pub async fn update_worker(paths: AppPaths) -> anyhow::Result<UpdateOutcome> {
	// 失敗は既に入っている場合だけのため無視する
	let _ = rustls::crypto::ring::default_provider().install_default();

	let config = Config::load(&paths.config)
		.with_context(|| format!("設定を読めません: {}", paths.config.display()))?;
	let _log_guard = logging::init(&config.bot, &config.paths.logs)
		.with_context(|| format!("ログを初期化できません: {}", config.paths.logs.display()))?;
	logging::install_panic_hook();

	let _instance = SingleInstance::acquire(MUTEX_NAME).map_err(|error| match error {
		SingleInstanceError::AlreadyRunning => anyhow!(UPDATE_WORKER_BOT_RUNNING),
		other => anyhow!(other).context("二重起動の確認に失敗しました"),
	})?;
	let job = Arc::new(Job::new().context("Job Object を作れません")?);

	let template = worker_spec(&config, &config.paths.worker_python);
	let tools = UvTools::from_config(&config, template, job);
	let updater = Updater::new(
		EnvStore::new(&config.paths.worker_envs).keep_python(&config.paths.worker_python),
		Arc::new(tools),
		config.update.recycle_workers_daily,
	);
	let outcome = updater.run(None, jiff::ZonedDateTime::now()).await;
	tracing::info!(?outcome, "update-worker を終えました");
	Ok(outcome)
}

/// `UpdateOutcome` を運用者向けの 1 行にする (純粋)。
///
/// # 引数
/// - `outcome`: 更新の結果
///
/// # 戻り値
/// 版や失敗の段と理由を含む 1 行
///
/// # 例
/// `println!("{}", describe_outcome(&outcome));`
pub fn describe_outcome(outcome: &UpdateOutcome) -> String {
	match outcome {
		UpdateOutcome::Updated { version } => format!("{OUTCOME_UPDATED} {version}"),
		UpdateOutcome::Unchanged { version, .. } => format!("{OUTCOME_UNCHANGED} {version}"),
		UpdateOutcome::Failed { stage, message } => {
			format!("{OUTCOME_FAILED} ({}): {message}", stage.label())
		}
		UpdateOutcome::Stopped => OUTCOME_STOPPED.to_owned(),
	}
}

/// ワーカーの起動指定を作る (純粋)。
///
/// - `python` は引数 (起動時に選んだ環境の python.exe)、`script = paths.worker_script`
/// - 引数は `--cachedir <paths.ytdlp_cache> --premium <worker.youtube_premium>` に、
///   `worker.cookies_browser` が空でなければ `--cookies <cookies_browser>`、
///   `worker.user_agent` が空でなければ `--user-agent <user_agent>` を足す
/// - `--request-budget-secs` は `WorkerPool` が足すため含めない
///
/// # 引数
/// - `config`: 設定
/// - `python`: ワーカーを起動する python.exe
///
/// # 戻り値
/// ワーカーの起動指定
///
/// # 例
/// `let pool = WorkerPool::start(worker_spec(&config, &python), &config.worker, job);`
pub fn worker_spec(config: &Config, python: &Path) -> WorkerSpec {
	let worker = &config.worker;
	let mut args = vec![
		ARG_CACHEDIR.to_owned(),
		config.paths.ytdlp_cache.to_string_lossy().into_owned(),
		ARG_PREMIUM.to_owned(),
		worker.youtube_premium.clone(),
	];
	if !worker.cookies_browser.is_empty() {
		args.push(ARG_COOKIES.to_owned());
		args.push(worker.cookies_browser.clone());
	}
	if !worker.user_agent.is_empty() {
		args.push(ARG_USER_AGENT.to_owned());
		args.push(worker.user_agent.clone());
	}
	WorkerSpec {
		python: python.to_path_buf(),
		script: config.paths.worker_script.clone(),
		args,
	}
}

/// `.env` の本文から 1 つのキーの値を取り出す (純粋)。
///
/// - 1 行 1 組の `KEY=VALUE`。`#` で始まる行と空行は飛ばす。先頭の BOM と行末の CR は無視する
/// - キーと値の前後の空白を除き、値が同じ引用符 (`"` か `'`) で囲まれていれば外す
/// - 同じキーが複数あれば後の行を使う
///
/// # 引数
/// - `text`: `.env` の本文
/// - `key`: 探すキー
///
/// # 戻り値
/// 値。キーが無ければ None (空の値は `Some("")`)
///
/// # 例
/// `assert_eq!(parse_env_value("A=1\n", "A").as_deref(), Some("1"));`
pub fn parse_env_value(text: &str, key: &str) -> Option<String> {
	let text = text.strip_prefix(BOM).unwrap_or(text);
	let mut found = None;
	for line in text.lines() {
		let line = line.trim();
		if line.is_empty() || line.starts_with(ENV_COMMENT) {
			continue;
		}
		let Some((name, value)) = line.split_once('=') else {
			continue;
		};
		if name.trim() != key {
			continue;
		}
		found = Some(unquote(value.trim()).to_owned());
	}
	found
}

/// 値を囲む同じ引用符を 1 組外す。
///
/// # 引数
/// - `value`: 前後の空白を除いた値
///
/// # 戻り値
/// 引用符を外した値。囲まれていなければそのまま
///
/// # 例
/// `assert_eq!(unquote("\"a\""), "a");`
fn unquote(value: &str) -> &str {
	for quote in ['"', '\''] {
		if let Some(inner) = value
			.strip_prefix(quote)
			.and_then(|rest| rest.strip_suffix(quote))
		{
			return inner;
		}
	}
	value
}

/// `.env` から `DISCORD_TOKEN` を読む。
///
/// - エラーの文言にトークンや他の値を含めない
///
/// # 引数
/// - `path`: `.env` のパス
///
/// # 戻り値
/// 空でないトークン
///
/// # エラー
/// ファイルを読めない、`DISCORD_TOKEN` が無いか空
///
/// # 例
/// `let token = read_token(Path::new(".env"))?;`
pub fn read_token(path: &Path) -> anyhow::Result<String> {
	let text = std::fs::read_to_string(path).with_context(|| {
		format!(
			"{} を読めません。{TOKEN_KEY} を書いた .env を置いてください (.env.example を写して作る)",
			path.display()
		)
	})?;
	match parse_env_value(&text, TOKEN_KEY) {
		Some(token) if !token.is_empty() => Ok(token),
		_ => bail!("{} に {TOKEN_KEY} がありません", path.display()),
	}
}

/// ファイルの親ディレクトリが無ければ作る。
///
/// # 引数
/// - `path`: ファイルのパス
///
/// # エラー
/// ディレクトリを作れない
///
/// # 例
/// `ensure_parent_dir(&config.paths.database)?;`
fn ensure_parent_dir(path: &Path) -> anyhow::Result<()> {
	if let Some(parent) = path.parent() {
		if !parent.as_os_str().is_empty() {
			std::fs::create_dir_all(parent)
				.with_context(|| format!("ディレクトリを作れません: {}", parent.display()))?;
		}
	}
	Ok(())
}

/// 起動時の python の出どころ (ログの文言): current.json の環境。
const PYTHON_ORIGIN_CURRENT: &str = "current.json の環境の python";
/// 起動時の python の出どころ (ログの文言): current.json の previous の環境。
const PYTHON_ORIGIN_PREVIOUS: &str = "直前の環境 (current.json の previous) の python";
/// 起動時の python の出どころ (ログの文言): 設定。
const PYTHON_ORIGIN_CONFIG: &str = "paths.worker_python";

/// ワーカーと PipeInput が使う python を選び、結果をログに残す。
///
/// - `paths.worker_envs` の `current.json` が指す python があればそれを使う
/// - 無ければ直前の環境 (`previous`) の python を、警告を出して使う
/// - 記録なし、壊れている、どちらの python も無いときは警告と理由を出し、`paths.worker_python` を使う
///
/// # 引数
/// - `config`: 設定
///
/// # 戻り値
/// (使う python.exe, その出どころの説明 (`PYTHON_ORIGIN_*`))
///
/// # 例
/// `let (python, origin) = choose_python(&config);`
fn choose_python(config: &Config) -> (PathBuf, &'static str) {
	let store = EnvStore::new(&config.paths.worker_envs);
	let (python, source) = select_python(&store, &config.paths.worker_python);
	let origin = match source {
		PythonSource::Current(name) => {
			tracing::info!(env = %name, python = %python.display(), "更新済みの環境の python を使います");
			PYTHON_ORIGIN_CURRENT
		}
		PythonSource::Previous { name, current } => {
			tracing::warn!(
				env = %name,
				current = %current,
				python = %python.display(),
				"current.json が指す python が無いため、直前の環境の python を使います"
			);
			PYTHON_ORIGIN_PREVIOUS
		}
		PythonSource::Fallback(reason) => {
			let reason = match reason {
				FallbackReason::NoRecord => "current.json がありません",
				FallbackReason::Corrupt => "current.json が壊れています",
				FallbackReason::MissingPython => {
					"current.json が指す python も、直前の環境の python もありません"
				}
			};
			tracing::warn!(
				reason,
				python = %python.display(),
				"paths.worker_python を使います"
			);
			PYTHON_ORIGIN_CONFIG
		}
	};
	(python, origin)
}

/// ワーカーの python とスクリプトの有無を確かめ、無ければエラーのログを残す。
///
/// - 起動は止めない (ワーカーの起動の失敗はプールが扱い、解決は Unavailable になる)
/// - python は yt-dlp (`yt-dlp[default,curl-cffi]`) を入れた venv の python.exe を指すこと。
///   リポジトリの `worker/.venv` は試験用で yt-dlp を入れていない
///
/// # 引数
/// - `config`: 設定
/// - `python`: 起動時に選んだ python.exe
/// - `origin`: `python` の出どころの説明 (`choose_python` の戻り値)
///
/// # 例
/// `check_worker_files(&config, &python, origin);`
fn check_worker_files(config: &Config, python: &Path, origin: &str) {
	tracing::info!(
		python = %python.display(),
		origin,
		"ワーカーの python。yt-dlp[default,curl-cffi] を入れた venv の python.exe を指すこと"
	);
	if !python.is_file() {
		tracing::error!(
			path = %python.display(),
			"{origin} がありません。yt-dlp[default,curl-cffi] を入れた venv の python.exe を指定してください"
		);
	}
	let script = &config.paths.worker_script;
	if !script.is_file() {
		tracing::error!(path = %script.display(), "paths.worker_script がありません");
	}
}
