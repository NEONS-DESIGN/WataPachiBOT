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
//! - ログの guard は実行の結果と一緒に返し、ランタイムを閉じた後に破棄する (`shutdown_runtime`)
//! - 子のモジュール: `.env` の読み取り (`env_file`)、ワーカーの起動指定と切り替え (`worker_launch`)、
//!   `update-worker` と更新役 (`update_cli`)、ランタイムの作成と片付け (`runtime`)
//!
//! 呼び出し方:
//! ```ignore
//! let runtime = app::build_runtime()?;
//! let finished = runtime.block_on(app::run(AppPaths::standard()));
//! app::shutdown_runtime(runtime, finished)?;
//! ```

mod env_file;
mod runtime;
mod update_cli;
mod worker_launch;

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context as _};
use arc_swap::ArcSwap;
use songbird::shards::TwilightMap;
use songbird::Songbird;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use twilight_gateway::{ConfigBuilder, Shard, ShardId};
use twilight_http::Client;

use crate::audio::access::VoiceAccess;
use crate::audio::backend::AudioBackend;
use crate::audio::chunked::{ChunkConfig, UrlRefresher};
use crate::audio::pipe::PipeSpec;
use crate::audio::songbird_backend::{InputFactory, SongbirdBackend};
use crate::config::Config;
use crate::db::{settings, Database};
use crate::gateway::dispatcher::{Dispatcher, DispatcherParts};
use crate::gateway::notifier::DiscordNotifier;
use crate::gateway::shard::{new_cache, presence, run_shard, ShardExit, INTENTS};
use crate::gateway::voice_access::DiscordVoiceAccess;
use crate::guild::notifier::Notifier;
use crate::guild::registry::{ActorDeps, GuildRegistry};
use crate::lang::GuildLangs;
use crate::logging::{self, LogGuard};
use crate::messages::MessageCatalog;
use crate::platform::job::Job;
use crate::platform::single_instance::{SingleInstance, SingleInstanceError, MUTEX_NAME};
use crate::platform::stop_request::{StopListener, STOP_EVENT_NAME};
use crate::resolver::cached::{CachedResolver, HttpStreamLength};
use crate::resolver::pool::{WorkerClient, WorkerPool};
use crate::resolver::Resolver;
use crate::scheduler::{self, SchedulerDeps};

pub use env_file::{parse_env_value, read_token};
pub use runtime::{build_runtime, shutdown_runtime, Finished};
pub use update_cli::{describe_outcome, update_worker, UPDATE_WORKER_BOT_RUNNING};
pub use worker_launch::{worker_spec, LiveSwitch};

use update_cli::build_updater;
use worker_launch::{check_worker_files, choose_python};

/// 現在のユーザー (Bot 自身) の取得を待つ上限。twilight-http の 1 要求の既定の上限 (10 秒) より長く取り、
/// HTTP クライアント側の失敗を先に受け取る。
const CURRENT_USER_TIMEOUT: Duration = Duration::from_secs(15);

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
/// # 戻り値
/// `run_with` と同じ
///
/// # エラー
/// `run_with` と同じ
///
/// # 例
/// `let finished = runtime.block_on(app::run(AppPaths::standard()));`
pub async fn run(paths: AppPaths) -> Finished<()> {
	run_with(paths, RunMode::Console).await
}

/// 決まった起動順で立ち上げ、終了の合図を待って終了順で止める。
///
/// - `.env`、設定、メッセージのどれかが不正なら、ログ・ミューテックス・ワーカーを作る前に止まる
/// - Shard が再接続しない形で終わったときも終了順で止め、エラーを返す
/// - ログの guard は結果と一緒に返す。ランタイムを閉じるまで残すため、`shutdown_runtime` に渡すこと
///
/// # 引数
/// - `paths`: 設定、メッセージ、`.env` の場所
/// - `mode`: コンソールかサービスか。終了の合図の待ち方が変わる
///
/// # 戻り値
/// 実行の結果とログの guard。Ctrl+C かサービスの停止で終えたら結果は `Ok(())`
///
/// # エラー
/// (`Finished::result` の Err)
/// - トークンが無い、設定かメッセージが不正、ログを初期化できない、既に起動している、
///   Job か DB を作れない、Discord の利用者情報を得られない
/// - Shard が再接続しない形で終わった (トークンの誤り、許されていない Intent など)
///
/// # 例
/// ```ignore
/// let finished = runtime.block_on(app::run_with(AppPaths::standard(), RunMode::Console));
/// ```
pub async fn run_with(paths: AppPaths, mode: RunMode) -> Finished<()> {
	let mut log_guard = None;
	let result = start_and_run(paths, mode, &mut log_guard).await;
	Finished { result, log_guard }
}

/// `run_with` の本体。ログを初期化したら guard を `log_guard` に置く。
///
/// # 引数
/// - `paths`: 設定、メッセージ、`.env` の場所
/// - `mode`: 終了の合図の待ち方
/// - `log_guard`: ログの guard の置き場所。ログの初期化の前に失敗したら None のまま
///
/// # エラー
/// `run_with` と同じ
///
/// # 例
/// `let result = start_and_run(paths, mode, &mut log_guard).await;`
async fn start_and_run(
	paths: AppPaths,
	mode: RunMode,
	log_guard: &mut Option<LogGuard>,
) -> anyhow::Result<()> {
	install_crypto_provider();

	let token = read_token(&paths.env)?;
	let config = load_config(&paths.config)?;
	let catalog = MessageCatalog::load(&paths.messages)
		.with_context(|| format!("メッセージを読めません: {}", paths.messages.display()))?;

	*log_guard = Some(init_logging(&config)?);
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
	// 型のまま渡す。main が二重起動を見分けて専用の終了コードを返すため
	let _instance = acquire_instance(|| anyhow::Error::new(SingleInstanceError::AlreadyRunning))?;
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
	let access = Arc::new(DiscordVoiceAccess::new(
		Arc::clone(&http),
		Arc::clone(&cache),
		bot_id,
		Duration::from_secs(config.voice.access_check_timeout_secs),
	));
	let registry = Arc::new(GuildRegistry::new(ActorDeps {
		audio: audio as Arc<dyn AudioBackend>,
		access: access as Arc<dyn VoiceAccess>,
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
	let updater = build_updater(config, template.clone(), Arc::clone(job));
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

/// rustls の暗号プロバイダを ring に決める。Discord や YouTube への通信の前に呼ぶ。
///
/// - 失敗は既に入っている (別の箇所が先に入れた) 場合だけのため無視する
///
/// # 例
/// `install_crypto_provider();`
fn install_crypto_provider() {
	let _ = rustls::crypto::ring::default_provider().install_default();
}

/// 設定ファイルを読んで検証する。失敗の文言に場所を添える。
///
/// # 引数
/// - `path`: config.toml のパス
///
/// # 戻り値
/// 検証済みの設定
///
/// # エラー
/// 読めない、TOML として解釈できない、値が不正
///
/// # 例
/// `let config = load_config(&paths.config)?;`
fn load_config(path: &Path) -> anyhow::Result<Config> {
	Config::load(path).with_context(|| format!("設定を読めません: {}", path.display()))
}

/// ログを初期化し、panic をログへ出すフックを入れる。
///
/// - 返した guard はランタイムを閉じるまで残すこと (`shutdown_runtime` で破棄する)
///
/// # 引数
/// - `config`: 設定 (水準、保持数、ログのディレクトリ)
///
/// # 戻り値
/// ログの guard
///
/// # エラー
/// ログを初期化できない (ファイルを作れない、水準が不正、既に初期化済み)
///
/// # 例
/// `*log_guard = Some(init_logging(&config)?);`
fn init_logging(config: &Config) -> anyhow::Result<LogGuard> {
	let guard = logging::init(&config.bot, &config.paths.logs)
		.with_context(|| format!("ログを初期化できません: {}", config.paths.logs.display()))?;
	logging::install_panic_hook();
	Ok(guard)
}

/// 二重起動の防止の mutex を取る。
///
/// - 既に起動していたときのエラーは `already_running` で作る (呼び出し側で文言や型を変えるため)
/// - それ以外の失敗は「二重起動の確認に失敗しました」を添えて返す
///
/// # 引数
/// - `already_running`: 既に起動していたときに返すエラーを作る
///
/// # 戻り値
/// 保持し続ける `SingleInstance`。破棄で mutex を手放す
///
/// # エラー
/// 既に起動している、mutex を作れない
///
/// # 例
/// `let _instance = acquire_instance(|| anyhow!(UPDATE_WORKER_BOT_RUNNING))?;`
fn acquire_instance(
	already_running: impl FnOnce() -> anyhow::Error,
) -> anyhow::Result<SingleInstance> {
	SingleInstance::acquire(MUTEX_NAME).map_err(|error| match error {
		SingleInstanceError::AlreadyRunning => already_running(),
		other => anyhow!(other).context("二重起動の確認に失敗しました"),
	})
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
