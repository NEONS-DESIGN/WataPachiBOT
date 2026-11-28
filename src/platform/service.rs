//! Windows サービスの登録・解除・実行。
//! - `install`: 権利の付与 (SeServiceLogonRight)、登録 (自動・遅延開始)、回復の設定、PRESHUTDOWN の猶予
//! - `uninstall`: 動いていれば止め、停止を待ってから削除する
//! - `run_dispatcher`: サービス制御マネージャーから起動されたときの入口。ここから `app::run_with` を呼ぶ
//! - 作業ディレクトリは起動引数 `--dir` で渡す (サービスの既定は System32 のため)
//! - パスワードは登録の呼び出しにだけ使い、どこにも保存しない
//! - ログを初期化する前の失敗は `<dir>/service-error.log` に追記する
//!
//! 呼び出し方:
//! ```ignore
//! use watapachi_bot::platform::service::{install, InstallOptions};
//! install(&InstallOptions { exe, dir, account: r".\user".into(), password })?;
//! ```

use std::ffi::OsString;
use std::fs::OpenOptions;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::ptr;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use windows_service::service::{
	Service, ServiceAccess, ServiceAction, ServiceActionType, ServiceControl, ServiceControlAccept,
	ServiceErrorControl, ServiceExitCode, ServiceFailureActions, ServiceFailureResetPeriod,
	ServiceInfo, ServiceStartType, ServiceState, ServiceStatus, ServiceType,
};
use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};
use windows_service::{define_windows_service, service_dispatcher};
use windows_sys::Win32::Foundation::{
	ERROR_FAILED_SERVICE_CONTROLLER_CONNECT, ERROR_INSUFFICIENT_BUFFER,
	ERROR_SERVICE_DOES_NOT_EXIST, ERROR_SERVICE_NOT_ACTIVE, STATUS_SUCCESS,
};
use windows_sys::Win32::Security::Authentication::Identity::{
	LsaAddAccountRights, LsaClose, LsaNtStatusToWinError, LsaOpenPolicy, LSA_HANDLE,
	LSA_OBJECT_ATTRIBUTES, LSA_UNICODE_STRING, POLICY_CREATE_ACCOUNT, POLICY_LOOKUP_NAMES,
};
use windows_sys::Win32::Security::{LookupAccountNameW, SID_NAME_USE};
use windows_sys::Win32::System::SystemInformation::GetLocalTime;

use crate::app::{self, AppPaths, RunMode};
use crate::config::{Config, ConfigError};

/// サービス名。`sc start` などで使う。
pub const SERVICE_NAME: &str = "WataPachiBOT";
/// サービスの表示名。
pub const SERVICE_DISPLAY_NAME: &str = "WataPachiBOT";
/// サービスの起動引数の区切り。`service run --dir <パス>`
pub const SERVICE_RUN_ARGS: [&str; 2] = ["service", "run"];
/// 作業ディレクトリを渡す引数名。
pub const DIR_FLAG: &str = "--dir";
/// `app::run_with` がエラーで終わったときにサービス固有の終了コードとして報告する値。
pub const SERVICE_EXIT_FAILED: u32 = 1;

/// 初期化前の失敗を残すファイル名 (作業ディレクトリ直下)。
const ERROR_LOG_FILE: &str = "service-error.log";
/// 停止の待ち時間の目安に足す余裕 (秒)。終了処理の上限の後のランタイムの片付けなどに充てる。
/// `watapachi-bot stop` の待ちの上限にも使う。再起動の前のスクリプトは、予告の 60 秒のうち 30 秒の時点で
/// 送るため、上限 20 秒と合わせて 60 秒に収まる値にする
const STOP_WAIT_MARGIN_SECS: u64 = 5;
/// 回復の設定: 1 回目、2 回目、3 回目以降の再起動までの待ち (秒)。
const RESTART_DELAYS_SECS: [u64; 3] = [10, 60, 300];
/// 回復の設定: 失敗の回数を 0 に戻すまでの時間 (秒)。
const FAILURE_RESET_SECS: u64 = 86_400;
/// 起動中の報告に付ける待ち時間の目安。
const START_WAIT_HINT: Duration = Duration::from_secs(30);
/// 停止の完了を調べる間隔。
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// 付与する権利の名前 (`SE_SERVICE_LOGON_NAME`)。
const LSA_RIGHT_SERVICE_LOGON: &str = "SeServiceLogonRight";

/// サービスの操作で起きるエラー。
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
	/// サービス制御の API の失敗。
	#[error("サービス制御の操作に失敗しました: {0}")]
	Windows(#[from] windows_service::Error),
	/// ファイルなどの入出力の失敗。
	#[error("入出力に失敗しました: {0}")]
	Io(#[from] io::Error),
	/// 既に同名のサービスがある。上書きしない。
	#[error("サービス {SERVICE_NAME} は既に登録されています。先に `service uninstall` を実行してください")]
	AlreadyInstalled,
	/// 解除や停止の対象のサービスが無い。
	#[error("サービス {SERVICE_NAME} は登録されていません")]
	NotInstalled,
	/// 作業ディレクトリに config.toml が無い。
	#[error("作業ディレクトリに config.toml がありません: {0}")]
	ConfigMissing(PathBuf),
	/// 作業ディレクトリの config.toml を読めない、または値が不正。登録しない。
	/// 詳細は原因 (`source`) に持つ。`{:#}` で表示すると続けて出る
	#[error("config.toml に誤りがあるため登録しません")]
	Config(#[from] ConfigError),
	/// 権利の付与 (LSA) の失敗。値は Win32 のエラーコード。
	#[error("ログオンの権利を付与できません (Win32 エラー {0})")]
	Lsa(u32),
	/// 停止を待つ上限を超えた。
	#[error("サービスが指定の時間内に停止しませんでした")]
	StopTimeout,
}

impl ServiceError {
	/// サービス制御マネージャーから起動されていないことを示すエラーか。
	///
	/// # 戻り値
	/// `service run` を手で実行したとき (`StartServiceCtrlDispatcher` の接続の失敗) は true
	///
	/// # 例
	/// `if error.is_not_started_as_service() { /* 案内を出す */ }`
	pub fn is_not_started_as_service(&self) -> bool {
		self.win32_code() == Some(ERROR_FAILED_SERVICE_CONTROLLER_CONNECT)
	}

	/// 元の Win32 のエラーコードを取り出す。
	///
	/// # 戻り値
	/// OS のエラーに由来するなら Some
	fn win32_code(&self) -> Option<u32> {
		match self {
			ServiceError::Windows(windows_service::Error::Winapi(error)) => error
				.raw_os_error()
				.and_then(|code| u32::try_from(code).ok()),
			_ => None,
		}
	}
}

/// 登録に使う値。パスワードは保存しない。
pub struct InstallOptions {
	/// 実行ファイルの絶対パス
	pub exe: PathBuf,
	/// 作業ディレクトリ (config.toml があること)
	pub dir: PathBuf,
	/// 実行するアカウント (`.\<ユーザー名>`)
	pub account: String,
	/// アカウントのパスワード。登録の呼び出しにだけ使う
	pub password: String,
}

/// 登録する作業ディレクトリの config.toml を読み、検証する。
///
/// - `install` が登録の前に呼ぶ。`service install` もパスワードを尋ねる前に呼び、誤りを先に知らせる
/// - 読めた設定の `shutdown.timeout_secs` を PRESHUTDOWN の猶予に使う
///
/// # 引数
/// - `dir`: 作業ディレクトリ
///
/// # 戻り値
/// 検証済みの設定
///
/// # エラー
/// - `ConfigMissing`: `dir` に config.toml が無い
/// - `Config`: 読めない、TOML として解釈できない、値が不正
///
/// # 例
/// `let config = load_install_config(&dir)?;`
pub fn load_install_config(dir: &Path) -> Result<Config, ServiceError> {
	let config_path = dir.join(app::CONFIG_PATH);
	if !config_path.is_file() {
		return Err(ServiceError::ConfigMissing(config_path));
	}
	Ok(Config::load(&config_path)?)
}

/// 登録するコマンドラインの引数を作る。
///
/// # 引数
/// - `dir`: 作業ディレクトリ
///
/// # 戻り値
/// `["service", "run", "--dir", <dir>]`
///
/// # 例
/// `let args = launch_arguments(Path::new(r"C:\bot"));`
pub fn launch_arguments(dir: &Path) -> Vec<OsString> {
	let mut args: Vec<OsString> = SERVICE_RUN_ARGS.iter().map(OsString::from).collect();
	args.push(OsString::from(DIR_FLAG));
	args.push(dir.as_os_str().to_owned());
	args
}

/// `service run` の引数から作業ディレクトリを読む。
///
/// # 引数
/// - `args`: 引数の並び (`--dir <パス>` を含む)
///
/// # 戻り値
/// `--dir` の次の値。無い、または値が欠けていれば None
///
/// # 例
/// `let dir = parse_dir(&launch_arguments(Path::new(r"C:\bot")));`
pub fn parse_dir(args: &[OsString]) -> Option<PathBuf> {
	let position = args.iter().position(|arg| arg == DIR_FLAG)?;
	args.get(position + 1).map(PathBuf::from)
}

/// 回復の設定を作る。
///
/// # 戻り値
/// 10 秒、60 秒、300 秒で再起動し、失敗の回数は 86400 秒で戻す設定
///
/// # 例
/// `service.update_failure_actions(failure_actions())?;`
pub fn failure_actions() -> ServiceFailureActions {
	let actions = RESTART_DELAYS_SECS
		.iter()
		.map(|secs| ServiceAction {
			action_type: ServiceActionType::Restart,
			delay: Duration::from_secs(*secs),
		})
		.collect();
	ServiceFailureActions {
		reset_period: ServiceFailureResetPeriod::After(Duration::from_secs(FAILURE_RESET_SECS)),
		reboot_msg: None,
		command: None,
		actions: Some(actions),
	}
}

/// 停止中の報告に付ける待ち時間の目安を求める。`watapachi-bot stop` の待ちの上限にも使う。
///
/// # 引数
/// - `shutdown_timeout_secs`: `shutdown.timeout_secs`
///
/// # 戻り値
/// `shutdown_timeout_secs` に余裕の 5 秒を足した時間
///
/// # 例
/// `assert_eq!(stop_wait_hint(20), Duration::from_secs(25));`
pub fn stop_wait_hint(shutdown_timeout_secs: u64) -> Duration {
	Duration::from_secs(shutdown_timeout_secs.saturating_add(STOP_WAIT_MARGIN_SECS))
}

/// サービスとして登録する。
///
/// - 先に `config.toml` (`load_install_config` で読み、検証する) と既存の登録を確かめ、権利を付与し、登録する
/// - PRESHUTDOWN の猶予は、読めた設定の `shutdown.timeout_secs` から求める
/// - 登録の後の設定 (遅延開始、回復、PRESHUTDOWN) が失敗したら、登録を削除して戻す
///
/// # 引数
/// - `options`: 実行ファイル、作業ディレクトリ、アカウント、パスワード
///
/// # エラー
/// - `ConfigMissing`: `dir` に config.toml が無い
/// - `Config`: config.toml を読めない、または値が不正
/// - `AlreadyInstalled`: 既に登録済み
/// - `Lsa`: 権利を付与できない
/// - `Windows`: 管理者権限が無い、アカウントかパスワードの誤りなど
///
/// # 例
/// `install(&options)?;`
pub fn install(options: &InstallOptions) -> Result<(), ServiceError> {
	let config = load_install_config(&options.dir)?;
	let manager = ServiceManager::local_computer(
		None::<&str>,
		ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
	)?;
	match manager.open_service(SERVICE_NAME, ServiceAccess::QUERY_STATUS) {
		Ok(_) => return Err(ServiceError::AlreadyInstalled),
		Err(error) if is_win32(&error, ERROR_SERVICE_DOES_NOT_EXIST) => {}
		Err(error) => return Err(error.into()),
	}
	grant_service_logon_right(&options.account)?;

	let info = ServiceInfo {
		name: OsString::from(SERVICE_NAME),
		display_name: OsString::from(SERVICE_DISPLAY_NAME),
		service_type: ServiceType::OWN_PROCESS,
		start_type: ServiceStartType::AutoStart,
		error_control: ServiceErrorControl::Normal,
		executable_path: options.exe.clone(),
		launch_arguments: launch_arguments(&options.dir),
		dependencies: vec![],
		account_name: Some(OsString::from(&options.account)),
		account_password: Some(OsString::from(&options.password)),
	};
	let service = manager.create_service(
		&info,
		ServiceAccess::CHANGE_CONFIG | ServiceAccess::DELETE | ServiceAccess::QUERY_STATUS,
	)?;
	if let Err(error) = configure(&service, config.shutdown.timeout_secs) {
		// 設定が途中の登録を残さない。削除の失敗は元のエラーを優先する
		let _ = service.delete();
		return Err(error.into());
	}
	Ok(())
}

/// 登録直後のサービスに、遅延開始、回復、PRESHUTDOWN の猶予を設定する。
///
/// # 引数
/// - `service`: 登録したサービス (`CHANGE_CONFIG` の権限を持つこと)
/// - `shutdown_timeout_secs`: `shutdown.timeout_secs`
///
/// # エラー
/// いずれかの設定の API が失敗した場合
///
/// # 例
/// `configure(&service, 20)?;`
fn configure(service: &Service, shutdown_timeout_secs: u64) -> windows_service::Result<()> {
	service.set_delayed_auto_start(true)?;
	service.update_failure_actions(failure_actions())?;
	service.set_failure_actions_on_non_crash_failures(true)?;
	service.set_preshutdown_timeout(stop_wait_hint(shutdown_timeout_secs))?;
	Ok(())
}

/// サービスを解除する。動いていれば止め、`wait` まで停止を待ってから削除する。
///
/// # 引数
/// - `wait`: 停止を待つ上限
///
/// # エラー
/// - `NotInstalled`: 登録されていない
/// - `StopTimeout`: `wait` の間に停止しなかった (削除はしない)
/// - `Windows`: 管理者権限が無いなど
///
/// # 例
/// `uninstall(Duration::from_secs(30))?;`
pub fn uninstall(wait: Duration) -> Result<(), ServiceError> {
	let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)?;
	let service = match manager.open_service(
		SERVICE_NAME,
		ServiceAccess::QUERY_STATUS | ServiceAccess::STOP | ServiceAccess::DELETE,
	) {
		Ok(service) => service,
		Err(error) if is_win32(&error, ERROR_SERVICE_DOES_NOT_EXIST) => {
			return Err(ServiceError::NotInstalled)
		}
		Err(error) => return Err(error.into()),
	};

	if service.query_status()?.current_state != ServiceState::Stopped {
		match service.stop() {
			Ok(_) => {}
			// 確認の後に止まった場合
			Err(error) if is_win32(&error, ERROR_SERVICE_NOT_ACTIVE) => {}
			Err(error) => return Err(error.into()),
		}
		let started = Instant::now();
		while service.query_status()?.current_state != ServiceState::Stopped {
			if started.elapsed() >= wait {
				return Err(ServiceError::StopTimeout);
			}
			std::thread::sleep(STOP_POLL_INTERVAL);
		}
	}
	service.delete()?;
	Ok(())
}

/// Windows のエラーコードが一致するか。
///
/// # 引数
/// - `error`: windows-service のエラー
/// - `code`: 比べる Win32 のエラーコード
///
/// # 戻り値
/// OS のエラーで、コードが一致すれば true
fn is_win32(error: &windows_service::Error, code: u32) -> bool {
	match error {
		windows_service::Error::Winapi(inner) => {
			inner.raw_os_error().and_then(|raw| u32::try_from(raw).ok()) == Some(code)
		}
		_ => false,
	}
}

/// アカウントに SeServiceLogonRight を与える。既にあっても成功する。
///
/// - `LookupAccountNameW` で SID を得て、`LsaOpenPolicy`、`LsaAddAccountRights`、`LsaClose` の順に呼ぶ
///
/// # 引数
/// - `account`: `.\ユーザー名` や `ドメイン\ユーザー名`
///
/// # エラー
/// - `Io`: アカウントの名前を解決できない
/// - `Lsa`: ポリシーを開けない、権利を追加できない (管理者権限が無いなど)
///
/// # 例
/// `grant_service_logon_right(r".\user")?;`
pub fn grant_service_logon_right(account: &str) -> Result<(), ServiceError> {
	let mut sid = lookup_account_sid(account)?;

	let attributes = LSA_OBJECT_ATTRIBUTES::default();
	let mut policy: LSA_HANDLE = 0;
	// SAFETY: attributes は初期化済みで、policy は書き込み先として有効。システム名は null (ローカル)
	let status = unsafe {
		LsaOpenPolicy(
			ptr::null(),
			&attributes,
			(POLICY_CREATE_ACCOUNT | POLICY_LOOKUP_NAMES) as u32,
			&mut policy,
		)
	};
	if status != STATUS_SUCCESS {
		return Err(ServiceError::Lsa(lsa_error(status)));
	}

	let mut right: Vec<u16> = LSA_RIGHT_SERVICE_LOGON.encode_utf16().collect();
	// 権利名は固定の短い文字列のため u16 の長さに収まる。Length はバイト数
	let byte_len = (right.len() * 2) as u16;
	let right_string = LSA_UNICODE_STRING {
		Length: byte_len,
		MaximumLength: byte_len,
		Buffer: right.as_mut_ptr(),
	};
	// SAFETY: policy は開いたハンドル、sid は LookupAccountNameW が埋めた有効な SID、
	// right_string は right を指し呼び出しの間生きている
	let status = unsafe { LsaAddAccountRights(policy, sid.as_mut_ptr().cast(), &right_string, 1) };
	// SAFETY: policy は LsaOpenPolicy が返した有効なハンドルで、ここで 1 回だけ閉じる
	unsafe { LsaClose(policy) };
	if status != STATUS_SUCCESS {
		return Err(ServiceError::Lsa(lsa_error(status)));
	}
	Ok(())
}

/// NTSTATUS を Win32 のエラーコードへ変換する。
///
/// # 引数
/// - `status`: LSA の関数が返した NTSTATUS
///
/// # 戻り値
/// Win32 のエラーコード
fn lsa_error(status: i32) -> u32 {
	// SAFETY: 引数は値渡しで、ポインタを扱わない
	unsafe { LsaNtStatusToWinError(status) }
}

/// アカウント名から SID のバイト列を得る。
///
/// # 引数
/// - `account`: アカウント名
///
/// # 戻り値
/// SID を収めたバッファ
///
/// # エラー
/// 名前を解決できない場合は `Io` (OS のエラー)
fn lookup_account_sid(account: &str) -> Result<Vec<u8>, ServiceError> {
	let wide: Vec<u16> = account.encode_utf16().chain(std::iter::once(0)).collect();
	let mut sid_len: u32 = 0;
	let mut domain_len: u32 = 0;
	let mut use_kind: SID_NAME_USE = 0;
	// SAFETY: 1 回目は大きさを知るための呼び出し。SID とドメインのバッファは null で、長さは 0
	let first = unsafe {
		LookupAccountNameW(
			ptr::null(),
			wide.as_ptr(),
			ptr::null_mut(),
			&mut sid_len,
			ptr::null_mut(),
			&mut domain_len,
			&mut use_kind,
		)
	};
	if first != 0 {
		return Err(ServiceError::Io(io::Error::other(
			"SID の長さを得られません",
		)));
	}
	let error = io::Error::last_os_error();
	if error.raw_os_error().and_then(|raw| u32::try_from(raw).ok())
		!= Some(ERROR_INSUFFICIENT_BUFFER)
	{
		return Err(ServiceError::Io(error));
	}

	let mut sid = vec![0u8; sid_len as usize];
	let mut domain = vec![0u16; domain_len as usize];
	// SAFETY: sid と domain は 1 回目で得た長さの書き込み先。wide は NUL 終端
	let second = unsafe {
		LookupAccountNameW(
			ptr::null(),
			wide.as_ptr(),
			sid.as_mut_ptr().cast(),
			&mut sid_len,
			domain.as_mut_ptr(),
			&mut domain_len,
			&mut use_kind,
		)
	};
	if second == 0 {
		return Err(ServiceError::Io(io::Error::last_os_error()));
	}
	Ok(sid)
}

define_windows_service!(ffi_service_main, service_main);

/// サービス制御マネージャーから呼ばれたときの入口。接続して本体 (`service_main`) を動かす。
///
/// - 呼び出したスレッドは、サービスが `Stopped` を報告するまで戻らない
///
/// # エラー
/// 制御マネージャーに接続できない場合。手で実行したときは
/// `ServiceError::is_not_started_as_service` が true になる
///
/// # 例
/// `run_dispatcher()?;`
pub fn run_dispatcher() -> Result<(), ServiceError> {
	service_dispatcher::start(SERVICE_NAME, ffi_service_main)?;
	Ok(())
}

/// ディスパッチャーから呼ばれる本体。エラーは `service-error.log` に残す。
///
/// # 引数
/// - `_arguments`: サービスの開始引数 (使わない。作業ディレクトリはプロセスの引数で渡す)
fn service_main(_arguments: Vec<OsString>) {
	let process_args: Vec<OsString> = std::env::args_os().skip(1).collect();
	let dir = parse_dir(&process_args);
	if let Err(message) = run_service(dir.as_deref()) {
		log_error(dir.as_deref(), &message);
	}
}

/// 状態の報告、ランタイムの用意、`app::run_with` の実行、終了の報告を行う。
///
/// # 引数
/// - `dir`: 作業ディレクトリ。None なら起動引数の不足
///
/// # 戻り値
/// 初期化前の失敗なら説明文。`app::run_with` のエラーも文言にして返す (ログにも出ているが原因の控えにする)
///
/// # 例
/// `run_service(Some(Path::new(r"C:\bot")))`
fn run_service(dir: Option<&Path>) -> Result<(), String> {
	let (stop_tx, stop_rx) = watch::channel(false);
	let handler_tx = stop_tx.clone();
	let status_handle =
		service_control_handler::register(SERVICE_NAME, move |control| match control {
			ServiceControl::Stop | ServiceControl::Preshutdown => {
				let _ = handler_tx.send(true);
				ServiceControlHandlerResult::NoError
			}
			ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
			_ => ServiceControlHandlerResult::NotImplemented,
		})
		.map_err(|error| format!("制御ハンドラーを登録できません: {error}"))?;

	let report = |state: ServiceState,
	              accepted: ServiceControlAccept,
	              exit_code: ServiceExitCode,
	              wait_hint: Duration| {
		let checkpoint = u32::from(matches!(
			state,
			ServiceState::StartPending | ServiceState::StopPending
		));
		let result = status_handle.set_service_status(ServiceStatus {
			service_type: ServiceType::OWN_PROCESS,
			current_state: state,
			controls_accepted: accepted,
			exit_code,
			checkpoint,
			wait_hint,
			process_id: None,
		});
		if let Err(error) = result {
			log_error(dir, &format!("状態を報告できません: {error}"));
		}
	};
	report(
		ServiceState::StartPending,
		ServiceControlAccept::empty(),
		ServiceExitCode::NO_ERROR,
		START_WAIT_HINT,
	);

	let outcome = prepare_and_run(dir, stop_rx, &report);
	let exit_code = match &outcome {
		Ok(()) => ServiceExitCode::NO_ERROR,
		Err(_) => ServiceExitCode::ServiceSpecific(SERVICE_EXIT_FAILED),
	};
	report(
		ServiceState::Stopped,
		ServiceControlAccept::empty(),
		exit_code,
		Duration::default(),
	);
	// 合図の送り手はここまで生かす (先に破棄すると本体が停止として扱う)
	drop(stop_tx);
	outcome
}

/// 作業ディレクトリへ移り、ランタイムを作って `app::run_with` を最後まで動かす。
///
/// # 引数
/// - `dir`: 作業ディレクトリ
/// - `stop`: 停止の合図の受け口
/// - `report`: 状態の報告 (状態、受け付ける制御、終了コード、待ち時間の目安)
///
/// # 戻り値
/// 失敗の説明文。`Running` の報告は準備が終わってから行う
///
/// # 例
/// `prepare_and_run(Some(dir), stop_rx, &report)?;`
fn prepare_and_run(
	dir: Option<&Path>,
	stop: watch::Receiver<bool>,
	report: &dyn Fn(ServiceState, ServiceControlAccept, ServiceExitCode, Duration),
) -> Result<(), String> {
	let dir = dir.ok_or_else(|| format!("起動引数に {DIR_FLAG} がありません"))?;
	std::env::set_current_dir(dir)
		.map_err(|error| format!("作業ディレクトリへ移れません ({}): {error}", dir.display()))?;
	let paths = AppPaths::standard();
	let config = Config::load(&paths.config)
		.map_err(|error| format!("設定を読めません ({}): {error}", paths.config.display()))?;
	let hint = stop_wait_hint(config.shutdown.timeout_secs);

	let runtime =
		app::build_runtime().map_err(|error| format!("非同期ランタイムを作れません: {error}"))?;

	let accepted = ServiceControlAccept::STOP | ServiceControlAccept::PRESHUTDOWN;
	report(
		ServiceState::Running,
		accepted,
		ServiceExitCode::NO_ERROR,
		Duration::default(),
	);

	// 停止の合図を受けたら、終了処理の間は StopPending と待ち時間の目安を報告する
	let mut stop_watch = stop.clone();
	let finished = runtime.block_on(async {
		let reporter = async {
			if stop_watch.wait_for(|stopped| *stopped).await.is_ok() {
				report(
					ServiceState::StopPending,
					ServiceControlAccept::empty(),
					ServiceExitCode::NO_ERROR,
					hint,
				);
			}
			std::future::pending::<std::convert::Infallible>().await
		};
		tokio::select! {
			finished = app::run_with(paths, RunMode::Service { stop }) => finished,
			never = reporter => match never {},
		}
	});
	// ランタイムを閉じてからログの guard を破棄する
	app::shutdown_runtime(runtime, finished).map_err(|error| format!("{error:#}"))
}

/// `<dir>/service-error.log` に日時付きで 1 行追記する。dir が無ければ実行ファイルのディレクトリ。
///
/// - 書けなくても何もしない (報告先が他に無いため)
///
/// # 引数
/// - `dir`: 作業ディレクトリ (あれば)
/// - `message`: 失敗の説明
///
/// # 例
/// `log_error(Some(Path::new(r"C:\bot")), "設定を読めません");`
fn log_error(dir: Option<&Path>, message: &str) {
	let base = dir.map(Path::to_path_buf).or_else(|| {
		std::env::current_exe()
			.ok()
			.and_then(|exe| exe.parent().map(Path::to_path_buf))
	});
	let Some(base) = base else { return };
	let line = format!("{} {message}\r\n", local_timestamp());
	if let Ok(mut file) = OpenOptions::new()
		.create(true)
		.append(true)
		.open(base.join(ERROR_LOG_FILE))
	{
		let _ = file.write_all(line.as_bytes());
	}
}

/// 現在のローカル時刻を `YYYY-MM-DD HH:MM:SS` で返す。
///
/// # 戻り値
/// 日時の文字列
fn local_timestamp() -> String {
	// SAFETY: GetLocalTime は渡した構造体へ書き込むだけ。0 で初期化した値は有効
	let time = unsafe {
		let mut time = std::mem::zeroed();
		GetLocalTime(&mut time);
		time
	};
	format!(
		"{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
		time.wYear, time.wMonth, time.wDay, time.wHour, time.wMinute, time.wSecond
	)
}
