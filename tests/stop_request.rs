//! 外からの停止の命令の試験。Local 名前空間の名前を使い、特権なしで動かす。

use std::time::{Duration, Instant};

use watapachi_bot::platform::single_instance::SingleInstance;
use watapachi_bot::platform::stop_request::{
	request_stop, StopListener, StopOutcome, StopRequestError,
};

/// 命令の待ちが十分に長いことを確かめる上限。
const LONG_WAIT: Duration = Duration::from_secs(5);
/// 「戻らない」ことを確かめる待ち時間。
const QUIET_WAIT: Duration = Duration::from_millis(300);
/// 待ちのスレッドが終わってイベントが消えるまでの上限。
const RELEASE_WAIT: Duration = Duration::from_secs(3);

/// 試験ごとに重ならないイベント名とミューテックス名を作る。
///
/// # 引数
/// - `test`: 試験の名前
///
/// # 戻り値
/// (イベント名, ミューテックス名)
///
/// # 例
/// `let (event, mutex) = names("not_running");`
fn names(test: &str) -> (String, String) {
	let pid = std::process::id();
	(
		format!("Local\\WataPachiBOT-stop-test-{pid}-{test}-event"),
		format!("Local\\WataPachiBOT-stop-test-{pid}-{test}-mutex"),
	)
}

#[test]
fn not_running_without_event_and_mutex() {
	let (event, mutex) = names("not_running");
	let outcome = request_stop(&event, &mutex, LONG_WAIT).unwrap();
	assert_eq!(outcome, StopOutcome::NotRunning);
}

#[test]
fn unsupported_when_mutex_without_event() {
	let (event, mutex) = names("unsupported");
	let _instance = SingleInstance::acquire(&mutex).unwrap();
	assert!(matches!(
		request_stop(&event, &mutex, LONG_WAIT),
		Err(StopRequestError::Unsupported)
	));
}

#[tokio::test(flavor = "multi_thread")]
async fn stop_is_delivered_and_waits_for_exit() {
	let (event, mutex) = names("delivered");
	let instance = SingleInstance::acquire(&mutex).unwrap();
	let mut listener = StopListener::create(&event).unwrap();
	let (event_for_thread, mutex_for_thread) = (event.clone(), mutex.clone());
	let sender =
		std::thread::spawn(move || request_stop(&event_for_thread, &mutex_for_thread, LONG_WAIT));
	let received = tokio::time::timeout(LONG_WAIT, listener.requested())
		.await
		.unwrap();
	assert_eq!(received, Some(()));
	drop(instance);
	let outcome = tokio::task::spawn_blocking(move || sender.join().unwrap())
		.await
		.unwrap()
		.unwrap();
	assert_eq!(outcome, StopOutcome::Stopped);
}

#[tokio::test(flavor = "multi_thread")]
async fn second_stop_during_shutdown_waits_instead_of_unsupported() {
	let (event, mutex) = names("second");
	let _instance = SingleInstance::acquire(&mutex).unwrap();
	let mut listener = StopListener::create(&event).unwrap();
	let (event_for_thread, mutex_for_thread) = (event.clone(), mutex.clone());
	let first =
		std::thread::spawn(move || request_stop(&event_for_thread, &mutex_for_thread, QUIET_WAIT));
	let received = tokio::time::timeout(LONG_WAIT, listener.requested())
		.await
		.unwrap();
	assert_eq!(received, Some(()));
	let first = tokio::task::spawn_blocking(move || first.join().unwrap())
		.await
		.unwrap();
	assert!(matches!(first, Err(StopRequestError::Timeout)));
	// 命令を受けた後 (終了処理の間) も受け口が残っていれば、送り直しは終わりを待つ
	let second = tokio::task::spawn_blocking(move || request_stop(&event, &mutex, QUIET_WAIT))
		.await
		.unwrap();
	assert!(matches!(second, Err(StopRequestError::Timeout)));
}

#[test]
fn timeout_when_bot_does_not_exit() {
	let (event, mutex) = names("timeout");
	let _instance = SingleInstance::acquire(&mutex).unwrap();
	let _listener = StopListener::create(&event).unwrap();
	let started = Instant::now();
	assert!(matches!(
		request_stop(&event, &mutex, QUIET_WAIT),
		Err(StopRequestError::Timeout)
	));
	assert!(started.elapsed() >= QUIET_WAIT);
}

#[tokio::test(flavor = "multi_thread")]
async fn create_resets_leftover_signal() {
	let (event, mutex) = names("leftover");
	let _first = StopListener::create(&event).unwrap();
	// ミューテックスが無いため、イベントをシグナルにした直後に止まったとみなして戻る
	assert_eq!(
		request_stop(&event, &mutex, LONG_WAIT).unwrap(),
		StopOutcome::Stopped
	);
	let mut second = StopListener::create(&event).unwrap();
	let result = tokio::time::timeout(QUIET_WAIT, second.requested()).await;
	assert!(result.is_err(), "残っていたシグナルで止まった");
}

#[test]
fn drop_closes_event() {
	let (event, mutex) = names("drop");
	let listener = StopListener::create(&event).unwrap();
	drop(listener);
	let started = Instant::now();
	loop {
		match request_stop(&event, &mutex, LONG_WAIT).unwrap() {
			StopOutcome::NotRunning => break,
			StopOutcome::Stopped => {
				// 待ちのスレッドがまだハンドルを持っている。シグナルを受けて終わるまで待つ
				assert!(started.elapsed() < RELEASE_WAIT, "イベントが閉じられない");
				std::thread::sleep(Duration::from_millis(50));
			}
		}
	}
}
