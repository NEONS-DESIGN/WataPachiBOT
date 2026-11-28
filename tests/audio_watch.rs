//! 入力の読み取りの見張り (`ReadActivity`、`WatchedSource`) の試験。

use std::io::{Cursor, Read, Seek, SeekFrom};
use std::sync::mpsc;
use std::time::Duration;

use watapachi_bot::audio::watch::{ReadActivity, WatchedSource};

/// 止まっているとみなすまでの猶予 (試験用)。
const GRACE: Duration = Duration::from_millis(100);
/// 見張りが止まりを知らせるまでの上限 (試験用)。
const NOTICE_LIMIT: Duration = Duration::from_secs(5);

/// 合図が来るまで読み取りを返さない入力。取得の待ちで止まったミキサーの読み取りを真似る。
struct BlockingReader {
	release: mpsc::Receiver<()>,
}

impl Read for BlockingReader {
	fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
		let _ = self.release.recv();
		Ok(0)
	}
}

#[test]
fn idle_source_is_not_reading() {
	let activity = ReadActivity::new();
	let mut source = WatchedSource::new(Cursor::new(vec![1u8, 2, 3]), activity.clone());
	assert!(activity.reading_for().is_none());
	let mut out = Vec::new();
	source.read_to_end(&mut out).unwrap();
	assert_eq!(out, [1, 2, 3]);
	assert_eq!(source.seek(SeekFrom::Start(1)).unwrap(), 1);
	assert!(
		activity.reading_for().is_none(),
		"読み取りとシークの後は読み取り中でない"
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_read_is_reported_as_stalled() {
	let activity = ReadActivity::new();
	let (release, receiver) = mpsc::channel();
	let mut source = WatchedSource::new(BlockingReader { release: receiver }, activity.clone());
	let reader = std::thread::spawn(move || {
		let mut buf = [0u8; 16];
		source.read(&mut buf).unwrap()
	});
	tokio::time::timeout(NOTICE_LIMIT, activity.wait_stalled(GRACE))
		.await
		.expect("止まった読み取りを知らせること");
	assert!(activity
		.reading_for()
		.is_some_and(|reading| reading >= GRACE));
	release.send(()).unwrap();
	assert_eq!(reader.join().unwrap(), 0);
	assert!(
		activity.reading_for().is_none(),
		"読み取りが返った後は読み取り中でない"
	);
}

#[tokio::test(start_paused = true)]
async fn idle_source_never_reports_stall() {
	let activity = ReadActivity::new();
	let waited = tokio::time::timeout(Duration::from_secs(10), activity.wait_stalled(GRACE)).await;
	assert!(
		waited.is_err(),
		"読み取りの無い入力を止まっているとみなした"
	);
}
