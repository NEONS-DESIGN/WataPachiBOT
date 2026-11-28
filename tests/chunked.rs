//! ChunkedStream / ChunkedRequest の試験。ローカルの Range サーバーを使い、外部のネットワークは使わない。
//!
//! - 時間の判定は余裕を大きく取る (負荷の高い PC でも通るように)

#[path = "support/http_client.rs"]
mod http_client;
#[path = "support/range_server.rs"]
mod range_server;

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use songbird::input::{AsyncAdapterStream, Compose};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use watapachi_bot::audio::chunked::{
	ChunkConfig, ChunkError, ChunkedRequest, ChunkedStream, UrlRefresher, EXPIRY_RECHECK_INTERVAL,
};
use watapachi_bot::domain::{ResolveError, ResolvedStream, VideoId};

use range_server::RangeServer;

/// 試験で使う format_id。
const FORMAT_ID: &str = "251";

/// 位置ごとに値が変わる試験データ。
fn data(len: usize) -> Vec<u8> {
	(0..len).map(|i| ((i * 31 + i / 251) % 251) as u8).collect()
}

fn now() -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap()
		.as_secs() as i64
}

fn resolved(url: String, filesize: u64, expires_at: Option<i64>) -> ResolvedStream {
	ResolvedStream {
		video_id: VideoId("dQw4w9WgXcQ".to_owned()),
		title: "title".to_owned(),
		duration_s: Some(10),
		url,
		http_headers: BTreeMap::from([("User-Agent".to_owned(), "test".to_owned())]),
		filesize,
		format_id: FORMAT_ID.to_owned(),
		expires_at,
		available_at: None,
		is_live: false,
		protocol: "https".to_owned(),
	}
}

fn config(chunk_size: u64) -> ChunkConfig {
	ChunkConfig {
		chunk_size,
		initial_chunk_size: 0,
		fetch_timeout: Duration::from_secs(5),
		refresh_timeout: Duration::from_secs(5),
		attempts: 3,
		refresh_margin: Duration::from_secs(300),
		available_wait_max: Duration::from_secs(60),
		expiry_recheck_interval: EXPIRY_RECHECK_INTERVAL,
	}
}

/// 決まった結果を返す偽の UrlRefresher。呼ばれた回数と引数を記録する。
struct FakeRefresher {
	next: Mutex<Option<ResolvedStream>>,
	calls: AtomicU32,
	format_ids: Mutex<Vec<String>>,
	delay: Duration,
}

impl FakeRefresher {
	fn new(next: Option<ResolvedStream>) -> Arc<Self> {
		Self::slow(next, Duration::ZERO)
	}

	/// 応答の前に `delay` 待つ偽実装。
	fn slow(next: Option<ResolvedStream>, delay: Duration) -> Arc<Self> {
		Arc::new(Self {
			next: Mutex::new(next),
			calls: AtomicU32::new(0),
			format_ids: Mutex::new(Vec::new()),
			delay,
		})
	}

	fn calls(&self) -> u32 {
		self.calls.load(Ordering::SeqCst)
	}
}

#[async_trait]
impl UrlRefresher for FakeRefresher {
	async fn refresh(
		&self,
		_video_id: &VideoId,
		format_id: &str,
	) -> Result<ResolvedStream, ResolveError> {
		self.calls.fetch_add(1, Ordering::SeqCst);
		self.format_ids.lock().unwrap().push(format_id.to_owned());
		if !self.delay.is_zero() {
			tokio::time::sleep(self.delay).await;
		}
		self.next
			.lock()
			.unwrap()
			.clone()
			.ok_or(ResolveError::Unavailable)
	}
}

fn open(
	stream: ResolvedStream,
	refresher: Arc<FakeRefresher>,
	config: ChunkConfig,
) -> ChunkedStream {
	ChunkedRequest::new(http_client::new_client(), stream, refresher, config).into_stream()
}

/// ChunkError を取り出す。
fn chunk_error(error: &std::io::Error) -> Option<&ChunkError> {
	error.get_ref().and_then(|e| e.downcast_ref::<ChunkError>())
}

/// 条件が成り立つまで最大 `limit` 待つ。
async fn wait_until(limit: Duration, mut condition: impl FnMut() -> bool) -> bool {
	let began = Instant::now();
	while began.elapsed() < limit {
		if condition() {
			return true;
		}
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	condition()
}

fn sorted(mut ranges: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
	ranges.sort();
	ranges
}

#[tokio::test]
async fn reads_whole_file_byte_exact() {
	let bytes = data(10_500);
	let server = RangeServer::start(bytes.clone()).await;
	let refresher = FakeRefresher::new(None);
	let mut stream = open(
		resolved(server.url("/a"), bytes.len() as u64, None),
		Arc::clone(&refresher),
		config(1000),
	);
	let mut out = Vec::new();
	stream.read_to_end(&mut out).await.unwrap();
	assert_eq!(out, bytes);
	let expected: Vec<(u64, u64)> = (0..11)
		.map(|i| (i * 1000, (i * 1000 + 999).min(10_499)))
		.collect();
	assert_eq!(sorted(server.ranges()), expected);
	assert_eq!(refresher.calls(), 0);
}

#[tokio::test]
async fn size_multiple_of_chunk_has_no_extra_request() {
	let bytes = data(3000);
	let server = RangeServer::start(bytes.clone()).await;
	let mut stream = open(
		resolved(server.url("/a"), 3000, None),
		FakeRefresher::new(None),
		config(1000),
	);
	let mut out = Vec::new();
	stream.read_to_end(&mut out).await.unwrap();
	assert_eq!(out, bytes);
	tokio::time::sleep(Duration::from_millis(200)).await;
	assert_eq!(
		sorted(server.ranges()),
		vec![(0, 999), (1000, 1999), (2000, 2999)]
	);
}

#[tokio::test]
async fn one_byte_file_reads() {
	let server = RangeServer::start(vec![42]).await;
	let mut stream = open(
		resolved(server.url("/a"), 1, None),
		FakeRefresher::new(None),
		config(1000),
	);
	let mut out = Vec::new();
	stream.read_to_end(&mut out).await.unwrap();
	assert_eq!(out, vec![42]);
	assert_eq!(server.ranges(), vec![(0, 0)]);
}

#[tokio::test]
async fn seek_forward_requests_only_target_chunk() {
	let bytes = data(10_000);
	let server = RangeServer::start(bytes.clone()).await;
	let mut stream = open(
		resolved(server.url("/a"), 10_000, None),
		FakeRefresher::new(None),
		config(1000),
	);
	let position = stream.seek(SeekFrom::Start(5500)).await.unwrap();
	assert_eq!(position, 5500);
	let mut out = vec![0u8; 100];
	stream.read_exact(&mut out).await.unwrap();
	assert_eq!(out, bytes[5500..5600]);
	let ranges = server.ranges();
	assert_eq!(ranges.first(), Some(&(5000, 5999)));
	assert!(
		ranges.iter().all(|(start, _)| *start >= 5000),
		"手前の範囲を要求した: {ranges:?}"
	);
}

/// 読み手が待つ取得は `initial_chunk_size`、先読みは `chunk_size` で取ること。
///
/// - 曲の始めは小さく取り、続きは小さな範囲の終わりから大きく取る (範囲が重ならず、取り直しも無い)
#[tokio::test]
async fn initial_chunk_is_small_and_prefetch_continues_from_its_end() {
	let bytes = data(5000);
	let server = RangeServer::start(bytes.clone()).await;
	let mut stream = open(
		resolved(server.url("/a"), 5000, None),
		FakeRefresher::new(None),
		ChunkConfig {
			initial_chunk_size: 250,
			..config(1000)
		},
	);
	let mut out = Vec::new();
	stream.read_to_end(&mut out).await.unwrap();
	assert_eq!(out, bytes);
	let ranges = server.ranges();
	assert_eq!(ranges.first(), Some(&(0, 249)), "最初の取得が小さくない");
	assert_eq!(
		sorted(ranges),
		vec![
			(0, 249),
			(250, 1249),
			(1250, 2249),
			(2250, 3249),
			(3250, 4249),
			(4250, 4999)
		]
	);
}

/// シークの直後の取得も `initial_chunk_size` の境界で小さく取ること。
#[tokio::test]
async fn seek_fetches_small_range_at_initial_boundary() {
	let bytes = data(10_000);
	let server = RangeServer::start(bytes.clone()).await;
	let mut stream = open(
		resolved(server.url("/a"), 10_000, None),
		FakeRefresher::new(None),
		ChunkConfig {
			initial_chunk_size: 250,
			..config(1000)
		},
	);
	stream.seek(SeekFrom::Start(5600)).await.unwrap();
	let mut out = vec![0u8; 100];
	stream.read_exact(&mut out).await.unwrap();
	assert_eq!(out, bytes[5600..5700]);
	assert_eq!(server.ranges().first(), Some(&(5500, 5749)));
}

#[tokio::test]
async fn seek_backward_reads_correct_bytes() {
	let bytes = data(10_000);
	let server = RangeServer::start(bytes.clone()).await;
	let mut stream = open(
		resolved(server.url("/a"), 10_000, None),
		FakeRefresher::new(None),
		config(1000),
	);
	let mut head = vec![0u8; 5500];
	stream.read_exact(&mut head).await.unwrap();
	assert_eq!(head, bytes[..5500]);

	stream.seek(SeekFrom::Start(1200)).await.unwrap();
	let mut out = vec![0u8; 300];
	stream.read_exact(&mut out).await.unwrap();
	assert_eq!(out, bytes[1200..1500]);

	let position = stream.seek(SeekFrom::Current(-1000)).await.unwrap();
	assert_eq!(position, 500);
	let mut out = vec![0u8; 1000];
	stream.read_exact(&mut out).await.unwrap();
	assert_eq!(out, bytes[500..1500]);

	// 終端まで読んでから先頭へ戻る
	stream.seek(SeekFrom::End(-10)).await.unwrap();
	let mut tail = Vec::new();
	stream.read_to_end(&mut tail).await.unwrap();
	assert_eq!(tail, bytes[9990..]);
	stream.seek(SeekFrom::Start(0)).await.unwrap();
	let mut out = vec![0u8; 10];
	stream.read_exact(&mut out).await.unwrap();
	assert_eq!(out, bytes[..10]);
}

/// 取得中にシークしたとき、シーク前の取得結果が新しい位置のバッファにならないこと。
#[tokio::test]
async fn seek_during_pending_fetch_reads_new_position() {
	let bytes = data(10_000);
	let server = RangeServer::start(bytes.clone()).await;
	server.set_delay(Duration::from_millis(300));
	let mut stream = open(
		resolved(server.url("/a"), 10_000, None),
		FakeRefresher::new(None),
		config(1000),
	);
	// 先頭のチャンクの取得を始めさせて、終わる前に読み取りを諦める
	let mut out = vec![0u8; 10];
	let started =
		tokio::time::timeout(Duration::from_millis(50), stream.read_exact(&mut out)).await;
	assert!(started.is_err(), "遅延中に読み取りが終わった");

	stream.seek(SeekFrom::Start(7300)).await.unwrap();
	// 中断された先頭の取得が終わる時間を過ぎてから読む
	tokio::time::sleep(Duration::from_millis(500)).await;
	let mut out = vec![0u8; 100];
	stream.read_exact(&mut out).await.unwrap();
	assert_eq!(out, bytes[7300..7400]);
}

#[tokio::test]
async fn next_chunk_is_prefetched_at_half() {
	let bytes = data(3000);
	let server = RangeServer::start(bytes.clone()).await;
	let mut stream = open(
		resolved(server.url("/a"), 3000, None),
		FakeRefresher::new(None),
		config(1000),
	);
	let mut out = vec![0u8; 400];
	stream.read_exact(&mut out).await.unwrap();
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert_eq!(server.ranges(), vec![(0, 999)], "半分より前に先読みした");

	let mut out = vec![0u8; 200];
	stream.read_exact(&mut out).await.unwrap();
	// これ以上読まずに次のチャンクの要求が届くこと
	let prefetched = wait_until(Duration::from_secs(3), || {
		server.ranges().contains(&(1000, 1999))
	})
	.await;
	assert!(prefetched, "先読みしていない: {:?}", server.ranges());
	assert!(!server.ranges().contains(&(2000, 2999)));
}

/// 先読みが失敗して終わっていても、読み手がそのチャンクに達したときに取り直すこと。
///
/// - 先読みは読み手より数十秒早く走るため、その間の一時的な回線断で入力全体を失敗させない
#[tokio::test]
async fn failed_prefetch_is_fetched_again_when_needed() {
	let bytes = data(2000);
	let server = RangeServer::start(bytes.clone()).await;
	let mut stream = open(
		resolved(server.url("/a"), 2000, None),
		FakeRefresher::new(None),
		ChunkConfig {
			attempts: 1,
			..config(1000)
		},
	);
	let mut out = vec![0u8; 10];
	stream.read_exact(&mut out).await.unwrap();
	server.fail_disconnect(1);
	let mut out = vec![0u8; 590];
	stream.read_exact(&mut out).await.unwrap();
	let prefetched = wait_until(Duration::from_secs(3), || {
		server.ranges().contains(&(1000, 1999))
	})
	.await;
	assert!(prefetched, "先読みしていない: {:?}", server.ranges());
	// 先読みのタスクが失敗で終わるのを待つ
	tokio::time::sleep(Duration::from_millis(300)).await;

	let mut rest = Vec::new();
	stream.read_to_end(&mut rest).await.unwrap();
	assert_eq!(rest, bytes[600..]);
	let refetched = server
		.ranges()
		.iter()
		.filter(|range| **range == (1000, 1999))
		.count();
	assert_eq!(refetched, 2, "取り直していない: {:?}", server.ranges());
}

/// 失敗した先読みの取り直しは 1 回だけで、それも失敗したら入力を失敗させること。
///
/// - 取り直しが先読みの扱いを引き継ぐと、失敗のたびに取り直しが続いてトラックが終わらない
#[tokio::test]
async fn failed_prefetch_is_fetched_again_only_once() {
	let bytes = data(2000);
	let server = RangeServer::start(bytes.clone()).await;
	let mut stream = open(
		resolved(server.url("/a"), 2000, None),
		FakeRefresher::new(None),
		ChunkConfig {
			attempts: 1,
			..config(1000)
		},
	);
	let mut out = vec![0u8; 10];
	stream.read_exact(&mut out).await.unwrap();
	// 先読みと、その取り直しの 2 回を失敗させる
	server.fail_disconnect(2);
	let mut rest = Vec::new();
	let error = stream.read_to_end(&mut rest).await.unwrap_err();
	assert!(
		matches!(chunk_error(&error), Some(ChunkError::Request(_))),
		"想定外のエラー: {error:?}"
	);
	assert_eq!(
		rest,
		bytes[10..1000],
		"失敗の前に先頭のチャンクを読み切ること"
	);
	let fetched = server
		.ranges()
		.iter()
		.filter(|range| **range == (1000, 1999))
		.count();
	assert_eq!(fetched, 2, "取り直しの回数が違う: {:?}", server.ranges());
	assert!(stream.failure().get().is_some(), "失敗が記録されていない");
}

#[tokio::test]
async fn transient_failure_is_retried() {
	let bytes = data(2500);
	let server = RangeServer::start(bytes.clone()).await;
	server.fail_disconnect(1);
	let mut stream = open(
		resolved(server.url("/a"), 2500, None),
		FakeRefresher::new(None),
		config(1000),
	);
	let mut out = Vec::new();
	stream.read_to_end(&mut out).await.unwrap();
	assert_eq!(out, bytes);
	let ranges = server.ranges();
	assert_eq!(ranges[0], (0, 999));
	assert_eq!(ranges[1], (0, 999));
}

#[tokio::test]
async fn failure_after_attempts_returns_error() {
	let bytes = data(2500);
	let server = RangeServer::start(bytes).await;
	server.fail_disconnect(100);
	let mut stream = open(
		resolved(server.url("/a"), 2500, None),
		FakeRefresher::new(None),
		config(1000),
	);
	let mut out = vec![0u8; 10];
	let error = stream.read_exact(&mut out).await.unwrap_err();
	assert!(
		chunk_error(&error).is_some(),
		"ChunkError ではない: {error:?}"
	);
	assert_eq!(server.ranges(), vec![(0, 999); 3]);
}

#[tokio::test]
async fn forbidden_triggers_refresh_and_same_range() {
	let bytes = data(2500);
	let server = RangeServer::start(bytes.clone()).await;
	server.fail_forbidden(1);
	let refresher = FakeRefresher::new(Some(resolved(server.url("/b"), 2500, Some(now() + 3600))));
	let mut stream = open(
		resolved(server.url("/a"), 2500, None),
		Arc::clone(&refresher),
		config(1000),
	);
	let mut out = Vec::new();
	stream.read_to_end(&mut out).await.unwrap();
	assert_eq!(out, bytes);
	assert_eq!(refresher.calls(), 1);
	assert_eq!(
		*refresher.format_ids.lock().unwrap(),
		vec![FORMAT_ID.to_owned()]
	);
	let requests = server.requests();
	assert_eq!(requests[0].path, "/a");
	assert_eq!(requests[0].range, Some((0, 999)));
	assert_eq!(requests[1].path, "/b");
	assert_eq!(requests[1].range, Some((0, 999)));
	assert!(requests[1..].iter().all(|r| r.path == "/b"));
}

/// 期限が近い URL は、先読みの前に取り直すこと。最初の読み取りは取り直しを待たずに今の URL で取ること。
#[tokio::test]
async fn near_expiry_refreshes_before_prefetch() {
	let bytes = data(2500);
	let server = RangeServer::start(bytes.clone()).await;
	let refresher = FakeRefresher::new(Some(resolved(server.url("/b"), 2500, Some(now() + 3600))));
	let mut stream = open(
		resolved(server.url("/a"), 2500, Some(now() + 10)),
		Arc::clone(&refresher),
		config(1000),
	);
	let mut out = Vec::new();
	stream.read_to_end(&mut out).await.unwrap();
	assert_eq!(out, bytes);
	assert_eq!(refresher.calls(), 1);
	let requests = server.requests();
	assert_eq!(requests[0].path, "/a", "最初の読み取りが取り直しを待った");
	assert!(requests.len() > 1);
	assert!(
		requests[1..].iter().all(|r| r.path == "/b"),
		"先読みで古い URL を使った: {requests:?}"
	);
}

#[tokio::test]
async fn refresh_with_changed_filesize_fails() {
	let bytes = data(2500);
	let server = RangeServer::start(bytes).await;
	server.fail_forbidden(1);
	let refresher = FakeRefresher::new(Some(resolved(server.url("/b"), 2501, Some(now() + 3600))));
	let mut stream = open(
		resolved(server.url("/a"), 2500, None),
		Arc::clone(&refresher),
		config(1000),
	);
	let mut out = vec![0u8; 10];
	let error = stream.read_exact(&mut out).await.unwrap_err();
	assert!(
		matches!(
			chunk_error(&error),
			Some(ChunkError::FilesizeChanged {
				old: 2500,
				new: 2501
			})
		),
		"想定外のエラー: {error:?}"
	);
	assert!(server.requests().iter().all(|r| r.path == "/a"));
}

#[tokio::test]
async fn hung_server_times_out() {
	let bytes = data(2500);
	let server = RangeServer::start(bytes).await;
	server.set_delay(Duration::from_secs(30));
	let mut stream = open(
		resolved(server.url("/a"), 2500, None),
		FakeRefresher::new(None),
		ChunkConfig {
			chunk_size: 1000,
			initial_chunk_size: 0,
			fetch_timeout: Duration::from_millis(300),
			refresh_timeout: Duration::from_secs(5),
			attempts: 2,
			refresh_margin: Duration::from_secs(300),
			available_wait_max: Duration::from_secs(60),
			expiry_recheck_interval: EXPIRY_RECHECK_INTERVAL,
		},
	);
	let began = Instant::now();
	let mut out = vec![0u8; 10];
	let result = tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut out))
		.await
		.expect("タイムアウトせずに待ち続けた");
	let error = result.unwrap_err();
	assert!(
		matches!(chunk_error(&error), Some(ChunkError::Timeout)),
		"想定外のエラー: {error:?}"
	);
	assert!(began.elapsed() >= Duration::from_millis(600));
	assert_eq!(server.ranges().len(), 2);
}

/// 終端まで読んだ後の後方シーク。AsyncAdapterStream 経由。
///
/// - 読み手が受け取っていない古い ReadZero がシーク中に処理され、`finalised` が立ったまま残ると
///   シーク後の最初の読み取りが 0 バイト (終端) を返す
/// - シーク先のチャンクが取得に時間のかかる状態にして、空のバッファを読む状況を作る
#[test]
fn backward_seek_after_reaching_end_through_adapter() {
	let runtime = tokio::runtime::Builder::new_multi_thread()
		.worker_threads(2)
		.enable_all()
		.build()
		.unwrap();
	let bytes = data(3000);
	let server = runtime.block_on(RangeServer::start(bytes.clone()));
	server.set_delay(Duration::from_millis(200));
	let _guard = runtime.enter();
	let stream = open(
		resolved(server.url("/a"), 3000, None),
		FakeRefresher::new(None),
		config(1000),
	);
	let mut adapter = AsyncAdapterStream::new(Box::new(stream), 64 * 1024);

	let mut first = vec![0u8; 3000];
	adapter.read_exact(&mut first).unwrap();
	assert_eq!(first, bytes);
	// 非同期側が終端に達して ReadZero を送るのを待つ
	std::thread::sleep(Duration::from_millis(500));

	let position = adapter.seek(SeekFrom::Start(0)).unwrap();
	assert_eq!(position, 0);
	let mut second = vec![0u8; 3000];
	adapter.read_exact(&mut second).unwrap();
	assert_eq!(second, bytes);

	adapter.seek(SeekFrom::Start(1500)).unwrap();
	let mut third = vec![0u8; 500];
	adapter.read_exact(&mut third).unwrap();
	assert_eq!(third, bytes[1500..2000]);
}

/// AsyncAdapterStream 経由で取得が失敗したとき、読み取りの終了後に失敗を取り出せること。
///
/// - アダプターは読み取りのエラーを終端と区別しないため、共有の失敗の記録で判別する
#[test]
fn adapter_failure_is_observable_after_read_ends() {
	let runtime = tokio::runtime::Builder::new_multi_thread()
		.worker_threads(2)
		.enable_all()
		.build()
		.unwrap();
	let bytes = data(3000);
	let server = runtime.block_on(RangeServer::start(bytes.clone()));
	let _guard = runtime.enter();
	let request = ChunkedRequest::new(
		http_client::new_client(),
		resolved(server.url("/a"), 3000, None),
		FakeRefresher::new(None),
		ChunkConfig {
			attempts: 1,
			..config(1000)
		},
	);
	let failure = request.failure();
	assert!(failure.get().is_none());
	// 非同期側は先読みでリングバッファを埋めるため、途中からの注入は競合する。最初から失敗させる
	server.fail_disconnect(100);
	let mut adapter = AsyncAdapterStream::new(Box::new(request.into_stream()), 64 * 1024);

	// 読み手から見ると終端 (0 バイト) かエラーで終わり、失敗と区別できない
	let mut rest = Vec::new();
	let _ = adapter.read_to_end(&mut rest);
	assert!(rest.len() < bytes.len());
	let recorded = failure.get().expect("失敗が記録されていない");
	assert!(
		matches!(*recorded, ChunkError::Request(_)),
		"想定外のエラー: {recorded:?}"
	);
}

/// 取得の失敗の文言に URL (署名を含む) が出ないこと。
#[tokio::test]
async fn error_text_does_not_contain_url() {
	// 閉じたポートへ接続させて reqwest のエラーを起こす
	let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
	let port = listener.local_addr().unwrap().port();
	drop(listener);
	let secret = "SECRETSIG123";
	let url = format!("http://127.0.0.1:{port}/videoplayback?sig={secret}&ip=192.0.2.1");
	let request = ChunkedRequest::new(
		http_client::new_client(),
		resolved(url, 2500, None),
		FakeRefresher::new(None),
		ChunkConfig {
			attempts: 1,
			..config(1000)
		},
	);
	let failure = request.failure();
	let mut stream = request.into_stream();
	let mut out = vec![0u8; 10];
	let error = stream.read_exact(&mut out).await.unwrap_err();
	let texts = [
		error.to_string(),
		format!("{error:?}"),
		failure
			.get()
			.map(|e| format!("{e} {e:?}"))
			.unwrap_or_default(),
	];
	assert!(!texts[2].is_empty(), "失敗が記録されていない");
	for text in texts {
		assert!(!text.contains(secret), "URL が含まれる: {text}");
		assert!(!text.contains("videoplayback"), "URL が含まれる: {text}");
	}
}

/// 一部の範囲に 200 (全体) で答えたら受け付けないこと。
#[tokio::test]
async fn partial_range_answered_with_200_is_rejected() {
	let server = RangeServer::start(data(2500)).await;
	server.set_ignore_range(true);
	let mut stream = open(
		resolved(server.url("/a"), 2500, None),
		FakeRefresher::new(None),
		ChunkConfig {
			attempts: 1,
			..config(1000)
		},
	);
	let mut out = vec![0u8; 10];
	let error = stream.read_exact(&mut out).await.unwrap_err();
	assert!(
		matches!(chunk_error(&error), Some(ChunkError::Status(200))),
		"想定外のエラー: {error:?}"
	);
}

/// 206 の Content-Length が要求の範囲と違えば、本文を読まずに受け付けないこと。
///
/// - サーバーは本文の途中で止まる。本文を読んでから長さを確かめる実装は `Timeout` になり、
///   ヘッダーで確かめる実装だけが `Length` を返す
#[tokio::test]
async fn content_length_mismatch_is_rejected() {
	let server = RangeServer::start(data(2500)).await;
	server.set_oversized_stall(true);
	let mut stream = open(
		resolved(server.url("/a"), 2500, None),
		FakeRefresher::new(None),
		ChunkConfig {
			attempts: 1,
			fetch_timeout: Duration::from_secs(1),
			..config(1000)
		},
	);
	let mut out = vec![0u8; 10];
	let error = stream.read_exact(&mut out).await.unwrap_err();
	assert!(
		matches!(
			chunk_error(&error),
			Some(ChunkError::Length {
				got: 1001,
				expected: 1000
			})
		),
		"想定外のエラー: {error:?}"
	);
}

/// 範囲がファイル全体なら 200 を受け付けること。
#[tokio::test]
async fn whole_file_answered_with_200_is_accepted() {
	let bytes = data(800);
	let server = RangeServer::start(bytes.clone()).await;
	server.set_ignore_range(true);
	let mut stream = open(
		resolved(server.url("/a"), 800, None),
		FakeRefresher::new(None),
		config(1000),
	);
	let mut out = Vec::new();
	stream.read_to_end(&mut out).await.unwrap();
	assert_eq!(out, bytes);
}

/// 全長 0 の入力は作成で失敗すること。
#[tokio::test]
async fn zero_filesize_is_rejected() {
	let server = RangeServer::start(Vec::new()).await;
	let mut request = ChunkedRequest::new(
		http_client::new_client(),
		resolved(server.url("/a"), 0, None),
		FakeRefresher::new(None),
		config(1000),
	);
	assert!(request.create_async().await.is_err());
	assert!(server.requests().is_empty());
}

/// 期限前の取り直しに失敗しても、期限前なら今の URL で読み続けること。
/// 取り直しはチャンクごとには行わない。
#[tokio::test]
async fn failed_near_expiry_refresh_keeps_current_url() {
	let bytes = data(3500);
	let server = RangeServer::start(bytes.clone()).await;
	let refresher = FakeRefresher::new(None);
	let mut stream = open(
		resolved(server.url("/a"), 3500, Some(now() + 120)),
		Arc::clone(&refresher),
		config(1000),
	);
	let mut out = Vec::new();
	stream.read_to_end(&mut out).await.unwrap();
	assert_eq!(out, bytes);
	assert_eq!(refresher.calls(), 1);
	assert!(server.requests().iter().all(|r| r.path == "/a"));
}

/// 取り直した期限がまだ余裕の内側でも、チャンクごとに取り直さないこと。
#[tokio::test]
async fn near_expiry_check_is_rate_limited() {
	let bytes = data(3500);
	let server = RangeServer::start(bytes.clone()).await;
	let refresher = FakeRefresher::new(Some(resolved(server.url("/b"), 3500, Some(now() + 120))));
	let mut stream = open(
		resolved(server.url("/a"), 3500, Some(now() + 60)),
		Arc::clone(&refresher),
		config(1000),
	);
	let mut out = Vec::new();
	stream.read_to_end(&mut out).await.unwrap();
	assert_eq!(out, bytes);
	assert_eq!(refresher.calls(), 1);
	// 最初の読み取りは今の URL で取り、先読みから取り直した URL を使う
	let requests = server.requests();
	assert_eq!(requests[0].path, "/a");
	assert!(requests[1..].iter().all(|r| r.path == "/b"), "{requests:?}");
}

/// 期限が近くても、読み手が待つ取得は取り直し (ワーカーの解決) を待たないこと。
#[tokio::test]
async fn first_read_does_not_wait_for_near_expiry_refresh() {
	let bytes = data(2500);
	let server = RangeServer::start(bytes.clone()).await;
	let refresher = FakeRefresher::slow(
		Some(resolved(server.url("/b"), 2500, Some(now() + 3600))),
		Duration::from_secs(30),
	);
	let mut stream = open(
		resolved(server.url("/a"), 2500, Some(now() + 10)),
		refresher,
		config(1000),
	);
	let began = Instant::now();
	let mut first = [0u8; 16];
	stream.read_exact(&mut first).await.unwrap();
	assert_eq!(&first[..], &bytes[..16]);
	assert!(
		began.elapsed() < Duration::from_secs(1),
		"最初の読み取りが取り直しを待った: {:?}",
		began.elapsed()
	);
	assert_eq!(server.requests()[0].path, "/a");
}

/// 応答しない取り直しは `refresh_timeout` で打ち切ること。
#[tokio::test]
async fn hung_refresh_times_out() {
	let server = RangeServer::start(data(2500)).await;
	server.fail_forbidden(1);
	let refresher = FakeRefresher::slow(
		Some(resolved(server.url("/b"), 2500, None)),
		Duration::from_secs(30),
	);
	let mut stream = open(
		resolved(server.url("/a"), 2500, None),
		refresher,
		ChunkConfig {
			refresh_timeout: Duration::from_millis(300),
			..config(1000)
		},
	);
	let mut out = vec![0u8; 10];
	let result = tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut out))
		.await
		.expect("取り直しを待ち続けた");
	let error = result.unwrap_err();
	assert!(
		matches!(
			chunk_error(&error),
			Some(ChunkError::Refresh(ResolveError::Timeout))
		),
		"想定外のエラー: {error:?}"
	);
}

/// `available_at` を持つ解決結果。
fn resolved_available(url: String, filesize: u64, available_at: Option<i64>) -> ResolvedStream {
	let mut stream = resolved(url, filesize, None);
	stream.available_at = available_at;
	stream
}

#[tokio::test]
async fn waits_until_available_at_before_first_fetch() {
	let bytes = data(3000);
	let server = RangeServer::start(bytes.clone()).await;
	let available_at = now() + 2;
	let began = Instant::now();
	let mut stream = open(
		resolved_available(server.url("/a"), 3000, Some(available_at)),
		FakeRefresher::new(None),
		config(1000),
	);
	let mut first = [0u8; 16];
	stream.read_exact(&mut first).await.unwrap();
	assert_eq!(&first[..], &bytes[..16]);
	let first_request = server.request_times()[0];
	assert!(first_request >= UNIX_EPOCH + Duration::from_secs(available_at as u64));
	assert!(began.elapsed() < Duration::from_secs(6));
}

#[tokio::test]
async fn available_wait_is_capped() {
	let bytes = data(2000);
	let server = RangeServer::start(bytes).await;
	let began = Instant::now();
	let mut stream = open(
		resolved_available(server.url("/a"), 2000, Some(now() + 3600)),
		FakeRefresher::new(None),
		ChunkConfig {
			available_wait_max: Duration::from_secs(1),
			..config(1000)
		},
	);
	let mut first = [0u8; 16];
	stream.read_exact(&mut first).await.unwrap();
	let elapsed = began.elapsed();
	assert!(elapsed >= Duration::from_millis(900), "{elapsed:?}");
	assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
}

#[tokio::test]
async fn past_available_at_does_not_wait() {
	let server = RangeServer::start(data(2000)).await;
	let began = Instant::now();
	let mut stream = open(
		resolved_available(server.url("/a"), 2000, Some(now() - 30)),
		FakeRefresher::new(None),
		config(1000),
	);
	let mut first = [0u8; 16];
	stream.read_exact(&mut first).await.unwrap();
	assert!(began.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn available_wait_applies_once_across_chunks() {
	let bytes = data(3000);
	let server = RangeServer::start(bytes.clone()).await;
	let available_at = now() + 2;
	let mut stream = open(
		resolved_available(server.url("/a"), 3000, Some(available_at)),
		FakeRefresher::new(None),
		config(1000),
	);
	let mut out = Vec::new();
	stream.read_to_end(&mut out).await.unwrap();
	assert_eq!(out, bytes);
	let limit = UNIX_EPOCH + Duration::from_secs(available_at as u64);
	assert!(server.request_times().iter().all(|t| *t >= limit));
}

#[tokio::test]
async fn refresh_with_available_at_waits_before_next_fetch() {
	let bytes = data(2000);
	let server = RangeServer::start(bytes.clone()).await;
	server.fail_forbidden(1);
	let available_at = now() + 2;
	let refresher = FakeRefresher::new(Some(resolved_available(
		server.url("/b"),
		2000,
		Some(available_at),
	)));
	let mut stream = open(
		resolved(server.url("/a"), 2000, None),
		Arc::clone(&refresher),
		config(1000),
	);
	let mut first = [0u8; 16];
	stream.read_exact(&mut first).await.unwrap();
	assert_eq!(&first[..], &bytes[..16]);
	assert_eq!(refresher.calls(), 1);
	let limit = UNIX_EPOCH + Duration::from_secs(available_at as u64);
	let after_refresh: Vec<_> = server
		.requests()
		.into_iter()
		.filter(|r| r.path == "/b")
		.collect();
	assert!(!after_refresh.is_empty());
	assert!(after_refresh.iter().all(|r| r.at >= limit));
}

/// 期限前の取り直しで得た URL の `available_at` が先なら、今の URL で待たずに読み続け、
/// 時刻が来てから新しい URL に差し替えること。
#[tokio::test]
async fn early_refresh_with_future_available_at_keeps_current_url_until_ready() {
	let bytes = data(4000);
	let server = RangeServer::start(bytes.clone()).await;
	let available_at = now() + 2;
	let refresher = FakeRefresher::new(Some(resolved_available(
		server.url("/b"),
		4000,
		Some(available_at),
	)));
	let mut stream = open(
		resolved(server.url("/a"), 4000, Some(now() + 10)),
		Arc::clone(&refresher),
		config(1000),
	);
	let began = Instant::now();
	let mut first = [0u8; 16];
	stream.read_exact(&mut first).await.unwrap();
	assert_eq!(&first[..], &bytes[..16]);
	assert!(
		began.elapsed() < Duration::from_secs(1),
		"available_at まで待った: {:?}",
		began.elapsed()
	);
	// 取り直しは先読みで行う。続きを読んで先読みを走らせる
	let mut more = vec![0u8; 1500 - first.len()];
	stream.read_exact(&mut more).await.unwrap();
	assert_eq!(&more[..], &bytes[first.len()..1500]);
	assert_eq!(refresher.calls(), 1);
	assert!(
		server.requests().iter().all(|r| r.path == "/a"),
		"まだ使えない URL を使った: {:?}",
		server.requests()
	);

	// available_at を過ぎてから先読みの先のチャンクを読むと、新しい URL に替わっている
	let limit = UNIX_EPOCH + Duration::from_secs(available_at as u64);
	while SystemTime::now() < limit + Duration::from_millis(200) {
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
	stream.seek(SeekFrom::Start(3000)).await.unwrap();
	let mut out = vec![0u8; 100];
	stream.read_exact(&mut out).await.unwrap();
	assert_eq!(out, bytes[3000..3100]);
	let requests = server.requests();
	let last = requests.last().unwrap();
	assert_eq!(last.path, "/b", "差し替えていない: {requests:?}");
	assert_eq!(last.range, Some((3000, 3999)));
	assert!(last.at >= limit);
	assert_eq!(refresher.calls(), 1, "差し替えで取り直しを重ねない");
}

/// 取り直した取得先を預けている間は、確認の間隔を過ぎても期限前の取り直しを重ねず、
/// 時刻が来たら差し替えた URL の期限で判定すること (預けた取得先を上書きし続けて差し替えが起きない不具合の再発防止)。
#[tokio::test]
async fn early_refresh_is_not_repeated_while_source_is_pending() {
	let bytes = data(4000);
	let server = RangeServer::start(bytes.clone()).await;
	let available_at = now() + 2;
	let mut fresh = resolved_available(server.url("/b"), 4000, Some(available_at));
	fresh.expires_at = Some(now() + 100_000);
	let refresher = FakeRefresher::new(Some(fresh));
	let mut stream = open(
		resolved(server.url("/a"), 4000, Some(now() + 10)),
		Arc::clone(&refresher),
		ChunkConfig {
			expiry_recheck_interval: Duration::ZERO,
			..config(1000)
		},
	);
	// 1 つ目のチャンクで取り直して預け、2 つ目のチャンクは預けたまま今の URL で読む
	let mut head = vec![0u8; 1500];
	stream.read_exact(&mut head).await.unwrap();
	assert_eq!(head, bytes[..1500]);
	assert_eq!(refresher.calls(), 1, "預けている間に取り直した");

	// 時刻が来たら差し替え、新しい URL の期限には余裕があるため取り直さない
	let limit = UNIX_EPOCH + Duration::from_secs(available_at as u64);
	while SystemTime::now() < limit + Duration::from_millis(200) {
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
	stream.seek(SeekFrom::Start(3000)).await.unwrap();
	let mut tail = vec![0u8; 100];
	stream.read_exact(&mut tail).await.unwrap();
	assert_eq!(tail, bytes[3000..3100]);
	let requests = server.requests();
	assert_eq!(
		requests.last().unwrap().path,
		"/b",
		"差し替えていない: {requests:?}"
	);
	assert_eq!(refresher.calls(), 1, "差し替えた後に取り直した");
}

/// 恒久的な 4xx (404) は再試行せず、その場で失敗すること。
#[tokio::test]
async fn permanent_client_error_is_not_retried() {
	let server = RangeServer::start(data(2500)).await;
	server.fail_status(404, 100);
	let mut stream = open(
		resolved(server.url("/a"), 2500, None),
		FakeRefresher::new(None),
		config(1000),
	);
	let began = Instant::now();
	let mut out = vec![0u8; 10];
	let error = stream.read_exact(&mut out).await.unwrap_err();
	assert!(
		matches!(chunk_error(&error), Some(ChunkError::Status(404))),
		"想定外のエラー: {error:?}"
	);
	assert_eq!(server.ranges(), vec![(0, 999)], "再試行した");
	assert!(
		began.elapsed() < Duration::from_millis(400),
		"再試行の待ちが入った: {:?}",
		began.elapsed()
	);
	assert!(stream.failure().get().is_some());
}

/// 5xx と 429 は一時的な失敗として再試行すること。
#[tokio::test]
async fn server_error_and_too_many_requests_are_retried() {
	for code in [503u16, 429] {
		let bytes = data(1500);
		let server = RangeServer::start(bytes.clone()).await;
		server.fail_status(code, 1);
		let mut stream = open(
			resolved(server.url("/a"), 1500, None),
			FakeRefresher::new(None),
			config(1000),
		);
		let mut out = Vec::new();
		stream.read_to_end(&mut out).await.unwrap();
		assert_eq!(out, bytes, "{code}");
		let ranges = server.ranges();
		assert_eq!(&ranges[..2], &[(0, 999), (0, 999)], "{code}: {ranges:?}");
	}
}

/// 先読みが恒久的な失敗で終わっていたら、読み手が達しても取り直さずに失敗すること。
#[tokio::test]
async fn permanently_failed_prefetch_is_not_fetched_again() {
	let bytes = data(2000);
	let server = RangeServer::start(bytes.clone()).await;
	let mut stream = open(
		resolved(server.url("/a"), 2000, None),
		FakeRefresher::new(None),
		config(1000),
	);
	let mut out = vec![0u8; 10];
	stream.read_exact(&mut out).await.unwrap();
	server.fail_status(410, 1);
	let mut out = vec![0u8; 590];
	stream.read_exact(&mut out).await.unwrap();
	let prefetched = wait_until(Duration::from_secs(3), || {
		server.ranges().contains(&(1000, 1999))
	})
	.await;
	assert!(prefetched, "先読みしていない: {:?}", server.ranges());
	tokio::time::sleep(Duration::from_millis(300)).await;

	let mut rest = Vec::new();
	let error = stream.read_to_end(&mut rest).await.unwrap_err();
	assert!(
		matches!(chunk_error(&error), Some(ChunkError::Status(410))),
		"想定外のエラー: {error:?}"
	);
	let fetched = server
		.ranges()
		.iter()
		.filter(|range| **range == (1000, 1999))
		.count();
	assert_eq!(fetched, 1, "取り直した: {:?}", server.ranges());
}

/// 取り消すと、応答を待っている取得がすぐに終わり、失敗として記録しないこと。
#[tokio::test]
async fn cancel_interrupts_pending_fetch() {
	let server = RangeServer::start(data(2500)).await;
	server.set_delay(Duration::from_secs(30));
	let request = ChunkedRequest::new(
		http_client::new_client(),
		resolved(server.url("/a"), 2500, None),
		FakeRefresher::new(None),
		config(1000),
	);
	let cancel = request.cancel_handle();
	let failure = request.failure();
	let mut stream = request.into_stream();
	let reader = tokio::spawn(async move {
		let mut out = vec![0u8; 10];
		stream.read_exact(&mut out).await.map(|_| ())
	});
	let requested = wait_until(Duration::from_secs(3), || !server.requests().is_empty()).await;
	assert!(requested, "取得が始まらない");
	let began = Instant::now();
	cancel.cancel();
	let result = tokio::time::timeout(Duration::from_secs(3), reader)
		.await
		.expect("取り消しても取得が終わらない")
		.unwrap();
	assert!(began.elapsed() < Duration::from_secs(1));
	let error = result.unwrap_err();
	assert!(
		matches!(chunk_error(&error), Some(ChunkError::Cancelled)),
		"想定外のエラー: {error:?}"
	);
	assert!(failure.get().is_none(), "取り消しを失敗として記録した");
	assert!(cancel.is_cancelled());
}

/// 取り消すと `available_at` の待ちも打ち切り、取得を始めないこと。
#[tokio::test]
async fn cancel_interrupts_available_wait() {
	let server = RangeServer::start(data(2500)).await;
	let request = ChunkedRequest::new(
		http_client::new_client(),
		resolved_available(server.url("/a"), 2500, Some(now() + 30)),
		FakeRefresher::new(None),
		config(1000),
	);
	let cancel = request.cancel_handle();
	let mut stream = request.into_stream();
	let reader = tokio::spawn(async move {
		let mut out = vec![0u8; 10];
		stream.read_exact(&mut out).await.map(|_| ())
	});
	tokio::time::sleep(Duration::from_millis(200)).await;
	cancel.cancel();
	let result = tokio::time::timeout(Duration::from_secs(3), reader)
		.await
		.expect("取り消しても待ちが終わらない")
		.unwrap();
	assert!(matches!(
		result.as_ref().map_err(chunk_error),
		Err(Some(ChunkError::Cancelled))
	));
	assert!(server.requests().is_empty(), "取り消した後に取得した");
}

/// 取り消した後の読み取りは、バッファに無い位置で新しい取得を始めずに失敗すること。
#[tokio::test]
async fn read_after_cancel_does_not_fetch() {
	let bytes = data(3000);
	let server = RangeServer::start(bytes.clone()).await;
	let request = ChunkedRequest::new(
		http_client::new_client(),
		resolved(server.url("/a"), 3000, None),
		FakeRefresher::new(None),
		config(1000),
	);
	let cancel = request.cancel_handle();
	let mut stream = request.into_stream();
	let mut out = vec![0u8; 10];
	stream.read_exact(&mut out).await.unwrap();
	cancel.cancel();
	let before = server.requests().len();
	stream.seek(SeekFrom::Start(2500)).await.unwrap();
	let error = stream.read_exact(&mut out).await.unwrap_err();
	assert!(matches!(chunk_error(&error), Some(ChunkError::Cancelled)));
	tokio::time::sleep(Duration::from_millis(200)).await;
	assert_eq!(server.requests().len(), before, "取り消した後に取得した");
}

#[test]
fn transient_classification() {
	assert!(ChunkError::Request("reset".to_owned()).is_transient());
	assert!(ChunkError::Timeout.is_transient());
	assert!(ChunkError::Status(500).is_transient());
	assert!(ChunkError::Status(503).is_transient());
	assert!(ChunkError::Status(429).is_transient());
	for code in [404u16, 410, 416, 400, 200] {
		assert!(!ChunkError::Status(code).is_transient(), "{code}");
	}
	assert!(!ChunkError::Forbidden.is_transient());
	assert!(ChunkError::Forbidden.is_recoverable());
	assert!(!ChunkError::Refresh(ResolveError::Timeout).is_transient());
	assert!(!ChunkError::FilesizeChanged { old: 1, new: 2 }.is_transient());
	assert!(!ChunkError::Length {
		got: 1,
		expected: 2
	}
	.is_recoverable());
	assert!(!ChunkError::Cancelled.is_recoverable());
}
