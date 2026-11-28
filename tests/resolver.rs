//! Resolver (CachedResolver) の試験。偽の WorkerClient と StreamLength を使い、ネットワークは使わない。

#[path = "support/fake_worker_client.rs"]
mod fake_worker_client;
#[path = "support/http_client.rs"]
mod http_client;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use tempfile::TempDir;
use watapachi_bot::config::{CacheConfig, DbConfig, PlaybackConfig};
use watapachi_bot::db::Database;
use watapachi_bot::domain::{Priority, ResolveError, ResolveErrorCode, VideoId};
use watapachi_bot::resolver::cached::{
	normalize_query, parse_content_range_total, CachedResolver, HttpStreamLength, StreamLength,
};
use watapachi_bot::resolver::protocol::WorkerRequest;
use watapachi_bot::resolver::Resolver;

use fake_worker_client::{FakeLength, FakeWorkerClient};

/// 試験の一式。TempDir は DB ファイルのために保持する。
struct Fixture {
	resolver: CachedResolver,
	client: Arc<FakeWorkerClient>,
	length: Arc<FakeLength>,
	_db: Database,
	_dir: TempDir,
}

fn now() -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap()
		.as_secs() as i64
}

fn playback() -> PlaybackConfig {
	PlaybackConfig {
		chunk_size_bytes: 1_048_576,
		initial_chunk_size_bytes: 262_144,
		chunk_fetch_timeout_secs: 15,
		chunk_fetch_attempts: 3,
		url_refresh_margin_secs: 300,
		available_wait_max_secs: 60,
		prefetch_tracks: 2,
		slow_notice_secs: 20,
		position_save_interval_secs: 5,
		skip_streak_limit: 3,
		pipe_format: "bestaudio".to_owned(),
	}
}

fn cache_config() -> CacheConfig {
	CacheConfig {
		playlist_ttl_secs: 14_400,
		search_days: 30,
		track_days: 183,
		history_days: 90,
	}
}

fn fixture(replies: Vec<Result<Value, ResolveError>>) -> Fixture {
	let dir = tempfile::tempdir().unwrap();
	let db = Database::open(
		&dir.path().join("t.db"),
		&DbConfig {
			batch_size: 16,
			read_connections: 2,
			writer_queue: 64,
		},
	)
	.unwrap();
	let client = Arc::new(FakeWorkerClient::new(replies));
	let length = Arc::new(FakeLength::new(777));
	let resolver = CachedResolver::new(
		client.clone(),
		db.handles.clone(),
		length.clone(),
		cache_config(),
		playback(),
	);
	Fixture {
		resolver,
		client,
		length,
		_db: db,
		_dir: dir,
	}
}

/// resolve の成功応答。`filesize` が None なら項目ごと省く。
fn resolve_reply(expires_in: i64, filesize: Option<u64>) -> Result<Value, ResolveError> {
	let mut v = json!({
		"id": "abc", "title": "song", "duration": 120, "url": "https://example.test/a",
		"http_headers": {"User-Agent": "x"}, "format_id": "251", "expires_at": now() + expires_in,
		"protocol": "https", "is_live": false
	});
	if let Some(size) = filesize {
		v["filesize"] = json!(size);
	}
	Ok(v)
}

fn vid() -> VideoId {
	VideoId("abcdefghijk".to_string())
}

/// 切り離したキャッシュの保存を待つ上限。
const SETTLE_LIMIT: Duration = Duration::from_secs(5);
/// 保存の終わりを確かめる間隔。
const SETTLE_POLL: Duration = Duration::from_millis(1);

/// 切り離したキャッシュの保存 (tokio::spawn したタスク) がすべて終わるまで待つ。
///
/// - CachedResolver はミスのときの保存を待たずに返すため、次の呼び出しがキャッシュに当たることを
///   確かめる前に呼ぶ
/// - 試験のランタイムで生きているタスクが 0 になるまで待つ (試験の本体は数に入らない)
async fn settle() {
	let metrics = tokio::runtime::Handle::current().metrics();
	tokio::time::timeout(SETTLE_LIMIT, async {
		while metrics.num_alive_tasks() > 0 {
			tokio::time::sleep(SETTLE_POLL).await;
		}
	})
	.await
	.expect("キャッシュの保存が上限内に終わること");
}

#[test]
fn normalize_query_applies_nfkc_and_lowercase() {
	assert_eq!(normalize_query("ＹＯＡＳＯＢＩ"), "yoasobi");
	assert_eq!(normalize_query("Ｈello ＷＯrld"), "hello world");
}

#[test]
fn content_range_total_is_parsed() {
	assert_eq!(parse_content_range_total("bytes 0-0/12345"), Some(12345));
	assert_eq!(parse_content_range_total("bytes 0-0/*"), None);
	assert_eq!(parse_content_range_total("garbage"), None);
}

#[tokio::test]
async fn second_resolve_uses_cache() {
	let f = fixture(vec![resolve_reply(3600, Some(1000))]);
	let a = f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	settle().await;
	let b = f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	assert_eq!(f.client.requests().len(), 1);
	assert_eq!(a.url, b.url);
	assert_eq!(b.filesize, 1000);
	assert_eq!(b.title, "song");
}

#[tokio::test]
async fn expiring_stream_is_resolved_again() {
	// 期限まで 100 秒 < 余裕 300 秒 なので 2 回目も取り直す
	let f = fixture(vec![
		resolve_reply(100, Some(1000)),
		resolve_reply(3600, Some(1000)),
	]);
	f.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	settle().await;
	f.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	assert_eq!(f.client.requests().len(), 2);
}

#[tokio::test]
async fn refresh_ignores_cache_and_passes_format_id() {
	let f = fixture(vec![
		resolve_reply(3600, Some(1000)),
		resolve_reply(3600, Some(1000)),
	]);
	f.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	f.resolver.refresh(&vid(), Some("251")).await.unwrap();
	let requests = f.client.requests();
	assert_eq!(requests.len(), 2);
	match &requests[1] {
		WorkerRequest::Resolve { url, format_id } => {
			assert!(url.ends_with("abcdefghijk"));
			assert_eq!(format_id.as_deref(), Some("251"));
		}
		other => panic!("想定外の要求: {other:?}"),
	}
}

#[tokio::test]
async fn missing_filesize_is_filled_by_stream_length() {
	let f = fixture(vec![resolve_reply(3600, None)]);
	let s = f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	assert_eq!(s.filesize, 777);
	assert_eq!(f.length.calls(), 1);
	settle().await;
	// 補完した値がキャッシュされ、2 回目は全長も取らない
	let s2 = f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	assert_eq!(s2.filesize, 777);
	assert_eq!(f.length.calls(), 1);
}

#[tokio::test]
async fn present_filesize_skips_stream_length() {
	let f = fixture(vec![resolve_reply(3600, Some(1000))]);
	f.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	assert_eq!(f.length.calls(), 0);
}

#[tokio::test]
async fn search_cache_hit_skips_worker() {
	let f = fixture(vec![Ok(json!({
		"title": null,
		"entries": [{"id": "abcdefghijk", "title": "Song", "duration": 200}]
	}))]);
	let a = f
		.resolver
		.search("ＹＯＡＳＯＢＩ", Priority::Interactive)
		.await
		.unwrap();
	settle().await;
	// 正規化後に同じ語になる検索はキャッシュに当たる
	let b = f
		.resolver
		.search("yoasobi", Priority::Interactive)
		.await
		.unwrap();
	assert_eq!(f.client.requests().len(), 1);
	assert_eq!(a.video_id, b.video_id);
	assert_eq!(b.duration_s, Some(200));
}

#[tokio::test]
async fn empty_search_is_not_found() {
	let f = fixture(vec![Ok(json!({"title": null, "entries": []}))]);
	let err = f
		.resolver
		.search("zzz", Priority::Interactive)
		.await
		.unwrap_err();
	assert!(matches!(
		err,
		ResolveError::Worker {
			code: ResolveErrorCode::NotFound,
			..
		}
	));
}

#[tokio::test]
async fn playlist_within_ttl_skips_worker() {
	let f = fixture(vec![Ok(json!({
		"title": "list",
		"entries": [
			{"id": "aaaaaaaaaaa", "title": "A", "duration": 10},
			{"id": "bbbbbbbbbbb", "title": "B", "duration": null}
		]
	}))]);
	let a = f
		.resolver
		.flat_playlist("https://www.youtube.com/playlist?list=x", 50)
		.await
		.unwrap();
	settle().await;
	let b = f
		.resolver
		.flat_playlist("https://www.youtube.com/playlist?list=x", 50)
		.await
		.unwrap();
	assert_eq!(f.client.requests().len(), 1);
	assert_eq!(a.entries.len(), 2);
	assert_eq!(b.title.as_deref(), Some("list"));
	assert_eq!(b.entries[1].duration_s, None);
	// 上限はキャッシュからの返却にも効く
	let c = f
		.resolver
		.flat_playlist("https://www.youtube.com/playlist?list=x", 1)
		.await
		.unwrap();
	assert_eq!(c.entries.len(), 1);
}

#[tokio::test]
async fn worker_error_code_is_mapped() {
	let f = fixture(vec![Err(ResolveError::Worker {
		code: ResolveErrorCode::NotFound,
		message: "gone".to_string(),
	})]);
	let err = f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap_err();
	assert!(matches!(
		err,
		ResolveError::Worker {
			code: ResolveErrorCode::NotFound,
			..
		}
	));
}

#[tokio::test]
async fn errors_are_not_cached() {
	let f = fixture(vec![
		Err(ResolveError::Timeout),
		resolve_reply(3600, Some(1000)),
	]);
	assert_eq!(
		f.resolver
			.resolve(&vid(), Priority::Interactive)
			.await
			.unwrap_err(),
		ResolveError::Timeout
	);
	f.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	assert_eq!(f.client.requests().len(), 2);
}

fn playlist_reply(count: usize) -> Result<Value, ResolveError> {
	let entries: Vec<Value> = (0..count)
		.map(|i| json!({"id": format!("id{i:09}"), "title": format!("t{i}"), "duration": 1}))
		.collect();
	Ok(json!({"title": "list", "entries": entries}))
}

const PL: &str = "https://www.youtube.com/playlist?list=x";

#[tokio::test]
async fn small_then_large_playlist_limit_refetches() {
	// 上限 2 で 2 件を保存した後、上限 5 を求めたら取り直す
	let f = fixture(vec![playlist_reply(2), playlist_reply(5)]);
	assert_eq!(
		f.resolver.flat_playlist(PL, 2).await.unwrap().entries.len(),
		2
	);
	settle().await;
	assert_eq!(
		f.resolver.flat_playlist(PL, 5).await.unwrap().entries.len(),
		5
	);
	settle().await;
	assert_eq!(f.client.requests().len(), 2);
	// 5 件で保存し直したので、小さい上限は取り直さずに切って返す
	assert_eq!(
		f.resolver.flat_playlist(PL, 3).await.unwrap().entries.len(),
		3
	);
	assert_eq!(f.client.requests().len(), 2);
}

#[tokio::test]
async fn complete_short_playlist_is_served_for_larger_limit() {
	// 上限 50 に対して 3 件なら全件。上限 500 でも取り直さない
	let f = fixture(vec![playlist_reply(3)]);
	f.resolver.flat_playlist(PL, 50).await.unwrap();
	settle().await;
	assert_eq!(
		f.resolver
			.flat_playlist(PL, 500)
			.await
			.unwrap()
			.entries
			.len(),
		3
	);
	assert_eq!(f.client.requests().len(), 1);
}

#[tokio::test]
async fn bad_playlist_entry_is_skipped() {
	let f = fixture(vec![Ok(json!({
		"title": "list",
		"entries": [
			{"id": "aaaaaaaaaaa", "title": "A", "duration": 1},
			{"title": "no id"},
			{"id": 5, "title": "numeric id"},
			"junk",
			{"id": "bbbbbbbbbbb", "title": "B", "duration": 2.0}
		]
	}))]);
	let list = f.resolver.flat_playlist(PL, 10).await.unwrap();
	let ids: Vec<&str> = list.entries.iter().map(|e| e.video_id.0.as_str()).collect();
	assert_eq!(ids, ["aaaaaaaaaaa", "bbbbbbbbbbb"]);
	settle().await;
	// 捨てた要素があるので全件とは言えず、大きい上限では取り直す
	f.resolver.flat_playlist(PL, 100).await.unwrap_err();
	assert_eq!(f.client.requests().len(), 2);
}

#[tokio::test]
async fn missing_expires_at_is_not_cached() {
	let mut reply = resolve_reply(3600, Some(1000)).unwrap();
	reply.as_object_mut().unwrap().remove("expires_at");
	let f = fixture(vec![Ok(reply.clone()), Ok(reply)]);
	let s = f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	assert_eq!(s.expires_at, None);
	settle().await;
	f.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	assert_eq!(f.client.requests().len(), 2);
}

#[tokio::test]
async fn float_expires_at_is_accepted() {
	let mut reply = resolve_reply(3600, Some(1000)).unwrap();
	let at = now() + 3600;
	reply["expires_at"] = json!(at as f64);
	reply["filesize"] = json!(1000.0);
	let f = fixture(vec![Ok(reply)]);
	let s = f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	assert_eq!(s.expires_at, Some(at));
	assert_eq!(s.filesize, 1000);
	// 整数値でない浮動小数は拒否する
	let mut bad = resolve_reply(3600, Some(1000)).unwrap();
	bad["expires_at"] = json!(1.5);
	let f = fixture(vec![Ok(bad)]);
	assert!(f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.is_err());
}

#[tokio::test]
async fn missing_required_fields_are_internal_errors() {
	for key in ["format_id", "protocol", "title", "url"] {
		let mut reply = resolve_reply(3600, Some(1000)).unwrap();
		reply.as_object_mut().unwrap().remove(key);
		let f = fixture(vec![Ok(reply)]);
		let err = f
			.resolver
			.resolve(&vid(), Priority::Interactive)
			.await
			.unwrap_err();
		match err {
			ResolveError::Worker {
				code: ResolveErrorCode::Internal,
				message,
			} => {
				assert!(message.contains(key), "{key}: {message}");
			}
			other => panic!("{key}: {other:?}"),
		}
	}
	// 型が違う項目も同じ
	let mut reply = resolve_reply(3600, Some(1000)).unwrap();
	reply["format_id"] = json!(251);
	let f = fixture(vec![Ok(reply)]);
	assert!(f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.is_err());
	// ライブは protocol と format_id が無くてもよい
	let mut live = resolve_reply(3600, None).unwrap();
	live["is_live"] = json!(true);
	live.as_object_mut().unwrap().remove("format_id");
	live.as_object_mut().unwrap().remove("protocol");
	let f = fixture(vec![Ok(live)]);
	let s = f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	assert!(s.is_live);
	assert_eq!(s.filesize, 0);
	assert_eq!(f.length.calls(), 0);
}

#[tokio::test]
async fn refresh_updates_cache() {
	let mut second = resolve_reply(3600, Some(1000)).unwrap();
	second["url"] = json!("https://example.test/new");
	let f = fixture(vec![resolve_reply(3600, Some(1000)), Ok(second)]);
	let first = f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	assert_eq!(first.url, "https://example.test/a");
	settle().await;
	f.resolver.refresh(&vid(), None).await.unwrap();
	settle().await;
	let again = f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	assert_eq!(again.url, "https://example.test/new");
	assert_eq!(f.client.requests().len(), 2);
}

#[tokio::test]
async fn malformed_reply_is_internal_error() {
	let f = fixture(vec![Ok(json!({"title": "no url"}))]);
	let err = f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap_err();
	assert!(matches!(
		err,
		ResolveError::Worker {
			code: ResolveErrorCode::Internal,
			..
		}
	));
}

/// 全長取得のエラーの文言に URL (署名と IP を含む) が出ないことを確かめる。
fn assert_no_url(error: &ResolveError, secret: &str) {
	for text in [error.to_string(), format!("{error:?}")] {
		assert!(!text.contains(secret), "URL が含まれる: {text}");
		assert!(!text.contains("videoplayback"), "URL が含まれる: {text}");
	}
}

/// 接続できないときの全長取得のエラーに URL が出ないこと (reqwest のエラーは URL を含む)。
#[tokio::test]
async fn length_send_error_does_not_contain_url() {
	let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
	let port = listener.local_addr().unwrap().port();
	drop(listener);
	let secret = "SECRETSIG123";
	let url = format!("http://127.0.0.1:{port}/videoplayback?sig={secret}&ip=192.0.2.1");
	let length =
		HttpStreamLength::new(http_client::new_client(), std::time::Duration::from_secs(5));
	let error = length.length(&url, &BTreeMap::new()).await.unwrap_err();
	assert!(matches!(
		error,
		ResolveError::Worker {
			code: ResolveErrorCode::Internal,
			..
		}
	));
	assert_no_url(&error, secret);
}

/// 500 を返すときの全長取得のエラーに URL が出ないこと。
#[tokio::test]
async fn length_server_error_does_not_contain_url() {
	let app =
		axum::Router::new().fallback(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR });
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
	let addr = listener.local_addr().unwrap();
	let server = tokio::spawn(async move {
		let _ = axum::serve(listener, app).await;
	});
	let secret = "SECRETSIG123";
	let url = format!("http://{addr}/videoplayback?sig={secret}&ip=192.0.2.1");
	let length =
		HttpStreamLength::new(http_client::new_client(), std::time::Duration::from_secs(5));
	let error = length.length(&url, &BTreeMap::new()).await.unwrap_err();
	server.abort();
	assert!(error.to_string().contains("500"), "想定外のエラー: {error}");
	assert_no_url(&error, secret);
}

#[tokio::test]
async fn resolve_parses_available_at() {
	let at = now() + 5;
	let mut reply = resolve_reply(3600, Some(1000)).unwrap();
	reply["available_at"] = json!(at);
	let f = fixture(vec![Ok(reply)]);
	let s = f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	assert_eq!(s.available_at, Some(at));
	// キーが無いときと null のときは None
	let f = fixture(vec![resolve_reply(3600, Some(1000))]);
	let s = f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	assert_eq!(s.available_at, None);
	let mut reply = resolve_reply(3600, Some(1000)).unwrap();
	reply["available_at"] = Value::Null;
	let f = fixture(vec![Ok(reply)]);
	let s = f
		.resolver
		.resolve(&vid(), Priority::Interactive)
		.await
		.unwrap();
	assert_eq!(s.available_at, None);
}

/// ワーカーが持ち時間で途中までにした一覧の応答。
fn truncated_playlist_reply(count: usize) -> Result<Value, ResolveError> {
	let mut reply = playlist_reply(count)?;
	reply["truncated"] = json!(true);
	Ok(reply)
}

#[tokio::test]
async fn truncated_playlist_is_not_treated_as_complete() {
	// 上限 50 に対して 3 件でも、途中までの一覧なら全件ではない。上限 50 では取り直し、3 件以下なら使う
	let f = fixture(vec![truncated_playlist_reply(3), playlist_reply(5)]);
	assert_eq!(
		f.resolver
			.flat_playlist(PL, 50)
			.await
			.unwrap()
			.entries
			.len(),
		3
	);
	settle().await;
	assert_eq!(
		f.resolver.flat_playlist(PL, 3).await.unwrap().entries.len(),
		3
	);
	assert_eq!(f.client.requests().len(), 1);
	assert_eq!(
		f.resolver
			.flat_playlist(PL, 50)
			.await
			.unwrap()
			.entries
			.len(),
		5
	);
	assert_eq!(f.client.requests().len(), 2);
}

#[tokio::test]
async fn non_boolean_truncated_is_an_internal_error() {
	let f = fixture(vec![Ok(
		json!({"title": "list", "entries": [], "truncated": "yes"}),
	)]);
	let err = f.resolver.flat_playlist(PL, 10).await.unwrap_err();
	assert!(matches!(
		err,
		ResolveError::Worker {
			code: ResolveErrorCode::Internal,
			..
		}
	));
}

/// `vid()` を返す検索の応答。
fn search_reply() -> Result<Value, ResolveError> {
	Ok(json!({
		"title": null,
		"entries": [{"id": "abcdefghijk", "title": "Song", "duration": 200}]
	}))
}

/// ワーカーの失敗の応答。
fn worker_error(code: ResolveErrorCode) -> Result<Value, ResolveError> {
	Err(ResolveError::Worker {
		code,
		message: "unavailable".to_string(),
	})
}

#[tokio::test]
async fn unavailable_video_is_removed_from_the_search_cache() {
	for code in [
		ResolveErrorCode::NotFound,
		ResolveErrorCode::Private,
		ResolveErrorCode::GeoBlocked,
	] {
		let f = fixture(vec![search_reply(), worker_error(code), search_reply()]);
		f.resolver.search("q", Priority::Interactive).await.unwrap();
		settle().await;
		f.resolver
			.resolve(&vid(), Priority::Interactive)
			.await
			.unwrap_err();
		settle().await;
		// キャッシュから消えたので、同じ検索語はワーカーに検索し直させる
		f.resolver.search("q", Priority::Interactive).await.unwrap();
		assert_eq!(f.client.requests().len(), 3, "{code:?}");
		assert!(matches!(
			f.client.requests()[2],
			WorkerRequest::Search { .. }
		));
	}
}

#[tokio::test]
async fn refresh_of_unavailable_video_removes_it_from_the_search_cache() {
	let f = fixture(vec![
		search_reply(),
		worker_error(ResolveErrorCode::Private),
		search_reply(),
	]);
	f.resolver.search("q", Priority::Interactive).await.unwrap();
	settle().await;
	f.resolver.refresh(&vid(), Some("251")).await.unwrap_err();
	settle().await;
	f.resolver.search("q", Priority::Interactive).await.unwrap();
	assert_eq!(f.client.requests().len(), 3);
}

#[tokio::test]
async fn transient_failure_keeps_the_search_cache() {
	for error in [
		Err(ResolveError::Timeout),
		worker_error(ResolveErrorCode::RateLimited),
		worker_error(ResolveErrorCode::AgeRestricted),
		worker_error(ResolveErrorCode::Internal),
	] {
		let f = fixture(vec![search_reply(), error]);
		f.resolver.search("q", Priority::Interactive).await.unwrap();
		settle().await;
		f.resolver
			.resolve(&vid(), Priority::Interactive)
			.await
			.unwrap_err();
		settle().await;
		// 時間が経てば解決できうる失敗では消さず、キャッシュに当たる
		f.resolver.search("q", Priority::Interactive).await.unwrap();
		assert_eq!(f.client.requests().len(), 2);
	}
}
