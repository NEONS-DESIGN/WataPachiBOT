//! 試験用の Range 対応 HTTP サーバー。axum で動かす。
//!
//! - どのパスでも同じデータを返す。パスは URL の取り直しの確認に使う
//! - 失敗の注入: 403 を N 回返す、任意の状態コードを N 回返す、接続を N 回切る、応答を遅らせる、
//!   Range を無視して 200 で全体を返す、
//!   Content-Length を要求より 1 バイト多くして本文の途中で止まる
//! - 受けた要求のパスと Range を記録する
//!
//! 呼び出し方:
//! ```ignore
//! let server = RangeServer::start(data).await;
//! server.fail_forbidden(1);
//! let url = server.url("/a");
//! let ranges = server.requests();
//! ```

#![allow(dead_code)]

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode, Uri};
use axum::response::Response;
use axum::Router;
use tokio::task::JoinHandle;

/// 受けた要求 1 件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recorded {
	/// 要求のパス (`/a` など)
	pub path: String,
	/// Range の開始と終了 (終端を含む)。Range が無ければ None
	pub range: Option<(u64, u64)>,
	/// 受信した時刻
	pub at: SystemTime,
}

/// 注入する失敗と記録。
#[derive(Default)]
struct Faults {
	forbidden: u32,
	disconnect: u32,
	/// 返す状態コードと残りの回数
	status: Option<(StatusCode, u32)>,
	delay: Duration,
	ignore_range: bool,
	oversized_stall: bool,
	recorded: Vec<Recorded>,
}

/// サーバーの共有状態。
struct Shared {
	data: Bytes,
	faults: Mutex<Faults>,
}

/// 起動中のサーバー。破棄すると停止する。
pub struct RangeServer {
	addr: SocketAddr,
	shared: Arc<Shared>,
	task: JoinHandle<()>,
}

/// `bytes=a-b` を解釈する。`b` が無ければ終端まで。
fn parse_range(value: &str, len: u64) -> Option<(u64, u64)> {
	let spec = value.strip_prefix("bytes=")?;
	let (start, end) = spec.split_once('-')?;
	let start: u64 = start.parse().ok()?;
	let end: u64 = if end.is_empty() {
		len.checked_sub(1)?
	} else {
		end.parse().ok()?
	};
	Some((start, end))
}

/// すべてのパスを受ける処理。
async fn handle(State(shared): State<Arc<Shared>>, uri: Uri, headers: HeaderMap) -> Response {
	let len = shared.data.len() as u64;
	let range = headers
		.get(header::RANGE)
		.and_then(|v| v.to_str().ok())
		.and_then(|v| parse_range(v, len));
	let (forbidden, disconnect, status, delay, ignore_range, oversized_stall) = {
		let mut faults = shared.faults.lock().unwrap();
		faults.recorded.push(Recorded {
			path: uri.path().to_owned(),
			range,
			at: SystemTime::now(),
		});
		let forbidden = faults.forbidden > 0;
		if forbidden {
			faults.forbidden -= 1;
		}
		let disconnect = !forbidden && faults.disconnect > 0;
		if disconnect {
			faults.disconnect -= 1;
		}
		let status = match faults.status {
			Some((code, left)) if !forbidden && !disconnect && left > 0 => {
				faults.status = Some((code, left - 1));
				Some(code)
			}
			_ => None,
		};
		(
			forbidden,
			disconnect,
			status,
			faults.delay,
			faults.ignore_range,
			faults.oversized_stall,
		)
	};
	if !delay.is_zero() {
		tokio::time::sleep(delay).await;
	}
	if forbidden {
		return Response::builder()
			.status(StatusCode::FORBIDDEN)
			.body(Body::empty())
			.unwrap();
	}
	if let Some(code) = status {
		return Response::builder()
			.status(code)
			.body(Body::empty())
			.unwrap();
	}
	if ignore_range {
		return Response::builder()
			.status(StatusCode::OK)
			.header(header::CONTENT_LENGTH, len)
			.body(Body::from(shared.data.clone()))
			.unwrap();
	}
	let (start, end) = range.unwrap_or((0, len.saturating_sub(1)));
	if start >= len || end < start {
		return Response::builder()
			.status(StatusCode::RANGE_NOT_SATISFIABLE)
			.body(Body::empty())
			.unwrap();
	}
	let end = if oversized_stall { end + 1 } else { end }.min(len - 1);
	let slice = shared.data.slice(start as usize..=end as usize);
	let body = if oversized_stall {
		// 先頭の一部だけ送って止まる。ヘッダーを確かめない実装は残りを待ち続ける
		let half = slice.slice(..slice.len() / 2);
		let first = futures::stream::iter(vec![Ok::<Bytes, Infallible>(half)]);
		Body::from_stream(futures::StreamExt::chain(first, futures::stream::pending()))
	} else if disconnect {
		// 先頭の一部だけ送って本文をエラーで終える。hyper は接続を切る
		let half = slice.slice(..slice.len() / 2);
		let parts: Vec<Result<Bytes, std::io::Error>> =
			vec![Ok(half), Err(std::io::Error::other("injected disconnect"))];
		Body::from_stream(futures::stream::iter(parts))
	} else {
		let parts: Vec<Result<Bytes, Infallible>> = vec![Ok(slice.clone())];
		Body::from_stream(futures::stream::iter(parts))
	};
	Response::builder()
		.status(StatusCode::PARTIAL_CONTENT)
		.header(header::CONTENT_LENGTH, slice.len())
		.header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{len}"))
		.body(body)
		.unwrap()
}

impl RangeServer {
	/// `data` を返すサーバーを 127.0.0.1 の空きポートで起動する。
	pub async fn start(data: Vec<u8>) -> Self {
		let shared = Arc::new(Shared {
			data: Bytes::from(data),
			faults: Mutex::new(Faults::default()),
		});
		let app = Router::new()
			.fallback(handle)
			.with_state(Arc::clone(&shared));
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let addr = listener.local_addr().unwrap();
		let task = tokio::spawn(async move {
			let _ = axum::serve(listener, app).await;
		});
		Self { addr, shared, task }
	}

	/// パスから URL を作る。例: `server.url("/a")`
	pub fn url(&self, path: &str) -> String {
		format!("http://{}{}", self.addr, path)
	}

	/// 次の `n` 回の要求に 403 を返す。
	pub fn fail_forbidden(&self, n: u32) {
		self.shared.faults.lock().unwrap().forbidden = n;
	}

	/// 次の `n` 回の要求で本文の途中で接続を切る。
	pub fn fail_disconnect(&self, n: u32) {
		self.shared.faults.lock().unwrap().disconnect = n;
	}

	/// 次の `n` 回の要求に状態コード `code` (本文なし) を返す。例: `server.fail_status(404, 1)`
	pub fn fail_status(&self, code: u16, n: u32) {
		let code = StatusCode::from_u16(code).unwrap();
		self.shared.faults.lock().unwrap().status = Some((code, n));
	}

	/// すべての応答を `delay` だけ遅らせる。
	pub fn set_delay(&self, delay: Duration) {
		self.shared.faults.lock().unwrap().delay = delay;
	}

	/// Range を無視して 200 でデータ全体を返すかを切り替える。
	pub fn set_ignore_range(&self, ignore: bool) {
		self.shared.faults.lock().unwrap().ignore_range = ignore;
	}

	/// 要求より 1 バイト多い Content-Length の 206 を返し、本文の途中で止めるかを切り替える。
	///
	/// - Content-Length を本文の前に確かめる実装だけがすぐに失敗する。確かめない実装はタイムアウトまで待つ
	pub fn set_oversized_stall(&self, enabled: bool) {
		self.shared.faults.lock().unwrap().oversized_stall = enabled;
	}

	/// これまでに受けた要求。
	pub fn requests(&self) -> Vec<Recorded> {
		self.shared.faults.lock().unwrap().recorded.clone()
	}

	/// 受けた要求の受信時刻を、受けた順に返す。
	pub fn request_times(&self) -> Vec<SystemTime> {
		self.requests().into_iter().map(|r| r.at).collect()
	}

	/// 受けた要求の Range だけを返す。
	pub fn ranges(&self) -> Vec<(u64, u64)> {
		self.requests()
			.into_iter()
			.filter_map(|r| r.range)
			.collect()
	}
}

impl Drop for RangeServer {
	fn drop(&mut self) {
		self.task.abort();
	}
}
