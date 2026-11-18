//! Resolver の試験用の偽 WorkerClient と偽 StreamLength。
//! - 応答は呼び出し順に取り出す。記録した要求で呼び出し回数と内容を確かめる

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::Value;
use watapachi_bot::domain::{Priority, ResolveError};
use watapachi_bot::resolver::cached::StreamLength;
use watapachi_bot::resolver::pool::WorkerClient;
use watapachi_bot::resolver::protocol::WorkerRequest;

/// 用意した応答を順に返す WorkerClient。
pub struct FakeWorkerClient {
	replies: Mutex<VecDeque<Result<Value, ResolveError>>>,
	requests: Mutex<Vec<WorkerRequest>>,
}

impl FakeWorkerClient {
	/// 応答を並べて作る。足りなくなったら `Unavailable` を返す。
	pub fn new(replies: Vec<Result<Value, ResolveError>>) -> Self {
		Self {
			replies: Mutex::new(replies.into()),
			requests: Mutex::new(Vec::new()),
		}
	}

	/// これまでに受けた要求。
	pub fn requests(&self) -> Vec<WorkerRequest> {
		self.requests.lock().unwrap().clone()
	}
}

#[async_trait]
impl WorkerClient for FakeWorkerClient {
	async fn submit(
		&self,
		request: WorkerRequest,
		_priority: Priority,
	) -> Result<Value, ResolveError> {
		self.requests.lock().unwrap().push(request);
		self.replies
			.lock()
			.unwrap()
			.pop_front()
			.unwrap_or(Err(ResolveError::Unavailable))
	}
}

/// 固定の長さを返し、呼ばれた回数を数える StreamLength。
pub struct FakeLength {
	pub size: u64,
	pub calls: AtomicUsize,
}

impl FakeLength {
	pub fn new(size: u64) -> Self {
		Self {
			size,
			calls: AtomicUsize::new(0),
		}
	}

	pub fn calls(&self) -> usize {
		self.calls.load(Ordering::SeqCst)
	}
}

#[async_trait]
impl StreamLength for FakeLength {
	async fn length(
		&self,
		_url: &str,
		_headers: &BTreeMap<String, String>,
	) -> Result<u64, ResolveError> {
		self.calls.fetch_add(1, Ordering::SeqCst);
		Ok(self.size)
	}
}
