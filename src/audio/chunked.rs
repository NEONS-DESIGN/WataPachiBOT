//! 小さな Range 要求で読み進める Songbird の入力。
//!
//! 背景:
//! - googlevideo は大きな範囲の Range を約 35KiB/s に制限する。標準の `HttpRequest` は読み飛ばしで
//!   シークするため、長時間の曲では実用にならない
//!
//! 方式:
//! - 読み手が待つ取得 (曲の始め、シークの直後、先読みの取りこぼし) は `initial_chunk_size` 単位
//!   (その境界に揃える) で取得し、先読みは `chunk_size` で取得する。メモリから返し、長時間の接続を持たない。
//!   最初の音までに受け切る量を減らすため (開発機の計測で、1MiB に比べ 256KiB は最初のパケットまでが約 13〜30ms 短い)
//! - 現在のチャンクを半分読んだら次のチャンクを先読みする。1 トラックのメモリは約 2 チャンク
//! - 再試行はこの入力の内部で完結させる。`AsyncAdapterSink::try_resume` には頼らない
//!   (`seen_bytes` がシーク後に誤った位置になりうるため)
//! - URL は `ArcSwap` に持つ。期限が近いとき、403 を受けたときは `UrlRefresher` で取り直す。
//!   取り直しの上限は `refresh_timeout` (ワーカーの応答の上限に合わせる)
//! - 解決結果の `available_at` (YouTube が URL を 403 で拒む期間の終わり) までは取得を始めない。
//!   待つのは取得の前で、読み手と先読みのどちらが先でも同じ時刻まで待つ。上限は `available_wait_max`。
//!   403 での取り直しで `available_at` が得られたら、次の取得の前に同じく待つ
//! - 期限前の取り直しで得た URL の `available_at` が先なら、その時刻まで今の URL を使い続け、
//!   時刻が来てから差し替える (まだ使える URL を捨てて取得を止めないため)
//! - 取り直しで全長が変わったらバイト位置が使えないため、入力を失敗させる
//! - 再試行するのは一時的な失敗 (送信・受信の失敗、時間切れ、5xx、429) だけ。恒久的な 4xx、
//!   全長の変化、取り直しの失敗はその場で入力を失敗させる (`ChunkError::is_transient`)
//! - 曲を止めたら `ChunkCancel` で取り消す。進行中の取得・`available_at` の待ち・取り直しを打ち切り、
//!   同期で読んでいるミキサーをすぐに解放する。取り消しは失敗として記録しない
//! - 読み取りの失敗は `ChunkFailure` に記録する。`AsyncAdapterStream` は読み取りのエラーを
//!   トラックの終端と区別しないため、音声バックエンドがトラックの終了時にこれを見て失敗か終端かを判別し、
//!   失敗ならアクターへ `AudioEvent::TrackFailed` を送る (アクターは `ChunkFailure` を見ない)
//! - URL には署名とクライアントの IP が入るため、ログにもエラーにも URL を出さない
//!
//! 注意:
//! - 取得中のタスクは 1 つだけ持つ。別のチャンクを要求するときは前のタスクを中断する。
//!   中断されたタスクの結果は読まないため、シーク前の取得が新しい位置のバッファを上書きしない
//! - ロックは持たない。URL の差し替えは `ArcSwap` で行う
//!
//! 呼び出し方:
//! ```ignore
//! let config = ChunkConfig::from_config(&cfg);
//! let request = ChunkedRequest::new(client, stream, resolver, config);
//! let failure = request.failure(); // バックエンドがトラックの終了時に failure.get() を見る
//! let cancel = request.cancel_handle(); // 曲を止めるときに cancel.cancel()
//! let input: Input = request.into();
//! ```

use std::future::Future;
use std::io::{Error as IoError, ErrorKind, Result as IoResult, SeekFrom};
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, RANGE};
use reqwest::{Client, StatusCode};
use songbird::input::{
	AsyncAdapterStream, AsyncMediaSource, AudioStream, AudioStreamError, Compose, Input,
};
use symphonia::core::io::MediaSource;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

use crate::config::Config;
use crate::db::now_secs;
use crate::domain::{ResolveError, ResolvedStream, VideoId};
use crate::resolver::cached::CachedResolver;
use crate::resolver::Resolver;

/// 再試行の前に待つ時間。設定項目に無いため固定値にする。
const RETRY_DELAY: Duration = Duration::from_millis(500);
/// 期限前の取り直しを試みた後、次に期限を確認するまでの間隔 (秒)。設定項目に無いため固定値にする。
/// 取り直した期限がまだ余裕の内側でも、チャンクごとに取り直さないようにする。
const EXPIRY_RECHECK_INTERVAL_SECS: i64 = 60;
/// アダプターのリングバッファの大きさ。Songbird 標準の `HttpRequest` と同じ値。
const ADAPTER_BUFFER: usize = 64 * 1024;
/// 現在のチャンクのこの割合 (分子 / 分母) まで読んだら次を先読みする。
/// 読み取りのたびに参照するホットパスのため固定値にする。
const PREFETCH_NUMERATOR: u64 = 1;
const PREFETCH_DENOMINATOR: u64 = 2;

/// 現在時刻 (UNIX ミリ秒)。時計が UNIX 時刻より前なら 0。
fn now_millis() -> i64 {
	std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.ok()
		.and_then(|d| i64::try_from(d.as_millis()).ok())
		.unwrap_or(0)
}

/// `available_at` から、取得を始めてよい時刻 (UNIX ミリ秒) を求める。
///
/// # 引数
/// - `available_at`: 解決結果の利用開始時刻 (UNIX 秒)。None は待たない
/// - `wait_max`: 今から待つ時間の上限
///
/// # 戻り値
/// `min(available_at, 今 + wait_max)` のミリ秒。待たないときは 0
///
/// # 例
/// `let ready = ready_at_ms(stream.available_at, Duration::from_secs(60));`
fn ready_at_ms(available_at: Option<i64>, wait_max: Duration) -> i64 {
	let Some(available_at) = available_at else {
		return 0;
	};
	let cap = now_millis().saturating_add(i64::try_from(wait_max.as_millis()).unwrap_or(i64::MAX));
	available_at.saturating_mul(1000).min(cap)
}

/// 入力の中から URL を取り直す手段。実装は `CachedResolver`。
///
/// 呼び出し方:
/// ```ignore
/// let fresh = refresher.refresh(&video_id, "251").await?;
/// ```
#[async_trait]
pub trait UrlRefresher: Send + Sync {
	/// 同じ形式で URL を取り直す。
	///
	/// # 引数
	/// - `video_id`: 動画 ID
	/// - `format_id`: 最初の解決で得た形式。同じ形式に固定する
	///
	/// # 戻り値
	/// 新しい解決結果
	///
	/// # エラー
	/// 解決の失敗 (`ResolveError`)
	async fn refresh(
		&self,
		video_id: &VideoId,
		format_id: &str,
	) -> Result<ResolvedStream, ResolveError>;
}

#[async_trait]
impl UrlRefresher for CachedResolver {
	/// `Resolver::refresh(video_id, Some(format_id))` へ委譲する。
	///
	/// # 例
	/// `let fresh = UrlRefresher::refresh(&resolver, &video_id, "251").await?;`
	async fn refresh(
		&self,
		video_id: &VideoId,
		format_id: &str,
	) -> Result<ResolvedStream, ResolveError> {
		Resolver::refresh(self, video_id, Some(format_id)).await
	}
}

/// チャンク取得の設定。
#[derive(Debug, Clone)]
pub struct ChunkConfig {
	/// 先読みの 1 回の取得量 (バイト)。0 は 1 として扱う
	pub chunk_size: u64,
	/// 読み手が待つ取得の 1 回の取得量 (バイト)。0、または `chunk_size` 以上なら `chunk_size` と同じ
	pub initial_chunk_size: u64,
	/// 1 回の要求 (送信から本文の受信まで) の上限
	pub fetch_timeout: Duration,
	/// URL の取り直し (`UrlRefresher::refresh`) 1 回の上限
	pub refresh_timeout: Duration,
	/// 1 チャンクの最大試行回数。0 は 1 として扱う
	pub attempts: u32,
	/// 期限までの残りがこれ以下なら取得の前に URL を取り直す
	pub refresh_margin: Duration,
	/// 解決結果の `available_at` まで最初の取得を待つ時間の上限。ZERO は待たない
	pub available_wait_max: Duration,
}

impl ChunkConfig {
	/// 設定から作る。
	///
	/// - 取得は `[playback]` の値を使う
	/// - 取り直しの上限は、ワーカーの応答の上限 (`worker.resolve_timeout_secs`) と、
	///   解決結果の全長を確かめる取得の上限 (`playback.chunk_fetch_timeout_secs`) の和にする。
	///   取り直しはワーカーへの解決の要求で、全長が無ければ HTTP で確かめるため
	///
	/// # 引数
	/// - `config`: 設定全体
	///
	/// # 戻り値
	/// チャンク取得の設定
	///
	/// # 例
	/// `let config = ChunkConfig::from_config(&cfg);`
	pub fn from_config(config: &Config) -> Self {
		let playback = &config.playback;
		Self {
			chunk_size: playback.chunk_size_bytes,
			initial_chunk_size: playback.initial_chunk_size_bytes,
			fetch_timeout: Duration::from_secs(playback.chunk_fetch_timeout_secs),
			refresh_timeout: Duration::from_secs(
				config
					.worker
					.resolve_timeout_secs
					.saturating_add(playback.chunk_fetch_timeout_secs),
			),
			attempts: playback.chunk_fetch_attempts,
			refresh_margin: Duration::from_secs(playback.url_refresh_margin_secs),
			available_wait_max: Duration::from_secs(playback.available_wait_max_secs),
		}
	}
}

/// チャンク取得のエラー。読み取りでは `std::io::Error` に包んで返す。
///
/// 取り出し方:
/// `error.get_ref().and_then(|e| e.downcast_ref::<ChunkError>())`
#[derive(Debug, Clone, thiserror::Error)]
pub enum ChunkError {
	/// 403 を受けた (URL の失効)。
	#[error("URL が拒否されました (403)")]
	Forbidden,
	/// 403 以外の失敗の状態コード。
	#[error("HTTP の状態が不正です: {0}")]
	Status(u16),
	/// 送信または本文の受信の失敗 (切断を含む)。文言は URL を除いた reqwest のエラー。
	/// 作るときは `ChunkError::request` を使う
	#[error("取得に失敗しました: {0}")]
	Request(String),
	/// `fetch_timeout` 以内に終わらなかった。
	#[error("取得がタイムアウトしました")]
	Timeout,
	/// 本文の長さが要求の範囲と違う。
	#[error("受け取ったバイト数が違います: {got} / {expected}")]
	Length { got: u64, expected: u64 },
	/// URL の取り直しに失敗した。
	#[error("URL の取り直しに失敗しました: {0}")]
	Refresh(#[source] ResolveError),
	/// 取り直した結果の全長が最初と違う。バイト位置が使えない。
	#[error("取り直しで全長が変わりました: {old} -> {new}")]
	FilesizeChanged { old: u64, new: u64 },
	/// 取得タスクが異常終了した。
	#[error("取得タスクが異常終了しました: {0}")]
	Task(String),
	/// 全長が 0。再生するものが無い (解決結果の不備)。
	#[error("ストリームの全長が 0 です")]
	Empty,
	/// 曲を止めたため取得を取り消した。失敗として記録しない。
	#[error("取得を取り消しました")]
	Cancelled,
	/// PipeInput の子 (yt-dlp) が途中で失敗した (異常終了、標準出力の読み取りの失敗)。
	#[error("yt-dlp が失敗しました: {0}")]
	Pipe(String),
}

impl ChunkError {
	/// reqwest のエラーから URL を除いて作る。URL には署名とクライアントの IP が入るため。
	///
	/// # 例
	/// `let error = ChunkError::request(e);`
	fn request(error: reqwest::Error) -> Self {
		Self::Request(error.without_url().to_string())
	}

	/// 待てば直りうる一時的な失敗か。再試行するかの判定に使う。
	///
	/// - true: 送信・受信の失敗 (`Request`)、時間切れ (`Timeout`)、5xx と 429 の状態コード
	/// - false: 恒久的な 4xx (404、410、416 など)、長さの不一致、取り直しの失敗、全長の変化、取り消しなど
	/// - 403 (`Forbidden`) は false。URL の取り直しで直すため、再試行とは別に扱う
	///
	/// # 戻り値
	/// 一時的なら true
	///
	/// # 例
	/// `if !error.is_transient() { return Err(error); }`
	pub fn is_transient(&self) -> bool {
		match self {
			Self::Request(_) | Self::Timeout => true,
			Self::Status(code) => StatusCode::from_u16(*code)
				.is_ok_and(|s| s.is_server_error() || s == StatusCode::TOO_MANY_REQUESTS),
			_ => false,
		}
	}

	/// 取得をやり直せば直りうるか。失敗した先読みを読み手が取り直すかの判定に使う。
	///
	/// - 一時的な失敗 (`is_transient`) と、URL の取り直しで直る 403
	///
	/// # 戻り値
	/// やり直す価値があれば true
	///
	/// # 例
	/// `Err(error) if prefetch && error.is_recoverable() => { /* 取り直す */ }`
	pub fn is_recoverable(&self) -> bool {
		self.is_transient() || matches!(self, Self::Forbidden)
	}
}

/// 入力の取得の取り消し。`ChunkedRequest` とそのストリームの取得タスクで共有する。
///
/// - `cancel` の後、進行中の取得・`available_at` の待ち・URL の取り直しは `ChunkError::Cancelled` で終わり、
///   以後の読み取りも同じエラーを返す。取り消しは戻せない
/// - 曲を止める・捨てるときにバックエンドが立てる
///
/// 呼び出し方:
/// ```ignore
/// let cancel = request.cancel_handle();
/// // 曲を止めるとき
/// cancel.cancel();
/// ```
#[derive(Clone)]
pub struct ChunkCancel(Arc<watch::Sender<bool>>);

impl ChunkCancel {
	/// 取り消していない状態で作る。
	///
	/// # 例
	/// `let cancel = ChunkCancel::new();`
	fn new() -> Self {
		let (tx, _rx) = watch::channel(false);
		Self(Arc::new(tx))
	}

	/// 取り消す。何度呼んでもよい。
	///
	/// # 例
	/// `cancel.cancel();`
	pub fn cancel(&self) {
		self.0.send_replace(true);
	}

	/// 取り消したか。
	///
	/// # 例
	/// `if cancel.is_cancelled() { return; }`
	pub fn is_cancelled(&self) -> bool {
		*self.0.borrow()
	}

	/// 取り消されるまで待つ。取り消し済みならすぐに返る。
	///
	/// # 例
	/// `tokio::select! { _ = cancel.cancelled() => {}, _ = work => {} }`
	async fn cancelled(&self) {
		let mut rx = self.0.subscribe();
		// 送り手は self が持つため閉じない。閉じたときも待ち続けずに返る
		let _ = rx.wait_for(|cancelled| *cancelled).await;
	}
}

/// 入力の失敗の記録。`ChunkedRequest` とそこから作ったストリームで共有する。
///
/// - `AsyncAdapterStream` 経由では読み取りのエラーがトラックの終端に見えるため、
///   トラックが終わったときにこれを見て、自然な終端か失敗かを判別する
/// - 最後に起きた失敗だけを持つ
///
/// 呼び出し方:
/// ```ignore
/// let failure = request.failure();
/// // トラックの終了時
/// if let Some(error) = failure.get() { /* 現在位置から再開 */ }
/// ```
#[derive(Clone, Default)]
pub struct ChunkFailure(Arc<ArcSwapOption<ChunkError>>);

impl ChunkFailure {
	/// 記録された失敗を返す。
	///
	/// # 戻り値
	/// 失敗していなければ None
	///
	/// # 例
	/// `let failed = failure.get().is_some();`
	pub fn get(&self) -> Option<Arc<ChunkError>> {
		self.0.load_full()
	}

	/// 失敗を記録する。読み取りがエラーで終わるときに呼ぶ。
	///
	/// # 例
	/// `failure.record(error.clone());`
	pub(crate) fn record(&self, error: ChunkError) {
		self.0.store(Some(Arc::new(error)));
	}

	/// 試験から失敗を記録する。本番のコードからは呼ばない。
	///
	/// # 例
	/// `failure.record_for_test(ChunkError::Forbidden);`
	#[doc(hidden)]
	pub fn record_for_test(&self, error: ChunkError) {
		self.record(error);
	}
}

/// 取得先。取り直すたびに丸ごと差し替える。
struct Source {
	url: String,
	headers: HeaderMap,
	expires_at: Option<i64>,
}

impl Source {
	/// 解決結果から作る。
	///
	/// - 不正なヘッダー名・値は警告して捨てる (要求全体を失敗させないため)
	///
	/// # 例
	/// `let source = Source::from_stream(&stream);`
	fn from_stream(stream: &ResolvedStream) -> Self {
		let mut headers = HeaderMap::new();
		for (name, value) in &stream.http_headers {
			match (
				HeaderName::from_bytes(name.as_bytes()),
				HeaderValue::from_str(value),
			) {
				(Ok(name), Ok(value)) => {
					headers.insert(name, value);
				}
				_ => tracing::warn!(header = %name, "不正なヘッダーを捨てました"),
			}
		}
		Self {
			url: stream.url.clone(),
			headers,
			expires_at: stream.expires_at,
		}
	}
}

/// 期限前の取り直しで得たが、`available_at` が来るまで使わない取得先。
struct PendingSource {
	source: Arc<Source>,
	/// 差し替えてよい時刻 (UNIX ミリ秒)。`available_at` を上限で抑えた値
	ready_at_ms: i64,
}

/// 取得の共有部分。ストリームと取得タスクが `Arc` で共有する。
struct Fetcher {
	client: Client,
	source: ArcSwap<Source>,
	video_id: VideoId,
	format_id: String,
	filesize: u64,
	refresher: Arc<dyn UrlRefresher>,
	config: ChunkConfig,
	/// 期限前の取り直しを次に試みてよい時刻 (UNIX 秒)
	next_expiry_check: AtomicI64,
	/// これより前は取得を始めない時刻 (UNIX ミリ秒)。0 以下は待たない。`available_at` を上限で抑えた値
	ready_at_ms: AtomicI64,
	/// 期限前の取り直しで得た、まだ使えない取得先
	pending_source: ArcSwapOption<PendingSource>,
	failure: ChunkFailure,
	cancel: ChunkCancel,
}

impl Fetcher {
	/// 1 チャンクの大きさを返す。
	///
	/// # 戻り値
	/// `chunk_size`。0 の設定でも止まらないよう 1 以上にする
	///
	/// # 例
	/// `let end = start + fetcher.chunk_size();`
	fn chunk_size(&self) -> u64 {
		self.config.chunk_size.max(1)
	}

	/// 読み手が待つ取得の大きさを返す。
	///
	/// # 戻り値
	/// `initial_chunk_size`。0 または `chunk_size` 以上なら `chunk_size`
	///
	/// # 例
	/// `let start = pos / fetcher.blocking_size() * fetcher.blocking_size();`
	fn blocking_size(&self) -> u64 {
		match self.config.initial_chunk_size {
			0 => self.chunk_size(),
			size => size.min(self.chunk_size()),
		}
	}

	/// 期限前の取り直しが要るかを判定する。要るときは次の確認を `EXPIRY_RECHECK_INTERVAL_SECS` 後に延ばす。
	///
	/// # 戻り値
	/// 取り直すべきなら今の URL の期限 (UNIX 秒)。期限が不明 (None)、余裕がある、
	/// 前回の試みから間が無いときは None
	///
	/// # 例
	/// `if let Some(expires_at) = self.early_refresh_due() { ... }`
	fn early_refresh_due(&self) -> Option<i64> {
		let expires_at = self.source.load().expires_at?;
		let margin = i64::try_from(self.config.refresh_margin.as_secs()).unwrap_or(i64::MAX);
		let now = now_secs();
		if now.saturating_add(margin) < expires_at {
			return None;
		}
		let next = self.next_expiry_check.load(Ordering::Relaxed);
		if now < next {
			return None;
		}
		// 先読みと並んで呼ばれても 1 回だけ取り直す
		self.next_expiry_check
			.compare_exchange(
				next,
				now.saturating_add(EXPIRY_RECHECK_INTERVAL_SECS),
				Ordering::Relaxed,
				Ordering::Relaxed,
			)
			.ok()?;
		Some(expires_at)
	}

	/// URL を取り直して差し替える。
	///
	/// - `UrlRefresher` の呼び出しを `refresh_timeout` で打ち切る
	/// - `early` (期限前の取り直し) で、新しい結果の `available_at` がまだ先なら差し替えずに預け、
	///   今の URL を使い続ける。差し替えは `promote_pending_source` が時刻の到来後に行う
	/// - それ以外 (403 を受けたとき) はすぐに差し替え、次の取得の前に `available_at` まで待たせる。
	///   預けていた取得先は捨てる
	///
	/// # 引数
	/// - `early`: 期限前の取り直しか
	///
	/// # エラー
	/// - `Refresh`: `UrlRefresher` の失敗。打ち切りは `Refresh(ResolveError::Timeout)`
	/// - `FilesizeChanged`: 全長が最初と違う。URL は差し替えない
	///
	/// # 例
	/// `self.refresh(false).await?;`
	async fn refresh(&self, early: bool) -> Result<(), ChunkError> {
		let fresh = timeout(
			self.config.refresh_timeout,
			self.refresher.refresh(&self.video_id, &self.format_id),
		)
		.await
		.map_err(|_| ChunkError::Refresh(ResolveError::Timeout))?
		.map_err(ChunkError::Refresh)?;
		if fresh.filesize != self.filesize {
			return Err(ChunkError::FilesizeChanged {
				old: self.filesize,
				new: fresh.filesize,
			});
		}
		let ready = ready_at_ms(fresh.available_at, self.config.available_wait_max);
		let source = Source::from_stream(&fresh);
		if early && ready > now_millis() {
			self.pending_source.store(Some(Arc::new(PendingSource {
				source: Arc::new(source),
				ready_at_ms: ready,
			})));
			tracing::info!(
				video_id = %self.video_id.0,
				wait_ms = ready.saturating_sub(now_millis()),
				"取り直した URL は available_at まで使えないため、それまで今の URL を使います"
			);
			return Ok(());
		}
		self.pending_source.store(None);
		self.source.store(Arc::new(source));
		self.ready_at_ms.store(ready, Ordering::Relaxed);
		tracing::info!(video_id = %self.video_id.0, "ストリームの URL を取り直しました");
		Ok(())
	}

	/// 預けていた取得先を、時刻が来ていれば今の取得先にする。
	///
	/// - 差し替えるのは `available_at` (を上限で抑えた時刻) を過ぎたとき、または今の URL が期限を過ぎたとき。
	///   後者では差し替えた上で `wait_until_available` が時刻まで待たせる
	/// - 預けていなければ何もしない
	///
	/// # 例
	/// `self.promote_pending_source();`
	fn promote_pending_source(&self) {
		let Some(pending) = self.pending_source.load_full() else {
			return;
		};
		let expired = self
			.source
			.load()
			.expires_at
			.is_some_and(|expires_at| now_secs() >= expires_at);
		if now_millis() < pending.ready_at_ms && !expired {
			return;
		}
		// 並んで呼ばれても 1 回だけ差し替える
		let Some(taken) = self.pending_source.swap(None) else {
			return;
		};
		self.source.store(Arc::clone(&taken.source));
		self.ready_at_ms.store(taken.ready_at_ms, Ordering::Relaxed);
		tracing::info!(video_id = %self.video_id.0, "預けていた URL に差し替えました");
	}

	/// `available_at` (を上限で抑えた時刻) まで待つ。過ぎていれば何もしない。
	///
	/// - 時刻は絶対値で持つため、読み手と先読みが並んで呼んでも同じ時刻に再開し、後の呼び出しは待たない
	/// - 待ちは `fetch_timeout` に含めない (取得の前に行う)
	///
	/// # 例
	/// `self.wait_until_available().await;`
	async fn wait_until_available(&self) {
		let ready = self.ready_at_ms.load(Ordering::Relaxed);
		if ready <= 0 {
			return;
		}
		let remaining = ready.saturating_sub(now_millis());
		let Ok(wait_ms) = u64::try_from(remaining) else {
			return;
		};
		if wait_ms == 0 {
			return;
		}
		tracing::info!(video_id = %self.video_id.0, wait_ms, "available_at まで待ちます");
		sleep(Duration::from_millis(wait_ms)).await;
	}

	/// 範囲を 1 回だけ要求する。
	///
	/// # 引数
	/// - `start`: 開始バイト
	/// - `end`: 終了バイト (終端を含まない)。`start < end` であること
	///
	/// - 206 だけを受け付ける。200 は範囲がファイル全体のときだけ受け付ける
	/// - 本文を読む前に `Content-Length` を確かめ、読む量も要求の範囲までに抑える
	///
	/// # エラー
	/// `Forbidden`、`Status`、`Request`、`Timeout`、`Length`
	async fn fetch_once(&self, start: u64, end: u64) -> Result<Bytes, ChunkError> {
		let source = self.source.load_full();
		let request = self
			.client
			.get(&source.url)
			.headers(source.headers.clone())
			.header(RANGE, format!("bytes={start}-{}", end - 1));
		let expected = end - start;
		let whole_file = start == 0 && end == self.filesize;
		timeout(self.config.fetch_timeout, async {
			let mut response = request.send().await.map_err(ChunkError::request)?;
			let status = response.status();
			if status == StatusCode::FORBIDDEN {
				return Err(ChunkError::Forbidden);
			}
			if status != StatusCode::PARTIAL_CONTENT && !(status == StatusCode::OK && whole_file) {
				return Err(ChunkError::Status(status.as_u16()));
			}
			if let Some(length) = response.content_length() {
				if length != expected {
					return Err(ChunkError::Length {
						got: length,
						expected,
					});
				}
			}
			// 範囲の大きさで確保する (expected は chunk_size 以下)
			let mut data = BytesMut::with_capacity(usize::try_from(expected).unwrap_or(0));
			while let Some(part) = response.chunk().await.map_err(ChunkError::request)? {
				let got = (data.len() + part.len()) as u64;
				if got > expected {
					return Err(ChunkError::Length { got, expected });
				}
				data.extend_from_slice(&part);
			}
			if data.len() as u64 != expected {
				return Err(ChunkError::Length {
					got: data.len() as u64,
					expected,
				});
			}
			Ok(data.freeze())
		})
		.await
		.map_err(|_| ChunkError::Timeout)?
	}

	/// 範囲を取得する。再試行と URL の取り直しを含み、取り消しと競わせる。
	///
	/// - 取り消されたら、取得・待ち・取り直しのどこにいても `Cancelled` で終わる
	///
	/// # 引数
	/// - `start`: 開始バイト
	/// - `end`: 終了バイト (終端を含まない)。`start < end` であること
	///
	/// # 戻り値
	/// 取得したバイト列 (長さは `end - start`)
	///
	/// # エラー
	/// `fetch_with_retry` のエラー、取り消しは `Cancelled`
	///
	/// # 例
	/// `let data = Arc::clone(&fetcher).fetch(0, 1024).await?;`
	async fn fetch(self: Arc<Self>, start: u64, end: u64) -> Result<Bytes, ChunkError> {
		tokio::select! {
			biased;
			() = self.cancel.cancelled() => Err(ChunkError::Cancelled),
			result = self.fetch_with_retry(start, end) => result,
		}
	}

	/// 範囲を取得する。再試行と URL の取り直しを含む。
	///
	/// - 試行の前に期限を確認し、近ければ先に取り直す。期限前に取り直しが失敗したら警告して
	///   今の URL を使い続ける (まだ使えるため)。期限を過ぎていれば失敗を返す
	/// - 期限前の取り直しは `EXPIRY_RECHECK_INTERVAL_SECS` に 1 回まで
	/// - 403 なら取り直して同じ範囲を待たずに要求する。一時的な失敗 (`is_transient`) は `RETRY_DELAY` 待つ
	/// - 恒久的な失敗 (404 などの 4xx、長さの不一致)、取り直しの失敗、全長の変化は再試行せずに返す
	///
	/// # 引数
	/// - `start`: 開始バイト
	/// - `end`: 終了バイト (終端を含まない)。`start < end` であること
	///
	/// # 戻り値
	/// 取得したバイト列 (長さは `end - start`)
	///
	/// # エラー
	/// 試行回数を使い切ったときは最後の失敗。恒久的な失敗と取り直しの失敗はその場で返す
	///
	/// # 例
	/// `let data = self.fetch_with_retry(0, 1024).await?;`
	async fn fetch_with_retry(&self, start: u64, end: u64) -> Result<Bytes, ChunkError> {
		let attempts = self.config.attempts.max(1);
		let mut last_error = ChunkError::Timeout;
		for attempt in 0..attempts {
			let is_last = attempt + 1 == attempts;
			if attempt > 0 && !matches!(last_error, ChunkError::Forbidden) {
				sleep(RETRY_DELAY).await;
			}
			if let Some(expires_at) = self.early_refresh_due() {
				if let Err(error) = self.refresh(true).await {
					if now_secs() >= expires_at {
						return Err(error);
					}
					tracing::warn!(video_id = %self.video_id.0, %error, "期限前の URL の取り直しに失敗しました。今の URL を使い続けます");
				}
			}
			self.promote_pending_source();
			self.wait_until_available().await;
			match self.fetch_once(start, end).await {
				Ok(data) => return Ok(data),
				Err(ChunkError::Forbidden) => {
					tracing::warn!(video_id = %self.video_id.0, start, "403 を受けました");
					if !is_last {
						self.refresh(false).await?;
					}
					last_error = ChunkError::Forbidden;
				}
				Err(error) => {
					tracing::warn!(video_id = %self.video_id.0, start, attempt, %error, "チャンクの取得に失敗しました");
					if !error.is_transient() {
						return Err(error);
					}
					last_error = error;
				}
			}
		}
		Err(last_error)
	}
}

/// 取得中のチャンク。
///
/// - `prefetch` は先読みで始めた取得か。先読みの失敗は読み手が達したときに取り直す
/// - `end` は取得する範囲の終わり (含まない)
struct Pending {
	start: u64,
	end: u64,
	prefetch: bool,
	task: JoinHandle<Result<Bytes, ChunkError>>,
}

/// チャンク単位で読むストリーム。`AsyncRead + AsyncSeek + AsyncMediaSource`。
///
/// 通常は `ChunkedRequest` を `Input` に変換して使う。試験では `ChunkedRequest::into_stream` で直接得る。
///
/// - 破棄すると取得中のタスクを中断する
pub struct ChunkedStream {
	fetcher: Arc<Fetcher>,
	pos: u64,
	buffer: Bytes,
	buffer_start: u64,
	pending: Option<Pending>,
}

impl ChunkedStream {
	/// `start` から始まるチャンクの取得を開始する。
	///
	/// - 同じ位置を取得中なら何もしない (先読みかどうかも始めたときのまま)
	/// - 別の位置を取得中なら中断する。中断した結果は読まない
	/// - 先読みは `chunk_size`、読み手が待つ取得は `blocking_size` だけ取得する
	///
	/// # 引数
	/// - `start`: 開始バイト。全長より小さいこと
	/// - `prefetch`: 先読みとして始めるか
	///
	/// # 例
	/// `self.request_chunk(1024, false);`
	fn request_chunk(&mut self, start: u64, prefetch: bool) {
		if self
			.pending
			.as_ref()
			.is_some_and(|pending| pending.start == start)
		{
			return;
		}
		if let Some(previous) = self.pending.take() {
			previous.task.abort();
		}
		let size = if prefetch {
			self.fetcher.chunk_size()
		} else {
			self.fetcher.blocking_size()
		};
		let end = start.saturating_add(size).min(self.fetcher.filesize);
		let task = tokio::spawn(Arc::clone(&self.fetcher).fetch(start, end));
		self.pending = Some(Pending {
			start,
			end,
			prefetch,
			task,
		});
	}

	/// 位置を読むために待つ取得の開始バイトを返す。
	///
	/// - 取得中の範囲 (先読みを含む) が位置を含むなら、その開始バイト (取得をやり直さない)
	/// - それ以外は、位置を含む `blocking_size` の境界の開始バイト
	///
	/// # 例
	/// `let start = self.chunk_start(self.pos);`
	fn chunk_start(&self, pos: u64) -> u64 {
		if let Some(pending) = &self.pending {
			if pending.start <= pos && pos < pending.end {
				return pending.start;
			}
		}
		let size = self.fetcher.blocking_size();
		pos / size * size
	}
}

impl ChunkedStream {
	/// 失敗の記録を返す。`ChunkedRequest::failure` と同じものを共有する。
	///
	/// # 例
	/// `let failure = stream.failure();`
	pub fn failure(&self) -> ChunkFailure {
		self.fetcher.failure.clone()
	}

	/// 読み取りの失敗を error ログに出し、記録して `io::Error` に包む。
	///
	/// - 取り消し (`Cancelled`) は失敗ではないため記録せず、debug ログだけにする
	///   (止めた曲を `TrackFailed` にしないため)
	///
	/// # 例
	/// `return Poll::Ready(Err(this.fail(error)));`
	fn fail(&self, error: ChunkError) -> IoError {
		if matches!(error, ChunkError::Cancelled) {
			tracing::debug!(video_id = %self.fetcher.video_id.0, pos = self.pos, "取り消したチャンク入力の読み取りを終えます");
		} else {
			tracing::error!(video_id = %self.fetcher.video_id.0, pos = self.pos, %error, "チャンク入力の読み取りに失敗しました");
			self.fetcher.failure.record(error.clone());
		}
		IoError::other(error)
	}
}

impl Drop for ChunkedStream {
	fn drop(&mut self) {
		if let Some(pending) = self.pending.take() {
			pending.task.abort();
		}
	}
}

impl AsyncRead for ChunkedStream {
	/// バッファから返す。無ければ現在位置のチャンクを取得して待つ。
	///
	/// - 終端以降は 0 バイト (EOF)
	/// - 取得の失敗は `ChunkError` を包んだ `io::Error`。同時に error ログを出し、`ChunkFailure` に記録する
	/// - `AsyncAdapterStream` 経由では、このエラーで非同期側が止まり、トラックは終端として終わる。
	///   失敗か終端かは `ChunkFailure` で判別する
	/// - 直接使う場合は、エラーの後の読み取りで同じチャンクの取得をやり直す
	/// - 先読みが失敗していたチャンクは、読み手が達したときに 1 回だけ取り直す。
	///   先読みは読み手より数十秒早く走るため、その間の一時的な回線断で入力を失敗させない。
	///   取り直すのは、やり直せば直りうる失敗 (`ChunkError::is_recoverable`) だけ
	/// - 取り消した後は新しい取得を始めず `Cancelled` を返す (記録しない)
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<IoResult<()>> {
		let this = self.get_mut();
		loop {
			if this.pos >= this.fetcher.filesize || buf.remaining() == 0 {
				return Poll::Ready(Ok(()));
			}
			let buffer_end = this.buffer_start + this.buffer.len() as u64;
			if this.pos >= this.buffer_start && this.pos < buffer_end {
				// 位置は buffer 内にあるため usize に収まる
				let offset = (this.pos - this.buffer_start) as usize;
				let count = buf.remaining().min(this.buffer.len() - offset);
				buf.put_slice(&this.buffer[offset..offset + count]);
				this.pos += count as u64;
				let threshold =
					this.buffer.len() as u64 * PREFETCH_NUMERATOR / PREFETCH_DENOMINATOR;
				if this.pos - this.buffer_start >= threshold
					&& buffer_end < this.fetcher.filesize
					&& !this.fetcher.cancel.is_cancelled()
				{
					this.request_chunk(buffer_end, true);
				}
				return Poll::Ready(Ok(()));
			}
			if this.fetcher.cancel.is_cancelled() {
				return Poll::Ready(Err(this.fail(ChunkError::Cancelled)));
			}
			let start = this.chunk_start(this.pos);
			this.request_chunk(start, false);
			let Some(pending) = this.pending.as_mut() else {
				return Poll::Ready(Err(
					this.fail(ChunkError::Task("取得タスクがありません".to_owned()))
				));
			};
			match Pin::new(&mut pending.task).poll(cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(joined) => {
					let prefetch = pending.prefetch;
					this.pending = None;
					let result =
						joined.unwrap_or_else(|error| Err(ChunkError::Task(error.to_string())));
					match result {
						Ok(data) => {
							this.buffer = data;
							this.buffer_start = start;
						}
						Err(error) if prefetch && error.is_recoverable() => {
							// pending を空にしたため、次の周回で先読みでない取得を始める
							tracing::warn!(video_id = %this.fetcher.video_id.0, start, %error, "先読みが失敗していたため取り直します");
						}
						Err(error) => return Poll::Ready(Err(this.fail(error))),
					}
				}
			}
		}
	}
}

impl AsyncSeek for ChunkedStream {
	/// 読み取り位置を変える。取得は次の読み取りで行う。
	///
	/// # エラー
	/// 先頭より前への移動は `InvalidInput`
	fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> IoResult<()> {
		let this = self.get_mut();
		let target = match position {
			SeekFrom::Start(offset) => i128::from(offset),
			SeekFrom::Current(offset) => i128::from(this.pos) + i128::from(offset),
			SeekFrom::End(offset) => i128::from(this.fetcher.filesize) + i128::from(offset),
		};
		this.pos = u64::try_from(target)
			.map_err(|_| IoError::new(ErrorKind::InvalidInput, "先頭より前へのシーク"))?;
		Ok(())
	}

	/// シークを完了する。`start_seek` で位置が決まっているため常にすぐ終わる。
	///
	/// # 戻り値
	/// 新しい位置
	fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<IoResult<u64>> {
		Poll::Ready(Ok(self.pos))
	}
}

#[async_trait]
impl AsyncMediaSource for ChunkedStream {
	/// 任意の位置へシークできる。常に true。
	fn is_seekable(&self) -> bool {
		true
	}

	/// 全長 (解決結果の `filesize`) を返す。通信はしない。
	async fn byte_len(&self) -> Option<u64> {
		Some(self.fetcher.filesize)
	}
}

/// `Input` へ変換できる遅延入力。`create_async` のたびに同じ URL 領域を共有するストリームを作る。
///
/// 呼び出し方:
/// ```ignore
/// let request = ChunkedRequest::new(client, stream, resolver, ChunkConfig::from_config(&cfg));
/// let failure = request.failure();
/// let input: Input = request.into();
/// ```
pub struct ChunkedRequest {
	fetcher: Arc<Fetcher>,
}

impl ChunkedRequest {
	/// 入力を作る。通信はまだ行わない。
	///
	/// # 引数
	/// - `client`: 共有の HTTP クライアント
	/// - `stream`: 解決結果。`filesize` は正確な全長であること
	/// - `refresher`: URL の取り直しに使う (`CachedResolver` など)
	/// - `config`: チャンク取得の設定
	///
	/// # 例
	/// `let request = ChunkedRequest::new(client, stream, resolver, config);`
	pub fn new(
		client: Client,
		stream: ResolvedStream,
		refresher: Arc<dyn UrlRefresher>,
		config: ChunkConfig,
	) -> Self {
		let config_wait_max = config.available_wait_max;
		let fetcher = Fetcher {
			client,
			source: ArcSwap::from_pointee(Source::from_stream(&stream)),
			video_id: stream.video_id,
			format_id: stream.format_id,
			filesize: stream.filesize,
			refresher,
			config,
			next_expiry_check: AtomicI64::new(i64::MIN),
			ready_at_ms: AtomicI64::new(ready_at_ms(stream.available_at, config_wait_max)),
			pending_source: ArcSwapOption::empty(),
			failure: ChunkFailure::default(),
			cancel: ChunkCancel::new(),
		};
		Self {
			fetcher: Arc::new(fetcher),
		}
	}

	/// 失敗の記録を返す。`Input` に変換する前に取っておき、トラックの終了時に見る。
	///
	/// # 例
	/// `let failure = request.failure(); let input: Input = request.into();`
	pub fn failure(&self) -> ChunkFailure {
		self.fetcher.failure.clone()
	}

	/// 取り消しの口を返す。`Input` に変換する前に取っておき、曲を止めるときに `cancel` を呼ぶ。
	///
	/// - この入力から作ったすべてのストリームの取得を取り消す
	///
	/// # 例
	/// `let cancel = request.cancel_handle(); let input: Input = request.into();`
	pub fn cancel_handle(&self) -> ChunkCancel {
		self.fetcher.cancel.clone()
	}

	/// 先頭位置のストリームを作る。通信はしない。
	///
	/// # 例
	/// `let stream = self.make_stream();`
	fn make_stream(&self) -> ChunkedStream {
		ChunkedStream {
			fetcher: Arc::clone(&self.fetcher),
			pos: 0,
			buffer: Bytes::new(),
			buffer_start: 0,
			pending: None,
		}
	}

	/// アダプターを通さずにストリームを得る。試験用。
	///
	/// # 例
	/// `let mut stream = request.into_stream(); stream.read_to_end(&mut out).await?;`
	pub fn into_stream(self) -> ChunkedStream {
		self.make_stream()
	}
}

#[async_trait]
impl Compose for ChunkedRequest {
	/// 同期での作成は扱わない。`should_create_async` が true のため呼ばれない。
	///
	/// # エラー
	/// 常に `AudioStreamError::Unsupported`
	fn create(&mut self) -> Result<AudioStream<Box<dyn MediaSource>>, AudioStreamError> {
		Err(AudioStreamError::Unsupported)
	}

	/// ストリームを `AsyncAdapterStream` で包んで返す。通信は最初の読み取りで行う。
	///
	/// # エラー
	/// 全長が 0 なら `AudioStreamError::Fail(ChunkError::Empty)`。`ChunkFailure` にも記録する
	async fn create_async(
		&mut self,
	) -> Result<AudioStream<Box<dyn MediaSource>>, AudioStreamError> {
		if self.fetcher.filesize == 0 {
			tracing::error!(video_id = %self.fetcher.video_id.0, "全長 0 のストリームは再生できません");
			self.fetcher.failure.record(ChunkError::Empty);
			return Err(AudioStreamError::Fail(Box::new(ChunkError::Empty)));
		}
		let adapter = AsyncAdapterStream::new(Box::new(self.make_stream()), ADAPTER_BUFFER);
		Ok(AudioStream {
			input: Box::new(adapter) as Box<dyn MediaSource>,
		})
	}

	/// 作成は非同期で行う。常に true。
	fn should_create_async(&self) -> bool {
		true
	}
}

impl From<ChunkedRequest> for Input {
	/// 遅延入力 (`Input::Lazy`) に変換する。
	///
	/// # 例
	/// `let input: Input = request.into();`
	fn from(request: ChunkedRequest) -> Self {
		Input::Lazy(Box::new(request))
	}
}
