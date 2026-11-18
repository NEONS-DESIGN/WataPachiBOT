//! SQLite のキャッシュを見てから WorkerClient に渡す `Resolver`。
//!
//! - キャッシュの読み書きの失敗は警告ログだけにして、ワーカーへの問い合わせは続ける
//!   (キャッシュは高速化の手段で、壊れていても再生できること)
//! - エラーはキャッシュしない。保存は成功した結果だけ
//! - 解決で動画が削除済み、非公開、地域制限と分かったら、その動画を指す検索のキャッシュを消す
//!   (同じ検索語で使えない動画を `search_days` の間返し続けないため)
//! - ワーカーが持ち時間で途中までにした一覧 (`truncated`) は、全件と扱わずに保存する
//! - 時刻は実時間。期限の判断は `expires_at` と `playback.url_refresh_margin_secs` で行う
//! - `expires_at` が不明 (None) の結果はキャッシュしない
//! - 応答の型は厳密に検査する。必須項目の欠落は `Worker(Internal)`、一覧の不正な要素は捨てる
//! - `filesize` が無い応答は `StreamLength` で全長を取る
//! - ミスのときのキャッシュの保存は切り離して行い、結果を待たずに返す。保存の失敗は警告ログだけにする
//!
//! 呼び出し方:
//! ```ignore
//! let http = reqwest::Client::builder().build()?;
//! let length = Arc::new(HttpStreamLength::new(http.clone(), Duration::from_secs(cfg.playback.chunk_fetch_timeout_secs)));
//! let resolver = CachedResolver::new(pool, db.handles.clone(), length, cfg.cache, cfg.playback);
//! let stream = resolver.resolve(&video_id, Priority::Interactive).await?;
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Map, Value};
use unicode_normalization::UnicodeNormalization;

use crate::config::{CacheConfig, PlaybackConfig};
use crate::db::{cache, now_secs, Connection, DbError, DbHandles, DbWriter};
use crate::domain::{
	PlaylistMeta, Priority, ResolveError, ResolveErrorCode, ResolvedStream, TrackMeta, VideoId,
};
use crate::resolver::pool::WorkerClient;
use crate::resolver::protocol::WorkerRequest;
use crate::resolver::Resolver;

/// 動画 ID からワーカーへ渡す URL を作る接頭辞。YouTube 専用のため固定値にする。
const WATCH_URL_PREFIX: &str = "https://www.youtube.com/watch?v=";
/// 検索で取得する件数。先頭 1 件だけ使う。
const SEARCH_LIMIT: u32 = 1;
/// 1 日の秒数。
const SECS_PER_DAY: i64 = 86_400;
/// 全長の取得に使う Range ヘッダの値。先頭の 1 バイトだけを求め、応答の `Content-Range` から全長を読む。
const RANGE_FIRST_BYTE: &str = "bytes=0-0";
/// 解決でこのエラーを受けた動画は、検索のキャッシュから消す。設定を変えても結果が変わらない失敗に限る
/// (年齢制限は Cookie の更新で、回数制限と内部エラーは時間が経てば解決できることがあるため含めない)。
const UNAVAILABLE_CODES: [ResolveErrorCode; 3] = [
	ResolveErrorCode::NotFound,
	ResolveErrorCode::Private,
	ResolveErrorCode::GeoBlocked,
];
/// `https` 配信のプロトコル名。全長が要るのはこれだけ。
const HTTPS_PROTOCOL: &str = "https";
/// 浮動小数で正確に表せる整数の上限 (2^53)。
const MAX_EXACT_FLOAT_INT: f64 = 9_007_199_254_740_992.0;

/// ストリームの全長 (バイト) を得る手段。`filesize` が無い応答の補完に使う。
///
/// 試験では偽実装を渡す。実装は `HttpStreamLength`。
#[async_trait]
pub trait StreamLength: Send + Sync {
	/// URL の全長を返す。
	///
	/// # 引数
	/// - `url`: ストリームの URL
	/// - `headers`: 取得時に付けるヘッダ
	///
	/// # 戻り値
	/// 全長 (バイト)
	///
	/// # エラー
	/// 取得失敗、タイムアウト、全長が読めない場合は `ResolveError::Worker` (`Internal`)
	async fn length(
		&self,
		url: &str,
		headers: &BTreeMap<String, String>,
	) -> Result<u64, ResolveError>;
}

/// `Range: bytes=0-0` を要求して `Content-Range` から全長を得る実装。
pub struct HttpStreamLength {
	client: reqwest::Client,
	timeout: Duration,
}

impl HttpStreamLength {
	/// 共有の HTTP クライアントで作る。
	///
	/// - クライアントはチャンクの取得と共有する (googlevideo への接続を使い回すため)
	/// - 上限はクライアントではなく、要求ごとに付ける
	///
	/// # 引数
	/// - `client`: 共有の HTTP クライアント
	/// - `timeout`: 1 回の要求全体の上限 (`playback.chunk_fetch_timeout_secs` を使う)
	///
	/// # 戻り値
	/// 全長を得る手段
	///
	/// # 例
	/// `let length = HttpStreamLength::new(http.clone(), Duration::from_secs(15));`
	pub fn new(client: reqwest::Client, timeout: Duration) -> Self {
		Self { client, timeout }
	}
}

/// 内部エラー (`Internal`) の `ResolveError` を作る。
///
/// # 引数
/// - `message`: エラーの説明 (利用者には見せず、ログに出る)
///
/// # 戻り値
/// `ResolveError::Worker { code: Internal, .. }`
///
/// # 例
/// `return Err(internal("Content-Length がありません"));`
fn internal(message: impl Into<String>) -> ResolveError {
	ResolveError::Worker {
		code: ResolveErrorCode::Internal,
		message: message.into(),
	}
}

/// `Content-Range` の値から全長を取り出す。
///
/// # 引数
/// - `value`: `bytes 0-0/12345` 形式の値
///
/// # 戻り値
/// 全長。全長が `*` (不明) や形式不正なら None
///
/// # 例
/// `assert_eq!(parse_content_range_total("bytes 0-0/12345"), Some(12345));`
pub fn parse_content_range_total(value: &str) -> Option<u64> {
	let (_, total) = value.trim().rsplit_once('/')?;
	total.trim().parse().ok()
}

#[async_trait]
impl StreamLength for HttpStreamLength {
	async fn length(
		&self,
		url: &str,
		headers: &BTreeMap<String, String>,
	) -> Result<u64, ResolveError> {
		let mut request = self.client.get(url).timeout(self.timeout);
		for (name, value) in headers {
			request = request.header(name, value);
		}
		let response = request
			.header(reqwest::header::RANGE, RANGE_FIRST_BYTE)
			.send()
			.await
			// URL には署名とクライアントの IP が入るため、エラーの文言から除く
			.map_err(|e| internal(format!("全長の取得に失敗しました: {}", e.without_url())))?;
		let status = response.status();
		if status == reqwest::StatusCode::PARTIAL_CONTENT {
			let total = response
				.headers()
				.get(reqwest::header::CONTENT_RANGE)
				.and_then(|v| v.to_str().ok())
				.and_then(parse_content_range_total);
			return total.ok_or_else(|| internal("Content-Range から全長を読めません"));
		}
		if status.is_success() {
			// Range を無視した 200。本文は読まずに捨て、Content-Length を全長とする
			return response
				.content_length()
				.ok_or_else(|| internal("Content-Length がありません"));
		}
		Err(internal(format!("全長の取得で HTTP {status}")))
	}
}

/// 検索語をキャッシュの鍵にそろえる。NFKC にして小文字化する。
///
/// # 例
/// `assert_eq!(normalize_query("ＹＯＡＳＯＢＩ"), "yoasobi");`
pub fn normalize_query(query: &str) -> String {
	query.nfkc().collect::<String>().to_lowercase()
}

/// 応答から取り出した解決結果 (全長の補完前)。
struct ParsedStream {
	title: String,
	duration_s: Option<u32>,
	url: String,
	http_headers: BTreeMap<String, String>,
	filesize: Option<u64>,
	format_id: String,
	expires_at: Option<i64>,
	available_at: Option<i64>,
	is_live: bool,
	protocol: String,
}

/// 応答の形が不正なときのエラー。ワーカーの `internal` と同じ扱い。
///
/// # 引数
/// - `what`: どこが不正かの説明
///
/// # 戻り値
/// `ResolveError::Worker { code: Internal, .. }`
///
/// # 例
/// `return Err(malformed("entries が配列ではありません"));`
fn malformed(what: impl std::fmt::Display) -> ResolveError {
	internal(format!("応答の形式が不正です: {what}"))
}

/// JSON の数を整数にする。整数値の浮動小数 (`1793000000.0`) も受け付ける。
///
/// # 引数
/// - `value`: JSON の値
///
/// # 戻り値
/// 整数。数でない、小数部がある、非有限、2^53 を超える浮動小数は `None`
///
/// # 例
/// `assert_eq!(as_integer(&json!(1793000000.0)), Some(1_793_000_000));`
fn as_integer(value: &Value) -> Option<i64> {
	if let Some(n) = value.as_i64() {
		return Some(n);
	}
	let f = value.as_f64()?;
	// 2^53 を超える値は浮動小数で正確に表せないので拒否する
	(f.is_finite() && f.fract() == 0.0 && f.abs() <= MAX_EXACT_FLOAT_INT).then_some(f as i64)
}

/// 任意の文字列の項目を読む。
///
/// # 引数
/// - `map`: 応答の連想配列
/// - `key`: 項目名
///
/// # 戻り値
/// 文字列。項目が無い、または null なら `None`
///
/// # エラー
/// 文字列でない値なら `malformed` (`Internal`)
///
/// # 例
/// `let title = opt_string(map, "title")?;`
fn opt_string(map: &Map<String, Value>, key: &str) -> Result<Option<String>, ResolveError> {
	match map.get(key) {
		None | Some(Value::Null) => Ok(None),
		Some(Value::String(text)) => Ok(Some(text.clone())),
		Some(_) => Err(malformed(format!("{key} が文字列ではありません"))),
	}
}

/// 必須の文字列の項目を読む。空文字列は無いものとして扱う。
///
/// # 引数
/// - `map`: 応答の連想配列
/// - `key`: 項目名
///
/// # 戻り値
/// 空でない文字列
///
/// # エラー
/// 無い、null、空、文字列でない値なら `malformed` (`Internal`)
///
/// # 例
/// `let url = required_string(map, "url")?;`
fn required_string(map: &Map<String, Value>, key: &str) -> Result<String, ResolveError> {
	opt_string(map, key)?
		.filter(|text| !text.is_empty())
		.ok_or_else(|| malformed(format!("{key} がありません")))
}

/// 任意の整数の項目を読む。整数値の浮動小数も受け付ける (`as_integer` と同じ)。
///
/// # 引数
/// - `map`: 応答の連想配列
/// - `key`: 項目名
///
/// # 戻り値
/// 整数。項目が無い、または null なら `None`
///
/// # エラー
/// 整数として読めない値なら `malformed` (`Internal`)
///
/// # 例
/// `let expires_at = opt_integer(map, "expires_at")?;`
fn opt_integer(map: &Map<String, Value>, key: &str) -> Result<Option<i64>, ResolveError> {
	match map.get(key) {
		None | Some(Value::Null) => Ok(None),
		Some(value) => as_integer(value)
			.map(Some)
			.ok_or_else(|| malformed(format!("{key} が整数ではありません"))),
	}
}

/// 秒数の項目 (小数もある) を、四捨五入した整数秒で読む。`u32` を超える値は上限に丸める。
///
/// # 引数
/// - `map`: 応答の連想配列
/// - `key`: 項目名
///
/// # 戻り値
/// 整数秒。無い、null、負、非有限なら `None`
///
/// # エラー
/// 数でない値なら `malformed` (`Internal`)
///
/// # 例
/// `let duration_s = opt_secs(map, "duration")?;  // 212.6 なら Some(213)`
fn opt_secs(map: &Map<String, Value>, key: &str) -> Result<Option<u32>, ResolveError> {
	match map.get(key) {
		None | Some(Value::Null) => Ok(None),
		Some(value) => {
			let f = value
				.as_f64()
				.ok_or_else(|| malformed(format!("{key} が数ではありません")))?;
			Ok((f.is_finite() && f >= 0.0).then(|| f.round().min(f64::from(u32::MAX)) as u32))
		}
	}
}

/// resolve 応答を解釈する。
///
/// - `url` と `title` は必須。ライブ以外は `protocol` と `format_id` も必須
/// - 型が違う項目はエラー。null と欠落は同じ扱い
///
/// # 引数
/// - `value`: ワーカーの成功応答の `result`
///
/// # 戻り値
/// 解釈した結果 (全長の補完前)。`filesize` が 0 以下なら `None`
///
/// # エラー
/// 連想配列でない、必須項目の欠落、型の違いは `malformed` (`Internal`)
///
/// # 例
/// `let parsed = parse_stream(&value)?;`
fn parse_stream(value: &Value) -> Result<ParsedStream, ResolveError> {
	let map = value
		.as_object()
		.ok_or_else(|| malformed("resolve の result が連想配列ではありません"))?;
	let is_live = match map.get("is_live") {
		None | Some(Value::Null) => false,
		Some(Value::Bool(b)) => *b,
		Some(_) => return Err(malformed("is_live が真偽値ではありません")),
	};
	let http_headers = match map.get("http_headers") {
		None | Some(Value::Null) => BTreeMap::new(),
		Some(Value::Object(headers)) => headers
			.iter()
			.map(|(k, v)| match v {
				Value::String(text) => Ok((k.clone(), text.clone())),
				_ => Err(malformed("http_headers の値が文字列ではありません")),
			})
			.collect::<Result<_, _>>()?,
		Some(_) => return Err(malformed("http_headers が連想配列ではありません")),
	};
	let (protocol, format_id) = if is_live {
		(
			opt_string(map, "protocol")?.unwrap_or_default(),
			opt_string(map, "format_id")?.unwrap_or_default(),
		)
	} else {
		(
			required_string(map, "protocol")?,
			required_string(map, "format_id")?,
		)
	};
	Ok(ParsedStream {
		title: required_string(map, "title")?,
		duration_s: opt_secs(map, "duration")?,
		url: required_string(map, "url")?,
		http_headers,
		// 0 以下は不明として扱う
		filesize: opt_integer(map, "filesize")?
			.and_then(|n| u64::try_from(n).ok().filter(|n| *n > 0)),
		format_id,
		expires_at: opt_integer(map, "expires_at")?,
		available_at: opt_integer(map, "available_at")?,
		is_live,
		protocol,
	})
}

/// 一覧の要素を `TrackMeta` にする。
///
/// # 引数
/// - `value`: 一覧応答の `entries` の要素
///
/// # 戻り値
/// 曲の情報。`title` が無ければ動画 ID を題名にする。サムネイルは持たない
///
/// # エラー
/// 連想配列でない、`id` が無い、型が違う項目は `malformed` (`Internal`)
///
/// # 例
/// `let track = parse_entry(&json!({"id": "abcdefghijk", "title": "A", "duration": 1}))?;`
fn parse_entry(value: &Value) -> Result<TrackMeta, ResolveError> {
	let map = value
		.as_object()
		.ok_or_else(|| malformed("要素が連想配列ではありません"))?;
	let id = required_string(map, "id")?;
	Ok(TrackMeta {
		title: opt_string(map, "title")?.unwrap_or_else(|| id.clone()),
		video_id: VideoId(id),
		duration_s: opt_secs(map, "duration")?,
		thumbnail: None,
	})
}

/// 一覧応答を解釈した結果。
struct ParsedList {
	title: Option<String>,
	entries: Vec<TrackMeta>,
	/// 不正で捨てた要素の数。
	skipped: usize,
	/// ワーカーが持ち時間のうちに読み切れず、途中までにした一覧か。
	truncated: bool,
}

impl ParsedList {
	/// 全件を取得できたとは言えない一覧か。
	///
	/// - 捨てた要素がある、またはワーカーが途中までにした一覧は、件数が上限に満たなくても全件とは言えない
	///
	/// # 戻り値
	/// 全件と言えなければ真
	///
	/// # 例
	/// `let fetch_limit = if list.incomplete() { list.entries.len() } else { limit };`
	fn incomplete(&self) -> bool {
		self.skipped > 0 || self.truncated
	}
}

/// 一覧応答 (flat_playlist、search) を解釈する。
///
/// - 不正な要素は警告ログを出して捨てる
/// - `truncated` は任意の真偽値 (無い、null なら偽)
///
/// # 引数
/// - `value`: ワーカーの成功応答の `result`
///
/// # 戻り値
/// 解釈した一覧
///
/// # エラー
/// 応答全体の形が違うとき (`malformed`、`Internal`)
///
/// # 例
/// `let list = parse_list(&value)?;`
fn parse_list(value: &Value) -> Result<ParsedList, ResolveError> {
	let map = value
		.as_object()
		.ok_or_else(|| malformed("一覧の result が連想配列ではありません"))?;
	let title = opt_string(map, "title")?;
	let truncated = match map.get("truncated") {
		None | Some(Value::Null) => false,
		Some(Value::Bool(flag)) => *flag,
		Some(_) => return Err(malformed("truncated が真偽値ではありません")),
	};
	let raw = match map.get("entries") {
		None | Some(Value::Null) => &[][..],
		Some(Value::Array(items)) => items.as_slice(),
		Some(_) => return Err(malformed("entries が配列ではありません")),
	};
	let mut entries = Vec::with_capacity(raw.len());
	let mut skipped = 0;
	for item in raw {
		match parse_entry(item) {
			Ok(entry) => entries.push(entry),
			Err(e) => {
				skipped += 1;
				tracing::warn!("不正な一覧の要素を捨てます: {e}");
			}
		}
	}
	Ok(ParsedList {
		title,
		entries,
		skipped,
		truncated,
	})
}

/// キャッシュへの書き込み (保存と削除) を切り離して行う。結果は待たない。
///
/// - 解決の結果を返すのを DB の書き込みで遅らせないため、`tokio::spawn` で書く
/// - 失敗は `what` を添えて警告ログに出すだけにする (キャッシュは高速化の手段のため)
/// - 終了時に書き込みスレッドが閉じていれば `WriterClosed` の警告になる
/// - tokio のランタイムの中で呼ぶこと
///
/// # 引数
/// - `writer`: 書き込み先
/// - `what`: ログに出す書き込みの種類 (`ストリーム` など)
/// - `store`: 書き込み接続で行う処理
///
/// # 例
/// `store_detached(&self.db.writer, "検索", move |c| cache::put_search(c, &key, &meta, now_secs()));`
fn store_detached<F>(writer: &DbWriter, what: &'static str, store: F)
where
	F: FnOnce(&Connection) -> Result<(), DbError> + Send + 'static,
{
	let writer = writer.clone();
	tokio::spawn(async move {
		if let Err(e) = writer.run(store).await {
			tracing::warn!("{what}のキャッシュの書き込みに失敗: {e}");
		}
	});
}

/// 解決の失敗が、動画が使えないこと (削除済み、非公開、地域制限) を示すか。
///
/// # 引数
/// - `error`: 解決の失敗
///
/// # 戻り値
/// ワーカーの失敗で、コードが `UNAVAILABLE_CODES` のいずれかなら真
///
/// # 例
/// `if is_unavailable(&error) { /* 検索のキャッシュから消す */ }`
fn is_unavailable(error: &ResolveError) -> bool {
	matches!(error, ResolveError::Worker { code, .. } if UNAVAILABLE_CODES.contains(code))
}

/// キャッシュを持つ `Resolver`。
pub struct CachedResolver {
	client: Arc<dyn WorkerClient>,
	db: DbHandles,
	length: Arc<dyn StreamLength>,
	cache: CacheConfig,
	playback: PlaybackConfig,
}

impl CachedResolver {
	/// 組み立てる。
	///
	/// # 引数
	/// - `client`: ワーカーへの要求の窓口 (通常は `WorkerPool`)
	/// - `db`: キャッシュの置き場
	/// - `length`: `filesize` が無い応答の補完手段
	/// - `cache`: `[cache]` の設定
	/// - `playback`: `[playback]` の設定
	pub fn new(
		client: Arc<dyn WorkerClient>,
		db: DbHandles,
		length: Arc<dyn StreamLength>,
		cache: CacheConfig,
		playback: PlaybackConfig,
	) -> Self {
		Self {
			client,
			db,
			length,
			cache,
			playback,
		}
	}

	/// ワーカーに解決させ、全長を補完し、キャッシュの保存を切り離して始める。
	///
	/// - ワーカーが動画を使えない (削除済み、非公開、地域制限) と返したら、その動画を指す検索のキャッシュの
	///   削除を切り離して始める。削除は待たない
	///
	/// # 引数
	/// - `video_id`: 動画 ID
	/// - `format_id`: Some なら同じ形式に固定する
	/// - `priority`: 待ち行列の段
	///
	/// # 戻り値
	/// 解決結果。`filesize` は常に埋まっている (長さの無いものは 0)
	///
	/// # エラー
	/// ワーカーの失敗、応答の不正、全長の取得失敗。キャッシュの書き込みは待たず、失敗は警告のみ
	///
	/// # 例
	/// `let stream = self.fetch_stream(&video_id, None, Priority::Interactive).await?;`
	async fn fetch_stream(
		&self,
		video_id: &VideoId,
		format_id: Option<&str>,
		priority: Priority,
	) -> Result<ResolvedStream, ResolveError> {
		let request = WorkerRequest::Resolve {
			url: format!("{WATCH_URL_PREFIX}{}", video_id.0),
			format_id: format_id.map(str::to_string),
		};
		let value = match self.client.submit(request, priority).await {
			Ok(value) => value,
			Err(error) => {
				if is_unavailable(&error) {
					self.forget_searches_for(video_id);
				}
				return Err(error);
			}
		};
		let parsed = parse_stream(&value)?;
		// 全長が要るのは https で配信されるもののみ。ライブと他のプロトコルは PipeInput になる
		let filesize = match parsed.filesize {
			Some(size) => size,
			None if !parsed.is_live && parsed.protocol == HTTPS_PROTOCOL => {
				self.length
					.length(&parsed.url, &parsed.http_headers)
					.await?
			}
			None => 0,
		};
		let stream = ResolvedStream {
			video_id: video_id.clone(),
			title: parsed.title,
			duration_s: parsed.duration_s,
			url: parsed.url,
			http_headers: parsed.http_headers,
			filesize,
			format_id: parsed.format_id,
			// None は期限不明。保存せず、期限の確認も省く (403 を受けたときの refresh に任せる)
			expires_at: parsed.expires_at,
			available_at: parsed.available_at,
			is_live: parsed.is_live,
			protocol: parsed.protocol,
		};
		let to_store = stream.clone();
		store_detached(&self.db.writer, "ストリーム", move |c| {
			cache::put_stream(c, &to_store)
		});
		Ok(stream)
	}

	/// 使えない動画を指す検索のキャッシュの削除を、切り離して始める。結果は待たない。
	///
	/// # 引数
	/// - `video_id`: 削除済み、非公開、地域制限と分かった動画の ID
	///
	/// # 例
	/// `self.forget_searches_for(&video_id);`
	fn forget_searches_for(&self, video_id: &VideoId) {
		let id = video_id.clone();
		store_detached(&self.db.writer, "使えない動画の検索", move |c| {
			let removed = cache::delete_search_by_video(c, &id)?;
			if removed > 0 {
				tracing::info!(video_id = %id.0, removed, "使えない動画を指す検索のキャッシュを消しました");
			}
			Ok(())
		});
	}
}

#[async_trait]
impl Resolver for CachedResolver {
	async fn resolve(
		&self,
		video_id: &VideoId,
		priority: Priority,
	) -> Result<ResolvedStream, ResolveError> {
		let id = video_id.clone();
		let margin = i64::try_from(self.playback.url_refresh_margin_secs).unwrap_or(i64::MAX);
		let hit = self
			.db
			.reader
			.read(move |c| cache::get_stream(c, &id, now_secs(), margin))
			.await;
		match hit {
			Ok(Some(stream)) => return Ok(stream),
			Ok(None) => {}
			Err(e) => tracing::warn!("ストリームのキャッシュ読み取りに失敗: {e}"),
		}
		self.fetch_stream(video_id, None, priority).await
	}

	async fn refresh(
		&self,
		video_id: &VideoId,
		format_id: Option<&str>,
	) -> Result<ResolvedStream, ResolveError> {
		self.fetch_stream(video_id, format_id, Priority::Interactive)
			.await
	}

	async fn search(&self, query: &str, priority: Priority) -> Result<TrackMeta, ResolveError> {
		let key = normalize_query(query);
		let ttl = i64::from(self.cache.search_days).saturating_mul(SECS_PER_DAY);
		let lookup = key.clone();
		let hit = self
			.db
			.reader
			.read(move |c| cache::get_search(c, &lookup, now_secs(), ttl))
			.await;
		match hit {
			Ok(Some(meta)) => return Ok(meta),
			Ok(None) => {}
			Err(e) => tracing::warn!("検索のキャッシュ読み取りに失敗: {e}"),
		}
		let request = WorkerRequest::Search {
			query: query.to_string(),
			limit: SEARCH_LIMIT,
		};
		let value = self.client.submit(request, priority).await?;
		let meta = parse_list(&value)?
			.entries
			.into_iter()
			.next()
			.ok_or_else(|| ResolveError::Worker {
				code: ResolveErrorCode::NotFound,
				message: "検索結果がありません".to_string(),
			})?;
		let to_store = meta.clone();
		store_detached(&self.db.writer, "検索", move |c| {
			cache::put_search(c, &key, &to_store, now_secs())
		});
		Ok(meta)
	}

	async fn flat_playlist(&self, url: &str, limit: u32) -> Result<PlaylistMeta, ResolveError> {
		let ttl = i64::try_from(self.cache.playlist_ttl_secs).unwrap_or(i64::MAX);
		let key = url.to_string();
		// 保存件数が limit に足りず、全件とも言えないキャッシュは None になり、取り直す
		let hit = self
			.db
			.reader
			.read(move |c| cache::get_playlist(c, &key, now_secs(), ttl, limit))
			.await;
		match hit {
			Ok(Some(meta)) => return Ok(meta),
			Ok(None) => {}
			Err(e) => tracing::warn!("プレイリストのキャッシュ読み取りに失敗: {e}"),
		}
		let request = WorkerRequest::FlatPlaylist {
			url: url.to_string(),
			limit,
		};
		let value = self.client.submit(request, Priority::Interactive).await?;
		let list = parse_list(&value)?;
		// 捨てた要素がある、またはワーカーが途中までにした一覧は、件数が上限に満たなくても全件とは言えない。
		// 取得時の上限を保存件数に下げ、これより大きい limit では取り直させる
		let fetch_limit = if list.incomplete() {
			u32::try_from(list.entries.len()).unwrap_or(u32::MAX)
		} else {
			limit
		};
		if list.truncated {
			tracing::warn!(
				url,
				limit,
				received = list.entries.len(),
				"プレイリストを持ち時間のうちに読み切れず、途中までの一覧になりました"
			);
		}
		let meta = PlaylistMeta {
			title: list.title,
			entries: list.entries,
		};
		let to_store = meta.clone();
		let key = url.to_string();
		store_detached(&self.db.writer, "プレイリスト", move |c| {
			cache::put_playlist(c, &key, &to_store, fetch_limit, now_secs())
		});
		Ok(meta)
	}
}
