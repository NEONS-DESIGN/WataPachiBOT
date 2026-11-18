//! ワーカー (Python / yt-dlp) との JSON Lines プロトコルの型。
//! - 定義の正は protocol/worker-protocol.json。Rust と Python の試験で一致を確かめる
//! - この層では `result` を `serde_json::Value` のまま持つ。型付けは呼び出し側で行う

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::domain::ResolveErrorCode;

/// プロトコルが持つ操作名。protocol.json の `ops` と同じ順序にする。
pub const OPS: [&str; 4] = ["resolve", "flat_playlist", "search", "ping"];

/// プロトコルの解釈で起きるエラー。
///
/// - `parse_response` と `WorkerRequest::to_line` が返す
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
	/// 行が JSON として不正、または型が合わない。
	#[error("JSON を解釈できません: {0}")]
	Json(#[from] serde_json::Error),
	/// JSON としては正しいが、プロトコルの形になっていない。
	#[error("応答の形式が不正です: {0}")]
	Malformed(&'static str),
}

/// ワーカーが返すエラーコード。protocol.json の `error_codes` と同じ。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
	NotFound,
	Private,
	GeoBlocked,
	AgeRestricted,
	RateLimited,
	/// YouTube 以外のホストなど、ワーカーが受け付けない URL。
	UnsupportedUrl,
	Internal,
}

impl ErrorCode {
	/// protocol.json の `error_codes` と同じ順序の全種類。
	pub const ALL: [ErrorCode; 7] = [
		ErrorCode::NotFound,
		ErrorCode::Private,
		ErrorCode::GeoBlocked,
		ErrorCode::AgeRestricted,
		ErrorCode::RateLimited,
		ErrorCode::UnsupportedUrl,
		ErrorCode::Internal,
	];

	/// プロトコル上の文字列表現を返す。
	///
	/// # 戻り値
	/// `"not_found"` のような文字列
	///
	/// # 例
	/// `assert_eq!(ErrorCode::Private.as_str(), "private");`
	pub fn as_str(self) -> &'static str {
		match self {
			ErrorCode::NotFound => "not_found",
			ErrorCode::Private => "private",
			ErrorCode::GeoBlocked => "geo_blocked",
			ErrorCode::AgeRestricted => "age_restricted",
			ErrorCode::RateLimited => "rate_limited",
			ErrorCode::UnsupportedUrl => "unsupported_url",
			ErrorCode::Internal => "internal",
		}
	}

	/// 文字列からエラーコードを得る。
	///
	/// - 未知の文字列は `Internal` として扱う (将来のワーカーが新しいコードを返しても落ちない)
	///
	/// # 引数
	/// - `text`: プロトコル上の文字列
	///
	/// # 例
	/// `assert_eq!(ErrorCode::from_str_lossy("weird"), ErrorCode::Internal);`
	pub fn from_str_lossy(text: &str) -> ErrorCode {
		ErrorCode::ALL
			.into_iter()
			.find(|code| code.as_str() == text)
			.unwrap_or(ErrorCode::Internal)
	}
}

impl From<ErrorCode> for ResolveErrorCode {
	/// プロトコルのエラーコードを共有の分類へ写す。
	///
	/// # 例
	/// `let code: ResolveErrorCode = ErrorCode::Private.into();`
	fn from(code: ErrorCode) -> Self {
		match code {
			ErrorCode::NotFound => ResolveErrorCode::NotFound,
			ErrorCode::Private => ResolveErrorCode::Private,
			ErrorCode::GeoBlocked => ResolveErrorCode::GeoBlocked,
			ErrorCode::AgeRestricted => ResolveErrorCode::AgeRestricted,
			ErrorCode::RateLimited => ResolveErrorCode::RateLimited,
			ErrorCode::UnsupportedUrl => ResolveErrorCode::UnsupportedUrl,
			ErrorCode::Internal => ResolveErrorCode::Internal,
		}
	}
}

/// ワーカーへの要求。`id` は送信時に `to_line` で付ける。
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WorkerRequest {
	/// 1 曲の解決。`format_id` を指定するとその形式に固定する。
	Resolve {
		url: String,
		format_id: Option<String>,
	},
	/// プレイリストの ID 一覧。
	FlatPlaylist { url: String, limit: u32 },
	/// 検索。
	Search { query: String, limit: u32 },
	/// 生存確認。
	Ping,
}

impl WorkerRequest {
	/// `id` を付けた 1 行の JSON (改行を含まない) にする。
	///
	/// # 引数
	/// - `id`: 応答との突き合わせに使う番号
	///
	/// # 戻り値
	/// 末尾の改行を含まない JSON。送信側が改行を足す
	///
	/// # エラー
	/// 直列化に失敗した場合 `ProtocolError::Json`
	///
	/// # 例
	/// `let line = WorkerRequest::Ping.to_line(4)?;  // {"op":"ping","id":4}`
	pub fn to_line(&self, id: u64) -> Result<String, ProtocolError> {
		let mut value = serde_json::to_value(self)?;
		if let Value::Object(map) = &mut value {
			map.insert("id".to_string(), Value::from(id));
		}
		Ok(serde_json::to_string(&value)?)
	}
}

/// ワーカーからの応答。
#[derive(Debug, Clone, PartialEq)]
pub enum WorkerResponse {
	/// 成功。`result` は操作ごとの JSON。
	Success { id: Option<u64>, result: Value },
	/// 失敗。要求が壊れていた場合などは `id` が `None` になる。
	Failure {
		id: Option<u64>,
		code: ErrorCode,
		message: String,
	},
}

/// 応答の生の形。`ok` で成功と失敗を分ける。
#[derive(Deserialize)]
struct RawResponse {
	id: Option<u64>,
	ok: bool,
	result: Option<Value>,
	error: Option<RawError>,
}

#[derive(Deserialize)]
struct RawError {
	code: String,
	message: String,
}

/// ワーカーの応答 1 行を解釈する。
///
/// # 引数
/// - `line`: 改行を除いた 1 行
///
/// # 戻り値
/// 成功または失敗の応答。未知のエラーコードは `ErrorCode::Internal` になる
///
/// # エラー
/// - JSON として不正: `ProtocolError::Json`
/// - `ok` が真なのに `result` が無い、偽なのに `error` が無い: `ProtocolError::Malformed`
///
/// # 例
/// `let response = parse_response(r#"{"id":1,"ok":true,"result":{}}"#)?;`
pub fn parse_response(line: &str) -> Result<WorkerResponse, ProtocolError> {
	let raw: RawResponse = serde_json::from_str(line)?;
	if raw.ok {
		let result = raw.result.ok_or(ProtocolError::Malformed(
			"ok が真なのに result がありません",
		))?;
		Ok(WorkerResponse::Success { id: raw.id, result })
	} else {
		let error = raw
			.error
			.ok_or(ProtocolError::Malformed("ok が偽なのに error がありません"))?;
		Ok(WorkerResponse::Failure {
			id: raw.id,
			code: ErrorCode::from_str_lossy(&error.code),
			message: error.message,
		})
	}
}
