//! ライブラリ層の共通エラー型。
//! - ライブラリ層は thiserror で型を持たせる。アプリ層（main など）は anyhow で受ける

use std::path::PathBuf;

/// 設定ファイルとメッセージファイルの読み込み・検証で起きるエラー。
///
/// - `Config::load` と `Messages::load` が返す
/// - 起動時に 1 回だけ発生しうるので、呼び出し側は表示して終了すればよい
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
	/// ファイルを読めなかった。
	#[error("ファイルを読めません: {path}: {source}")]
	Read {
		path: PathBuf,
		#[source]
		source: std::io::Error,
	},
	/// TOML の構文エラー、未知のキー、型の不一致。
	#[error("TOML を解釈できません: {path}: {source}")]
	Parse {
		path: PathBuf,
		#[source]
		source: Box<toml::de::Error>,
	},
	/// 値の範囲が不正。`key` は `worker.worker_count` の形式。
	#[error("設定値が不正です: {key}: {reason}")]
	Invalid { key: String, reason: String },
	/// 必須のメッセージキーが無い。`key` は `play.queued` の形式。
	#[error("メッセージキーがありません: {key}")]
	MissingMessageKey { key: String },
	/// メッセージキーの形が違う (通知が文字列、色の名前が不正など)。`key` は `play.queued` の形式。
	#[error("メッセージキーの形が不正です: {key}: {reason}")]
	InvalidMessage { key: String, reason: String },
}
