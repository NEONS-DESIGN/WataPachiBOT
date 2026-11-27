//! 再生入力。
//! - `access` がボイスチャンネルへの参加の前の確認 (権限と満員)
//! - `backend` が音声処理を隠す trait
//! - `limits` が音声の呼び出しの上限時間 (内側のバックエンドと外側のアクターの入れ子)
//! - `chunked::ChunkedRequest` が通常の再生に使う入力。小さな Range 要求で読み、シークできる
//! - `pipe::PipeInput` が予備の入力。yt-dlp の標準出力を読み、シークできない
//! - `songbird_backend` が Songbird による `AudioBackend` の実装

pub mod access;
pub mod backend;
pub mod chunked;
pub mod limits;
pub mod pipe;
pub mod songbird_backend;
