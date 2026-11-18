//! サーバーごとのアクターと、その入出力の型。
//! - `registry` がアクターの登録簿と送信口。外からはここを通してアクターへ送る
//! - `actor` がアクターの本体、`state` がその状態、`prefetch` が先読みの状態
//!   (`commands.rs` は `actor` の子のモジュールで、`?p` 以外の操作コマンドと自動退出を処理する。
//!   `supervise.rs` も `actor` の子のモジュールで、失敗時の手順と接続の回復を処理する。
//!   `persist.rs` も `actor` の子のモジュールで、再生位置の保存、起動時の復元の受け取り、終了を処理する)
//! - `restore` が起動時の復元の段取り (`playback_state` の全行をずらしながらアクターへ送る)
//! - `health` が音声接続の健全性の判定、`recovery` が失敗の回数と再接続の制限の記録 (どちらも I/O なし)
//! - `store` がアクターの DB の合成処理、`notices` がアクターの送る通知の組み立て
//! - `message` がアクターの受け取るメッセージ、`query` が `?p` の引数の分類
//! - `notifier` が利用者への応答の trait。Discord への依存を切り離す

mod actor;
pub mod health;
pub mod message;
mod notices;
pub mod notifier;
mod prefetch;
pub mod query;
mod recovery;
pub mod registry;
pub mod restore;
pub mod state;
mod store;
