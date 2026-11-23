//! Discord (Gateway) まわり。
//! - `shard`: Shard の受信ループ (キャッシュ、Songbird、Dispatcher への受け渡し)
//! - `dispatcher`: イベントの振り分けと、アクターを通さないコマンドの実行
//! - `route`: メッセージの振り分けと権限・時間の判定 (純粋な関数)
//! - `ready`: 起動の準備の門 (`Ready` の後、全ギルドが揃うまで待つ)
//! - `notifier`: Discord への通知の送信 (`Notifier` の実装)
//! - `cache_view`: キャッシュからの取り出し (ギルド全体の権限の計算の入力、聴者の数)
//! - `page_timers`: ページ送りのボタンを灰色にする期限の記録
//! - `permissions`: 権限の計算 (キャッシュからの取り出しは呼び出し側が行う)

pub mod cache_view;
pub mod dispatcher;
pub mod notifier;
pub mod page_timers;
pub mod permissions;
pub mod ready;
pub mod requester;
pub mod route;
pub mod shard;
