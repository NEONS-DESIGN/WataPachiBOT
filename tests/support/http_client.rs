//! 試験用の HTTP クライアント。
//!
//! - reqwest は rustls の暗号プロバイダを持たない設定 (`rustls-no-provider`) で使うため、
//!   プロバイダを入れずに `Client::new()` を呼ぶと panic する
//! - 本番と同じ ring のプロバイダを入れてからクライアントを作る (2 回目以降の登録の失敗は無視する)

/// ring の暗号プロバイダを入れた上で、既定の設定の reqwest のクライアントを作る。
///
/// # 戻り値
/// `reqwest::Client::new()` と同じ設定のクライアント
///
/// # 呼び出し例
/// ```ignore
/// let client = http_client::new_client();
/// ```
pub fn new_client() -> reqwest::Client {
	// 別の試験が先に入れていれば Err が返るが、入っていることに変わりはない
	let _ = rustls::crypto::ring::default_provider().install_default();
	reqwest::Client::new()
}
