//! yt-dlp の更新運用。
//!
//! - `envs`: ワーカーの環境 (venv) の記録 (`current.json`) と、起動時に使う python の選択、古い環境の片付け
//! - `procedure`: 更新の手順 (新しい環境の作成、版の比較、検査、切り替え、記録、片付け)
//! - `tools`: uv と実際のワーカーによる環境の作成と検査

pub mod envs;
pub mod procedure;
pub mod tools;
