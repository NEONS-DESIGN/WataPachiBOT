//! 利用者に見せる部品の組み立て。Discord への送信はここでは行わない。
//! - `embed`: 通知と曲の一覧を Embed にする
//! - `pagination`: ページ送りのボタンと、押されたボタンの custom_id の読み書き

pub mod embed;
pub mod pagination;
