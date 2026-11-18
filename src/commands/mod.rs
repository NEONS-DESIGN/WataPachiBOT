//! テキストコマンドの解析。

pub mod admin;
pub mod info;
pub mod parser;

pub use parser::{parse, ParseError, ParsedCommand, SettingCommand};
