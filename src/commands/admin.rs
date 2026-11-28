//! 管理系のコマンド (`?setting`)。
//! - 権限の確認は呼び出し側 (Dispatcher) が済ませてから呼ぶ
//! - Discord には触れず、結果の通知を返す

use crate::commands::SettingCommand;
use crate::db::{settings, DbError, DbHandles};
use crate::domain::GuildId;
use crate::guild::message::GuildMessage;
use crate::guild::notifier::Notice;
use crate::guild::registry::{GuildRegistry, SendError};
use crate::lang::GuildLangs;

/// 結果の通知を作る。引数なしなら `args` は空。
///
/// # 引数
/// - `key`: 文面のキー
/// - `args`: 文面に埋める値 (名前と値)
///
/// # 戻り値
/// 文字だけの通知
///
/// # 呼び出し例
/// `notice("setting.lang", Vec::new())`
fn notice(key: &'static str, args: Vec<(&'static str, String)>) -> Notice {
	Notice::text(key, args)
}

/// ユーザーのメンションの文字列。
///
/// # 引数
/// - `id`: ユーザー ID の値
///
/// # 戻り値
/// `<@id>` の形の文字列
///
/// # 呼び出し例
/// `mention(user.get())`
fn mention(id: u64) -> String {
	format!("<@{id}>")
}

/// `?setting` を適用する。
///
/// - 権限 (サーバー管理者か Bot 管理者) の確認を済ませてから呼ぶ
/// - 書き込みは DbWriter を通す。書き込めたら、動いているアクターへ `SettingsChanged` を送る
/// - アクターが動いていなければ送らない (キャッシュが無いため)
/// - メールボックスが満杯で送れなくても、設定は書き込み済みなので成功として返す (警告だけ記録する)
///
/// # 引数
/// - `db`: DB のハンドル
/// - `registry`: ギルドのアクターの登録簿
/// - `langs`: サーバーごとの言語の表 (`?setting lang` の保存に成功したら更新する)
/// - `guild`: 対象のギルド
/// - `command`: 解析済みの操作
///
/// # 戻り値
/// 利用者へ返す通知。
///
/// # エラー
/// - DB の書き込みの失敗
///
/// # 呼び出し例
/// ```ignore
/// let notice = apply_setting(&db, &registry, &langs, guild, &SettingCommand::QueueLimit(30)).await?;
/// ```
pub async fn apply_setting(
	db: &DbHandles,
	registry: &GuildRegistry,
	langs: &GuildLangs,
	guild: GuildId,
	command: &SettingCommand,
) -> Result<Notice, DbError> {
	let result = match command {
		SettingCommand::AdminAdd(user) => {
			let user = *user;
			let added = db
				.writer
				.run(move |c| settings::add_admin(c, guild, user))
				.await?;
			let key = if added {
				"setting.admin_added"
			} else {
				"setting.admin_already"
			};
			notice(key, vec![("user", mention(user.get()))])
		}
		SettingCommand::AdminRemove(user) => {
			let user = *user;
			let removed = db
				.writer
				.run(move |c| settings::remove_admin(c, guild, user))
				.await?;
			let key = if removed {
				"setting.admin_removed"
			} else {
				"setting.admin_missing"
			};
			notice(key, vec![("user", mention(user.get()))])
		}
		SettingCommand::QueueLimit(value) => {
			let value = *value;
			db.writer
				.run(move |c| settings::set_queue_limit(c, guild, value))
				.await?;
			notice("setting.queue_limit", vec![("value", value.to_string())])
		}
		SettingCommand::PlaylistLimit(value) => {
			let value = *value;
			db.writer
				.run(move |c| settings::set_playlist_limit(c, guild, value))
				.await?;
			notice("setting.playlist_limit", vec![("value", value.to_string())])
		}
		SettingCommand::AutoLeave(secs) => {
			let secs = *secs;
			db.writer
				.run(move |c| settings::set_alone_timeout(c, guild, secs))
				.await?;
			if secs == 0 {
				notice("setting.autoleave_off", Vec::new())
			} else {
				notice("setting.autoleave", vec![("secs", secs.to_string())])
			}
		}
		SettingCommand::Lang(lang) => {
			let lang = *lang;
			db.writer
				.run(move |c| settings::set_lang(c, guild, lang))
				.await?;
			// 保存できた後にだけ表を変える。アクターは言語を持たないので SettingsChanged は送らない
			langs.set(guild, lang);
			return Ok(notice("setting.lang", Vec::new()));
		}
	};
	match registry.send_existing(guild, GuildMessage::SettingsChanged) {
		Ok(()) | Err(SendError::NotRunning) => {}
		Err(SendError::Busy) => {
			tracing::warn!(
				guild = guild.get(),
				"設定の変更をアクターへ伝えられませんでした"
			);
		}
	}
	Ok(result)
}
