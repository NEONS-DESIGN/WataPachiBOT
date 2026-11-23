//! メッセージの振り分けと、振り分けに使う判定 (すべて純粋な関数)。
//! - `route` はメッセージを振り分ける。Bot、DM、起動の準備中、メンション、コマンドを見る
//! - `setting_access` と `is_timed_out` は `?setting` を使えるかを決める
//! - `page_build_budget` はボタンのページを作るのに使える時間を決める (snowflake の時刻を使う)

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use twilight_model::util::Timestamp;

use crate::commands::{parse, ParseError, ParsedCommand};
use crate::config::LimitsConfig;
use crate::domain::{GuildId, UserId};

/// ボタンのページを作るのを待つ上限 (ボタンが押された時刻から数える)。
/// Discord はボタンへの応答を 3 秒まで待つため、送信の分を残して打ち切る。
pub const PAGE_BUILD_LIMIT: Duration = Duration::from_secs(2);
/// ボタンが押されてから受信するまでの遅れとして、上限から差し引く最大の値。
/// 時計のずれで遅れを大きく見積もっても、ページを作る時間がこれ以上は削られないようにする
pub const MAX_TRUSTED_EVENT_DELAY: Duration = Duration::from_secs(1);
/// Discord の snowflake の時刻の起点 (2015-01-01T00:00:00Z の UNIX ミリ秒)。
const DISCORD_EPOCH_MS: u64 = 1_420_070_400_000;
/// snowflake の中で時刻より下にあるビットの数。
const SNOWFLAKE_TIMESTAMP_SHIFT: u32 = 22;

/// 振り分けに使うメッセージの中身。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomingMessage {
	/// DM なら None
	pub guild_id: Option<GuildId>,
	pub author_is_bot: bool,
	pub content: String,
	/// メンションされた利用者 (返信の通知を含む)
	pub mentions: Vec<UserId>,
}

/// メッセージの振り分けの結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
	/// 応答しない
	Ignore,
	/// 起動の準備中 (復元の前)。`error.starting` を返す
	Starting,
	/// Bot へのメンション。`help.mention` を返す
	Mention,
	/// コマンドの引数の誤り
	ParseError(ParseError),
	/// 解析済みのコマンド
	Command(ParsedCommand),
}

/// メッセージを振り分ける (純粋)。
///
/// - DM と Bot のメッセージは無視する
/// - コマンドなら、受付の前は `Starting`、受付中は解析の結果
/// - コマンドでなく、本文に Bot へのメンション (`<@id>` か `<@!id>`) があり、`mentions` にも Bot がいれば `Mention`
///   (受付の前でも返す。状態に触れない案内のため)。返信の通知だけで本文にメンションが無いものは無視する
///
/// # 引数
/// - `prefix`: コマンドの接頭辞
/// - `limits`: 数値引数の上限
/// - `accepting`: コマンドを受け付けているか (起動の準備が済んだか)
/// - `bot_id`: この Bot のユーザー ID
/// - `message`: メッセージの中身
///
/// # 呼び出し例
/// ```ignore
/// match route("?", &config.limits, true, bot_id, &incoming) { Route::Command(cmd) => { /* ... */ } _ => {} }
/// ```
pub fn route(
	prefix: &str,
	limits: &LimitsConfig,
	accepting: bool,
	bot_id: UserId,
	message: &IncomingMessage,
) -> Route {
	if message.guild_id.is_none() || message.author_is_bot {
		return Route::Ignore;
	}
	if let Some(parsed) = parse(prefix, &message.content, limits) {
		if !accepting {
			return Route::Starting;
		}
		return match parsed {
			Ok(command) => Route::Command(command),
			Err(error) => Route::ParseError(error),
		};
	}
	if mentions_bot(message, bot_id) {
		return Route::Mention;
	}
	Route::Ignore
}

/// 本文と `mentions` の両方に Bot へのメンションがあるか。
fn mentions_bot(message: &IncomingMessage, bot_id: UserId) -> bool {
	if !message.mentions.contains(&bot_id) {
		return false;
	}
	let plain = format!("<@{bot_id}>");
	let nick = format!("<@!{bot_id}>");
	message.content.contains(&plain) || message.content.contains(&nick)
}

/// Discord の時刻を UNIX 秒にする (秒未満は切り捨て)。
///
/// # 呼び出し例
/// ```ignore
/// let at = unix_secs(message.timestamp);
/// ```
pub fn unix_secs(timestamp: Timestamp) -> i64 {
	timestamp.as_secs()
}

/// タイムアウト中か。`until` が今より後なら true。
///
/// # 引数
/// - `until`: メンバーの `communication_disabled_until`
/// - `now`: 今の UNIX 秒
///
/// # 呼び出し例
/// ```ignore
/// let timed_out = is_timed_out(member.communication_disabled_until, now_secs());
/// ```
pub fn is_timed_out(until: Option<Timestamp>, now: i64) -> bool {
	until.is_some_and(|until| unix_secs(until) > now)
}

/// `?setting` の権限の判定の結果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingAccess {
	/// サーバー管理者なので許す
	Allowed,
	/// Bot 管理者 (DB) かどうかで決める
	CheckBotAdmin,
	/// タイムアウト中のため許さない
	Denied,
}

/// `?setting` の権限を、DB を引く前に判定する。
///
/// - サーバー管理者は許す。それ以外は Bot 管理者かを DB で確かめる
/// - タイムアウト中のメンバーは、Bot 管理者でも許さない
///
/// # 呼び出し例
/// ```ignore
/// match setting_access(admin, timed_out) { SettingAccess::CheckBotAdmin => { /* DB */ } _ => {} }
/// ```
pub fn setting_access(server_admin: bool, timed_out: bool) -> SettingAccess {
	if server_admin {
		SettingAccess::Allowed
	} else if timed_out {
		SettingAccess::Denied
	} else {
		SettingAccess::CheckBotAdmin
	}
}

/// Discord の snowflake (ID) が作られた時刻を UNIX ミリ秒にする。
///
/// # 引数
/// - `snowflake`: ID の値
///
/// # 戻り値
/// 作られた時刻 (UNIX ミリ秒)
///
/// # 呼び出し例
/// ```ignore
/// let created = snowflake_unix_ms(interaction.id.get());
/// ```
pub fn snowflake_unix_ms(snowflake: u64) -> u64 {
	(snowflake >> SNOWFLAKE_TIMESTAMP_SHIFT).saturating_add(DISCORD_EPOCH_MS)
}

/// 今の時刻の UNIX ミリ秒。時計が起点より前なら None。
pub(crate) fn unix_ms_now() -> Option<u64> {
	let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
	u64::try_from(elapsed.as_millis()).ok()
}

/// ボタンを受信した時点から、ページを作るのに使える時間。
///
/// - `PAGE_BUILD_LIMIT` から、押されてから受信するまでの遅れを差し引く
/// - 遅れは `MAX_TRUSTED_EVENT_DELAY` までしか差し引かない (手元と Discord の時計のずれへの備え)
/// - 受信の時刻が押された時刻より前 (時計のずれ) なら遅れは 0 とする
///
/// # 引数
/// - `pressed_unix_ms`: ボタンが押された時刻 (インタラクションの ID の時刻)
/// - `received_unix_ms`: 受信した時刻
///
/// # 戻り値
/// 使える時間
///
/// # 呼び出し例
/// ```ignore
/// let deadline = tokio::time::Instant::now() + page_build_budget(pressed, received);
/// ```
pub fn page_build_budget(pressed_unix_ms: u64, received_unix_ms: u64) -> Duration {
	let delay = Duration::from_millis(received_unix_ms.saturating_sub(pressed_unix_ms));
	PAGE_BUILD_LIMIT.saturating_sub(delay.min(MAX_TRUSTED_EVENT_DELAY))
}
