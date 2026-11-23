//! Gateway の純粋な部品の試験。
//! - `route`: メッセージの振り分け (Bot、DM、起動の準備中、メンション、コマンド)
//! - `ReadyGate`: Ready の後、全ギルドが揃うか期限を過ぎたら 1 回だけ開く
//! - 聴者の数え方、タイムアウト中のメンバーの扱い
//! - ページ送りのボタンの期限の記録 (`PageTimers`)、ボタンの応答の時間の計算、カードの拒否の判定

use std::time::{Duration, Instant};

use twilight_http::response::StatusCode;
use twilight_model::channel::message::component::{Button, ButtonStyle, Component};
use twilight_model::util::Timestamp;

use watapachi_bot::commands::{ParseError, ParsedCommand};
use watapachi_bot::config::LimitsConfig;
use watapachi_bot::domain::{ChannelId, GuildId, MessageId, UserId};
use watapachi_bot::gateway::cache_view::count_listeners;
use watapachi_bot::gateway::notifier::is_content_rejection;
use watapachi_bot::gateway::page_timers::{Due, PageTimers, Settled};
use watapachi_bot::gateway::ready::ReadyGate;
use watapachi_bot::gateway::route::{
	is_timed_out, page_build_budget, route, setting_access, snowflake_unix_ms, unix_secs,
	IncomingMessage, Route, SettingAccess, MAX_TRUSTED_EVENT_DELAY, PAGE_BUILD_LIMIT,
};
use watapachi_bot::guild::message::GuildCommand;
use watapachi_bot::guild::notifier::MessageRef;

const BOT: UserId = UserId::new(100);
const USER: UserId = UserId::new(200);
const G: GuildId = GuildId::new(1);
const G1: GuildId = GuildId::new(11);
const G2: GuildId = GuildId::new(12);
const G3: GuildId = GuildId::new(13);
const WAIT: Duration = Duration::from_secs(30);

/// 試験用の上限値。
fn limits() -> LimitsConfig {
	LimitsConfig {
		queue_limit_max: 10000,
		playlist_limit_max: 500,
		alone_timeout_max_secs: 600,
		actor_mailbox: 64,
		actor_idle_timeout_secs: 60,
	}
}

/// メンションなしのメッセージ。
fn msg(guild_id: Option<GuildId>, author_is_bot: bool, content: &str) -> IncomingMessage {
	IncomingMessage {
		guild_id,
		author_is_bot,
		content: content.to_owned(),
		mentions: Vec::new(),
	}
}

/// Bot へのメンションを含むメッセージ。
fn mentioning(content: &str) -> IncomingMessage {
	IncomingMessage {
		mentions: vec![BOT],
		..msg(Some(G), false, content)
	}
}

#[test]
fn route_ignores_bots_dm_and_noise() {
	assert_eq!(
		route("?", &limits(), true, BOT, &msg(None, false, "?skip")),
		Route::Ignore
	); // DM
	assert_eq!(
		route("?", &limits(), true, BOT, &msg(Some(G), true, "?skip")),
		Route::Ignore
	); // Bot
	assert_eq!(
		route("?", &limits(), true, BOT, &msg(Some(G), false, "??")),
		Route::Ignore
	);
	assert_eq!(
		route("?", &limits(), true, BOT, &msg(Some(G), false, "hello")),
		Route::Ignore
	);
}

#[test]
fn route_starting_before_ready() {
	assert_eq!(
		route("?", &limits(), false, BOT, &msg(Some(G), false, "?skip")),
		Route::Starting
	);
	// 引数の誤りも、準備中は準備中として返す
	assert_eq!(
		route("?", &limits(), false, BOT, &msg(Some(G), false, "?p")),
		Route::Starting
	);
}

#[test]
fn route_noise_before_ready_is_ignored() {
	assert_eq!(
		route("?", &limits(), false, BOT, &msg(Some(G), false, "hello")),
		Route::Ignore
	);
}

#[test]
fn route_command_and_parse_error() {
	assert_eq!(
		route("?", &limits(), true, BOT, &msg(Some(G), false, "?skip")),
		Route::Command(ParsedCommand::Guild(GuildCommand::Skip))
	);
	assert_eq!(
		route("?", &limits(), true, BOT, &msg(Some(G), false, "?p")),
		Route::ParseError(ParseError::Usage {
			usage: "help.usage_p"
		})
	);
	// 削除したコマンドは未知のコマンドとして無視する
	assert_eq!(
		route("?", &limits(), true, BOT, &msg(Some(G), false, "?purge 5")),
		Route::Ignore
	);
}

#[test]
fn route_mention() {
	assert_eq!(
		route("?", &limits(), true, BOT, &mentioning("<@100> help me")),
		Route::Mention
	);
	assert_eq!(
		route("?", &limits(), true, BOT, &mentioning("hey <@!100>")),
		Route::Mention
	);
	assert_eq!(
		route("?", &limits(), true, BOT, &mentioning("?skip <@100>")),
		Route::Command(ParsedCommand::Guild(GuildCommand::Skip))
	);
	// 返信による通知だけで、本文にメンションが無ければ案内しない
	assert_eq!(
		route("?", &limits(), true, BOT, &mentioning("thanks")),
		Route::Ignore
	);
	// 本文の形だけ合っても、mentions に Bot が無ければ案内しない (コードブロックなど)
	assert_eq!(
		route("?", &limits(), true, BOT, &msg(Some(G), false, "<@100>")),
		Route::Ignore
	);
	// 他の利用者へのメンションは案内しない
	assert_eq!(
		route("?", &limits(), true, BOT, &mentioning("<@200>")),
		Route::Ignore
	);
	// Bot のメンションは DM と Bot からは受けない
	let mut from_bot = mentioning("<@100>");
	from_bot.author_is_bot = true;
	assert_eq!(route("?", &limits(), true, BOT, &from_bot), Route::Ignore);
}

#[test]
fn ready_gate_waits_for_guilds_or_deadline() {
	let mut gate = ReadyGate::new(WAIT);
	let t0 = Instant::now();
	gate.on_ready([G1, G2], t0);
	assert!(!gate.poll(t0));
	gate.on_guild_create(G1);
	assert!(!gate.poll(t0));
	gate.on_guild_create(G2);
	assert!(gate.poll(t0));
	assert!(!gate.poll(t0)); // 2 回目は false
}

#[test]
fn ready_gate_opens_when_guild_create_is_handled_before_ready() {
	// Ready は別タスクで門に渡し、GuildCreate はその場で渡すため、GuildCreate が Ready より先に届きうる
	let mut gate = ReadyGate::new(WAIT);
	let t0 = Instant::now();
	gate.on_guild_create(G1);
	assert!(!gate.poll(t0)); // Ready の前は開かない
	gate.on_ready([G1, G2], t0);
	assert!(!gate.poll(t0));
	gate.on_guild_create(G2);
	assert!(gate.poll(t0)); // 期限を待たずに開く

	// 全ギルドの GuildCreate が先に処理されたら、Ready の直後に開く
	let mut gate = ReadyGate::new(WAIT);
	gate.on_guild_create(G2);
	gate.on_guild_create(G1);
	gate.on_ready([G1, G2], t0);
	assert!(gate.poll(t0));
}

#[test]
fn ready_gate_deadline() {
	let mut gate = ReadyGate::new(WAIT);
	let t0 = Instant::now();
	gate.on_ready([G1, G2], t0);
	gate.on_guild_create(G1);
	assert!(!gate.poll(t0 + WAIT - Duration::from_millis(1)));
	assert!(gate.poll(t0 + WAIT));
	assert!(!gate.poll(t0 + WAIT * 2));
}

#[test]
fn ready_gate_ignores_second_ready() {
	let mut gate = ReadyGate::new(WAIT);
	let t0 = Instant::now();
	gate.on_ready([G1], t0);
	gate.on_guild_create(G1);
	assert!(gate.poll(t0));
	let t1 = t0 + Duration::from_secs(60);
	gate.on_ready([G3], t1);
	assert!(!gate.poll(t1));
	assert!(!gate.poll(t1 + WAIT * 2));
}

#[test]
fn ready_gate_second_ready_while_waiting_keeps_first_deadline() {
	let mut gate = ReadyGate::new(WAIT);
	let t0 = Instant::now();
	gate.on_ready([G1], t0);
	// 再接続の Ready は待つギルドも期限も変えない
	gate.on_ready([G1, G2], t0 + Duration::from_secs(20));
	gate.on_guild_create(G1);
	assert!(gate.poll(t0 + Duration::from_secs(20)));
}

#[test]
fn ready_gate_without_guilds_opens_at_once() {
	let mut gate = ReadyGate::new(WAIT);
	let t0 = Instant::now();
	assert!(!gate.poll(t0 + WAIT * 2)); // Ready の前は開かない
	gate.on_guild_create(G1); // Ready の前の GuildCreate は無視する
	gate.on_ready(std::iter::empty::<GuildId>(), t0);
	assert!(gate.poll(t0));
}

#[test]
fn listeners_exclude_bots_and_count_unknown_users() {
	let other_bot = UserId::new(300);
	let unknown = UserId::new(400);
	let states = [
		(BOT, Some(true)),
		(USER, Some(false)),
		(other_bot, Some(true)),
		(unknown, None),
	];
	assert_eq!(count_listeners(states, BOT), 2);
	// キャッシュに Bot 自身の印が無くても、自分は数えない
	assert_eq!(count_listeners([(BOT, None)], BOT), 0);
	assert_eq!(count_listeners([], BOT), 0);
}

#[test]
fn unix_secs_truncates_micros() {
	let ts = Timestamp::from_micros(1_700_000_000_999_999).unwrap();
	assert_eq!(unix_secs(ts), 1_700_000_000);
}

#[test]
fn timed_out_only_while_in_future() {
	let now = 1_700_000_000;
	let at = |secs: i64| Some(Timestamp::from_secs(secs).unwrap());
	assert!(!is_timed_out(None, now));
	assert!(!is_timed_out(at(now - 1), now));
	assert!(!is_timed_out(at(now), now));
	assert!(is_timed_out(at(now + 1), now));
}

#[test]
fn timed_out_member_loses_setting() {
	// setting_access(server_admin, timed_out)
	assert_eq!(setting_access(true, false), SettingAccess::Allowed);
	assert_eq!(setting_access(true, true), SettingAccess::Allowed);
	assert_eq!(setting_access(false, false), SettingAccess::CheckBotAdmin);
	assert_eq!(setting_access(false, true), SettingAccess::Denied);
}

/// 試験用のメッセージの場所。
fn target(message: u64) -> MessageRef {
	MessageRef {
		channel: ChannelId::new(10),
		message: MessageId::new(message),
	}
}

#[tokio::test(start_paused = true)]
async fn page_timers_extend_on_touch() {
	let timers = PageTimers::new();
	let t0 = tokio::time::Instant::now();
	let first = t0 + Duration::from_secs(120);
	let later = t0 + Duration::from_secs(180);
	assert!(timers.touch(target(1), Vec::new(), first));
	// 押されたら期限を延ばす。新しい記録ではない
	assert!(!timers.touch(target(1), Vec::new(), later));
	assert!(matches!(timers.due(MessageId::new(1), first), Due::Wait(at) if at == later));
	match timers.due(MessageId::new(1), later) {
		Due::Expired { target: t, .. } => assert_eq!(t, target(1)),
		_ => panic!("期限で Expired にならない"),
	}
	// 灰色にする編集を送り終えたら記録を消す
	assert_eq!(timers.settle(MessageId::new(1)), Settled::Done);
	assert!(matches!(timers.due(MessageId::new(1), later), Due::Gone));
}

/// 試験用のボタンの並び (印に使う custom_id のボタン 1 つ)。
fn buttons(id: &str) -> Vec<Component> {
	vec![Component::Button(Button {
		id: None,
		custom_id: Some(id.to_owned()),
		disabled: false,
		emoji: None,
		label: None,
		style: ButtonStyle::Primary,
		url: None,
		sku_id: None,
	})]
}

#[tokio::test(start_paused = true)]
async fn page_timers_forget_drops_armed_entry() {
	// ボタンの無いページに書き換えたら、期限に古いボタンを灰色で戻さない
	let timers = PageTimers::new();
	let t0 = tokio::time::Instant::now();
	let deadline = t0 + Duration::from_secs(120);
	assert!(timers.touch(target(1), buttons("old"), deadline));
	timers.forget(MessageId::new(1));
	assert!(matches!(timers.due(MessageId::new(1), deadline), Due::Gone));
	// 記録が無ければ何もしない
	timers.forget(MessageId::new(2));
	// 消した後の touch は新しい記録 (期限のタスクを起動し直す)
	assert!(timers.touch(target(1), buttons("new"), deadline));
}

#[tokio::test(start_paused = true)]
async fn page_timers_touch_while_sending_restores_new_buttons() {
	// 灰色にする編集の送信中にボタンが押されたら、送った後に新しいボタンへ戻して待ち直す
	let timers = PageTimers::new();
	let t0 = tokio::time::Instant::now();
	let first = t0 + Duration::from_secs(120);
	let later = t0 + Duration::from_secs(300);
	assert!(timers.touch(target(1), buttons("old"), first));
	assert!(matches!(
		timers.due(MessageId::new(1), first),
		Due::Expired { .. }
	));
	// 送信中の touch は新しい記録ではない (今のタスクが続けて扱う)
	assert!(!timers.touch(target(1), buttons("new"), later));
	assert_eq!(
		timers.settle(MessageId::new(1)),
		Settled::Resend(buttons("new"))
	);
	// 戻した後に変化が無ければ、延ばした期限まで待ち直す
	assert_eq!(timers.settle(MessageId::new(1)), Settled::Wait(later));
	assert!(matches!(timers.due(MessageId::new(1), first), Due::Wait(at) if at == later));
}

#[tokio::test(start_paused = true)]
async fn page_timers_forget_while_sending_clears_buttons() {
	// 灰色にする編集の送信中にボタンの無いページへ書き換えられたら、送った後にボタンを外して終える
	let timers = PageTimers::new();
	let t0 = tokio::time::Instant::now();
	let deadline = t0 + Duration::from_secs(120);
	assert!(timers.touch(target(1), buttons("old"), deadline));
	assert!(matches!(
		timers.due(MessageId::new(1), deadline),
		Due::Expired { .. }
	));
	timers.forget(MessageId::new(1));
	assert_eq!(
		timers.settle(MessageId::new(1)),
		Settled::Resend(Vec::new())
	);
	assert_eq!(timers.settle(MessageId::new(1)), Settled::Done);
	assert!(matches!(timers.due(MessageId::new(1), deadline), Due::Gone));
}

#[test]
fn snowflake_time_is_unix_millis() {
	// Discord の資料の例: 175928847299117063 は 2016-04-30T11:18:25.796Z
	assert_eq!(
		snowflake_unix_ms(175_928_847_299_117_063),
		1_462_015_105_796
	);
}

#[test]
fn page_build_budget_counts_from_press() {
	let pressed = 1_700_000_000_000;
	// 受信が遅れた分だけ短くする
	assert_eq!(page_build_budget(pressed, pressed), PAGE_BUILD_LIMIT);
	assert_eq!(
		page_build_budget(pressed, pressed + 300),
		PAGE_BUILD_LIMIT - Duration::from_millis(300)
	);
	// 時計のずれで受信が押下より前に見えても、上限は延ばさない
	assert_eq!(page_build_budget(pressed, pressed - 500), PAGE_BUILD_LIMIT);
	// 大きな遅れは時計のずれとみなし、差し引くのは MAX_TRUSTED_EVENT_DELAY まで
	assert_eq!(
		page_build_budget(pressed, pressed + 60_000),
		PAGE_BUILD_LIMIT - MAX_TRUSTED_EVENT_DELAY
	);
}

#[test]
fn only_bad_request_is_content_rejection() {
	// 中身を変えても通らない 403 / 404 は、簡易表示で送り直さない
	assert!(is_content_rejection(StatusCode::BAD_REQUEST));
	assert!(!is_content_rejection(StatusCode::FORBIDDEN));
	assert!(!is_content_rejection(StatusCode::NOT_FOUND));
	assert!(!is_content_rejection(StatusCode::TOO_MANY_REQUESTS));
	assert!(!is_content_rejection(StatusCode::GATEWAY_UNAVAILABLE));
}

#[tokio::test(start_paused = true)]
async fn page_timers_unknown_message_is_gone() {
	let timers = PageTimers::new();
	assert!(matches!(
		timers.due(MessageId::new(9), tokio::time::Instant::now()),
		Due::Gone
	));
}
