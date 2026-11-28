//! 参加の前の権限の判定 (純粋な関数) と、キャッシュと REST による確認役の試験。

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use twilight_cache_inmemory::DefaultInMemoryCache;
use twilight_http::Client;
use twilight_model::channel::{Channel, ChannelFlags};
use twilight_model::gateway::payload::incoming::{ChannelUpdate, GuildCreate};
use twilight_model::guild::{Guild, Permissions};

use watapachi_bot::audio::access::{AccessVerdict, Denial, VoiceAccess, VoicePermission};
use watapachi_bot::domain::{ChannelId, GuildId, UserId};
use watapachi_bot::gateway::voice_access::{count_occupants, judge, DiscordVoiceAccess};

/// 参加に要る 3 つの権限をすべて持つ。
const ALL: Permissions = Permissions::VIEW_CHANNEL
	.union(Permissions::CONNECT)
	.union(Permissions::SPEAK);

#[test]
fn allows_with_all_three() {
	assert_eq!(judge(ALL, None, 0), AccessVerdict::Allowed);
}

#[test]
fn lists_every_missing_permission_in_order() {
	assert_eq!(
		judge(Permissions::SPEAK, None, 0),
		AccessVerdict::Denied(Denial::Missing(vec![
			VoicePermission::ViewChannel,
			VoicePermission::Connect,
		]))
	);
}

#[test]
fn missing_speak_alone_is_denied() {
	assert_eq!(
		judge(Permissions::VIEW_CHANNEL | Permissions::CONNECT, None, 0),
		AccessVerdict::Denied(Denial::Missing(vec![VoicePermission::Speak]))
	);
}

#[test]
fn full_channel_is_denied() {
	assert_eq!(judge(ALL, Some(2), 2), AccessVerdict::Denied(Denial::Full));
}

#[test]
fn move_members_bypasses_the_limit() {
	assert_eq!(
		judge(ALL | Permissions::MOVE_MEMBERS, Some(2), 2),
		AccessVerdict::Allowed
	);
}

#[test]
fn zero_limit_means_unlimited() {
	assert_eq!(judge(ALL, Some(0), 99), AccessVerdict::Allowed);
}

#[test]
fn below_limit_is_allowed() {
	assert_eq!(judge(ALL, Some(2), 1), AccessVerdict::Allowed);
}

#[test]
fn missing_permission_wins_over_full() {
	assert_eq!(
		judge(Permissions::VIEW_CHANNEL | Permissions::SPEAK, Some(1), 1),
		AccessVerdict::Denied(Denial::Missing(vec![VoicePermission::Connect]))
	);
}

#[test]
fn occupants_exclude_the_bot_itself() {
	assert_eq!(
		count_occupants(
			[UserId::new(1), UserId::new(9), UserId::new(2)],
			UserId::new(9)
		),
		2
	);
}

#[test]
fn denial_display_names_the_missing_permissions() {
	let denial = Denial::Missing(vec![VoicePermission::ViewChannel, VoicePermission::Connect]);
	assert_eq!(
		denial.to_string(),
		"権限がありません: VIEW_CHANNEL, CONNECT"
	);
	assert_eq!(Denial::Full.to_string(), "チャンネルが満員です");
}

const GUILD: u64 = 1;
const OWNER: u64 = 3;
const BOT: u64 = 9;
const VC: u64 = 20;
const BOT_ROLE: u64 = 5;
const MISSING_ROLE: u64 = 6;
/// @everyone の権限 (見る・接続・発言)。
const EVERYONE_PERMS: &str = "3146752";

/// 役職の JSON。
fn role(id: u64, perms: &str) -> Value {
	json!({"id": id.to_string(), "name": format!("r{id}"), "permissions": perms, "position": 0,
		"color": 0, "colors": {"primary_color": 0}, "hoist": false, "managed": false,
		"mentionable": false, "flags": 0})
}

/// ギルドを GuildCreate でキャッシュへ入れる。
/// - ボイスチャンネル `VC` は @everyone の接続を拒み、`allow_role` の役職に接続を許す
/// - Bot (`BOT`) の役職は `bot_roles`。キャッシュの役職は @everyone と `BOT_ROLE` だけ
fn cache_with(
	bot_roles: &[u64],
	allow_role: u64,
	user_limit: u32,
	occupants: &[u64],
) -> Arc<DefaultInMemoryCache> {
	let mut overwrites =
		vec![json!({"id": GUILD.to_string(), "type": 0, "allow": "0", "deny": "1048576"})];
	overwrites
		.push(json!({"id": allow_role.to_string(), "type": 0, "allow": "1048576", "deny": "0"}));
	let states: Vec<Value> = occupants
		.iter()
		.map(|user| {
			json!({
		"user_id": user.to_string(), "channel_id": VC.to_string(), "session_id": format!("s{user}"),
		"deaf": false, "mute": false, "self_deaf": false, "self_mute": false, "self_video": false,
		"suppress": false, "request_to_speak_timestamp": null})
		})
		.collect();
	let guild = json!({
		"id": GUILD.to_string(), "name": "g", "owner_id": OWNER.to_string(),
		"premium_progress_bar_enabled": false,
		"afk_timeout": 300, "default_message_notifications": 0, "explicit_content_filter": 0,
		"features": [], "mfa_level": 0, "nsfw_level": 0, "preferred_locale": "ja",
		"premium_tier": 0, "system_channel_flags": 0, "verification_level": 0,
		"roles": [role(GUILD, EVERYONE_PERMS), role(BOT_ROLE, "0")],
		"channels": [{"id": VC.to_string(), "type": 2, "name": "vc", "position": 0,
			"user_limit": user_limit, "permission_overwrites": overwrites}],
		"members": [{"user": {"id": BOT.to_string(), "username": "bot", "discriminator": "0",
			"avatar": null, "bot": true},
			"roles": bot_roles.iter().map(u64::to_string).collect::<Vec<_>>(),
			"joined_at": "2024-01-01T00:00:00.000000+00:00", "deaf": false, "mute": false, "flags": 0}],
		"voice_states": states, "emojis": [], "stickers": [], "threads": [], "presences": [],
		"stage_instances": [], "guild_scheduled_events": []
	});
	let guild: Guild = serde_json::from_value(guild).unwrap();
	let cache = Arc::new(DefaultInMemoryCache::new());
	cache.update(&GuildCreate::Available(guild));
	cache
}

/// REST がすぐ失敗する確認役 (つながらない代理を通す)。キャッシュの役職で判定する経路を試す。
fn access(cache: Arc<DefaultInMemoryCache>) -> DiscordVoiceAccess {
	let http = Client::builder()
		.token("Bot test".to_string())
		.proxy("127.0.0.1:1".to_string(), true)
		.build();
	DiscordVoiceAccess::new(
		Arc::new(http),
		cache,
		UserId::new(BOT),
		Duration::from_secs(3),
	)
}

async fn check(cache: Arc<DefaultInMemoryCache>) -> AccessVerdict {
	access(cache)
		.check(GuildId::new(GUILD), ChannelId::new(VC))
		.await
}

#[tokio::test]
async fn role_overwrite_allows_with_cached_roles() {
	let cache = cache_with(&[BOT_ROLE], BOT_ROLE, 0, &[]);
	assert_eq!(check(cache).await, AccessVerdict::Allowed);
}

#[tokio::test]
async fn everyone_deny_without_role_overwrite_is_missing_connect() {
	let cache = cache_with(&[BOT_ROLE], MISSING_ROLE, 0, &[]);
	assert_eq!(
		check(cache).await,
		AccessVerdict::Denied(Denial::Missing(vec![VoicePermission::Connect]))
	);
}

#[tokio::test]
async fn role_missing_from_cache_is_unknown_not_denied() {
	// 接続を許す役職がキャッシュに無い。黙って落とすと誤って断るため、確かめられないとする
	let cache = cache_with(&[BOT_ROLE, MISSING_ROLE], MISSING_ROLE, 0, &[]);
	assert_eq!(check(cache).await, AccessVerdict::Unknown);
}

#[tokio::test]
async fn full_channel_counts_cached_voice_states_without_the_bot() {
	let full = cache_with(&[BOT_ROLE], BOT_ROLE, 2, &[100, 101]);
	assert_eq!(check(full).await, AccessVerdict::Denied(Denial::Full));
	let with_bot = cache_with(&[BOT_ROLE], BOT_ROLE, 2, &[100, BOT]);
	assert_eq!(check(with_bot).await, AccessVerdict::Allowed);
}

#[tokio::test]
async fn unknown_channel_or_other_guild_is_unknown() {
	let cache = cache_with(&[BOT_ROLE], BOT_ROLE, 0, &[]);
	let access = access(cache);
	assert_eq!(
		access.check(GuildId::new(GUILD), ChannelId::new(999)).await,
		AccessVerdict::Unknown
	);
	assert_eq!(
		access.check(GuildId::new(2), ChannelId::new(VC)).await,
		AccessVerdict::Unknown
	);
}

/// 隠された (Bot が見られない) ボイスチャンネル `VC` の Channel Update を入れる。
/// - Discord の形のとおり、名前を伏せ、上書きは @everyone の VIEW_CHANNEL を拒む 1 件だけにする
fn obfuscate_vc(cache: &DefaultInMemoryCache) {
	let channel = json!({"id": VC.to_string(), "guild_id": GUILD.to_string(), "type": 2,
		"name": "___hidden___", "position": 0,
		"flags": ChannelFlags::CHANNEL_OBFUSCATED.bits(),
		"permission_overwrites": [{"id": GUILD.to_string(), "type": 0, "allow": "0",
			"deny": Permissions::VIEW_CHANNEL.bits().to_string()}]});
	let channel: Channel = serde_json::from_value(channel).unwrap();
	cache.update(&ChannelUpdate(channel));
}

/// 隠しの解けた `VC` の Channel Update を入れる。@everyone の閲覧を拒み、`BOT_ROLE` に閲覧を許す。
fn reveal_vc(cache: &DefaultInMemoryCache) {
	let view = Permissions::VIEW_CHANNEL.bits().to_string();
	let channel = json!({"id": VC.to_string(), "guild_id": GUILD.to_string(), "type": 2,
		"name": "vc", "position": 0, "flags": 0, "user_limit": 0,
		"permission_overwrites": [
			{"id": GUILD.to_string(), "type": 0, "allow": "0", "deny": view},
			{"id": BOT_ROLE.to_string(), "type": 0, "allow": view, "deny": "0"}]});
	let channel: Channel = serde_json::from_value(channel).unwrap();
	cache.update(&ChannelUpdate(channel));
}

/// 隠されたチャンネルは、Bot が見られないことの表れなので、閲覧の不足として断る (確かめずに参加しない)。
#[tokio::test]
async fn obfuscated_channel_is_denied_for_view_channel() {
	let cache = cache_with(&[BOT_ROLE], BOT_ROLE, 0, &[]);
	obfuscate_vc(&cache);
	let flags = cache
		.channel(twilight_model::id::Id::new(VC))
		.and_then(|channel| channel.flags);
	assert_eq!(flags, Some(ChannelFlags::CHANNEL_OBFUSCATED));
	assert_eq!(
		check(cache).await,
		AccessVerdict::Denied(Denial::Missing(vec![VoicePermission::ViewChannel]))
	);
}

/// 見られるようになって完全な Channel Update が届けば、入れる。
#[tokio::test]
async fn revealed_channel_is_allowed_again() {
	let cache = cache_with(&[BOT_ROLE], BOT_ROLE, 0, &[]);
	obfuscate_vc(&cache);
	reveal_vc(&cache);
	assert_eq!(check(cache).await, AccessVerdict::Allowed);
}

/// Bot のメンバー情報を固定の役職で返す、Discord の REST の代わりのサーバーを起動する。
///
/// # 戻り値
/// 待ち受けの番地 (`127.0.0.1:<port>`) と、受けた要求のパスの記録
async fn start_member_server(roles: &[u64]) -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
	let member = json!({
		"user": {"id": BOT.to_string(), "username": "bot", "discriminator": "0", "avatar": null, "bot": true},
		"roles": roles.iter().map(u64::to_string).collect::<Vec<_>>(),
		"joined_at": "2024-01-01T00:00:00.000000+00:00", "deaf": false, "mute": false, "flags": 0
	});
	let paths = Arc::new(std::sync::Mutex::new(Vec::new()));
	let recorded = Arc::clone(&paths);
	let app = axum::Router::new().fallback(move |uri: axum::http::Uri| {
		let member = member.clone();
		let recorded = Arc::clone(&recorded);
		async move {
			recorded.lock().unwrap().push(uri.path().to_owned());
			axum::Json(member)
		}
	});
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
	let addr = listener.local_addr().unwrap().to_string();
	tokio::spawn(async move {
		let _ = axum::serve(listener, app).await;
	});
	(addr, paths)
}

/// REST で取り直した Bot の役職で判定すること (キャッシュの役職が古くても、新しい役職で入れる)。
#[tokio::test]
async fn rest_roles_override_stale_cached_roles() {
	// キャッシュの Bot には役職が無く、それだけなら接続を断られる
	let cache = cache_with(&[], BOT_ROLE, 0, &[]);
	assert_eq!(
		check(Arc::clone(&cache)).await,
		AccessVerdict::Denied(Denial::Missing(vec![VoicePermission::Connect]))
	);
	// REST は接続を許す役職を返す
	let (addr, paths) = start_member_server(&[BOT_ROLE]).await;
	let http = Client::builder()
		.token("Bot test".to_string())
		.proxy(addr, true)
		.build();
	let access = DiscordVoiceAccess::new(
		Arc::new(http),
		cache,
		UserId::new(BOT),
		Duration::from_secs(3),
	);
	assert_eq!(
		access.check(GuildId::new(GUILD), ChannelId::new(VC)).await,
		AccessVerdict::Allowed
	);
	let paths = paths.lock().unwrap().clone();
	assert_eq!(paths.len(), 1, "{paths:?}");
	assert!(
		paths[0].ends_with(&format!("/guilds/{GUILD}/members/{BOT}")),
		"{paths:?}"
	);
}
