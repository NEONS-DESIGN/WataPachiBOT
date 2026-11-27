//! `InMemoryCache` からの取り出し。ギルド全体の権限の計算の入力と、ボイスチャンネルの聴者の数。
//! - キャッシュの参照 (DashMap のロック) は関数の中で手放し、持ち出すのは複製した値だけにする
//!   (参照を持ったまま await すると、Gateway の受信のキャッシュの更新を止めるため)
//! - メンバーの役職はキャッシュに頼らず、呼び出し側が渡す (`GUILD_MEMBERS` の Intent を使わないため、
//!   実行者の役職は `MessageCreate` の `member` から取る)

use twilight_cache_inmemory::DefaultInMemoryCache;
use twilight_model::guild::Permissions;
use twilight_model::id::marker::RoleMarker;
use twilight_model::id::Id;

use super::permissions::PermissionInput;
use crate::domain::{ChannelId, GuildId, UserId};

/// ギルド全体の権限の計算に要る情報の複製。`input` で `PermissionInput` を作る。
#[derive(Debug, Clone)]
pub struct GuildPermissionData {
	owner_id: UserId,
	everyone: Permissions,
	member_roles: Vec<(Id<RoleMarker>, Permissions)>,
}

impl GuildPermissionData {
	/// キャッシュで見つかった、メンバーの役職の数 (`@everyone` を含まない)。
	///
	/// - 渡した役職の数より少なければ、キャッシュに無い役職がある (その役職の権限と上書きは計算に入らない)
	///
	/// # 呼び出し例
	/// ```ignore
	/// let complete = data.known_roles() == role_ids.len();
	/// ```
	pub fn known_roles(&self) -> usize {
		self.member_roles.len()
	}

	/// 権限の計算の入力を作る。
	///
	/// # 引数
	/// - `guild_id`: 対象のギルド
	/// - `user_id`: 権限を調べるメンバー
	///
	/// # 戻り値
	/// `self` を借りた入力
	///
	/// # 呼び出し例
	/// ```ignore
	/// let admin = is_server_admin(&data.input(guild, user));
	/// ```
	pub fn input(&self, guild_id: GuildId, user_id: UserId) -> PermissionInput<'_> {
		PermissionInput {
			guild_id,
			user_id,
			owner_id: self.owner_id,
			everyone: self.everyone,
			member_roles: &self.member_roles,
		}
	}
}

/// キャッシュから、ギルド全体の権限の計算の入力 (所有者、`@everyone`、メンバーの役職) を集める。
///
/// - チャンネルは見ない (サーバー管理者の判定はチャンネルの上書きに左右されないため)
/// - キャッシュに無い役職は権限なしとして扱う (安全側)
///
/// # 引数
/// - `cache`: キャッシュ
/// - `guild`: 対象のギルド
/// - `role_ids`: メンバーの役職 (`@everyone` を含めない)
///
/// # 戻り値
/// ギルドか `@everyone` の役職がキャッシュに無ければ None
///
/// # 呼び出し例
/// ```ignore
/// let data = guild_permission_data(&cache, guild, &member.roles);
/// ```
pub fn guild_permission_data(
	cache: &DefaultInMemoryCache,
	guild: GuildId,
	role_ids: &[Id<RoleMarker>],
) -> Option<GuildPermissionData> {
	let owner_id = cache.guild(guild)?.owner_id();
	let everyone = cache.role(guild.cast())?.resource().permissions;
	let member_roles = role_ids
		.iter()
		.filter_map(|id| {
			cache
				.role(*id)
				.map(|role| (*id, role.resource().permissions))
		})
		.collect();
	Some(GuildPermissionData {
		owner_id,
		everyone,
		member_roles,
	})
}

/// 利用者がそのギルドで入っているボイスチャンネル。
///
/// # 戻り値
/// 入っていない、またはキャッシュに無ければ None
///
/// # 呼び出し例
/// ```ignore
/// let voice_channel = voice_channel_of(&cache, guild, user);
/// ```
pub fn voice_channel_of(
	cache: &DefaultInMemoryCache,
	guild: GuildId,
	user: UserId,
) -> Option<ChannelId> {
	cache
		.voice_state(user, guild)
		.map(|state| state.channel_id())
}

/// ボイスチャンネルの聴者 (Bot 以外) を数える。
///
/// - `members` は (利用者, Bot か)。Bot かが分からない (キャッシュに無い) 利用者は人として数える
/// - `bot_id` は印が無くても数えない
///
/// # 引数
/// - `members`: チャンネルにいる利用者
/// - `bot_id`: この Bot のユーザー ID
///
/// # 戻り値
/// 聴者の人数
///
/// # 呼び出し例
/// ```ignore
/// let listeners = count_listeners([(user, Some(false))], bot_id);
/// ```
pub fn count_listeners(
	members: impl IntoIterator<Item = (UserId, Option<bool>)>,
	bot_id: UserId,
) -> u32 {
	let count = members
		.into_iter()
		.filter(|(user, is_bot)| *user != bot_id && *is_bot != Some(true))
		.count();
	u32::try_from(count).unwrap_or(u32::MAX)
}

/// キャッシュの、ボイスチャンネルの聴者 (Bot 以外) の人数。
///
/// - チャンネルの有無は確かめない。誰もいなければ 0
///
/// # 呼び出し例
/// ```ignore
/// let listeners = listeners_in_channel(&cache, channel, bot_id);
/// ```
pub fn listeners_in_channel(
	cache: &DefaultInMemoryCache,
	channel: ChannelId,
	bot_id: UserId,
) -> u32 {
	// 声の状態のロックを手放してから利用者を引く
	let users: Vec<UserId> = match cache.voice_channel_states(channel) {
		Some(states) => states.map(|state| state.user_id()).collect(),
		None => Vec::new(),
	};
	let members = users
		.into_iter()
		.map(|user| (user, cache.user(user).map(|found| found.bot)));
	count_listeners(members, bot_id)
}

/// 復元のための、ボイスチャンネルの聴者 (Bot 以外) の人数。
///
/// # 戻り値
/// チャンネルがキャッシュに無い、または別のギルドのチャンネルなら None
///
/// # 呼び出し例
/// ```ignore
/// restore_all(&registry, &db, |g, c| restore_listeners(&cache, g, c, bot_id), stagger).await;
/// ```
pub fn restore_listeners(
	cache: &DefaultInMemoryCache,
	guild: GuildId,
	channel: ChannelId,
	bot_id: UserId,
) -> Option<u32> {
	let in_guild = cache.channel(channel)?.guild_id == Some(guild);
	in_guild.then(|| listeners_in_channel(cache, channel, bot_id))
}
