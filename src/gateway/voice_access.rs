//! 参加の前のボイスチャンネルの確認 (`VoiceAccess`) の本番の実装と、その判定。
//! - `judge` と `count_occupants` は Discord にもキャッシュにも触れない純粋な関数
//! - `DiscordVoiceAccess` はチャンネルと役職の権限をキャッシュから、Bot の役職を REST から取って判定する
//!   (`GUILD_MEMBERS` の Intent を使わないため、キャッシュの Bot の役職は起動の後の変更で古くなりうる)

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use twilight_cache_inmemory::DefaultInMemoryCache;
use twilight_http::Client;
use twilight_model::channel::permission_overwrite::PermissionOverwrite;
use twilight_model::channel::ChannelType;
use twilight_model::guild::Permissions;
use twilight_model::id::marker::RoleMarker;
use twilight_model::id::Id;

use super::cache_view::guild_permission_data;
use super::permissions::channel_permissions;
use crate::audio::access::{AccessVerdict, Denial, VoiceAccess, VoicePermission};
use crate::domain::{ChannelId, GuildId, UserId};

/// 参加に要る権限と、足りないときに挙げる名前。挙げる順に並べる。
const REQUIRED: [(Permissions, VoicePermission); 3] = [
	(Permissions::VIEW_CHANNEL, VoicePermission::ViewChannel),
	(Permissions::CONNECT, VoicePermission::Connect),
	(Permissions::SPEAK, VoicePermission::Speak),
];

/// チャンネルでの Bot の権限と人数から、参加してよいかを決める。
///
/// - 足りない権限があれば、すべて挙げて断る (満員より先に見る)
/// - 人数の上限 (1 以上) に達していて `MOVE_MEMBERS` が無ければ、満員として断る。上限 0 と None は無制限
///
/// # 引数
/// - `perms`: チャンネルの権限の上書きを適用した Bot の権限
/// - `user_limit`: チャンネルの人数の上限
/// - `occupants`: チャンネルにいる利用者の数 (この Bot 自身を除く)
///
/// # 戻り値
/// `Allowed` か `Denied`。`Unknown` は返さない
///
/// # 呼び出し例
/// ```ignore
/// let verdict = judge(perms, channel.user_limit, occupants);
/// ```
pub fn judge(perms: Permissions, user_limit: Option<u32>, occupants: u32) -> AccessVerdict {
	let missing: Vec<VoicePermission> = REQUIRED
		.iter()
		.filter(|(perm, _)| !perms.contains(*perm))
		.map(|(_, name)| *name)
		.collect();
	if !missing.is_empty() {
		return AccessVerdict::Denied(Denial::Missing(missing));
	}
	let full = user_limit.is_some_and(|limit| limit > 0 && occupants >= limit);
	if full && !perms.contains(Permissions::MOVE_MEMBERS) {
		return AccessVerdict::Denied(Denial::Full);
	}
	AccessVerdict::Allowed
}

/// チャンネルにいる利用者を、この Bot 自身を除いて数える (Bot は数える)。
///
/// - 回復の再接続では、Bot 自身がまだチャンネルに残っていることがあるため除く
///
/// # 引数
/// - `users`: チャンネルにいる利用者
/// - `bot_id`: この Bot のユーザー ID
///
/// # 戻り値
/// 人数
///
/// # 呼び出し例
/// ```ignore
/// let occupants = count_occupants(states.map(|s| s.user_id()), bot_id);
/// ```
pub fn count_occupants(users: impl IntoIterator<Item = UserId>, bot_id: UserId) -> u32 {
	let count = users.into_iter().filter(|user| *user != bot_id).count();
	u32::try_from(count).unwrap_or(u32::MAX)
}

/// キャッシュから複製した、判定に要るボイスチャンネルの情報。
struct VoiceChannelInfo {
	kind: ChannelType,
	overwrites: Vec<PermissionOverwrite>,
	user_limit: Option<u32>,
}

/// Discord (キャッシュと REST) で参加の前の確認をする `VoiceAccess` の本番の実装。
pub struct DiscordVoiceAccess {
	http: Arc<Client>,
	cache: Arc<DefaultInMemoryCache>,
	bot_id: UserId,
	rest_timeout: Duration,
}

impl DiscordVoiceAccess {
	/// 確認役を作る。
	///
	/// # 引数
	/// - `http`: Bot の役職を取り直す REST のクライアント
	/// - `cache`: チャンネル・役職・ボイスの状態を引くキャッシュ
	/// - `bot_id`: この Bot のユーザー ID
	/// - `rest_timeout`: REST の上限時間 (`voice.access_check_timeout_secs`)
	///
	/// # 戻り値
	/// 確認役
	///
	/// # 呼び出し例
	/// ```ignore
	/// let access = DiscordVoiceAccess::new(http, cache, bot_id, Duration::from_secs(3));
	/// ```
	pub fn new(
		http: Arc<Client>,
		cache: Arc<DefaultInMemoryCache>,
		bot_id: UserId,
		rest_timeout: Duration,
	) -> Self {
		Self {
			http,
			cache,
			bot_id,
			rest_timeout,
		}
	}

	/// キャッシュからボイスチャンネルの情報を複製する。参照はこの中で手放す。
	///
	/// # 引数
	/// - `guild`: ギルド
	/// - `channel`: 入ろうとするチャンネル
	///
	/// # 戻り値
	/// チャンネルの情報
	///
	/// # エラー
	/// 確かめない理由 (無い、別のギルドのもの、ボイスチャンネル (`GuildVoice`) でない)
	///
	/// # 呼び出し例
	/// ```ignore
	/// let info = self.voice_channel(guild, channel)?;
	/// ```
	fn voice_channel(
		&self,
		guild: GuildId,
		channel: ChannelId,
	) -> Result<VoiceChannelInfo, &'static str> {
		let Some(cached) = self.cache.channel(channel) else {
			return Err("チャンネルがキャッシュに無いため、権限を確かめません");
		};
		if cached.guild_id != Some(guild) {
			return Err("別のギルドのチャンネルのため、権限を確かめません");
		}
		if cached.kind != ChannelType::GuildVoice {
			return Err("ボイスチャンネルでない (ステージチャンネルなど) ため、権限を確かめません");
		}
		Ok(VoiceChannelInfo {
			kind: cached.kind,
			overwrites: cached.permission_overwrites.clone().unwrap_or_default(),
			user_limit: cached.user_limit,
		})
	}

	/// Bot の役職を取る。REST で取り直し、失敗したらキャッシュのものを使う。
	///
	/// - REST は `rest_timeout` で打ち切る。失敗と時間切れは WARN に残す
	///
	/// # 引数
	/// - `guild`: ギルド
	///
	/// # 戻り値
	/// Bot の役職 (`@everyone` を含まない)。REST でもキャッシュでも分からなければ None
	///
	/// # 呼び出し例
	/// ```ignore
	/// let roles = self.bot_roles(guild).await;
	/// ```
	async fn bot_roles(&self, guild: GuildId) -> Option<Vec<Id<RoleMarker>>> {
		let request = async {
			let response = self
				.http
				.guild_member(guild, self.bot_id)
				.await
				.map_err(|error| error.to_string())?;
			response.model().await.map_err(|error| error.to_string())
		};
		match tokio::time::timeout(self.rest_timeout, request).await {
			Ok(Ok(member)) => return Some(member.roles),
			Ok(Err(error)) => tracing::warn!(
				guild = guild.get(),
				%error,
				"Bot の役職を取り直せないため、キャッシュの役職で確かめます"
			),
			Err(_) => tracing::warn!(
				guild = guild.get(),
				"Bot の役職の取り直しが上限時間を過ぎたため、キャッシュの役職で確かめます"
			),
		}
		self.cache
			.member(guild, self.bot_id)
			.map(|member| member.roles().to_vec())
	}
}

#[async_trait]
impl VoiceAccess for DiscordVoiceAccess {
	/// チャンネルでの Bot の権限と人数から、参加してよいかを確かめる。
	///
	/// - チャンネル・ギルド・Bot の役職のどれかが分からなければ `Unknown` (debug に残す)。
	///   Bot の役職の一部がキャッシュに無いときも `Unknown`
	/// - 判定は `judge`。人数は Bot 自身を除いて数える
	///
	/// # 引数
	/// - `guild`: ギルド
	/// - `channel`: 入ろうとするチャンネル
	///
	/// # 戻り値
	/// `Allowed`、`Denied` (理由つき)、確かめられなければ `Unknown`
	///
	/// # 呼び出し例
	/// ```ignore
	/// let verdict = access.check(guild, channel).await;
	/// ```
	async fn check(&self, guild: GuildId, channel: ChannelId) -> AccessVerdict {
		let info = match self.voice_channel(guild, channel) {
			Ok(info) => info,
			Err(reason) => {
				tracing::debug!(guild = guild.get(), channel = channel.get(), "{reason}");
				return AccessVerdict::Unknown;
			}
		};
		let Some(roles) = self.bot_roles(guild).await else {
			tracing::debug!(
				guild = guild.get(),
				"Bot の役職が分からないため、権限を確かめません"
			);
			return AccessVerdict::Unknown;
		};
		let Some(data) = guild_permission_data(&self.cache, guild, &roles) else {
			tracing::debug!(
				guild = guild.get(),
				"ギルドの情報がキャッシュに無いため、権限を確かめません"
			);
			return AccessVerdict::Unknown;
		};
		// キャッシュに無い役職は権限も上書きも計算に入らず、入れるのに断ることがあるため、確かめない
		if data.known_roles() != roles.len() {
			tracing::debug!(
				guild = guild.get(),
				roles = roles.len(),
				known = data.known_roles(),
				"Bot の役職の一部がキャッシュに無いため、権限を確かめません"
			);
			return AccessVerdict::Unknown;
		}
		let perms =
			channel_permissions(&data.input(guild, self.bot_id), info.kind, &info.overwrites);
		// 声の状態のロックは集めた後に手放す
		let users: Vec<UserId> = match self.cache.voice_channel_states(channel) {
			Some(states) => states.map(|state| state.user_id()).collect(),
			None => Vec::new(),
		};
		judge(perms, info.user_limit, count_occupants(users, self.bot_id))
	}
}
