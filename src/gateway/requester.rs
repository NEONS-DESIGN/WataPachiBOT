//! カードのフッターに出す、リクエストした人の表示名とアイコンを作る。
//! - 名前とアイコンは送る時点の `InMemoryCache` から引く (`GUILD_MEMBERS` の Intent は使わないため、
//!   キャッシュに入るのは発言した人とボイスチャンネルにいる人だけ)
//! - キャッシュの参照は関数の中で手放し、複製した値だけを持ち出す
//! - 呼び出し: `let requester = lookup_requester(&cache, &config.ui, guild, user);`

use twilight_cache_inmemory::DefaultInMemoryCache;

use crate::config::UiConfig;
use crate::domain::{GuildId, UserId};
use crate::ui::embed::Requester;

/// 既定のアバターの種類の数。番号は `(ユーザー ID >> 22) % 6` (Discord の新しいユーザー名の方式)
const DEFAULT_AVATAR_COUNT: u64 = 6;
/// 既定のアバターの番号を取り出すときに、ユーザー ID を右へずらす桁数 (ID の時刻の部分)
const DEFAULT_AVATAR_SHIFT: u32 = 22;

/// キャッシュから取り出した、リクエストした人の情報。どれも無いことがある。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequesterSource {
	/// サーバーのニックネーム
	pub nick: Option<String>,
	/// サーバーのアバターのハッシュ
	pub member_avatar: Option<String>,
	/// 表示名 (global name)
	pub global_name: Option<String>,
	/// ユーザー名
	pub username: Option<String>,
	/// ユーザーのアバターのハッシュ
	pub user_avatar: Option<String>,
}

/// キャッシュの情報から、フッターの表示名とアイコンを作る。
///
/// - 名前はニックネーム、表示名、ユーザー名の順で、最初にあるもの。どれも無ければ None
/// - アイコンはサーバーのアバター、ユーザーのアバター、既定のアバターの順。URL は `ui` の雛形から作る
///
/// # 引数
/// - `ui`: アイコンの URL の雛形を持つ設定
/// - `guild`: カードを送るギルド
/// - `user`: リクエストした人
/// - `source`: キャッシュから取り出した情報
///
/// # 戻り値
/// 名前が分かれば表示、分からなければ None
///
/// # 呼び出し例
/// ```ignore
/// let requester = requester_from(&config.ui, guild, user, &source);
/// ```
pub fn requester_from(
	ui: &UiConfig,
	guild: GuildId,
	user: UserId,
	source: &RequesterSource,
) -> Option<Requester> {
	let name = source
		.nick
		.as_ref()
		.or(source.global_name.as_ref())
		.or(source.username.as_ref())?
		.clone();
	let user_id = user.get().to_string();
	let icon_url = if let Some(hash) = &source.member_avatar {
		ui.member_avatar_url
			.replace("{guild}", &guild.get().to_string())
			.replace("{user}", &user_id)
			.replace("{hash}", hash)
	} else if let Some(hash) = &source.user_avatar {
		ui.user_avatar_url
			.replace("{user}", &user_id)
			.replace("{hash}", hash)
	} else {
		let index = (user.get() >> DEFAULT_AVATAR_SHIFT) % DEFAULT_AVATAR_COUNT;
		ui.default_avatar_url.replace("{index}", &index.to_string())
	};
	Some(Requester { name, icon_url })
}

/// キャッシュを引いて、フッターの表示名とアイコンを作る。
///
/// - メンバーとユーザーのどちらもキャッシュに無ければ None (カードはメンションの項目で出す)
///
/// # 引数
/// - `cache`: Gateway のキャッシュ
/// - `ui`: アイコンの URL の雛形を持つ設定
/// - `guild`: カードを送るギルド
/// - `user`: リクエストした人
///
/// # 戻り値
/// 表示。分からなければ None
///
/// # 呼び出し例
/// ```ignore
/// let requester = lookup_requester(&cache, &config.ui, guild, user);
/// ```
pub fn lookup_requester(
	cache: &DefaultInMemoryCache,
	ui: &UiConfig,
	guild: GuildId,
	user: UserId,
) -> Option<Requester> {
	let mut source = RequesterSource::default();
	if let Some(member) = cache.member(guild, user) {
		source.nick = member.nick().map(str::to_owned);
		source.member_avatar = member.avatar().map(|hash| hash.to_string());
	}
	if let Some(cached) = cache.user(user) {
		source.global_name = cached.global_name.clone();
		source.username = Some(cached.name.clone());
		source.user_avatar = cached.avatar.map(|hash| hash.to_string());
	}
	requester_from(ui, guild, user, &source)
}
