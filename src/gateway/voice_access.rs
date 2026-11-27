//! 参加の前のボイスチャンネルの確認 (`VoiceAccess`) の本番の実装と、その判定。
//! - `judge` と `count_occupants` は Discord にもキャッシュにも触れない純粋な関数

use twilight_model::guild::Permissions;

use crate::audio::access::{AccessVerdict, Denial, VoicePermission};
use crate::domain::UserId;

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
