//! 権限の計算。Discord にもキャッシュにも触れない純粋な関数だけを置く。
//! - 計算は twilight-util の `PermissionCalculator` に任せる
//! - 入力 (所有者、役職の権限) は呼び出し側がキャッシュから集めて渡す。
//!   チャンネルの権限を求めるときだけ、チャンネルの種類と上書きを別に渡す

use twilight_model::channel::permission_overwrite::PermissionOverwrite;
use twilight_model::channel::ChannelType;
use twilight_model::guild::Permissions;
use twilight_model::id::marker::RoleMarker;
use twilight_model::id::Id;
use twilight_util::permission_calculator::PermissionCalculator;

use crate::domain::{GuildId, UserId};

/// ギルド全体の権限の計算に要る情報。
#[derive(Debug, Clone, Copy)]
pub struct PermissionInput<'a> {
	pub guild_id: GuildId,
	pub user_id: UserId,
	pub owner_id: UserId,
	/// `@everyone` 役職の権限
	pub everyone: Permissions,
	/// メンバーが持つ役職と、その権限 (`@everyone` は含めない)
	pub member_roles: &'a [(Id<RoleMarker>, Permissions)],
}

impl PermissionInput<'_> {
	/// 計算機を作る (所有者を設定済み)。
	fn calculator(&self) -> PermissionCalculator<'_> {
		PermissionCalculator::new(
			self.guild_id,
			self.user_id,
			self.everyone,
			self.member_roles,
		)
		.owner_id(self.owner_id)
	}
}

/// メンバーの、そのチャンネルでの権限を返す。
///
/// - 今の本番の処理は使わない (`?setting` はギルド全体の権限で判定する)。チャンネルの権限が要る機能のために置く
///
/// # 引数
/// - `input`: ギルド全体の権限の計算に要る情報
/// - `channel_kind`: チャンネルの種類 (スレッドなら親のもの)
/// - `overwrites`: チャンネルの権限の上書き (スレッドなら親のもの)
///
/// # 戻り値
/// 役職の権限にチャンネルの上書きを適用した結果。所有者と `ADMINISTRATOR` は全権限。
///
/// # 呼び出し例
/// ```ignore
/// let perms = channel_permissions(&input, ChannelType::GuildText, &overwrites);
/// ```
pub fn channel_permissions(
	input: &PermissionInput<'_>,
	channel_kind: ChannelType,
	overwrites: &[PermissionOverwrite],
) -> Permissions {
	input.calculator().in_channel(channel_kind, overwrites)
}

/// サーバー管理者か。所有者、または `ADMINISTRATOR` を持つなら true。
///
/// - チャンネルの上書きでは `ADMINISTRATOR` を奪えないので、ギルド全体の権限で判定する
///
/// # 引数
/// - `input`: ギルド全体の権限の計算に要る情報
///
/// # 戻り値
/// サーバー管理者なら true
///
/// # 呼び出し例
/// ```ignore
/// if is_server_admin(&input) { /* ?setting を許す */ }
/// ```
pub fn is_server_admin(input: &PermissionInput<'_>) -> bool {
	input
		.calculator()
		.root()
		.contains(Permissions::ADMINISTRATOR)
}
