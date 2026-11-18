//! 権限の計算 (純粋な関数) の試験。

use twilight_model::channel::permission_overwrite::{PermissionOverwrite, PermissionOverwriteType};
use twilight_model::channel::ChannelType;
use twilight_model::guild::Permissions;
use twilight_model::id::marker::RoleMarker;
use twilight_model::id::Id;

use watapachi_bot::domain::{GuildId, UserId};
use watapachi_bot::gateway::permissions::{channel_permissions, is_server_admin, PermissionInput};

const GUILD: u64 = 1;
const USER: u64 = 2;
const OWNER: u64 = 3;
const ROLE: u64 = 4;

fn role() -> Id<RoleMarker> {
	Id::new(ROLE)
}

/// 基本の入力。呼び出し側が所有者と役職を差し替える。
fn input(owner: u64, member_roles: &[(Id<RoleMarker>, Permissions)]) -> PermissionInput<'_> {
	PermissionInput {
		guild_id: GuildId::new(GUILD),
		user_id: UserId::new(USER),
		owner_id: UserId::new(owner),
		everyone: Permissions::VIEW_CHANNEL | Permissions::SEND_MESSAGES,
		member_roles,
	}
}

/// テキストチャンネルで、上書きを適用して `perm` を持つか。
fn has(input: &PermissionInput<'_>, overwrites: &[PermissionOverwrite], perm: Permissions) -> bool {
	channel_permissions(input, ChannelType::GuildText, overwrites).contains(perm)
}

#[test]
fn owner_is_admin() {
	let i = input(USER, &[]);
	assert!(is_server_admin(&i));
	assert!(has(&i, &[], Permissions::MANAGE_ROLES));
}

#[test]
fn plain_member_is_not_admin() {
	let i = input(OWNER, &[]);
	assert!(!is_server_admin(&i));
	assert!(!has(&i, &[], Permissions::MANAGE_ROLES));
	assert!(has(&i, &[], Permissions::SEND_MESSAGES));
}

#[test]
fn administrator_role_is_admin_and_has_all() {
	let roles = [(role(), Permissions::ADMINISTRATOR)];
	let i = input(OWNER, &roles);
	assert!(is_server_admin(&i));
	assert!(has(&i, &[], Permissions::MANAGE_ROLES));
}

#[test]
fn administrator_is_kept_even_if_channel_denies_everything() {
	// サーバー管理者の判定はチャンネルに左右されない
	let roles = [(role(), Permissions::ADMINISTRATOR)];
	let overwrites = [PermissionOverwrite {
		allow: Permissions::empty(),
		deny: Permissions::all(),
		id: role().cast(),
		kind: PermissionOverwriteType::Role,
	}];
	let i = input(OWNER, &roles);
	assert!(is_server_admin(&i));
	assert!(has(&i, &overwrites, Permissions::MANAGE_ROLES));
}

#[test]
fn channel_overwrite_denies_role_permission() {
	let roles = [(role(), Permissions::ATTACH_FILES)];
	let i = input(OWNER, &roles);
	assert!(has(&i, &[], Permissions::ATTACH_FILES));
	let overwrites = [PermissionOverwrite {
		allow: Permissions::empty(),
		deny: Permissions::ATTACH_FILES,
		id: role().cast(),
		kind: PermissionOverwriteType::Role,
	}];
	assert!(!has(&i, &overwrites, Permissions::ATTACH_FILES));
	assert!(!is_server_admin(&i));
}

#[test]
fn member_overwrite_allows() {
	let overwrites = [PermissionOverwrite {
		allow: Permissions::ATTACH_FILES,
		deny: Permissions::empty(),
		id: Id::new(USER),
		kind: PermissionOverwriteType::Member,
	}];
	let i = input(OWNER, &[]);
	assert!(has(&i, &overwrites, Permissions::ATTACH_FILES));
}
