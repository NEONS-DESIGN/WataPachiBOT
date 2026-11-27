//! 参加の前の権限の判定 (純粋な関数) の試験。

use twilight_model::guild::Permissions;

use watapachi_bot::audio::access::{AccessVerdict, Denial, VoicePermission};
use watapachi_bot::domain::UserId;
use watapachi_bot::gateway::voice_access::{count_occupants, judge};

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
	assert_eq!(denial.to_string(), "権限がありません: VIEW_CHANNEL, CONNECT");
	assert_eq!(Denial::Full.to_string(), "チャンネルが満員です");
}
