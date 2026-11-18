//! アクターが送る通知の組み立て。
//! - キーと差し込みの名前は `keys` にまとめる。messages/ja.toml・messages/en.toml と src/messages.rs の NOTICE_KEYS に同じキーがある
//! - 呼び出し: `self.notify(channel, titled(keys::QUEUED, &title));`

use super::message::LookupKind;
use super::notifier::{Card, Notice, PlaylistCard};
use crate::domain::{ResolveError, ResolveErrorCode};
use crate::ui::embed::{code_span_text, escape_markdown};

/// 通知のキーと差し込みの名前。
pub(super) mod keys {
	pub const PREPARING: &str = "play.preparing";
	pub const SLOW: &str = "play.slow";
	pub const QUEUED: &str = "play.queued";
	pub const NOW_PLAYING: &str = "play.now_playing";
	pub const PLAYLIST_QUEUED: &str = "play.playlist_queued";
	pub const PLAYLIST_TRUNCATED: &str = "play.playlist_truncated";
	pub const QUEUE_FULL: &str = "play.queue_full";
	pub const NOT_IN_VOICE: &str = "error.not_in_voice";
	pub const UNSUPPORTED_URL: &str = "error.unsupported_url";
	pub const LOOKUP_FAILED: &str = "error.lookup_failed";
	pub const LOOKUP_SEARCH_EMPTY: &str = "error.lookup_search_empty";
	pub const LOOKUP_NOT_FOUND: &str = "error.lookup_not_found";
	pub const LOOKUP_PRIVATE: &str = "error.lookup_private";
	pub const LOOKUP_GEO_BLOCKED: &str = "error.lookup_geo_blocked";
	pub const LOOKUP_AGE_RESTRICTED: &str = "error.lookup_age_restricted";
	pub const LOOKUP_TIMEOUT: &str = "error.lookup_timeout";
	pub const LOOKUP_PLAYLIST_UNAVAILABLE: &str = "error.lookup_playlist_unavailable";
	pub const PLAYLIST_EMPTY: &str = "error.playlist_empty";
	pub const TRACK_SKIPPED: &str = "error.track_skipped";
	pub const CONNECT_FAILED: &str = "error.connect_failed";
	pub const PAUSED: &str = "control.paused";
	pub const RESUMED: &str = "control.resumed";
	pub const SKIPPED: &str = "control.skipped";
	pub const LEFT: &str = "control.left";
	pub const AUTO_LEFT: &str = "control.auto_left";
	pub const MOVED: &str = "control.moved";
	pub const VOLUME: &str = "control.volume";
	pub const LOOP_ON: &str = "control.loop_on";
	pub const LOOP_OFF: &str = "control.loop_off";
	pub const CLEARED: &str = "queue.cleared";
	pub const SHUFFLED: &str = "queue.shuffled";
	pub const NOTHING_PLAYING: &str = "error.nothing_playing";
	pub const ALREADY_PAUSED: &str = "error.already_paused";
	pub const NOT_PAUSED: &str = "error.not_paused";
	pub const NOT_CONNECTED: &str = "error.not_connected";
	pub const SAME_CHANNEL: &str = "error.same_channel";
	pub const INDEX_OUT_OF_RANGE: &str = "error.index_out_of_range";
	pub const CLEAR_OUT_OF_RANGE: &str = "error.clear_out_of_range";
	pub const QUEUE_EMPTY: &str = "error.queue_empty";
	pub const VOLUME_OUT_OF_RANGE: &str = "error.volume_out_of_range";
	pub const AUDIO_FAILED: &str = "error.audio_failed";
	pub const SAVE_FAILED: &str = "error.save_failed";
	pub const NOT_SAME_CHANNEL: &str = "error.not_same_channel";
	pub const RECOVERING: &str = "error.recovering";
	pub const VIDEO_NOT_FOUND: &str = "error.video_not_found";
	pub const VIDEO_PRIVATE: &str = "error.video_private";
	pub const VIDEO_GEO_BLOCKED: &str = "error.video_geo_blocked";
	pub const VIDEO_AGE_RESTRICTED: &str = "error.video_age_restricted";
	pub const REJOIN_LIMIT: &str = "error.rejoin_limit";
	pub const REJOIN_FAILED: &str = "error.rejoin_failed";
	pub const FETCH_STOPPED: &str = "error.fetch_stopped";
	pub const RATE_LIMITED_LOOKUP: &str = "error.rate_limited_lookup";
	pub const RATE_LIMITED_STOPPED: &str = "error.rate_limited_stopped";
	pub const RESTORE_FAILED: &str = "error.restore_failed";
	pub const LIVE_UNSUPPORTED: &str = "error.live_unsupported";
	pub const LIVE_SKIPPED: &str = "error.live_skipped";
	pub const RATE_LIMITED: &str = "play.rate_limited";
	pub const PIPE_FALLBACK: &str = "play.pipe_fallback";
	pub const REJOINED: &str = "control.rejoined";
	pub const KICKED: &str = "control.kicked";
	pub const RESTORED: &str = "play.restored";
	pub const RESTORED_PAUSED: &str = "play.restored_paused";
	pub const COMPLETED: &str = "play.completed";
	pub const ARG_TITLE: &str = "title";
	pub const ARG_ADDED: &str = "added";
	pub const ARG_REJECTED: &str = "rejected";
	pub const ARG_COUNT: &str = "count";
	pub const ARG_PERCENT: &str = "percent";
	pub const ARG_MIN: &str = "min";
	pub const ARG_MAX: &str = "max";
	pub const ARG_PREFIX: &str = "prefix";
	pub const ARG_LIMIT: &str = "limit";
	pub const ARG_CHANNEL: &str = "channel";
}

/// キー 1 つだけの通知を作る。
///
/// # 引数
/// - `key`: メッセージのキー
///
/// # 戻り値
/// 差し込みの無い通知
///
/// # 呼び出し例
/// ```ignore
/// self.notify(channel, plain(keys::SLOW));
/// ```
pub(super) fn plain(key: &'static str) -> Notice {
	Notice::text(key, Vec::new())
}

/// 曲名を差し込む通知を作る。
///
/// # 引数
/// - `key`: メッセージのキー
/// - `title`: 曲名
///
/// # 戻り値
/// `title` を差し込む通知
///
/// # 呼び出し例
/// ```ignore
/// self.notify(channel, titled(keys::QUEUED, &title));
/// ```
pub(super) fn titled(key: &'static str, title: &str) -> Notice {
	Notice {
		key,
		args: vec![(keys::ARG_TITLE, title.to_owned())],
		card: None,
	}
}

/// 曲名を地の文へ差し込む通知を作る。曲名の Markdown を逃がす。
///
/// - 文面の `{title}` が囲み無しで置かれている通知 (`play.restored` など) に使う
///
/// # 引数
/// - `key`: メッセージのキー
/// - `title`: 曲名
///
/// # 戻り値
/// 逃がした曲名を差し込む通知
///
/// # 呼び出し例
/// ```ignore
/// self.notify(channel, titled_text(keys::RESTORED, &title));
/// ```
pub(super) fn titled_text(key: &'static str, title: &str) -> Notice {
	titled(key, &escape_markdown(title))
}

/// 曲名をコードの囲み (`` `{title}` ``) へ差し込む通知を作る。囲みを壊すバッククォートを置き換える。
///
/// - 飛ばしの通知 (`error.track_skipped`、`error.video_*`、`error.live_skipped`) に使う
///
/// # 引数
/// - `key`: メッセージのキー
/// - `title`: 曲名
///
/// # 戻り値
/// 置き換えた曲名を差し込む通知
///
/// # 呼び出し例
/// ```ignore
/// self.notify(channel, titled_code(keys::TRACK_SKIPPED, &title));
/// ```
pub(super) fn titled_code(key: &'static str, title: &str) -> Notice {
	titled(key, &code_span_text(title))
}

/// 値を差し込む通知を作る。
///
/// # 引数
/// - `key`: メッセージのキー
/// - `args`: 差し込みの名前と値の組
///
/// # 戻り値
/// `args` を差し込む通知
///
/// # 呼び出し例
/// ```ignore
/// self.notify(channel, with_args(keys::CLEARED, [(keys::ARG_COUNT, removed.to_string())]));
/// ```
pub(super) fn with_args<const N: usize>(
	key: &'static str,
	args: [(&'static str, String); N],
) -> Notice {
	Notice {
		key,
		args: args.into(),
		card: None,
	}
}

/// `?p` の問い合わせの失敗の通知のキーを選ぶ (旧 Bot の文言の出し分け)。
///
/// - 利用者側の原因 (見つからない、非公開、地域制限、年齢制限、対応外の URL) は理由の文言
///   - 見つからないときは、検索語なら検索用、プレイリストならプレイリスト用の文言
///   - プレイリストの非公開も、プレイリスト用の文言
/// - プールの一時停止 (`Paused`) と YouTube の利用制限 (`rate_limited`) は `error.rate_limited_lookup`
/// - タイムアウトは `error.lookup_timeout`
/// - それ以外 (`internal`、ワーカーの終了、ワーカーが無い) は `error.lookup_failed`
///
/// # 引数
/// - `error`: 問い合わせのエラー
/// - `kind`: 問い合わせの種類
///
/// # 戻り値
/// 差し込みの無い通知のキー
///
/// # 呼び出し例
/// ```ignore
/// self.notify(channel, plain(lookup_failure_key(&error, kind)));
/// ```
pub(super) fn lookup_failure_key(error: &ResolveError, kind: LookupKind) -> &'static str {
	let code = match error {
		ResolveError::Paused { .. } => return keys::RATE_LIMITED_LOOKUP,
		ResolveError::Timeout => return keys::LOOKUP_TIMEOUT,
		ResolveError::WorkerDied | ResolveError::Unavailable => return keys::LOOKUP_FAILED,
		ResolveError::Worker { code, .. } => *code,
	};
	match (code, kind) {
		(ResolveErrorCode::NotFound | ResolveErrorCode::Private, LookupKind::Playlist) => {
			keys::LOOKUP_PLAYLIST_UNAVAILABLE
		}
		(ResolveErrorCode::NotFound, LookupKind::Search) => keys::LOOKUP_SEARCH_EMPTY,
		(ResolveErrorCode::NotFound, _) => keys::LOOKUP_NOT_FOUND,
		(ResolveErrorCode::Private, _) => keys::LOOKUP_PRIVATE,
		(ResolveErrorCode::GeoBlocked, _) => keys::LOOKUP_GEO_BLOCKED,
		(ResolveErrorCode::AgeRestricted, _) => keys::LOOKUP_AGE_RESTRICTED,
		(ResolveErrorCode::UnsupportedUrl, _) => keys::UNSUPPORTED_URL,
		(ResolveErrorCode::RateLimited, _) => keys::RATE_LIMITED_LOOKUP,
		(ResolveErrorCode::Internal, _) => keys::LOOKUP_FAILED,
	}
}

/// プレイリストの追加のカード付きの通知を作る。断った曲があれば `play.playlist_truncated` にする。
///
/// - 差し込みの `added` と `rejected` も持つ (カードを描けないときの記録と試験のため)
///
/// # 引数
/// - `card`: プレイリストのカード (追加数と断った数を含む)
///
/// # 戻り値
/// カード付きの通知
///
/// # 呼び出し例
/// ```ignore
/// self.notify(channel, counted(card));
/// ```
pub(super) fn counted(card: PlaylistCard) -> Notice {
	let key = if card.rejected > 0 {
		keys::PLAYLIST_TRUNCATED
	} else {
		keys::PLAYLIST_QUEUED
	};
	Notice {
		key,
		args: vec![
			(keys::ARG_ADDED, card.added.to_string()),
			(keys::ARG_REJECTED, card.rejected.to_string()),
		],
		card: Some(Card::Playlist(card)),
	}
}
