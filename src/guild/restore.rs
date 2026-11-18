//! 起動時の再生状態の復元。
//! - `restore_all` が `playback_state` の全行を読み、ギルドごとに `restore.stagger_ms` ずつずらして処理する
//! - そのギルドのアクターがすでにあれば、DB に触らずにアクターへ `GuildMessage::Restore` を送り、判断を任せる
//!   (聞き手が居ない、チャンネルが無い場合も。チャンネルが無いことは聞き手 0 人として伝える)。
//!   アクターがセッション中なら、今のセッションの行を消さないため
//! - アクターが無く、聞き手 (Bot 以外) が居れば、`Restore` を送る (アクターが作られる)。
//!   接続、解決、位置の指定、一時停止、通知はアクターが行う
//! - アクターが無く、聞き手が居ない、またはチャンネルが無い (削除、権限の喪失) なら再参加しない。
//!   今の行が読み込んだときの行と同じときだけ、曲をキューの先頭へ戻し、`playback_state` を消す (キューは残す)
//! - `Restore` を送れなければ (メールボックスが満杯、終了処理中)、行もキューも変えずにログに残す
//!   (行が残れば次の起動で改めて復元する)
//! - 呼び出し: Discord の準備ができた後に
//!   `restore_all(&registry, &db, |guild, channel| listeners_of(guild, channel), stagger).await;`

use std::time::Duration;

use super::message::{GuildMessage, RestoreRequest};
use super::registry::GuildRegistry;
use super::store;
use crate::db::playback::{self, PlaybackState};
use crate::db::DbHandles;
use crate::domain::{ChannelId, GuildId};

/// `playback_state` に残っている全ギルドの再生を復元する。
///
/// - ギルドの順 (ID の昇順) に 1 つずつ処理し、ギルドの間で `stagger` だけ待つ
/// - アクターへの送信は待たない。アクターの復元の完了も待たない
/// - `playback_state` を読めなければ、ログに残して何もしない
/// - DB の書き込み (キューへ戻す処理) の失敗は、1 回やり直してからログに残し、次のギルドへ進む
/// - アクターへ送れないとき、読み込んだ後に行が変わったときは、行もキューも変えない
///
/// # 引数
/// - `registry`: アクターの登録簿
/// - `db`: DB のハンドル
/// - `listeners`: ギルドとボイスチャンネルから、聞き手 (Bot 以外) の人数を返す。
///   チャンネルが無い、または権限が無ければ None
/// - `stagger`: ギルドの間の待ち時間 (`restore.stagger_ms`)
///
/// # 呼び出し例
/// ```ignore
/// let stagger = Duration::from_millis(config.restore.stagger_ms);
/// restore_all(&registry, &db, |guild, channel| cache.listeners(guild, channel), stagger).await;
/// ```
pub async fn restore_all(
	registry: &GuildRegistry,
	db: &DbHandles,
	listeners: impl Fn(GuildId, ChannelId) -> Option<u32>,
	stagger: Duration,
) {
	let states = match db.reader.read(playback::load_all).await {
		Ok(states) => states,
		Err(error) => {
			tracing::error!(%error, "再生状態を読めないため復元しません");
			return;
		}
	};
	tracing::info!(count = states.len(), "再生状態を復元します");
	for (index, state) in states.into_iter().enumerate() {
		if index > 0 {
			tokio::time::sleep(stagger).await;
		}
		restore_one(registry, db, &listeners, state).await;
	}
}

/// 1 ギルドの再生状態を復元する。
///
/// - アクターがあるか、聞き手が居れば、アクターへ `Restore` を送る。送れなければ何も変えない
/// - アクターが無く、聞き手が居ない、またはチャンネルが無ければ、行が変わっていないときだけ曲をキューへ戻して消す
///
/// # 引数
/// - `registry`: アクターの登録簿
/// - `db`: DB のハンドル
/// - `listeners`: 聞き手の人数を返す処理 (`restore_all` と同じ)
/// - `state`: 読み込んだときの再生状態
///
/// # 呼び出し例
/// ```ignore
/// restore_one(registry, db, &listeners, state).await;
/// ```
async fn restore_one(
	registry: &GuildRegistry,
	db: &DbHandles,
	listeners: &impl Fn(GuildId, ChannelId) -> Option<u32>,
	state: PlaybackState,
) {
	let guild = state.guild_id;
	let found = listeners(guild, state.voice_channel_id);
	match found {
		Some(0) => tracing::info!(guild = guild.get(), "ボイスチャンネルに聞き手が居ません"),
		None => tracing::warn!(
			guild = guild.get(),
			channel = state.voice_channel_id.get(),
			"ボイスチャンネルが無いか権限がありません"
		),
		Some(_) => {}
	}
	// チャンネルが無いことは、再参加しない点で聞き手 0 人と同じなので 0 人として伝える
	let count = found.unwrap_or(0);
	if count == 0 && !registry.contains(guild) {
		requeue(db, state).await;
		return;
	}
	let request = GuildMessage::Restore(RestoreRequest {
		state,
		listeners: count,
	});
	if registry.get_or_spawn(guild).try_send(request).is_err() {
		tracing::warn!(
			guild = guild.get(),
			"アクターが受け取れないため復元しません。再生状態とキューはそのまま残します"
		);
	}
}

/// 曲をキューの先頭へ戻し、`playback_state` を消す (1 トランザクション)。
///
/// - 今の行が読み込んだときの行と同じときだけ行う (`store::requeue_saved`)。変わっていたらログに残すだけ
/// - 失敗したら 1 回だけやり直す。それでも失敗したらログに残す
///   (`playback_state` が残れば、次の起動で改めて復元を試みる)
///
/// # 引数
/// - `db`: DB のハンドル
/// - `state`: 読み込んだときの再生状態
///
/// # 呼び出し例
/// ```ignore
/// requeue(db, state).await;
/// ```
async fn requeue(db: &DbHandles, state: PlaybackState) {
	let guild = state.guild_id;
	let result =
		store::write_with_retry(&db.writer, guild, move |c| store::requeue_saved(c, &state)).await;
	match result {
		Ok(true) => tracing::info!(
			guild = guild.get(),
			"再参加せず、曲をキューの先頭へ戻して再生状態を消しました"
		),
		Ok(false) => tracing::info!(
			guild = guild.get(),
			"読み込んだ後に再生状態が変わったため、キューへ戻しません"
		),
		Err(error) => tracing::error!(guild = guild.get(), %error, "曲をキューへ戻せません"),
	}
}
