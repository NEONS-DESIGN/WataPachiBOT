//! GuildActor の再生位置の保存、起動時の復元、終了。
//! - `actor.rs` の子のモジュール。アクターの状態を直接扱う
//! - 保存: 再生中 (Playing) は `playback.position_save_interval_secs` ごとに、`select!` のタイマーで位置を保存する。
//!   `?pause`、`?resume`、作り直した曲の再生の開始、自動の回復の上限での一時停止でもすぐ保存する
//! - 位置は音声から得る (`voice.call_timeout_secs` で打ち切る)。得られなければ経過時間からの見積もりを使う
//! - 保存は `DbWriter::save_position` で、結果を待たない (同じギルドの未処理分は 1 件にまとまる)
//! - 保存する一時停止の状態は、Paused か、一時停止に戻す予定 (`pause_on_start`) があるか
//! - 復元: Idle のときだけ受ける。保存されていたボイスチャンネルへ接続し、解決し (`Priority::Interactive`)、
//!   保存した位置から再生する。一時停止していた曲は一時停止のまま載せる (`PlayRequest::paused`。音を出さない)。
//!   最初の再生で、保存されていたテキストチャンネルへ復元を通知する (再生中の通知の代わり)
//! - Idle 以外で復元が届いたら、2 つ目のセッションは始めず、`playback_state` も消さない。
//!   曲が今のセッションの曲でなければ、キューの先頭へ戻す
//! - 聞き手が 0 人 (チャンネルが無い場合を含む) の復元が Idle で届いたら、再参加しない。
//!   行が変わっていなければ曲をキューの先頭へ戻し、`playback_state` を消す
//! - 復元の接続に失敗したら (チャンネルが無い、権限が無いなど)、同じく曲をキューの先頭へ戻して
//!   `playback_state` を消し、キューを残して `?p` での再開を案内する
//! - 終了: 位置を保存し、`playback_state` を残したまま切断して応答する (次の起動で復元する)
//! - 呼び出し: アクターの `handle`、`run` のタイマー、`on_connected`、`on_started`、`start_next` から呼ばれる

use std::future::Future;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::Instant;

use super::GuildActor;
use crate::db::playback::{self, PlaybackState};
use crate::db::queue::{self, NewQueueItem};
use crate::db::DbError;
use crate::guild::message::RestoreRequest;
use crate::guild::notices::{keys, titled, titled_text, with_args};
use crate::guild::notifier::{Card, TrackCard};
use crate::guild::state::{PlayerState, SessionChannels};
use crate::guild::store;
use crate::ui::embed::escape_markdown;

impl GuildActor {
	/// 定期的な保存の時刻を更新する。`run` のループの先頭で呼ぶ。
	///
	/// - 再生中 (Playing) で時刻が無ければ、今から `playback.position_save_interval_secs` 後にする
	/// - 再生中でなければ消す (一時停止、準備、回復の間は定期的に保存しない)
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.refresh_save_timer();
	/// ```
	pub(super) fn refresh_save_timer(&mut self) {
		if self.state != PlayerState::Playing {
			self.save_at = None;
		} else if self.save_at.is_none() {
			let interval =
				Duration::from_secs(self.deps.config.playback.position_save_interval_secs);
			self.save_at = Some(Instant::now() + interval);
		}
	}

	/// 定期的な保存の時刻になった (または `Tick(TickKind::Position)` が届いた)。再生中なら位置を保存する。
	///
	/// - 次の時刻はループの先頭の `refresh_save_timer` が今から数え直す
	///
	/// # 呼び出し例
	/// ```ignore
	/// () = wait_deadline(save) => self.on_save_due().await,
	/// ```
	pub(super) async fn on_save_due(&mut self) {
		self.save_at = None;
		if self.state == PlayerState::Playing {
			self.save_current_position().await;
		}
	}

	/// 今の曲の位置と一時停止の状態を `playback_state` へ保存する。今の曲が無ければ何もしない。
	///
	/// - 位置は音声から得る。得られなければ見積もり (音声の呼び出しの時間切れは回復のきっかけになる)
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.save_current_position().await;
	/// ```
	pub(super) async fn save_current_position(&mut self) {
		let paused = self.persisted_paused();
		if let Some(position) = self.current_position().await {
			self.persist_position(position, paused).await;
		}
	}

	/// 位置と一時停止の状態を `playback_state` へ保存する。結果は待たず、失敗はログに残すだけ。
	///
	/// - `playback_state` に行が無ければ何も起きない (書き込みスレッドの UPDATE が 0 行になる)
	///
	/// # 引数
	/// - `position`: 保存する位置
	/// - `paused`: 一時停止中として保存するか
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.persist_position(position, paused).await;
	/// ```
	pub(super) async fn persist_position(&mut self, position: Duration, paused: bool) {
		let position_ms = u64::try_from(position.as_millis()).unwrap_or(u64::MAX);
		if let Err(error) = self
			.deps
			.db
			.writer
			.save_position(self.guild, position_ms, paused)
			.await
		{
			tracing::error!(guild = self.guild.get(), %error, "再生位置を保存できません");
		}
	}

	/// 保存する一時停止の状態。Paused か、今の曲を一時停止に戻す予定 (`pause_on_start`) があれば true。
	///
	/// # 戻り値
	/// 一時停止中として保存するなら true
	///
	/// # 呼び出し例
	/// ```ignore
	/// let paused = self.persisted_paused();
	/// ```
	pub(super) fn persisted_paused(&self) -> bool {
		self.state == PlayerState::Paused
			|| self
				.current
				.as_ref()
				.is_some_and(|current| current.pause_on_start)
	}

	/// 起動時の復元の要求を受ける。
	///
	/// - Idle 以外なら復元しない (セッションを二重に始めない)。`playback_state` は今のセッションの行なので消さない。
	///   曲が今のセッションの曲 (読み込みが今のセッションの書き込みの後だった) でなければ、キューの先頭へ戻す
	/// - Idle で聞き手が 0 人 (チャンネルが無い場合を含む) なら再参加しない。
	///   行が読み込んだときと同じなら、曲をキューの先頭へ戻して `playback_state` を消す (1 トランザクション)
	/// - Idle で聞き手が居れば、保存されていたボイスチャンネルとテキストチャンネルでセッションを始め、
	///   接続を別タスクで始める。接続の結果は `Connected` で届き、成功なら `start_restored` で再生する。
	///   失敗なら `on_restore_connect_failed` がキューを残して案内する
	/// - 準備中の通知は送らない (復元の通知は再生を始めたときに送る)
	///
	/// # 引数
	/// - `request`: 読み込んだときの再生状態と、聞き手の人数 (チャンネルが無ければ 0)
	///
	/// # 呼び出し例
	/// ```ignore
	/// GuildMessage::Restore(request) => self.on_restore(request).await,
	/// ```
	pub(super) async fn on_restore(&mut self, request: RestoreRequest) {
		let RestoreRequest {
			state: saved,
			listeners,
		} = request;
		self.listeners = listeners;
		if self.state != PlayerState::Idle {
			if self.is_session_state(&saved) {
				tracing::info!(
					guild = self.guild.get(),
					"今のセッションの再生状態のため、復元もキューへの追加もしません"
				);
				return;
			}
			tracing::warn!(
				guild = self.guild.get(),
				state = ?self.state,
				"セッション中のため復元せず、曲をキューの先頭へ戻します"
			);
			self.requeue_restored(saved.item).await;
			return;
		}
		if listeners == 0 {
			tracing::info!(guild = self.guild.get(), "聞き手が居ないため再参加しません");
			let _ = self.requeue_saved_state(saved).await;
			return;
		}
		tracing::info!(
			guild = self.guild.get(),
			video_id = %saved.item.video_id.0,
			position_ms = saved.position_ms,
			paused = saved.paused,
			"再生状態を復元します"
		);
		self.open_session(SessionChannels {
			voice: saved.voice_channel_id,
			text: saved.text_channel_id,
		});
		self.pending_restore = Some(saved);
	}

	/// 復元の再生状態が、今のセッションの曲の行か (セッションが書いた行を、復元が後から読み込んだ)。
	///
	/// - 今の曲と動画が同じで、通知先がセッションのテキストチャンネルと同じなら true
	/// - 前回の起動の曲が偶然同じ曲・同じ通知先だった場合も true になる (その曲は今再生中なので失われない)
	///
	/// # 引数
	/// - `saved`: 読み込んだときの再生状態
	///
	/// # 戻り値
	/// 今のセッションの行とみなすなら true
	///
	/// # 呼び出し例
	/// ```ignore
	/// if self.is_session_state(&saved) { return; }
	/// ```
	fn is_session_state(&self, saved: &PlaybackState) -> bool {
		let same_track = self
			.current
			.as_ref()
			.is_some_and(|current| current.item.video_id == saved.item.video_id);
		let same_text = self
			.channels
			.is_some_and(|channels| channels.text == saved.text_channel_id);
		same_track && same_text
	}

	/// 再生しない復元の曲をキューの先頭へ戻し、`playback_state` を消す
	/// (Idle で聞き手が 0 人のとき、復元の接続に失敗したとき)。
	///
	/// - 行が読み込んだときと同じときだけ行う (`store::requeue_saved`)。変わっていればログに残すだけ
	/// - 失敗は 1 回やり直してからログに残す
	///
	/// # 引数
	/// - `saved`: 読み込んだときの再生状態
	///
	/// # 戻り値
	/// 戻して消したら `Ok(true)`。行が無い、または変わっていたら `Ok(false)`
	///
	/// # エラー
	/// 書き込みの失敗 (やり直しても失敗したとき)
	///
	/// # 呼び出し例
	/// ```ignore
	/// let _ = self.requeue_saved_state(saved).await;
	/// ```
	fn requeue_saved_state(
		&self,
		saved: PlaybackState,
	) -> impl Future<Output = Result<bool, DbError>> + Send + 'static {
		let guild = self.guild;
		let write = self.write(move |c| store::requeue_saved(c, &saved));
		async move {
			let result = write.await;
			match &result {
				Ok(true) => tracing::info!(
					guild = guild.get(),
					"復元しない曲をキューの先頭へ戻し、再生状態を消しました"
				),
				Ok(false) => tracing::info!(
					guild = guild.get(),
					"読み込んだ後に再生状態が変わったため、キューへ戻しません"
				),
				Err(error) => {
					tracing::error!(guild = guild.get(), %error, "復元しない曲をキューへ戻せません");
				}
			}
			result
		}
	}

	/// 復元の接続に失敗した (チャンネルが消えた、権限が無いなど)。キューを残して退出する。
	///
	/// - 保存されていた曲をキューの先頭へ戻し、`playback_state` を消す (1 トランザクション、`requeue_saved_state`)。
	///   書き込みと切断は並行して行う
	/// - キューは消さない。接続を待つ間に `?p` で入った曲も残る
	/// - 保存されていたテキストチャンネルへ、`?p` での再開を案内する。書き込みに失敗したら `error.save_failed` を送る
	///
	/// # 引数
	/// - `saved`: 復元しようとした再生状態
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_restore_connect_failed(saved).await;
	/// ```
	pub(super) async fn on_restore_connect_failed(&mut self, saved: PlaybackState) {
		let channels = self.channels;
		// 書き込みと切断を並行して行う
		let requeue = self.requeue_saved_state(saved);
		let result = self.leave_while(requeue).await;
		let Some(channels) = channels else {
			return;
		};
		let notice = self.resume_notice(keys::RESTORE_FAILED, &result);
		self.notify(channels.text, notice);
	}

	/// 復元の接続ができた。保存されていた曲を今の曲にし、解決を別タスクで始める。
	///
	/// - 解決が終わると、保存した位置から再生する (`PlayRequest::start_at`。バックエンドが開いた後にシークする)
	/// - 一時停止していた曲は、一時停止のまま載せる (`pause_on_start` を立て、`play_stream` が `PlayRequest::paused` にする)
	/// - 解決の失敗は通常の曲と同じく、エラーコードごとにやり直すか飛ばして次の曲へ進む
	///
	/// # 引数
	/// - `saved`: 保存されていた再生状態
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.start_restored(saved);
	/// ```
	pub(super) fn start_restored(&mut self, saved: PlaybackState) {
		let position = Duration::from_millis(saved.position_ms);
		let video_id = saved.item.video_id.clone();
		self.set_current(Some(saved.item));
		if let Some(current) = self.current.as_mut() {
			current.start_at = position;
			current.clock.set(position);
			current.pause_on_start = saved.paused;
			current.restored = true;
		}
		self.state = PlayerState::Preparing;
		self.spawn_resolve(video_id);
	}

	/// 再生を始めた曲を通知する (曲ごとに最初の再生で 1 回)。
	///
	/// - 通常の曲は再生中のカード付きの通知 、復元の曲は復元の通知。一時停止に戻した復元の曲は、`?resume` の案内を添える
	///
	/// # 引数
	/// - `item`: 再生を始めた曲
	/// - `waiting`: 通知を作る時点のキューの曲数 (再生中のカードに載せる)
	/// - `restored`: 復元の曲か
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.announce_start(&item, waiting, restored);
	/// ```
	pub(super) fn announce_start(&self, item: &NewQueueItem, waiting: u32, restored: bool) {
		let Some(channels) = self.channels else {
			return;
		};
		let title = item.title.as_str();
		let notice = match (restored, self.state) {
			(false, _) => {
				let mut notice = titled(keys::NOW_PLAYING, title);
				notice.card = Some(Card::NowPlaying(TrackCard {
					guild: self.guild,
					video_id: item.video_id.clone(),
					title: title.to_owned(),
					duration_s: item.duration_s,
					requester: item.requester_id,
					waiting,
				}));
				notice
			}
			(true, PlayerState::Paused) => with_args(
				keys::RESTORED_PAUSED,
				[
					(keys::ARG_PREFIX, self.deps.config.bot.prefix.clone()),
					(keys::ARG_TITLE, escape_markdown(title)),
				],
			),
			(true, _) => titled_text(keys::RESTORED, title),
		};
		self.notify(channels.text, notice);
	}

	/// 復元しない曲をキューの先頭へ戻す (セッション中に、今のセッションの曲でない復元が届いたとき)。
	///
	/// - `playback_state` は今のセッションのものなので消さない
	/// - 失敗は 1 回やり直してからログに残す
	/// - 準備済みの次の曲が次の曲でなくなるため、先読みを見直す
	///
	/// # 引数
	/// - `item`: 復元しない曲
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.requeue_restored(saved.item).await;
	/// ```
	async fn requeue_restored(&mut self, item: NewQueueItem) {
		let guild = self.guild;
		if let Err(error) = self
			.write(move |c| queue::push_front(c, guild, &item))
			.await
		{
			tracing::error!(guild = guild.get(), %error, "復元しない曲をキューへ戻せません");
		}
		self.kick_prefetch().await;
	}

	/// 次の曲を取り出せなかったとき、終わった曲の `playback_state` を消す。
	///
	/// - 残すと、次の起動で終わった曲を再生し直してしまう。キューは残す
	/// - 失敗は 1 回やり直してからログに残す
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.discard_saved_state().await;
	/// ```
	pub(super) async fn discard_saved_state(&mut self) {
		let guild = self.guild;
		if let Err(error) = self.write(move |c| playback::delete(c, guild)).await {
			tracing::error!(guild = guild.get(), %error, "終わった曲の再生状態を消せません");
		}
	}

	/// 終了の要求を受ける (終了順の 2 番目)。
	///
	/// - 起動中の処理を止め、今の曲の位置と一時停止の状態を保存し、切断してから応答する
	/// - `playback_state` は消さない (次の起動で復元する)
	/// - 音声の呼び出しはどれも上限時間で打ち切るため (切断は `VoiceLimits::disconnect`)、応答は遅れても止まらない
	///
	/// # 引数
	/// - `done`: 応答口
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_shutdown(done).await;
	/// ```
	pub(super) async fn on_shutdown(&mut self, done: oneshot::Sender<()>) {
		self.tasks.abort_all();
		if self.current.is_some() {
			self.save_current_position().await;
		}
		if let Some(session) = self.voice.take() {
			self.disconnect_session(session).await;
		}
		self.retire();
		// 待つ側が諦めて応答口を捨てていても問題ない
		let _ = done.send(());
	}
}
