//! GuildActor の操作コマンドと、聞き手が居ないときの自動退出。
//! - `actor.rs` の子のモジュール。アクターの状態を直接扱う
//! - 応答はコマンドを送ったテキストチャンネル (`CommandContext::text_channel`) へ送る
//! - 再生が要るコマンド (?pause ?resume ?skip ?replay ?pnow) は、再生中でなければエラーを通知して何も変えない
//! - 番号 (?pnow ?clear) は今の曲を含まないキューの 1 始まりで、`?qlist` の番号と同じ。範囲外はエラーを通知する
//! - 準備済みの次の曲がキューの変更で次の曲でなくなったら、`kick_prefetch` がバックエンドから捨てる
//! - 再生を変えるコマンド (?pause ?resume ?skip ?replay ?pnow ?clear ?sh ?vol ?loop ?leave) は、
//!   Bot のセッションがある間 (Idle 以外。接続中、回復中を含む) は、実行者がセッションのボイスチャンネルに
//!   居るときだけ受け付ける。Idle では確かめない
//! - 回復中 (Recovering) は ?p (キューへの追加のみ)、?leave、?rejoin (回復中の通知) 以外を断る
//! - `?rejoin` は `supervise.rs` の回復の手順を回数の制限なしで実行する
//! - 音声の呼び出しは `call_voice` で行う (時間切れは切断として回復に入る)
//! - `?pause` と `?resume` は、成功したら位置と一時停止の状態をすぐ保存する
//! - 呼び出し: `self.on_command(command, context).await`

use std::time::Duration;

use tokio::time::Instant;

use super::supervise::Trigger;
use super::GuildActor;
use crate::db::{playback, settings};
use crate::domain::ChannelId;
use crate::guild::actor::check_access;
use crate::guild::message::{ClearRange, CommandContext, GuildCommand};
use crate::guild::notices::{denial_notice, keys, plain, titled, with_args};
use crate::guild::state::{PlayerState, SessionChannels};
use crate::guild::store::{self, QueueEdit};

/// `?vol` で受け付ける百分率の下限。
const VOLUME_PERCENT_MIN: u16 = 1;
/// `?vol` で受け付ける百分率の上限。
const VOLUME_PERCENT_MAX: u16 = 200;
/// 百分率を音量 (1.0 が等倍) に直すときに割る数。
const PERCENT_PER_UNIT: f32 = 100.0;

/// Bot と同じボイスチャンネルに居る人だけが使えるコマンドか。
///
/// # 引数
/// - `command`: コマンド
///
/// # 戻り値
/// 同じボイスチャンネルを求めるなら true。`?p`、`?move`、`?rejoin` は false
///
/// # 呼び出し例
/// ```ignore
/// if needs_same_channel(&command) { ... }
/// ```
fn needs_same_channel(command: &GuildCommand) -> bool {
	matches!(
		command,
		GuildCommand::Pause
			| GuildCommand::Resume
			| GuildCommand::Skip
			| GuildCommand::Replay
			| GuildCommand::PlayNow { .. }
			| GuildCommand::Clear { .. }
			| GuildCommand::Shuffle
			| GuildCommand::SetVolume { .. }
			| GuildCommand::ToggleLoop
			| GuildCommand::Leave
	)
}

/// 回復中に断るコマンドか。`?p` (キューへの追加のみ)、`?leave`、`?rejoin` (回復の処理が応答する) 以外を断る。
///
/// # 引数
/// - `command`: コマンド
///
/// # 戻り値
/// 断るなら true
///
/// # 呼び出し例
/// ```ignore
/// if self.state == PlayerState::Recovering && blocked_while_recovering(&command) { ... }
/// ```
fn blocked_while_recovering(command: &GuildCommand) -> bool {
	!matches!(
		command,
		GuildCommand::Play { .. } | GuildCommand::Leave | GuildCommand::Rejoin
	)
}

/// `?vol` の百分率を音量 (1.0 が等倍) に直す。1〜200 は 0.01〜2.0 になる。
///
/// # 引数
/// - `percent`: 百分率 (範囲の確認は呼び出し側で済ませる)
///
/// # 戻り値
/// 音量
///
/// # 呼び出し例
/// ```ignore
/// let volume = volume_from_percent(150); // 1.5
/// ```
fn volume_from_percent(percent: u16) -> f32 {
	f32::from(percent) / PERCENT_PER_UNIT
}

impl GuildActor {
	/// コマンドを種類ごとの処理へ振り分ける。
	///
	/// - 回復中に断るコマンドは、回復中の通知を送って何もしない
	/// - 同じボイスチャンネルを求めるコマンドは、Idle 以外で実行者がセッションのボイスチャンネルに居なければ、
	///   エラーを通知して何もしない
	///
	/// # 引数
	/// - `command`: 利用者のコマンド
	/// - `context`: コマンドの発行元。応答は `context.text_channel` へ送る
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_command(command, context).await;
	/// ```
	pub(super) async fn on_command(&mut self, command: GuildCommand, context: CommandContext) {
		let reply = context.text_channel;
		if self.state == PlayerState::Recovering && blocked_while_recovering(&command) {
			self.notify(reply, plain(keys::RECOVERING));
			return;
		}
		if needs_same_channel(&command) && !self.in_bot_channel(context.voice_channel) {
			self.notify(reply, plain(keys::NOT_SAME_CHANNEL));
			return;
		}
		match command {
			GuildCommand::Play { query } => self.on_play(&query, context).await,
			GuildCommand::Pause => self.on_pause(reply).await,
			GuildCommand::Resume => self.on_resume(reply).await,
			GuildCommand::Replay => self.on_replay(reply).await,
			GuildCommand::Skip => self.on_skip(reply).await,
			GuildCommand::PlayNow { index } => self.on_play_now(index, reply).await,
			GuildCommand::Clear { range } => self.on_clear(range, reply).await,
			GuildCommand::ToggleLoop => self.on_toggle_loop(reply).await,
			GuildCommand::Shuffle => self.on_shuffle(reply).await,
			GuildCommand::SetVolume { percent } => self.on_set_volume(percent, reply).await,
			GuildCommand::Move => self.on_move(context.voice_channel, reply).await,
			GuildCommand::Leave => self.on_leave(reply).await,
			GuildCommand::Rejoin => self.begin_recovery(Trigger::Manual(reply)).await,
		}
	}

	/// 実行者が Bot のセッションのボイスチャンネルに居るか。
	///
	/// - Idle 以外ならセッションのボイスチャンネルと比べる。音声セッションの無い接続中 (Connecting) と
	///   回復中 (Recovering) も比べる (外の人が退出や飛ばしを通せないように)
	/// - Idle (セッションが無い) では確かめずに true
	///
	/// # 引数
	/// - `invoker`: 実行者のボイスチャンネル
	///
	/// # 戻り値
	/// 同じチャンネルに居る、または Idle なら true
	///
	/// # 呼び出し例
	/// ```ignore
	/// if !self.in_bot_channel(context.voice_channel) { ... }
	/// ```
	fn in_bot_channel(&self, invoker: Option<ChannelId>) -> bool {
		match (self.state, self.channels) {
			(PlayerState::Idle, _) | (_, None) => true,
			(_, Some(channels)) => invoker == Some(channels.voice),
		}
	}

	/// `?pause`: 再生中の曲を一時停止する。
	///
	/// - Playing のときだけ止める。Paused なら「一時停止中」、それ以外は「再生中の曲が無い」を通知する
	/// - 止めたら、一時停止に戻す予定を消し (一時停止できたため)、位置と一時停止の状態をすぐ保存する
	/// - 音声の呼び出しに失敗したら通知し、状態は変えない
	///
	/// # 引数
	/// - `reply`: 応答先
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_pause(reply).await;
	/// ```
	async fn on_pause(&mut self, reply: ChannelId) {
		match self.state {
			PlayerState::Playing => {}
			PlayerState::Paused => {
				self.notify(reply, plain(keys::ALREADY_PAUSED));
				return;
			}
			_ => {
				self.notify(reply, plain(keys::NOTHING_PLAYING));
				return;
			}
		}
		match self.call_voice(|voice| voice.pause()).await {
			Ok(()) => {
				self.state = PlayerState::Paused;
				if let Some(current) = self.current.as_mut() {
					current.clock.halt(Instant::now());
					current.pause_on_start = false;
				}
				self.notify(reply, plain(keys::PAUSED));
				self.save_current_position().await;
			}
			Err(error) => {
				tracing::warn!(guild = self.guild.get(), %error, "一時停止できません");
				self.notify(reply, plain(keys::AUDIO_FAILED));
			}
		}
	}

	/// `?resume`: 一時停止中の曲を再開する。
	///
	/// - Paused のときだけ再開する。Playing なら「一時停止していない」、それ以外は「再生中の曲が無い」を通知する
	/// - 再開したら、一時停止に戻す予定を消し (利用者が再開を選んだため)、位置と一時停止の状態をすぐ保存する
	/// - 音声の呼び出しに失敗したら通知し、状態は変えない
	///
	/// # 引数
	/// - `reply`: 応答先
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_resume(reply).await;
	/// ```
	async fn on_resume(&mut self, reply: ChannelId) {
		match self.state {
			PlayerState::Paused => {}
			PlayerState::Playing => {
				self.notify(reply, plain(keys::NOT_PAUSED));
				return;
			}
			_ => {
				self.notify(reply, plain(keys::NOTHING_PLAYING));
				return;
			}
		}
		match self.call_voice(|voice| voice.resume()).await {
			Ok(()) => {
				self.state = PlayerState::Playing;
				if let Some(current) = self.current.as_mut() {
					current.clock.run(Instant::now());
					current.pause_on_start = false;
				}
				self.notify(reply, plain(keys::RESUMED));
				self.save_current_position().await;
			}
			Err(error) => {
				tracing::warn!(guild = self.guild.get(), %error, "再開できません");
				self.notify(reply, plain(keys::AUDIO_FAILED));
			}
		}
	}

	/// `?replay`: 今の曲を止め、解決し直して新しい曲として先頭から再生する。
	///
	/// - 再生を始めた曲 (Playing / Paused) だけが対象。解決中の曲はもともと先頭から始まるため対象外
	/// - 世代を進めるので、止めた曲の終了のイベントや古い解決結果は捨てられる
	/// - 再生できなければ、通常の再生の失敗と同じく曲を飛ばして次の曲へ進む
	/// - `playback_state` の位置を先頭・一時停止なしに書き直す (`start_track` は書かないため。
	///   書かないと、次の定期保存の前に再起動したとき前の位置や一時停止の状態で復元される)
	///
	/// # 引数
	/// - `reply`: 応答先
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_replay(reply).await;
	/// ```
	async fn on_replay(&mut self, reply: ChannelId) {
		let started = matches!(self.state, PlayerState::Playing | PlayerState::Paused);
		let item = self
			.current
			.as_ref()
			.filter(|_| started)
			.map(|current| current.item.clone());
		let Some(item) = item else {
			self.notify(reply, plain(keys::NOTHING_PLAYING));
			return;
		};
		self.stop_playback().await;
		self.persist_position(Duration::ZERO, false).await;
		let next = self.start_track(item).await;
		self.advance_if(next).await;
	}

	/// `?skip`: 今の曲を飛ばして次の曲を始める。
	///
	/// - 今の曲が無ければエラーを通知する。解決中の曲も飛ばせる
	/// - 飛ばしたことは先に通知する (音声の停止を待たせない。停止の失敗はログに残すだけで、通知は変わらない)
	/// - 再生を始めていた曲は止める。世代を進めるので、解決中の結果が後から届いても再生しない
	/// - 飛ばした曲は終わった曲と同じく記録し、次の曲の取り出しと 1 回の書き込みで行う
	///   (再生を始めていれば履歴へ、ループ中はキューの末尾へ)
	///
	/// # 引数
	/// - `reply`: 応答先
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_skip(reply).await;
	/// ```
	async fn on_skip(&mut self, reply: ChannelId) {
		let Some(skipped) = self.current.take() else {
			self.notify(reply, plain(keys::NOTHING_PLAYING));
			return;
		};
		self.notify(reply, titled(keys::SKIPPED, &skipped.item.title));
		if !skipped.is_preparing() {
			self.stop_playback().await;
		}
		self.set_current(None);
		self.finish_and_start_next(skipped).await;
	}

	/// `?pnow <番号>`: キューの指定の曲を今すぐ再生し、今の曲を次の曲に回す。
	///
	/// - 今の曲が無ければエラーを通知する。キューが空、番号が範囲外ならエラーを通知して何も変えない
	/// - 曲の入れ替えと `playback_state` の書き込みは 1 トランザクション
	/// - 次の曲に回した曲は、再び回ってきたときに先頭から再生する
	///
	/// # 引数
	/// - `index`: キューの番号 (1 始まり)
	/// - `reply`: 応答先
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_play_now(2, reply).await;
	/// ```
	async fn on_play_now(&mut self, index: u32, reply: ChannelId) {
		let (Some(current), Some(channels)) = (self.current.as_ref(), self.channels) else {
			self.notify(reply, plain(keys::NOTHING_PLAYING));
			return;
		};
		let started = !current.is_preparing();
		let current_item = current.item.clone();
		let guild = self.guild;
		let edit = self
			.write(move |c| store::play_now(c, guild, index, &current_item, channels))
			.await;
		let chosen = match edit {
			Ok(QueueEdit::Done(item)) => item,
			Ok(other) => {
				self.notify_rejected_edit(&other, reply, keys::INDEX_OUT_OF_RANGE);
				return;
			}
			Err(error) => {
				tracing::error!(guild = guild.get(), %error, "?pnow の曲を入れ替えられません");
				self.notify(reply, plain(keys::SAVE_FAILED));
				return;
			}
		};
		if started {
			self.stop_playback().await;
		}
		let next = self.start_track(chosen).await;
		self.advance_if(next).await;
	}

	/// `?clear`: キューの曲を消す (全件、先頭 N 件、範囲)。今の曲は消さない。
	///
	/// - キューが空、番号が範囲外ならエラーを通知して何も消さない
	/// - 消した後に先読みを見直す (準備済みの曲が次の曲でなくなれば捨てる)
	///
	/// # 引数
	/// - `range`: 消す範囲
	/// - `reply`: 応答先
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_clear(ClearRange::All, reply).await;
	/// ```
	async fn on_clear(&mut self, range: ClearRange, reply: ChannelId) {
		let guild = self.guild;
		match self.write(move |c| store::clear(c, guild, range)).await {
			Ok(QueueEdit::Done(removed)) => {
				self.notify(
					reply,
					with_args(keys::CLEARED, [(keys::ARG_COUNT, removed.to_string())]),
				);
				self.kick_prefetch().await;
			}
			Ok(other) => self.notify_rejected_edit(&other, reply, keys::CLEAR_OUT_OF_RANGE),
			Err(error) => {
				tracing::error!(guild = guild.get(), %error, "キューを消せません");
				self.notify(reply, plain(keys::SAVE_FAILED));
			}
		}
	}

	/// `?sh`: キューをシャッフルする。今の曲は動かさない。
	///
	/// - キューが空ならエラーを通知する
	/// - 並べ替えた後に先読みを見直す (準備済みの曲が次の曲でなくなれば捨てる)
	///
	/// # 引数
	/// - `reply`: 応答先
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_shuffle(reply).await;
	/// ```
	async fn on_shuffle(&mut self, reply: ChannelId) {
		let guild = self.guild;
		match self.write(move |c| store::shuffle(c, guild)).await {
			Ok(QueueEdit::Done(_)) => {
				self.notify(reply, plain(keys::SHUFFLED));
				self.kick_prefetch().await;
			}
			Ok(other) => self.notify_rejected_edit(&other, reply, keys::INDEX_OUT_OF_RANGE),
			Err(error) => {
				tracing::error!(guild = guild.get(), %error, "キューをシャッフルできません");
				self.notify(reply, plain(keys::SAVE_FAILED));
			}
		}
	}

	/// キューの操作を断った理由 (空、範囲外) を通知する。
	///
	/// # 引数
	/// - `edit`: 操作の結果 (`QueueEdit::Done` 以外)
	/// - `reply`: 応答先
	/// - `out_of_range_key`: 範囲外のときの通知のキー (`?pnow` と `?clear` で文言が違う)
	///
	/// # 呼び出し例
	/// ```ignore
	/// Ok(other) => self.notify_rejected_edit(&other, reply, keys::INDEX_OUT_OF_RANGE),
	/// ```
	fn notify_rejected_edit<T>(
		&self,
		edit: &QueueEdit<T>,
		reply: ChannelId,
		out_of_range_key: &'static str,
	) {
		match edit {
			QueueEdit::Empty => self.notify(reply, plain(keys::QUEUE_EMPTY)),
			QueueEdit::OutOfRange { count } => self.notify(
				reply,
				with_args(out_of_range_key, [(keys::ARG_COUNT, count.to_string())]),
			),
			QueueEdit::Done(_) => {}
		}
	}

	/// `?loop`: キューのループを切り替え、ギルドの設定に保存する。
	///
	/// - 次に曲が終わったとき (飛ばしたときを含む) から効く
	/// - 保存に失敗したらエラーを通知し、切り替えない
	///
	/// # 引数
	/// - `reply`: 応答先
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_toggle_loop(reply).await;
	/// ```
	async fn on_toggle_loop(&mut self, reply: ChannelId) {
		let enabled = !self.settings().await.loop_enabled;
		let guild = self.guild;
		if let Err(error) = self
			.write(move |c| settings::set_loop(c, guild, enabled))
			.await
		{
			tracing::error!(guild = guild.get(), %error, "ループの設定を保存できません");
			self.notify(reply, plain(keys::SAVE_FAILED));
			return;
		}
		if let Some(cached) = self.settings.as_mut() {
			cached.loop_enabled = enabled;
		}
		let key = if enabled {
			keys::LOOP_ON
		} else {
			keys::LOOP_OFF
		};
		self.notify(reply, plain(key));
	}

	/// `?vol <1-200>`: 音量をギルドの設定に保存し、接続中なら今の音声にも反映する。
	///
	/// - 範囲外ならエラーを通知して何も変えない
	/// - 保存に失敗したらエラーを通知し、反映もしない
	/// - 保存できたら、今の音声への反映より先に通知する (反映を待たせない)
	/// - 今の音声への反映の失敗はログに残すだけ (保存済みなので次の曲から効く。通知は変わらない)
	///
	/// # 引数
	/// - `percent`: 百分率
	/// - `reply`: 応答先
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_set_volume(150, reply).await;
	/// ```
	async fn on_set_volume(&mut self, percent: u16, reply: ChannelId) {
		if !(VOLUME_PERCENT_MIN..=VOLUME_PERCENT_MAX).contains(&percent) {
			self.notify(
				reply,
				with_args(
					keys::VOLUME_OUT_OF_RANGE,
					[
						(keys::ARG_MIN, VOLUME_PERCENT_MIN.to_string()),
						(keys::ARG_MAX, VOLUME_PERCENT_MAX.to_string()),
					],
				),
			);
			return;
		}
		let volume = volume_from_percent(percent);
		let guild = self.guild;
		if let Err(error) = self
			.write(move |c| settings::set_volume(c, guild, volume))
			.await
		{
			tracing::error!(guild = guild.get(), %error, "音量の設定を保存できません");
			self.notify(reply, plain(keys::SAVE_FAILED));
			return;
		}
		if let Some(cached) = self.settings.as_mut() {
			cached.volume = volume;
		}
		self.notify(
			reply,
			with_args(keys::VOLUME, [(keys::ARG_PERCENT, percent.to_string())]),
		);
		if self.voice.is_some() {
			if let Err(error) = self.call_voice(|voice| voice.set_volume(volume)).await {
				tracing::warn!(guild = guild.get(), %error, "今の音声に音量を反映できません");
			}
		}
	}

	/// `?move`: 実行者のボイスチャンネルへ移動する。再生は止めない。
	///
	/// - Bot が接続していなければ、実行者がボイスチャンネルに居なければ、同じチャンネルなら、エラーを通知する
	/// - 移動の前に移動先の権限と満員を確かめ (`check_access`)、断るなら理由を通知して移動しない
	/// - 移動できたら、以後の `playback_state` の宛先を移動先にし、今の `playback_state` も書き換える
	/// - 移動できたら自動退出の計時を取り消す (移動先の聞き手の人数は、改めて届く人数の変化で判断する)
	/// - 移動できたら健全性の判定の基準と回数を捨てる (移動先で送信の暗号化が始め直されるため)。
	///   再生を始めた曲なら、猶予の起点を移動した時刻にする (曲の開始のイベントは改めて届かないため)。
	///   開始が届く前の曲は、開始が届いたときに起点が決まる
	/// - `playback_state` の書き換えの失敗はログに残すだけ (移動は済んでいる)
	///
	/// # 引数
	/// - `target`: 実行者のボイスチャンネル
	/// - `reply`: 応答先
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_move(context.voice_channel, reply).await;
	/// ```
	async fn on_move(&mut self, target: Option<ChannelId>, reply: ChannelId) {
		let Some(channels) = self.channels.filter(|_| self.voice.is_some()) else {
			self.notify(reply, plain(keys::NOT_CONNECTED));
			return;
		};
		let Some(target) = target else {
			self.notify(reply, plain(keys::NOT_IN_VOICE));
			return;
		};
		if channels.voice == target {
			self.notify(reply, plain(keys::SAME_CHANNEL));
			return;
		}
		let access_limit = self.voice_limits().access();
		let access = self.deps.access.clone();
		if let Some(denial) = check_access(access.as_ref(), access_limit, self.guild, target).await
		{
			self.notify(reply, denial_notice(&denial, target));
			return;
		}
		// バックエンドの参加の上限 (`voice.join_timeout_secs`) より長く待つ
		let limit = self.voice_limits().move_to();
		if let Err(error) = self
			.call_voice_within(limit, |voice| voice.move_to(target))
			.await
		{
			tracing::warn!(guild = self.guild.get(), %error, "ボイスチャンネルを移動できません");
			self.notify(reply, plain(keys::AUDIO_FAILED));
			return;
		}
		self.channels = Some(SessionChannels {
			voice: target,
			..channels
		});
		self.alone = None;
		self.sup.judge.reset();
		if self.sup.started_at.is_some() {
			self.sup.started_at = Some(Instant::now());
		}
		let guild = self.guild;
		if let Err(error) = self
			.write(move |c| playback::set_voice_channel(c, guild, target))
			.await
		{
			tracing::error!(guild = guild.get(), %error, "移動先を再生状態に保存できません");
		}
		self.notify(
			reply,
			with_args(
				keys::MOVED,
				[(keys::ARG_CHANNEL, format!("<#{}>", target.get()))],
			),
		);
	}

	/// `?leave`: 退出し、キューと `playback_state` を消す。
	///
	/// - Idle ならエラーを通知する
	/// - 解決や問い合わせ、回復の再接続の途中でも、その結果で再生を始めない (起動中の処理を止め、番号で捨てる)
	/// - キューと `playback_state` を消せなければ、成功の代わりに保存の失敗を通知する
	///
	/// # 引数
	/// - `reply`: 応答先
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_leave(reply).await;
	/// ```
	async fn on_leave(&mut self, reply: ChannelId) {
		if self.state == PlayerState::Idle {
			self.notify(reply, plain(keys::NOT_CONNECTED));
			return;
		}
		let key = match self.leave_and_clear().await {
			Ok(()) => keys::LEFT,
			Err(_) => keys::SAVE_FAILED,
		};
		self.notify(reply, plain(key));
	}

	/// ボイスチャンネルの聞き手の人数の変化を受け、自動退出のタイマーを始める、または止める。
	///
	/// - 0 人になったら、ギルドの `alone_timeout_secs` (未設定なら既定値) の後に退出する時刻を決める。
	///   0 秒の設定なら退出しない。すでに数えていれば数え直さない
	/// - 1 人以上に戻ったら取り消す
	/// - Idle では何もしない
	///
	/// # 引数
	/// - `listeners`: Bot 以外の聞き手の人数
	///
	/// # 呼び出し例
	/// ```ignore
	/// self.on_voice_members(listeners).await;
	/// ```
	pub(super) async fn on_voice_members(&mut self, listeners: u32) {
		self.listeners = listeners;
		if listeners > 0 {
			if self.alone.take().is_some() {
				tracing::debug!(
					guild = self.guild.get(),
					"聞き手が戻ったため自動退出を取り消します"
				);
			}
			return;
		}
		if self.state == PlayerState::Idle || self.alone.is_some() {
			return;
		}
		let secs = self.settings().await.alone_timeout_secs;
		if secs == 0 {
			return;
		}
		self.alone = Some(Instant::now() + Duration::from_secs(u64::from(secs)));
	}

	/// 設定の変更の通知を受け、キャッシュした設定を捨てる。
	///
	/// - キャッシュが無い、または Idle なら捨てるだけで読み直さない (次に使うときに読む)
	/// - セッション中でキャッシュがあれば読み直し、自動退出の秒数が変わっていれば、
	///   一人きりのタイマーを今の聴者数で決め直す
	///
	/// # 呼び出し例
	/// ```ignore
	/// GuildMessage::SettingsChanged => self.on_settings_changed().await,
	/// ```
	pub(super) async fn on_settings_changed(&mut self) {
		let Some(cached) = self.settings.take() else {
			return;
		};
		if self.state == PlayerState::Idle {
			return;
		}
		let after = self.settings().await.alone_timeout_secs;
		if cached.alone_timeout_secs != after {
			self.alone = None;
			self.on_voice_members(self.listeners).await;
		}
	}

	/// 自動退出の時刻になった。退出し、キューと `playback_state` を消して通知する。
	///
	/// - 消せなければ、成功の代わりに保存の失敗を通知する
	///
	/// # 呼び出し例
	/// ```ignore
	/// () = wait_deadline(alone) => self.on_alone_timeout().await,
	/// ```
	pub(super) async fn on_alone_timeout(&mut self) {
		self.alone = None;
		let Some(channels) = self.channels else {
			return;
		};
		tracing::info!(guild = self.guild.get(), "聞き手が居ないため自動退出します");
		let key = match self.leave_and_clear().await {
			Ok(()) => keys::AUTO_LEFT,
			Err(_) => keys::SAVE_FAILED,
		};
		self.notify(channels.text, plain(key));
	}
}
