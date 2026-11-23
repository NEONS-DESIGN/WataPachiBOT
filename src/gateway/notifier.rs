//! Discord への通知の送信 (`Notifier` の実装)。
//! - 文面は送り先のチャンネルのサーバーの言語で作る (チャンネルからサーバーはキャッシュで引く)
//! - 通知は `notice_embed` で Embed にして送る。本文に置いた利用者のメンションは Embed の中なので通知音を鳴らさない
//! - カードは、送る時点のキャッシュからリクエストした人の名前とアイコンを引いてフッターに出す
//! - Discord がカードを拒んだら (400 の応答、送る前の検査の失敗)、旧 Bot と同じ簡易表示で送り直す
//! - 再試行は twilight-http に任せる。twilight-http の時間切れは HTTP の要求だけに掛かり、
//!   レート制限の待ちと 429 の後の再送には掛からないため、送信 1 回の全体を `SEND_TIMEOUT` で打ち切る
//! - 失敗は warn のログを残し、None / false を返す

use std::future::IntoFuture;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use twilight_cache_inmemory::DefaultInMemoryCache;
use twilight_http::error::{Error as HttpError, ErrorType};
use twilight_http::response::StatusCode;
use twilight_http::Client;
use twilight_model::channel::message::component::Component;
use twilight_model::channel::message::Embed;

use super::requester::lookup_requester;
use crate::config::UiConfig;
use crate::domain::ChannelId;
use crate::guild::notifier::{MessageRef, Notice, Notifier};
use crate::lang::GuildLangs;
use crate::messages::{MessageCatalog, Messages};
use crate::ui::embed::{card_fallback_embed, notice_embed};

/// Discord への送信 1 回 (レート制限の待ちと再送を含む) を待つ上限。設定項目に無いため固定値にする。
/// - 混雑で 429 が続いても、返信やボタンの差し替えのタスクが溜まり続けないようにする
pub const SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Discord への送信を `SEND_TIMEOUT` で打ち切る。
///
/// # 引数
/// - `request`: twilight-http の要求 (送信から本文の受信まで)
///
/// # 戻り値
/// 上限までに終われば要求の結果。過ぎたら None
///
/// # 呼び出し例
/// ```ignore
/// let Some(result) = within_send_timeout(self.http.create_message(channel)).await else { ... };
/// ```
pub async fn within_send_timeout<T>(request: impl IntoFuture<Output = T>) -> Option<T> {
	tokio::time::timeout(SEND_TIMEOUT, request.into_future())
		.await
		.ok()
}

/// 送信や編集の結果。
enum Delivery {
	/// 届いた。送信なら送ったメッセージの場所を持つ
	Done(Option<MessageRef>),
	/// Discord が中身を拒んだ (400、送る前の検査の失敗)。簡易表示で送り直す価値がある
	Rejected,
	/// それ以外の失敗 (通信、400 以外の 4xx、5xx、応答を読めないなど)
	Failed,
}

/// Discord へ通知を送る `Notifier`。
///
/// 呼び出し方:
/// ```ignore
/// let notifier = DiscordNotifier::new(http, cache, catalog, langs, config.ui.clone(), config.bot.prefix.clone());
/// let sent = notifier.send(channel, notice).await;
/// ```
pub struct DiscordNotifier {
	http: Arc<Client>,
	cache: Arc<DefaultInMemoryCache>,
	catalog: Arc<MessageCatalog>,
	langs: Arc<GuildLangs>,
	ui: UiConfig,
	prefix: String,
}

impl DiscordNotifier {
	/// 送信口を作る。
	///
	/// # 引数
	/// - `http`: Discord の HTTP クライアント
	/// - `cache`: Gateway のキャッシュ (リクエストした人の名前とアイコンを引く)
	/// - `catalog`: 言語ごとのメッセージ表
	/// - `langs`: サーバーごとの言語の表
	/// - `ui`: Embed の URL の雛形
	/// - `prefix`: コマンドの接頭辞 (本文の `{prefix}` を置き換える)
	///
	/// # 呼び出し例
	/// ```ignore
	/// let notifier = DiscordNotifier::new(http, cache, catalog, langs, config.ui.clone(), "?".into());
	/// ```
	pub fn new(
		http: Arc<Client>,
		cache: Arc<DefaultInMemoryCache>,
		catalog: Arc<MessageCatalog>,
		langs: Arc<GuildLangs>,
		ui: UiConfig,
		prefix: String,
	) -> Self {
		Self {
			http,
			cache,
			catalog,
			langs,
			ui,
			prefix,
		}
	}

	/// 送り先のチャンネルのサーバーの言語のメッセージ表。
	///
	/// - チャンネルからサーバーをキャッシュで引く。引けなければ既定の言語
	///
	/// # 引数
	/// - `channel`: 送り先のテキストチャンネル (スレッドも可)
	///
	/// # 戻り値
	/// そのサーバーの言語のメッセージ表
	///
	/// # 呼び出し例
	/// ```ignore
	/// let embed = notice_embed(self.messages_for(channel), &self.ui, &self.prefix, notice, None);
	/// ```
	fn messages_for(&self, channel: ChannelId) -> &Messages {
		let guild = self
			.cache
			.channel(channel)
			.and_then(|channel| channel.guild_id);
		self.catalog.get(self.langs.get(guild))
	}

	/// 通知を Embed にする。カードなら、キャッシュからリクエストした人の表示を引いて渡す。
	fn embed(&self, channel: ChannelId, notice: &Notice) -> Embed {
		let requester = notice.card.as_ref().and_then(|card| {
			let (guild, user) = card.requester();
			lookup_requester(&self.cache, &self.ui, guild, user)
		});
		notice_embed(
			self.messages_for(channel),
			&self.ui,
			&self.prefix,
			notice,
			requester.as_ref(),
		)
	}

	/// カードの通知の簡易表示。カードでなければ None。
	fn fallback(&self, channel: ChannelId, notice: &Notice) -> Option<Embed> {
		notice
			.card
			.as_ref()
			.map(|card| card_fallback_embed(self.messages_for(channel), notice.key, card))
	}

	/// Embed とボタンを 1 通のメッセージとして送る。
	///
	/// # 引数
	/// - `channel`: 送り先のテキストチャンネル
	/// - `embed`: 本文
	/// - `components`: ボタン (空なら付けない)
	///
	/// # 戻り値
	/// 送ったメッセージの場所。失敗したら None (warn のログを残す)
	///
	/// # 呼び出し例
	/// ```ignore
	/// let sent = notifier.send_embed(channel, reply.embed, &reply.components).await;
	/// ```
	pub async fn send_embed(
		&self,
		channel: ChannelId,
		embed: Embed,
		components: &[Component],
	) -> Option<MessageRef> {
		match self.post(channel, embed, components).await {
			Delivery::Done(sent) => sent,
			Delivery::Rejected | Delivery::Failed => None,
		}
	}

	/// 送信済みのメッセージのボタンを差し替える (本文は変えない)。空の並びならボタンを外す。
	///
	/// # 戻り値
	/// 差し替えられたら true。失敗 (削除済みなど) は debug のログだけ残して false
	///
	/// # 呼び出し例
	/// ```ignore
	/// notifier.set_components(target, &disabled).await;
	/// ```
	pub async fn set_components(&self, target: MessageRef, components: &[Component]) -> bool {
		let request = self
			.http
			.update_message(target.channel, target.message)
			.components(Some(components));
		let Some(result) = within_send_timeout(request).await else {
			tracing::debug!(
				channel = target.channel.get(),
				message = target.message.get(),
				"ボタンの差し替えが上限時間を過ぎました"
			);
			return false;
		};
		match result {
			Ok(_) => true,
			Err(error) => {
				tracing::debug!(
					channel = target.channel.get(),
					message = target.message.get(),
					%error,
					"ボタンを差し替えられませんでした"
				);
				false
			}
		}
	}

	/// Embed とボタンを送り、結果を分類する。失敗は warn のログを残す。
	async fn post(&self, channel: ChannelId, embed: Embed, components: &[Component]) -> Delivery {
		let embeds = [embed];
		let mut request = self.http.create_message(channel).embeds(&embeds);
		if !components.is_empty() {
			request = request.components(components);
		}
		let sent = within_send_timeout(async {
			match request.await {
				Ok(response) => Ok(response.model().await),
				Err(error) => Err(error),
			}
		})
		.await;
		let Some(sent) = sent else {
			tracing::warn!(
				channel = channel.get(),
				"メッセージの送信が上限時間を過ぎました"
			);
			return Delivery::Failed;
		};
		let model = match sent {
			Ok(model) => model,
			Err(error) => {
				tracing::warn!(channel = channel.get(), %error, "メッセージを送れませんでした");
				return classify(&error);
			}
		};
		match model {
			Ok(message) => Delivery::Done(Some(MessageRef {
				channel,
				message: message.id,
			})),
			Err(error) => {
				tracing::warn!(channel = channel.get(), %error, "送ったメッセージの応答を読めませんでした");
				Delivery::Failed
			}
		}
	}

	/// 送信済みのメッセージの Embed を書き換え、結果を分類する。失敗は warn のログを残す。
	async fn patch(&self, target: MessageRef, embed: Embed) -> Delivery {
		let embeds = [embed];
		let request = self
			.http
			.update_message(target.channel, target.message)
			.embeds(Some(&embeds));
		let Some(result) = within_send_timeout(request).await else {
			tracing::warn!(
				channel = target.channel.get(),
				message = target.message.get(),
				"メッセージの編集が上限時間を過ぎました"
			);
			return Delivery::Failed;
		};
		match result {
			Ok(_) => Delivery::Done(None),
			Err(error) => {
				tracing::warn!(
					channel = target.channel.get(),
					message = target.message.get(),
					%error,
					"メッセージを編集できませんでした"
				);
				classify(&error)
			}
		}
	}
}

/// Discord の応答の状態が、中身を拒んだもの (簡易表示で送り直す価値がある) か。
///
/// - 400 (Invalid Form Body など、中身の検証のエラー) だけを拒否とみなす。
///   403 (権限なし) や 404 (チャンネルやメッセージが無い) は中身を変えても通らないため、送り直さない
///
/// # 引数
/// - `status`: 応答の状態
///
/// # 戻り値
/// 中身を拒んだものなら true
///
/// # 呼び出し例
/// ```ignore
/// if is_content_rejection(status) { /* 簡易表示で送り直す */ }
/// ```
pub fn is_content_rejection(status: StatusCode) -> bool {
	status == StatusCode::BAD_REQUEST
}

/// HTTP のエラーを、中身を拒まれたか (400、送る前の検査の失敗) で分ける。
fn classify(error: &HttpError) -> Delivery {
	match error.kind() {
		ErrorType::Response { status, .. } if is_content_rejection(*status) => Delivery::Rejected,
		ErrorType::Validation => Delivery::Rejected,
		_ => Delivery::Failed,
	}
}

#[async_trait]
impl Notifier for DiscordNotifier {
	/// 通知を Embed にして送る。カードを拒まれたら簡易表示で送り直す。
	///
	/// # 戻り値
	/// 送ったメッセージの場所。失敗したら None
	///
	/// # 呼び出し例
	/// ```ignore
	/// let sent = notifier.send(channel, notice).await;
	/// ```
	async fn send(&self, channel: ChannelId, notice: Notice) -> Option<MessageRef> {
		match self.post(channel, self.embed(channel, &notice), &[]).await {
			Delivery::Done(sent) => sent,
			Delivery::Rejected => {
				let fallback = self.fallback(channel, &notice)?;
				tracing::warn!(
					channel = channel.get(),
					key = notice.key,
					"カードを拒まれたため簡易表示で送り直します"
				);
				self.send_embed(channel, fallback, &[]).await
			}
			Delivery::Failed => None,
		}
	}

	/// 送信済みのメッセージを通知の Embed に書き換える。カードを拒まれたら簡易表示に書き換える。
	///
	/// # 戻り値
	/// 編集できたら true。失敗したら false
	///
	/// # 呼び出し例
	/// ```ignore
	/// let edited = notifier.edit(target, notice).await;
	/// ```
	async fn edit(&self, target: MessageRef, notice: Notice) -> bool {
		match self
			.patch(target, self.embed(target.channel, &notice))
			.await
		{
			Delivery::Done(_) => true,
			Delivery::Rejected => match self.fallback(target.channel, &notice) {
				Some(fallback) => {
					tracing::warn!(
						key = notice.key,
						"カードを拒まれたため簡易表示に書き換えます"
					);
					matches!(self.patch(target, fallback).await, Delivery::Done(_))
				}
				None => false,
			},
			Delivery::Failed => false,
		}
	}
}
