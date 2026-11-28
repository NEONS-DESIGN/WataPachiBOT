//! アクターを通さない、読み取りだけのコマンド (`?help`、`?qlist`、`?history`)。
//! - Discord には触れず、送る内容 (`Reply`) を作って返す
//! - `?qlist` と `?history` はアクターを通さず、DbReader から直接読む。件数と行は 1 回の読み取りで読む
//! - ページは 0 始まり。範囲外は `clamp_page` で丸める

use rusqlite::Connection;
use twilight_model::channel::message::component::Component;
use twilight_model::channel::message::Embed;
use twilight_util::builder::embed::{EmbedBuilder, EmbedFieldBuilder};

use crate::config::{LimitsConfig, UiConfig};
use crate::db::queue::NewQueueItem;
use crate::db::{history, queue, DbError, DbHandles};
use crate::domain::GuildId;
use crate::guild::notifier::Notice;
use crate::messages::Messages;
use crate::ui::embed::{
	notice_embed, track_page, truncate, TrackLine, COLOR_GREEN, FIELD_NAME_LIMIT,
	FIELD_VALUE_LIMIT, MAX_FIELDS, PER_PAGE, TITLE_LIMIT,
};
use crate::ui::pagination::{clamp_page, page_buttons, total_pages, PageKind};

/// ヘルプのページ数。
const HELP_PAGES: u32 = 3;
/// ヘルプのタイトルの key (ページ順)。
const HELP_TITLE_KEYS: [&str; HELP_PAGES as usize] =
	["help.title_1", "help.title_2", "help.title_3"];
/// ヘルプの項目の key (ページ順)。
const HELP_PAGE_KEYS: [&str; HELP_PAGES as usize] = ["help.page_1", "help.page_2", "help.page_3"];

/// 利用者へ返す内容。Embed とボタン。
#[derive(Debug, Clone)]
pub struct Reply {
	pub embed: Embed,
	pub components: Vec<Component>,
}

/// 一覧の 1 ページの組み立てに要る、種類ごとの情報。
struct ListKind {
	kind: PageKind,
	title_key: &'static str,
	empty_key: &'static str,
}

/// `?qlist` の一覧の情報。
const QUEUE_LIST: ListKind = ListKind {
	kind: PageKind::Queue,
	title_key: "ui.queue_title",
	empty_key: "error.queue_empty",
};
/// `?history` の一覧の情報。
const HISTORY_LIST: ListKind = ListKind {
	kind: PageKind::History,
	title_key: "ui.history_title",
	empty_key: "error.history_empty",
};

/// ヘルプの 1 ページを作る。
///
/// - 全 3 ページ。ボタンで前後に移る
/// - 各コマンドを項目 (名前と説明。横に並べない) にする (旧 Bot と同じ)。25 個を超えた分は付けない
/// - 項目の `{prefix}` は `prefix`、`{queue_max}`・`{playlist_max}`・`{autoleave_max}` は上限の設定に置き換える
///
/// # 引数
/// - `messages`: メッセージ表
/// - `ui`: UI の設定 (ヘルプは URL を持たないので今は使わない)
/// - `limits`: 上限の設定 (`?setting` の説明に出す)
/// - `prefix`: コマンドの接頭辞
/// - `page`: 0 始まりのページ (範囲外は最後のページ)
/// - `issued_at`: ボタンの発行時刻 (UNIX 秒)
///
/// # 呼び出し例
/// ```ignore
/// let reply = help_page(&messages, &config.ui, &config.limits, "?", 0, now).await;
/// ```
pub async fn help_page(
	messages: &Messages,
	_ui: &UiConfig,
	limits: &LimitsConfig,
	prefix: &str,
	page: u32,
	issued_at: i64,
) -> Reply {
	let page = clamp_page(page, HELP_PAGES);
	let index = page as usize;
	let title = messages.render(HELP_TITLE_KEYS[index], &[]);
	let queue_max = limits.queue_limit_max.to_string();
	let playlist_max = limits.playlist_limit_max.to_string();
	let autoleave_max = limits.alone_timeout_max_secs.to_string();
	let args = [
		("prefix", prefix),
		("queue_max", queue_max.as_str()),
		("playlist_max", playlist_max.as_str()),
		("autoleave_max", autoleave_max.as_str()),
	];
	let fields = messages.fields(HELP_PAGE_KEYS[index], &args);
	let mut builder = EmbedBuilder::new()
		.title(truncate(&title, TITLE_LIMIT))
		.color(COLOR_GREEN);
	for (name, value) in fields.into_iter().take(MAX_FIELDS) {
		builder = builder.field(EmbedFieldBuilder::new(
			truncate(&name, FIELD_NAME_LIMIT),
			truncate(&value, FIELD_VALUE_LIMIT),
		));
	}
	let embed = builder.build();
	Reply {
		embed,
		components: page_buttons(PageKind::Help, page, HELP_PAGES, issued_at),
	}
}

/// 空の一覧の Reply (説明だけの Embed、ボタンなし)。
///
/// # 引数
/// - `messages`: メッセージ表
/// - `ui`: UI の設定
/// - `prefix`: コマンドの接頭辞 (文面の `{prefix}` を置き換える)
/// - `key`: 文面のキー
fn empty_reply(messages: &Messages, ui: &UiConfig, prefix: &str, key: &'static str) -> Reply {
	let notice = Notice::text(key, Vec::new());
	Reply {
		embed: notice_embed(messages, ui, prefix, &notice, None),
		components: Vec::new(),
	}
}

/// 一覧の件数を数える関数 (`queue::count` など)。
type CountFn = fn(&Connection, GuildId) -> Result<u32, DbError>;
/// 一覧の行を開始位置と件数で読む関数 (`queue::list` など)。
type ListFn<T> = fn(&Connection, GuildId, u32, u32) -> Result<Vec<T>, DbError>;

/// 一覧の 1 ページ分の読み取りの結果。
struct PageRows {
	/// 全件数
	count: u32,
	/// 範囲に丸めたページ (0 始まり)
	page: u32,
	rows: Vec<NewQueueItem>,
}

/// 件数と 1 ページ分の行を、1 回の読み取りで同じ時点の内容から読む。
///
/// - 読み取りのトランザクションの中で件数と行を読む (間に書き込みが入っても食い違わない)
/// - 件数が 0 なら行は読まない
/// - ページは件数に合わせて丸める
///
/// # 引数
/// - `db`: DB のハンドル
/// - `guild`: 対象のギルド
/// - `page`: 0 始まりのページ
/// - `count`: 件数を数える関数
/// - `list`: 開始位置と件数を受けて行を読む関数
/// - `item`: 行から曲を取り出す関数
///
/// # エラー
/// - DB の読み取りの失敗
///
/// # 呼び出し例
/// ```ignore
/// let read = read_page(db, guild, page, queue::count, queue::list, |q| q.item).await?;
/// ```
async fn read_page<T: 'static>(
	db: &DbHandles,
	guild: GuildId,
	page: u32,
	count: CountFn,
	list: ListFn<T>,
	item: fn(T) -> NewQueueItem,
) -> Result<PageRows, DbError> {
	db.reader
		.read(move |c| {
			// 読み取りだけのため、終わりは破棄 (ROLLBACK) のままでよい
			let tx = c.unchecked_transaction()?;
			let count = count(&tx, guild)?;
			if count == 0 {
				return Ok(PageRows {
					count,
					page: 0,
					rows: Vec::new(),
				});
			}
			let page = clamp_page(page, total_pages(count, PER_PAGE));
			let rows = list(&tx, guild, page * PER_PAGE, PER_PAGE)?
				.into_iter()
				.map(item)
				.collect();
			Ok(PageRows { count, page, rows })
		})
		.await
}

/// 件数と 1 ページ分の行から Reply を組み立てる。
///
/// - `lines` の番号は呼び出し側が振る
fn list_reply(
	messages: &Messages,
	ui: &UiConfig,
	list: &ListKind,
	page: u32,
	total: u32,
	lines: &[TrackLine],
	issued_at: i64,
) -> Reply {
	let title = messages.render(
		list.title_key,
		&[
			("page", &(page + 1).to_string()),
			("total", &total.to_string()),
		],
	);
	Reply {
		embed: track_page(title, lines, ui),
		components: page_buttons(list.kind, page, total, issued_at),
	}
}

/// `?qlist` の 1 ページを作る。
///
/// - アクターを通さず DbReader から読む
/// - キューが空なら `error.queue_empty` の 1 ページ (ボタンなし)
///
/// # 引数
/// - `db`: DB のハンドル
/// - `messages`: メッセージ表
/// - `ui`: UI の設定
/// - `prefix`: コマンドの接頭辞 (空の一覧の文面の `{prefix}` を置き換える)
/// - `guild`: 対象のギルド
/// - `page`: 0 始まりのページ (範囲外は丸める)
/// - `issued_at`: ボタンの発行時刻 (UNIX 秒)
///
/// # エラー
/// - DB の読み取りの失敗
///
/// # 呼び出し例
/// ```ignore
/// let reply = queue_page(&db, &messages, &config.ui, "?", guild, 0, now).await?;
/// ```
pub async fn queue_page(
	db: &DbHandles,
	messages: &Messages,
	ui: &UiConfig,
	prefix: &str,
	guild: GuildId,
	page: u32,
	issued_at: i64,
) -> Result<Reply, DbError> {
	let read = read_page(db, guild, page, queue::count, queue::list, |q| q.item).await?;
	Ok(rows_reply(
		messages,
		ui,
		prefix,
		&QUEUE_LIST,
		read,
		issued_at,
	))
}

/// 読んだ 1 ページ分の曲から Reply を組み立てる。件数が 0 なら空の一覧の Reply。
///
/// # 引数
/// - `messages`: メッセージ表
/// - `ui`: UI の設定
/// - `prefix`: コマンドの接頭辞
/// - `list`: 一覧の種類
/// - `read`: 読み取りの結果
/// - `issued_at`: ボタンの発行時刻 (UNIX 秒)
fn rows_reply(
	messages: &Messages,
	ui: &UiConfig,
	prefix: &str,
	list: &ListKind,
	read: PageRows,
	issued_at: i64,
) -> Reply {
	if read.count == 0 {
		return empty_reply(messages, ui, prefix, list.empty_key);
	}
	let total = total_pages(read.count, PER_PAGE);
	let offset = read.page * PER_PAGE;
	let lines: Vec<TrackLine> = read
		.rows
		.into_iter()
		.enumerate()
		.map(|(i, item)| TrackLine {
			number: offset + i as u32 + 1,
			video_id: item.video_id,
			title: item.title,
			duration_s: item.duration_s,
		})
		.collect();
	list_reply(messages, ui, list, read.page, total, &lines, issued_at)
}

/// `?history` の 1 ページを作る (新しい順)。
///
/// - アクターを通さず DbReader から読む
/// - 履歴が空なら `error.history_empty` の 1 ページ (ボタンなし)
///
/// # 引数
/// - `db`: DB のハンドル
/// - `messages`: メッセージ表
/// - `ui`: UI の設定
/// - `prefix`: コマンドの接頭辞 (空の一覧の文面の `{prefix}` を置き換える)
/// - `guild`: 対象のギルド
/// - `page`: 0 始まりのページ (範囲外は丸める)
/// - `issued_at`: ボタンの発行時刻 (UNIX 秒)
///
/// # エラー
/// - DB の読み取りの失敗
///
/// # 呼び出し例
/// ```ignore
/// let reply = history_page(&db, &messages, &config.ui, "?", guild, 0, now).await?;
/// ```
pub async fn history_page(
	db: &DbHandles,
	messages: &Messages,
	ui: &UiConfig,
	prefix: &str,
	guild: GuildId,
	page: u32,
	issued_at: i64,
) -> Result<Reply, DbError> {
	let read = read_page(db, guild, page, history::count, history::list, |e| e.item).await?;
	Ok(rows_reply(
		messages,
		ui,
		prefix,
		&HISTORY_LIST,
		read,
		issued_at,
	))
}
