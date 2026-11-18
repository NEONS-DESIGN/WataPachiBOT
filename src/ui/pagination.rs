//! ページ送りのボタン。
//! - 状態を持たない。ボタンの custom_id に種類・移動先のページ・発行時刻を入れ、押されたときに読み戻す
//! - ページ番号は 0 始まり

use twilight_model::channel::message::component::{ActionRow, Button, ButtonStyle, Component};

/// custom_id の区切り文字。
const SEPARATOR: char = ':';
/// custom_id の項目数 (種類、スロット、移動先、発行時刻)。
const TOKEN_FIELDS: usize = 4;
/// 先頭へ戻るボタンのスロット。
const SLOT_FIRST: char = 'f';
/// 前のページのボタンのスロット。
const SLOT_PREV: char = 'p';
/// 次のページのボタンのスロット。
const SLOT_NEXT: char = 'n';
/// 最後へ進むボタンのスロット。
const SLOT_LAST: char = 'l';
/// 先頭・最後のボタンを出す最小のページ数。これ未満なら前と次だけで足りる
const JUMP_MIN_PAGES: u32 = 3;
/// 先頭へ戻るボタンのラベル。
const LABEL_FIRST: &str = "❚◀";
/// 前のページのボタンのラベル。
const LABEL_PREV: &str = "◀";
/// 次のページのボタンのラベル。
const LABEL_NEXT: &str = "▶";
/// 最後へ進むボタンのラベル。
const LABEL_LAST: &str = "▶❚";

/// ページ送りの対象。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageKind {
	Help,
	Queue,
	History,
}

impl PageKind {
	/// custom_id に入れる文字列。
	fn as_str(self) -> &'static str {
		match self {
			PageKind::Help => "help",
			PageKind::Queue => "qlist",
			PageKind::History => "history",
		}
	}

	/// custom_id の文字列から種類を戻す。知らない文字列は None。
	fn parse(text: &str) -> Option<PageKind> {
		match text {
			"help" => Some(PageKind::Help),
			"qlist" => Some(PageKind::Queue),
			"history" => Some(PageKind::History),
			_ => None,
		}
	}
}

/// ボタンの custom_id に入れる中身。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageToken {
	pub kind: PageKind,
	/// 押したときに表示するページ (0 始まり)
	pub target: u32,
	/// ボタンの位置 (f/p/n/l)。同じページを指すボタンの custom_id が重ならないようにする
	pub slot: char,
	/// ボタンを付けたメッセージを作った時刻 (UNIX 秒)
	pub issued_at: i64,
}

/// `PageToken` を custom_id の文字列にする。形式は `{kind}:{slot}:{target}:{issued_at}`。
///
/// # 戻り値
/// 100 文字以内の文字列 (Discord の custom_id の上限)。
///
/// # 例
/// ```
/// # use watapachi_bot::ui::pagination::{encode, PageKind, PageToken};
/// let token = PageToken { kind: PageKind::Queue, target: 3, slot: 'n', issued_at: 1 };
/// assert_eq!(encode(&token), "qlist:n:3:1");
/// ```
pub fn encode(token: &PageToken) -> String {
	format!(
		"{}{SEPARATOR}{}{SEPARATOR}{}{SEPARATOR}{}",
		token.kind.as_str(),
		token.slot,
		token.target,
		token.issued_at
	)
}

/// custom_id の文字列を `PageToken` に戻す。
///
/// # 戻り値
/// 形式が違う、種類やスロットが未知、数値でない場合は None (他の機能のボタンかもしれない)。
///
/// # 例
/// ```
/// # use watapachi_bot::ui::pagination::decode;
/// assert!(decode("qlist:n:3:1").is_some());
/// assert!(decode("other").is_none());
/// ```
pub fn decode(custom_id: &str) -> Option<PageToken> {
	let parts: Vec<&str> = custom_id.split(SEPARATOR).collect();
	if parts.len() != TOKEN_FIELDS {
		return None;
	}
	let kind = PageKind::parse(parts[0])?;
	let mut slot_chars = parts[1].chars();
	let slot = slot_chars.next()?;
	if slot_chars.next().is_some()
		|| !matches!(slot, SLOT_FIRST | SLOT_PREV | SLOT_NEXT | SLOT_LAST)
	{
		return None;
	}
	Some(PageToken {
		kind,
		slot,
		target: parts[2].parse().ok()?,
		issued_at: parts[3].parse().ok()?,
	})
}

/// ボタン 1 つを作る。
fn button(
	kind: PageKind,
	slot: char,
	target: u32,
	issued_at: i64,
	label: &str,
	style: ButtonStyle,
	disabled: bool,
) -> Component {
	Component::Button(Button {
		id: None,
		custom_id: Some(encode(&PageToken {
			kind,
			target,
			slot,
			issued_at,
		})),
		disabled,
		emoji: None,
		label: Some(label.to_owned()),
		style,
		url: None,
		sku_id: None,
	})
}

/// 先頭・前・次・最後のボタンを 1 行にして返す。
///
/// - `total_pages` が 1 以下ならボタンなし (空の Vec)
/// - `total_pages` が 3 未満なら先頭と最後は出さない
/// - 押しても動かないボタン (先頭ページの「前」など) は disabled にする
///
/// # 引数
/// - `kind`: 対象
/// - `page`: 今のページ (0 始まり。範囲外なら最後のページとして扱う)
/// - `total_pages`: 全ページ数
/// - `issued_at`: 発行時刻 (UNIX 秒)
///
/// # 戻り値
/// `Reply` の components にそのまま入れられる並び。
///
/// # 例
/// ```
/// # use watapachi_bot::ui::pagination::{page_buttons, PageKind};
/// let components = page_buttons(PageKind::Queue, 0, 5, 1_793_000_000);
/// assert_eq!(components.len(), 1);
/// ```
pub fn page_buttons(kind: PageKind, page: u32, total_pages: u32, issued_at: i64) -> Vec<Component> {
	if total_pages <= 1 {
		return Vec::new();
	}
	let last = total_pages - 1;
	let page = page.min(last);
	let at_start = page == 0;
	let at_end = page == last;
	let make = |slot, target, label, style, disabled| {
		button(kind, slot, target, issued_at, label, style, disabled)
	};
	let mut buttons = Vec::with_capacity(4);
	if total_pages >= JUMP_MIN_PAGES {
		buttons.push(make(
			SLOT_FIRST,
			0,
			LABEL_FIRST,
			ButtonStyle::Primary,
			at_start,
		));
	}
	buttons.push(make(
		SLOT_PREV,
		page.saturating_sub(1),
		LABEL_PREV,
		ButtonStyle::Success,
		at_start,
	));
	buttons.push(make(
		SLOT_NEXT,
		(page + 1).min(last),
		LABEL_NEXT,
		ButtonStyle::Success,
		at_end,
	));
	if total_pages >= JUMP_MIN_PAGES {
		buttons.push(make(
			SLOT_LAST,
			last,
			LABEL_LAST,
			ButtonStyle::Primary,
			at_end,
		));
	}
	vec![Component::ActionRow(ActionRow {
		id: None,
		components: buttons,
	})]
}

/// ボタンの並びの複製を作り、すべてのボタンを押せない (灰色) にする。
///
/// - ラベル、色、custom_id は変えない。行 (ActionRow) の中のボタンだけを対象にする
///
/// # 引数
/// - `components`: メッセージのボタンの並び
///
/// # 戻り値
/// 灰色にした並び
///
/// # 例
/// ```
/// # use watapachi_bot::ui::pagination::{disable_all, page_buttons, PageKind};
/// let disabled = disable_all(&page_buttons(PageKind::Queue, 0, 5, 1));
/// assert_eq!(disabled.len(), 1);
/// ```
pub fn disable_all(components: &[Component]) -> Vec<Component> {
	components
		.iter()
		.cloned()
		.map(|component| match component {
			Component::ActionRow(mut row) => {
				for inner in &mut row.components {
					if let Component::Button(button) = inner {
						button.disabled = true;
					}
				}
				Component::ActionRow(row)
			}
			Component::Button(mut button) => {
				button.disabled = true;
				Component::Button(button)
			}
			other => other,
		})
		.collect()
}

/// ボタンが期限切れか。発行から `timeout_secs` 秒を超えたら true。
///
/// - 時計が戻って `now` が発行時刻より前になっても期限切れにしない
///
/// # 例
/// ```
/// # use watapachi_bot::ui::pagination::{is_expired, PageKind, PageToken};
/// let token = PageToken { kind: PageKind::Help, target: 0, slot: 'f', issued_at: 100 };
/// assert!(is_expired(&token, 800, 600));
/// ```
pub fn is_expired(token: &PageToken, now: i64, timeout_secs: u64) -> bool {
	let elapsed = now.saturating_sub(token.issued_at);
	elapsed > 0 && elapsed as u64 > timeout_secs
}

/// 全ページ数。件数が 0 でも 1 ページとする。`per_page` が 0 なら 1。
///
/// # 例
/// ```
/// # use watapachi_bot::ui::pagination::total_pages;
/// assert_eq!(total_pages(11, 10), 2);
/// ```
pub fn total_pages(count: u32, per_page: u32) -> u32 {
	if per_page == 0 {
		return 1;
	}
	count.div_ceil(per_page).max(1)
}

/// ページ番号を `0..total_pages` に収める。`total_pages` が 0 なら 0。
///
/// # 例
/// ```
/// # use watapachi_bot::ui::pagination::clamp_page;
/// assert_eq!(clamp_page(9, 3), 2);
/// ```
pub fn clamp_page(page: u32, total_pages: u32) -> u32 {
	page.min(total_pages.saturating_sub(1))
}
