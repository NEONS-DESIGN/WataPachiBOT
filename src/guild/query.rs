//! `?p` の引数の分類。
//! - URL なら YouTube の動画かプレイリストかを見分ける。他のサイトや知らない形の URL は受け付けない
//! - `watch?v=X&list=Y` は動画 X だけを再生する (プレイリストとして扱わない)
//! - URL でない文字列は検索語として扱う。囲みと空白を除いて何も残らなければ `Empty` (検索しない)
//! - 呼び出し: `match query::classify(text) { Query::Video(id) => ..., ... }`

use url::Url;

use crate::domain::VideoId;

/// 動画とプレイリストの URL を受け付ける YouTube のホスト (小文字)。
const YOUTUBE_HOSTS: &[&str] = &[
	"youtube.com",
	"www.youtube.com",
	"m.youtube.com",
	"music.youtube.com",
];
/// 短縮 URL のホスト。パスの先頭が動画 ID。
const SHORT_HOST: &str = "youtu.be";
/// URL として扱う接頭辞 (小文字で比べる)。
const URL_SCHEMES: &[&str] = &["http://", "https://"];
/// 接頭辞の無い URL に補う接頭辞。
const DEFAULT_SCHEME: &str = "https://";
/// 動画 ID の長さ。
const VIDEO_ID_LEN: usize = 11;
/// プレイリストの URL を組み立て直すときの基底。キャッシュの鍵を揃えるため、ホストを 1 つに寄せる。
const PLAYLIST_BASE: &str = "https://www.youtube.com/playlist";
/// 動画 ID を持つクエリ引数の名前。
const VIDEO_PARAM: &str = "v";
/// プレイリスト ID を持つクエリ引数の名前。
const LIST_PARAM: &str = "list";
/// 動画のパス。
const WATCH_PATH: &str = "/watch";
/// プレイリストのパス。
const PLAYLIST_PATH: &str = "/playlist";
/// ショート動画のパスの接頭辞。後ろが動画 ID。
const SHORTS_PREFIX: &str = "/shorts/";

/// `?p` の引数の種類。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
	/// 動画 1 件。
	Video(VideoId),
	/// プレイリスト。`https://www.youtube.com/playlist?list=...` に組み立て直した URL を持つ。
	Playlist(String),
	/// 検索語 (前後の空白を除いたもの)。
	Search(String),
	/// 受け付けない URL。
	Unsupported,
	/// 囲みと空白を除くと何も残らない引数 (`<>`、`< >` など)。検索しない。
	Empty,
}

/// `?p` の引数を分類する。
///
/// - 前後の空白と、Discord で埋め込みを抑える `<...>` の囲みを外し、囲みの内側の前後の空白も除いてから見る
/// - `http://` か `https://` で始まるもの、または空白を含まず YouTube のホストと `/` で始まるものを URL とみなす
///
/// # 引数
/// - `text`: `?p` の後ろの文字列
///
/// # 戻り値
/// 分類の結果。URL として解釈できない、または対応外の形なら `Unsupported`。何も残らなければ `Empty`
///
/// # 呼び出し例
/// ```
/// use watapachi_bot::guild::query::{classify, Query};
/// assert_eq!(classify("好きな曲"), Query::Search("好きな曲".to_string()));
/// assert_eq!(classify("< >"), Query::Empty);
/// ```
pub fn classify(text: &str) -> Query {
	let trimmed = text.trim();
	let text = trimmed
		.strip_prefix('<')
		.and_then(|t| t.strip_suffix('>'))
		.map_or(trimmed, str::trim);
	if text.is_empty() {
		return Query::Empty;
	}
	let lower = text.to_ascii_lowercase();
	if URL_SCHEMES.iter().any(|scheme| lower.starts_with(scheme)) {
		return Url::parse(text).map_or(Query::Unsupported, |url| classify_url(&url));
	}
	if !text.contains(char::is_whitespace) && starts_with_known_host(&lower) {
		return Url::parse(&format!("{DEFAULT_SCHEME}{text}"))
			.map_or(Query::Unsupported, |url| classify_url(&url));
	}
	Query::Search(text.to_owned())
}

/// 接頭辞の無い文字列が、既知のホストと `/` で始まるか。
///
/// # 引数
/// - `lower`: 小文字にした文字列
///
/// # 戻り値
/// 始まるなら true
fn starts_with_known_host(lower: &str) -> bool {
	YOUTUBE_HOSTS
		.iter()
		.chain(std::iter::once(&SHORT_HOST))
		.any(|host| {
			lower
				.strip_prefix(host)
				.is_some_and(|rest| rest.starts_with('/'))
		})
}

/// 解釈済みの URL を分類する。
///
/// # 引数
/// - `url`: http または https の URL
///
/// # 戻り値
/// 動画、プレイリスト、または `Unsupported`
fn classify_url(url: &Url) -> Query {
	let Some(host) = url.host_str().map(str::to_ascii_lowercase) else {
		return Query::Unsupported;
	};
	if host == SHORT_HOST {
		let id = url.path_segments().and_then(|mut s| s.next()).unwrap_or("");
		return video(id);
	}
	if !YOUTUBE_HOSTS.contains(&host.as_str()) {
		return Query::Unsupported;
	}
	let path = url.path().trim_end_matches('/');
	if path == WATCH_PATH {
		return param(url, VIDEO_PARAM).map_or(Query::Unsupported, |id| video(&id));
	}
	if path == PLAYLIST_PATH {
		return match param(url, LIST_PARAM) {
			Some(list) if is_id_text(&list) => {
				Query::Playlist(format!("{PLAYLIST_BASE}?{LIST_PARAM}={list}"))
			}
			_ => Query::Unsupported,
		};
	}
	if let Some(id) = path.strip_prefix(SHORTS_PREFIX) {
		return video(id);
	}
	Query::Unsupported
}

/// クエリ引数の最初の値を返す。
///
/// # 引数
/// - `url`: 対象の URL
/// - `name`: 引数の名前
///
/// # 戻り値
/// 値。無ければ None
fn param(url: &Url, name: &str) -> Option<String> {
	url.query_pairs()
		.find(|(key, _)| key == name)
		.map(|(_, value)| value.into_owned())
}

/// 動画 ID として正しければ `Video`、正しくなければ `Unsupported` を返す。
///
/// # 引数
/// - `id`: 動画 ID の候補
///
/// # 戻り値
/// 分類の結果
fn video(id: &str) -> Query {
	if id.len() == VIDEO_ID_LEN && is_id_text(id) {
		Query::Video(VideoId(id.to_owned()))
	} else {
		Query::Unsupported
	}
}

/// YouTube の ID に使われる文字 (英数字、`-`、`_`) だけでできた、空でない文字列か。
///
/// # 引数
/// - `text`: 調べる文字列
///
/// # 戻り値
/// 条件を満たせば true
fn is_id_text(text: &str) -> bool {
	!text.is_empty()
		&& text
			.bytes()
			.all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[cfg(test)]
mod tests {
	use super::*;

	const ID: &str = "dQw4w9WgXcQ";

	fn video_of(id: &str) -> Query {
		Query::Video(VideoId(id.to_string()))
	}

	#[test]
	fn watch_urls_on_every_host_are_videos() {
		for host in YOUTUBE_HOSTS {
			let text = format!("https://{host}/watch?v={ID}");
			assert_eq!(classify(&text), video_of(ID), "{text}");
		}
		assert_eq!(
			classify(&format!("http://www.youtube.com/watch?v={ID}&t=30s")),
			video_of(ID)
		);
	}

	#[test]
	fn short_and_shorts_urls_are_videos() {
		assert_eq!(classify(&format!("https://youtu.be/{ID}")), video_of(ID));
		assert_eq!(
			classify(&format!("https://youtu.be/{ID}?si=abc")),
			video_of(ID)
		);
		assert_eq!(
			classify(&format!("https://www.youtube.com/shorts/{ID}")),
			video_of(ID)
		);
	}

	#[test]
	fn playlist_url_is_rebuilt_on_one_host() {
		let expected =
			Query::Playlist("https://www.youtube.com/playlist?list=PLabc_-1".to_string());
		assert_eq!(
			classify("https://music.youtube.com/playlist?list=PLabc_-1&si=x"),
			expected
		);
		assert_eq!(
			classify("https://www.youtube.com/playlist?list=PLabc_-1"),
			expected
		);
	}

	#[test]
	fn watch_with_list_plays_only_the_video() {
		assert_eq!(
			classify(&format!(
				"https://www.youtube.com/watch?v={ID}&list=PLabc&index=3"
			)),
			video_of(ID)
		);
	}

	#[test]
	fn other_urls_are_unsupported() {
		for text in [
			"https://example.com/watch?v=dQw4w9WgXcQ",
			"https://www.youtube.com/@channel",
			"https://www.youtube.com/watch",
			"https://www.youtube.com/watch?v=short",
			"https://www.youtube.com/playlist",
			"https://www.youtube.com/playlist?list=bad%20id",
			"https://youtu.be/",
			"https://notyoutube.com/watch?v=dQw4w9WgXcQ",
			"https://",
		] {
			assert_eq!(classify(text), Query::Unsupported, "{text}");
		}
	}

	#[test]
	fn plain_text_is_search() {
		assert_eq!(
			classify("  Artist: Song  "),
			Query::Search("Artist: Song".to_string())
		);
		assert_eq!(
			classify("youtube.com の曲"),
			Query::Search("youtube.com の曲".to_string())
		);
	}

	#[test]
	fn empty_text_after_brackets_is_not_searched() {
		for text in ["<>", "< >", "  <  >  ", "", "   "] {
			assert_eq!(classify(text), Query::Empty, "{text:?}");
		}
		assert_eq!(classify("< 曲名 >"), Query::Search("曲名".to_string()));
	}

	#[test]
	fn url_forms_without_scheme_or_in_brackets_are_accepted() {
		assert_eq!(classify(&format!("<https://youtu.be/{ID}>")), video_of(ID));
		assert_eq!(
			classify(&format!("www.youtube.com/watch?v={ID}")),
			video_of(ID)
		);
		assert_eq!(classify(&format!("youtu.be/{ID}")), video_of(ID));
		assert_eq!(
			classify(&format!("HTTPS://WWW.YOUTUBE.COM/watch?v={ID}")),
			video_of(ID)
		);
	}
}
