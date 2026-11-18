CREATE TABLE guild_settings (
	guild_id       INTEGER PRIMARY KEY,
	volume         REAL,      -- NULL は config.toml の既定値に従う
	queue_limit    INTEGER,
	playlist_limit INTEGER,
	alone_timeout  INTEGER,
	loop_enabled   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE bot_admins (
	guild_id INTEGER NOT NULL,
	user_id  INTEGER NOT NULL,
	PRIMARY KEY (guild_id, user_id)
);

-- キューの本体。position の小さい順に再生する
CREATE TABLE queue_items (
	id           INTEGER PRIMARY KEY,
	guild_id     INTEGER NOT NULL,
	position     INTEGER NOT NULL,
	video_id     TEXT    NOT NULL,
	title        TEXT    NOT NULL,
	duration_s   INTEGER,
	requester_id INTEGER NOT NULL,
	added_at     INTEGER NOT NULL,
	UNIQUE (guild_id, position)
);

-- 再生中の曲はキューから移してここに持つ
CREATE TABLE playback_state (
	guild_id         INTEGER PRIMARY KEY,
	voice_channel_id INTEGER NOT NULL,
	text_channel_id  INTEGER NOT NULL,
	video_id         TEXT    NOT NULL,
	title            TEXT    NOT NULL,
	duration_s       INTEGER,
	requester_id     INTEGER NOT NULL,
	position_ms      INTEGER NOT NULL DEFAULT 0,
	paused           INTEGER NOT NULL DEFAULT 0,
	updated_at       INTEGER NOT NULL
);

CREATE TABLE history (
	id           INTEGER PRIMARY KEY,
	guild_id     INTEGER NOT NULL,
	video_id     TEXT    NOT NULL,
	title        TEXT    NOT NULL,
	duration_s   INTEGER,
	requester_id INTEGER NOT NULL,
	played_at    INTEGER NOT NULL
);
CREATE INDEX history_guild_time ON history (guild_id, played_at DESC);

-- ストリーム URL のキャッシュ。expires_at を過ぎたものは使わない
CREATE TABLE resolve_cache (
	video_id     TEXT PRIMARY KEY,
	url          TEXT    NOT NULL,
	http_headers TEXT    NOT NULL,  -- JSON
	filesize     INTEGER NOT NULL,
	format_id    TEXT    NOT NULL,
	expires_at   INTEGER NOT NULL
);

CREATE TABLE track_meta (
	video_id   TEXT PRIMARY KEY,
	title      TEXT NOT NULL,
	duration_s INTEGER,
	thumbnail  TEXT,
	fetched_at INTEGER NOT NULL
);

-- query は NFKC 正規化と小文字化をした検索語
CREATE TABLE search_cache (
	query      TEXT PRIMARY KEY,
	video_id   TEXT    NOT NULL,
	fetched_at INTEGER NOT NULL
);

CREATE TABLE playlist_cache (
	url        TEXT PRIMARY KEY,
	title      TEXT,
	fetched_at INTEGER NOT NULL
);

CREATE TABLE playlist_items (
	url        TEXT    NOT NULL,
	idx        INTEGER NOT NULL,
	video_id   TEXT    NOT NULL,
	title      TEXT    NOT NULL,
	duration_s INTEGER,
	PRIMARY KEY (url, idx)
) WITHOUT ROWID;
