-- サーバーごとの文面の言語 ('ja' / 'en')。NULL は config.toml の [defaults] lang に従う
ALTER TABLE guild_settings ADD COLUMN lang TEXT;
