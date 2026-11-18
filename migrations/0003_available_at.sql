-- 解決結果のキャッシュに、URL の利用開始時刻 (UNIX 秒) を持たせる。
-- YouTube は再生前の広告の秒数が過ぎるまで URL を 403 で拒む。NULL は待たなくてよい (既存の行を含む)。
ALTER TABLE resolve_cache ADD COLUMN available_at INTEGER;
