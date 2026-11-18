-- プレイリストのキャッシュに、取得時の件数の上限を持たせる。
-- 保存した件数が上限に満たなければ全件を取得済み、上限と同じなら続きがあるかもしれない。
-- 既存の行は NULL (不明) で、再利用せず取り直す。
ALTER TABLE playlist_cache ADD COLUMN fetch_limit INTEGER;
