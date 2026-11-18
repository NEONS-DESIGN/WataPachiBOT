-- 履歴の一覧 (ORDER BY played_at DESC, id DESC) を索引の逆順の走査だけで返す。
-- 降順の索引 (guild_id, played_at DESC) では id の並びが合わず、一覧のたびに一時的な並べ替えが起きていた。
-- 昇順の索引は行の id (rowid) の昇順で並ぶため、逆順に読めば両方の降順を満たす。
DROP INDEX IF EXISTS history_guild_time;
CREATE INDEX history_guild_played ON history (guild_id, played_at);
