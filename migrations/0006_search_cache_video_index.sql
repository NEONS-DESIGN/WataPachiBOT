-- 使えなくなった動画を指す検索結果の削除 (DELETE ... WHERE video_id) が表全体を走査しないようにする。
CREATE INDEX search_cache_video ON search_cache (video_id);
