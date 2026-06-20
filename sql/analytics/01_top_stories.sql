-- Query 1 — Top Stories
-- Top 20 HackerNews stories with the highest score in the past 24 hours.
-- Output: title, author, score, comments_count, hours_since_posted.

SELECT
    title,
    author_username AS author,
    score,
    comments_count,
    ROUND(
        EXTRACT(EPOCH FROM (NOW() - posted_at)) / 3600,
        2
    ) AS hours_since_posted
FROM silver.hackernews_content
WHERE content_type = 'story'
  AND posted_at >= NOW() - INTERVAL '24 hours'
  AND is_valid = TRUE
ORDER BY score DESC NULLS LAST,
         comments_count DESC NULLS LAST
LIMIT 20;