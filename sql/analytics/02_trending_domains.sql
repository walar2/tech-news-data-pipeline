-- Query 2 — Trending Domains
-- Domains that appear most often in HackerNews over the past 7 days,
-- ranked by share count and total score received.

WITH hackernews_domains AS (
    SELECT
        LOWER(
            REGEXP_REPLACE(
                SPLIT_PART(
                    REGEXP_REPLACE(COALESCE(url, ''), '^https?://', '', 'i'),
                    '/',
                    1
                ),
                '^www\.',
                '',
                'i'
            )
        ) AS domain,
        COALESCE(score, 0) AS score
    FROM silver.hackernews_content
    WHERE content_type = 'story'
      AND posted_at >= NOW() - INTERVAL '7 days'
      AND is_valid = TRUE
      AND url IS NOT NULL
      AND url <> ''
)
SELECT
    domain,
    COUNT(*) AS share_count,
    SUM(score) AS total_score,
    ROUND(AVG(score)::NUMERIC, 2) AS avg_score
FROM hackernews_domains
WHERE domain <> ''
GROUP BY domain
ORDER BY share_count DESC,
         total_score DESC
LIMIT 20;