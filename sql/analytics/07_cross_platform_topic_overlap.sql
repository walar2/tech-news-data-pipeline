-- Query 7 — Cross-Platform Topic Overlap
-- Finds keywords extracted from story/article titles that appear on both
-- HackerNews and Dev.to in the same week.
-- Uses SQL tokenisation and a JOIN-based overlap approach.

WITH stop_words AS (
    SELECT UNNEST(ARRAY[
        'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
        'has', 'have', 'how', 'in', 'is', 'it', 'its', 'new', 'of', 'on',
        'or', 'that', 'the', 'this', 'to', 'was', 'what', 'when', 'with',
        'you', 'your', 'using', 'use', 'into', 'over', 'under', 'after',
        'before', 'about', 'more', 'less', 'not', 'can', 'will'
    ]) AS word
),
hackernews_keywords AS (
    SELECT
        DATE_TRUNC('week', hn.posted_at)::DATE AS week_start,
        LOWER(token) AS keyword,
        COUNT(*) AS hackernews_mentions
    FROM silver.hackernews_content hn
    CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(
        REGEXP_REPLACE(COALESCE(hn.title, ''), '[^A-Za-z0-9]+', ' ', 'g'),
        '\s+'
    ) AS token
    LEFT JOIN stop_words sw
        ON LOWER(token) = sw.word
    WHERE hn.content_type = 'story'
      AND hn.is_valid = TRUE
      AND hn.posted_at >= NOW() - INTERVAL '30 days'
      AND LENGTH(token) >= 3
      AND sw.word IS NULL
    GROUP BY week_start, LOWER(token)
),
devto_keywords AS (
    SELECT
        DATE_TRUNC('week', da.published_at)::DATE AS week_start,
        LOWER(token) AS keyword,
        COUNT(*) AS devto_mentions
    FROM silver.devto_article da
    CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(
        REGEXP_REPLACE(COALESCE(da.title, ''), '[^A-Za-z0-9]+', ' ', 'g'),
        '\s+'
    ) AS token
    LEFT JOIN stop_words sw
        ON LOWER(token) = sw.word
    WHERE da.is_valid = TRUE
      AND da.published_at >= NOW() - INTERVAL '30 days'
      AND LENGTH(token) >= 3
      AND sw.word IS NULL
    GROUP BY week_start, LOWER(token)
)
SELECT
    hn.week_start,
    hn.keyword,
    hn.hackernews_mentions,
    da.devto_mentions,
    hn.hackernews_mentions + da.devto_mentions AS combined_mentions
FROM hackernews_keywords hn
JOIN devto_keywords da
    ON hn.week_start = da.week_start
   AND hn.keyword = da.keyword
ORDER BY
    hn.week_start DESC,
    combined_mentions DESC,
    hn.keyword
LIMIT 100;