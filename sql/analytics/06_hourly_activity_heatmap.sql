-- Query 6 — Hourly Activity Heatmap
-- Which UTC hour sees the most posts and highest engagement?
-- Produces data suitable for a 7x24 heatmap: day-of-week x hour.
-- Combines HackerNews and Dev.to.

WITH platform_posts AS (
    SELECT
        'HackerNews' AS platform,
        posted_at AS post_timestamp,
        COALESCE(score, 0) + (2 * COALESCE(comments_count, 0)) AS engagement_score
    FROM silver.hackernews_content
    WHERE content_type = 'story'
      AND is_valid = TRUE

    UNION ALL

    SELECT
        'Dev.to' AS platform,
        published_at AS post_timestamp,
        COALESCE(reactions_count, 0) + (2 * COALESCE(comments_count, 0)) AS engagement_score
    FROM silver.devto_article
    WHERE is_valid = TRUE
),
heatmap_base AS (
    SELECT
        EXTRACT(DOW FROM post_timestamp AT TIME ZONE 'UTC')::INT AS day_of_week_number,
        TO_CHAR(post_timestamp AT TIME ZONE 'UTC', 'Dy') AS day_of_week_name,
        EXTRACT(HOUR FROM post_timestamp AT TIME ZONE 'UTC')::INT AS hour_utc,
        platform,
        engagement_score
    FROM platform_posts
    WHERE post_timestamp >= NOW() - INTERVAL '30 days'
)
SELECT
    day_of_week_number,
    day_of_week_name,
    hour_utc,
    COUNT(*) AS post_count,
    SUM(engagement_score) AS total_engagement,
    ROUND(AVG(engagement_score)::NUMERIC, 2) AS avg_engagement,
    COUNT(*) FILTER (WHERE platform = 'HackerNews') AS hackernews_posts,
    COUNT(*) FILTER (WHERE platform = 'Dev.to') AS devto_posts
FROM heatmap_base
GROUP BY
    day_of_week_number,
    day_of_week_name,
    hour_utc
ORDER BY
    day_of_week_number,
    hour_utc;