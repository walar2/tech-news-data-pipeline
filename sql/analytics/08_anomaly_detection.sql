-- Query 8 — Anomaly Detection
-- Identifies days in the past 30 days where the number of stories/articles
-- is more than 2 standard deviations above the mean.
-- Uses window functions: LAG, AVG OVER, STDDEV_SAMP OVER.

WITH daily_story_counts AS (
    SELECT
        post_date,
        COUNT(*) AS story_count
    FROM (
        SELECT
            posted_at::DATE AS post_date
        FROM silver.hackernews_content
        WHERE content_type = 'story'
          AND is_valid = TRUE
          AND posted_at >= CURRENT_DATE - INTERVAL '30 days'

        UNION ALL

        SELECT
            published_at::DATE AS post_date
        FROM silver.devto_article
        WHERE is_valid = TRUE
          AND published_at >= CURRENT_DATE - INTERVAL '30 days'
    ) combined_posts
    GROUP BY post_date
),
scored_days AS (
    SELECT
        post_date,
        story_count,
        LAG(story_count) OVER (ORDER BY post_date) AS previous_day_story_count,
        ROUND(AVG(story_count) OVER ()::NUMERIC, 2) AS mean_story_count_30d,
        ROUND(STDDEV_SAMP(story_count) OVER ()::NUMERIC, 2) AS stddev_story_count_30d
    FROM daily_story_counts
)
SELECT
    post_date,
    story_count,
    previous_day_story_count,
    story_count - COALESCE(previous_day_story_count, 0) AS day_over_day_change,
    mean_story_count_30d,
    stddev_story_count_30d,
    ROUND(
        (story_count - mean_story_count_30d)
        / NULLIF(stddev_story_count_30d, 0),
        2
    ) AS z_score
FROM scored_days
WHERE story_count > mean_story_count_30d + (2 * stddev_story_count_30d)
ORDER BY z_score DESC,
         post_date DESC;