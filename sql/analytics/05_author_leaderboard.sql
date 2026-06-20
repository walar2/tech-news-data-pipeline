-- Query 5 — Author Leaderboard
-- Top 20 authors by engagement score across both platforms.
-- Engagement score = SUM(score + 2 * comments).
-- HackerNews uses score.
-- Dev.to uses reactions_count as the score equivalent.

WITH platform_author_engagement AS (
    SELECT
        'HackerNews' AS platform,
        author_username AS author,
        COALESCE(score, 0) AS score_value,
        COALESCE(comments_count, 0) AS comments_value
    FROM silver.hackernews_content
    WHERE content_type = 'story'
      AND is_valid = TRUE
      AND author_username IS NOT NULL

    UNION ALL

    SELECT
        'Dev.to' AS platform,
        author_username AS author,
        COALESCE(reactions_count, 0) AS score_value,
        COALESCE(comments_count, 0) AS comments_value
    FROM silver.devto_article
    WHERE is_valid = TRUE
      AND author_username IS NOT NULL
)
SELECT
    author,
    COUNT(*) AS total_posts,
    SUM(score_value) AS total_score_or_reactions,
    SUM(comments_value) AS total_comments,
    SUM(score_value + (2 * comments_value)) AS engagement_score,
    COUNT(*) FILTER (WHERE platform = 'HackerNews') AS hackernews_posts,
    COUNT(*) FILTER (WHERE platform = 'Dev.to') AS devto_posts,
    STRING_AGG(DISTINCT platform, ', ' ORDER BY platform) AS platforms
FROM platform_author_engagement
GROUP BY author
ORDER BY engagement_score DESC,
         total_posts DESC
LIMIT 20;