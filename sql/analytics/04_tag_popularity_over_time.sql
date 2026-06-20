-- Query 4 — Tag Popularity Over Time
-- Top 10 Dev.to tags by total reactions, broken down by week over the past 30 days.
-- Result is pivoted by week.

WITH exploded_tags AS (
    SELECT
        LOWER(TRIM(tag)) AS tag_name,
        DATE_TRUNC('week', published_at)::DATE AS week_start,
        COALESCE(reactions_count, 0) AS reactions_count
    FROM silver.devto_article
    CROSS JOIN LATERAL UNNEST(tags) AS tag
    WHERE published_at >= NOW() - INTERVAL '30 days'
      AND is_valid = TRUE
      AND tags IS NOT NULL
),
top_tags AS (
    SELECT
        tag_name,
        SUM(reactions_count) AS total_reactions
    FROM exploded_tags
    GROUP BY tag_name
    ORDER BY total_reactions DESC
    LIMIT 10
)
SELECT
    et.tag_name,
    SUM(et.reactions_count) AS total_reactions_30d,
    COALESCE(
        SUM(et.reactions_count) FILTER (
            WHERE et.week_start = DATE_TRUNC('week', NOW())::DATE
        ),
        0
    ) AS current_week_reactions,
    COALESCE(
        SUM(et.reactions_count) FILTER (
            WHERE et.week_start = (DATE_TRUNC('week', NOW()) - INTERVAL '1 week')::DATE
        ),
        0
    ) AS previous_week_reactions,
    COALESCE(
        SUM(et.reactions_count) FILTER (
            WHERE et.week_start = (DATE_TRUNC('week', NOW()) - INTERVAL '2 weeks')::DATE
        ),
        0
    ) AS two_weeks_ago_reactions,
    COALESCE(
        SUM(et.reactions_count) FILTER (
            WHERE et.week_start = (DATE_TRUNC('week', NOW()) - INTERVAL '3 weeks')::DATE
        ),
        0
    ) AS three_weeks_ago_reactions,
    COALESCE(
        SUM(et.reactions_count) FILTER (
            WHERE et.week_start = (DATE_TRUNC('week', NOW()) - INTERVAL '4 weeks')::DATE
        ),
        0
    ) AS four_weeks_ago_reactions
FROM exploded_tags et
JOIN top_tags tt
    ON et.tag_name = tt.tag_name
GROUP BY et.tag_name
ORDER BY total_reactions_30d DESC;