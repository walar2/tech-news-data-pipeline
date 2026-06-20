-- Query 3 — Viral Velocity
-- Calculates HackerNews score growth rate for each story in its first 6 hours.
-- Uses gold.fact_story_snapshot as required.
-- HackerNews is used because hackernews_hourly_pipeline runs hourly,
-- while Dev.to runs daily and is not suitable for velocity tracking.

WITH hackernews_story_base AS (
    SELECT
        fs.story_key,
        fs.source_item_id,
        hn.title,
        hn.author_username AS author,
        hn.posted_at
    FROM gold.fact_story fs
    JOIN gold.dim_source ds
        ON fs.source_key = ds.source_key
    JOIN silver.hackernews_content hn
        ON fs.source_item_id = hn.source_item_id
    WHERE ds.source_name ILIKE 'hacker%'
      AND hn.content_type = 'story'
      AND hn.is_valid = TRUE
      AND hn.posted_at IS NOT NULL
),
first_six_hour_snapshots AS (
    SELECT
        hsb.story_key,
        hsb.source_item_id,
        hsb.title,
        hsb.author,
        hsb.posted_at,
        fss.snapshot_at,
        fss.score
    FROM hackernews_story_base hsb
    JOIN gold.fact_story_snapshot fss
        ON hsb.story_key = fss.story_key
    WHERE fss.snapshot_at >= hsb.posted_at
      AND fss.snapshot_at < hsb.posted_at + INTERVAL '2 hours'
),
velocity AS (
    SELECT
        story_key,
        source_item_id,
        title,
        author,
        MIN(snapshot_at) AS first_snapshot_at,
        MAX(snapshot_at) AS latest_snapshot_at,
        (ARRAY_AGG(score ORDER BY snapshot_at ASC))[1] AS first_score,
        (ARRAY_AGG(score ORDER BY snapshot_at DESC))[1] AS latest_score,
        COUNT(*) AS snapshot_count
    FROM first_six_hour_snapshots
    GROUP BY
        story_key,
        source_item_id,
        title,
        author
)
SELECT
    source_item_id,
    title,
    author,
    snapshot_count,
    first_score,
    latest_score,
    latest_score - first_score AS score_growth,
    ROUND(
        (latest_score - first_score)::NUMERIC
        / NULLIF(EXTRACT(EPOCH FROM (latest_snapshot_at - first_snapshot_at)) / 3600, 0),
        2
    ) AS score_growth_per_hour,
    first_snapshot_at,
    latest_snapshot_at
FROM velocity
WHERE latest_snapshot_at > first_snapshot_at
ORDER BY score_growth_per_hour DESC NULLS LAST,
         score_growth DESC
LIMIT 10;