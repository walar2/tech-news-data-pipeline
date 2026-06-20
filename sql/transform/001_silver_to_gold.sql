INSERT INTO gold.dim_author (
    username,
    source,
    first_seen_at
)
SELECT DISTINCT
    author_username,
    'Dev.to',
    processed_at
FROM silver.devto_article
WHERE is_valid
  AND batch_id = %(batch_id)s
  AND author_username IS NOT NULL
ON CONFLICT (username, source)
WHERE is_current
DO NOTHING;


INSERT INTO gold.dim_domain (domain)
SELECT DISTINCT
    LOWER(
        COALESCE(
            NULLIF(
                SPLIT_PART(
                    REGEXP_REPLACE(url, '^https?://', ''),
                    '/',
                    1
                ),
                ''
            ),
            'unknown'
        )
    )
FROM silver.devto_article
WHERE is_valid
  AND batch_id = %(batch_id)s
  AND url IS NOT NULL
ON CONFLICT (domain)
DO NOTHING;


WITH valid_articles AS (
    SELECT
        article.*,
        LOWER(
            COALESCE(
                NULLIF(
                    SPLIT_PART(
                        REGEXP_REPLACE(article.url, '^https?://', ''),
                        '/',
                        1
                    ),
                    ''
                ),
                'unknown'
            )
        ) AS domain,
        TO_CHAR(article.published_at AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER AS date_key,
        EXTRACT(HOUR FROM article.published_at AT TIME ZONE 'UTC')::SMALLINT AS hour_key
    FROM silver.devto_article article
    WHERE article.is_valid
      AND article.batch_id = %(batch_id)s
),
upserted_stories AS (
    INSERT INTO gold.fact_story (
        source_item_id,
        source_key,
        author_key,
        domain_key,
        date_key,
        hour_key,
        score,
        comments_count,
        reactions_count,
        reading_time
    )
    SELECT
        article.source_item_id,
        source_dim.source_key,
        author_dim.author_key,
        domain_dim.domain_key,
        article.date_key,
        article.hour_key,
        0,
        article.comments_count,
        article.reactions_count,
        article.reading_time
    FROM valid_articles article
    JOIN gold.dim_source source_dim
      ON source_dim.source_name = 'Dev.to'
    JOIN gold.dim_author author_dim
      ON author_dim.username = article.author_username
     AND author_dim.source = 'Dev.to'
     AND author_dim.is_current
    JOIN gold.dim_domain domain_dim
      ON domain_dim.domain = article.domain
    ON CONFLICT (source_key, source_item_id)
    DO UPDATE SET
        author_key = EXCLUDED.author_key,
        domain_key = EXCLUDED.domain_key,
        date_key = EXCLUDED.date_key,
        hour_key = EXCLUDED.hour_key,
        score = EXCLUDED.score,
        comments_count = EXCLUDED.comments_count,
        reactions_count = EXCLUDED.reactions_count,
        reading_time = EXCLUDED.reading_time
    RETURNING
        story_key,
        score,
        comments_count
),
snapshots AS (
    INSERT INTO gold.fact_story_snapshot (
        story_key,
        snapshot_at,
        score,
        comments_count
    )
    SELECT
        story_key,
        DATE_TRUNC('hour', NOW()),
        score,
        comments_count
    FROM upserted_stories
    ON CONFLICT (story_key, snapshot_at)
    DO UPDATE SET
        score = EXCLUDED.score,
        comments_count = EXCLUDED.comments_count
)
INSERT INTO gold.dim_tag (tag_name)
SELECT DISTINCT
    UNNEST(tags)
FROM valid_articles
ON CONFLICT (tag_name)
DO NOTHING;


INSERT INTO gold.bridge_story_tag (
    story_key,
    tag_key
)
SELECT
    fact.story_key,
    tag_dim.tag_key
FROM silver.devto_article article
JOIN gold.dim_source source_dim
  ON source_dim.source_name = 'Dev.to'
JOIN gold.fact_story fact
  ON fact.source_key = source_dim.source_key
 AND fact.source_item_id = article.source_item_id
CROSS JOIN LATERAL UNNEST(article.tags) AS article_tag
JOIN gold.dim_tag tag_dim
  ON tag_dim.tag_name = article_tag
WHERE article.is_valid
  AND article.batch_id = %(batch_id)s
ON CONFLICT
DO NOTHING;