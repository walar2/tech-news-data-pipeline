CREATE DATABASE tech_news;

\connect tech_news

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;


-- =========================================================
-- BRONZE: SOURCE-SPECIFIC RAW API RESPONSES
-- =========================================================

CREATE TABLE bronze.hackernews_payload (
    payload_key BIGSERIAL PRIMARY KEY,
    endpoint TEXT NOT NULL,
    source_item_id TEXT,
    batch_id UUID NOT NULL,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_bronze_hackernews_payload
ON bronze.hackernews_payload (
    endpoint,
    COALESCE(source_item_id, ''),
    batch_id
);


CREATE TABLE bronze.devto_payload (
    payload_key BIGSERIAL PRIMARY KEY,
    endpoint TEXT NOT NULL,
    source_item_id TEXT,
    batch_id UUID NOT NULL,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_bronze_devto_payload
ON bronze.devto_payload (
    endpoint,
    COALESCE(source_item_id, ''),
    batch_id
);


-- =========================================================
-- SILVER: SOURCE-SPECIFIC CLEANED TABLES
-- =========================================================

CREATE TABLE silver.hackernews_content (
    source_item_id TEXT PRIMARY KEY,
    content_type TEXT NOT NULL
        CHECK (content_type IN ('story', 'comment', 'job')),

    parent_item_id TEXT,
    author_username TEXT,

    title TEXT,
    text TEXT,
    url TEXT,

    posted_at TIMESTAMPTZ,

    score INTEGER NOT NULL DEFAULT 0,
    comments_count INTEGER NOT NULL DEFAULT 0,

    is_valid BOOLEAN NOT NULL,
    validation_errors TEXT[] NOT NULL DEFAULT '{}',

    batch_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE silver.hackernews_user_profile (
    username TEXT PRIMARY KEY,
    about TEXT,
    karma INTEGER,
    account_created_at TIMESTAMPTZ,

    is_valid BOOLEAN NOT NULL,
    validation_errors TEXT[] NOT NULL DEFAULT '{}',

    batch_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE silver.devto_article (
    source_item_id TEXT PRIMARY KEY,

    author_username TEXT,
    title TEXT,
    description TEXT,
    url TEXT,

    published_at TIMESTAMPTZ,

    comments_count INTEGER NOT NULL DEFAULT 0,
    reactions_count INTEGER NOT NULL DEFAULT 0,
    reading_time INTEGER,

    tags TEXT[] NOT NULL DEFAULT '{}',

    is_valid BOOLEAN NOT NULL,
    validation_errors TEXT[] NOT NULL DEFAULT '{}',

    batch_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- =========================================================
-- GOLD: SHARED STAR SCHEMA
-- =========================================================

CREATE TABLE gold.dim_date (
    date_key INTEGER PRIMARY KEY,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    weekday SMALLINT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);


CREATE TABLE gold.dim_time (
    hour_key SMALLINT PRIMARY KEY,
    hour SMALLINT NOT NULL UNIQUE
        CHECK (hour BETWEEN 0 AND 23),
    time_of_day TEXT NOT NULL
        CHECK (time_of_day IN ('morning', 'afternoon', 'evening', 'night'))
);


CREATE TABLE gold.dim_source (
    source_key SMALLSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL UNIQUE
        CHECK (source_name IN ('HackerNews', 'Dev.to'))
);


CREATE TABLE gold.dim_author (
    author_key BIGSERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    source TEXT NOT NULL
        CHECK (source IN ('HackerNews', 'Dev.to')),
    first_seen_at TIMESTAMPTZ NOT NULL,

    valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    UNIQUE (username, source, valid_from)
);

CREATE UNIQUE INDEX uq_dim_author_current
ON gold.dim_author (username, source)
WHERE is_current;


CREATE TABLE gold.dim_domain (
    domain_key BIGSERIAL PRIMARY KEY,
    domain TEXT NOT NULL UNIQUE
);


CREATE TABLE gold.dim_tag (
    tag_key BIGSERIAL PRIMARY KEY,
    tag_name TEXT NOT NULL UNIQUE
);


CREATE TABLE gold.fact_story (
    story_key BIGSERIAL PRIMARY KEY,

    source_item_id TEXT NOT NULL,

    source_key SMALLINT NOT NULL
        REFERENCES gold.dim_source(source_key),

    author_key BIGINT NOT NULL
        REFERENCES gold.dim_author(author_key),

    domain_key BIGINT NOT NULL
        REFERENCES gold.dim_domain(domain_key),

    date_key INTEGER NOT NULL
        REFERENCES gold.dim_date(date_key),

    hour_key SMALLINT NOT NULL
        REFERENCES gold.dim_time(hour_key),

    score INTEGER NOT NULL DEFAULT 0,
    comments_count INTEGER NOT NULL DEFAULT 0,
    reactions_count INTEGER NOT NULL DEFAULT 0,
    reading_time INTEGER,

    UNIQUE (source_key, source_item_id)
);


CREATE TABLE gold.fact_story_snapshot (
    snapshot_key BIGSERIAL PRIMARY KEY,

    story_key BIGINT NOT NULL
        REFERENCES gold.fact_story(story_key),

    snapshot_at TIMESTAMPTZ NOT NULL,
    score INTEGER NOT NULL DEFAULT 0,
    comments_count INTEGER NOT NULL DEFAULT 0,

    UNIQUE (story_key, snapshot_at)
);


CREATE TABLE gold.bridge_story_tag (
    story_key BIGINT NOT NULL
        REFERENCES gold.fact_story(story_key),

    tag_key BIGINT NOT NULL
        REFERENCES gold.dim_tag(tag_key),

    PRIMARY KEY (story_key, tag_key)
);


-- =========================================================
-- AUDIT
-- =========================================================

CREATE TABLE audit.pipeline_run (
    run_key BIGSERIAL PRIMARY KEY,
    dag_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    batch_id UUID,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    records_processed INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,

    UNIQUE (dag_id, run_id)
);


-- =========================================================
-- SEED DIMENSIONS
-- =========================================================

INSERT INTO gold.dim_source (source_name)
VALUES
    ('HackerNews'),
    ('Dev.to');


INSERT INTO gold.dim_time (
    hour_key,
    hour,
    time_of_day
)
SELECT
    hour_value,
    hour_value,
    CASE
        WHEN hour_value BETWEEN 5 AND 11 THEN 'morning'
        WHEN hour_value BETWEEN 12 AND 16 THEN 'afternoon'
        WHEN hour_value BETWEEN 17 AND 20 THEN 'evening'
        ELSE 'night'
    END
FROM generate_series(0, 23) AS hour_value;


INSERT INTO gold.dim_date (
    date_key,
    year,
    quarter,
    month,
    day,
    weekday,
    is_weekend
)
SELECT
    TO_CHAR(date_value, 'YYYYMMDD')::INTEGER,
    EXTRACT(YEAR FROM date_value)::SMALLINT,
    EXTRACT(QUARTER FROM date_value)::SMALLINT,
    EXTRACT(MONTH FROM date_value)::SMALLINT,
    EXTRACT(DAY FROM date_value)::SMALLINT,
    EXTRACT(ISODOW FROM date_value)::SMALLINT,
    EXTRACT(ISODOW FROM date_value) IN (6, 7)
FROM generate_series(
    '2020-01-01'::DATE,
    '2035-12-31'::DATE,
    INTERVAL '1 day'
) AS date_value;