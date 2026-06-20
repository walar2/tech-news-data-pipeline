# Data Lineage

This document traces how data moves from the external APIs through the warehouse layers and into reporting outputs.

## High-Level Lineage

```text
HackerNews API ── bronze.hackernews_payload ── silver.hackernews_content ───────┐
                                                                               ├── Gold ── data_quality_checks.py ── telegram_daily_report.py
Dev.to API ────── bronze.devto_payload ─────── silver.devto_article ────────────┘

## DAG-Level Lineage

| Stage | DAG / File | Main responsibility | Output |
|---|---|---|---|
| HackerNews ingestion | `hackernews_hourly_pipeline.py` | Fetch top/best stories and story details from HackerNews API | Bronze, Silver, Gold HackerNews records |
| Dev.to ingestion | `devto_daily_pipeline.py` | Fetch Dev.to articles and article details | Bronze, Silver, Gold Dev.to records |
| Data quality and reporting snapshot | `data_quality_checks.py` | Validate freshness, row counts, null rates, operational status, and create daily report tables | Audit tables and `gold.daily_report_*` tables |
| Telegram reporting | `telegram_daily_report.py` | Read report-ready data from `gold.daily_report_*` and send Telegram message/file | Telegram channel message and attached report file |

## HackerNews Lineage

### 1. Source

Data starts from the HackerNews API.

Typical source endpoints:

```text
topstories.json
beststories.json
item/{id}.json
user/{id}.json
```

### 2. Bronze

Raw responses are loaded into:

```text
bronze.hackernews_payload
```

| Column | Lineage role |
|---|---|
| endpoint | Records which API endpoint produced the payload. |
| source_item_id | Stores HackerNews item/user ID when available. |
| batch_id | Connects records to one ingestion batch. |
| payload | Stores the raw JSON response. |
| ingested_at | Stores ingestion timestamp. |

### 3. Silver

Raw item payloads are transformed into:

```text
silver.hackernews_content
```

Raw user payloads are transformed into:

```text
silver.hackernews_user_profile
```

Important transformations:

| Bronze source | Silver output | Transformation |
|---|---|---|
| `payload.id` | `source_item_id` | Converted to text ID. |
| `payload.type` | `content_type` | Identifies story/comment/job/user content type. |
| `payload.by` | `author_username` | Extracts HackerNews author username. |
| `payload.title` | `title` | Extracts story title. |
| `payload.text` | `text` | Extracts text body. |
| `payload.url` | `url` | Extracts URL; if missing for stories, fallback HackerNews item URL can be used. |
| `payload.time` | `posted_at` | Converts Unix timestamp to timestamp with time zone. |
| `payload.score` | `score` | Stores HackerNews score. |
| `payload.descendants` | `comments_count` | Stores story comment count. |
| Validation logic | `is_valid`, `validation_errors` | Records whether the row passed validation. |

### 4. Gold

Validated HackerNews stories are transformed into analytics-ready Gold tables:

```text
gold.fact_story
gold.fact_story_snapshot
gold.dim_author
gold.dim_domain
gold.dim_source
gold.dim_date
gold.dim_time
```

Important mappings:

| Silver source | Gold output | Business meaning |
|---|---|---|
| `silver.hackernews_content.source_item_id` | `gold.fact_story.source_item_id` | Preserves original HackerNews story ID. |
| `author_username` | `gold.dim_author.username` / `gold.fact_story.author_key` | Author dimension relationship. |
| `url` | `gold.dim_domain.domain` / `gold.fact_story.domain_key` | Domain dimension relationship. |
| `posted_at` | `gold.dim_date`, `gold.dim_time` | Date and hour dimensions. |
| `score` | `gold.fact_story.score` | Current story score. |
| `comments_count` | `gold.fact_story.comments_count` | Current story comment count. |
| Current metrics | `gold.fact_story_snapshot` | Snapshot used for viral velocity analytics. |

## Dev.to Lineage

### 1. Source

Data starts from the Dev.to API.

Typical source endpoints:

```text
/articles
/articles/{id}
```

### 2. Bronze

Raw responses are loaded into:

```text
bronze.devto_payload
```

| Column | Lineage role |
|---|---|
| endpoint | Records which Dev.to endpoint produced the payload. |
| source_item_id | Stores Dev.to article ID when available. |
| batch_id | Connects records to one ingestion batch. |
| payload | Stores the raw JSON response. |
| ingested_at | Stores ingestion timestamp. |

### 3. Silver

Raw article payloads are transformed into:

```text
silver.devto_article
```

Important transformations:

| Bronze source | Silver output | Transformation |
|---|---|---|
| `payload.id` | `source_item_id` | Converts Dev.to article ID to text. |
| User fields | `author_username` | Extracts Dev.to author username. |
| `payload.title` | `title` | Extracts article title. |
| `payload.description` | `description` | Extracts article description. |
| `payload.url` | `url` | Extracts article URL. |
| `payload.published_at` | `published_at` | Converts publish timestamp. |
| `payload.comments_count` | `comments_count` | Stores comment count. |
| Reactions fields | `reactions_count` | Stores reaction count; used as score equivalent for analytics. |
| `payload.reading_time_minutes` | `reading_time` | Stores estimated reading time. |
| `payload.tags` / `payload.tag_list` | `tags` | Stores article tags as an array. |
| Validation logic | `is_valid`, `validation_errors` | Records whether the row passed validation. |

### 4. Gold

Validated Dev.to articles are transformed into shared Gold analytics tables:

```text
gold.fact_story
gold.dim_author
gold.dim_domain
gold.dim_source
gold.dim_date
gold.dim_time
gold.dim_tag
gold.bridge_story_tag
```

Important mappings:

| Silver source | Gold output | Business meaning |
|---|---|---|
| `silver.devto_article.source_item_id` | `gold.fact_story.source_item_id` | Preserves original Dev.to article ID. |
| `author_username` | `gold.dim_author.username` / `gold.fact_story.author_key` | Author dimension relationship. |
| `url` | `gold.dim_domain.domain` / `gold.fact_story.domain_key` | Domain dimension relationship. |
| `published_at` | `gold.dim_date`, `gold.dim_time` | Date and hour dimensions. |
| `reactions_count` | `gold.fact_story.reactions_count` | Dev.to reaction metric. |
| `comments_count` | `gold.fact_story.comments_count` | Dev.to comment count. |
| `reading_time` | `gold.fact_story.reading_time` | Reading time metric. |
| `tags` | `gold.dim_tag`, `gold.bridge_story_tag` | Tag analytics relationship. |

## Data Quality Lineage

The `data_quality_checks.py` DAG reads Silver and Gold warehouse outputs and writes quality/audit results.

Inputs:

```text
silver.hackernews_content
silver.devto_article
Airflow run status metadata
```

Outputs:

```text
audit.data_quality_result
audit.daily_pipeline_status
gold.daily_report_top_stories
gold.daily_report_trending_domains
gold.daily_report_top_devto_articles
gold.daily_report_summary_metrics
```

Checks performed:

| Check | Source table | Output |
|---|---|---|
| Freshness check | `silver.hackernews_content.posted_at`, `silver.devto_article.published_at` | `audit.data_quality_result` |
| Row count anomaly check | Silver source tables | `audit.data_quality_result` |
| Null rate check | Required Silver columns | `audit.data_quality_result` |
| Source DAG status check | Airflow metadata / custom check logic | `audit.daily_pipeline_status` |

## Daily Report Snapshot Lineage

`data_quality_checks.py` creates report-ready Gold output tables for `telegram_daily_report.py`.

| Report table | Source | Transformation |
|---|---|---|
| `gold.daily_report_top_stories` | `silver.hackernews_content` | Selects HackerNews stories from the past 24 hours with title, author, score, comments, URL, and posted timestamp. |
| `gold.daily_report_trending_domains` | `silver.hackernews_content` | Extracts domains from HackerNews URLs and aggregates share count and total score. |
| `gold.daily_report_top_devto_articles` | `silver.devto_article` | Selects top Dev.to articles from the past 24 hours with title, tags, reactions, comments, URL, and published timestamp. |
| `gold.daily_report_summary_metrics` | `silver.hackernews_content` | Calculates story count, average score, average comments, unique authors, and unique domains. |

## Telegram Report Lineage

`telegram_daily_report.py` does not read raw API data directly.

It reads the report-ready outputs created by `data_quality_checks.py`:

```text
gold.daily_report_top_stories
gold.daily_report_trending_domains
gold.daily_report_top_devto_articles
gold.daily_report_summary_metrics
```

Then it sends:

- Telegram text summary
- Attached daily report file

This preserves the final project lineage:

```text
API → Bronze → Silver → Gold → data_quality_checks.py → telegram_daily_report.py
```

## SQL Analytics Lineage

The SQL analytics queries use Silver and Gold tables depending on the requirement.

| Query | Main source tables | Reason |
|---|---|---|
| Top Stories | `silver.hackernews_content` | Needs latest HackerNews title, score, comments, and posted timestamp. |
| Trending Domains | `silver.hackernews_content` | Needs URL/domain extraction over recent HackerNews stories. |
| Viral Velocity | `gold.fact_story_snapshot`, `gold.fact_story`, `silver.hackernews_content` | Requires repeated HackerNews snapshots over time. |
| Tag Popularity Over Time | `silver.devto_article` | Needs Dev.to tags and reactions over time. |
| Author Leaderboard | `silver.hackernews_content`, `silver.devto_article` | Combines authors across both platforms. |
| Hourly Activity Heatmap | `silver.hackernews_content`, `silver.devto_article` | Needs post timestamps and engagement by hour. |
| Cross-Platform Topic Overlap | `silver.hackernews_content`, `silver.devto_article` | Tokenizes titles from both platforms. |
| Anomaly Detection | `silver.hackernews_content`, `silver.devto_article` | Counts daily story/article volume. |