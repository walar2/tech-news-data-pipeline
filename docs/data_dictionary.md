# Data Dictionary

This data dictionary documents the active project warehouse schemas:

- `audit`
- `bronze`
- `silver`
- `gold`

Note: duplicate `tech_news.daily_report_*` tables may exist from earlier testing. The active reporting tables for the final project should be the `gold.daily_report_*` tables.

## audit.daily_pipeline_status

Stores the daily operational result from `data_quality_checks.py`.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| status_date | date | `data_quality_checks.py` | The date being checked. |
| hackernews_status | text | Airflow DAG status check | Same-day status of `hackernews_hourly_pipeline`. |
| devto_status | text | Airflow DAG status check | Same-day status of `devto_daily_pipeline`. |
| quality_status | text | Data quality logic | Overall quality result, such as `passed` or `failed`. |
| message | text | Data quality logic | Human-readable summary of quality/operational issues. |
| checked_at | timestamp with time zone | System timestamp | Time when the daily status was written. |

## audit.data_quality_result

Stores individual quality check results.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| result_key | bigint | Database generated | Unique row identifier for a quality result. |
| check_date | date | `data_quality_checks.py` | Date of the quality check. |
| check_name | text | Data quality logic | Name of the check, e.g. freshness, row count anomaly, null rate. |
| source_name | text | Data quality logic | Source checked, e.g. HackerNews or Dev.to. |
| passed | boolean | Data quality logic | Whether the check passed. |
| observed_value | numeric | Data quality logic | Actual measured value. |
| expected_value | text | Data quality logic | Expected threshold or condition. |
| details | jsonb | Data quality logic | Extra metadata explaining the check result. |
| checked_at | timestamp with time zone | System timestamp | Time when the check was recorded. |

## audit.pipeline_run

Tracks pipeline execution metadata.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| run_key | bigint | Database generated | Unique identifier for a pipeline run record. |
| dag_id | text | Airflow | DAG that produced the run. |
| run_id | text | Airflow | Airflow run identifier. |
| batch_id | uuid | Pipeline extraction task | Batch identifier used to trace records through Bronze/Silver/Gold. |
| started_at | timestamp with time zone | Airflow/pipeline logic | Time the run started. |
| finished_at | timestamp with time zone | Airflow/pipeline logic | Time the run finished. |
| status | text | Airflow/pipeline logic | Run status such as success or failed. |
| records_processed | integer | Pipeline task output | Number of records processed. |
| error_message | text | Pipeline error handling | Error message if the run failed. |

## bronze.hackernews_payload

Stores raw HackerNews API responses.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| payload_key | bigint | Database generated | Unique raw payload row identifier. |
| endpoint | text | HackerNews API extraction | API endpoint used, e.g. `topstories`, `beststories`, `item/{id}`. |
| source_item_id | text | HackerNews API | HackerNews item/user ID when available. |
| batch_id | uuid | Extraction task | Batch identifier for tracing ingestion. |
| payload | jsonb | HackerNews API | Full raw JSON response. |
| ingested_at | timestamp with time zone | System timestamp | Time raw payload was inserted. |

## bronze.devto_payload

Stores raw Dev.to API responses.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| payload_key | bigint | Database generated | Unique raw payload row identifier. |
| endpoint | text | Dev.to API extraction | API endpoint used for article list/detail retrieval. |
| source_item_id | text | Dev.to API | Dev.to article ID when available. |
| batch_id | uuid | Extraction task | Batch identifier for tracing ingestion. |
| payload | jsonb | Dev.to API | Full raw JSON response. |
| ingested_at | timestamp with time zone | System timestamp | Time raw payload was inserted. |

## silver.hackernews_content

Cleaned HackerNews content records.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| source_item_id | text | `bronze.hackernews_payload.payload` | HackerNews item ID. |
| content_type | text | HackerNews payload `type` | Type of content, e.g. story, comment, job. |
| parent_item_id | text | HackerNews payload `parent` | Parent item ID for comments. |
| author_username | text | HackerNews payload `by` | HackerNews author username. |
| title | text | HackerNews payload `title` | Story title. |
| text | text | HackerNews payload `text` | Text body for comments or text posts. |
| url | text | HackerNews payload `url` or fallback item URL | External story URL or HackerNews item URL. |
| posted_at | timestamp with time zone | HackerNews payload `time` | Original posting timestamp. |
| score | integer | HackerNews payload `score` | HackerNews score. |
| comments_count | integer | HackerNews payload `descendants` | Number of comments for stories. |
| is_valid | boolean | Validation logic | Whether the record passed validation. |
| validation_errors | ARRAY | Validation logic | List of validation errors. |
| batch_id | uuid | Bronze payload | Batch identifier from ingestion. |
| processed_at | timestamp with time zone | System timestamp | Time the Silver row was processed. |

## silver.hackernews_user_profile

Cleaned HackerNews user profile records.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| username | text | HackerNews user payload `id` | HackerNews username. |
| about | text | HackerNews user payload `about` | User profile/about text. |
| karma | integer | HackerNews user payload `karma` | HackerNews karma score. |
| account_created_at | timestamp with time zone | HackerNews user payload `created` | Account creation timestamp. |
| is_valid | boolean | Validation logic | Whether the profile record passed validation. |
| validation_errors | ARRAY | Validation logic | List of validation errors. |
| batch_id | uuid | Bronze payload | Batch identifier from ingestion. |
| processed_at | timestamp with time zone | System timestamp | Time the profile row was processed. |

## silver.devto_article

Cleaned Dev.to article records.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| source_item_id | text | Dev.to payload `id` | Dev.to article ID. |
| author_username | text | Dev.to payload user fields | Dev.to author username. |
| title | text | Dev.to payload `title` | Article title. |
| description | text | Dev.to payload `description` | Article description/summary. |
| url | text | Dev.to payload `url` | Article URL. |
| published_at | timestamp with time zone | Dev.to payload `published_at` | Article publish timestamp. |
| comments_count | integer | Dev.to payload `comments_count` | Number of comments. |
| reactions_count | integer | Dev.to payload reactions count | Number of public reactions; used as score proxy. |
| reading_time | integer | Dev.to payload reading time | Estimated reading time in minutes. |
| tags | ARRAY | Dev.to payload tags | Tags assigned to the article. |
| is_valid | boolean | Validation logic | Whether the article passed validation. |
| validation_errors | ARRAY | Validation logic | List of validation errors. |
| batch_id | uuid | Bronze payload | Batch identifier from ingestion. |
| processed_at | timestamp with time zone | System timestamp | Time the Silver row was processed. |

## gold.dim_source

Dimension table for content source/platform.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| source_key | smallint | Gold transform | Surrogate key for source/platform. |
| source_name | text | Pipeline source mapping | Source name, e.g. HackerNews or Dev.to. |

## gold.dim_author

Dimension table for authors.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| author_key | bigint | Gold transform | Surrogate key for author. |
| username | text | Silver source tables | Author username. |
| source | text | Gold transform | Platform where the author was observed. |
| first_seen_at | timestamp with time zone | Gold transform | First time the author appeared in the warehouse. |
| valid_from | timestamp with time zone | Gold SCD logic | Start time of the current author dimension version. |
| valid_to | timestamp with time zone | Gold SCD logic | End time of the author dimension version. |
| is_current | boolean | Gold SCD logic | Whether this is the current author record. |

## gold.dim_domain

Dimension table for article/story domains.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| domain_key | bigint | Gold transform | Surrogate key for a domain. |
| domain | text | URL parsing logic | Extracted domain, e.g. `github.com`, `youtube.com`. |

## gold.dim_date

Date dimension.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| date_key | integer | Gold transform | Date key, usually formatted as YYYYMMDD. |
| year | smallint | Derived from timestamp | Calendar year. |
| quarter | smallint | Derived from timestamp | Calendar quarter. |
| month | smallint | Derived from timestamp | Calendar month. |
| day | smallint | Derived from timestamp | Day of month. |
| weekday | smallint | Derived from timestamp | Day of week number. |
| is_weekend | boolean | Derived from timestamp | Whether the date is Saturday/Sunday. |

## gold.dim_time

Hour/time dimension.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| hour_key | smallint | Gold transform | Hour key. |
| hour | smallint | Derived from timestamp | Hour of day, 0–23. |
| time_of_day | text | Gold transform | Time bucket label such as morning/afternoon/evening. |

## gold.dim_tag

Tag dimension, mainly from Dev.to.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| tag_key | bigint | Gold transform | Surrogate key for a tag. |
| tag_name | text | `silver.devto_article.tags` | Normalized tag name. |

## gold.fact_story

Main analytics fact table for HackerNews stories and Dev.to articles.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| story_key | bigint | Gold transform | Surrogate key for a story/article. |
| source_item_id | text | Silver source tables | Original platform item/article ID. |
| source_key | smallint | `gold.dim_source` | Source/platform foreign key. |
| author_key | bigint | `gold.dim_author` | Author foreign key. |
| domain_key | bigint | `gold.dim_domain` | Domain foreign key. |
| date_key | integer | `gold.dim_date` | Posting date foreign key. |
| hour_key | smallint | `gold.dim_time` | Posting hour foreign key. |
| score | integer | HackerNews score or platform score equivalent | Main score metric. |
| comments_count | integer | Silver source tables | Number of comments. |
| reactions_count | integer | Dev.to reactions or zero for HackerNews | Reaction metric. |
| reading_time | integer | Dev.to article metadata | Estimated reading time; mainly applies to Dev.to. |

## gold.fact_story_snapshot

Snapshot table for tracking story metrics over time.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| snapshot_key | bigint | Database generated | Unique snapshot row identifier. |
| story_key | bigint | `gold.fact_story` | Story/article being snapshotted. |
| snapshot_at | timestamp with time zone | System timestamp | Time the snapshot was captured. |
| score | integer | Current story score at snapshot time | Score value used for viral velocity. |
| comments_count | integer | Current comment count at snapshot time | Comment count at snapshot time. |

## gold.bridge_story_tag

Many-to-many bridge between stories/articles and tags.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| story_key | bigint | `gold.fact_story` | Story/article key. |
| tag_key | bigint | `gold.dim_tag` | Tag key. |

## gold.daily_report_top_stories

Report output table created by `data_quality_checks.py` for Telegram reporting.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| report_date | date | `data_quality_checks.py` | Report date. |
| source_item_id | text | `silver.hackernews_content` | HackerNews story ID. |
| title | text | `silver.hackernews_content.title` | Story title. |
| author | text | `silver.hackernews_content.author_username` | Story author. |
| score | integer | `silver.hackernews_content.score` | HackerNews score. |
| comments_count | integer | `silver.hackernews_content.comments_count` | Story comment count. |
| url | text | `silver.hackernews_content.url` | Story URL. |
| posted_at | timestamp with time zone | `silver.hackernews_content.posted_at` | Story posting time. |
| created_at | timestamp with time zone | System timestamp | Time the report row was created. |

## gold.daily_report_trending_domains

Report output table for top HackerNews domains.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| report_date | date | `data_quality_checks.py` | Report date. |
| domain | text | URL parsing from HackerNews stories | Domain name. |
| share_count | integer | Aggregation logic | Number of stories from that domain. |
| total_score | integer | Aggregation logic | Total HackerNews score for that domain. |
| created_at | timestamp with time zone | System timestamp | Time the report row was created. |

## gold.daily_report_top_devto_articles

Report output table for top Dev.to articles.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| report_date | date | `data_quality_checks.py` | Report date. |
| article_id | text | `silver.devto_article.source_item_id` | Dev.to article ID. |
| title | text | `silver.devto_article.title` | Article title. |
| tags | text | `silver.devto_article.tags` | Article tags as text for reporting. |
| reactions | integer | `silver.devto_article.reactions_count` | Dev.to reaction count. |
| comments | integer | `silver.devto_article.comments_count` | Dev.to comment count. |
| url | text | `silver.devto_article.url` | Article URL. |
| published_at | timestamp with time zone | `silver.devto_article.published_at` | Publish timestamp. |
| created_at | timestamp with time zone | System timestamp | Time the report row was created. |

## gold.daily_report_summary_metrics

Report output table for daily summary metrics.

| Column | Data type | Source | Business meaning |
|---|---:|---|---|
| report_date | date | `data_quality_checks.py` | Report date. |
| story_count | integer | Aggregation from daily HackerNews stories | Number of stories in the report window. |
| avg_score | numeric | Aggregation from HackerNews scores | Average story score. |
| avg_comments | numeric | Aggregation from HackerNews comments | Average comment count. |
| unique_authors | integer | Aggregation from HackerNews authors | Number of unique authors. |
| unique_domains | integer | Aggregation from HackerNews URLs | Number of unique domains. |
| created_at | timestamp with time zone | System timestamp | Time the report summary was created. |