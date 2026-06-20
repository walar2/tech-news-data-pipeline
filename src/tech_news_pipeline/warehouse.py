"""Warehouse functions for Bronze, Silver, and Gold."""

from __future__ import annotations

from datetime import datetime,timezone
from pathlib import Path
from uuid import UUID
from urllib.parse import urlparse
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json, execute_values


POSTGRES_CONN_ID = "tech_news_postgres"


def get_hook() -> PostgresHook:
    """Return the Airflow PostgreSQL hook."""

    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def load_devto_bronze(
    batch_id: UUID | str,
    responses: list[dict],
) -> int:
    """Load raw DEV.to API responses into source-specific Bronze."""

    rows = [
        (
            response["endpoint"],
            response.get("source_item_id"),
            str(batch_id),
            Json(response["payload"]),
        )
        for response in responses
    ]

    sql = """
        INSERT INTO bronze.devto_payload (
            endpoint,
            source_item_id,
            batch_id,
            payload
        )
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    with get_hook().get_conn() as connection:
        with connection.cursor() as cursor:
            execute_values(cursor, sql, rows)

    return len(rows)


def parse_timestamp(value) -> datetime | None:
    """Parse timestamp values safely."""

    if value is None:
        return None

    return datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )


def integer_or_default(
    value,
    default: int = 0,
) -> int:
    """Convert a value to integer safely."""

    try:
        if value is None:
            return default

        parsed = int(value)

        if parsed < 0:
            return default

        return parsed

    except (TypeError, ValueError):
        return default


def normalize_devto_article(
    payload: dict,
    batch_id: UUID | str,
) -> tuple:
    """Normalize one DEV.to article-detail payload into Silver."""

    errors: list[str] = []

    article_id = payload.get("id")

    if article_id is None:
        errors.append("id:null")

    title = payload.get("title")

    if not isinstance(title, str) or not title.strip():
        title = None
        errors.append("title:null_or_invalid")

    url = payload.get("url")

    if not isinstance(url, str) or not url.strip():
        url = None
        errors.append("url:null_or_invalid")

    user = payload.get("user")

    if not isinstance(user, dict):
        user = {}

    author_username = user.get("username")

    if not isinstance(author_username, str) or not author_username.strip():
        author_username = None
        errors.append("user.username:null_or_invalid")

    published_value = (
        payload.get("published_timestamp")
        or payload.get("published_at")
    )

    try:
        published_at = parse_timestamp(published_value)
    except ValueError:
        published_at = None
        errors.append("published_at:invalid_type")

    if published_at is None:
        errors.append("published_at:null")

    tags = payload.get("tag_list") or payload.get("tags") or []

    if isinstance(tags, str):
        tags = [
            tag.strip()
            for tag in tags.split(",")
            if tag.strip()
        ]

    if not isinstance(tags, list):
        tags = []
        errors.append("tags:invalid_type")

    return (
        str(article_id),
        author_username,
        title,
        payload.get("description"),
        url,
        published_at,
        integer_or_default(payload.get("comments_count")),
        integer_or_default(payload.get("public_reactions_count")),
        integer_or_default(payload.get("reading_time_minutes")),
        tags,
        not errors,
        errors,
        str(batch_id),
    )


def transform_devto_batch_to_silver(
    batch_id: UUID | str,
) -> int:
    """Transform DEV.to Bronze article-detail payloads into Silver."""

    records = get_hook().get_records(
        """
        SELECT endpoint, payload
        FROM bronze.devto_payload
        WHERE batch_id = %s
          AND endpoint LIKE 'articles/%%'
        """,
        parameters=(str(batch_id),),
    )

    rows = []

    for endpoint, payload in records:
        article_id_part = endpoint.replace("articles/", "")

        if not article_id_part.isdigit():
            continue

        if isinstance(payload, dict):
            rows.append(
                normalize_devto_article(
                    payload=payload,
                    batch_id=batch_id,
                )
            )

    if not rows:
        return 0

    sql = """
        INSERT INTO silver.devto_article (
            source_item_id,
            author_username,
            title,
            description,
            url,
            published_at,
            comments_count,
            reactions_count,
            reading_time,
            tags,
            is_valid,
            validation_errors,
            batch_id
        )
        VALUES %s
        ON CONFLICT (source_item_id)
        DO UPDATE SET
            author_username = EXCLUDED.author_username,
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            url = EXCLUDED.url,
            published_at = EXCLUDED.published_at,
            comments_count = EXCLUDED.comments_count,
            reactions_count = EXCLUDED.reactions_count,
            reading_time = EXCLUDED.reading_time,
            tags = EXCLUDED.tags,
            is_valid = EXCLUDED.is_valid,
            validation_errors = EXCLUDED.validation_errors,
            batch_id = EXCLUDED.batch_id,
            processed_at = NOW()
    """

    with get_hook().get_conn() as connection:
        with connection.cursor() as cursor:
            execute_values(cursor, sql, rows)

    return len(rows)


def load_gold(
    batch_id: UUID | str,
) -> None:
    """Run Silver-to-Gold SQL transformation."""

    sql_path = Path(
        "/opt/airflow/sql/transform/001_silver_to_gold.sql"
    )

    get_hook().run(
        sql_path.read_text(encoding="utf-8"),
        parameters={
            "batch_id": str(batch_id),
        },
    )
def load_hackernews_bronze(batch_id: str, responses: list[dict]) -> int:
    """Load raw Hacker News API responses into the Hacker News Bronze table."""
    rows = [
        (
            response["endpoint"],
            response.get("source_item_id"),
            batch_id,
            Json(response["payload"]),
        )
        for response in responses
    ]

    if not rows:
        return 0

    sql = """
        INSERT INTO bronze.hackernews_payload
            (endpoint, source_item_id, batch_id, payload)
        VALUES %s
        ON CONFLICT (endpoint, COALESCE(source_item_id, ''), batch_id)
        DO NOTHING
    """

    with get_hook().get_conn() as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, sql, rows)

    return len(rows)


def _parse_hackernews_time(value) -> datetime | None:
    """Convert Hacker News Unix timestamp values into UTC datetimes."""
    if value is None:
        return None

    return datetime.fromtimestamp(int(value), tz=timezone.utc)


def _safe_int(value, default: int = 0) -> int:
    """Convert API numeric values safely."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_text(value) -> str | None:
    """Return stripped text when the value is usable."""
    if isinstance(value, str) and value.strip():
        return value.strip()

    return None


def transform_hackernews_batch_to_silver(batch_id: str) -> int:
    """Transform Hacker News Bronze payloads into Silver content and user profile tables."""
    records = get_hook().get_records(
        """
        SELECT endpoint, source_item_id, payload
        FROM bronze.hackernews_payload
        WHERE batch_id = %s
        """,
        parameters=(batch_id,),
    )

    content_rows = []
    user_rows = []

    for endpoint, source_item_id, payload in records:
        if not isinstance(payload, dict):
            continue

        if endpoint.startswith("item/"):
            errors = []

            item_id = payload.get("id")
            content_type = _safe_text(payload.get("type")) or "story"
            author_username = _safe_text(payload.get("by"))
            posted_at = None

            try:
                posted_at = _parse_hackernews_time(payload.get("time"))
            except (TypeError, ValueError, OverflowError):
                errors.append("time:invalid")

            title = _safe_text(payload.get("title"))
            text = _safe_text(payload.get("text"))
            url = _safe_text(payload.get("url"))
            if content_type == "story" and not url and item_id is not None:
                url = f"https://news.ycombinator.com/item?id={item_id}"

            if item_id is None:
                errors.append("id:missing")

            if content_type == "story" and not title:
                errors.append("title:missing")

            content_rows.append(
                (
                    str(item_id),
                    content_type,
                    str(payload.get("parent")) if payload.get("parent") is not None else None,
                    author_username,
                    title,
                    text,
                    url,
                    posted_at,
                    _safe_int(payload.get("score")),
                    _safe_int(payload.get("descendants")),
                    not errors,
                    errors,
                    batch_id,
                )
            )

        elif endpoint.startswith("user/"):
            errors = []

            username = _safe_text(payload.get("id"))
            if not username:
                errors.append("id:missing")

            account_created_at = None
            try:
                account_created_at = _parse_hackernews_time(payload.get("created"))
            except (TypeError, ValueError, OverflowError):
                errors.append("created:invalid")

            user_rows.append(
                (
                    username,
                    _safe_text(payload.get("about")),
                    _safe_int(payload.get("karma")),
                    account_created_at,
                    not errors,
                    errors,
                    batch_id,
                )
            )

    with get_hook().get_conn() as conn:
        with conn.cursor() as cursor:
            if content_rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver.hackernews_content (
                        source_item_id, content_type, parent_item_id,
                        author_username, title, text, url, posted_at,
                        score, comments_count, is_valid, validation_errors,
                        batch_id
                    )
                    VALUES %s
                    ON CONFLICT (source_item_id)
                    DO UPDATE SET
                        content_type = EXCLUDED.content_type,
                        parent_item_id = EXCLUDED.parent_item_id,
                        author_username = EXCLUDED.author_username,
                        title = EXCLUDED.title,
                        text = EXCLUDED.text,
                        url = EXCLUDED.url,
                        posted_at = EXCLUDED.posted_at,
                        score = EXCLUDED.score,
                        comments_count = EXCLUDED.comments_count,
                        is_valid = EXCLUDED.is_valid,
                        validation_errors = EXCLUDED.validation_errors,
                        batch_id = EXCLUDED.batch_id,
                        processed_at = NOW()
                    """,
                    content_rows,
                )

            if user_rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver.hackernews_user_profile (
                        username, about, karma, account_created_at,
                        is_valid, validation_errors, batch_id
                    )
                    VALUES %s
                    ON CONFLICT (username)
                    DO UPDATE SET
                        about = EXCLUDED.about,
                        karma = EXCLUDED.karma,
                        account_created_at = EXCLUDED.account_created_at,
                        is_valid = EXCLUDED.is_valid,
                        validation_errors = EXCLUDED.validation_errors,
                        batch_id = EXCLUDED.batch_id,
                        processed_at = NOW()
                    """,
                    user_rows,
                )

    return len(content_rows) + len(user_rows)


def _domain_from_url(url: str | None) -> str:
    """Extract a domain for Gold dimension loading."""
    if not url:
        return "news.ycombinator.com"

    parsed = urlparse(url)
    return parsed.netloc or "news.ycombinator.com"


def load_hackernews_gold(batch_id: str) -> int:
    """Load valid Silver Hacker News stories into Gold fact and dimension tables."""
    stories = get_hook().get_records(
        """
        SELECT
            source_item_id,
            author_username,
            title,
            COALESCE(url, 'https://news.ycombinator.com/item?id=' || source_item_id) AS url,
            posted_at,
            score,
            comments_count
        FROM silver.hackernews_content
        WHERE batch_id = %s
          AND is_valid = TRUE
          AND content_type = 'story'
          AND title IS NOT NULL
          AND posted_at IS NOT NULL
        """,
        parameters=(batch_id,),
    )

    if not stories:
        return 0

    with get_hook().get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT source_key FROM gold.dim_source WHERE source_name = 'HackerNews'"
            )
            source_key = cursor.fetchone()[0]

            loaded_count = 0

            for source_item_id, author_username, title, url, posted_at, score, comments_count in stories:
                domain = _domain_from_url(url)
                username = author_username or "unknown"

                cursor.execute(
                    """
                    INSERT INTO gold.dim_author
                        (username, source, first_seen_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (username, source) WHERE is_current
                    DO UPDATE SET username = EXCLUDED.username
                    RETURNING author_key
                    """,
                    (username, "HackerNews", posted_at),
                )
                author_key = cursor.fetchone()[0]

                cursor.execute(
                    """
                    INSERT INTO gold.dim_domain (domain)
                    VALUES (%s)
                    ON CONFLICT (domain)
                    DO UPDATE SET domain = EXCLUDED.domain
                    RETURNING domain_key
                    """,
                    (domain,),
                )
                domain_key = cursor.fetchone()[0]

                date_key = int(posted_at.strftime("%Y%m%d"))
                hour_key = posted_at.hour

                cursor.execute(
                    """
                    INSERT INTO gold.fact_story (
                        source_item_id, source_key, author_key, domain_key,
                        date_key, hour_key, score, comments_count,
                        reactions_count, reading_time
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, NULL)
                    ON CONFLICT (source_key, source_item_id)
                    DO UPDATE SET
                        author_key = EXCLUDED.author_key,
                        domain_key = EXCLUDED.domain_key,
                        date_key = EXCLUDED.date_key,
                        hour_key = EXCLUDED.hour_key,
                        score = EXCLUDED.score,
                        comments_count = EXCLUDED.comments_count
                    """,
                    (
                        source_item_id,
                        source_key,
                        author_key,
                        domain_key,
                        date_key,
                        hour_key,
                        score,
                        comments_count,
                    ),
                )

                loaded_count += 1

    return loaded_count