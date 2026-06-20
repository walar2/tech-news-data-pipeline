"""Custom Airflow hook for Tech News warehouse quality checks."""

from __future__ import annotations
import pendulum

import os
import re
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook


_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class TechNewsQualityHook(PostgresHook):
    """Project-specific Postgres hook for warehouse quality checks.

    This custom hook extends Airflow's built-in PostgresHook with reusable
    methods for DAG03 data quality checks. It keeps SQL/database logic out of
    the DAG file so the DAG can focus on orchestration, sensors, XCom, and
    alerting.
    """

    def __init__(
        self,
        postgres_conn_id: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Create the custom quality hook.

        Args:
            postgres_conn_id: Airflow Postgres connection ID. If not provided,
                the hook uses POSTGRES_CONN_ID from the environment, then falls
                back to postgres_default.
        """

        super().__init__(
            postgres_conn_id=postgres_conn_id
            or os.environ.get("POSTGRES_CONN_ID", "postgres_default"),
            *args,
            **kwargs,
        )

    @staticmethod
    def _validate_identifier(identifier: str) -> str:
        """Validate a SQL identifier such as schema, table, or column name."""

        if not _IDENTIFIER_PATTERN.fullmatch(identifier):
            raise ValueError(f"Unsafe SQL identifier: {identifier}")

        return identifier

    def get_row_count(
        self,
        schema_name: str,
        table_name: str,
        where_clause: str | None = None,
        parameters: tuple[Any, ...] | None = None,
    ) -> int:
        """Return the number of rows in a warehouse table.

        Args:
            schema_name: Database schema name.
            table_name: Database table name.
            where_clause: Optional SQL WHERE clause without the word WHERE.
            parameters: Optional SQL parameters for the WHERE clause.

        Returns:
            Row count as an integer.
        """

        schema_name = self._validate_identifier(schema_name)
        table_name = self._validate_identifier(table_name)

        sql = f"SELECT COUNT(*) FROM {schema_name}.{table_name}"

        if where_clause:
            sql += f" WHERE {where_clause}"

        result = self.get_first(sql, parameters=parameters)

        return int(result[0] or 0)

    def get_latest_timestamp(
        self,
        schema_name: str,
        table_name: str,
        timestamp_column: str,
        where_clause: str | None = None,
        parameters: tuple[Any, ...] | None = None,
    ):
        """Return the latest timestamp from a table column."""

        schema_name = self._validate_identifier(schema_name)
        table_name = self._validate_identifier(table_name)
        timestamp_column = self._validate_identifier(timestamp_column)

        sql = (
            f"SELECT MAX({timestamp_column}) "
            f"FROM {schema_name}.{table_name}"
        )

        if where_clause:
            sql += f" WHERE {where_clause}"

        result = self.get_first(sql, parameters=parameters)

        return result[0] if result else None

    def get_today_count(
        self,
        schema_name: str,
        table_name: str,
        timestamp_column: str,
    ) -> int:
        """Return today's row count based on a timestamp column."""

        schema_name = self._validate_identifier(schema_name)
        table_name = self._validate_identifier(table_name)
        timestamp_column = self._validate_identifier(timestamp_column)

        sql = f"""
            SELECT COUNT(*)
            FROM {schema_name}.{table_name}
            WHERE {timestamp_column} >= CURRENT_DATE
              AND {timestamp_column} < CURRENT_DATE + INTERVAL '1 day'
        """

        result = self.get_first(sql)

        return int(result[0] or 0)

    def get_7_day_average_count(
        self,
        schema_name: str,
        table_name: str,
        timestamp_column: str,
    ) -> float:
        """Return the average daily row count for the previous 7 days.

        Today is excluded because the current day may still be incomplete.
        """

        schema_name = self._validate_identifier(schema_name)
        table_name = self._validate_identifier(table_name)
        timestamp_column = self._validate_identifier(timestamp_column)

        sql = f"""
            WITH daily_counts AS (
                SELECT
                    DATE({timestamp_column}) AS record_date,
                    COUNT(*) AS row_count
                FROM {schema_name}.{table_name}
                WHERE {timestamp_column} >= CURRENT_DATE - INTERVAL '7 days'
                  AND {timestamp_column} < CURRENT_DATE
                GROUP BY DATE({timestamp_column})
            )
            SELECT COALESCE(AVG(row_count), 0)
            FROM daily_counts
        """

        result = self.get_first(sql)

        return float(result[0] or 0)

    def get_null_counts(
        self,
        schema_name: str,
        table_name: str,
        required_columns: list[str],
    ) -> dict[str, int]:
        """Return null counts for required columns.

        Args:
            schema_name: Database schema name.
            table_name: Database table name.
            required_columns: Columns that must not contain null values.

        Returns:
            Dictionary such as {"title": 0, "url": 0, "score": 0}.
        """

        schema_name = self._validate_identifier(schema_name)
        table_name = self._validate_identifier(table_name)

        safe_columns = [
            self._validate_identifier(column)
            for column in required_columns
        ]

        select_parts = [
            f"COUNT(*) FILTER (WHERE {column} IS NULL) AS {column}_null_count"
            for column in safe_columns
        ]

        sql = f"""
            SELECT {", ".join(select_parts)}
            FROM {schema_name}.{table_name}
        """

        result = self.get_first(sql)

        return {
            column: int(result[index] or 0)
            for index, column in enumerate(safe_columns)
        }

    def _qualified_table_name(self, schema_name: str, table_name: str) -> str:
        """Return a safely validated schema-qualified table name."""

        schema_name = self._validate_identifier(schema_name)
        table_name = self._validate_identifier(table_name)

        return f"{schema_name}.{table_name}"

    def _get_table_columns(self, schema_name: str, table_name: str) -> set[str]:
        """Return all column names for a warehouse table."""

        schema_name = self._validate_identifier(schema_name)
        table_name = self._validate_identifier(table_name)

        sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
        """

        rows = self.get_records(sql, parameters=(schema_name, table_name))

        return {row[0] for row in rows}

    def _pick_column(
            self,
            available_columns: set[str],
            candidates: list[str],
            label: str,
            required: bool = True,
    ) -> str | None:
        """Pick the first available column from a list of possible names."""

        for column in candidates:
            if column in available_columns:
                return self._validate_identifier(column)

        if required:
            raise ValueError(
                f"Could not find required {label} column. "
                f"Tried: {candidates}. "
                f"Available columns: {sorted(available_columns)}"
            )

        return None

    def create_daily_report_tables(
            self,
            report_schema: str = "tech_news",
    ) -> None:
        """Create DAG03 report snapshot tables used by DAG04.

        These tables are the official DAG03 output for the Telegram reporting
        DAG. DAG04 should read from these tables instead of directly querying
        raw, bronze, silver, or source pipeline tables.
        """

        report_schema = self._validate_identifier(report_schema)

        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {report_schema}")

                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {report_schema}.daily_report_top_stories (
                        report_date DATE NOT NULL,
                        source_item_id TEXT NOT NULL,
                        title TEXT,
                        author TEXT,
                        score INTEGER,
                        comments_count INTEGER,
                        url TEXT,
                        posted_at TIMESTAMPTZ,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (report_date, source_item_id)
                    )
                    """
                )

                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {report_schema}.daily_report_trending_domains (
                        report_date DATE NOT NULL,
                        domain TEXT NOT NULL,
                        share_count INTEGER NOT NULL,
                        total_score INTEGER NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (report_date, domain)
                    )
                    """
                )

                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {report_schema}.daily_report_top_devto_articles (
                        report_date DATE NOT NULL,
                        article_id TEXT NOT NULL,
                        title TEXT,
                        tags TEXT,
                        reactions INTEGER,
                        comments INTEGER,
                        url TEXT,
                        published_at TIMESTAMPTZ,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (report_date, article_id)
                    )
                    """
                )

                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {report_schema}.daily_report_summary_metrics (
                        report_date DATE PRIMARY KEY,
                        story_count INTEGER NOT NULL,
                        avg_score NUMERIC(10, 2),
                        avg_comments NUMERIC(10, 2),
                        unique_authors INTEGER NOT NULL,
                        unique_domains INTEGER NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )

            conn.commit()

    def build_daily_report_snapshot(
            self,
            report_schema: str = "tech_news",
            hackernews_schema: str = "tech_news",
            hackernews_table: str = "hackernews_content",
            devto_schema: str = "tech_news",
            devto_table: str = "devto_article",
    ) -> dict[str, Any]:
        """Build DAG03 daily report snapshot tables for DAG04.

        The generated snapshot contains:
        - all HackerNews stories from the past 24 hours
        - top 20 trending HackerNews domains
        - top 20 Dev.to articles from the past 24 hours
        - daily summary metrics for HackerNews
        """

        report_schema = self._validate_identifier(report_schema)
        hackernews_source = self._qualified_table_name(
            hackernews_schema,
            hackernews_table,
        )
        devto_source = self._qualified_table_name(
            devto_schema,
            devto_table,
        )

        self.create_daily_report_tables(report_schema=report_schema)

        hackernews_columns = self._get_table_columns(
            hackernews_schema,
            hackernews_table,
        )
        devto_columns = self._get_table_columns(
            devto_schema,
            devto_table,
        )

        hn_id_col = self._pick_column(
            hackernews_columns,
            ["source_item_id", "item_id", "id"],
            "HackerNews ID",
        )
        hn_title_col = self._pick_column(
            hackernews_columns,
            ["title"],
            "HackerNews title",
        )
        hn_author_col = self._pick_column(
            hackernews_columns,
            ["author_username", "author", "by"],
            "HackerNews author",
        )
        hn_score_col = self._pick_column(
            hackernews_columns,
            ["score"],
            "HackerNews score",
        )
        hn_comments_col = self._pick_column(
            hackernews_columns,
            ["comments_count", "comment_count", "descendants"],
            "HackerNews comments",
        )
        hn_url_col = self._pick_column(
            hackernews_columns,
            ["url"],
            "HackerNews URL",
        )
        hn_posted_at_col = self._pick_column(
            hackernews_columns,
            ["posted_at", "created_at", "time"],
            "HackerNews posted timestamp",
        )
        hn_content_type_col = self._pick_column(
            hackernews_columns,
            ["content_type", "type"],
            "HackerNews content type",
            required=False,
        )

        devto_id_col = self._pick_column(
            devto_columns,
            ["source_item_id", "article_id", "id", "source_article_id"],
            "Dev.to article ID",
        )

        devto_title_col = self._pick_column(
            devto_columns,
            ["title"],
            "Dev.to title",
        )
        devto_tags_col = self._pick_column(
            devto_columns,
            ["tags", "tag_list"],
            "Dev.to tags",
            required=False,
        )
        devto_reactions_col = self._pick_column(
            devto_columns,
            ["reactions_count", "reactions", "public_reactions_count", "positive_reactions_count"],
            "Dev.to reactions",
            required = False,
        )
        devto_comments_col = self._pick_column(
            devto_columns,
            ["comments_count", "comments"],
            "Dev.to comments",
            required=False,
        )
        devto_url_col = self._pick_column(
            devto_columns,
            ["url", "canonical_url"],
            "Dev.to URL",
            required=False,
        )
        devto_published_at_col = self._pick_column(
            devto_columns,
            ["published_at", "published_timestamp", "created_at"],
            "Dev.to published timestamp",
        )

        hn_story_filter = ""
        if hn_content_type_col:
            hn_story_filter = f"AND {hn_content_type_col} = 'story'"

        devto_tags_expr = (
            f"{devto_tags_col}::TEXT"
            if devto_tags_col
            else "NULL::TEXT"
        )
        devto_reactions_expr = (
            f"COALESCE({devto_reactions_col}, 0)"
            if devto_reactions_col
            else "0"
        )
        devto_comments_expr = (
            f"COALESCE({devto_comments_col}, 0)"
            if devto_comments_col
            else "0"
        )
        devto_url_expr = (
            f"{devto_url_col}"
            if devto_url_col
            else "NULL::TEXT"
        )

        domain_expr = f"""
            LOWER(
                REGEXP_REPLACE(
                    SPLIT_PART(
                        REGEXP_REPLACE(
                            COALESCE({hn_url_col}, ''),
                            '^https?://',
                            '',
                            'i'
                        ),
                        '/',
                        1
                    ),
                    '^www\\.',
                    '',
                    'i'
                )
            )
        """

        report_date = pendulum.now("Asia/Yangon").date()

        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"DELETE FROM {report_schema}.daily_report_top_stories WHERE report_date = %s",
                    (report_date,),
                )
                cursor.execute(
                    f"DELETE FROM {report_schema}.daily_report_trending_domains WHERE report_date = %s",
                    (report_date,),
                )
                cursor.execute(
                    f"DELETE FROM {report_schema}.daily_report_top_devto_articles WHERE report_date = %s",
                    (report_date,),
                )
                cursor.execute(
                    f"DELETE FROM {report_schema}.daily_report_summary_metrics WHERE report_date = %s",
                    (report_date,),
                )

                cursor.execute(
                    f"""
                    INSERT INTO {report_schema}.daily_report_top_stories (
                        report_date,
                        source_item_id,
                        title,
                        author,
                        score,
                        comments_count,
                        url,
                        posted_at
                    )
                    SELECT
                        %s AS report_date,
                        {hn_id_col}::TEXT AS source_item_id,
                        {hn_title_col} AS title,
                        {hn_author_col} AS author,
                        COALESCE({hn_score_col}, 0) AS score,
                        COALESCE({hn_comments_col}, 0) AS comments_count,
                        COALESCE(
                            NULLIF({hn_url_col}, ''),
                            'https://news.ycombinator.com/item?id=' || {hn_id_col}::TEXT
                        ) AS url,
                        {hn_posted_at_col} AS posted_at
                    FROM {hackernews_source}
                    WHERE {hn_posted_at_col} >= NOW() - INTERVAL '24 hours'
                    {hn_story_filter}
                    ORDER BY COALESCE({hn_score_col}, 0) DESC,
                             COALESCE({hn_comments_col}, 0) DESC
                    """,
                    (report_date,),
                )

                cursor.execute(
                    f"""
                    INSERT INTO {report_schema}.daily_report_trending_domains (
                        report_date,
                        domain,
                        share_count,
                        total_score
                    )
                    SELECT
                        %s AS report_date,
                        {domain_expr} AS domain,
                        COUNT(*) AS share_count,
                        SUM(COALESCE({hn_score_col}, 0)) AS total_score
                    FROM {hackernews_source}
                    WHERE {hn_posted_at_col} >= NOW() - INTERVAL '24 hours'
                    {hn_story_filter}
                      AND {hn_url_col} IS NOT NULL
                      AND {hn_url_col} <> ''
                    GROUP BY {domain_expr}
                    HAVING {domain_expr} <> ''
                    ORDER BY share_count DESC, total_score DESC
                    LIMIT 20
                    """,
                    (report_date,),
                )

                cursor.execute(
                    f"""
                    INSERT INTO {report_schema}.daily_report_top_devto_articles (
                        report_date,
                        article_id,
                        title,
                        tags,
                        reactions,
                        comments,
                        url,
                        published_at
                    )
                    SELECT
                        %s AS report_date,
                        {devto_id_col}::TEXT AS article_id,
                        {devto_title_col} AS title,
                        {devto_tags_expr} AS tags,
                        {devto_reactions_expr} AS reactions,
                        {devto_comments_expr} AS comments,
                        {devto_url_expr} AS url,
                        {devto_published_at_col} AS published_at
                    FROM {devto_source}
                    WHERE {devto_published_at_col} >= NOW() - INTERVAL '24 hours'
                    ORDER BY reactions DESC, comments DESC
                    LIMIT 20
                    """,
                    (report_date,),
                )

                cursor.execute(
                    f"""
                    INSERT INTO {report_schema}.daily_report_summary_metrics (
                        report_date,
                        story_count,
                        avg_score,
                        avg_comments,
                        unique_authors,
                        unique_domains
                    )
                    SELECT
                        %s AS report_date,
                        COUNT(*) AS story_count,
                        ROUND(AVG(COALESCE({hn_score_col}, 0))::NUMERIC, 2) AS avg_score,
                        ROUND(AVG(COALESCE({hn_comments_col}, 0))::NUMERIC, 2) AS avg_comments,
                        COUNT(DISTINCT {hn_author_col}) AS unique_authors,
                        COUNT(DISTINCT NULLIF({domain_expr}, '')) AS unique_domains
                    FROM {hackernews_source}
                    WHERE {hn_posted_at_col} >= NOW() - INTERVAL '24 hours'
                    {hn_story_filter}
                    """,
                    (report_date,),
                )

                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {report_schema}.daily_report_top_stories
                    WHERE report_date = %s
                    """,
                    (report_date,),
                )
                top_story_count = int(cursor.fetchone()[0] or 0)

                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {report_schema}.daily_report_top_devto_articles
                    WHERE report_date = %s
                    """,
                    (report_date,),
                )
                top_devto_count = int(cursor.fetchone()[0] or 0)

            conn.commit()

        return {
            "report_date": str(report_date),
            "report_schema": report_schema,
            "top_story_count": top_story_count,
            "top_devto_count": top_devto_count,
            "top_stories_table": f"{report_schema}.daily_report_top_stories",
            "trending_domains_table": f"{report_schema}.daily_report_trending_domains",
            "top_devto_articles_table": f"{report_schema}.daily_report_top_devto_articles",
            "summary_metrics_table": f"{report_schema}.daily_report_summary_metrics",
        }
    def insert_quality_audit_result(
        self,
        check_name: str,
        status: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Insert one quality-check result into the audit table.

        This assumes DAG03's audit table exists. If your audit table has a
        different name or columns, we will adjust this method after checking
        your current 002_quality_audit.sql file.
        """

        sql = """
            INSERT INTO audit.quality_check_result (
                check_name,
                status,
                message,
                details,
                checked_at
            )
            VALUES (%s, %s, %s, %s::jsonb, NOW())
        """

        import json

        self.run(
            sql,
            parameters=(
                check_name,
                status,
                message,
                json.dumps(details or {}),
            ),
        )