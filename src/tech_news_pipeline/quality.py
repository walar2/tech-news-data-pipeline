"""Data quality checks and audit status helpers for the tech news warehouse."""

from __future__ import annotations

import importlib.util
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import re
from urllib.parse import urlparse
from airflow.providers.postgres.hooks.postgres import PostgresHook


WAREHOUSE_CONN_ID = "tech_news_postgres"
AIRFLOW_METADATA_CONN_ID = "airflow_db"

DEVTO_DAG_ID = "devto_daily_pipeline"
HACKERNEWS_DAG_ID = "hackernews_hourly_pipeline"

PIPELINE_SRC_DIR = Path(
    os.environ.get(
        "PIPELINE_SRC_DIR",
        Path(__file__).resolve().parent,
    )
)

QUALITY_HOOK_FILE = PIPELINE_SRC_DIR / "quality_hook.py"


def load_python_file(module_name: str, file_path: Path):
    """Load a Python module directly from a file path.

    This keeps the project compatible with the current Airflow Docker setup,
    where DAGs load project files from the mounted source directory instead of
    relying only on package imports.
    """

    if not file_path.exists():
        raise FileNotFoundError(f"Required file was not found:\n{file_path}")

    spec = importlib.util.spec_from_file_location(module_name, file_path)

    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from:\n{file_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


quality_hook_module = load_python_file(
    module_name="pipeline_quality_hook",
    file_path=QUALITY_HOOK_FILE,
)

TechNewsQualityHook = quality_hook_module.TechNewsQualityHook


def get_warehouse_hook() -> TechNewsQualityHook:
    """Return the custom warehouse quality hook.

    The hook extends Airflow's PostgresHook with project-specific helper
    methods while still supporting normal PostgresHook methods such as run()
    and get_first().
    """

    return TechNewsQualityHook(postgres_conn_id=WAREHOUSE_CONN_ID)


def get_airflow_metadata_hook() -> PostgresHook:
    """Return the Airflow metadata database hook.

    This hook reads Airflow's own metadata database so DAG03 can check whether
    DAG01 and DAG02 succeeded, failed, or have no run history.
    """

    return PostgresHook(postgres_conn_id=AIRFLOW_METADATA_CONN_ID)


def get_latest_dag_status(dag_id: str) -> str:
    """Return the latest DAG status.

    Returns:
        One of success, failed, running, missing, or not_configured.
    """

    hook = get_airflow_metadata_hook()

    dag_exists = hook.get_first(
        """
        SELECT 1
        FROM dag
        WHERE dag_id = %s
        LIMIT 1;
        """,
        parameters=(dag_id,),
    )

    if dag_exists is None:
        return "not_configured"

    latest_run = hook.get_first(
        """
        SELECT state
        FROM dag_run
        WHERE dag_id = %s
        ORDER BY run_after DESC NULLS LAST, logical_date DESC NULLS LAST
        LIMIT 1;
        """,
        parameters=(dag_id,),
    )

    if latest_run is None or latest_run[0] is None:
        return "missing"

    state = str(latest_run[0]).lower()

    if state == "success":
        return "success"

    if state in {"running", "queued", "scheduled"}:
        return "running"

    return "failed"


def calculate_age_hours(latest_timestamp: datetime | None) -> float | None:
    """Calculate how many hours old the latest story timestamp is."""

    if latest_timestamp is None:
        return None

    if latest_timestamp.tzinfo is None:
        latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)

    return (
        datetime.now(timezone.utc) - latest_timestamp
    ).total_seconds() / 3600.0


def upsert_quality_result(
    check_name: str,
    source_name: str,
    passed: bool,
    observed_value: float | int | None,
    expected_value: str,
    details: dict[str, Any],
) -> dict[str, Any]:
    """Insert or update one daily quality result idempotently.

    The result is stored in audit.data_quality_result so DAG03 has an audit
    trail and DAG04 can later read the latest result for Telegram reporting.
    """

    hook = get_warehouse_hook()

    hook.run(
        """
        INSERT INTO audit.data_quality_result (
            check_date,
            check_name,
            source_name,
            passed,
            observed_value,
            expected_value,
            details,
            checked_at
        )
        VALUES (
            CURRENT_DATE,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s::jsonb,
            NOW()
        )
        ON CONFLICT (check_date, check_name, source_name)
        DO UPDATE SET
            passed = EXCLUDED.passed,
            observed_value = EXCLUDED.observed_value,
            expected_value = EXCLUDED.expected_value,
            details = EXCLUDED.details,
            checked_at = NOW();
        """,
        parameters=(
            check_name,
            source_name,
            passed,
            observed_value,
            expected_value,
            json.dumps(details),
        ),
    )

    return {
        "check_name": check_name,
        "source_name": source_name,
        "passed": passed,
        "observed_value": observed_value,
        "expected_value": expected_value,
        "details": details,
    }


def run_freshness_check(
    source_name: str,
    latest_timestamp_sql: str,
    freshness_column: str,
) -> dict[str, Any]:
    """Check that the newest story for a source is no older than 2 hours."""

    hook = get_warehouse_hook()
    row = hook.get_first(latest_timestamp_sql)

    latest_timestamp = row[0] if row and row[0] is not None else None
    age_hours = calculate_age_hours(latest_timestamp)
    passed = age_hours is not None and age_hours <= 2

    return upsert_quality_result(
        check_name="freshness_check",
        source_name=source_name,
        passed=passed,
        observed_value=age_hours,
        expected_value="most recent story age <= 2 hours",
        details={
            "latest_timestamp": (
                latest_timestamp.isoformat()
                if isinstance(latest_timestamp, datetime)
                else None
            ),
            "measured_at": datetime.now(timezone.utc).isoformat(),
            "freshness_column": freshness_column,
        },
    )


def run_row_count_anomaly_check(
    source_name: str,
    row_count_sql: str,
    count_column: str,
) -> dict[str, Any]:
    """Check today's row count against 50% of the previous 7-day average."""

    hook = get_warehouse_hook()
    row = hook.get_first(row_count_sql)

    today_count = int(row[0]) if row and row[0] is not None else 0
    seven_day_average = float(row[1]) if row and row[1] is not None else 0.0
    minimum_expected = seven_day_average * 0.5

    passed = (
        today_count > 0
        if seven_day_average == 0
        else today_count >= minimum_expected
    )

    return upsert_quality_result(
        check_name="row_count_anomaly_check",
        source_name=source_name,
        passed=passed,
        observed_value=today_count,
        expected_value="today row count >= 50% of previous 7-day average",
        details={
            "today_count": today_count,
            "seven_day_average": seven_day_average,
            "minimum_expected": minimum_expected,
            "count_column": count_column,
        },
    )


def run_null_rate_check(
    source_name: str,
    null_count_sql: str,
    required_columns: list[str],
    score_mapping: str | None = None,
) -> dict[str, Any]:
    """Check that required fields have zero null values."""

    hook = get_warehouse_hook()
    row = hook.get_first(null_count_sql)

    null_count = int(row[0]) if row and row[0] is not None else 0
    passed = null_count == 0

    details: dict[str, Any] = {
        "required_columns": required_columns,
    }

    if score_mapping:
        details["score_mapping"] = score_mapping

    return upsert_quality_result(
        check_name="null_rate_check",
        source_name=source_name,
        passed=passed,
        observed_value=null_count,
        expected_value="title, url, and score must have 0 nulls",
        details=details,
    )


def run_hackernews_quality_checks() -> list[dict[str, Any]]:
    """Run all HackerNews quality checks required by DAG03."""

    return [
        run_freshness_check(
            source_name="HackerNews",
            latest_timestamp_sql="""
                SELECT MAX(posted_at)
                FROM silver.hackernews_content
                WHERE content_type = 'story';
            """,
            freshness_column="silver.hackernews_content.posted_at",
        ),
        run_row_count_anomaly_check(
            source_name="HackerNews",
            row_count_sql="""
                WITH counts AS (
                    SELECT
                        posted_at::date AS story_date,
                        COUNT(*) AS row_count
                    FROM silver.hackernews_content
                    WHERE content_type = 'story'
                      AND posted_at IS NOT NULL
                    GROUP BY posted_at::date
                ),
                today_count AS (
                    SELECT COALESCE(MAX(row_count), 0) AS row_count
                    FROM counts
                    WHERE story_date = CURRENT_DATE
                ),
                seven_day_average AS (
                    SELECT AVG(row_count)::numeric AS avg_count
                    FROM counts
                    WHERE story_date >= CURRENT_DATE - INTERVAL '7 days'
                      AND story_date < CURRENT_DATE
                )
                SELECT
                    today_count.row_count,
                    seven_day_average.avg_count
                FROM today_count
                CROSS JOIN seven_day_average;
            """,
            count_column="silver.hackernews_content.posted_at::date",
        ),
        run_null_rate_check(
            source_name="HackerNews",
            null_count_sql="""
                SELECT COUNT(*)
                FROM silver.hackernews_content
                WHERE content_type = 'story'
                  AND (
                      title IS NULL
                      OR url IS NULL
                      OR score IS NULL
                  );
            """,
            required_columns=["title", "url", "score"],
        ),
    ]


def run_devto_quality_checks() -> list[dict[str, Any]]:
    """Run all Dev.to quality checks required by DAG03."""

    return [
        run_freshness_check(
            source_name="Dev.to",
            latest_timestamp_sql="""
                SELECT MAX(published_at)
                FROM silver.devto_article;
            """,
            freshness_column="silver.devto_article.published_at",
        ),
        run_row_count_anomaly_check(
            source_name="Dev.to",
            row_count_sql="""
                WITH counts AS (
                    SELECT
                        published_at::date AS article_date,
                        COUNT(*) AS row_count
                    FROM silver.devto_article
                    WHERE published_at IS NOT NULL
                    GROUP BY published_at::date
                ),
                today_count AS (
                    SELECT COALESCE(MAX(row_count), 0) AS row_count
                    FROM counts
                    WHERE article_date = CURRENT_DATE
                ),
                seven_day_average AS (
                    SELECT AVG(row_count)::numeric AS avg_count
                    FROM counts
                    WHERE article_date >= CURRENT_DATE - INTERVAL '7 days'
                      AND article_date < CURRENT_DATE
                )
                SELECT
                    today_count.row_count,
                    seven_day_average.avg_count
                FROM today_count
                CROSS JOIN seven_day_average;
            """,
            count_column="silver.devto_article.published_at::date",
        ),
        run_null_rate_check(
            source_name="Dev.to",
            null_count_sql="""
                SELECT COUNT(*)
                FROM silver.devto_article
                WHERE title IS NULL
                   OR url IS NULL
                   OR reactions_count IS NULL;
            """,
            required_columns=["title", "url", "reactions_count"],
            score_mapping="Dev.to score proxy = reactions_count",
        ),
    ]


def build_status_message(
    hackernews_status: str,
    devto_status: str,
    quality_status: str,
    failed_quality_checks: list[dict[str, Any]] | None = None,
) -> str:
    """Build a daily warehouse status message for alerting and reporting."""

    failed_quality_checks = failed_quality_checks or []

    failed_sources = []

    if hackernews_status != "success":
        failed_sources.append("HackerNews")

    if devto_status != "success":
        failed_sources.append("Dev.to")

    message_parts = []

    if len(failed_sources) == 2:
        message_parts.append(
            "Both HackerNews and Dev.to upstream DAGs failed, are missing, "
            "or have not completed successfully."
        )
    elif failed_sources:
        message_parts.append(
            f"{failed_sources[0]} upstream DAG failed, is missing, "
            "or has not completed successfully."
        )

    if failed_quality_checks:
        failed_check_names = ", ".join(
            f"{result['source_name']} {result['check_name']}"
            for result in failed_quality_checks
        )
        message_parts.append(f"Failed quality checks: {failed_check_names}.")

    if not message_parts and quality_status == "success":
        message_parts.append("Daily quality checks completed successfully.")

    if not message_parts:
        message_parts.append("One or more data quality checks failed.")

    return " ".join(message_parts)


def upsert_daily_pipeline_status(
    hackernews_status: str,
    devto_status: str,
    quality_status: str,
    message: str,
) -> None:
    """Insert or update today's overall pipeline status idempotently.

    This table acts as the daily summary table for DAG03 and will be useful for
    DAG04 when composing Telegram notification messages.
    """

    hook = get_warehouse_hook()

    hook.run(
        """
        INSERT INTO audit.daily_pipeline_status (
            status_date,
            hackernews_status,
            devto_status,
            quality_status,
            message,
            checked_at
        )
        VALUES (
            CURRENT_DATE,
            %s,
            %s,
            %s,
            %s,
            NOW()
        )
        ON CONFLICT (status_date)
        DO UPDATE SET
            hackernews_status = EXCLUDED.hackernews_status,
            devto_status = EXCLUDED.devto_status,
            quality_status = EXCLUDED.quality_status,
            message = EXCLUDED.message,
            checked_at = NOW();
        """,
        parameters=(
            hackernews_status,
            devto_status,
            quality_status,
            message,
        ),
    )
REPORT_SCHEMA = os.environ.get("REPORT_SCHEMA", "tech_news")

HACKERNEWS_CONTENT_TABLE = os.environ.get(
    "HACKERNEWS_CONTENT_TABLE",
    f"{REPORT_SCHEMA}.hackernews_content",
)

DEVTO_ARTICLE_TABLE = os.environ.get(
    "DEVTO_ARTICLE_TABLE",
    f"{REPORT_SCHEMA}.devto_article",
)


def _validate_relation_name(relation_name: str) -> str:
    """Validate a SQL relation name before safely placing it into SQL text."""

    pattern = r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$"

    if not re.match(pattern, relation_name):
        raise ValueError(f"Unsafe SQL relation name: {relation_name}")

    return relation_name


def _extract_domain(url: str | None) -> str | None:
    """Extract a clean domain name from a URL."""

    if not url:
        return None

    parsed_url = urlparse(url)

    if not parsed_url.netloc:
        return None

    return parsed_url.netloc.removeprefix("www.").lower()





def run_quality_gate(
    upstream_statuses: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run DAG03's full quality gate.

    The quality gate checks same-day upstream DAG status, runs source-specific
    warehouse quality checks when the upstream DAG succeeded, stores audit
    results, and returns a structured dictionary for XCom and alerting.
    """

    upstream_statuses = upstream_statuses or {}

    hackernews_status = upstream_statuses.get(
        "hackernews_status",
        get_latest_dag_status(HACKERNEWS_DAG_ID),
    )
    devto_status = upstream_statuses.get(
        "devto_status",
        get_latest_dag_status(DEVTO_DAG_ID),
    )

    quality_results: list[dict[str, Any]] = []

    if hackernews_status == "success":
        quality_results.extend(run_hackernews_quality_checks())

    if devto_status == "success":
        quality_results.extend(run_devto_quality_checks())

    failed_quality_checks = [
        result for result in quality_results if not result["passed"]
    ]

    operational_failures = [
        source_name
        for source_name, status in {
            "HackerNews": hackernews_status,
            "Dev.to": devto_status,
        }.items()
        if status != "success"
    ]

    if operational_failures or failed_quality_checks:
        quality_status = "failed"
    else:
        quality_status = "success"

    message = build_status_message(
        hackernews_status=hackernews_status,
        devto_status=devto_status,
        quality_status=quality_status,
        failed_quality_checks=failed_quality_checks,
    )

    upsert_daily_pipeline_status(
        hackernews_status=hackernews_status,
        devto_status=devto_status,
        quality_status=quality_status,
        message=message,
    )

    result = {
        "quality_check_date": upstream_statuses.get("quality_check_date"),
        "hackernews_status": hackernews_status,
        "devto_status": devto_status,
        "quality_status": quality_status,
        "message": message,
        "quality_results": quality_results,
        "failed_quality_checks": failed_quality_checks,
        "operational_failures": operational_failures,
        "alert_required": bool(operational_failures or failed_quality_checks),
    }

    quality_hook = TechNewsQualityHook(postgres_conn_id="tech_news_postgres")
    result["daily_report_snapshot"] = quality_hook.build_daily_report_snapshot(
        report_schema="gold",
        hackernews_schema="silver",
        hackernews_table="hackernews_content",
        devto_schema="silver",
        devto_table="devto_article",
    )

    return result