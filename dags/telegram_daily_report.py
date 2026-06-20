"""Daily Telegram report DAG for the tech news warehouse.

This DAG waits for DAG03 data_quality_checks to finish successfully, builds a
daily HackerNews summary report, sends the summary text to Telegram, and attaches
a CSV file containing the full HackerNews dataset from the past 24 hours.
"""

from __future__ import annotations

import csv
import os
from datetime import timedelta
from html import escape
from pathlib import Path
from typing import Any

import pendulum
import requests

try:
    from airflow.sdk import DAG, task
except ImportError:
    from airflow import DAG
    from airflow.decorators import task



try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:
    PostgresHook = None


HACKERNEWS_TABLE = os.environ.get(
    "HACKERNEWS_SILVER_TABLE",
    "silver.hackernews_content",
)

POSTGRES_CONN_ID = os.environ.get("POSTGRES_CONN_ID", "tech_news_postgres")

REPORT_OUTPUT_DIR = Path("/tmp/telegram_reports")


def get_warehouse_connection():
    """Return a PostgreSQL connection for reading warehouse report data."""

    if PostgresHook is None:
        raise RuntimeError("PostgresHook is unavailable in this Airflow environment.")

    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


def require_env(name: str) -> str:
    """Return a required environment variable or raise a clear error."""

    value = os.environ.get(name, "").strip()

    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")

    return value


with DAG(
    dag_id="telegram_daily_report",
    description="Builds and sends the daily tech news report to Telegram.",
    schedule="30 1 * * *",  # 01:30 UTC = 08:00 Myanmar time
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    tags=["telegram", "report", "hackernews", "gold"],
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    @task()
    def check_same_day_data_quality_success() -> dict[str, str]:
        """Check that data_quality_checks produced today's report snapshot."""

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        report_date = pendulum.now("Asia/Yangon").date()

        hook = PostgresHook(postgres_conn_id="tech_news_postgres")

        result = hook.get_first(
            """
            SELECT
                EXISTS (
                    SELECT 1
                    FROM audit.daily_pipeline_status
                    WHERE status_date = %s
                ) AS has_quality_status,
                EXISTS (
                    SELECT 1
                    FROM gold.daily_report_summary_metrics
                    WHERE report_date = %s
                ) AS has_report_snapshot
            """,
            parameters=(report_date, report_date),
        )

        has_quality_status = bool(result[0])
        has_report_snapshot = bool(result[1])

        if not has_quality_status:
            raise RuntimeError(
                f"No audit.daily_pipeline_status row found for {report_date}. "
                "Run data_quality_checks.py first."
            )

        if not has_report_snapshot:
            raise RuntimeError(
                f"No gold.daily_report_summary_metrics row found for {report_date}. "
                "data_quality_checks.py has not produced today's report snapshot."
            )

        return {
            "report_date": str(report_date),
            "data_quality_status_found": str(has_quality_status),
            "report_snapshot_found": str(has_report_snapshot),
        }




    @task()
    def build_daily_telegram_report() -> dict[str, Any]:
        """Build Telegram message and 4-sheet XLSX from data_quality_checks output."""

        import zipfile
        from datetime import datetime
        from xml.sax.saxutils import escape as xml_escape

        report_date = pendulum.now("Asia/Yangon").date()
        report_date_text = str(report_date)

        REPORT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        with get_warehouse_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT title, author, score, comments_count, url, posted_at
                    FROM gold.daily_report_top_stories
                    WHERE report_date = %s
                    ORDER BY score DESC NULLS LAST, comments_count DESC NULLS LAST
                    """,
                    (report_date,),
                )
                top_stories = cursor.fetchall()

                cursor.execute(
                    """
                    SELECT domain, share_count, total_score
                    FROM gold.daily_report_trending_domains
                    WHERE report_date = %s
                    ORDER BY share_count DESC, total_score DESC
                    LIMIT 20
                    """,
                    (report_date,),
                )
                trending_domains = cursor.fetchall()

                cursor.execute(
                    """
                    SELECT title, tags, reactions, comments, url, published_at
                    FROM gold.daily_report_top_devto_articles
                    WHERE report_date = %s
                    ORDER BY reactions DESC NULLS LAST, comments DESC NULLS LAST
                    LIMIT 20
                    """,
                    (report_date,),
                )
                top_devto_articles = cursor.fetchall()

                cursor.execute(
                    """
                    SELECT story_count, avg_score, avg_comments, unique_authors, unique_domains
                    FROM gold.daily_report_summary_metrics
                    WHERE report_date = %s
                    """,
                    (report_date,),
                )
                summary = cursor.fetchone()

        if summary is None:
            raise RuntimeError(
                f"No gold.daily_report_summary_metrics found for {report_date_text}. "
                "Run data_quality_checks.py first."
            )

        story_count, avg_score, avg_comments, unique_authors, unique_domains = summary

        lines = [
            f"📰 <b>Daily Tech News Report — {escape(report_date_text)}</b>",
            "",
            "<b>Top 10 HackerNews stories from the past 24 hours</b>",
            "",
        ]

        if not top_stories:
            lines.append("No HackerNews stories found in the past 24 hours.")
        else:
            for rank, story in enumerate(top_stories[:10], start=1):
                title, author, score, comments_count, url, posted_at = story

                lines.extend(
                    [
                        f'{rank}. <a href="{escape(url or "", quote=True)}">{escape(title or "[No title]")}</a>',
                        (
                            f"   Author: {escape(author or 'unknown')} | "
                            f"Score: {score or 0} | Comments: {comments_count or 0}"
                        ),
                        "",
                    ]
                )

        lines.extend(
            [
                "<b>Summary</b>",
                f"Total stories ingested in past 24 hours: {story_count}",
                f"Average score: {avg_score}",
                f"Average comments: {avg_comments}",
                f"Unique authors: {unique_authors}",
                f"Unique domains: {unique_domains}",
                "",
                "📎 See the attached XLSX file for the full daily dataset.",
            ]
        )

        message_text = "\n".join(lines)

        xlsx_path = REPORT_OUTPUT_DIR / f"daily_report_{report_date_text}.xlsx"

        sheets = [
            (
                "Top Stories",
                ["title", "author", "score", "comments_count", "url", "posted_at"],
                top_stories,
            ),
            (
                "Trending Domains",
                ["domain", "share_count", "total_score"],
                trending_domains,
            ),
            (
                "Top Dev.to Articles",
                ["title", "tags", "reactions", "comments", "url", "published_at"],
                top_devto_articles,
            ),
            (
                "Daily Summary Metrics",
                ["metric", "value"],
                [
                    ("story_count", story_count),
                    ("avg_score", avg_score),
                    ("avg_comments", avg_comments),
                    ("unique_authors", unique_authors),
                    ("unique_domains", unique_domains),
                ],
            ),
        ]

        def excel_column_name(column_number: int) -> str:
            """Convert a 1-based column number to an Excel column name."""

            name = ""

            while column_number:
                column_number, remainder = divmod(column_number - 1, 26)
                name = chr(65 + remainder) + name

            return name

        def cell_xml(row_number: int, column_number: int, value: Any) -> str:
            """Return one XLSX worksheet cell as XML."""

            cell_ref = f"{excel_column_name(column_number)}{row_number}"

            if value is None:
                return f'<c r="{cell_ref}" t="inlineStr"><is><t></t></is></c>'

            if isinstance(value, bool):
                return f'<c r="{cell_ref}" t="b"><v>{1 if value else 0}</v></c>'

            if isinstance(value, (int, float)):
                return f'<c r="{cell_ref}"><v>{value}</v></c>'

            if isinstance(value, datetime):
                value = value.replace(tzinfo=None).isoformat(sep=" ")

            text = xml_escape(str(value))

            return f'<c r="{cell_ref}" t="inlineStr"><is><t>{text}</t></is></c>'

        def worksheet_xml(headers: list[str], rows: list[tuple[Any, ...]]) -> str:
            """Build one worksheet XML document."""

            all_rows = [tuple(headers), *rows]
            row_xml_parts = []

            for row_index, row in enumerate(all_rows, start=1):
                cells = [
                    cell_xml(row_index, column_index, value)
                    for column_index, value in enumerate(row, start=1)
                ]
                row_xml_parts.append(
                    f'<row r="{row_index}">{"".join(cells)}</row>'
                )

            return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
        <sheetData>
            {"".join(row_xml_parts)}
        </sheetData>
    </worksheet>
    """

        workbook_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
              xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
        <sheets>
            <sheet name="Top Stories" sheetId="1" r:id="rId1"/>
            <sheet name="Trending Domains" sheetId="2" r:id="rId2"/>
            <sheet name="Top Dev.to Articles" sheetId="3" r:id="rId3"/>
            <sheet name="Daily Summary Metrics" sheetId="4" r:id="rId4"/>
        </sheets>
    </workbook>
    """

        workbook_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
        <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
        <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>
        <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet3.xml"/>
        <Relationship Id="rId4" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet4.xml"/>
    </Relationships>
    """

        root_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
        <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
    </Relationships>
    """

        content_types_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
        <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
        <Default Extension="xml" ContentType="application/xml"/>
        <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
        <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
        <Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
        <Override PartName="/xl/worksheets/sheet3.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
        <Override PartName="/xl/worksheets/sheet4.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
    </Types>
    """

        with zipfile.ZipFile(xlsx_path, "w", zipfile.ZIP_DEFLATED) as xlsx:
            xlsx.writestr("[Content_Types].xml", content_types_xml)
            xlsx.writestr("_rels/.rels", root_rels_xml)
            xlsx.writestr("xl/workbook.xml", workbook_xml)
            xlsx.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)

            for index, sheet in enumerate(sheets, start=1):
                sheet_name, headers, rows = sheet
                xlsx.writestr(
                    f"xl/worksheets/sheet{index}.xml",
                    worksheet_xml(headers, rows),
                )

        return {
            "report_date": report_date_text,
            "message_text": message_text,
            "xlsx_path": str(xlsx_path),
            "total_stories": story_count,
        }

    @task()
    def send_telegram_report(report_payload: dict[str, Any]) -> dict[str, Any]:
        """Send the daily report message and CSV attachment to Telegram."""

        token = require_env("TELEGRAM_BOT_TOKEN")
        chat_id = require_env("TELEGRAM_CHAT_ID")

        message_response = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": report_payload["message_text"],
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        message_response.raise_for_status()

        xlsx_path = Path(report_payload["xlsx_path"])

        with xlsx_path.open("rb") as file:
            document_response = requests.post(
                f"https://api.telegram.org/bot{token}/sendDocument",
                data={
                    "chat_id": chat_id,
                    "caption": (
                        f"Full daily tech news report for "
                        f"{report_payload['report_date']}."
                    ),
                },
                files={
                    "document": (
                        xlsx_path.name,
                        file,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                },
                timeout=60,
            )

        document_response.raise_for_status()

        return {
            "message_sent": True,
            "document_sent": True,
            "chat_id": chat_id,
            "total_stories": report_payload["total_stories"],
            "xlsx_path": report_payload["xlsx_path"],
        }


    data_quality_status = check_same_day_data_quality_success()
    report_payload = build_daily_telegram_report()
    send_result = send_telegram_report(report_payload)

    data_quality_status >> report_payload >> send_result