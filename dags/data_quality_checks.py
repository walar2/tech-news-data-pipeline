"""Daily data quality checks for the tech news warehouse."""

from __future__ import annotations

import importlib.util
import os
import smtplib
import ssl
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import pendulum
import time
from datetime import date
from airflow.providers.postgres.hooks.postgres import PostgresHook


try:
    from airflow.sdk import DAG, Variable, task, get_current_context
except ImportError:
    from airflow import DAG
    from airflow.decorators import task, get_current_context
    from airflow.models import Variable

try:
    from airflow.utils.email import send_email
except ImportError:
    send_email = None


PIPELINE_SRC_DIR = Path(
    os.environ.get(
        "PIPELINE_SRC_DIR",
        "/opt/airflow/src/tech_news_pipeline",
    )
)

QUALITY_FILE = PIPELINE_SRC_DIR / "quality.py"


def load_python_file(module_name: str, file_path: Path):
    """Load a Python file directly from the mounted project source folder.

    This avoids package import issues inside Airflow containers by loading the
    required source file from /opt/airflow/src/tech_news_pipeline.
    """

    if not file_path.exists():
        raise FileNotFoundError(f"Required file was not found:\n{file_path}")

    spec = importlib.util.spec_from_file_location(module_name, file_path)

    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from:\n{file_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


def get_airflow_variable(name: str, default_value: str = "") -> str:
    """Read an Airflow Variable safely across Airflow versions."""

    try:
        return Variable.get(name, default=default_value)
    except TypeError:
        return Variable.get(name, default_var=default_value)


quality = load_python_file(
    module_name="pipeline_quality",
    file_path=QUALITY_FILE,
)


with DAG(
    dag_id="data_quality_checks",
    description=(
        "Runs daily data quality checks after HackerNews and Dev.to ingestion."
    ),
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    tags=["quality", "audit", "devto", "hackernews"],
    default_args={
        "owner": "airflow",
        "retries": 0,
    },
) as dag:
    def send_smtp_email(
            to_email: str,
            subject: str,
            html_content: str,
    ) -> None:
        """Send an HTML email using SMTP settings from environment variables.

        This project uses a custom SMTP sender because the local Docker network may
        present a self-signed certificate chain when Airflow's built-in email
        helper starts TLS.
        """

        smtp_host = os.environ.get("AIRFLOW__SMTP__SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.environ.get("AIRFLOW__SMTP__SMTP_PORT", "587"))
        smtp_user = os.environ["AIRFLOW__SMTP__SMTP_USER"]
        smtp_password = os.environ["AIRFLOW__SMTP__SMTP_PASSWORD"]
        smtp_from = os.environ.get("AIRFLOW__SMTP__SMTP_MAIL_FROM", smtp_user)

        message = MIMEText(html_content, "html")
        message["Subject"] = subject
        message["From"] = smtp_from
        message["To"] = to_email

        ssl_context = ssl._create_unverified_context()

        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as smtp_server:
            smtp_server.starttls(context=ssl_context)
            smtp_server.login(smtp_user, smtp_password)
            smtp_server.sendmail(
                smtp_from,
                [to_email],
                message.as_string(),
            )
    @task()
    def check_upstream_dag_runs_task() -> dict[str, Any]:
        """Check DAG01 and DAG02 run status for the quality-check date.

        This custom Python sensor checks Airflow metadata directly. It waits
        briefly for same-day upstream DAG runs to reach a terminal state before
        DAG03 runs the warehouse quality checks.
        """

        context = get_current_context()
        logical_date = context["logical_date"]

        quality_check_date = logical_date.date()

        metadata_hook = PostgresHook(postgres_conn_id="airflow_db")

        upstream_dag_ids = {
            "hackernews_status": "hackernews_hourly_pipeline",
            "devto_status": "devto_daily_pipeline",
        }

        max_wait_seconds = 300
        poke_interval_seconds = 30
        started_at = time.monotonic()

        def get_same_day_status(dag_id: str, target_date: date) -> str:
            """Return latest same-day DAG run status from Airflow metadata."""

            dag_exists = metadata_hook.get_first(
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

            latest_run = metadata_hook.get_first(
                """
                SELECT state
                FROM dag_run
                WHERE dag_id = %s
                  AND (
                      logical_date::date = %s
                      OR run_after::date = %s
                  )
                ORDER BY run_after DESC NULLS LAST,
                         logical_date DESC NULLS LAST
                LIMIT 1;
                """,
                parameters=(dag_id, target_date, target_date),
            )

            if latest_run is None or latest_run[0] is None:
                return "missing"

            state = str(latest_run[0]).lower()

            if state == "success":
                return "success"

            if state in {"running", "queued", "scheduled"}:
                return "running"

            return "failed"

        while True:
            upstream_statuses = {
                status_key: get_same_day_status(dag_id, quality_check_date)
                for status_key, dag_id in upstream_dag_ids.items()
            }

            terminal_statuses = {
                "success",
                "failed",
                "missing",
                "not_configured",
            }

            if all(
                    status in terminal_statuses
                    for status in upstream_statuses.values()
            ):
                break

            if time.monotonic() - started_at >= max_wait_seconds:
                break

            time.sleep(poke_interval_seconds)

        return {
            "quality_check_date": quality_check_date.isoformat(),
            **upstream_statuses,
        }


    @task()
    def run_quality_gate_task(
            upstream_statuses: dict[str, Any],
    ) -> dict[str, Any]:
        """Run warehouse quality checks and write results into audit tables.

        The upstream status dictionary is passed through XCom from the custom
        Python sensor task. The returned quality result is passed to the alert
        task through XCom.
        """

        return quality.run_quality_gate(
            upstream_statuses=upstream_statuses,
        )

    @task()
    def send_quality_alert_task(quality_result: dict[str, Any]) -> dict[str, Any]:
        """Send an email alert when upstream DAGs or quality checks fail.

        This is the mandatory fallback alert system for DAG03. Telegram alerts
        will be handled later by DAG04.
        """

        if not quality_result.get("alert_required"):
            return {
                "alert_sent": False,
                "reason": "No alert condition found.",
            }

        alert_email_to = (
                get_airflow_variable("alert_email_to", "")
                or os.environ.get("ALERT_EMAIL_TO", "")
        ).strip()

        if not alert_email_to:
            return {
                "alert_sent": False,
                "reason": "alert_email_to Airflow Variable is empty.",
                "message": quality_result.get("message"),
            }



        operational_failures = quality_result.get("operational_failures", [])
        failed_quality_checks = quality_result.get("failed_quality_checks", [])

        if len(operational_failures) == 2:
            subject = "[CRITICAL] Both source ingestion DAGs failed"
        elif len(operational_failures) == 1:
            subject = f"[WARNING] {operational_failures[0]} ingestion DAG failed"
        else:
            subject = "[WARNING] Data quality check failed"

        html_content = f"""
        <h3>Tech News Pipeline Alert</h3>
        <p><strong>Quality status:</strong> {quality_result.get("quality_status")}</p>
        <p><strong>Message:</strong> {quality_result.get("message")}</p>
        <p><strong>HackerNews status:</strong> {quality_result.get("hackernews_status")}</p>
        <p><strong>Dev.to status:</strong> {quality_result.get("devto_status")}</p>
        <p><strong>Operational failures:</strong> {operational_failures}</p>
        <p><strong>Failed quality checks:</strong> {failed_quality_checks}</p>
        """

        try:
            send_smtp_email(
                to_email=alert_email_to,
                subject=subject,
                html_content=html_content,
            )
        except Exception as error:
            return {
                "alert_sent": False,
                "reason": f"email_send_failed: {type(error).__name__}: {error}",
                "subject": subject,
                "to": alert_email_to,
            }

        return {
            "alert_sent": True,
            "subject": subject,
            "to": alert_email_to,
        }


    upstream_statuses = check_upstream_dag_runs_task()
    quality_result = run_quality_gate_task(upstream_statuses)

    send_quality_alert_task(quality_result)