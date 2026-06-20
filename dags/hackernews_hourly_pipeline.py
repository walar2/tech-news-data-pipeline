"""Hourly Hacker News ingestion from public API to the Gold warehouse."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from airflow.sdk import DAG, task, task_group


SOURCE_CODE_DIR = Path("/opt/airflow/src/tech_news_pipeline")

ALERT_FILE = SOURCE_CODE_DIR / "alert.py"
CLIENTS_FILE = SOURCE_CODE_DIR / "clients.py"
CONFIG_FILE = SOURCE_CODE_DIR / "config.py"
WAREHOUSE_FILE = SOURCE_CODE_DIR / "warehouse.py"


def load_python_file(module_name: str, file_path: Path):
    """Load one Python file directly from the mounted source directory."""
    if not file_path.exists():
        raise FileNotFoundError(f"Required file was not found:\n{file_path}")

    spec = importlib.util.spec_from_file_location(module_name, file_path)

    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load Python file:\n{file_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


alert = load_python_file("pipeline_alert", ALERT_FILE)
clients = load_python_file("pipeline_clients", CLIENTS_FILE)
config = load_python_file("pipeline_config", CONFIG_FILE)
warehouse = load_python_file("pipeline_warehouse", WAREHOUSE_FILE)

notify_failure = alert.notify_failure
fetch_hackernews_data = clients.fetch_hackernews_data
get_variable = config.get_variable
load_hackernews_bronze = warehouse.load_hackernews_bronze
transform_hackernews_batch_to_silver = warehouse.transform_hackernews_batch_to_silver
load_hackernews_gold = warehouse.load_hackernews_gold


DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_failure,
}


with DAG(
    dag_id="hackernews_hourly_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["hackernews", "hourly", "bronze", "silver", "gold"],
) as dag:

    @task
    def extract() -> dict:
        """Fetch Hacker News top/best feed IDs and story details into a temporary file."""
        batch_id = str(uuid4())

        responses = asyncio.run(
            fetch_hackernews_data(
                base_url=get_variable("hackernews_base_url"),
                concurrency=10,
                include_comments=False,
                include_users=False,
            )
        )

        path = Path(tempfile.gettempdir()) / f"hackernews-{batch_id}.json"
        path.write_text(json.dumps(responses), encoding="utf-8")

        return {
            "batch_id": batch_id,
            "path": str(path),
            "response_count": len(responses),
        }

    @task
    def validate(metadata: dict) -> dict:
        """Validate that Hacker News extraction returned required feeds and story details."""
        responses = json.loads(Path(metadata["path"]).read_text(encoding="utf-8"))

        feed_count = sum(
            response["endpoint"] in {"topstories.json", "beststories.json"}
            for response in responses
        )

        item_count = sum(
            response["endpoint"].startswith("item/")
            for response in responses
        )

        if feed_count < 2:
            raise ValueError(
                f"Expected topstories and beststories feeds, received {feed_count}"
            )

        if item_count == 0:
            raise ValueError("Expected at least one Hacker News story detail response")

        metadata["validation"] = {
            "feed_count": feed_count,
            "item_count": item_count,
            "response_count": len(responses),
        }

        return metadata

    @task_group(group_id="processing")
    def processing(metadata: dict) -> None:
        """Load Hacker News data through Bronze, Silver, and Gold warehouse layers."""

        @task
        def load_bronze_task(metadata: dict) -> dict:
            """Load validated Hacker News raw API responses into Bronze."""
            responses = json.loads(Path(metadata["path"]).read_text(encoding="utf-8"))
            metadata["bronze_count"] = load_hackernews_bronze(
                metadata["batch_id"],
                responses,
            )
            return metadata

        @task
        def transform_silver_task(metadata: dict) -> dict:
            """Transform Hacker News Bronze responses into Silver content tables."""
            metadata["silver_count"] = transform_hackernews_batch_to_silver(
                metadata["batch_id"]
            )
            return metadata

        @task(task_id="load_gold")
        def load_gold_task(metadata: dict) -> dict:
            """Load valid Silver Hacker News stories into Gold fact tables."""
            metadata["gold_count"] = load_hackernews_gold(metadata["batch_id"])
            Path(metadata["path"]).unlink(missing_ok=True)
            return metadata

        load_gold_task(transform_silver_task(load_bronze_task(metadata)))

    processing(validate(extract()))