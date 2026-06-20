"""DEV.to pipeline from API to Bronze, Silver, and shared Gold."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from airflow.sdk import DAG, Variable, task


SOURCE_CODE_DIR = Path("/opt/airflow/src/tech_news_pipeline")

CLIENTS_FILE = SOURCE_CODE_DIR / "clients.py"
WAREHOUSE_FILE = SOURCE_CODE_DIR / "warehouse.py"


def load_python_file(module_name: str, file_path: Path):
    """Load a Python file directly without relying on package imports."""

    if not file_path.exists():
        raise FileNotFoundError(
            f"Required file was not found:\n{file_path}"
        )

    sys.path.insert(0, str(file_path.parent))

    spec = importlib.util.spec_from_file_location(
        module_name,
        file_path,
    )

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


clients = load_python_file(
    module_name="pipeline_clients",
    file_path=CLIENTS_FILE,
)

warehouse = load_python_file(
    module_name="pipeline_warehouse",
    file_path=WAREHOUSE_FILE,
)


fetch_devto_data = clients.fetch_devto_data
load_devto_bronze = warehouse.load_devto_bronze
load_gold = warehouse.load_gold
transform_devto_batch_to_silver = warehouse.transform_devto_batch_to_silver


DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}


with DAG(
    dag_id="devto_daily_pipeline",
    description="Collect DEV.to articles and load Bronze, Silver, and shared Gold.",
    schedule="0 1 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["devto", "bronze", "silver", "gold"],
) as dag:

    @task
    def extract_devto() -> dict:
        """Extract DEV.to top, tag, latest, and detail API responses."""

        batch_id = str(uuid4())

        base_url = Variable.get("devto_base_url")

        latest_pages = int(
            Variable.get(
                "devto_latest_pages",
                default=5,
            )
        )

        concurrency = int(
            Variable.get(
                "devto_concurrency",
                default=10,
            )
        )

        if latest_pages < 5:
            raise ValueError(
                "devto_latest_pages must be at least 5 to collect 500 latest articles."
            )

        responses = asyncio.run(
            fetch_devto_data(
                base_url=base_url,
                latest_pages=latest_pages,
                concurrency=concurrency,
            )
        )

        temp_path = (
            Path(tempfile.gettempdir())
            / f"devto-{batch_id}.json"
        )

        temp_path.write_text(
            json.dumps(responses),
            encoding="utf-8",
        )

        return {
            "batch_id": batch_id,
            "path": str(temp_path),
            "response_count": len(responses),
            "latest_pages": latest_pages,
        }

    @task
    def validate_extract(metadata: dict) -> dict:
        """Validate DEV.to extraction envelopes and article-detail coverage."""

        responses = json.loads(
            Path(metadata["path"]).read_text(encoding="utf-8")
        )

        if not responses:
            raise ValueError("DEV.to extraction returned no responses.")

        malformed_count = sum(
            1
            for response in responses
            if not isinstance(response, dict)
            or "endpoint" not in response
            or "payload" not in response
        )

        if malformed_count:
            raise ValueError(
                f"DEV.to extraction returned {malformed_count} malformed responses."
            )

        top_count = sum(
            1
            for response in responses
            if response["endpoint"].startswith("articles?per_page=50&top=7")
        )

        tag_count = sum(
            1
            for response in responses
            if response["endpoint"].startswith("articles?tag=")
        )

        latest_page_count = sum(
            1
            for response in responses
            if response["endpoint"].startswith("articles/latest")
        )

        detail_count = sum(
            1
            for response in responses
            if response["endpoint"].startswith("articles/")
            and response.get("source_item_id") is not None
        )

        if top_count != 1:
            raise ValueError(f"Expected 1 DEV.to top endpoint response, got {top_count}.")

        if tag_count != 8:
            raise ValueError(f"Expected 8 DEV.to tag endpoint responses, got {tag_count}.")

        if latest_page_count < 5:
            raise ValueError(
                f"Expected at least 5 latest pages for 500 articles, got {latest_page_count}."
            )

        if detail_count < 500:
            raise ValueError(
                f"Expected at least 500 DEV.to article detail responses, got {detail_count}."
            )

        metadata["top_count"] = top_count
        metadata["tag_count"] = tag_count
        metadata["latest_page_count"] = latest_page_count
        metadata["detail_count"] = detail_count

        return metadata

    @task
    def load_bronze_task(metadata: dict) -> dict:
        """Load raw DEV.to API responses into source-specific Bronze."""

        responses = json.loads(
            Path(metadata["path"]).read_text(encoding="utf-8")
        )

        metadata["bronze_count"] = load_devto_bronze(
            batch_id=metadata["batch_id"],
            responses=responses,
        )

        return metadata

    @task
    def transform_silver_task(metadata: dict) -> dict:
        """Transform DEV.to Bronze article details into source-specific Silver."""

        metadata["silver_count"] = transform_devto_batch_to_silver(
            batch_id=metadata["batch_id"],
        )

        if metadata["silver_count"] == 0:
            raise ValueError("No DEV.to records were transformed into Silver.")

        return metadata

    @task
    def load_gold_task(metadata: dict) -> dict:
        """Load valid DEV.to Silver articles into the shared Gold star schema."""

        load_gold(
            batch_id=metadata["batch_id"],
        )

        Path(metadata["path"]).unlink(missing_ok=True)

        return metadata

    load_gold_task(
        transform_silver_task(
            load_bronze_task(
                validate_extract(
                    extract_devto()
                )
            )
        )
    )