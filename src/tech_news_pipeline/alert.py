"""Shared Airflow failure alert callback."""

from __future__ import annotations

import requests
from airflow.sdk import Variable


def notify_failure(context) -> None:
    """Send task failure details to the configured Slack webhook, if available."""
    webhook = Variable.get("alert_slack_webhook", default="")

    task_instance = context.get("task_instance")
    message = (
        f"Tech news pipeline failure: "
        f"DAG={task_instance.dag_id}, "
        f"task={task_instance.task_id}, "
        f"run={task_instance.run_id}"
    )

    if not webhook:
        print(f"No alert_slack_webhook configured. {message}")
        return

    response = requests.post(
        webhook,
        json={"text": message},
        timeout=30,
    )
    response.raise_for_status()