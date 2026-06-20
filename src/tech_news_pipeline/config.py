"""Shared pipeline configuration."""
import airflow
from airflow.sdk import Variable


HACKERNEWS_SOURCE_NAME = "hackernews"
DEVTO_SOURCE_NAME = "devto"

REQUIRED_DEVTO_TAGS = (
    "python",
    "javascript",
    "ai",
    "machinelearning",
    "dataengineering",
    "webdev",
    "devops",
    "react",
)


def get_variable(name: str) -> str:
    """Return a required Airflow variable."""

    value = Variable.get(name, default=None)

    if not value:
        raise ValueError(
            f"Required Airflow variable is missing: {name}"
        )

    return value