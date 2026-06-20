import asyncio
import importlib.util
import json
import sys
from pathlib import Path


CLIENTS_FILE = Path(
    r"C:\Users\minkh\Desktop\Mytel-project\pythonProject1\tech-news-data-pipeline\src\tech_news_pipeline\clients.py"
)

if not CLIENTS_FILE.exists():
    raise FileNotFoundError(
        f"clients.py was not found at this path:\n{CLIENTS_FILE}"
    )

sys.path.insert(0, str(CLIENTS_FILE.parent))

spec = importlib.util.spec_from_file_location("clients", CLIENTS_FILE)
clients = importlib.util.module_from_spec(spec)
spec.loader.exec_module(clients)


async def main():
    print("Loaded clients.py from:")
    print(CLIENTS_FILE)

    try:
        hn_responses = await clients.fetch_hackernews_data(
            base_url="https://hacker-news.firebaseio.com/v0",
            max_items_per_feed=1,
            concurrency=2,
            include_comments=False,
        )

        hn_counts = {
            "feeds": 0,
            "items": 0,
            "users": 0,
        }

        for response in hn_responses:
            endpoint = response["endpoint"]

            if endpoint in {
                "topstories.json",
                "newstories.json",
                "beststories.json",
            }:
                hn_counts["feeds"] += 1
            elif endpoint.startswith("item/"):
                hn_counts["items"] += 1
            elif endpoint.startswith("user/"):
                hn_counts["users"] += 1

        print("\nHacker News:")
        print(json.dumps(hn_counts, indent=2))
        print(f"Total Hacker News responses: {len(hn_responses)}")

    except Exception as error:
        print("\nHacker News test failed.")
        print("This is likely a network timeout to hacker-news.firebaseio.com.")
        print(type(error).__name__)
        print(error)

    devto_result = clients.fetch_devto_data(
        base_url="https://dev.to/api",
        latest_pages=1,
        concurrency=2,
    )

    if asyncio.iscoroutine(devto_result):
        devto_responses = await devto_result
    else:
        devto_responses = devto_result

    devto_counts = {
        "top": 0,
        "tags": 0,
        "latest": 0,
        "details": 0,
    }

    for response in devto_responses:
        endpoint = response["endpoint"]

        if endpoint.startswith("articles?per_page=50&top=7"):
            devto_counts["top"] += 1
        elif endpoint.startswith("articles?tag="):
            devto_counts["tags"] += 1
        elif endpoint.startswith("articles/latest"):
            devto_counts["latest"] += 1
        elif endpoint.startswith("articles/"):
            devto_counts["details"] += 1

    print("\nDEV.to:")
    print(json.dumps(devto_counts, indent=2))
    print(f"Total DEV.to responses: {len(devto_responses)}")


if __name__ == "__main__":
    asyncio.run(main())