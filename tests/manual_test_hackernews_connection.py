import asyncio
import os

import aiohttp


async def main() -> None:
    """Check Hacker News connectivity using the same aiohttp style as DAG01."""
    base_url = (
        os.getenv("HACKERNEWS_BASE_URL")
        or os.getenv("AIRFLOW_VAR_HACKERNEWS_BASE_URL")
        or "https://hacker-news.firebaseio.com/v0"
    ).rstrip("/")

    url = f"{base_url}/topstories.json"
    print(f"Checking: {url}")

    timeout = aiohttp.ClientTimeout(total=60)
    headers = {"User-Agent": "tech-news-data-pipeline/1.0"}

    try:
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            async with session.get(url) as response:
                print("Status:", response.status)
                response.raise_for_status()
                data = await response.json()
                print("Count:", len(data))
                print("Sample:", data[:5])
    except Exception as error:
        print(type(error).__name__)
        print(error)


if __name__ == "__main__":
    asyncio.run(main())