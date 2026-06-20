import asyncio
import os

import aiohttp


FEEDS = ("topstories", "newstories", "beststories")


async def check_feed(session: aiohttp.ClientSession, base_url: str, feed: str) -> None:
    """Check one Hacker News feed endpoint."""
    url = f"{base_url}/{feed}.json"
    print("=" * 80)
    print(f"Checking: {url}")
    try:
        async with session.get(url) as response:
            print("Status:", response.status)
            response.raise_for_status()
            payload = await response.json()
            print("Count:", len(payload))
            print("Sample:", payload[:5])
    except Exception as error:
        print(type(error).__name__)
        print(error)


async def main() -> None:
    """Check all required Hacker News feed endpoints one at a time."""
    base_url = (
        os.getenv("HACKERNEWS_BASE_URL")
        or os.getenv("AIRFLOW_VAR_HACKERNEWS_BASE_URL")
        or "https://hacker-news.firebaseio.com/v0"
    ).rstrip("/")

    timeout = aiohttp.ClientTimeout(total=60)
    headers = {"User-Agent": "tech-news-data-pipeline/1.0"}

    async with aiohttp.ClientSession(
        timeout=timeout,
        headers=headers,
        trust_env=True,
    ) as session:
        for feed in FEEDS:
            await check_feed(session, base_url, feed)


if __name__ == "__main__":
    asyncio.run(main())