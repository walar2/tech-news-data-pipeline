"""API clients for Hacker News and DEV Community."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from typing import Any

import aiohttp


HACKERNEWS_FEEDS = (
    "topstories",

    "beststories",
)


DEVTO_TOP_DAYS = 7
DEVTO_REQUIRED_TAGS = (
    "python",
    "javascript",
    "ai",
    "machinelearning",
    "dataengineering",
    "webdev",
    "devops",
    "react",
)


def create_raw_response(
    endpoint: str,
    payload: Any,
    source_item_id: str | int | None = None,
) -> dict[str, Any]:
    """
    Wrap an unchanged API payload with routing metadata.

    Only payload is stored as raw JSONB in Bronze.
    The metadata helps Airflow route the response.
    """

    return {
        "endpoint": endpoint,
        "source_item_id": str(source_item_id) if source_item_id is not None else None,
        "payload": payload,
    }


async def get_json(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    attempts: int = 3,
) -> Any:
    """Request JSON with bounded concurrency and retry handling."""

    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            async with semaphore:
                async with session.get(url) as response:
                    response.raise_for_status()
                    return await response.json()

        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
            last_error = error

            if attempt == attempts:
                raise

            await asyncio.sleep(2 ** (attempt - 1))

    raise RuntimeError("Request failed") from last_error


# ============================================================
# Hacker News Client
# ============================================================

async def fetch_hackernews_items(
    session: aiohttp.ClientSession,
    base_url: str,
    item_ids: Iterable[int],
    semaphore: asyncio.Semaphore,
) -> list[dict[str, Any]]:
    """Fetch Hacker News item responses concurrently."""

    unique_ids = list(dict.fromkeys(item_ids))

    if not unique_ids:
        return []

    payloads = await asyncio.gather(
        *[
            get_json(
                session=session,
                url=f"{base_url}/item/{item_id}.json",
                semaphore=semaphore,
            )
            for item_id in unique_ids
        ]
    )

    return [
        create_raw_response(
            endpoint=f"item/{item_id}.json",
            payload=payload,
            source_item_id=item_id,
        )
        for item_id, payload in zip(unique_ids, payloads, strict=True)
    ]


async def fetch_hackernews_comments(
    session: aiohttp.ClientSession,
    base_url: str,
    parent_items: list[dict[str, Any]],
    semaphore: asyncio.Semaphore,
) -> list[dict[str, Any]]:
    """
    Recursively fetch Hacker News comments from kids IDs.

    Stories can have comments.
    Comments can also have replies.
    This continues until no unseen kids remain.
    """

    collected_responses: list[dict[str, Any]] = []
    visited_ids: set[int] = set()

    pending_ids = list(
        dict.fromkeys(
            child_id
            for item in parent_items
            if isinstance(item, dict)
            for child_id in item.get("kids", [])
            if isinstance(child_id, int)
        )
    )

    while pending_ids:
        current_ids = [
            item_id
            for item_id in pending_ids
            if item_id not in visited_ids
        ]

        if not current_ids:
            break

        visited_ids.update(current_ids)

        responses = await fetch_hackernews_items(
            session=session,
            base_url=base_url,
            item_ids=current_ids,
            semaphore=semaphore,
        )

        collected_responses.extend(responses)

        pending_ids = [
            child_id
            for response in responses
            if isinstance(response.get("payload"), dict)
            for child_id in response["payload"].get("kids", [])
            if isinstance(child_id, int) and child_id not in visited_ids
        ]

    return collected_responses


async def fetch_hackernews_users(
    session: aiohttp.ClientSession,
    base_url: str,
    items: list[dict[str, Any]],
    semaphore: asyncio.Semaphore,
) -> list[dict[str, Any]]:
    """Fetch every unique Hacker News user profile referenced by collected items."""

    usernames = sorted(
        {
            item["by"]
            for item in items
            if isinstance(item, dict)
            and isinstance(item.get("by"), str)
            and item.get("by")
        }
    )

    if not usernames:
        return []

    profiles = await asyncio.gather(
        *[
            get_json(
                session=session,
                url=f"{base_url}/user/{username}.json",
                semaphore=semaphore,
            )
            for username in usernames
        ]
    )

    return [
        create_raw_response(
            endpoint=f"user/{username}.json",
            payload=profile,
            source_item_id=username,
        )
        for username, profile in zip(usernames, profiles, strict=True)
    ]


async def fetch_hackernews_data(
    base_url: str,
    max_items_per_feed: None = None,
    concurrency: int = 40,
    include_comments: bool = True,
    include_users: bool = True,
) -> list[dict[str, Any]]:
    """
    Fetch all requested Hacker News endpoints.

    Covered endpoints:
    - topstories.json
    - newstories.json
    - beststories.json
    - item/{id}.json
    - user/{username}.json
    """

    base_url = base_url.rstrip("/")

    timeout = aiohttp.ClientTimeout(total=600)
    semaphore = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
        feed_payloads = await asyncio.gather(
            *[
                get_json(
                    session=session,
                    url=f"{base_url}/{feed_name}.json",
                    semaphore=semaphore,
                )
                for feed_name in HACKERNEWS_FEEDS
            ]
        )

        responses = [
            create_raw_response(
                endpoint=f"{feed_name}.json",
                payload=payload,
            )
            for feed_name, payload in zip(
                HACKERNEWS_FEEDS,
                feed_payloads,
                strict=True,
            )
        ]

        feed_item_ids = list(
            dict.fromkeys(
                item_id
                for feed_payload in feed_payloads
                if isinstance(feed_payload, list)
                for item_id in (
                    feed_payload if max_items_per_feed is None else feed_payload[:max_items_per_feed]
                )
                if isinstance(item_id, int)
            )
        )

        item_responses = await fetch_hackernews_items(
            session=session,
            base_url=base_url,
            item_ids=feed_item_ids,
            semaphore=semaphore,
        )

        responses.extend(item_responses)

        item_payloads = [
            response["payload"]
            for response in item_responses
            if isinstance(response.get("payload"), dict)
        ]

        comment_responses: list[dict[str, Any]] = []

        if include_comments:
            comment_responses = await fetch_hackernews_comments(
                session=session,
                base_url=base_url,
                parent_items=item_payloads,
                semaphore=semaphore,
            )

            responses.extend(comment_responses)

        comment_payloads = [
            response["payload"]
            for response in comment_responses
            if isinstance(response.get("payload"), dict)
        ]

        if include_users:
            user_responses = await fetch_hackernews_users(
                session=session,
                base_url=base_url,
                items=[*item_payloads, *comment_payloads],
                semaphore=semaphore,
            )

            responses.extend(user_responses)

    return responses


# ============================================================
# DEV Community Client
# ============================================================

async def fetch_devto_list_endpoint(
    session: aiohttp.ClientSession,
    base_url: str,
    endpoint: str,
    semaphore: asyncio.Semaphore,
) -> dict[str, Any]:
    """Fetch one DEV list endpoint and preserve the list response."""

    payload = await get_json(
        session=session,
        url=f"{base_url}/{endpoint}",
        semaphore=semaphore,
    )

    return create_raw_response(
        endpoint=endpoint,
        payload=payload,
    )


async def fetch_devto_article_details(
    session: aiohttp.ClientSession,
    base_url: str,
    article_ids: Iterable[int],
    semaphore: asyncio.Semaphore,
) -> list[dict[str, Any]]:
    """Fetch DEV article detail responses.

    Some articles returned by the Dev.to list endpoint can later return 404
    from the detail endpoint. Those records are skipped instead of failing
    the whole DAG run.
    """

    unique_ids = list(dict.fromkeys(article_ids))

    if not unique_ids:
        return []

    results = await asyncio.gather(
        *[
            get_json(
                session=session,
                url=f"{base_url}/articles/{article_id}",
                semaphore=semaphore,
            )
            for article_id in unique_ids
        ],
        return_exceptions=True,
    )

    raw_responses: list[dict[str, Any]] = []

    for article_id, result in zip(unique_ids, results, strict=True):
        if isinstance(result, aiohttp.ClientResponseError):
            if result.status == 404:
                print(f"Skipping Dev.to article {article_id}: 404 Not Found")
                continue

            raise result

        if isinstance(result, Exception):
            raise result

        raw_responses.append(
            create_raw_response(
                endpoint=f"articles/{article_id}",
                payload=result,
                source_item_id=article_id,
            )
        )

    return raw_responses


def extract_devto_article_ids(list_responses: list[dict[str, Any]]) -> list[int]:
    """Extract unique article IDs from DEV list responses."""

    article_ids: list[int] = []

    for response in list_responses:
        payload = response.get("payload")

        if not isinstance(payload, list):
            continue

        for article in payload:
            if isinstance(article, dict) and isinstance(article.get("id"), int):
                article_ids.append(article["id"])

    return list(dict.fromkeys(article_ids))


async def fetch_devto_data(
    base_url: str,
    latest_pages: int = 5,
    concurrency: int = 30,
) -> list[dict[str, Any]]:
    """
    Fetch all requested DEV Community endpoints.

    Covered endpoints:
    - articles?per_page=50&top=7
    - articles?tag={tag}&per_page=30
    - articles/latest?per_page=100&page={page}
    - articles/{id}
    """

    base_url = base_url.rstrip("/")

    timeout = aiohttp.ClientTimeout(total=600)
    semaphore = asyncio.Semaphore(concurrency)

    top_endpoint = f"articles?per_page=50&top={DEVTO_TOP_DAYS}"

    tag_endpoints = [
        f"articles?tag={tag}&per_page=30"
        for tag in DEVTO_REQUIRED_TAGS
    ]

    latest_endpoints = [
        f"articles/latest?per_page=100&page={page}"
        for page in range(1, latest_pages + 1)
    ]

    list_endpoints = [
        top_endpoint,
        *tag_endpoints,
        *latest_endpoints,
    ]

    async with aiohttp.ClientSession(timeout=timeout) as session:
        list_responses = await asyncio.gather(
            *[
                fetch_devto_list_endpoint(
                    session=session,
                    base_url=base_url,
                    endpoint=endpoint,
                    semaphore=semaphore,
                )
                for endpoint in list_endpoints
            ]
        )

        article_ids = extract_devto_article_ids(list_responses)

        detail_responses = await fetch_devto_article_details(
            session=session,
            base_url=base_url,
            article_ids=article_ids,
            semaphore=semaphore,
        )

    return [
        *list_responses,
        *detail_responses,
    ]
