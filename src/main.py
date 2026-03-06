"""TikTok Keyword Video Scraper - Apify Actor.

Scrapes TikTok videos by keywords using TikTok's internal search API.
For each keyword provided, it paginates through search results and
stores video metadata (URL, author, likes, comments, shares, plays, etc.)
into the Apify dataset.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import httpx
from apify import Actor

# ---------------------------------------------------------------------------
# TikTok internal search API endpoint
# ---------------------------------------------------------------------------
SEARCH_API_URL = "https://www.tiktok.com/api/search/general/full/"

# Realistic browser-like headers to avoid immediate blocking
BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.tiktok.com/",
    "Origin": "https://www.tiktok.com",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
}


def _build_params(keyword: str, cursor: int = 0, count: int = 30) -> dict[str, str]:
    """Build query parameters for the TikTok search API request."""
    return {
        "keyword": keyword,
        "offset": str(cursor),
        "count": str(min(count, 30)),  # TikTok max per page is ~30
        "type": "1",  # 1 = videos
        "from_page": "search",
        "web_id": "7000000000000000000",
        "device_platform": "web_pc",
        "region": "US",
        "priority_region": "",
        "os": "windows",
        "referer": "",
        "root_referer": "https://www.tiktok.com/",
        "msToken": "",
    }


def _parse_video(item: dict[str, Any], keyword: str) -> dict[str, Any] | None:
    """Extract relevant fields from a raw TikTok search result item."""
    try:
        # The item structure differs based on the result type
        # Video items have an 'item' sub-key or are directly a video object
        video = item.get("item", item)

        video_id: str = str(video.get("id", ""))
        if not video_id:
            return None

        author = video.get("author", {})
        stats = video.get("stats", {})
        desc: str = video.get("desc", "")
        cover = video.get("video", {}).get("cover", "") or ""
        create_time = video.get("createTime", 0)

        author_username: str = author.get("uniqueId", author.get("unique_id", ""))
        author_display: str = author.get("nickname", "")

        return {
            "id": video_id,
            "url": f"https://www.tiktok.com/@{author_username}/video/{video_id}",
            "description": desc,
            "author_username": author_username,
            "author_display_name": author_display,
            "likes": int(stats.get("diggCount", stats.get("heart", 0))),
            "comments": int(stats.get("commentCount", 0)),
            "shares": int(stats.get("shareCount", 0)),
            "plays": int(stats.get("playCount", 0)),
            "cover_url": cover,
            "created_at": (
                datetime.fromtimestamp(int(create_time), tz=timezone.utc).isoformat()
                if create_time
                else None
            ),
            "keyword": keyword,
            "scraped_at": datetime.now(tz=timezone.utc).isoformat(),
        }
    except Exception as exc:  # noqa: BLE001
        Actor.log.warning(f"Failed to parse video item: {exc}")
        return None


async def scrape_keyword(
    client: httpx.AsyncClient,
    keyword: str,
    max_results: int,
    delay: float = 1.5,
) -> list[dict[str, Any]]:
    """Scrape TikTok videos for a single keyword.

    Args:
        client: Shared httpx async client (with proxy if configured).
        keyword: Search term.
        max_results: Maximum number of video records to collect.
        delay: Seconds to sleep between paginated requests.

    Returns:
        List of parsed video dictionaries.
    """
    results: list[dict[str, Any]] = []
    cursor = 0
    page = 0

    while len(results) < max_results:
        remaining = max_results - len(results)
        params = _build_params(keyword, cursor=cursor, count=min(remaining, 30))

        Actor.log.info(
            f'[{keyword}] Fetching page {page + 1} (cursor={cursor}, '
            f'collected={len(results)}/{max_results})...'
        )

        try:
            response = await client.get(
                SEARCH_API_URL,
                params=params,
                headers=BASE_HEADERS,
                timeout=30,
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            Actor.log.error(
                f'[{keyword}] HTTP error {exc.response.status_code} on page {page + 1}. '
                f'Stopping pagination for this keyword.'
            )
            break
        except httpx.RequestError as exc:
            Actor.log.error(f'[{keyword}] Request error: {exc}. Stopping pagination.')
            break

        try:
            data = response.json()
        except json.JSONDecodeError:
            Actor.log.error(f'[{keyword}] Failed to decode JSON response. Stopping.')
            break

        # TikTok returns results under different keys depending on endpoint version
        raw_items: list[dict] = (
            data.get("data", [])
            or data.get("item_list", [])
            or data.get("itemList", [])
        )

        if not raw_items:
            Actor.log.info(f'[{keyword}] No more results returned. Done.')
            break

        for raw in raw_items:
            video = _parse_video(raw, keyword)
            if video:
                results.append(video)
                if len(results) >= max_results:
                    break

        # Advance cursor for next page
        cursor = int(data.get("cursor", cursor + len(raw_items)))
        has_more = data.get("has_more", 0)
        page += 1

        if not has_more:
            Actor.log.info(f'[{keyword}] Reached last page (has_more=0). Done.')
            break

        await asyncio.sleep(delay)

    Actor.log.info(f'[{keyword}] Collected {len(results)} videos.')
    return results


async def main() -> None:
    """Main entry point for the TikTok Keyword Video Scraper Actor."""
    async with Actor:
        actor_input = await Actor.get_input() or {}

        # ---- Input parameters ----------------------------------------
        keywords: list[str] = actor_input.get("keywords", [])
        max_results: int = int(actor_input.get("max_results_per_keyword", 30))
        request_delay: float = float(actor_input.get("request_delay_seconds", 1.5))
        proxy_config_input: dict = actor_input.get("proxy_configuration", {})

        if not keywords:
            Actor.log.error("No keywords provided in Actor input. Exiting.")
            await Actor.exit()
            return

        Actor.log.info(
            f"Starting TikTok scraper | "
            f"keywords={keywords} | max_results_per_keyword={max_results}"
        )

        # ---- Proxy setup --------------------------------------------
        proxy_url: str | None = None
        if proxy_config_input:
            try:
                proxy_configuration = await Actor.create_proxy_configuration(
                    actor_proxy_input=proxy_config_input
                )
                if proxy_configuration:
                    proxy_url = await proxy_configuration.new_url()
                    Actor.log.info("Proxy configured successfully.")
            except Exception as exc:  # noqa: BLE001
                Actor.log.warning(f"Could not set up proxy: {exc}")

        # ---- HTTP client -------------------------------------------
        proxies = {"https://": proxy_url, "http://": proxy_url} if proxy_url else None
        cookies = {
            "tiktok_webapp_theme": "light",
            "tt_chain_token": "",
        }

        async with httpx.AsyncClient(
            proxies=proxies,
            cookies=cookies,
            follow_redirects=True,
        ) as client:
            # Warm-up request to acquire session cookies
            try:
                Actor.log.info("Warming up session with TikTok homepage...")
                await client.get(
                    "https://www.tiktok.com/",
                    headers=BASE_HEADERS,
                    timeout=15,
                )
                await asyncio.sleep(1)
            except Exception as exc:  # noqa: BLE001
                Actor.log.warning(f"Warm-up request failed (non-fatal): {exc}")

            # ---- Scrape each keyword --------------------------------
            for keyword in keywords:
                Actor.log.info(f"=== Scraping keyword: '{keyword}' ===")
                videos = await scrape_keyword(
                    client,
                    keyword=keyword,
                    max_results=max_results,
                    delay=request_delay,
                )

                if videos:
                    await Actor.push_data(videos)
                    Actor.log.info(
                        f"Pushed {len(videos)} videos for keyword '{keyword}' to dataset."
                    )
                else:
                    Actor.log.warning(
                        f"No videos collected for keyword '{keyword}'. "
                        "TikTok may be blocking requests — try using a proxy."
                    )

                # Pause between keywords to avoid rate limiting
                if keyword != keywords[-1]:
                    await asyncio.sleep(request_delay * 2)

        Actor.log.info("All keywords processed. Actor finished.")
