"""TikTok Keyword Video Scraper - Apify Actor.

Uses Playwright to open TikTok's search page in a real Chromium browser.
TikTok's own JavaScript generates properly signed API requests; we intercept
the JSON responses directly, so no third-party TikTok library is needed.

Supports time-range filtering: last_day, last_week, last_month,
last_3_months, last_year, or all (no filter).
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote_plus

from apify import Actor
from playwright.async_api import BrowserContext, Page, async_playwright

# ---------------------------------------------------------------------------
# Time-filter helpers
# ---------------------------------------------------------------------------
TIME_FILTER_DELTAS: dict[str, timedelta | None] = {
    "all": None,
    "last_day": timedelta(days=1),
    "last_week": timedelta(weeks=1),
    "last_month": timedelta(days=30),
    "last_3_months": timedelta(days=90),
    "last_year": timedelta(days=365),
}


def _get_cutoff(time_filter: str) -> datetime | None:
    delta = TIME_FILTER_DELTAS.get(time_filter)
    return (datetime.now(tz=timezone.utc) - delta) if delta else None


def _parse_video(raw: dict[str, Any], keyword: str) -> dict[str, Any] | None:
    """Flatten a raw TikTok video dict into a storage-ready record."""
    try:
        # TikTok wraps the video under an 'item' key in some endpoints
        v = raw.get("item", raw)
        video_id = str(v.get("id", ""))
        if not video_id:
            return None

        author = v.get("author", {})
        stats = v.get("stats", {})
        create_time = int(v.get("createTime", 0))
        username = author.get("uniqueId", author.get("unique_id", ""))

        return {
            "id": video_id,
            "url": f"https://www.tiktok.com/@{username}/video/{video_id}",
            "description": v.get("desc", ""),
            "author_username": username,
            "author_display_name": author.get("nickname", ""),
            "likes": int(stats.get("diggCount", stats.get("heart", 0))),
            "comments": int(stats.get("commentCount", 0)),
            "shares": int(stats.get("shareCount", 0)),
            "plays": int(stats.get("playCount", 0)),
            "cover_url": v.get("video", {}).get("cover", "") or "",
            "created_at": (
                datetime.fromtimestamp(create_time, tz=timezone.utc).isoformat()
                if create_time else None
            ),
            "keyword": keyword,
            "scraped_at": datetime.now(tz=timezone.utc).isoformat(),
        }
    except Exception as exc:  # noqa: BLE001
        Actor.log.warning(f"Failed to parse video: {exc}")
        return None


# ---------------------------------------------------------------------------
# Per-keyword scrape using Playwright network interception
# ---------------------------------------------------------------------------

async def _scrape_keyword(
    context: BrowserContext,
    keyword: str,
    max_results: int,
    cutoff: datetime | None,
    scroll_pause: float = 2.0,
) -> list[dict[str, Any]]:
    """Open TikTok search in a browser tab and collect videos via intercepted API calls."""
    results: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    # Queue that receives parsed response payloads from the response handler
    payload_queue: asyncio.Queue[list[dict]] = asyncio.Queue()

    page: Page = await context.new_page()

    async def on_response(response) -> None:  # noqa: ANN001
        url: str = response.url
        # Capture TikTok's search/explore API calls
        if (
            "tiktok.com/api/search" in url
            or "tiktok.com/api/explore" in url
            or "tiktok.com/api/recommend" in url
        ):
            try:
                body = await response.body()
                data = json.loads(body)
                items: list[dict] = (
                    data.get("data")
                    or data.get("item_list")
                    or data.get("itemList")
                    or []
                )
                if items:
                    await payload_queue.put(items)
            except Exception:  # noqa: BLE001
                pass

    page.on("response", on_response)

    search_url = f"https://www.tiktok.com/search/video?q={quote_plus(keyword)}"
    Actor.log.info(f"[{keyword}] Opening: {search_url}")

    try:
        # 'commit' fires as soon as the response headers arrive (first byte),
        # which is the fastest signal and avoids waiting for heavy JS to finish.
        await page.goto(search_url, wait_until="commit", timeout=60_000)
        Actor.log.info(f"[{keyword}] Page navigation committed, waiting for API data...")
    except Exception as exc:  # noqa: BLE001
        # Even on a timeout the browser may have partially loaded and fired
        # API requests, so continue rather than giving up immediately.
        Actor.log.warning(f"[{keyword}] Navigation warning (continuing): {exc}")

    # Give TikTok's JS time to initialise and fire its first API requests.
    await asyncio.sleep(4)


    # Wait for the DOM body to be present before we start scrolling.
    # (with wait_until='commit' the body may not exist yet)
    try:
        await page.wait_for_selector("body", timeout=20_000)
        Actor.log.info(f"[{keyword}] DOM body ready.")
    except Exception:  # noqa: BLE001
        Actor.log.warning(f"[{keyword}] Timed out waiting for body — will attempt scroll anyway.")

    too_old_streak = 0
    no_new_content_streak = 0

    while len(results) < max_results:
        # Drain the queue of any intercepted payloads
        new_items_this_round = 0
        while not payload_queue.empty():
            raw_items = await payload_queue.get()
            for raw in raw_items:
                parsed = _parse_video(raw, keyword)
                if not parsed or parsed["id"] in seen_ids:
                    continue
                seen_ids.add(parsed["id"])

                # Time filter check
                create_ts = raw.get("item", raw).get("createTime", 0)
                if cutoff and create_ts:
                    video_dt = datetime.fromtimestamp(int(create_ts), tz=timezone.utc)
                    if video_dt < cutoff:
                        too_old_streak += 1
                        if too_old_streak >= 10:
                            Actor.log.info(
                                f"[{keyword}] 10 consecutive videos older than cutoff. Stopping early."
                            )
                            break
                        continue
                    too_old_streak = 0

                results.append(parsed)
                new_items_this_round += 1
                Actor.log.info(
                    f"[{keyword}] {len(results)}/{max_results}: {parsed['url']}"
                )
                if len(results) >= max_results:
                    break

            if too_old_streak >= 10:
                break

        if len(results) >= max_results or too_old_streak >= 10:
            break

        if new_items_this_round == 0:
            no_new_content_streak += 1
            if no_new_content_streak >= 4:
                Actor.log.info(f"[{keyword}] No new content after scrolling. Done.")
                break
        else:
            no_new_content_streak = 0

        # Scroll down to trigger more results.
        # Guard against document.body still being null on very slow loads.
        await page.evaluate(
            "document.body && window.scrollTo(0, document.body.scrollHeight)"
        )
        await asyncio.sleep(scroll_pause)

    await page.close()
    Actor.log.info(f"[{keyword}] Collected {len(results)} videos.")
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    """Main entry point for the TikTok Keyword Video Scraper Actor."""
    async with Actor:
        actor_input = await Actor.get_input() or {}

        keywords: list[str] = actor_input.get("keywords", [])
        max_results: int = int(actor_input.get("max_results_per_keyword", 30))
        time_filter: str = actor_input.get("time_filter", "all").strip().lower()
        ms_token: str = actor_input.get("ms_token", "").strip()
        proxy_config_input: dict = actor_input.get("proxy_configuration", {})

        if not keywords:
            Actor.log.error("No keywords provided in Actor input. Exiting.")
            await Actor.exit()
            return

        if time_filter not in TIME_FILTER_DELTAS:
            Actor.log.warning(f"Unknown time_filter '{time_filter}', defaulting to 'all'.")
            time_filter = "all"

        cutoff = _get_cutoff(time_filter)
        Actor.log.info(
            f"TikTok scraper starting | keywords={keywords} | "
            f"max_results_per_keyword={max_results} | time_filter={time_filter}"
            + (f" | cutoff={cutoff.isoformat()}" if cutoff else "")
        )

        # ── Proxy ─────────────────────────────────────────────────────────
        proxy_url: str | None = None
        if proxy_config_input:
            try:
                proxy_configuration = await Actor.create_proxy_configuration(
                    actor_proxy_input=proxy_config_input
                )
                if proxy_configuration:
                    proxy_url = await proxy_configuration.new_url()
                    Actor.log.info(f"Proxy configured: {proxy_url[:40]}...")
            except Exception as exc:  # noqa: BLE001
                Actor.log.warning(f"Could not set up proxy: {exc}")

        # ── Playwright ────────────────────────────────────────────────────
        async with async_playwright() as pw:
            launch_args = [
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-setuid-sandbox",
                "--disable-gpu",
            ]
            proxy_settings = (
                {"server": proxy_url} if proxy_url else None
            )

            browser = await pw.chromium.launch(
                headless=True,
                args=launch_args,
                proxy=proxy_settings,
            )

            # Build cookies list
            cookies_list = [
                {"name": "tiktok_webapp_theme", "value": "light",
                 "domain": ".tiktok.com", "path": "/"},
            ]
            if ms_token:
                cookies_list.append(
                    {"name": "msToken", "value": ms_token,
                     "domain": ".tiktok.com", "path": "/"}
                )

            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
                locale="en-US",
                timezone_id="America/New_York",
                proxy=proxy_settings,
            )
            await context.add_cookies(cookies_list)

            for keyword in keywords:
                Actor.log.info(f"=== Scraping keyword: '{keyword}' ===")
                try:
                    videos = await _scrape_keyword(
                        context=context,
                        keyword=keyword,
                        max_results=max_results,
                        cutoff=cutoff,
                    )
                except Exception as exc:  # noqa: BLE001
                    Actor.log.error(f"[{keyword}] Unexpected error: {exc}")
                    videos = []

                if videos:
                    await Actor.push_data(videos)
                    Actor.log.info(f"[{keyword}] Pushed {len(videos)} videos to dataset.")
                else:
                    Actor.log.warning(
                        f"[{keyword}] No videos collected. "
                        "Try a different ms_token or check proxy settings."
                    )

                if keyword != keywords[-1]:
                    await asyncio.sleep(3)

            await context.close()
            await browser.close()

        Actor.log.info("All keywords processed. Actor finished.")
