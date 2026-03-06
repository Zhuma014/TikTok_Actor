"""TikTok Keyword Video Scraper - Apify Actor.

Uses the TikTokApi library (Playwright-backed) to search TikTok by keywords
and store video metadata into the Apify dataset.

Supports time-range filtering: last_day, last_week, last_month,
last_3_months, last_year, or all (no filter).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from apify import Actor
from TikTokApi import TikTokApi

# ---------------------------------------------------------------------------
# Time-filter helper
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
    """Return the UTC cutoff datetime for the given filter name, or None for 'all'."""
    delta = TIME_FILTER_DELTAS.get(time_filter)
    if delta is None:
        return None
    return datetime.now(tz=timezone.utc) - delta


def _within_period(video_dict: dict[str, Any], cutoff: datetime | None) -> bool:
    """Return True if the video's createTime is at or after the cutoff."""
    if cutoff is None:
        return True
    create_time: int = video_dict.get("createTime", 0)
    if not create_time:
        return True  # unknown timestamp → keep
    video_dt = datetime.fromtimestamp(int(create_time), tz=timezone.utc)
    return video_dt >= cutoff


def _is_older_than_cutoff(video_dict: dict[str, Any], cutoff: datetime | None) -> bool:
    """Return True if the video is OLDER than the cutoff (used as early-stop signal)."""
    if cutoff is None:
        return False
    create_time: int = video_dict.get("createTime", 0)
    if not create_time:
        return False
    video_dt = datetime.fromtimestamp(int(create_time), tz=timezone.utc)
    return video_dt < cutoff


# ---------------------------------------------------------------------------
# Video data extraction
# ---------------------------------------------------------------------------

def _parse_video(video: Any, keyword: str) -> dict[str, Any] | None:
    """Convert a TikTokApi Video object into a flat dictionary for storage."""
    try:
        d = video.as_dict
        author = d.get("author", {})
        stats = d.get("stats", {})
        create_time = d.get("createTime", 0)
        video_id = str(d.get("id", ""))
        author_username = author.get("uniqueId", author.get("unique_id", ""))

        return {
            "id": video_id,
            "url": f"https://www.tiktok.com/@{author_username}/video/{video_id}",
            "description": d.get("desc", ""),
            "author_username": author_username,
            "author_display_name": author.get("nickname", ""),
            "likes": int(stats.get("diggCount", stats.get("heart", 0))),
            "comments": int(stats.get("commentCount", 0)),
            "shares": int(stats.get("shareCount", 0)),
            "plays": int(stats.get("playCount", 0)),
            "cover_url": d.get("video", {}).get("cover", "") or "",
            "created_at": (
                datetime.fromtimestamp(int(create_time), tz=timezone.utc).isoformat()
                if create_time
                else None
            ),
            "keyword": keyword,
            "scraped_at": datetime.now(tz=timezone.utc).isoformat(),
        }
    except Exception as exc:  # noqa: BLE001
        Actor.log.warning(f"Failed to parse video: {exc}")
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    """Main entry point for the TikTok Keyword Video Scraper Actor."""
    async with Actor:
        actor_input = await Actor.get_input() or {}

        # ── Input parameters ──────────────────────────────────────────────
        keywords: list[str] = actor_input.get("keywords", [])
        max_results: int = int(actor_input.get("max_results_per_keyword", 30))
        time_filter: str = actor_input.get("time_filter", "all").strip().lower()
        ms_token: str = actor_input.get("ms_token", "").strip()

        if not keywords:
            Actor.log.error("No keywords provided in Actor input. Exiting.")
            await Actor.exit()
            return

        if time_filter not in TIME_FILTER_DELTAS:
            Actor.log.warning(
                f"Unknown time_filter '{time_filter}', defaulting to 'all'."
            )
            time_filter = "all"

        cutoff = _get_cutoff(time_filter)
        Actor.log.info(
            f"TikTok scraper starting | keywords={keywords} | "
            f"max_results_per_keyword={max_results} | time_filter={time_filter}"
            + (f" | cutoff={cutoff.isoformat()}" if cutoff else "")
        )

        # ── TikTokApi session ────────────────────────────────────────────
        # ms_token is optional but improves reliability when provided.
        async with TikTokApi() as api:
            await api.create_sessions(
                ms_tokens=[ms_token] if ms_token else None,
                num_sessions=1,
                sleep_after=3,
                headless=True,
            )

            for keyword in keywords:
                Actor.log.info(f"=== Scraping keyword: '{keyword}' ===")
                collected: list[dict[str, Any]] = []
                too_old_streak = 0  # consecutive videos older than cutoff → early-stop

                try:
                    async for video in api.search.videos(keyword, count=max_results):
                        raw = video.as_dict

                        # Early stop: if several consecutive videos are older than
                        # the cutoff, TikTok has passed the relevant window.
                        if _is_older_than_cutoff(raw, cutoff):
                            too_old_streak += 1
                            if too_old_streak >= 5:
                                Actor.log.info(
                                    f"[{keyword}] 5 consecutive videos older than "
                                    f"cutoff ({time_filter}). Stopping early."
                                )
                                break
                            continue
                        else:
                            too_old_streak = 0

                        if not _within_period(raw, cutoff):
                            continue

                        parsed = _parse_video(video, keyword)
                        if parsed:
                            collected.append(parsed)
                            Actor.log.info(
                                f"[{keyword}] Collected {len(collected)}/{max_results}: "
                                f"{parsed['url']}"
                            )

                        if len(collected) >= max_results:
                            break

                except Exception as exc:  # noqa: BLE001
                    Actor.log.error(
                        f"[{keyword}] Error during scraping: {exc}. "
                        "If this persists, try providing an ms_token in the input."
                    )

                if collected:
                    await Actor.push_data(collected)
                    Actor.log.info(
                        f"[{keyword}] Pushed {len(collected)} videos to dataset."
                    )
                else:
                    Actor.log.warning(
                        f"[{keyword}] No videos collected. "
                        "Try providing a valid ms_token or check your proxy settings."
                    )

                # Brief pause between keywords
                if keyword != keywords[-1]:
                    await asyncio.sleep(2)

        Actor.log.info("All keywords processed. Actor finished.")
