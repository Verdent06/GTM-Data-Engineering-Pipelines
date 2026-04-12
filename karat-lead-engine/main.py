"""
Creator Underwriting Pipeline — YouTube Data API v3 integration.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any

import requests
from dotenv import load_dotenv

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
DEFAULT_RECENT_VIDEOS = 15

logger = logging.getLogger(__name__)


def _get_api_key() -> str:
    load_dotenv()
    key = os.getenv("YOUTUBE_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "Missing YOUTUBE_API_KEY. Set it in a .env file in the project root."
        )
    return key


def _youtube_get(
    path: str,
    params: dict[str, Any],
    api_key: str,
    timeout: int = 30,
) -> dict[str, Any]:
    """GET YouTube Data API with error handling."""
    url = f"{YOUTUBE_API_BASE}/{path}"
    full_params = {**params, "key": api_key}
    try:
        response = requests.get(url, params=full_params, timeout=timeout)
        response.raise_for_status()
    except requests.exceptions.Timeout as exc:
        logger.error("Request timed out: %s", path)
        raise RuntimeError(f"YouTube API request timed out: {path}") from exc
    except requests.exceptions.ConnectionError as exc:
        logger.error("Connection error calling %s", path)
        raise RuntimeError(f"Could not reach YouTube API: {path}") from exc
    except requests.exceptions.HTTPError as exc:
        body = ""
        if exc.response is not None:
            try:
                body = exc.response.text[:500]
            except Exception:
                body = "(no body)"
        logger.error("HTTP %s for %s: %s", exc.response.status_code if exc.response else "?", path, body)
        raise RuntimeError(
            f"YouTube API HTTP error for {path}: {exc.response.status_code if exc.response else 'unknown'}"
        ) from exc
    except requests.exceptions.RequestException as exc:
        logger.error("Request failed for %s: %s", path, exc)
        raise RuntimeError(f"YouTube API request failed: {path}") from exc

    try:
        return response.json()
    except ValueError as exc:
        logger.error("Invalid JSON from %s", path)
        raise RuntimeError(f"Invalid JSON response from YouTube API: {path}") from exc


def resolve_channel_id_from_handle(handle: str, api_key: str) -> str:
    """
    Resolve a modern YouTube handle (e.g. '@mrbeast') to channel id via channels.list forHandle.
    """
    h = handle.strip()
    if not h.startswith("@"):
        h = f"@{h.lstrip('@')}"

    logger.info("Resolving handle to channel id: %s", h)
    data = _youtube_get(
        "channels",
        {"part": "id", "forHandle": h},
        api_key,
    )
    items = data.get("items") or []
    if not items:
        raise ValueError(f"No channel found for handle {h!r}")
    channel_id = items[0].get("id")
    if not channel_id:
        raise ValueError(f"Channel response missing id for handle {h!r}")
    logger.info("Resolved %s -> %s", h, channel_id)
    return channel_id


def fetch_channel_metrics(channel_id: str, api_key: str) -> dict[str, Any]:
    """
    Fetch subscriberCount, viewCount, and uploads playlist id for a channel.
    """
    logger.info("Fetching channel metrics for %s", channel_id)
    data = _youtube_get(
        "channels",
        {
            "part": "statistics,contentDetails",
            "id": channel_id,
        },
        api_key,
    )
    items = data.get("items") or []
    if not items:
        raise ValueError(f"No channel data for id {channel_id!r}")

    item = items[0]
    stats = item.get("statistics") or {}
    content = item.get("contentDetails") or {}
    related = content.get("relatedPlaylists") or {}
    uploads_id = related.get("uploads")
    if not uploads_id:
        raise ValueError(f"No uploads playlist for channel {channel_id!r}")

    subs_raw = stats.get("subscriberCount")
    views_raw = stats.get("viewCount")

    out = {
        "subscriber_count": int(subs_raw) if subs_raw is not None else None,
        "channel_view_count": int(views_raw) if views_raw is not None else None,
        "uploads_playlist_id": uploads_id,
    }
    logger.info(
        "Channel metrics: subs=%s, channel_views=%s, uploads_playlist=%s",
        out["subscriber_count"],
        out["channel_view_count"],
        uploads_id,
    )
    return out


def fetch_recent_video_ids(
    uploads_playlist_id: str,
    api_key: str,
    max_results: int = DEFAULT_RECENT_VIDEOS,
) -> list[str]:
    """
    Return video ids for the most recent items in the channel uploads playlist.
    """
    logger.info(
        "Fetching up to %d recent video ids from playlist %s",
        max_results,
        uploads_playlist_id,
    )
    data = _youtube_get(
        "playlistItems",
        {
            "part": "contentDetails",
            "playlistId": uploads_playlist_id,
            "maxResults": min(max_results, 50),
        },
        api_key,
    )
    ids: list[str] = []
    for row in data.get("items") or []:
        cd = row.get("contentDetails") or {}
        vid = cd.get("videoId")
        if vid:
            ids.append(vid)
    logger.info("Retrieved %d video id(s)", len(ids))
    return ids


def _chunk_ids(ids: list[str], size: int = 50) -> list[list[str]]:
    return [ids[i : i + size] for i in range(0, len(ids), size)]


def compute_video_engagement_metrics(
    video_ids: list[str],
    api_key: str,
) -> dict[str, Any]:
    """
    For the given video ids, fetch statistics and compute V15 (avg views) and
    recent engagement rate: ((likes + comments) / views) * 100.
    """
    if not video_ids:
        return {
            "videos_analyzed": 0,
            "v15_average_views": None,
            "recent_engagement_rate_percent": None,
            "total_views": 0,
            "total_likes": 0,
            "total_comments": 0,
        }

    all_items: list[dict[str, Any]] = []
    for chunk in _chunk_ids(video_ids, 50):
        id_param = ",".join(chunk)
        logger.info("Fetching video statistics batch (%d ids)", len(chunk))
        data = _youtube_get(
            "videos",
            {"part": "statistics", "id": id_param},
            api_key,
        )
        all_items.extend(data.get("items") or [])

    total_views = 0
    total_likes = 0
    total_comments = 0
    per_video_views: list[int] = []

    for item in all_items:
        st = item.get("statistics") or {}
        # Some fields may be hidden on private/unlisted content
        v_raw = st.get("viewCount")
        l_raw = st.get("likeCount")
        c_raw = st.get("commentCount")

        v = int(v_raw) if v_raw is not None else 0
        total_views += v
        per_video_views.append(v)
        total_likes += int(l_raw) if l_raw is not None else 0
        total_comments += int(c_raw) if c_raw is not None else 0

    n = len(per_video_views)
    v15 = round(sum(per_video_views) / n, 2) if n else None

    if total_views > 0:
        engagement_pct = round(
            ((total_likes + total_comments) / total_views) * 100.0,
            4,
        )
    else:
        engagement_pct = None

    return {
        "videos_analyzed": n,
        "v15_average_views": v15,
        "recent_engagement_rate_percent": engagement_pct,
        "total_views": total_views,
        "total_likes": total_likes,
        "total_comments": total_comments,
    }


def build_creator_risk_profile(handle: str, api_key: str) -> dict[str, Any]:
    """Run the full pipeline and return a structured profile dict."""
    channel_id = resolve_channel_id_from_handle(handle, api_key)
    metrics = fetch_channel_metrics(channel_id, api_key)
    video_ids = fetch_recent_video_ids(
        metrics["uploads_playlist_id"],
        api_key,
        max_results=DEFAULT_RECENT_VIDEOS,
    )
    engagement = compute_video_engagement_metrics(video_ids, api_key)

    h = handle.strip()
    if not h.startswith("@"):
        h = f"@{h.lstrip('@')}"

    return {
        "handle": h,
        "channel_id": channel_id,
        "subscriber_count": metrics["subscriber_count"],
        "channel_lifetime_view_count": metrics["channel_view_count"],
        "uploads_playlist_id": metrics["uploads_playlist_id"],
        "recent_video_sample_size": engagement["videos_analyzed"],
        "v15_average_views": engagement["v15_average_views"],
        "recent_engagement_rate_percent": engagement["recent_engagement_rate_percent"],
        "recent_totals": {
            "views": engagement["total_views"],
            "likes": engagement["total_likes"],
            "comments": engagement["total_comments"],
        },
    }


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    try:
        api_key = _get_api_key()
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    # The Batch Processing Target List
    target_handles = [
        "@mrbeast", 
        "@mkbhd", 
        "@emmachamberlain", 
        "@colinandsamir", 
        "@cleoabram"
    ]

    batch_payload = []

    logger.info("Starting batch underwriting process for %d creators...", len(target_handles))

    for handle in target_handles:
        logger.info("--------------------------------------------------")
        logger.info("Processing target: %s", handle)
        try:
            profile = build_creator_risk_profile(handle, api_key)
            
            creator_data = {
                "creator_risk_profile": {
                    "handle": profile["handle"],
                    "channel_id": profile["channel_id"],
                    "subscriber_count": profile["subscriber_count"],
                    "v15_average_views": profile["v15_average_views"],
                    "recent_engagement_rate_percent": profile["recent_engagement_rate_percent"],
                },
                "pipeline_metadata": {
                    "recent_video_sample_size": profile["recent_video_sample_size"],
                    "channel_lifetime_view_count": profile["channel_lifetime_view_count"],
                    "uploads_playlist_id": profile["uploads_playlist_id"],
                    "recent_totals": profile["recent_totals"],
                }
            }
            batch_payload.append(creator_data)
        except (RuntimeError, ValueError) as exc:
            logger.error("Pipeline failed for %s: %s", handle, exc)
            # We don't exit here. We catch the error and keep processing the rest of the batch!

    # Final Master Payload
    final_output = {
        "batch_job_status": "SUCCESS",
        "creators_processed": len(batch_payload),
        "underwriting_data": batch_payload
    }

    print("\n" + "="*50)
    print("=== Batch Creator Risk Profiles (JSON) ===")
    print("="*50)
    print(json.dumps(final_output, indent=2, ensure_ascii=False))
    print()


if __name__ == "__main__":
    main()