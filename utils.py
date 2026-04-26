"""
utils.py — Shared helpers for YouTube scraper.

Covers:
- Randomised browser-like request headers
- Extraction of ytInitialData / ytInitialPlayerResponse from raw HTML
- Async retry wrapper with exponential back-off
- YouTube search-filter SP parameter encoding
- Numeric string parsing (view counts, subscriber counts)
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# User-agent pool (desktop Chrome on various OSes)
# ---------------------------------------------------------------------------
_USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

_ACCEPT_LANGUAGES: list[str] = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9",
    "en-US,en;q=0.8,es;q=0.5",
    "en-US,en;q=0.9,fr;q=0.8",
]


def get_random_headers() -> dict[str, str]:
    """Return a randomised set of browser-like HTTP request headers."""
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice(_ACCEPT_LANGUAGES),
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }


def get_api_headers(user_agent: str | None = None) -> dict[str, str]:
    """Return headers suitable for YouTube's internal JSON API endpoints."""
    return {
        "User-Agent": user_agent or random.choice(_USER_AGENTS),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/json",
        "X-YouTube-Client-Name": "1",
        "X-YouTube-Client-Version": "2.20240415.00.00",
        "Origin": "https://www.youtube.com",
        "Referer": "https://www.youtube.com/",
    }


# ---------------------------------------------------------------------------
# ytInitialData / ytInitialPlayerResponse extraction
# ---------------------------------------------------------------------------
#
# YouTube embeds these objects as JS variable assignments inside <script> tags.
# The values are deeply nested JSON (100+ levels), so regex with .*? (non-greedy)
# stops at the very first "}" and produces truncated, unparseable JSON.
# json.JSONDecoder.raw_decode() handles arbitrary nesting correctly by tracking
# brace depth internally — it is the only reliable approach.

# Markers to locate the start of each JSON object in the raw HTML
_YT_INITIAL_DATA_MARKERS: list[str] = [
    "var ytInitialData = ",
    "window[\"ytInitialData\"] = ",
    "ytInitialData = ",
]

_YT_INITIAL_PLAYER_MARKERS: list[str] = [
    "var ytInitialPlayerResponse = ",
    "window[\"ytInitialPlayerResponse\"] = ",
    "ytInitialPlayerResponse = ",
]

_JSON_DECODER = json.JSONDecoder()


def _raw_decode_at_marker(html: str, markers: list[str]) -> dict[str, Any] | None:
    """
    Locate the first matching marker in *html*, then use raw_decode to parse
    the JSON object that follows it.  Returns None if no marker is found or
    parsing fails for every marker.
    """
    for marker in markers:
        idx = html.find(marker)
        if idx == -1:
            continue
        start = idx + len(marker)
        # Skip optional whitespace between '=' and '{'
        while start < len(html) and html[start] in (" ", "\t", "\n", "\r"):
            start += 1
        if start >= len(html) or html[start] != "{":
            continue
        try:
            obj, _ = _JSON_DECODER.raw_decode(html, start)
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            continue
    return None


def extract_yt_initial_data(html: str) -> dict[str, Any]:
    """
    Extract and parse the ytInitialData JSON object from a YouTube HTML page.

    Raises:
        ValueError: if the object cannot be found or parsed.
    """
    data = _raw_decode_at_marker(html, _YT_INITIAL_DATA_MARKERS)
    if data is None:
        raise ValueError("ytInitialData not found in page HTML")
    return data


def extract_yt_initial_player_response(html: str) -> dict[str, Any]:
    """
    Extract and parse the ytInitialPlayerResponse JSON object from a YouTube
    video page.

    Raises:
        ValueError: if the object cannot be found or parsed.
    """
    data = _raw_decode_at_marker(html, _YT_INITIAL_PLAYER_MARKERS)
    if data is None:
        raise ValueError("ytInitialPlayerResponse not found in page HTML")
    return data


# ---------------------------------------------------------------------------
# Safe deep-get helpers
# ---------------------------------------------------------------------------

def safe_get(obj: Any, *keys: str | int, default: Any = None) -> Any:
    """Safely traverse a nested dict/list without raising KeyError/IndexError."""
    current = obj
    for key in keys:
        try:
            current = current[key]
        except (KeyError, IndexError, TypeError):
            return default
    return current


def get_text(obj: Any, *keys: str | int, default: str = "") -> str:
    """Extract a 'runs[0].text' or 'simpleText' string from a YouTube text node."""
    node = safe_get(obj, *keys)
    if node is None:
        return default
    if isinstance(node, str):
        return node
    # simpleText
    if "simpleText" in node:
        return node["simpleText"]
    # runs array
    runs = node.get("runs")
    if runs and isinstance(runs, list):
        return "".join(run.get("text", "") for run in runs)
    return default


# ---------------------------------------------------------------------------
# Search-filter (SP) parameter encoding
# ---------------------------------------------------------------------------
# YouTube encodes search filters as protobuf-serialised + base64-encoded blobs.
# These are the well-known static values obtained by reverse engineering.
# Format: {sort_key}_{date_key}  →  base64 SP value

_SORT_CODES: dict[str, str] = {
    "relevance": "",        # default – no sort modifier needed
    "upload_date": "CAI%3D",
    "view_count": "CAM%3D",
    "rating": "CAE%3D",
}

_DATE_CODES: dict[str, str] = {
    "all": "",
    "last_hour": "EgIIAQ%3D%3D",
    "today": "EgIIAg%3D%3D",
    "this_week": "EgIIAw%3D%3D",
    "this_month": "EgIIBA%3D%3D",
    "this_year": "EgIIBQ%3D%3D",
}

# Combined sort+date SP values (derived from YouTube URL observation)
_COMBINED_SP: dict[tuple[str, str], str] = {
    # (sort_by, upload_date_filter): sp value
    ("relevance", "all"): "",
    ("relevance", "last_hour"): "EgIIAQ%3D%3D",
    ("relevance", "today"): "EgIIAg%3D%3D",
    ("relevance", "this_week"): "EgIIAw%3D%3D",
    ("relevance", "this_month"): "EgIIBA%3D%3D",
    ("relevance", "this_year"): "EgIIBQ%3D%3D",
    ("upload_date", "all"): "CAI%3D",
    ("upload_date", "last_hour"): "CAISBAgBEAE%3D",
    ("upload_date", "today"): "CAISBAgCEAE%3D",
    ("upload_date", "this_week"): "CAISBAgDEAE%3D",
    ("upload_date", "this_month"): "CAISBAgEEAE%3D",
    ("upload_date", "this_year"): "CAISBAgFEAE%3D",
    ("view_count", "all"): "CAM%3D",
    ("view_count", "last_hour"): "CAMSBAgBEAE%3D",
    ("view_count", "today"): "CAMSBAgCEAE%3D",
    ("view_count", "this_week"): "CAMSBAgDEAE%3D",
    ("view_count", "this_month"): "CAMSBAgEEAE%3D",
    ("view_count", "this_year"): "CAMSBAgFEAE%3D",
}


def encode_search_filters(sort_by: str = "relevance", upload_date_filter: str = "all") -> str:
    """
    Return the URL-encoded 'sp' query parameter value for the given sort and
    date-filter combination.  Returns an empty string when no filter is needed.
    """
    sort_by = sort_by.lower().strip()
    upload_date_filter = upload_date_filter.lower().strip()

    key = (sort_by, upload_date_filter)
    sp = _COMBINED_SP.get(key, "")
    return sp


# ---------------------------------------------------------------------------
# Numeric parsing utilities
# ---------------------------------------------------------------------------

_MULTIPLIERS: dict[str, int] = {
    "k": 1_000,
    "m": 1_000_000,
    "b": 1_000_000_000,
}


def parse_count(text: str) -> int | None:
    """
    Parse YouTube shorthand numbers like '1.2M views', '45K', '1,234,567'.

    Returns None if the text cannot be parsed.
    """
    if not text:
        return None
    text = text.lower().strip()
    # Strip non-numeric trailing words (e.g. "views", "subscribers", "likes")
    text = re.sub(r"[^\d.,kmb]", "", text)
    if not text:
        return None

    # Handle multiplier suffix
    suffix = text[-1] if text[-1] in _MULTIPLIERS else None
    if suffix:
        text = text[:-1]

    # Remove commas (thousands separators)
    text = text.replace(",", "")

    try:
        value = float(text)
    except ValueError:
        return None

    if suffix:
        value *= _MULTIPLIERS[suffix]

    return int(value)


def format_duration(seconds: int | str) -> str:
    """Convert duration in seconds to HH:MM:SS or MM:SS string."""
    try:
        total = int(seconds)
    except (ValueError, TypeError):
        return str(seconds)

    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


# ---------------------------------------------------------------------------
# Async retry with exponential back-off
# ---------------------------------------------------------------------------

async def async_retry(
    coro_factory,
    *,
    max_retries: int = 3,
    backoff_base: float = 2.0,
    jitter: float = 0.5,
    retriable_exceptions: tuple[type[Exception], ...] = (
        httpx.TimeoutException,
        httpx.ConnectError,
        httpx.RemoteProtocolError,
        httpx.HTTPStatusError,
    ),
) -> Any:
    """
    Call ``coro_factory()`` (a zero-argument async callable / coroutine factory)
    and retry on transient errors using exponential back-off with jitter.

    Args:
        coro_factory:  A callable that returns a fresh coroutine each time.
        max_retries:   Number of retry attempts (not counting the first try).
        backoff_base:  Base for exponential delay: delay = backoff_base ** attempt.
        jitter:        Extra random seconds [0, jitter] added to each delay.
        retriable_exceptions: Exception types that trigger a retry.

    Returns:
        The return value of the successful coroutine.

    Raises:
        The last exception if all attempts fail.
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            return await coro_factory()
        except retriable_exceptions as exc:
            last_exc = exc
            if attempt == max_retries:
                break
            # For HTTP status errors only retry on 429 / 5xx
            if isinstance(exc, httpx.HTTPStatusError):
                if exc.response.status_code not in {429, 500, 502, 503, 504}:
                    raise
            delay = (backoff_base ** attempt) + random.uniform(0, jitter)
            logger.warning(
                "Request failed (attempt %d/%d): %s — retrying in %.1fs",
                attempt + 1,
                max_retries + 1,
                exc,
                delay,
            )
            await asyncio.sleep(delay)

    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# URL normalization helpers
# ---------------------------------------------------------------------------

def normalize_channel_url(url: str) -> str:
    """
    Ensure the channel URL points to the /videos tab.

    Accepts: /c/Name, /@handle, /channel/UCxxx, /user/Name
    """
    url = url.rstrip("/")
    if not url.startswith("http"):
        url = "https://www.youtube.com/" + url.lstrip("/")
    # Strip existing tab
    for tab in ("/videos", "/about", "/playlists", "/community", "/featured"):
        if url.endswith(tab):
            url = url[: -len(tab)]
    return url + "/videos"


def build_video_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def build_channel_url(channel_id: str) -> str:
    return f"https://www.youtube.com/channel/{channel_id}"
