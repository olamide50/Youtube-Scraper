"""
scraper.py — Core YouTube scraping logic.

Public API
----------
YouTubeScraper
    .search_videos(query)          → list[VideoItem]
    .scrape_video_details(url)     → VideoItem
    .scrape_channel(url)           → ChannelItem
    .scrape_trending()             → list[VideoItem]
    .scrape_comments(video_id)     → list[CommentItem]
    .run(input_data)               → list[dict]  (orchestrator)
"""

from __future__ import annotations

import asyncio
import logging
import random
import urllib.parse
from typing import Any, TypedDict

import httpx

from utils import (
    async_retry,
    build_channel_url,
    build_video_url,
    encode_search_filters,
    extract_yt_initial_data,
    extract_yt_initial_player_response,
    format_duration,
    get_api_headers,
    get_random_headers,
    get_text,
    normalize_channel_url,
    parse_count,
    safe_get,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type definitions
# ---------------------------------------------------------------------------

class VideoItem(TypedDict, total=False):
    video_id: str
    video_url: str
    title: str
    channel_name: str
    channel_url: str
    views: int | None
    likes: int | None
    upload_date: str | None
    duration: str | None
    description: str | None
    tags: list[str]
    thumbnail_url: str | None
    subtitles: list[dict] | None
    source: str  # "search" | "video_url" | "channel"


class ChannelItem(TypedDict, total=False):
    channel_name: str
    channel_url: str
    subscribers: str | None
    total_videos: int | None
    channel_description: str | None
    videos: list[VideoItem]


class CommentItem(TypedDict, total=False):
    author: str
    text: str
    likes: int | None
    published_time: str | None
    is_pinned: bool


# ---------------------------------------------------------------------------
# YouTube internal API constants
# ---------------------------------------------------------------------------

YT_BASE = "https://www.youtube.com"
YT_INNERTUBE_URL = "https://www.youtube.com/youtubei/v1/{endpoint}"
YT_INNERTUBE_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"

_INNERTUBE_CONTEXT = {
    "client": {
        "clientName": "WEB",
        "clientVersion": "2.20240415.00.00",
        "hl": "en",
        "gl": "US",
        "userAgent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36,gzip(gfe)"
        ),
        "timeZone": "America/New_York",
        "utcOffsetMinutes": -300,
    }
}


def _innertube_payload(extra: dict[str, Any]) -> dict[str, Any]:
    payload = {"context": _INNERTUBE_CONTEXT}
    payload.update(extra)
    return payload


# ---------------------------------------------------------------------------
# Main scraper class
# ---------------------------------------------------------------------------

class YouTubeScraper:
    """
    Async YouTube scraper that reverse-engineers ytInitialData / ytInitialPlayerResponse
    and YouTube's internal Innertube API for pagination.

    Parameters
    ----------
    proxy_url:
        Optional HTTP/HTTPS proxy URL (e.g. ``http://user:pass@proxy.host:8000``).
    max_results:
        Maximum number of videos to return per query / channel (default 20).
    sort_by:
        Search sort order: ``relevance`` | ``upload_date`` | ``view_count``.
    upload_date_filter:
        Search date filter: ``all`` | ``last_hour`` | ``today`` | ``this_week``
        | ``this_month`` | ``this_year``.
    concurrency:
        Maximum number of simultaneous HTTP requests (default 5).
    request_delay:
        Random sleep range (seconds) between requests to avoid rate limiting.
    """

    def __init__(
        self,
        proxy_url: str | None = None,
        max_results: int = 20,
        sort_by: str = "relevance",
        upload_date_filter: str = "all",
        concurrency: int = 5,
        request_delay: tuple[float, float] = (0.5, 2.0),
    ) -> None:
        self.proxy_url = proxy_url
        self.max_results = max_results
        self.sort_by = sort_by
        self.upload_date_filter = upload_date_filter
        self._semaphore = asyncio.Semaphore(concurrency)
        self._delay_range = request_delay
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "YouTubeScraper":
        self._client = httpx.AsyncClient(
            headers=get_random_headers(),
            follow_redirects=True,
            timeout=httpx.Timeout(30.0, connect=15.0),
            proxy=self.proxy_url,  # httpx>=0.23 uses proxy= (str|None), not proxies=
            http2=True,
        )
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Internal HTTP helpers
    # ------------------------------------------------------------------

    async def _get(self, url: str, **kwargs: Any) -> httpx.Response:
        """GET with semaphore + retry + random delay."""
        assert self._client is not None, "Use scraper inside async context manager"

        async def _do() -> httpx.Response:
            resp = await self._client.get(url, headers=get_random_headers(), **kwargs)
            resp.raise_for_status()
            return resp

        async with self._semaphore:
            await asyncio.sleep(random.uniform(*self._delay_range))
            return await async_retry(_do)

    async def _post_innertube(
        self, endpoint: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """POST to YouTube's internal Innertube API."""
        assert self._client is not None

        url = YT_INNERTUBE_URL.format(endpoint=endpoint)
        params = {"key": YT_INNERTUBE_KEY, "prettyPrint": "false"}

        async def _do() -> dict[str, Any]:
            resp = await self._client.post(
                url,
                params=params,
                json=payload,
                headers=get_api_headers(),
            )
            resp.raise_for_status()
            return resp.json()

        async with self._semaphore:
            await asyncio.sleep(random.uniform(*self._delay_range))
            return await async_retry(_do)

    # ------------------------------------------------------------------
    # ytInitialData navigation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_video_renderer(renderer: dict[str, Any]) -> VideoItem | None:
        """
        Parse a ``videoRenderer`` dict from ytInitialData into a VideoItem.
        Returns None if the renderer is not a video renderer.
        """
        video_id = renderer.get("videoId")
        if not video_id:
            return None

        title = get_text(renderer, "title")
        duration_text = get_text(renderer, "lengthText")
        view_count_text = get_text(renderer, "viewCountText")
        published_time = get_text(renderer, "publishedTimeText")
        channel_name = get_text(renderer, "ownerText")
        if not channel_name:
            channel_name = get_text(renderer, "longBylineText")

        # Channel URL
        channel_browse_ep = safe_get(
            renderer, "ownerText", "runs", 0, "navigationEndpoint",
            "browseEndpoint"
        )
        if not channel_browse_ep:
            channel_browse_ep = safe_get(
                renderer, "longBylineText", "runs", 0, "navigationEndpoint",
                "browseEndpoint"
            )
        channel_url = ""
        if channel_browse_ep:
            canonical = channel_browse_ep.get("canonicalBaseUrl", "")
            channel_id = channel_browse_ep.get("browseId", "")
            channel_url = (
                f"{YT_BASE}{canonical}" if canonical else build_channel_url(channel_id)
            )

        # Best-quality thumbnail
        thumbnails: list[dict] = safe_get(renderer, "thumbnail", "thumbnails") or []
        thumbnail_url = thumbnails[-1]["url"] if thumbnails else None

        views = parse_count(view_count_text)

        return VideoItem(
            video_id=video_id,
            video_url=build_video_url(video_id),
            title=title,
            channel_name=channel_name,
            channel_url=channel_url,
            views=views,
            likes=None,
            upload_date=published_time or None,
            duration=duration_text or None,
            description=None,
            tags=[],
            thumbnail_url=thumbnail_url,
            subtitles=None,
        )

    @staticmethod
    def _find_continuation_token(contents: list[dict]) -> str | None:
        """
        Walk a list of renderer dicts and return the first continuation token
        found inside a ``continuationItemRenderer``.
        """
        for item in contents:
            token = safe_get(
                item,
                "continuationItemRenderer",
                "continuationEndpoint",
                "continuationCommand",
                "token",
            )
            if token:
                return token
        return None

    # ------------------------------------------------------------------
    # search_videos
    # ------------------------------------------------------------------

    async def search_videos(self, query: str) -> list[VideoItem]:
        """
        Search YouTube for *query* and return up to ``max_results`` VideoItems.

        Uses the initial HTML page for the first batch, then paginates via
        the Innertube /search continuation endpoint.
        """
        logger.info("Searching: %r (max=%d)", query, self.max_results)
        sp = encode_search_filters(self.sort_by, self.upload_date_filter)
        encoded_query = urllib.parse.quote_plus(query)
        url = f"{YT_BASE}/results?search_query={encoded_query}"
        if sp:
            url += f"&sp={sp}"

        resp = await self._get(url)
        try:
            data = extract_yt_initial_data(resp.text)
        except ValueError as exc:
            logger.error("Failed to extract ytInitialData for query %r: %s", query, exc)
            return []

        videos: list[VideoItem] = []
        continuation_token: str | None = None

        # Navigate into search results
        primary = safe_get(
            data,
            "contents",
            "twoColumnSearchResultsRenderer",
            "primaryContents",
            "sectionListRenderer",
            "contents",
        )
        if primary:
            videos, continuation_token = self._parse_search_contents(primary)
        else:
            # Log top-level keys so we can diagnose unexpected page structures
            top_keys = list(data.keys()) if isinstance(data, dict) else []
            logger.warning(
                "search_videos(%r): twoColumnSearchResultsRenderer not found. "
                "Top-level ytInitialData keys: %s",
                query, top_keys,
            )

        # Paginate until max_results satisfied
        while len(videos) < self.max_results and continuation_token:
            payload = _innertube_payload({"continuation": continuation_token})
            try:
                cont_data = await self._post_innertube("search", payload)
            except httpx.HTTPError as exc:
                logger.warning("Continuation request failed: %s", exc)
                break

            new_videos, continuation_token = self._parse_search_continuation(cont_data)
            videos.extend(new_videos)

        logger.info("Search %r → %d results", query, len(videos[: self.max_results]))
        return videos[: self.max_results]

    def _parse_search_contents(
        self, contents: list[dict]
    ) -> tuple[list[VideoItem], str | None]:
        videos: list[VideoItem] = []
        continuation_token: str | None = None

        for section in contents:
            # Continuation token at section level
            token = safe_get(
                section,
                "continuationItemRenderer",
                "continuationEndpoint",
                "continuationCommand",
                "token",
            )
            if token:
                continuation_token = token
                continue

            items = safe_get(section, "itemSectionRenderer", "contents") or []
            for item in items:
                renderer = item.get("videoRenderer") or item.get("compactVideoRenderer")
                if renderer:
                    video = self._extract_video_renderer(renderer)
                    if video:
                        videos.append(video)
                        if len(videos) >= self.max_results:
                            return videos, continuation_token

        return videos, continuation_token

    def _parse_search_continuation(
        self, data: dict[str, Any]
    ) -> tuple[list[VideoItem], str | None]:
        videos: list[VideoItem] = []
        continuation_token: str | None = None

        # Continuation responses nest differently
        on_response_received = data.get("onResponseReceivedCommands", [])
        for cmd in on_response_received:
            append_action = cmd.get("appendContinuationItemsAction", {})
            items = append_action.get("continuationItems", [])
            for item in items:
                token = safe_get(
                    item,
                    "continuationItemRenderer",
                    "continuationEndpoint",
                    "continuationCommand",
                    "token",
                )
                if token:
                    continuation_token = token
                    continue
                section_items = safe_get(item, "itemSectionRenderer", "contents") or []
                for si in section_items:
                    renderer = si.get("videoRenderer") or si.get("compactVideoRenderer")
                    if renderer:
                        video = self._extract_video_renderer(renderer)
                        if video:
                            videos.append(video)

        return videos, continuation_token

    # ------------------------------------------------------------------
    # scrape_video_details
    # ------------------------------------------------------------------

    async def scrape_video_details(self, video_url: str) -> VideoItem:
        """
        Fetch full details for a single YouTube video URL.

        Extracts data from both ytInitialData (community/view data) and
        ytInitialPlayerResponse (player/media metadata).
        """
        logger.info("Scraping video: %s", video_url)
        resp = await self._get(video_url)
        html = resp.text

        # --- ytInitialPlayerResponse: player metadata ---
        player: dict[str, Any] = {}
        try:
            player = extract_yt_initial_player_response(html)
        except ValueError as exc:
            logger.warning("ytInitialPlayerResponse missing: %s", exc)

        video_details = player.get("videoDetails", {})
        microformat = safe_get(player, "microformat", "playerMicroformatRenderer") or {}

        video_id = video_details.get("videoId", "")
        title = video_details.get("title", "")
        description = video_details.get("shortDescription", "") or microformat.get("description", {}).get("simpleText", "")
        tags: list[str] = video_details.get("keywords", []) or []
        duration_seconds = video_details.get("lengthSeconds")
        duration = format_duration(duration_seconds) if duration_seconds else None
        channel_name = video_details.get("author", "")
        channel_id = video_details.get("channelId", "")
        channel_url = build_channel_url(channel_id) if channel_id else ""

        # Thumbnails – pick highest resolution
        raw_thumbs: list[dict] = (
            safe_get(video_details, "thumbnail", "thumbnails") or
            safe_get(microformat, "thumbnail", "thumbnails") or []
        )
        thumbnail_url = raw_thumbs[-1]["url"] if raw_thumbs else None

        # Subtitles / captions
        caption_tracks: list[dict] = (
            safe_get(player, "captions", "playerCaptionsTracklistRenderer", "captionTracks") or []
        )
        subtitles = [
            {
                "language": ct.get("name", {}).get("simpleText", ""),
                "language_code": ct.get("languageCode", ""),
                "url": ct.get("baseUrl", ""),
                "is_auto_generated": ct.get("kind", "") == "asr",
            }
            for ct in caption_tracks
        ]

        # Upload date
        upload_date = (
            microformat.get("uploadDate")
            or microformat.get("publishDate")
            or None
        )

        # --- ytInitialData: view count, likes ---
        page_data: dict[str, Any] = {}
        try:
            page_data = extract_yt_initial_data(html)
        except ValueError as exc:
            logger.warning("ytInitialData missing: %s", exc)

        views: int | None = None
        likes: int | None = None

        # View count from ytInitialData
        video_primary = safe_get(
            page_data,
            "contents",
            "twoColumnWatchNextResults",
            "results",
            "results",
            "contents",
        ) or []

        for section in video_primary:
            primary_info = section.get("videoPrimaryInfoRenderer", {})
            if primary_info:
                view_text = get_text(primary_info, "viewCount", "videoViewCountRenderer", "viewCount")
                if not view_text:
                    view_text = get_text(primary_info, "viewCount", "videoViewCountRenderer", "shortViewCount")
                views = parse_count(view_text)

                # Likes button
                like_btn = safe_get(
                    primary_info,
                    "videoActions",
                    "menuRenderer",
                    "topLevelButtons",
                )
                if like_btn:
                    for btn_item in like_btn:
                        label = safe_get(
                            btn_item,
                            "segmentedLikeDislikeButtonViewModel",
                            "likeButtonViewModel",
                            "likeButtonViewModel",
                            "toggleButtonViewModel",
                            "toggleButtonViewModel",
                            "defaultButtonViewModel",
                            "buttonViewModel",
                            "accessibilityText",
                        )
                        if label and "like" in str(label).lower():
                            likes = parse_count(str(label))
                            break
                break

        if not video_id and video_url:
            # Derive from URL
            parsed = urllib.parse.urlparse(video_url)
            qp = urllib.parse.parse_qs(parsed.query)
            video_id = qp.get("v", [""])[0]

        return VideoItem(
            video_id=video_id,
            video_url=video_url,
            title=title,
            channel_name=channel_name,
            channel_url=channel_url,
            views=views,
            likes=likes,
            upload_date=upload_date,
            duration=duration,
            description=description,
            tags=tags,
            thumbnail_url=thumbnail_url,
            subtitles=subtitles if subtitles else None,
        )

    # ------------------------------------------------------------------
    # scrape_channel
    # ------------------------------------------------------------------

    async def scrape_channel(self, channel_url: str) -> ChannelItem:
        """
        Scrape a YouTube channel's metadata and video list.

        Returns a ChannelItem with embedded VideoItems (up to ``max_results``).
        """
        logger.info("Scraping channel: %s", channel_url)
        videos_url = normalize_channel_url(channel_url)

        resp = await self._get(videos_url)
        try:
            data = extract_yt_initial_data(resp.text)
        except ValueError as exc:
            logger.error("Failed to extract ytInitialData for channel %s: %s", channel_url, exc)
            return ChannelItem(
                channel_name="",
                channel_url=channel_url,
                subscribers=None,
                total_videos=None,
                channel_description=None,
                videos=[],
            )

        # --- Header ---
        header = safe_get(data, "header", "c4TabbedHeaderRenderer") or {}
        channel_name = get_text(header, "title") or ""
        subscriber_count_text = get_text(header, "subscriberCountText")
        subscribers = subscriber_count_text or None
        channel_description = get_text(header, "description") or None
        channel_id = safe_get(header, "channelId") or ""
        canonical_url = safe_get(header, "navigationEndpoint", "browseEndpoint", "canonicalBaseUrl")
        resolved_channel_url = (
            f"{YT_BASE}{canonical_url}" if canonical_url else (build_channel_url(channel_id) if channel_id else channel_url)
        )

        # --- Video grid ---
        videos: list[VideoItem] = []
        continuation_token: str | None = None

        tabs = safe_get(data, "contents", "twoColumnBrowseResultsRenderer", "tabs") or []
        for tab in tabs:
            tab_renderer = tab.get("tabRenderer", {})
            if not tab_renderer.get("selected") and tab_renderer.get("title") != "Videos":
                continue
            grid = safe_get(tab_renderer, "content", "richGridRenderer", "contents") or []
            for item in grid:
                token = safe_get(
                    item,
                    "continuationItemRenderer",
                    "continuationEndpoint",
                    "continuationCommand",
                    "token",
                )
                if token:
                    continuation_token = token
                    continue

                renderer = safe_get(item, "richItemRenderer", "content", "videoRenderer")
                if renderer:
                    video = self._extract_video_renderer(renderer)
                    if video:
                        video["source"] = "channel"
                        videos.append(video)
            break

        # Paginate
        while len(videos) < self.max_results and continuation_token:
            payload = _innertube_payload({"continuation": continuation_token})
            try:
                cont_data = await self._post_innertube("browse", payload)
            except httpx.HTTPError as exc:
                logger.warning("Channel continuation failed: %s", exc)
                break

            new_items = safe_get(
                cont_data,
                "onResponseReceivedActions",
                0,
                "appendContinuationItemsAction",
                "continuationItems",
            ) or []
            continuation_token = None
            for item in new_items:
                token = safe_get(
                    item,
                    "continuationItemRenderer",
                    "continuationEndpoint",
                    "continuationCommand",
                    "token",
                )
                if token:
                    continuation_token = token
                    continue
                renderer = safe_get(item, "richItemRenderer", "content", "videoRenderer")
                if renderer:
                    video = self._extract_video_renderer(renderer)
                    if video:
                        video["source"] = "channel"
                        videos.append(video)

        # Total video count
        total_videos: int | None = None
        metadata = safe_get(data, "metadata", "channelMetadataRenderer") or {}
        videos_count_text = metadata.get("videosCountText", "")
        if videos_count_text:
            total_videos = parse_count(videos_count_text)

        return ChannelItem(
            channel_name=channel_name,
            channel_url=resolved_channel_url,
            subscribers=subscribers,
            total_videos=total_videos,
            channel_description=channel_description,
            videos=videos[: self.max_results],
        )

    # ------------------------------------------------------------------
    # scrape_trending
    # ------------------------------------------------------------------

    async def scrape_trending(self) -> list[VideoItem]:
        """Scrape the YouTube trending feed (max_results applies)."""
        logger.info("Scraping trending feed")
        resp = await self._get(f"{YT_BASE}/feed/trending?gl=US&hl=en")
        try:
            data = extract_yt_initial_data(resp.text)
        except ValueError as exc:
            logger.error("Failed to extract trending ytInitialData: %s", exc)
            return []

        videos: list[VideoItem] = []
        tabs = safe_get(data, "contents", "twoColumnBrowseResultsRenderer", "tabs") or []
        for tab in tabs:
            grid = safe_get(tab, "tabRenderer", "content", "sectionListRenderer", "contents") or []
            for section in grid:
                items = safe_get(section, "itemSectionRenderer", "contents") or []
                for item in items:
                    # Trending can wrap in shelfRenderer
                    shelf_items = safe_get(item, "shelfRenderer", "content", "expandedShelfContentsRenderer", "items") or []
                    for shelf_item in shelf_items:
                        renderer = shelf_item.get("videoRenderer")
                        if renderer:
                            video = self._extract_video_renderer(renderer)
                            if video:
                                video["source"] = "trending"
                                videos.append(video)
                    # Direct videoRenderer
                    renderer = item.get("videoRenderer")
                    if renderer:
                        video = self._extract_video_renderer(renderer)
                        if video:
                            video["source"] = "trending"
                            videos.append(video)
                    if len(videos) >= self.max_results:
                        return videos[: self.max_results]

        # Also try richGrid layout
        if not videos:
            rich_grid = safe_get(
                data,
                "contents",
                "twoColumnBrowseResultsRenderer",
                "tabs",
                0,
                "tabRenderer",
                "content",
                "richGridRenderer",
                "contents",
            ) or []
            for item in rich_grid:
                renderer = safe_get(item, "richItemRenderer", "content", "videoRenderer")
                if renderer:
                    video = self._extract_video_renderer(renderer)
                    if video:
                        video["source"] = "trending"
                        videos.append(video)
                if len(videos) >= self.max_results:
                    break

        logger.info("Trending → %d videos", len(videos))
        return videos[: self.max_results]

    # ------------------------------------------------------------------
    # scrape_comments
    # ------------------------------------------------------------------

    async def scrape_comments(
        self, video_id: str, max_comments: int = 20
    ) -> list[CommentItem]:
        """
        Scrape comments for a video using the Innertube API.

        YouTube loads comments via a separate continuation request; we first
        fetch the video page to obtain the initial comment continuation token,
        then paginate.
        """
        logger.info("Scraping comments for video %s (max=%d)", video_id, max_comments)
        video_url = build_video_url(video_id)

        resp = await self._get(video_url)
        try:
            data = extract_yt_initial_data(resp.text)
        except ValueError:
            return []

        # Find the comment section continuation token
        continuation_token: str | None = None
        engagements = safe_get(
            data,
            "engagementPanels",
        ) or []
        for panel in engagements:
            token = safe_get(
                panel,
                "engagementPanelSectionListRenderer",
                "content",
                "sectionListRenderer",
                "contents",
                0,
                "itemSectionRenderer",
                "contents",
                0,
                "continuationItemRenderer",
                "continuationEndpoint",
                "continuationCommand",
                "token",
            )
            if token:
                continuation_token = token
                break

        # Also try from page contents
        if not continuation_token:
            contents = safe_get(data, "contents", "twoColumnWatchNextResults", "results", "results", "contents") or []
            for section in contents:
                for item in (safe_get(section, "itemSectionRenderer", "contents") or []):
                    token = safe_get(
                        item,
                        "continuationItemRenderer",
                        "continuationEndpoint",
                        "continuationCommand",
                        "token",
                    )
                    if token:
                        continuation_token = token
                        break

        if not continuation_token:
            logger.warning("No comment continuation token found for %s", video_id)
            return []

        comments: list[CommentItem] = []
        while continuation_token and len(comments) < max_comments:
            payload = _innertube_payload({"continuation": continuation_token})
            try:
                cont_data = await self._post_innertube("next", payload)
            except httpx.HTTPError as exc:
                logger.warning("Comment continuation failed: %s", exc)
                break

            continuation_token = None
            # Parse the comment thread renderers
            actions = cont_data.get("onResponseReceivedEndpoints", [])
            for action in actions:
                items = (
                    safe_get(action, "reloadContinuationItemsCommand", "continuationItems")
                    or safe_get(action, "appendContinuationItemsAction", "continuationItems")
                    or []
                )
                for item in items:
                    token = safe_get(
                        item,
                        "continuationItemRenderer",
                        "continuationEndpoint",
                        "continuationCommand",
                        "token",
                    )
                    if token:
                        continuation_token = token
                        continue

                    thread = safe_get(item, "commentThreadRenderer", "comment", "commentRenderer") or {}
                    if thread:
                        text = get_text(thread, "contentText")
                        author = get_text(thread, "authorText")
                        likes_text = get_text(thread, "voteCount")
                        published = get_text(thread, "publishedTimeText")
                        is_pinned = bool(thread.get("pinnedCommentBadge"))
                        comments.append(
                            CommentItem(
                                author=author,
                                text=text,
                                likes=parse_count(likes_text),
                                published_time=published or None,
                                is_pinned=is_pinned,
                            )
                        )
                        if len(comments) >= max_comments:
                            break

        logger.info("Comments for %s → %d", video_id, len(comments))
        return comments[:max_comments]

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------

    async def run(self, input_data: dict[str, Any]) -> list[dict]:
        """
        Read Apify Actor input and dispatch all scraping tasks concurrently.

        Keys read from input_data
        -------------------------
        searchQueries   : list[str]
        channelUrls     : list[str]
        videoUrls       : list[str]
        scrape_trending  : bool   (default False)
        maxComments     : int    (default 0 — disable comment scraping)
        """
        search_queries: list[str] = input_data.get("searchQueries") or []
        channel_urls: list[str] = input_data.get("channelUrls") or []
        video_urls: list[str] = input_data.get("videoUrls") or []
        do_trending: bool = bool(input_data.get("scrapeTrending", False))
        max_comments: int = int(input_data.get("maxComments", 0))

        logger.info(
            "Tasks: %d search queries, %d channel URLs, %d video URLs, trending=%s",
            len(search_queries), len(channel_urls), len(video_urls), do_trending,
        )

        results: list[dict] = []

        async def _search(q: str) -> None:
            try:
                videos = await self.search_videos(q)
                for v in videos:
                    v["source"] = "search"
                    v["search_query"] = q
                    if max_comments and v.get("video_id"):
                        v["comments"] = await self.scrape_comments(
                            v["video_id"], max_comments
                        )
                    results.append(dict(v))
            except Exception as exc:
                logger.error("search_videos(%r) failed: %s", q, exc)

        async def _channel(url: str) -> None:
            try:
                channel = await self.scrape_channel(url)
                results.append(dict(channel))
            except Exception as exc:
                logger.error("scrape_channel(%r) failed: %s", url, exc)

        async def _video(url: str) -> None:
            try:
                video = await self.scrape_video_details(url)
                video["source"] = "video_url"
                if max_comments and video.get("video_id"):
                    video["comments"] = await self.scrape_comments(
                        video["video_id"], max_comments
                    )
                results.append(dict(video))
            except Exception as exc:
                logger.error("scrape_video_details(%r) failed: %s", url, exc)

        tasks: list[Any] = []
        for q in search_queries:
            tasks.append(_search(q))
        for url in channel_urls:
            tasks.append(_channel(url))
        for url in video_urls:
            tasks.append(_video(url))
        if do_trending:
            async def _trending() -> None:
                try:
                    videos = await self.scrape_trending()
                    for v in videos:
                        results.append(dict(v))
                except Exception as exc:
                    logger.error("scrape_trending() failed: %s", exc)
            tasks.append(_trending())

        if tasks:
            await asyncio.gather(*tasks)
        else:
            logger.warning("No input provided (searchQueries / channelUrls / videoUrls are all empty).")

        logger.info("Run complete — %d result(s) collected.", len(results))
        return results
