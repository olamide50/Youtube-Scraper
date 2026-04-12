"""
main.py — Apify Actor entry point for the YouTube Scraper.

Usage
-----
Locally (without an Apify account):
    Create the file  apify_storage/key_value_stores/default/INPUT.json
    with your input, then run:  python main.py

On Apify Platform:
    Deploy the actor and provide input via the Apify Console / API.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

# ---------------------------------------------------------------------------
# Logging setup (Apify SDK captures stdout; use INFO level by default)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("youtube_scraper")

# ---------------------------------------------------------------------------
# Apify import with graceful fallback for local development
# ---------------------------------------------------------------------------
try:
    from apify import Actor
    _HAS_APIFY = True
except ImportError:
    _HAS_APIFY = False
    logger.warning("apify package not installed — running in local mode")

from scraper import YouTubeScraper


# ---------------------------------------------------------------------------
# Local mode helper (reads INPUT.json from disk)
# ---------------------------------------------------------------------------

def _read_local_input() -> dict:
    """Read INPUT_EXAMPLE.json or apify_storage INPUT.json for local runs."""
    storage_path = os.path.join(
        "apify_storage", "key_value_stores", "default", "INPUT.json"
    )
    if os.path.exists(storage_path):
        import json
        with open(storage_path, encoding="utf-8") as f:
            return json.load(f)
    example_path = "INPUT_EXAMPLE.json"
    if os.path.exists(example_path):
        import json
        logger.info("Using INPUT_EXAMPLE.json as input")
        with open(example_path, encoding="utf-8") as f:
            return json.load(f)
    logger.warning("No input file found — using empty input")
    return {}


def _print_local_results(results: list[dict]) -> None:
    """Pretty-print results to stdout when running locally."""
    import json
    logger.info("=== Results (%d items) ===", len(results))
    print(json.dumps(results, indent=2, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Main coroutine
# ---------------------------------------------------------------------------

async def main() -> None:
    if _HAS_APIFY:
        await _run_as_actor()
    else:
        await _run_locally()


async def _run_as_actor() -> None:
    """Full Apify Actor run: reads input, scrapes, pushes to dataset."""
    async with Actor:
        Actor.log.info("YouTube Scraper actor starting")

        input_data: dict = await Actor.get_input() or {}
        Actor.log.info("Input received: %s", list(input_data.keys()))

        # --- Proxy setup ---
        proxy_url: str | None = None
        proxy_config = input_data.get("proxyConfiguration")
        if proxy_config:
            try:
                proxy_configuration = await Actor.create_proxy_configuration(
                    actor_proxy_input=proxy_config
                )
                proxy_url = await proxy_configuration.new_url()
                Actor.log.info("Proxy configured: %s", proxy_url[:30] + "…")
            except Exception as exc:
                Actor.log.warning("Proxy setup failed, continuing without proxy: %s", exc)

        # --- Build scraper ---
        scraper = YouTubeScraper(
            proxy_url=proxy_url,
            max_results=int(input_data.get("maxResults", 20)),
            sort_by=input_data.get("sortBy", "relevance"),
            upload_date_filter=input_data.get("uploadDateFilter", "all"),
            concurrency=int(input_data.get("concurrency", 5)),
        )

        # --- Run ---
        async with scraper:
            results = await scraper.run(input_data)

        # --- Push results ---
        if results:
            Actor.log.info("Pushing %d item(s) to dataset", len(results))
            await Actor.push_data(results)
        else:
            Actor.log.warning("No results collected")

        Actor.log.info("YouTube Scraper actor finished")


async def _run_locally() -> None:
    """Local development run: reads INPUT_EXAMPLE.json, prints to stdout."""
    logger.info("Running in LOCAL mode (no Apify SDK)")

    input_data = _read_local_input()
    logger.info("Input: %s", input_data)

    scraper = YouTubeScraper(
        proxy_url=None,
        max_results=int(input_data.get("maxResults", 10)),
        sort_by=input_data.get("sortBy", "relevance"),
        upload_date_filter=input_data.get("uploadDateFilter", "all"),
        concurrency=3,
    )

    async with scraper:
        results = await scraper.run(input_data)

    _print_local_results(results)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(main())
