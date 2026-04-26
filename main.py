"""
main.py — Apify Actor entry point for the YouTube Scraper.

Uses apify-client (the lightweight REST client) instead of the full apify SDK
to avoid the crawlee / pydantic version incompatibility present in apify v2.

Environment variables set automatically by the Apify platform
--------------------------------------------------------------
APIFY_IS_AT_HOME              "1" when running inside Apify
APIFY_TOKEN                   API token
APIFY_DEFAULT_KEY_VALUE_STORE_ID  KV store that holds INPUT and OUTPUT
APIFY_DEFAULT_DATASET_ID      Dataset to push results into
APIFY_PROXY_PASSWORD          Password for Apify proxy (when proxy enabled)

Local development
-----------------
Run `python main.py`; the script falls back to INPUT_EXAMPLE.json and prints
results to stdout.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("youtube_scraper")

from scraper import YouTubeScraper


# ---------------------------------------------------------------------------
# Apify platform helpers (via apify-client REST client)
# ---------------------------------------------------------------------------

def _is_on_apify() -> bool:
    return os.getenv("APIFY_IS_AT_HOME", "0") == "1"


def _get_apify_input() -> dict[str, Any]:
    """Fetch actor input from the Apify key-value store."""
    try:
        from apify_client import ApifyClient
        token = os.environ["APIFY_TOKEN"]
        store_id = os.environ["APIFY_DEFAULT_KEY_VALUE_STORE_ID"]
        client = ApifyClient(token)
        record = client.key_value_store(store_id).get_record("INPUT")
        if record and record.get("value"):
            value = record["value"]
            # The platform may return a string (JSON) or already-parsed dict
            if isinstance(value, str):
                return json.loads(value)
            return value
    except Exception as exc:
        logger.warning("Could not fetch Apify input: %s", exc)
    return {}


def _push_apify_results(results: list[dict]) -> None:
    """Push results to the Apify default dataset."""
    try:
        from apify_client import ApifyClient
        token = os.environ["APIFY_TOKEN"]
        dataset_id = os.environ["APIFY_DEFAULT_DATASET_ID"]
        client = ApifyClient(token)
        # push_items accepts a list; split into chunks of 1000 to stay within limits
        chunk_size = 1000
        for i in range(0, len(results), chunk_size):
            client.dataset(dataset_id).push_items(results[i : i + chunk_size])
            logger.info("Pushed items %d–%d to dataset", i + 1, min(i + chunk_size, len(results)))
    except Exception as exc:
        logger.error("Failed to push results to Apify dataset: %s", exc)
        raise


def _build_proxy_url(proxy_config: dict[str, Any] | None) -> str | None:
    """
    Construct an Apify proxy URL from the proxyConfiguration input field.

    The Apify platform exposes APIFY_PROXY_PASSWORD when proxy is enabled.
    Proxy URL format: http://groups-<GROUP>:<password>@proxy.apify.com:8000
    """
    if not proxy_config:
        return None

    use_apify_proxy = proxy_config.get("useApifyProxy", False)
    if not use_apify_proxy:
        # Custom proxies
        custom_urls: list[str] = proxy_config.get("proxyUrls", [])
        return custom_urls[0] if custom_urls else None

    password = os.getenv("APIFY_PROXY_PASSWORD", "")
    if not password:
        logger.warning("useApifyProxy=true but APIFY_PROXY_PASSWORD not set")
        return None

    groups: list[str] = proxy_config.get("apifyProxyGroups", [])
    if groups:
        username = f"groups-{'+'.join(groups)}"
    else:
        username = "auto"

    country = proxy_config.get("apifyProxyCountry", "")
    if country:
        username += f",country-{country}"

    return f"http://{username}:{password}@proxy.apify.com:8000"


# ---------------------------------------------------------------------------
# Local development helpers
# ---------------------------------------------------------------------------

def _read_local_input() -> dict[str, Any]:
    """Read input from apify_storage or INPUT_EXAMPLE.json for local runs."""
    storage_path = os.path.join(
        "apify_storage", "key_value_stores", "default", "INPUT.json"
    )
    if os.path.exists(storage_path):
        with open(storage_path, encoding="utf-8") as f:
            return json.load(f)
    if os.path.exists("INPUT_EXAMPLE.json"):
        logger.info("Using INPUT_EXAMPLE.json as actor input")
        with open("INPUT_EXAMPLE.json", encoding="utf-8") as f:
            return json.load(f)
    logger.warning("No input file found — using empty input")
    return {}


def _print_results(results: list[dict]) -> None:
    logger.info("=== Results (%d item(s)) ===", len(results))
    print(json.dumps(results, indent=2, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Core run logic (shared between Apify and local modes)
# ---------------------------------------------------------------------------

async def _run(input_data: dict[str, Any], proxy_url: str | None) -> list[dict]:
    scraper = YouTubeScraper(
        proxy_url=proxy_url,
        max_results=int(input_data.get("maxResults", 20)),
        sort_by=input_data.get("sortBy", "relevance"),
        upload_date_filter=input_data.get("uploadDateFilter", "all"),
        concurrency=int(input_data.get("concurrency", 5)),
    )
    async with scraper:
        return await scraper.run(input_data)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

async def main() -> None:
    if _is_on_apify():
        logger.info("Running on Apify platform")
        input_data = _get_apify_input()
        logger.info("Input keys: %s", list(input_data.keys()))

        proxy_url = _build_proxy_url(input_data.get("proxyConfiguration"))
        if proxy_url:
            logger.info("Proxy configured: %s…", proxy_url[:40])

        results = await _run(input_data, proxy_url)

        if results:
            logger.info("Pushing %d item(s) to Apify dataset", len(results))
            _push_apify_results(results)
        else:
            logger.warning("No results collected — dataset will be empty")

        logger.info("Actor finished successfully")
    else:
        logger.info("Running in LOCAL mode")
        input_data = _read_local_input()
        results = await _run(input_data, proxy_url=None)
        _print_results(results)


if __name__ == "__main__":
    asyncio.run(main())
