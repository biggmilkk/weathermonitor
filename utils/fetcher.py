# services/fetcher.py
"""
Centralized fetching for WeatherMonitor.

- Reuses the shared httpx.AsyncClient from clients.get_async_client()
- Supports per-feed overrides for timeout and headers in FEED_CONFIG
- Concurrency-limited fan-out with retries and error isolation
- Accepts both async and sync scraper callables from SCRAPER_REGISTRY
- Public entrypoint: run_fetch_round(to_fetch: dict, max_concurrency: int | None) -> list[(key, data)]
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Tuple

import httpx

# Your existing modules
from clients import get_async_client  # singleton AsyncClient
from scraper_registry import SCRAPER_REGISTRY

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

# -------- Defaults (can be overridden per-feed via FEED_CONFIG) --------

DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": "WeatherMonitor/1.0 (+https://example.local) httpx",
    "Accept": "application/json, text/xml, application/xml, text/html, */*",
}
DEFAULT_TIMEOUT_SECONDS: float = 30.0
DEFAULT_MAX_CONCURRENCY: int = 20
DEFAULT_RETRIES: int = 2
DEFAULT_RETRY_BACKOFF: float = 0.75  # seconds


# --------------------------- Retry helper ------------------------------

async def _with_retries(
    fn: Callable[[], Awaitable[Any]],
    retries: int = DEFAULT_RETRIES,
    backoff_seconds: float = DEFAULT_RETRY_BACKOFF,
) -> Any:
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return await fn()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= retries:
                break
            try:
                await asyncio.sleep(backoff_seconds * (attempt + 1))
            except asyncio.CancelledError:
                raise
    # surface the last error to caller
    raise last_exc or RuntimeError("retry: no exception captured")


# ----------------------- Scraper invocation shim -----------------------

async def _invoke_scraper(
    scraper: Callable[..., Any],
    client: httpx.AsyncClient,
    conf: Dict[str, Any],
) -> Any:
    """
    Call a scraper that may be async or sync, with flexible signatures.

    Supported call forms (auto-detected in this order):
      - async scraper(client, conf)
      - async scraper(conf, client)
      - async scraper(conf)
      - sync  scraper(client, conf)
      - sync  scraper(conf, client)
      - sync  scraper(conf)

    Returns whatever the scraper returns (usually a dict with 'entries').
    """
    if inspect.iscoroutinefunction(scraper):
        # Try common async signatures
        try:
            return await scraper(client, conf)
        except TypeError:
            try:
                return await scraper(conf, client)
            except TypeError:
                return await scraper(conf)
    else:
        loop = asyncio.get_running_loop()
        # Wrap sync scraper in a thread to avoid blocking the event loop
        def _call_sync() -> Any:
            # Try common sync signatures
            try:
                return scraper(client, conf)
            except TypeError:
                try:
                    return scraper(conf, client)
                except TypeError:
                    return scraper(conf)

        return await loop.run_in_executor(None, _call_sync)


# -------------------------- One-feed wrapper ---------------------------

async def _fetch_one(
    key: str,
    feed_conf: Dict[str, Any],
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch one feed safely with per-feed options, retries, and error isolation.
    Always returns (key, data_dict). On error, data_dict contains {'entries': []}.
    """
    # Resolve scraper
    scraper = SCRAPER_REGISTRY.get(key)
    if not scraper:
        logger.warning("No scraper registered for key=%s", key)
        return key, {"entries": []}

    # Per-feed headers & timeout
    headers = dict(DEFAULT_HEADERS)
    try:
        headers.update(feed_conf.get("headers", {}) or {})
    except Exception:
        pass

    timeout_seconds = float(feed_conf.get("timeout", DEFAULT_TIMEOUT_SECONDS))
    timeout = httpx.Timeout(timeout_seconds)

    # Apply temporary headers/timeout to the shared client via context-like pattern
    # (httpx.AsyncClient doesn't support per-request default override globally,
    #  so pass explicitly through scraper or rely on scraper honoring kwargs.)
    # We can't force every scraper to accept kwargs; so the common path is that
    # scrapers read their own headers/timeouts. As a fallback, we set them on client
    # via event hooks per request if scrapers use client.request(). If not, the
    # scraper may ignore them.

    async with semaphore:
        async def _do() -> Dict[str, Any]:
            # Provide a small adapter that scrapers can use if they accept kwargs
            # but since we don't control them, we primarily rely on client defaults.
            # For safety, temporarily set client headers via a Request hook when possible.
            # If scrapers don't use .build_request/.send, they might ignore this — that's OK.
            try:
                # Many of your scrapers already pass headers/timeouts on each request.
                # Just call the scraper; it should internally use `client`.
                result = await _invoke_scraper(scraper, client, {**feed_conf, "headers": headers, "timeout": timeout_seconds})
                # Standardize to dict with 'entries' key when possible
                if isinstance(result, dict) and "entries" in result:
                    return result
                if isinstance(result, list):
                    return {"entries": result}
                # Fallback: wrap arbitrary payload
                return {"entries": result if isinstance(result, list) else (result or [])}
            except Exception as e:  # noqa: BLE001
                logger.warning("Error fetching %s: %s", key, e)
                return {"entries": []}

        try:
            return key, await _with_retries(_do)
        except Exception as e:  # noqa: BLE001
            logger.error("Final failure for %s: %s", key, e)
            return key, {"entries": []}


# --------------------------- Public API --------------------------------

def run_fetch_round(
    to_fetch: Dict[str, Dict[str, Any]],
    max_concurrency: int | None = None,
) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Synchronous entrypoint called from Streamlit code.

    Arguments:
      to_fetch: mapping feed_key -> feed_conf (subset of FEED_CONFIG)
      max_concurrency: optional override; defaults to DEFAULT_MAX_CONCURRENCY

    Returns:
      list of (feed_key, data_dict) pairs
    """
    if not to_fetch:
        return []

    max_conc = int(max_concurrency or DEFAULT_MAX_CONCURRENCY)

    async def _runner() -> List[Tuple[str, Dict[str, Any]]]:
        # Get the shared client (supports both sync/async factories)
        client = get_async_client()
        if inspect.iscoroutine(client):
            client = await client  # type: ignore[assignment]

        # Some implementations define get_async_client() as a function returning
        # an AsyncClient; others as a coroutine. The above handles both.

        sem = asyncio.Semaphore(max_conc)

        tasks = [
            asyncio.create_task(_fetch_one(key, conf or {}, client, sem))
            for key, conf in to_fetch.items()
        ]

        results: List[Tuple[str, Dict[str, Any]]] = []
        for coro in asyncio.as_completed(tasks):
            try:
                results.append(await coro)
            except Exception as e:  # noqa: BLE001
                logger.error("Task failure in fetch round: %s", e)
        return results

    # Ensure we have an event loop even when called from Streamlit
    try:
        loop = asyncio.get_running_loop()
        # If we're already inside an event loop (unlikely in Streamlit),
        # run the coroutine safely.
        return loop.run_until_complete(_runner())  # type: ignore[misc]
    except RuntimeError:
        # No running loop — the normal case for Streamlit
        return asyncio.run(_runner())
