# utils/fetcher.py
"""
Centralized fetching for WeatherMonitor.

- Reuses the shared httpx.AsyncClient from utils.clients.get_async_client()
- Supports per-feed overrides for timeout and headers in FEED_CONFIG
- Concurrency-limited fan-out with retries and error isolation
- Accepts both async and sync scraper callables from SCRAPER_REGISTRY
- Public entrypoint: run_fetch_round(to_fetch: dict, max_concurrency: int | None) -> list[(key, data)]
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Awaitable, Callable, Dict, List, Tuple

import httpx

# Relative imports (since this file lives in utils/)
from .clients import get_async_client  # singleton AsyncClient
from .scraper_registry import SCRAPER_REGISTRY

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

# -------- Defaults (can be overridden per-feed via FEED_CONFIG) --------

DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": "WeatherMonitor/1.0 (httpx)",
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
            await asyncio.sleep(backoff_seconds * (attempt + 1))
    raise last_exc or RuntimeError("retry: no exception captured")


# ----------------------- Scraper invocation shim -----------------------

async def _invoke_scraper(
    scraper,  # Callable[..., Any]
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
        try:
            return await scraper(client, conf)
        except TypeError:
            try:
                return await scraper(conf, client)
            except TypeError:
                return await scraper(conf)
    else:
        loop = asyncio.get_running_loop()

        def _call_sync() -> Any:
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

    async with semaphore:
        async def _do() -> Dict[str, Any]:
            try:
                # Pass merged headers/timeout to scraper via conf; most scrapers already read these.
                result = await _invoke_scraper(
                    scraper,
                    client,
                    {**feed_conf, "headers": headers, "timeout": timeout_seconds, "httpx_timeout": timeout},
                )
                if isinstance(result, dict) and "entries" in result:
                    return result
                if isinstance(result, list):
                    return {"entries": result}
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
        client = get_async_client()
        if inspect.iscoroutine(client):
            client = await client  # type: ignore[assignment]

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

    try:
        # Normal Streamlit path: no running loop
        return asyncio.run(_runner())
    except RuntimeError:
        # If already inside an event loop, create a task group and block until done
        loop = asyncio.get_running_loop()
        return loop.run_until_complete(_runner())  # type: ignore[misc]
