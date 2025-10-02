# utils/fetcher.py
"""
Centralized fetching for WeatherMonitor.

- Reuses the shared httpx.AsyncClient from utils.clients.get_async_client()
- Looks up scrapers by FEED 'type' (primary) then by feed key (fallback)
- Concurrency-limited fan-out with retries and error isolation
- Accepts both async and sync scrapers (including callable objects with async __call__)
- Public entrypoint: run_fetch_round(to_fetch: dict, max_concurrency: int | None) -> list[(key, data)]
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Awaitable, Callable, Dict, List, Tuple

import httpx

from .clients import get_async_client
from .scraper_registry import SCRAPER_REGISTRY

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

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
    scraper: Any,  # function or object with __call__
    client: httpx.AsyncClient,
    conf: Dict[str, Any],
) -> Any:
    """
    Call a scraper that may be async or sync, function or callable object.

    We try the common signatures in order and await if the result is awaitable:
      (client, conf) -> ...
      (conf, client) -> ...
      (conf)         -> ...
    """

    async def _maybe_await(res: Any) -> Any:
        if inspect.isawaitable(res):
            return await res
        return res

    # Try async/sync callable in a single path; if signature doesn't match, try next
    try:
        return await _maybe_await(scraper(client, conf))
    except TypeError:
        try:
            return await _maybe_await(scraper(conf, client))
        except TypeError:
            return await _maybe_await(scraper(conf))


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
    # Use feed TYPE to find the scraper (primary), fall back to feed KEY
    scraper_key = (feed_conf.get("type") or "").strip() or key
    scraper = SCRAPER_REGISTRY.get(scraper_key) or SCRAPER_REGISTRY.get(key)
    if not scraper:
        logger.warning("No scraper registered for key=%s (type=%s)", key, feed_conf.get("type"))
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
                # Pass merged headers/timeout via conf; scrapers already read these.
                result = await _invoke_scraper(
                    scraper,
                    client,
                    {**feed_conf, "headers": headers, "timeout": timeout_seconds, "httpx_timeout": timeout},
                )
                # Normalize to {'entries': ...}
                if isinstance(result, dict) and "entries" in result:
                    return result
                if isinstance(result, list):
                    return {"entries": result}
                # Wrap arbitrary payload
                return {"entries": result if isinstance(result, list) else (result or [])}
            except Exception as e:  # noqa: BLE001
                logger.warning("Error fetching %s (type=%s): %s", key, feed_conf.get("type"), e)
                return {"entries": []}

        try:
            return key, await _with_retries(_do)
        except Exception as e:  # noqa: BLE001
            logger.error("Final failure for %s (type=%s): %s", key, feed_conf.get("type"), e)
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

    # Normal Streamlit path: no running loop
    try:
        return asyncio.run(_runner())
    except RuntimeError:
        # If already inside a running loop (rare), reuse it
        loop = asyncio.get_running_loop()
        return loop.run_until_complete(_runner())  # type: ignore[misc]
