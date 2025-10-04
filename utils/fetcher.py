# utils/fetcher.py
"""
Centralized fetching for WeatherMonitor (fixed):

- Looks up scrapers by FEED type (primary), then by key (fallback)
- Calls scrapers as await scraper(conf, client)  <-- correct order for ScraperEntry
- Builds call_conf like the original app: merges nested "conf", strips label/type
- Uses a fresh httpx.AsyncClient per round to avoid cross-loop issues
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Tuple

import httpx

from .scraper_registry import SCRAPER_REGISTRY

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": "WeatherMonitor/1.0 (httpx)",
    "Accept": "application/json, text/xml, application/xml, text/html, */*",
}
DEFAULT_TIMEOUT_SECONDS: float = 15.0  # tightened from 30s for snappier rounds
DEFAULT_MAX_CONCURRENCY: int = 20
DEFAULT_RETRIES: int = 2
DEFAULT_RETRY_BACKOFF: float = 0.75  # seconds


def _build_call_conf(feed_conf: Dict[str, Any]) -> Dict[str, Any]:
    """
    Match the original behavior:
      - Drop UI-only keys: label, type
      - Flatten nested 'conf' dict into the call args
    """
    call_conf: Dict[str, Any] = {}
    for k, v in (feed_conf or {}).items():
        if k in ("label", "type"):
            continue
        if k == "conf" and isinstance(v, dict):
            call_conf.update(v)
        else:
            call_conf[k] = v
    return call_conf


async def _with_retries(fn, retries: int = DEFAULT_RETRIES, backoff: float = DEFAULT_RETRY_BACKOFF):
    last_exc: BaseException | None = None
    for attempt in range(retries + 1):
        try:
            return await fn()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= retries:
                break
            await asyncio.sleep(backoff * (attempt + 1))
    raise last_exc or RuntimeError("retry: unknown failure")


async def _fetch_one(
    key: str,
    feed_conf: Dict[str, Any],
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch one feed with isolation. Always returns (key, {'entries': ...}).
    """
    # 1) Resolve scraper by TYPE first, then fallback to KEY
    scraper_key = (feed_conf.get("type") or "").strip() or key
    scraper = SCRAPER_REGISTRY.get(scraper_key) or SCRAPER_REGISTRY.get(key)
    if not scraper:
        logger.warning("No scraper registered for key=%s (type=%s)", key, feed_conf.get("type"))
        return key, {"entries": []}

    # 2) Build the exact conf the scraper expects (old app behavior)
    call_conf = _build_call_conf(feed_conf)

    # 3) Pass useful defaults; most scrapers read timeout/headers from conf
    try:
        headers = {**DEFAULT_HEADERS, **(feed_conf.get("headers") or {})}
    except Exception:
        headers = dict(DEFAULT_HEADERS)
    timeout_seconds = float(feed_conf.get("timeout", DEFAULT_TIMEOUT_SECONDS))
    call_conf.setdefault("headers", headers)
    call_conf.setdefault("timeout", timeout_seconds)

    async with sem:
        async def _do() -> Dict[str, Any]:
            try:
                # >>> Correct order for ScraperEntry: (conf, client) <<<
                result = await scraper(call_conf, client)
                # Normalize to {'entries': ...}
                if isinstance(result, dict) and "entries" in result:
                    return result
                if isinstance(result, list):
                    return {"entries": result}
                return {"entries": result if isinstance(result, list) else (result or [])}
            except Exception as e:  # noqa: BLE001
                logger.warning("Error fetching %s (type=%s): %s", key, feed_conf.get("type"), e)
                return {"entries": []}

        try:
            return key, await _with_retries(_do)
        except Exception as e:  # noqa: BLE001
            logger.error("Final failure for %s (type=%s): %s", key, feed_conf.get("type"), e)
            return key, {"entries": []}


def run_fetch_round(
    to_fetch: Dict[str, Dict[str, Any]],
    max_concurrency: int | None = None,
) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Synchronous entrypoint from Streamlit.

    Returns list[(feed_key, {'entries': ...})].
    """
    if not to_fetch:
        return []

    max_conc = int(max_concurrency or DEFAULT_MAX_CONCURRENCY)

    async def _runner() -> List[Tuple[str, Dict[str, Any]]]:
        # Create a fresh client per round to avoid cross-loop issues with cached clients.
        limits = httpx.Limits(max_connections=max_conc, max_keepalive_connections=max_conc)
        transport = httpx.AsyncHTTPTransport(retries=3)
        timeout = httpx.Timeout(DEFAULT_TIMEOUT_SECONDS)

        async with httpx.AsyncClient(limits=limits, transport=transport, timeout=timeout) as client:
            sem = asyncio.Semaphore(max_conc)
            tasks = [asyncio.create_task(_fetch_one(k, (conf or {}), client, sem)) for k, conf in to_fetch.items()]

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
        # If there is an active loop (e.g. nest_asyncio), re-use it
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(_runner())  # type: ignore[misc]
