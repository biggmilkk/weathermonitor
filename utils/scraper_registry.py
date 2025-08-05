import json
import asyncio

from scraper.environment_canada import scrape_ec_async
from scraper.meteoalarm import scrape_meteoalarm_async
from scraper.nws_active_alerts import scrape_nws_async
from scraper.cma import scrape_cma_async

# Mapping of scraper types to their async functions
SCRAPER_REGISTRY = {
    "json": lambda conf, client: scrape_nws_async(conf.get("url"), client),
    "ec_async": lambda conf, client: scrape_ec_async(
        json.load(open(conf.get("source_file"))), client
    ),
    "rss_meteoalarm": lambda conf, client: scrape_meteoalarm_async(conf, client),
    "rss_cma": lambda conf, client: scrape_cma_async(conf, client),
}

async def fetch_all_async(configs: dict, max_concurrency: int = 20):
    """
    Run all registered scrapers in parallel, bounded by max_concurrency.

    Args:
        configs: Mapping of scraper names to their config dict.
        max_concurrency: Maximum simultaneous HTTP fetches.

    Returns:
        List of (scraper_name, data_dict) tuples.
    """
    # Delay import to avoid circular imports
    from utils.clients import get_async_client
    sem = asyncio.Semaphore(max_concurrency)
    client = get_async_client()

    async def bound_fetch(name: str, func, conf: dict):
        async with sem:
            try:
                result = await func(conf, client)
            except Exception as e:
                result = {"entries": [], "error": str(e), "source": conf}
            return name, result

    tasks = [
        asyncio.create_task(bound_fetch(name, func, configs.get(name, {})))
        for name, func in SCRAPER_REGISTRY.items()
    ]

    return await asyncio.gather(*tasks)(configs: dict, max_concurrency: int = 20):
    """
    Run all registered scrapers in parallel, bounded by max_concurrency.

    Args:
        configs: Mapping of scraper names to their config dict.
        max_concurrency: Maximum simultaneous HTTP fetches.

    Returns:
        List of (scraper_name, data_dict) tuples.
    """
    sem = asyncio.Semaphore(max_concurrency)
    client = get_async_client()

    async def bound_fetch(name: str, func, conf: dict):
        async with sem:
            try:
                result = await func(conf, client)
            except Exception as e:
                result = {"entries": [], "error": str(e), "source": conf}
            return name, result

    tasks = [
        asyncio.create_task(bound_fetch(name, func, configs.get(name, {})))
        for name, func in SCRAPER_REGISTRY.items()
    ]

    return await asyncio.gather(*tasks)
