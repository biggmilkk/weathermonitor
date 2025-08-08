import json
from importlib import import_module
from typing import Callable, Dict


class ScraperEntry:
    """
    Lazily imports scraper modules and invokes the specified async function.
    Optionally applies a loader to transform conf before calling the scraper.
    """
    def __init__(self, module_name: str, func_name: str, loader: Callable[[dict], dict] = None):
        self.module_name = module_name
        self.func_name = func_name
        self.loader = loader

    async def __call__(self, conf: dict, client) -> dict:
        # Load or transform conf if a loader is provided
        conf_arg = self.loader(conf) if self.loader else conf
        mod = import_module(f"scraper.{self.module_name}")
        fn = getattr(mod, self.func_name)
        return await fn(conf_arg, client)


def _load_ec_conf(conf: dict) -> dict:
    """
    Loads the Environment Canada feed definition JSON.
    """
    source_file = conf.get("source_file")
    if not source_file:
        raise ValueError("Missing 'source_file' in EC config")
    with open(source_file, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_jma_conf(conf: dict) -> dict:
    """
    Loads JMA warning feed definition, merging area codes and weather mapping.
    """
    area_file = conf.get("area_code_file")
    weather_file = conf.get("weather_file")
    if not area_file or not weather_file:
        raise ValueError("Missing 'area_code_file' or 'weather_file' in JMA config")

    # Load area codes mapping (JSON)
    with open(area_file, "r", encoding="utf-8") as f:
        area_codes = json.load(f)

    # Load weather phenomena mapping (JSON)
    with open(weather_file, "r", encoding="utf-8") as f:
        weather_map = json.load(f)

    # Merge original config with loaded data
    merged_conf = dict(conf)
    merged_conf["area_codes"] = area_codes
    merged_conf["weather"] = weather_map
    return merged_conf


SCRAPER_REGISTRY: Dict[str, ScraperEntry] = {
    # NWS active alerts (json)
    "json": ScraperEntry("nws_active_alerts", "scrape_nws_async"),

    # Environment Canada RSS feeds
    "ec_async": ScraperEntry(
        "environment_canada", "scrape_ec_async", loader=_load_ec_conf),

    # MeteoAlarm countries
    "rss_meteoalarm": ScraperEntry("meteoalarm", "scrape_meteoalarm_async"),

    # China Meteorological Admin regions
    "rss_cma": ScraperEntry("cma", "scrape_cma_async"),

    # BOM multi-state Australia
    "rss_bom_multi": ScraperEntry("bom", "scrape_bom_multi_async"),

    # Japan Meteorological Agency warnings (only warning levels)
    "rss_jma": ScraperEntry("jma", "scrape_jma_async", loader=_load_jma_conf),
}
