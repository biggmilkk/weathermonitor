import json
import asyncio
from scraper.environment_canada import scrape_ec
from scraper.meteoalarm import scrape_meteoalarm
from scraper.nws_active_alerts import scrape_nws

SCRAPER_REGISTRY = {
    "json": lambda conf: scrape_nws(conf.get("url")),
    "ec_async": lambda conf: asyncio.run(scrape_ec(json.load(open(conf.get("source_file"))))),
    "rss_meteoalarm": lambda conf: scrape_meteoalarm(conf),
    "rss_cma": lambda conf: asyncio.run(scrape_cma(conf)),
}
