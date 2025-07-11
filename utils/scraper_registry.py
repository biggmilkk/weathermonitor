import json
import asyncio
from scraper.environment_canada import scrape_async
from scraper.meteoalarm import scrape as scrape_meteoalarm
from scraper.nws_active_alerts import scrape as scrape_nws

SCRAPER_REGISTRY = {
    "json": lambda conf: scrape_nws(conf["url"]),
    "ec_async": lambda conf: asyncio.run(scrape_async(json.load(open(conf["source_file"])))),
    "rss_meteoalarm": lambda conf: scrape_meteoalarm(conf["url"]),
}
