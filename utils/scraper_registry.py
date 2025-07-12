import json
import asyncio
from scraper.environment_canada import scrape_ec
from scraper.meteoalarm import scrape_meteoalarm
from scraper.nws_active_alerts import scrape_nws

SCRAPER_REGISTRY = {
    "json": lambda conf: scrape_nws(conf["url"]),
    "ec_async": lambda conf: asyncio.run(scrape_ec(json.load(open(conf["source_file"])))),
    "rss_meteoalarm": scrape_meteoalarm,
}
