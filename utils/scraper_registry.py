from scraper.environment_canada import scrape_async
from scraper.meteoalarm import scrape_meteoalarm
from utils.domain_router import get_scraper

SCRAPER_REGISTRY = {
    "json": lambda conf: get_scraper(conf["url"])(conf["url"]),
    "ec_async": lambda conf: asyncio.run(scrape_async(json.load(open(conf["source_file"])))),
    "rss_meteoalarm": lambda conf: scrape_meteoalarm(conf["url"]),
}
