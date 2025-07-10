from scrapers import nws_rss

SCRAPER_MAP = {
    "alerts.weather.gov": nws_rss.scrape,
    "www.weather.gov/rss_page.php": nws_rss.scrape,
    "www.weather.gov": nws_rss.scrape,  # optional fallback
}

def get_scraper(domain):
    return SCRAPER_MAP.get(domain)
