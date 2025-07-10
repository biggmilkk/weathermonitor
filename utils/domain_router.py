from scrapers import weather_gov, nws_rss

SCRAPER_MAP = {
    "www.weather.gov": weather_gov.scrape,
    "alerts.weather.gov": nws_rss.scrape,
    "www.weather.gov/rss_page.php": nws_rss.scrape,
    # Add more domains here
}

def get_scraper(domain):
    return SCRAPER_MAP.get(domain)
