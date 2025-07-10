from scraper import nws_active_alerts

SCRAPER_MAP = {
    "api.weather.gov": nws_active_alerts.scrape,
    # other domains...
}

def get_scraper(domain):
    return SCRAPER_MAP.get(domain)
