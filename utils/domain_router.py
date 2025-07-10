from scraper import nws_active_alerts
from scraper import environment_canada

SCRAPER_MAP = {
    "api.weather.gov": nws_active_alerts.scrape,
    "environment.canada": environment_canada.scrape,
    # other domains...
}

def get_scraper(domain):
    return SCRAPER_MAP.get(domain)
