from scraper import environment_canada, nws_active_alerts

SCRAPER_MAP = {
    "api.weather.gov": nws_active_alerts.scrape,
    "weather.gc.ca": environment_canada.scrape
}

def get_scraper(domain):
    return SCRAPER_MAP.get(domain)
