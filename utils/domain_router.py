from scrapers import weather_gov

SCRAPER_MAP = {
    "www.weather.gov": weather_gov.scrape,
    # Add more domains as needed
}

def get_scraper(domain):
    return SCRAPER_MAP.get(domain)
