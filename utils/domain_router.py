from scraper import nws_rss

SCRAPER_MAP = {
    "www.weather.gov/rss_page.php": nws_rss.scrape,
    "www.weather.gov": nws_rss.scrape,  # optional fallback
}

