import json

# Lazy-loading scraper functions to prevent circular imports
SCRAPER_REGISTRY = {
    "json": lambda conf, client: __import__(
        'scraper.nws_active_alerts', fromlist=['scrape_nws_async']
    ).scrape_nws_async(conf.get('url'), client),
    "ec_async": lambda conf, client: __import__(
        'scraper.environment_canada', fromlist=['scrape_ec_async']
    ).scrape_ec_async(
        json.load(open(conf.get('source_file'))), client
    ),
    "rss_meteoalarm": lambda conf, client: __import__(
        'scraper.meteoalarm', fromlist=['scrape_meteoalarm_async']
    ).scrape_meteoalarm_async(conf, client),
    "rss_cma": lambda conf, client: __import__(
        'scraper.cma', fromlist=['scrape_cma_async']
    ).scrape_cma_async(conf, client),
}
