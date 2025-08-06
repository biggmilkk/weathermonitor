import json

# Lazy-loading scraper functions to prevent circular imports
SCRAPER_REGISTRY = {
    # NWS active alerts
    "json": lambda conf, client: __import__(
        'scraper.nws_active_alerts', fromlist=['scrape_nws_async']
    ).scrape_nws_async(conf, client),

    # Environment Canada RSS feeds
    "ec_async": lambda conf, client: __import__(
        'scraper.environment_canada', fromlist=['scrape_ec_async']
    ).scrape_ec_async(json.load(open(conf.get('source_file'))), client),

    # MeteoAlarm countries
    "rss_meteoalarm": lambda conf, client: __import__(
        'scraper.meteoalarm', fromlist=['scrape_meteoalarm_async']
    ).scrape_meteoalarm_async(conf, client),

    # China Meteorological Admin regions
    "rss_cma": lambda conf, client: __import__(
        'scraper.cma', fromlist=['scrape_cma_async']
    ).scrape_cma_async(conf, client),

    # BOM multi-state Australia
    "rss_bom_multi": lambda conf, client: __import__(
        'scraper.bom', fromlist=['scrape_bom_multi_async']
    ).scrape_bom_multi_async(conf, client),

    # JMA
    "rss_jma": lambda conf, client: __import__(
        "scraper.jma", fromlist=["scrape_jma_table_async"]
    ).scrape_jma_table_async(client, conf),
}
