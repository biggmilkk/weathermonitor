def get_feed_definitions():
    return {
        "nws": {
            "label": "NWS Alerts",
            "type": "json",
            "url": "https://api.weather.gov/alerts/active"
        },
        "ec": {
            "label": "Environment Canada",
            "type": "ec_async",
            "source_file": "environment_canada_sources.json"
        },
        "meteoalarm": {
            "label": "Meteoalarm Europe",
            "type": "rss_meteoalarm",
            "url": "https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-europe"
        },
        "cma_china": {
            "label": "China Alerts",
            "type": "rss_cma",
            "url": "https://severeweather.wmo.int/v2/cap-alerts/cn-cma-xx/rss.xml"
        },
    }
