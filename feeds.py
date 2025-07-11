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
        }
    }
