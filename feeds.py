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
        "bom_all": {
            "label": "Australia BOM",
            "type": "rss_bom_multi",
            "urls": [
                "https://www.bom.gov.au/fwo/IDZ00054.warnings_nsw.xml",  # NSW & ACT
                "https://www.bom.gov.au/fwo/IDZ00059.warnings_vic.xml",  # Victoria
                "https://www.bom.gov.au/fwo/IDZ00056.warnings_qld.xml",  # Queensland
                "https://www.bom.gov.au/fwo/IDZ00060.warnings_wa.xml",   # West Australia
                "https://www.bom.gov.au/fwo/IDZ00057.warnings_sa.xml",   # South Australia
                "https://www.bom.gov.au/fwo/IDZ00058.warnings_tas.xml",  # Tasmania
                "https://www.bom.gov.au/fwo/IDZ00055.warnings_nt.xml",   # Northern Territory
            ],
            "states": [
                "NSW & ACT",
                "Victoria",
                "Queensland",
                "West Australia",
                "South Australia",
                "Tasmania",
                "Northern Territory",
            ],
        },
        "jma": {
            "label": "JMA Warnings",
            "type": "jma",
            "url": "https://www.jma.go.jp/bosai/warning/#lang=en",
        },
    }
