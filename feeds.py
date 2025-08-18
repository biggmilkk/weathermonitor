def get_feed_definitions():
    return {
        "nws": {
            "label": "NWS (US)",
            "type": "nws_grouped_compact",
            "url": "https://api.weather.gov/alerts/active",
        },
        "ec": {
            "label": "EC (Canada)",
            "type": "ec_async",
            "source_file": "environment_canada_sources.json",
        },
        "meteoalarm": {
            "label": "Meteoalarm (Europe)",
            "type": "rss_meteoalarm",
            "url": "https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-europe",
        },
        "cma_china": {
            "label": "CMA (China)",
            "type": "rss_cma",
            "url": "https://severeweather.wmo.int/v2/cap-alerts/cn-cma-xx/rss.xml",
        },
        "jma": {
            "label": "JMA (Japan)",
            "type": "rss_jma",
            "office_codes": [
                "011000","012000","013000","014100","014030","015000","016000","017000",
                "020000","030000","040000","050000","060000","070000","080000","090000",
                "100000","110000","120000","130000","140000","190000","200000","210000",
                "220000","230000","240000","150000","160000","170000","180000","250000",
                "260000","270000","280000","290000","300000","310000","320000","330000",
                "340000","360000","370000","380000","390000","350000","400000","410000",
                "420000","430000","440000","450000","460100","460040","471000","472000",
                "473000","474000"
            ],
            "region_map_file": "scraper/region_area_codes.json",
        },
        "bom_all": {
            "label": "BOM (Australia)",
            "type": "rss_bom_multi",
            "urls": [
                "https://www.bom.gov.au/fwo/IDZ00054.warnings_nsw.xml",  # NSW & ACT
                "https://www.bom.gov.au/fwo/IDZ00059.warnings_vic.xml",  # Victoria
                "https://www.bom.gov.au/fwo/IDZ00056.warnings_qld.xml",  # Queensland
                "https://www.bom.gov.au/fwo/IDZ00060.warnings_wa.xml",   # Western Australia
                "https://www.bom.gov.au/fwo/IDZ00057.warnings_sa.xml",   # South Australia
                "https://www.bom.gov.au/fwo/IDZ00058.warnings_tas.xml",  # Tasmania
                "https://www.bom.gov.au/fwo/IDZ00055.warnings_nt.xml",   # Northern Territory
            ],
            "states": [
                "NSW & ACT",
                "Victoria",
                "Queensland",
                "Western Australia",
                "South Australia",
                "Tasmania",
                "Northern Territory",
            ],
        },
    }
