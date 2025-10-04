# feeds.py

def get_feed_definitions():
    """
    Groups:
      - g1       : refresh every 1 minute
      - g2_even  : refresh every 2 minutes (minutes 2 & 4 of each 4-min cycle)
      - g2_odd   : refresh every 2 minutes (minutes 1 & 3)
      - g4_1..4  : refresh every 4 minutes, on minute 1/2/3/4 respectively
    """
    return {
        "ec": {
            "label": "EC (Canada)",
            "type": "ec_async",
            "source_file": "environment_canada_sources.json",
            # Heavier parsing across many provinces; medium cadence
            "group": "g2_even",
        },

        "metoffice_uk": {
            "label": "Met Office (UK)",
            "type": "uk_grouped_compact",
            "urls": [
                "https://www.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/os",
                "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/he",
                "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/gr",
                "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/st",
                "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/ta",
                "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/dg",
                "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/ni",
                "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/wl",
                "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/nw",
                "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/ne",
                "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/yh",
                "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/wm",
                "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/em",
                "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/ee",
                "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/sw",
                "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/se",
            ],
            "regions": [
                "Orkney & Shetland",
                "Highlands & Eilean Siar",
                "Grampian",
                "Strathclyde",
                "Central, Tayside & Fife",
                "SW Scotland, Lothian Borders",
                "Northern Ireland",
                "Wales",
                "North West England",
                "North East England",
                "Yorkshire & Humber",
                "West Midlands",
                "East Midlands",
                "East of England",
                "South West England",
                "London & South East England",
            ],
            # Many region feeds; heavier & slower → spread to 4-minute slot
            "group": "g4_2",
        },

        "meteoalarm": {
            "label": "Meteoalarm (Europe)",
            "type": "rss_meteoalarm",
            "url": "https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-europe",
            # Very “live” across EU → every minute
            "group": "g1",
        },

        "imd_india_today": {
            "label": "IMD (India)",
            "type": "imd_current_orange_red",
            "conf": {
                "ids": list(range(1, 35))
            },
            # Broad coverage; can be heavier → slower slot
            "group": "g4_1",
        },

        "cma_china": {
            "label": "CMA (China)",
            "type": "rss_cma",
            "conf": {
                "translate_to_en": True,
                "expiry_grace_minutes": 0,
            },
            # Typically slower moving → 4-minute slot
            "group": "g4_4",
        },

        "jma": {
            "label": "JMA (Japan)",
            "type": "rss_jma",
            "region_map_file": "scraper/region_area_codes.json",
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
            # Moderate load; keep at 2-minute cadence
            "group": "g2_odd",
        },

        "pagasa": {
            "label": "PAGASA (Philippines)",
            "type": "rss_pagasa",
            "url": "https://publicalert.pagasa.dost.gov.ph/feeds/",
            # Medium frequency; pair opposite EC for balance
            "group": "g2_even",
        },

        "bom_multi": {
            "label": "BOM (Australia)",
            "type": "rss_bom_multi",
            "urls": [
                "https://www.bom.gov.au/fwo/IDZ00054.warnings_nsw.xml",
                "https://www.bom.gov.au/fwo/IDZ00059.warnings_vic.xml",
                "https://www.bom.gov.au/fwo/IDZ00056.warnings_qld.xml",
                "https://www.bom.gov.au/fwo/IDZ00060.warnings_wa.xml",
                "https://www.bom.gov.au/fwo/IDZ00057.warnings_sa.xml",
                "https://www.bom.gov.au/fwo/IDZ00058.warnings_tas.xml",
                "https://www.bom.gov.au/fwo/IDZ00055.warnings_nt.xml",
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
            # Medium load; alternate with JMA on the other 2-min slot
            "group": "g2_odd",
        },

        "nws": {
            "label": "NWS (US)",
            "type": "nws_grouped_compact",
            "url": "https://api.weather.gov/alerts/active",
            # High volume / highly live → every minute
            "group": "g1",
        },
    }
