import httpx
import requests
from dateutil import parser as dateparser
from json import JSONDecodeError

# ———————————————
# 1) LOOKUP TABLES (load once)
# ———————————————

# A) Area codes → human name
area_index = requests.get(
    "https://www.jma.go.jp/bosai/common/const/class20s/index.json"
).json()
AREA_NAME = { entry["code"]: entry["name"] for entry in area_index }

# B) Warning type keys → labels
TYPE_LABEL = {
    "rain_fall":    {"ja": "大雨警報",      "en": "Heavy Rain Warning"},
    "flood":        {"ja": "洪水警報",      "en": "Flood Warning"},
    "land_slide":   {"ja": "土砂災害警戒情報","en": "Landslide Advisory"},
    "storm_surge":  {"ja": "高潮警報",      "en": "Storm Surge Warning"},
    # … extend as needed …
}

# C) Japanese status → English
STATUS_LABEL = {
    "発表": "Issued",
    "継続": "Continued",
    "解除": "Cancelled",
}


async def scrape_jma_table_async(conf: dict, client: httpx.AsyncClient):
    """
    Fetches JMA warning/map.json, filters only '警報' (level >= 30),
    and returns a dict: {'alerts': { key: […entries…] }, 'source': conf}.
    """
    url      = conf.get("url",
               "https://www.jma.go.jp/bosai/warning/data/warning/map.json")
    feed_key = conf.get("key", "rss_jma")

    # 2) GET + (robust) JSON parsing
    try:
        resp = await client.get(
            url,
            headers={"Referer": "https://www.jma.go.jp/bosai/warning/"}
        )
        resp.raise_for_status()
        text = resp.text
        if not text or not text.strip():
            # empty body → no alerts
            return {"alerts": {}, "source": conf}

        try:
            data = resp.json()
        except JSONDecodeError:
            # invalid JSON → treat as no data
            return {"alerts": {}, "source": conf}
    except httpx.HTTPError as e:
        return {
            "alerts": {},
            "error": f"JMA fetch failed: {e}",
            "source": conf,
        }

    # 3) Validate structure
    if not isinstance(data, dict):
        return {
            "alerts": {},
            "error": f"Unexpected JSON: got {type(data).__name__}",
            "source": conf,
        }

    entries = []

    # 4) Walk prefectures → areas → warning types
    for pref_code, region in data.items():
        ts = region.get("time")
        try:
            published = dateparser.parse(ts).isoformat()
        except Exception:
            published = None

        for area_code, warns in region.get("areas", {}).items():
            if not isinstance(warns, dict):
                continue

            area_name = AREA_NAME.get(area_code, area_code)

            for typ, level in warns.items():
                if not isinstance(level, (int, float)) or level <= 0:
                    continue

                # only 警報: treat level ≥ 30 as warning
                if level < 30:
                    continue

                type_label = TYPE_LABEL.get(typ, {})\
                                      .get(conf.get("lang","en"), typ)
                uid = f"jma|{area_code}|{typ}|{published}"
                entries.append({
                    "id":          uid,
                    "area_code":   area_code,
                    "area_name":   area_name,
                    "type_key":    typ,
                    "type_label":  type_label,
                    "level":       level,
                    "description": f"{type_label} (level {level}) in {area_name}",
                    "link":        url,
                    "published":   published,
                })

    return {
        "alerts": { feed_key: entries },
        "source":  conf,
    }
