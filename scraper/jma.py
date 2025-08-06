import httpx
from dateutil import parser as dateparser
import requests

# Pre-load JMA’s code→label→category table
_class25s_url = "https://www.jma.go.jp/bosai/common/const/class25s/index.json"
_class25s = requests.get(_class25s_url).json()
# Map numeric code → { name, enName, category }
CODE_INFO = { entry["code"]: entry for entry in _class25s }

async def scrape_jma_table_async(conf: dict, client: httpx.AsyncClient):
    """
    Fetch JMA warning/map.json, extract only true 警報 (category == "warning")
    and return them under `alerts[conf['key']]`.
    Each alert has: id, region, code, label, description, link, published.
    """
    url     = conf.get("url", "https://www.jma.go.jp/bosai/warning/data/warning/map.json")
    feed_key= conf.get("key", "rss_jma")

    # 1) Fetch JSON
    try:
        resp = await client.get(url, headers={"Referer": "https://www.jma.go.jp/bosai/warning/"})
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return { "alerts": {}, "error": f"JMA fetch failed: {e}", "source": conf }

    if not isinstance(data, dict):
        return {
            "alerts": {},
            "error": f"Unexpected JSON structure: expected object, got {type(data).__name__}",
            "source": conf
        }

    entries = []

    # 2) Walk prefectures → areas → warning‐type keys → numeric code
    for pref_code, region in data.items():
        ts = region.get("time")
        try:
            published = dateparser.parse(ts).isoformat()
        except Exception:
            published = None

        areas = region.get("areas", {})
        if not isinstance(areas, dict):
            continue

        for area_code, warns in areas.items():
            if not isinstance(warns, dict):
                continue

            for typ_key, numeric_code in warns.items():
                # skip non‐numeric or zero
                if not isinstance(numeric_code, (int, float)) or numeric_code == 0:
                    continue

                info = CODE_INFO.get(int(numeric_code))
                # only true "警報" entries
                if not info or info.get("category") != "warning":
                    continue

                # human labels
                ja_label = info["name"]
                en_label = info.get("enName", ja_label)

                uid = f"jma|{pref_code}|{area_code}|{numeric_code}|{published}"
                entries.append({
                    "id":          uid,
                    "region":      area_code,
                    "code":        numeric_code,
                    "label_ja":    ja_label,
                    "label_en":    en_label,
                    "description": f"{ja_label} in {area_code}",
                    "link":        url,
                    "published":   published,
                })

    return {
        "alerts": { feed_key: entries },
        "source":  conf,
    }
