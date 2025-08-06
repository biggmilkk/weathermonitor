import httpx
from dateutil import parser as dateparser

async def scrape_jma_table_async(conf: dict, client: httpx.AsyncClient):
    url = conf.get(
        "url",
        "https://www.jma.go.jp/bosai/warning/data/warning/map.json"
    )
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()   # ← this is a list of regions, not a dict
    except Exception as e:
        return {
            "alerts": {},
            "error": f"JMA fetch failed: {e}",
            "source": conf,
        }

    entries = []
    for region in data:
        # if the JSON objects look like {"code": "011000", "time": "...", "areas": {...}}
        pref_code = region.get("code")
        ts        = region.get("time")

        try:
            published = dateparser.parse(ts).isoformat()
        except Exception:
            published = None

        for area_name, warns in region.get("areas", {}).items():
            for warning_type, level in warns.items():
                if level and level > 0:
                    uid = f"jma|{pref_code}|{area_name}|{warning_type}|{published}"
                    entries.append({
                        "id":          uid,
                        "region":      area_name,
                        "title":       f"{area_name}: {warning_type} (level {level})",
                        "description": f"JMA advisory: {warning_type} level {level} in {area_name}",
                        "link":        url,
                        "published":   published,
                        "pref_code":   pref_code,
                        "type":        warning_type,
                        "level":       level,
                    })

    # wrap under the "alerts" key so your runner/renderer sees it properly
    key = conf.get("key", "jma")
    return {
        "alerts": { key: entries },
        "source": conf,
    }
