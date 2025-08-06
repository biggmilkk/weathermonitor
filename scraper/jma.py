import httpx
from dateutil import parser as dateparser

async def scrape_jma_table_async(conf: dict, client: httpx.AsyncClient):
    """
    Fetch JMA warning/map.json, extract any non‐zero warning levels,
    and return them as a list of dict entries.
    """
    url = conf.get("url", "https://www.jma.go.jp/bosai/warning/data/warning/map.json")
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return {
            "entries": [],
            "error": f"JMA fetch failed: {e}",
            "source": conf,
        }

    entries = []
    # data is a dict of prefecture_code → region info
    for prefecture_code, region_info in data.items():
        ts = region_info.get("time")
        try:
            published = dateparser.parse(ts).isoformat()
        except Exception:
            published = None

        for area_name, warns in region_info.get("areas", {}).items():
            for warning_type, level in warns.items():
                if level and level > 0:
                    uid = f"jma|{prefecture_code}|{area_name}|{warning_type}|{published}"
                    entries.append({
                        "id":          uid,
                        "region":      area_name,             # ← add this
                        "title":       f"{area_name}: {warning_type} (level {level})",
                        "description": f"JMA advisory: {warning_type} level {level} in {area_name}",
                        "link":        url,
                        "published":   published,
                        "pref_code":   prefecture_code,
                        "type":        warning_type,
                        "level":       level,
                    })

    return {
        "entries": entries,
        "source":  conf,
    }
