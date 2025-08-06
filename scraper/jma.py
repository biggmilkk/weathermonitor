import httpx
from dateutil import parser as dateparser


async def scrape_jma_table_async(conf: dict, client: httpx.AsyncClient):
    """
    Fetch JMA warning/map.json, extract any non‐zero warning levels,
    and return them as a list of dict entries under key "alerts".
    """
    url = conf.get(
        "url",
        "https://www.jma.go.jp/bosai/warning/data/warning/map.json"
    )
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return {
            "alerts": {},
            "error": f"JMA fetch failed: {e}",
            "source": conf,
        }

    entries = []
    for pref_code, region in data.items():
        # parse the timestamp
        ts = region.get("time")
        try:
            published = dateparser.parse(ts).isoformat()
        except Exception:
            published = None

        # each region has an "areas" dict mapping area_name -> { type: level, ... }
        for area_name, warns in region.get("areas", {}).items():
            for warning_type, level in warns.items():
                # only care about numeric levels > 0
                if isinstance(level, (int, float)) and level > 0:
                    uid = f"jma|{pref_code}|{area_name}|{warning_type}|{published}"
                    entries.append({
                        "id":         uid,
                        "region":     area_name,
                        "type":       warning_type,
                        "description": f"{warning_type} level {level} in {area_name}",
                        "link":       url,
                        "published":  published,
                    })

    return {
        # wrap your list under "alerts" so the main renderer can do
        # country.get("alerts", {}).values()
        "alerts": {"jma": entries},
        "source":  conf,
    }
