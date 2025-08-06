# scraper/jma.py

import httpx
from dateutil import parser as dateparser

async def scrape_jma_table_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Fetch JMA warning/map.json, extract any non‐zero warning levels,
    and return a dict with an 'entries' list and the original 'source'.
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
            "entries": [],
            "error": f"JMA fetch failed: {e}",
            "source": conf,
        }

    entries = []
    for pref_code, region in data.items():
        ts = region.get("time")
        try:
            published = dateparser.parse(ts).isoformat()
        except Exception:
            published = None

        for area_name, warns in region.get("areas", {}).items():
            for warning_type, level in warns.items():
                # only include non‐zero numeric levels
                if isinstance(level, (int, float)) and level > 0:
                    uid = f"jma|{pref_code}|{area_name}|{warning_type}|{ts}"
                    entries.append({
                        "id":          uid,
                        "title":       f"{area_name}: {warning_type} (level {level})",
                        "description": f"JMA advisory—{warning_type} level {level} in {area_name}",
                        "link":        url,
                        "published":   published,
                        # extra fields for downstream renderers
                        "pref_code":   pref_code,
                        "area":        area_name,
                        "type":        warning_type,
                        "level":       level,
                    })

    return {
        "entries": entries,
        "source":  conf,
    }
