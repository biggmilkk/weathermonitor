import httpx
from dateutil import parser as dateparser

async def scrape_jma_table_async(conf: dict, client: httpx.AsyncClient):
    url = conf.get(
        "url",
        "https://www.jma.go.jp/bosai/warning/data/warning/map.json"
    )
    try:
        resp = await client.get(url, headers={"Referer": "https://www.jma.go.jp/bosai/warning/"})
        resp.raise_for_status()
        reports = resp.json()
    except Exception as e:
        return {
            "entries": [],
            "error": f"JMA fetch failed: {e}",
            "source": conf,
        }

    entries = []
    # reports is a list of report objects; pick the latest one if you like:
    for report in reports:
        ts = report.get("reportDatetime") or report.get("time")
        try:
            published = dateparser.parse(ts).isoformat()
        except Exception:
            published = None

        # Now dive into each areaType → areas → warnings
        for area_type in report.get("areaTypes", []):
            # You may need to extract a human‐readable type name here…
            warns_list = area_type.get("areas", [])
            for area in warns_list:
                area_code = area.get("code")
                for warn in area.get("warnings", []):
                    status = warn.get("status")
                    level  = warn.get("code")  # or whatever numeric level they use
                    if status != "解除":  # skip “cleared” alerts
                        uid = f"jma|{area_code}|{warn.get('code')}|{published}"
                        entries.append({
                            "id":          uid,
                            "title":       f"{area_code}: warning {warn.get('code')} [{status}]",
                            "description": f"JMA warning {warn.get('code')} in {area_code} (status: {status})",
                            "link":        url,
                            "published":   published,
                            "area_code":   area_code,
                            "status":      status,
                            "level":       level,
                        })

    return {
        "entries": entries,
        "source":  conf,
    }
