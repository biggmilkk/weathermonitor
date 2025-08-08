# scraper/jma.py
import httpx
import datetime

async def scrape_jma_async(conf, client):
    base_url = "https://www.jma.go.jp/bosai/warning/data/warning"
    entries = []

    # Get map.json (list of all prefectures/regions with codes)
    map_url = f"{base_url}/map.json"
    map_data = (await client.get(map_url)).json()

    for area_code, info in map_data.items():
        # Fetch area detail JSON
        detail_url = f"{base_url}/{area_code}.json"
        detail_data = (await client.get(detail_url)).json()

        # detail_data["warnings"] (name may differ) holds warnings/advisories
        for warning in detail_data.get("warnings", []):
            # Filter to Warning level only
            if warning.get("status") != "発表":  # or match code for warning
                continue
            if "Warning" not in warning.get("kind", {}).get("name", ""):
                continue

            region_name = conf["area_codes"].get(area_code, {}).get("name", area_code)
            phenomenon_code = warning.get("kind", {}).get("code")
            phenomenon_name = conf["weather"].get(phenomenon_code, phenomenon_code)

            entries.append({
                "title": f"{phenomenon_name} – {region_name}",
                "region": region_name,
                "level": "Warning",
                "type": phenomenon_name,
                "summary": warning.get("text", ""),
                "published": datetime.datetime.utcnow().isoformat() + "Z",
                "link": "https://www.jma.go.jp/bosai/warning/#lang=en"
            })

    return {"entries": entries}
