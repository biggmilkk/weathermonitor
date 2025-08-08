import json
import logging
from datetime import datetime, timezone
import httpx

JMA_MAP_URL = "https://www.jma.go.jp/bosai/warning/data/warning/map.json"

AREACODE_PATHS = ["scraper/areacode.json"]

# Office-level phenomena we care about
PHENOMENA = {
    "03": "Heavy Rain",   # special heavy rain (treat as Alert)
    "04": "Flood",
    "14": "Thunderstorm",
    "15": "Dense Fog",
    "18": "High Wave",
    "20": "Storm Surge",
    # If you want Storm/Gale later, add their codes here when you have them.
}

def _load_areacode():
    for p in AREACODE_PATHS:
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
                return d.get("centers", {}), d.get("offices", {}), d.get("class10s", {})
        except Exception:
            continue
    logging.warning("[JMA DEBUG] areacode.json not found; using codes as names.")
    return {}, {}, {}

def _office_display_name(code, centers, offices, class10s):
    c10 = class10s.get(code)
    office = offices.get(code)
    if c10 and office:
        center = centers.get(office.get("parent", ""), {})
        pref = center.get("enName") or ""
        region = c10.get("enName") or office.get("enName") or code
        return f"{pref}: {region}" if pref else region
    if office:
        center = centers.get(office.get("parent", ""), {})
        pref = center.get("enName")
        name = office.get("enName") or code
        return f"{pref}: {name}" if pref else name
    return code

def _level_for_code(code: str) -> str:
    # Treat 03 (special heavy rain) as Alert; everything else we keep as Warning
    return "Alert" if code == "03" else "Warning"

def _heavy_rain_suffix(w_item: dict) -> str:
    cond = (w_item.get("condition") or "")
    atts = "、".join(w_item.get("attentions") or [])
    text = f"{cond}、{atts}"
    has_landslide = "土砂" in text
    has_inundation = "浸水" in text
    if has_inundation and not has_landslide:
        return "(Inundation)"
    return "(Landslide)"

async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    url = conf.get("url", JMA_MAP_URL)
    centers, offices, class10s = _load_areacode()

    try:
        resp = await client.get(url, timeout=15.0)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {e}")
        return {"entries": [], "error": str(e), "source": url}

    if not isinstance(payload, list) or not payload:
        logging.warning("[JMA DEBUG] Unexpected payload format.")
        return {"entries": [], "source": url}

    # Use ONLY the most recent report (avoid 100s of dupes)
    latest = max(
        payload,
        key=lambda r: r.get("reportDatetime", "") or ""
    )

    entries = []
    seen = set()
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    area_types = latest.get("areaTypes") or []
    if not area_types:
        logging.warning("[JMA DEBUG] No areaTypes in latest report.")
        return {"entries": [], "source": url}

    office_layer = area_types[0]  # offices (warnings); areaTypes[1] = municipalities/advisories
    for area in office_layer.get("areas", []) or []:
        office_code = str(area.get("code", ""))
        if not office_code:
            continue
        region_name = _office_display_name(office_code, centers, offices, class10s)

        for w in area.get("warnings", []) or []:
            code = str(w.get("code", "")).zfill(2)
            if code not in PHENOMENA:
                continue

            level = _level_for_code(code)
            phen = PHENOMENA[code]
            if phen == "Heavy Rain":
                phen = f"Heavy Rain {_heavy_rain_suffix(w)}"

            dedup_key = (office_code, phen, level)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            entries.append({
                "title": f"{level} – {phen}",
                "region": region_name,
                "level": level,
                "type": phen,
                "summary": "",
                "published": now_iso,
                "link": f"https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={office_code}",
            })

    logging.warning(f"[JMA DEBUG] Parsed {len(entries)} office warnings/alerts (latest report only)")
    # Sort for stable display
    entries.sort(key=lambda e: (e["region"], e["title"]))
    return {"entries": entries, "source": url}
