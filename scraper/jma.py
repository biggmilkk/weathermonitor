import asyncio
import json
import logging
from datetime import datetime, timezone

import httpx

JMA_MAP_URL = "https://www.jma.go.jp/bosai/warning/data/warning/map.json"

# Try both paths: repo and uploaded
AREACODE_PATHS = [
    "scraper/areacode.json",
    "/mnt/data/areacode.json",
]

# Phenomena code -> English label (office-level warnings)
PHENOMENA = {
    "03": "Heavy Rain",   # Special warning (we’ll label as Alert below)
    "04": "Flood",
    "14": "Thunderstorm",
    "15": "Dense Fog",
    "18": "High Wave",
    "20": "Storm Surge",
    # add others if JMA starts using them at office level
}

def _load_areacode():
    data = None
    for p in AREACODE_PATHS:
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
                break
        except Exception:
            continue
    if data is None:
        logging.warning("[JMA DEBUG] areacode.json not found; using codes as names.")
        return {}, {}, {}
    centers = data.get("centers", {})
    offices = data.get("offices", {})
    class10s = data.get("class10s", {})
    return centers, offices, class10s

def _office_display_name(code, centers, offices, class10s):
    """
    Build 'Prefecture: Region' like 'Hokkaido: Soya Region' for an office code
    using class10s[office_code].enName and the parent center's enName.
    Fallback to office.enName or the raw code.
    """
    # Prefer class10s entry with same code for the “Region” wording when it exists
    c10 = class10s.get(code)
    office = offices.get(code)
    if c10 and office:
        # center -> offices[parent] is the office; offices[parent].parent points to the center code
        center_code = office.get("parent")
        center = centers.get(center_code, {})
        pref_en = center.get("enName") or ""
        region_en = c10.get("enName") or office.get("enName") or code
        if pref_en:
            return f"{pref_en}: {region_en}"
        return region_en

    # Fallbacks
    if office:
        office_en = office.get("enName") or code
        # Try to prepend center if we can
        center = centers.get(office.get("parent", ""), {})
        pref_en = center.get("enName")
        if pref_en:
            return f"{pref_en}: {office_en}"
        return office_en

    return code  # last resort

def _level_for_code(code: str) -> str:
    # Treat '03' (special heavy rain) as Alert; others at office level as Warning
    return "Alert" if code == "03" else "Warning"

def _heavy_rain_suffix(w_item: dict) -> str:
    """
    Decide (Landslide) vs (Inundation) for Heavy Rain using 'condition'/'attentions'
    when JMA provides them. Default to '(Landslide)' if only 土砂災害; to '(Inundation)'
    if only 浸水; to '(Landslide)' if unclear (matches the examples you want surfaced).
    """
    cond = (w_item.get("condition") or "")  # e.g., "土砂災害、浸水害"
    atts = "、".join(w_item.get("attentions", []) or [])
    text = f"{cond}、{atts}"

    has_landslide = ("土砂" in text)
    has_inundation = ("浸水" in text)

    if has_landslide and has_inundation:
        # You can choose to emit two separate entries if you prefer — keeping one keeps feed concise
        return "(Landslide)"  # matches your examples
    if has_inundation and not has_landslide:
        return "(Inundation)"
    if has_landslide and not has_inundation:
        return "(Landslide)"
    # No hint — default to landslide to match JMA’s usual special-warning emphasis
    return "(Landslide)"

async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Parse JMA office-level warnings (Warning / Alert / Emergency only) from map.json.
    Ignores municipality/advisory layer so you don't see Advisories.
    """
    url = conf.get("url", JMA_MAP_URL)
    # Load name dictionaries
    centers, offices, class10s = _load_areacode()

    # Fetch map
    try:
        resp = await client.get(url, timeout=15.0)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {e}")
        return {"entries": [], "error": str(e), "source": url}

    entries = []
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # payload is a list of “reports”
    for report in payload:
        # areaTypes[0] = office/class10 (warning layer), areaTypes[1] = municipalities (advisory)
        area_types = report.get("areaTypes") or []
        if not area_types:
            continue
        office_layer = area_types[0]
        for area in office_layer.get("areas", []):
            office_code = str(area.get("code", ""))
            if not office_code:
                continue
            region_name = _office_display_name(office_code, centers, offices, class10s)

            for w in area.get("warnings", []) or []:
                code = str(w.get("code", "")).zfill(2)
                if code not in PHENOMENA:
                    # Not a phenomenon you care about
                    continue

                phen = PHENOMENA[code]
                level = _level_for_code(code)

                # Split Heavy Rain variants
                if code == "03" or phen == "Heavy Rain":
                    phen = f"Heavy Rain {_heavy_rain_suffix(w)}"

                entries.append({
                    "title": f"{level} – {phen}",
                    "region": region_name,
                    "level": level,
                    "type": phen,
                    "summary": "",
                    "published": now_iso,  # JMA doesn’t give a clean per-item ts here
                    "link": f"https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={office_code}",
                })

    logging.warning(f"[JMA DEBUG] Parsed {len(entries)} office warnings/alerts")
    # Sort newest first by region then title for a stable view
    entries.sort(key=lambda e: (e["region"], e["title"]))
    return {"entries": entries, "source": url}
