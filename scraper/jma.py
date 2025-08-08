import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional

import httpx

# --- Config for the test run: just office 014100 (Kushiro/Nemuro) ---
TEST_OFFICE_CODE = "014100"  # Hokkaido: Kushiro Local Meteorological Office

# Places we’ll try for your uploaded files
BASE_DIR = Path(__file__).resolve().parent
AREACODE_CANDIDATES = [
    BASE_DIR / "areacode.json",
    Path("/mnt/data/areacode.json"),
    Path("areacode.json"),
]

# Map JMA “values” to a coarse severity. We only keep >= 30.
# 00 none, 10 advisory-ish, 20 still advisory, 30 warning, 40 alert/special warning, 50 emergency (rare).
def value_to_level(v: str) -> Optional[str]:
    try:
        n = int(v)
    except Exception:
        return None
    if n >= 50:
        return "Emergency"
    if n >= 40:
        return "Alert"
    if n >= 30:
        return "Warning"
    return None  # advisory or none

# Phenomenon code -> label (base)
# 04 Flood; 10 Heavy Rain (we’ll refine to Inundation/Landslide by reading which sub-hazard crossed threshold)
PHENOMENON_LABEL = {
    "04": "Flood",
    "10": "Heavy Rain",   # refined to (Inundation)/(Landslide)
    "19": "Storm Surge",
    "14": "Thunderstorm",  # almost always advisory in your sample
    "20": "Dense Fog",
}

# For Heavy Rain sub-flavors: which level "type" corresponds to which suffix
HEAVY_RAIN_SUBTYPES = {
    "土砂災害危険度": "Heavy Rain (Landslide)",
    "浸水害危険度":   "Heavy Rain (Inundation)",
}

def _load_areacode_map() -> Dict[str, str]:
    """
    Build a mapping of area code -> 'Prefecture: Region' from areacode.json you uploaded.
    Accept a few common shapes. If a code is missing, we leave the raw code.
    """
    for p in AREACODE_CANDIDATES:
        try:
            if not p.exists():
                continue
            data = json.loads(p.read_text(encoding="utf-8"))
            # Flat dict { "014010": "Hokkaido: Kushiro Region", ... }
            if isinstance(data, dict) and all(isinstance(v, str) for v in data.values()):
                return {str(k): v for k, v in data.items()}

            # Dict of dicts with a name field
            if isinstance(data, dict) and any(isinstance(v, dict) for v in data.values()):
                m = {}
                for k, v in data.items():
                    if isinstance(v, dict):
                        name = v.get("name_en") or v.get("name") or v.get("label")
                        if isinstance(name, str):
                            m[str(k)] = name
                if m:
                    return m

            # Nested under top key (offices/areas/regions)
            for top in ("offices", "areas", "regions"):
                if isinstance(data, dict) and top in data and isinstance(data[top], dict):
                    m = {}
                    for k, v in data[top].items():
                        if isinstance(v, str):
                            m[str(k)] = v
                        elif isinstance(v, dict):
                            name = v.get("name_en") or v.get("name") or v.get("label")
                            if isinstance(name, str):
                                m[str(k)] = name
                    if m:
                        return m
        except Exception as e:
            logging.warning("[JMA DEBUG] Failed reading areacode.json at %s: %s", p, e)
    logging.warning("[JMA DEBUG] areacode.json not found/parsed; region names may show as raw codes.")
    return {}

def _utc_rfc_time(report_dt_jst: str) -> str:
    # Example: "2025-08-08T15:27:00+09:00" -> "Fri, 08 Aug 2025 06:27 UTC"
    dt = datetime.fromisoformat(report_dt_jst)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%a, %d %b %Y %H:%M UTC")

def _pick_severity_from_timeseries(area_code: str, phen_code: str, ts_block: dict) -> Optional[str]:
    """
    Look into timeSeries for a given area and phenomenon code.
    If any horizon has value >=30, return the strongest severity seen (Emergency > Alert > Warning).
    """
    best = None  # None < Warning < Alert < Emergency
    rank = {"Warning": 1, "Alert": 2, "Emergency": 3}

    for area_types in ts_block.get("areaTypes", []):
        for area in area_types.get("areas", []):
            if str(area.get("code")) != area_code:
                continue
            for warn in area.get("warnings", []):
                if str(warn.get("code")) != phen_code:
                    continue
                # Each warning has levels -> localAreas -> values[]
                for lvl in warn.get("levels", []):
                    for la in lvl.get("localAreas", []):
                        for v in la.get("values", []):
                            sev = value_to_level(v)
                            if sev and (best is None or rank[sev] > rank[best]):
                                best = sev
    return best

def _label_for_heavy_rain(area_code: str, ts_block: dict) -> Optional[str]:
    """
    Decide whether Heavy Rain should be (Landslide) or (Inundation) for this area,
    based on which sub-hazard reaches warning level.
    """
    pick = None
    # Scan both subtypes; choose the stronger one if both apply
    rank = {"Warning": 1, "Alert": 2, "Emergency": 3}
    for level in ts_block.get("areaTypes", []):
        for area in level.get("areas", []):
            if str(area.get("code")) != area_code:
                continue
            for warn in area.get("warnings", []):
                if str(warn.get("code")) != "10":
                    continue
                for lvl in warn.get("levels", []):
                    t = lvl.get("type")
                    suffix = HEAVY_RAIN_SUBTYPES.get(t)
                    if not suffix:
                        continue
                    best = None
                    for la in lvl.get("localAreas", []):
                        for v in la.get("values", []):
                            sev = value_to_level(v)
                            if not sev:
                                continue
                            if best is None or rank[sev] > rank[best]:
                                best = sev
                    if best and (pick is None or rank[best] > rank[pick[1]]):
                        pick = (suffix, best)
    return pick[0] if pick else None

async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Test-run scraper: ONLY office 014100.
    Emits sub-region entries ONLY for Warning/Alert/Emergency.
    """
    # Where to load JSON from: use remote unless testing locally is requested
    office_code = TEST_OFFICE_CODE
    url = f"https://www.jma.go.jp/bosai/warning/data/warning/{office_code}.json"
    try:
        resp = await client.get(url, timeout=20)
        resp.raise_for_status()
        office = resp.json()
    except Exception as e:
        logging.warning("[JMA FETCH ERROR] %s", e)
        # try local fallback for your uploaded file
        p = Path("/mnt/data") / f"{office_code}.json"
        if p.exists():
            office = json.loads(p.read_text(encoding="utf-8"))
        else:
            return {"entries": [], "error": str(e), "source": url}

    areaname = _load_areacode_map()
    report_dt = office.get("reportDatetime") or office.get("reportDateTime") or office.get("report_date_time")
    published = _utc_rfc_time(report_dt) if report_dt else datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M UTC")

    entries: List[Dict[str, Any]] = []

    # We’ll use the first timeSeries block for severity lookups (it carries the values arrays)
    ts_block = None
    for ts in office.get("timeSeries", []):
        if "areaTypes" in ts:
            ts_block = ts
            break

    # The top-level areaTypes[0].areas are the sub-regions we want (e.g., Kushiro 014010, Nemuro 014020)
    for area_types in office.get("areaTypes", []):
        for a in area_types.get("areas", []):
            sub_code = str(a.get("code"))
            # Resolve friendly name; fall back to code if missing
            region_name = areaname.get(sub_code, sub_code)

            # Each area has a list of warnings (by phenomenon code)
            for w in a.get("warnings", []):
                phen_code = str(w.get("code"))
                base_label = PHENOMENON_LABEL.get(phen_code)
                if not base_label:
                    continue  # ignore phenomena we don’t track

                # Decide severity by reading the timeSeries values (>=30 means we keep it)
                sev = _pick_severity_from_timeseries(sub_code, phen_code, ts_block) if ts_block else None
                if not sev:
                    continue  # advisory or none → skip

                # For heavy rain, refine to (Landslide)/(Inundation)
                if phen_code == "10":
                    subtype = _label_for_heavy_rain(sub_code, ts_block)
                    if not subtype:
                        # if nothing at warning level for subtypes, skip heavy rain entirely
                        continue
                    title = subtype
                else:
                    title = base_label

                entries.append({
                    "title": f"{sev} – {title}",
                    "region": region_name,
                    "level": sev,
                    "type": title,
                    "summary": "",
                    "published": published,
                    "link": url,
                })

    logging.warning("[JMA DEBUG] Built %d entries for office %s", len(entries), office_code)
    return {"entries": entries, "source": url}
