# scraper/jma.py
import logging
import datetime
from typing import Dict, Any, List, Optional
import httpx

# ---- config ----
MAP_URL = "https://www.jma.go.jp/bosai/warning/data/warning/map.json"

# Codes to KEEP (warning-or-higher). Advisory (e.g. "10") is excluded.
WARNING_OR_HIGHER = {"03","04","14","18","19","20","21"}

PHENOMENA = {
    "03": "Heavy Rain (Landslide)",
    "04": "Heavy Rain (Inundation)",
    "14": "Flood",
    "18": "Storm",
    "19": "Gale",
    "20": "High Wave",
    "21": "Storm Surge",
    # If JMA ever publishes Thunderstorm/Dense Fog as WARNING with distinct codes, add them here.
}

def _load_areacode_from_conf(conf: dict) -> Optional[Dict[str, Any]]:
    """
    Try to load areacode.json either from explicit conf path or default 'scraper/areacode.json'.
    """
    import json, os
    paths = []
    if "area_code_file" in conf and conf["area_code_file"]:
        paths.append(conf["area_code_file"])
    # default fallback (your repo structure)
    paths.append("scraper/areacode.json")
    paths.append("areacode.json")

    for p in paths:
        try:
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            logging.warning(f"[JMA DEBUG] failed loading {p}: {e}")
    logging.warning("[JMA DEBUG] areacode.json not found; will show raw codes.")
    return None


def _pref_and_region_from_code(class10_code: str, ac: Dict[str, Any]) -> (str, str):
    """
    Given a class10 code (e.g., '014010'), return:
      - left: Prefecture (or 'Hokkaido')
      - right: '<... Region>' name (class10.enName)
    """
    # class10 node
    class10 = ac.get("class10s", {}).get(class10_code, {})
    region_en = class10.get("enName", class10_code)

    office_code = class10.get("parent")  # e.g., '014100' (Kushiro Nemuro) or '020000' (Aomori)
    offices = ac.get("offices", {})
    centers = ac.get("centers", {})

    pref_left = office_code or ""
    if office_code in offices:
        office = offices[office_code]
        # If the center of this office is Hokkaido, we show 'Hokkaido'
        center_code = office.get("parent")
        center = centers.get(center_code, {}) if center_code else {}
        center_en = center.get("enName")
        office_en = office.get("enName", office_code)

        if center_en == "Hokkaido":
            left = "Hokkaido"
        else:
            # For non-Hokkaido prefectures, the office name is already the prefecture (e.g., Aomori)
            left = office_en
    else:
        # Fallback: try to infer Hokkaido vs others by code
        left = "Hokkaido" if class10_code.startswith("01") else office_code or "Unknown"

    return left, region_en


async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Parse JMA warning map JSON and emit only Warning/Alert/Emergency-level phenomena
    with the desired naming:
      '<Prefecture or Hokkaido>: <Region>'
      '<Level> – <Phenomenon>'
    """
    ac = _load_areacode_from_conf(conf)

    # fetch map.json
    try:
        resp = await client.get(conf.get("url", MAP_URL), timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {e}")
        return {"entries": [], "error": str(e), "source": conf.get("url", MAP_URL)}

    entries: List[Dict[str, Any]] = []
    now_iso = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    link = "https://www.jma.go.jp/bosai/warning/#lang=en"

    # The file is a list of reports; each has areaTypes -> areas
    # Each `areas` item has 'code' and a list of 'warnings' [{code, status, ...}]
    for report in data:
        area_types = report.get("areaTypes", [])
        for at in area_types:
            for area in at.get("areas", []):
                area_code = str(area.get("code"))
                for w in area.get("warnings", []):
                    wcode = str(w.get("code"))
                    if wcode not in WARNING_OR_HIGHER:
                        # drop advisories and anything not in our keep-set
                        continue

                    phen = PHENOMENA.get(wcode)
                    if not phen:
                        # Unknown/unsupported warning type; skip
                        continue

                    # Determine class10 code to label the Region exactly
                    # map.json sometimes lists class10 codes (e.g., '014010'), sometimes municipality codes (7 digits).
                    # We only want class10 region rows. Those are 6-digit strings and should exist in areacode.class10s.
                    # If areacode missing, just print the raw code.
                    if ac and len(area_code) == 6 and ac.get("class10s", {}).get(area_code):
                        left, region = _pref_and_region_from_code(area_code, ac)
                        region_label = f"{left}: {region}"
                    else:
                        # Fallback: we can't confidently format; show code
                        region_label = area_code

                    # Level wording: JMA English UI uses “Alert” for 特別警報; our keep-set here is all warnings-or-higher.
                    # We don’t have a separate flag for 特別警報 in map.json sample you shared, so default to "Warning".
                    # If you later add a separate SPECIAL set, you can flip to "Alert" as needed.
                    level_text = "Warning"

                    entries.append({
                        "title": f"{level_text} – {phen}",
                        "region": region_label,
                        "level": level_text,
                        "type": phen,
                        "summary": "",
                        "published": now_iso,   # map.json has per-report time; using 'now' keeps it simple in the UI
                        "link": link
                    })

    logging.warning(f"[JMA DEBUG] Parsed {len(entries)} office warnings/alerts")
    return {"entries": entries, "source": conf.get("url", MAP_URL)}
