import logging
import datetime
from typing import Dict, Any, List, Tuple, Optional
import httpx

MAP_URL = "https://www.jma.go.jp/bosai/warning/data/warning/map.json"

# Keep only warning-or-higher. Advisory (e.g. "10") is excluded.
WARNING_OR_HIGHER = {"03","04","14","18","19","20","21"}

PHENOMENA = {
    "03": "Heavy Rain (Landslide)",
    "04": "Heavy Rain (Inundation)",
    "14": "Flood",
    "18": "Storm",
    "19": "Gale",
    "20": "High Wave",
    "21": "Storm Surge",
}

def _load_areacode_from_conf(conf: dict) -> Optional[Dict[str, Any]]:
    import json, os
    for p in [conf.get("area_code_file"), "scraper/areacode.json", "areacode.json"]:
        if not p:
            continue
        try:
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            logging.warning(f"[JMA DEBUG] failed loading {p}: {e}")
    logging.warning("[JMA DEBUG] areacode.json not found; will show raw codes.")
    return None

def _pref_and_region_from_code(class10_code: str, ac: Dict[str, Any]) -> Tuple[str, str]:
    class10 = ac.get("class10s", {}).get(class10_code, {})
    region_en = class10.get("enName", class10_code)

    office_code = class10.get("parent")
    offices = ac.get("offices", {})
    centers = ac.get("centers", {})

    if office_code in offices:
        office = offices[office_code]
        center = centers.get(office.get("parent", ""), {})
        # If the center of this office is Hokkaido, show "Hokkaido"
        left = "Hokkaido" if center.get("enName") == "Hokkaido" else office.get("enName", office_code)
    else:
        left = "Hokkaido" if class10_code.startswith("01") else "Unknown"

    return left, region_en

async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    ac = _load_areacode_from_conf(conf)

    # fetch map.json
    try:
        resp = await client.get(conf.get("url", MAP_URL), timeout=20)
        resp.raise_for_status()
        data = resp.json()  # list of reports
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {e}")
        return {"entries": [], "error": str(e), "source": conf.get("url", MAP_URL)}

    entries: List[Dict[str, Any]] = []
    seen_pairs: set = set()  # (class10_code, wcode)
    link = "https://www.jma.go.jp/bosai/warning/#lang=en"

    # Iterate reports
    for report in data:
        # stable published time from JMA, not "now"
        rep_dt = report.get("reportDatetime")
        try:
            # Normalize to UTC Z (it’s already UTC ISO, but ensure format)
            published_iso = (
                datetime.datetime.fromisoformat(rep_dt.replace("Z", "+00:00"))
                .replace(tzinfo=datetime.timezone.utc, microsecond=0)
                .isoformat()
                .replace("+00:00","Z")
            ) if rep_dt else datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"
        except Exception:
            published_iso = datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"

        area_types = report.get("areaTypes", [])
        # Find only the class10 layer
        class10_block = None
        for at in area_types:
            # Many dumps label this layer as "class10s"
            if str(at.get("name") or at.get("type") or "").lower() == "class10s":
                class10_block = at
                break
        if not class10_block:
            continue

        for area in class10_block.get("areas", []):
            area_code = str(area.get("code", ""))
            # Only consider class10 codes that look like 6-digit region codes and exist in our areacode map
            if not (len(area_code) == 6 and (not ac or area_code in ac.get("class10s", {}))):
                continue

            for w in area.get("warnings", []):
                wcode = str(w.get("code", ""))
                if wcode not in WARNING_OR_HIGHER:
                    continue  # drop advisories and anything else

                if (area_code, wcode) in seen_pairs:
                    continue  # dedupe within this refresh
                seen_pairs.add((area_code, wcode))

                phen = PHENOMENA.get(wcode)
                if not phen:
                    continue

                # Label "Prefecture (or Hokkaido): Region"
                if ac and area_code in ac.get("class10s", {}):
                    left, region = _pref_and_region_from_code(area_code, ac)
                    region_label = f"{left}: {region}"
                else:
                    region_label = area_code

                # Map to a level string. We default to "Warning" because these codes are warning-or-higher.
                # If you later want to split Special Warning as "Alert", we can add a SPECIAL set and switch here.
                level_text = "Warning"

                entries.append({
                    "title": f"{level_text} – {phen}",
                    "region": region_label,
                    "level": level_text,
                    "type": phen,
                    "summary": "",
                    "published": published_iso,  # stable per JMA report
                    "link": link,
                })

    logging.warning(f"[JMA DEBUG] Parsed {len(entries)} office warnings/alerts")
    return {"entries": entries, "source": conf.get("url", MAP_URL)}
