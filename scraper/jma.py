# scraper/jma.py
import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import httpx

JMA_MAP_URL = "https://www.jma.go.jp/bosai/warning/data/warning/map.json"

# Minimal phenomenon code map (covers what shows on the English UI)
PHENOMENON = {
    "03": "Heavy Rain",     # 大雨
    "14": "Flood",          # 洪水
    "15": "Storm",          # 暴風
    "16": "Gale",           # 強風
    "18": "High Wave",      # 波浪
    "19": "Storm Surge",    # 高潮
    "20": "Thunderstorm",   # 雷
    "22": "Dense Fog",      # 濃霧
}

def _utc_iso(dt_str: str) -> str:
    """
    Convert JMA reportDatetime (e.g., '2025-08-08T09:55:00+09:00') to UTC ISO8601 with Z.
    """
    try:
        dt = datetime.fromisoformat(dt_str)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        utc = dt.astimezone(timezone.utc)
        return utc.isoformat().replace("+00:00", "Z")
    except Exception:
        return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def _condition_suffix(jp: Optional[str]) -> str:
    """
    Map JMA 'condition' field to English suffix shown on the site.
    """
    if not jp:
        return ""
    has_landslide = "土砂" in jp  # 土砂災害
    has_inundation = "浸水" in jp  # 浸水害
    if has_landslide and has_inundation:
        return " (Landslide/Inundation)"
    if has_landslide:
        return " (Landslide)"
    if has_inundation:
        return " (Inundation)"
    return ""

def _level_from_status(jp_status: Optional[str]) -> str:
    """
    Determine level label for 'warnings' rows:
      - '特別警報' -> 'Alert' (Special Warning)
      - otherwise -> 'Warning'
    (Advisories live in 'attentions' and are filtered separately.)
    """
    if not jp_status:
        return "Warning"
    if "特別警報" in jp_status:
        return "Alert"
    return "Warning"

def _include_warning_status(jp_status: Optional[str]) -> bool:
    """
    Only include active/ongoing warnings. Skip advisories and cancellations/downgrades.
    """
    if not jp_status:
        return True
    if "解除" in jp_status:       # canceled
        return False
    if "注意報" in jp_status:     # advisory / downgraded to advisory
        return False
    return True

def _load_areacode_dict(conf: dict) -> Dict[str, Dict[str, Any]]:
    """
    Load area code mapping. Prefer conf['area_code_file'] if present; else data/areacode.json
    If not found, return empty dict (we'll fall back to raw codes).
    """
    # Try explicit file from conf
    path = conf.get("area_code_file")
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"[JMA DEBUG] Failed to load area_code_file '{path}': {e}")

    # Try bundled data/areacode.json next to this module
    here = os.path.dirname(os.path.abspath(__file__))
    default_path = os.path.join(here, "data", "areacode.json")
    if os.path.exists(default_path):
        try:
            with open(default_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"[JMA DEBUG] Failed to load default areacode '{default_path}': {e}")

    logging.warning("[JMA DEBUG] areacode.json not found; using codes as names.")
    return {"offices": {}, "centers": {}}

async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Build JMA office-level Warning/Alert/Emergency entries from the public JSON.
    Output fields used by your app:
      - title: e.g., "Warning – Heavy Rain (Landslide)"
      - region: e.g., "Hokkaido: Soya"
      - level: "Warning" | "Alert" | "Emergency" (Emergency not seen often)
      - type:  English phenomenon (e.g., "Heavy Rain")
      - summary: empty
      - published: UTC ISO8601
      - link: JMA English office page with area_code
    """
    try:
        # 1) Load map.json
        resp = await client.get(JMA_MAP_URL, timeout=20.0, headers={"User-Agent": "weathermonitor/1.0"})
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            logging.warning("[JMA DEBUG] Unexpected map.json shape; expected list.")
            return {"entries": [], "source": JMA_MAP_URL}

        # 2) Load area codes (for names)
        ac = _load_areacode_dict(conf)
        offices = ac.get("offices", {})
        centers = ac.get("centers", {})

        def office_full_name(code: str) -> str:
            off = offices.get(code, {})
            office_name = off.get("enName", code)
            center_code = off.get("parent")
            pref_name = centers.get(center_code, {}).get("enName")
            return f"{pref_name}: {office_name}" if pref_name else office_name

        entries: List[Dict[str, Any]] = []

        # 3) Walk every entry (each has 'reportDatetime' and 'areaTypes' with 'areas')
        for block in data:
            report_dt = _utc_iso(block.get("reportDatetime", ""))
            area_types = block.get("areaTypes") or []
            for at in area_types:
                areas = at.get("areas") or []
                # We only care about "offices" rows (we detect by code presence in offices dict)
                contains_office_codes = any(a.get("code") in offices for a in areas)
                if not contains_office_codes:
                    continue

                for area in areas:
                    code = area.get("code")
                    if not code or code not in offices:
                        continue

                    region_str = office_full_name(code)
                    # 'warnings' => warning/special-warning (keep)
                    for w in area.get("warnings", []) or []:
                        if not _include_warning_status(w.get("status")):
                            continue
                        phen = PHENOMENON.get(w.get("code", ""), w.get("code", ""))
                        level = _level_from_status(w.get("status"))
                        # Emergency (緊急警報) rarely shows; if it appears in status, upgrade.
                        if w.get("status") and "緊急" in w["status"]:
                            level = "Emergency"

                        # Suffix like (Landslide) / (Inundation) for Heavy Rain
                        suffix = _condition_suffix(w.get("condition"))
                        title = f"{level} – {phen}{suffix}"

                        entries.append({
                            "title": title,
                            "region": region_str,
                            "level": level,
                            "type": phen,
                            "summary": "",
                            "published": report_dt,
                            "link": f"https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={code}",
                        })

                    # We explicitly ignore 'attentions' (advisories)

        logging.warning(f"[JMA DEBUG] Parsed {len(entries)} office warnings/alerts")
        return {"entries": entries, "source": JMA_MAP_URL}

    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {e}")
        return {"entries": [], "error": str(e), "source": JMA_MAP_URL}
