import os
import json
import datetime
import logging
from typing import Dict, Any, List, Optional

import httpx

# Phenomenon code → English label (as seen on JMA English UI)
PHENOMENON: Dict[str, str] = {
    "10": "Heavy Rain",                # refined below with condition
    "20": "Flood",
    "30": "Storm",
    "40": "Gale",
    "50": "High Wave",
    "60": "Storm Surge",
    "70": "Thunderstorm",
    "80": "Dense Fog",
    # If JMA adds codes, they'll pass through as the numeric code string
}

JP_TO_EN_LEVEL = {
    "注意報": "Advisory",
    "警報": "Warning",
    "特別警報": "Alert",      # JMA shows this as “Alert” on English UI (Special Warning)
    "緊急警報": "Emergency",   # Rare; included for completeness
}


def _safe_load_area_names(path: Optional[str]) -> Dict[str, str]:
    if not path:
        logging.warning("[JMA DEBUG] area_code_file not provided; using codes as names.")
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Expecting {"110000": "Hokkaido: ....", ...}
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
        logging.warning("[JMA DEBUG] areacode.json is not a dict; using codes as names.")
        return {}
    except FileNotFoundError:
        logging.warning("[JMA DEBUG] areacode.json not found; using codes as names.")
        return {}
    except Exception as e:
        logging.warning(f"[JMA DEBUG] Failed to load areacode.json: {e}")
        return {}


def _english_level_from_text(text: str) -> Optional[str]:
    """
    Heuristic: map any Japanese level keyword in the text to EN level.
    """
    if "緊急" in text or "緊急警報" in text:
        return "Emergency"
    if "特別警報" in text:
        return "Alert"
    if "警報" in text:
        return "Warning"
    if "注意報" in text:
        return "Advisory"
    return None


def _infer_level(status: str, attentions: List[str]) -> Optional[str]:
    """
    Determine the EN level for a warning record, using both status and attentions.
    We only keep Warning / Alert / Emergency.
    """
    # Check attentions first (often contains the “...警報” tokens)
    for a in attentions or []:
        lvl = _english_level_from_text(a)
        if lvl in ("Emergency", "Alert", "Warning"):
            return lvl
        if lvl == "Advisory":
            # keep looking—maybe status escalates it
            pass

    # Fallback to status
    lvl = _english_level_from_text(status or "")
    if lvl in ("Emergency", "Alert", "Warning"):
        return lvl

    # Not a warning-grade alert; drop it
    return None


def _refine_heavy_rain_label(pcode: str, condition: str) -> str:
    """
    Refine 'Heavy Rain' into (Landslide) / (Inundation) when the condition hints
    at 土砂災害 (landslide) or 浸水害 (inundation).
    """
    base = PHENOMENON.get(pcode, pcode)
    if pcode != "10":
        return base

    cond = condition or ""
    has_landslide = ("土砂" in cond) or ("土砂災害" in cond)
    has_inundation = ("浸水" in cond) or ("浸水害" in cond)

    if has_landslide and has_inundation:
        return "Heavy Rain (Landslide/Inundation)"
    if has_landslide:
        return "Heavy Rain (Landslide)"
    if has_inundation:
        return "Heavy Rain (Inundation)"
    return base


async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Scrape the JMA JSON 'map' endpoint and build entries for the 'offices' level only.
    Keeps only Warning / Alert / Emergency. Maps office codes to friendly names.
    """
    url = conf.get("url", "https://www.jma.go.jp/bosai/warning/data/warning/map.json")
    area_code_file = conf.get("area_code_file")  # e.g., "scraper/areacode.json"

    area_names = _safe_load_area_names(area_code_file)
    entries: List[Dict[str, Any]] = []

    try:
        resp = await client.get(url, timeout=20.0)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {e}")
        return {"entries": [], "error": str(e), "source": url}

    # map.json is a list of “reports” (usually length 1)
    if not isinstance(data, list):
        logging.warning("[JMA DEBUG] map.json shape not a list; got %s", type(data).__name__)
        return {"entries": [], "source": url}

    now_iso = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    total_office_items = 0

    for report in data:
        if not isinstance(report, dict):
            continue

        report_dt = (report.get("reportDatetime") or now_iso).replace("+09:00", "Z")
        area_types = report.get("areaTypes", []) or []

        for at in area_types:
            # Keep ONLY “offices”
            code_key = (at.get("code") or "").lower()
            name_key = (at.get("name") or "").lower()
            if code_key != "offices" and name_key != "offices":
                continue

            for area in at.get("areas", []) or []:
                acode = str(area.get("code", "")).strip()
                warnings = area.get("warnings", []) or []
                if not acode or not warnings:
                    continue

                # Friendly region name
                region_name = area_names.get(acode, acode)
                # Super-defensive: skip obvious municipality-like long numeric codes
                if len(acode) >= 7 and region_name == acode:
                    # Unmapped 7+ digit code → likely municipality; skip
                    continue

                for w in warnings:
                    if not isinstance(w, dict):
                        continue

                    pcode = str(w.get("code", "")).strip()  # phenomenon code like "10"
                    status = w.get("status", "")            # JP text
                    attentions = w.get("attentions", []) or []
                    condition = w.get("condition", "") or ""

                    level = _infer_level(status, attentions)
                    if level not in ("Warning", "Alert", "Emergency"):
                        # Drop Advisories and anything unknown
                        continue

                    phen = _refine_heavy_rain_label(pcode, condition)
                    if phen == pcode:
                        # Unknown code → leave as original numeric (rare)
                        phen = PHENOMENON.get(pcode, pcode)

                    entries.append({
                        "title": f"{level} – {phen}",
                        "region": region_name,
                        "level": level,
                        "type": phen,
                        "summary": condition,
                        "published": report_dt,
                        "link": f"https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={acode}",
                    })
                    total_office_items += 1

    logging.warning(f"[JMA DEBUG] Parsed {total_office_items} office warnings/alerts")
    return {"entries": entries, "source": url}
