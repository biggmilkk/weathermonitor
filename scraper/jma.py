# scraper/jma.py

import json
import logging
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime, timezone
import httpx

OFFICE_JSON = "https://www.jma.go.jp/bosai/warning/data/warning/{office}.json"
OFFICE_PAGE = "https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={office}"

# Phenomena we care about (codes in JMA warning JSONs)
# Your requested English labels:
#   Heavy Rain (Inundation), Heavy Rain (Landslide), Flood, Storm, Gale,
#   High Wave, Storm Surge, Thunder Storm, Dense Fog
PHENOMENON = {
    "04": "Flood",
    # Heavy rain is split into variants; base code 03 or 10
    # will be expanded to "(Inundation)" or "(Landslide)" from attentions/levels.
    "14": "Thunder Storm",
    "15": "Gale",
    "16": "High Wave",
    "19": "Storm Surge",
    "20": "Dense Fog",
    # Some offices expose "18" as explicit inundation (rare)
    "18": "Heavy Rain (Inundation)",
}

# JMA uses either "03" or "10" for heavy rain buckets depending on office
HEAVY_RAIN_CODES: Set[str] = {"03", "10"}

def _utc_pub(jst_iso: str) -> str:
    """Convert +09:00 ISO to 'Fri, 08 Aug 2025 06:27 UTC'."""
    if not jst_iso:
        return ""
    try:
        dt = datetime.fromisoformat(jst_iso.replace("Z", "+00:00"))
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%a, %d %b %Y %H:%M UTC")
    except Exception:
        return jst_iso

def _load_region_map(path: str) -> Dict[str, str]:
    """Your curated area-code → 'Pref: Region' mapping. Only these are allowed."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Normalize keys to strings
            return {str(k): v for k, v in data.items()}
    except Exception as e:
        logging.warning(f"[JMA DEBUG] Failed to load region map '{path}': {e}")
        return {}

async def _fetch_office(client: httpx.AsyncClient, office: str) -> Optional[Dict[str, Any]]:
    url = OFFICE_JSON.format(office=office)
    try:
        r = await client.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.warning(f"[JMA DEBUG] fetch {office} failed: {e}")
        return None

def _iter_area_blocks(doc: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    """Yield (reportDatetime, areas[]) from each 'areaTypes' block."""
    pub = doc.get("reportDatetime", "")
    for block in doc.get("areaTypes", []):
        areas = block.get("areas")
        if isinstance(areas, list):
            yield pub, areas

# ---------- Heavy Rain helpers ----------

def _heavy_rain_variant_and_level(w: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Decide Heavy Rain variant and possibly level override from attentions/levels.

    Rules you requested:
      - '土砂災害警戒' -> Alert – Heavy Rain (Landslide)
      - '浸水警戒'   -> Warning – Heavy Rain (Inundation)
      - If only '…注意' (advisory), skip.
    """
    attentions = w.get("attentions") or []
    if isinstance(attentions, list):
        txt = " ".join(str(s) for s in attentions)

        # Strong signals with 警戒
        if "土砂災害警戒" in txt:
            return "Landslide", "Alert"
        if "浸水警戒" in txt:
            return "Inundation", "Warning"

        # If we only see 注意 (advisory), skip entirely
        if ("土砂" in txt and "注意" in txt) or ("浸水" in txt and "注意" in txt):
            return None, None

    # Fallback: look at levels' 'type' labels (no level override here)
    levels = w.get("levels")
    if isinstance(levels, list):
        for lvl in levels:
            t = str(lvl.get("type", ""))
            if "浸水" in t:
                return "Inundation", None
            if "土砂" in t:
                return "Landslide", None

    return None, None

def _include_other_phenomenon(code: str, w: Dict[str, Any]) -> Optional[str]:
    """
    Decide whether to include non-heavy-rain phenomena (14,15,16,19,20,18)
    and return its English label.

    We include them only if:
      - status explicitly contains '警報' (warning/special/urgent), or
      - attentions contain '警戒' (vigilance).
    This keeps out advisory-only cases (e.g., 雷注意報, 濃霧注意報).
    """
    label = PHENOMENON.get(code)
    if not label:
        return None

    status = str(w.get("status", "") or "")
    if "警報" in status or "特別警報" in status or "緊急" in status:
        return label

    attentions = w.get("attentions") or []
    if isinstance(attentions, list):
        txt = " ".join(str(s) for s in attentions)
        if "警戒" in txt:
            return label

    # Otherwise treat as advisory-only -> skip
    return None

# ---------------------------------------

async def scrape_jma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    conf required:
      - office_codes: list of office codes (e.g., ["011000","012000",...])
      - region_map_file: path to curated area codes JSON (only these areas are parsed)
    """
    office_codes: List[str] = conf.get("office_codes") or []
    region_map_path: str = conf.get("region_map_file") or "scraper/region_area_codes.json"

    if not office_codes:
        logging.warning("[JMA DEBUG] No office_codes configured.")
        return {"entries": [], "source": "JMA offices"}

    region_map = _load_region_map(region_map_path)
    if not region_map:
        logging.warning("[JMA DEBUG] Region map empty; nothing will match (0 entries expected).")

    allowed_codes: Set[str] = set(region_map.keys())

    dedupe: Set[Tuple[str, str, str]] = set()  # (area_code, phenomenon, level)
    entries: List[Dict[str, Any]] = []

    for office in office_codes:
        doc = await _fetch_office(client, office)
        if not doc:
            continue

        office_link = OFFICE_PAGE.format(office=office)

        for pub_iso, areas in _iter_area_blocks(doc):
            published_str = _utc_pub(pub_iso) or _utc_pub(doc.get("reportDatetime") or "")

            for area in areas:
                area_code = str(area.get("code", ""))

                # Only parse areas the user cares about
                if area_code not in allowed_codes:
                    continue

                region_name = region_map.get(area_code, area_code)
                warnings = area.get("warnings")
                if not isinstance(warnings, list):
                    continue

                for w in warnings:
                    code = str(w.get("code", ""))

                    # --- Heavy Rain handling (03/10) ---
                    if code in HEAVY_RAIN_CODES:
                        variant, override_level = _heavy_rain_variant_and_level(w)
                        if not variant:
                            continue  # no valid variant or only advisory

                        level = override_level
                        if not level:
                            # if no override (rare), do not guess -> skip to avoid wrong level
                            continue

                        pheno = f"Heavy Rain ({variant})"
                        key = (area_code, pheno, level)
                        if key in dedupe:
                            continue
                        dedupe.add(key)

                        entries.append({
                            "title": f"{level} – {pheno}",
                            "region": region_name,
                            "type": pheno,
                            "level": level,
                            "link": office_link,
                            "published": published_str,
                        })
                        continue

                    # --- Flood (04) as Warning when issued/continuing ---
                    if code == "04":
                        status = str(w.get("status", "") or "")
                        if any(s in status for s in ("発表", "継続", "警報")) and "解除" not in status:
                            pheno = "Flood"
                            level = "Warning"
                            key = (area_code, pheno, level)
                            if key not in dedupe:
                                dedupe.add(key)
                                entries.append({
                                    "title": f"{level} – {pheno}",
                                    "region": region_name,
                                    "type": pheno,
                                    "level": level,
                                    "link": office_link,
                                    "published": published_str,
                                })
                        continue

                    # --- Other phenomena (14,15,16,19,20,18) only if warning/special/urgent or '警戒' present ---
                    pheno_other = _include_other_phenomenon(code, w)
                    if pheno_other:
                        # Level: default to "Warning" unless status indicates "Alert"/"Emergency"
                        status = str(w.get("status", "") or "")
                        if "緊急" in status:
                            level = "Emergency"
                        elif "特別警報" in status:
                            level = "Alert"
                        else:
                            level = "Warning"

                        key = (area_code, pheno_other, level)
                        if key in dedupe:
                            continue
                        dedupe.add(key)

                        entries.append({
                            "title": f"{level} – {pheno_other}",
                            "region": region_name,
                            "type": pheno_other,
                            "level": level,
                            "link": office_link,
                            "published": published_str,
                        })

    logging.warning(f"[JMA DEBUG] Parsed {len(entries)} filtered warnings/alerts")
    return {"entries": entries, "source": "JMA offices"}
