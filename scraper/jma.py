import json
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

import httpx

# -----------------------------
# Constants & simple mappings
# -----------------------------

OFFICE_JSON = "https://www.jma.go.jp/bosai/warning/data/warning/{office}.json"
OFFICE_PAGE = "https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={office}"

# Phenomenon code → English label (subset relevant to your UI list)
PHENOMENON = {
    "04": "Flood",
    "10": "Heavy Rain",      # (variant decided from 'attentions' or levels)
    "14": "Thunder Storm",
    "15": "High Wave",
    "18": "Heavy Rain (Inundation)",  # sometimes appears as separate code in some offices
    "19": "Storm Surge",
    "20": "Dense Fog",
    # keep add-ons here if JMA adds new codes you care about
}

# Japanese status → level we show (filter out advisories/clears)
# - "発表" (issued), "継続" (continues) => Warning (unless it's explicitly a special warning)
# - "特別警報" (special warning) => Alert
# - "緊急警報" (emergency) => Emergency (extremely rare)
# - "注意報" (advisory) => skip
# - "解除" (cleared) => skip
def _status_to_level(status: str) -> Optional[str]:
    if not status:
        return None
    if "緊急" in status:          # emergency
        return "Emergency"
    if "特別警報" in status:       # special warning
        return "Alert"
    if "注意報" in status:         # advisory
        return None
    if "解除" in status:           # cleared
        return None
    # Common cases: 発表, 継続, 警報から注意報(=downgraded)
    if "警報から注意報" in status:   # downgraded to advisory -> skip
        return None
    if "発表" in status or "継続" in status or "警報" in status:
        return "Warning"
    return None


def _utc_pub(jst_iso: str) -> str:
    """
    Convert JMA's ISO with +09:00 to RFC-like UTC string for display.
    Example in → '2025-08-08T15:27:00+09:00'
    out → 'Fri, 08 Aug 2025 06:27 UTC'
    """
    if not jst_iso:
        return ""
    try:
        dt = datetime.fromisoformat(jst_iso.replace("Z", "+00:00"))
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%a, %d %b %Y %H:%M UTC")
    except Exception:
        return jst_iso


def _load_region_map(path: str) -> Dict[str, str]:
    """
    Your curated region map: { "014020": "Hokkaido: Kushiro Region", ... }
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
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


def _heavy_rain_variant_from_warning(w: Dict[str, Any]) -> Optional[str]:
    """
    Decide Heavy Rain variant from the warning entry itself.
    Priority from 'attentions' strings if present:
      - contains '浸水' => Inundation
      - contains '土砂' => Landslide
    Fallback: None (caller can try reading from 'levels' if desired)
    """
    # attentions can be list like ["土砂災害注意","浸水注意"]
    for key in ("attentions",):
        vals = w.get(key)
        if isinstance(vals, list):
            joined = " ".join(str(v) for v in vals)
            if "浸水" in joined:
                return "Inundation"
            if "土砂" in joined:
                return "Landslide"
    return None


def _heavy_rain_variant_from_levels(w: Dict[str, Any]) -> Optional[str]:
    """
    Inspect 'levels' section when present; types may include '浸水害危険度' or '土砂災害危険度'.
    """
    levels = w.get("levels")
    if not isinstance(levels, list):
        return None
    for lvl in levels:
        t = str(lvl.get("type", ""))
        if "浸水" in t:
            return "Inundation"
        if "土砂" in t:
            return "Landslide"
    return None


def _phenomenon_name(code: str, w: Dict[str, Any]) -> Optional[str]:
    """
    For code '10' (Heavy Rain), append the variant. For others, use PHENOMENON map.
    """
    if code == "10":
        variant = _heavy_rain_variant_from_warning(w) or _heavy_rain_variant_from_levels(w)
        if not variant:
            # If truly unknown, we can skip Heavy Rain rather than show ambiguous
            return None
        return f"Heavy Rain ({variant})"
    # Some offices may encode inundation as code 18 explicitly:
    if code == "18":
        return "Heavy Rain (Inundation)"
    name = PHENOMENON.get(code)
    return name


def _iter_area_blocks(doc: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Yield (publish_time_iso, area_block['areas']) for each block set under 'areaTypes'.
    """
    pub = doc.get("reportDatetime", "")
    for block in doc.get("areaTypes", []):
        areas = block.get("areas")
        if isinstance(areas, list):
            yield pub, areas


async def scrape_jma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    Configuration (required):
      - office_codes: List[str]   (e.g. ["011000","012000","014100",...])
      - region_map_file: str      (path to your curated region_area_codes.json)

    Returns {"entries": [...], "source": "JMA offices"}
    """
    office_codes: List[str] = conf.get("office_codes") or []
    region_map_path: str = conf.get("region_map_file") or "scraper/region_area_codes.json"

    if not office_codes:
        logging.warning("[JMA DEBUG] No office_codes configured.")
        return {"entries": [], "source": "JMA offices"}

    region_map = _load_region_map(region_map_path)
    if not region_map:
        logging.warning("[JMA DEBUG] Region map is empty; will fall back to raw codes.")

    entries: List[Dict[str, Any]] = []

    # Use the provided client as-is (do NOT re-open it)
    for office in office_codes:
        doc = await _fetch_office(client, office)
        if not doc:
            continue

        published = _utc_pub(doc.get("reportDatetime") or "")

        for pub_iso, areas in _iter_area_blocks(doc):
            # prefer top-level published; fallback to this block time if needed
            published_str = _utc_pub(pub_iso) or published

            for area in areas:
                area_code = str(area.get("code", ""))
                region_name = region_map.get(area_code, area_code)  # fallback to code

                warnings = area.get("warnings")
                if not isinstance(warnings, list):
                    continue

                for w in warnings:
                    code = str(w.get("code", ""))
                    status = str(w.get("status", "") or "")
                    level = _status_to_level(status)
                    if level is None:
                        continue  # skip advisories/clears/etc.

                    pheno = _phenomenon_name(code, w)
                    if not pheno:
                        continue  # skip if we can't resolve a valid phenomenon label

                    # Build link to the office page (not the JSON)
                    link = OFFICE_PAGE.format(office=office)

                    entries.append({
                        "title": f"{level} – {pheno}",
                        "region": region_name,
                        "type": pheno,
                        "level": level,
                        "link": link,
                        "published": published_str,
                    })

    logging.warning(f"[JMA DEBUG] Parsed {len(entries)} office warnings/alerts")
    return {"entries": entries, "source": "JMA offices"}
