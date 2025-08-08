# scraper/jma.py
import json
import logging
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime, timezone
import httpx

OFFICE_JSON = "https://www.jma.go.jp/bosai/warning/data/warning/{office}.json"
OFFICE_PAGE = "https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={office}"

# Human-readable phenomenon names (base)
PHENOMENON = {
    "04": "Flood",
    "10": "Heavy Rain",               # will expand to (Inundation/Landslide)
    "14": "Thunder Storm",
    "15": "High Wave",
    "19": "Storm Surge",
    "20": "Dense Fog",
    # Occasionally seen as an explicit inundation hazard in some offices:
    "18": "Heavy Rain (Inundation)",
}

# Codes that are typically *warnable* (not just advisory-only),
# used alongside the status text to decide if "継続/発表" should count as Warning.
WARNABLE_CODES = {"04", "10", "15", "18", "19"}  # 14/20 usually advisory unless explicitly 警報

def _status_to_level(status: str) -> Optional[str]:
    """
    Map JMA status (JP) to the levels we keep.
    - 緊急... => Emergency
    - 特別警報 => Alert
    - 注意報 / 解除 / 警報から注意報 => drop
    - 発表 / 継続 / (contains 警報) => Warning
    """
    if not status:
        return None
    if "緊急" in status:
        return "Emergency"
    if "特別警報" in status:
        return "Alert"
    if "注意報" in status:
        return None
    if "解除" in status:
        return None
    if "警報から注意報" in status:
        return None
    if "発表" in status or "継続" in status or "警報" in status:
        return "Warning"
    return None

def _is_warnable_for_code(code: str, status: str, level: str) -> bool:
    """
    Additional guard so we don't mistakenly promote advisory-only phenomena.
    - Thunder Storm (14) & Dense Fog (20) are usually advisories.
      Keep them ONLY if the status text clearly indicates a 警報 (or Alert/Emergency).
    - For other codes, allow if they’re in WARNABLE_CODES.
    """
    code = str(code)
    if level in ("Alert", "Emergency"):
        return True
    if code in ("14", "20"):
        # require explicit 警報 in the status text to treat as Warning
        return "警報" in status
    return code in WARNABLE_CODES

def _utc_pub(jst_iso: str) -> str:
    """Convert '+09:00' ISO string to 'Fri, 08 Aug 2025 06:27 UTC'."""
    if not jst_iso:
        return ""
    try:
        dt = datetime.fromisoformat(jst_iso)
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%a, %d %b %Y %H:%M UTC")
    except Exception:
        # Fallback: try replacing trailing 'Z' if present
        try:
            dt = datetime.fromisoformat(jst_iso.replace("Z", "+00:00"))
            dt_utc = dt.astimezone(timezone.utc)
            return dt_utc.strftime("%a, %d %b %Y %H:%M UTC")
        except Exception:
            return jst_iso

def _load_region_map(path: str) -> Tuple[Dict[str, str], Set[str]]:
    """
    Load your curated region map.
    Supports either:
      { "Hokkaido: Kushiro Region": "014010", ... }  (name -> code)
    or
      { "014010": "Hokkaido: Kushiro Region", ... }  (code -> name)
    Returns:
      code_to_name, allowed_codes
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        logging.warning(f"[JMA DEBUG] Failed to load region map '{path}': {e}")
        return {}, set()

    # Detect direction by peeking at a key
    if not raw:
        return {}, set()

    # Heuristic: codes are all digits; names are not (contain letters/colon/space)
    sample_key = next(iter(raw.keys()))
    if str(sample_key).isdigit():
        # Already code -> name
        code_to_name = {str(k): str(v) for k, v in raw.items()}
        allowed_codes = set(code_to_name.keys())
    else:
        # name -> code; invert
        name_to_code = {str(k): str(v) for k, v in raw.items()}
        code_to_name = {code: name for name, code in name_to_code.items()}
        allowed_codes = set(code_to_name.keys())

    return code_to_name, allowed_codes

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
    Use 'attentions' to resolve Heavy Rain variant.
    e.g., ["土砂災害注意","浸水注意"]
    """
    vals = w.get("attentions")
    if isinstance(vals, list):
        joined = " ".join(str(v) for v in vals)
        if "浸水" in joined:
            return "Inundation"
        if "土砂" in joined:
            return "Landslide"
    return None

def _heavy_rain_variant_from_levels(w: Dict[str, Any]) -> Optional[str]:
    """
    Use 'levels' -> [{ "type": "土砂災害危険度" / "浸水害危険度", ...}] to resolve variant.
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
    Return display name for a warning code, resolving Heavy Rain variants.
    If we cannot resolve Heavy Rain to (Inundation/Landslide), skip it to avoid ambiguity.
    """
    code = str(code)
    if code == "10":
        variant = _heavy_rain_variant_from_warning(w) or _heavy_rain_variant_from_levels(w)
        if not variant:
            return None
        return f"Heavy Rain ({variant})"
    if code == "18":
        return "Heavy Rain (Inundation)"
    return PHENOMENON.get(code)

def _iter_area_blocks(doc: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    """Yield (reportDatetime, areas[]) for each block in areaTypes."""
    pub = doc.get("reportDatetime", "")
    for block in doc.get("areaTypes", []):
        areas = block.get("areas")
        if isinstance(areas, list):
            yield pub, areas

async def scrape_jma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    conf required:
      - office_codes: list[str] of office codes (e.g., ["020000","050000","460100", ...])
      - region_map_file: path to curated region code map (either name->code or code->name)
    """
    office_codes: List[str] = conf.get("office_codes") or []
    region_map_path: str = conf.get("region_map_file") or "scraper/region_area_codes.json"

    if not office_codes:
        logging.warning("[JMA DEBUG] No office_codes configured.")
        return {"entries": [], "source": "JMA offices"}

    code_to_name, allowed_codes = _load_region_map(region_map_path)
    if not allowed_codes:
        logging.warning("[JMA DEBUG] Region map empty; nothing will match.")
        return {"entries": [], "source": "JMA offices"}

    dedupe: Set[Tuple[str, str, str]] = set()  # (area_code, phenomenon, level)
    entries: List[Dict[str, Any]] = []

    for office in office_codes:
        doc = await _fetch_office(client, office)
        if not doc:
            continue

        for pub_iso, areas in _iter_area_blocks(doc):
            published_str = _utc_pub(pub_iso) or _utc_pub(doc.get("reportDatetime") or "")

            for area in areas:
                area_code = str(area.get("code", ""))

                # Only parse areas the user cares about
                if area_code not in allowed_codes:
                    continue

                region_name = code_to_name.get(area_code, area_code)
                warnings = area.get("warnings")
                if not isinstance(warnings, list):
                    continue

                for w in warnings:
                    code = str(w.get("code", ""))
                    status = str(w.get("status", "") or "").strip()
                    level = _status_to_level(status)
                    if level is None:
                        continue  # skip advisories/clears/downgrades

                    if not _is_warnable_for_code(code, status, level):
                        continue  # drop advisory-only types unless explicitly a warning

                    pheno = _phenomenon_name(code, w)
                    if not pheno:
                        continue  # skip if no valid phenomenon resolution

                    key = (area_code, pheno, level)
                    if key in dedupe:
                        continue
                    dedupe.add(key)

                    entries.append({
                        "title": f"{level} – {pheno}",
                        "region": region_name,                    # "Pref: Region" from your map
                        "type": pheno,
                        "level": level,
                        "link": OFFICE_PAGE.format(office=office), # JMA UI page
                        "published": published_str,
                    })

    logging.warning(f"[JMA DEBUG] Parsed {len(entries)} filtered warnings/alerts")
    return {"entries": entries, "source": "JMA offices"}
