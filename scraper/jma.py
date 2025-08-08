# scraper/jma.py
import json
import logging
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime, timezone
import httpx

OFFICE_JSON = "https://www.jma.go.jp/bosai/warning/data/warning/{office}.json"
OFFICE_PAGE = "https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={office}"

# Only the phenomena you asked to keep
ALLOWED_CODES: Set[str] = {"04", "10", "18", "19"}  # Flood, Heavy Rain (+variants), Storm Surge

PHENOMENON = {
    "04": "Flood",
    "10": "Heavy Rain",               # will expand to (Inundation/Landslide)
    "18": "Heavy Rain (Inundation)",
    "19": "Storm Surge",
}

def _status_to_level(status: str) -> Optional[str]:
    """
    Map JMA status (JP) to the levels we keep, STRICT:
      - 緊急...        -> Emergency
      - 特別警報       -> Alert
      - Anything with '注意報' / '解除' / '警報から注意報' -> drop
      - Only treat as Warning if the text actually contains '警報'
        (i.e., don't promote plain '発表' or '継続' anymore).
    """
    if not status:
        return None
    if "緊急" in status:
        return "Emergency"
    if "特別警報" in status:
        return "Alert"
    if "注意報" in status or "解除" in status or "警報から注意報" in status:
        return None
    if "警報" in status:
        return "Warning"
    return None

def _utc_pub(jst_iso: str) -> str:
    """Convert '+09:00' ISO string to 'Fri, 08 Aug 2025 06:27 UTC'."""
    if not jst_iso:
        return ""
    for s in (jst_iso, jst_iso.replace("Z", "+00:00")):
        try:
            dt = datetime.fromisoformat(s)
            return dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M UTC")
        except Exception:
            pass
    return jst_iso

def _load_region_map(path: str) -> Tuple[Dict[str, str], Set[str]]:
    """
    Load curated region map. Accepts either:
      { "Hokkaido: Kushiro Region": "014010", ... }  (name -> code)
      { "014010": "Hokkaido: Kushiro Region", ... }  (code -> name)
    Returns (code_to_name, allowed_codes)
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        logging.warning(f"[JMA DEBUG] Failed to load region map '{path}': {e}")
        return {}, set()

    if not raw:
        return {}, set()

    sample_key = next(iter(raw.keys()))
    if str(sample_key).isdigit():
        code_to_name = {str(k): str(v) for k, v in raw.items()}
    else:
        # invert name -> code
        code_to_name = {str(v): str(k) for k, v in raw.items()}

    return code_to_name, set(code_to_name.keys())

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
    vals = w.get("attentions")
    if isinstance(vals, list):
        joined = " ".join(str(v) for v in vals)
        if "浸水" in joined:
            return "Inundation"
        if "土砂" in joined:
            return "Landslide"
    return None

def _heavy_rain_variant_from_levels(w: Dict[str, Any]) -> Optional[str]:
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
    code = str(code)
    if code not in ALLOWED_CODES:
        return None
    if code == "10":
        variant = _heavy_rain_variant_from_warning(w) or _heavy_rain_variant_from_levels(w)
        if not variant:
            return None  # skip ambiguous plain "Heavy Rain"
        return f"Heavy Rain ({variant})"
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
      - office_codes: list[str] (e.g., ["020000","050000","460100", ...])
      - region_map_file: path to curated region code map (either name->code or code->name)
    """
    office_codes: List[str] = conf.get("office_codes") or []
    region_map_path: str = conf.get("region_map_file") or "scraper/region_area_codes.json"

    if not office_codes:
        logging.warning("[JMA DEBUG] No office_codes configured.")
        return {"entries": [], "source": "JMA offices"}

    code_to_name, allowed_codes = _load_region_map(region_map_path)

    logging.warning(f"[JMA DEBUG] region_map_file='{region_map_path}' "
                    f"loaded {len(code_to_name)} mappings; allowed_codes={len(allowed_codes)}")
    # Show a few examples so we know the direction is right
    try:
        sample_codes = list(sorted(allowed_codes))[:10]
        logging.warning(f"[JMA DEBUG] sample allowed codes: {sample_codes}")
        for sc in sample_codes[:5]:
            logging.warning(f"[JMA DEBUG] {sc} -> {code_to_name.get(sc)}")
    except Exception:
        pass

    if not allowed_codes:
        logging.warning("[JMA DEBUG] Region map empty; nothing will match.")
        return {"entries": [], "source": "JMA offices"}

    dedupe: Set[Tuple[str, str, str]] = set()  # (area_code, phenomenon, level)
    entries: List[Dict[str, Any]] = []

    for office in office_codes:
        doc = await _fetch_office(client, office)
        if not doc:
            logging.warning(f"[JMA DEBUG] skipped office {office} (fetch failed)")
            continue

        # Gather area codes present in the JSON for this office for visibility
        present_codes: Set[str] = set()
        total_area_rows = 0
        for _, areas in _iter_area_blocks(doc):
            for a in areas:
                total_area_rows += 1
                ac = str(a.get("code", ""))
                if ac:
                    present_codes.add(ac)

        logging.warning(f"[JMA DEBUG] office {office}: present area rows={total_area_rows}, "
                        f"unique area codes={len(present_codes)}")
        # Intersection we actually try to parse
        intersect = present_codes & allowed_codes
        logging.warning(f"[JMA DEBUG] office {office}: intersects allowed={len(intersect)}; "
                        f"examples={list(sorted(intersect))[:10]}")

        # Now do the real parse pass
        for pub_iso, areas in _iter_area_blocks(doc):
            published_str = _utc_pub(pub_iso) or _utc_pub(doc.get("reportDatetime") or "")

            for area in areas:
                area_code = str(area.get("code", ""))
                if area_code not in allowed_codes:
                    continue

                region_name = code_to_name.get(area_code, area_code)
                warnings = area.get("warnings")
                if not isinstance(warnings, list):
                    continue

                for w in warnings:
                    code = str(w.get("code", "")).strip()
                    status = str(w.get("status", "") or "").strip()

                    level = _status_to_level(status)
                    if level is None:
                        continue
                    if not _is_warnable_for_code(code, status, level):
                        continue

                    pheno = _phenomenon_name(code, w)
                    if not pheno:
                        continue

                    key = (area_code, pheno, level)
                    if key in dedupe:
                        continue
                    dedupe.add(key)

                    entries.append({
                        "title": f"{level} – {pheno}",
                        "region": region_name,
                        "type": pheno,
                        "level": level,
                        "link": OFFICE_PAGE.format(office=office),
                        "published": published_str,
                    })

        logging.warning(f"[JMA DEBUG] office {office}: added so far={len(entries)}")

    logging.warning(f"[JMA DEBUG] FINAL parsed entries={len(entries)}")
    return {"entries": entries, "source": "JMA offices"}
