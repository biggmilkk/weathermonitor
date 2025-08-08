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

# Codes that are typically warnable (not just advisory-only),
# used alongside the status text to decide if 継続/発表 should count as Warning.
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
    Strict rule:
      - If Alert/Emergency -> keep.
      - For Flood(04), Heavy Rain(10/18): allow on 発表/継続 (already mapped to 'Warning' by _status_to_level).
      - For Thunder Storm(14), High Wave(15), Storm Surge(19), Dense Fog(20): 
        require explicit '警報' in status (or Alert/Emergency).
    """
    code = str(code)
    if level in ("Alert", "Emergency"):
        return True

    # Codes allowed with plain 発表/継続:
    if code in {"04", "10", "18"}:
        return True

    # Everything else needs explicit 警報 wording to count as a Warning
    return "警報" in status

def _utc_pub(jst_iso: str) -> str:
    """Convert '+09:00' ISO to 'Fri, 08 Aug 2025 06:27 UTC'."""
    if not jst_iso:
        return ""
    try:
        dt = datetime.fromisoformat(jst_iso)
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%a, %d %b %Y %H:%M UTC")
    except Exception:
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

    if not raw:
        return {}, set()

    # Detect direction (codes are numeric)
    sample_key = next(iter(raw.keys()))
    if str(sample_key).isdigit():
        # Already code -> name
        code_to_name = {str(k): str(v) for k, v in raw.items()}
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

def _scan_heavy_rain_variants_from_levels(w: Dict[str, Any], out: Set[str]) -> None:
    """Look at warnings[].levels to infer浸水/土砂 variants."""
    levels = w.get("levels")
    if isinstance(levels, list):
        for lvl in levels:
            t = str(lvl.get("type", ""))
            if "浸水" in t:
                out.add("Inundation")
            if "土砂" in t:
                out.add("Landslide")
            # Sometimes 'attentions' are tucked inside levels/localAreas
            for la in lvl.get("localAreas", []) or []:
                atts = la.get("attentions")
                if isinstance(atts, list):
                    joined = " ".join(map(str, atts))
                    if "浸水" in joined:
                        out.add("Inundation")
                    if "土砂" in joined:
                        out.add("Landslide")

def _collect_hr_variants_from_timeseries(doc: Dict[str, Any]) -> Dict[str, Set[str]]:
    """
    Build area_code -> set of {'Inundation','Landslide'} from timeSeries.
    This lets us resolve code==10 at the top-level when the row lacks variant detail.
    """
    res: Dict[str, Set[str]] = {}
    for ts_block in doc.get("timeSeries", []) or []:
        for area_block in ts_block.get("areaTypes", []) or []:
            for area in area_block.get("areas", []) or []:
                a_code = str(area.get("code", ""))
                if not a_code:
                    continue
                for w in area.get("warnings", []) or []:
                    if str(w.get("code", "")) != "10":
                        continue
                    s = res.setdefault(a_code, set())
                    _scan_heavy_rain_variants_from_levels(w, s)
    return res

def _heavy_rain_variant_from_row(w: Dict[str, Any]) -> Optional[str]:
    """
    Try to resolve Heavy Rain variant from the same warnings[] row (attentions/levels).
    Prefer Landslide if both found, else Inundation.
    """
    found: Set[str] = set()

    # From row-level attentions
    vals = w.get("attentions")
    if isinstance(vals, list):
        joined = " ".join(str(v) for v in vals)
        if "浸水" in joined:
            found.add("Inundation")
        if "土砂" in joined:
            found.add("Landslide")

    # From row-level levels[].type and possible localAreas attentions
    _scan_heavy_rain_variants_from_levels(w, found)

    if "Landslide" in found:
        return "Landslide"
    if "Inundation" in found:
        return "Inundation"
    return None

def _phenomenon_name(code: str, w: Dict[str, Any], ts_variants: Optional[Set[str]]) -> Optional[str]:
    """
    Return display name for a warning code, resolving Heavy Rain variants.
    If we cannot resolve Heavy Rain to (Inundation/Landslide), skip it to avoid ambiguity.
    """
    code = str(code)
    if code == "10":
        # Try the row, then fallback to any variant we learned from timeSeries for this area
        v = _heavy_rain_variant_from_row(w)
        if not v and ts_variants:
            if "Landslide" in ts_variants:
                v = "Landslide"
            elif "Inundation" in ts_variants:
                v = "Inundation"
        if not v:
            return None
        return f"Heavy Rain ({v})"
    if code == "18":
        return "Heavy Rain (Inundation)"
    return PHENOMENON.get(code)

def _iter_area_blocks(doc: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    """Yield (reportDatetime, areas[]) for each block in areaTypes."""
    pub = doc.get("reportDatetime", "")
    for block in doc.get("areaTypes", []) or []:
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

        # Pre-scan Heavy Rain variants from timeSeries for this office
        ts_hr_variants = _collect_hr_variants_from_timeseries(doc)

        # Debug: what area codes are present & how many intersect our allowed set
        present_codes: List[str] = []
        for _, areas in _iter_area_blocks(doc):
            present_codes.extend(str(a.get("code", "")) for a in areas if a.get("code") is not None)
        uniq_present = set(present_codes)
        inter = uniq_present & allowed_codes
        logging.warning(
            f"[JMA DEBUG] office {office}: present area rows={len(present_codes)}, "
            f"unique area codes={len(uniq_present)}"
        )
        logging.warning(
            f"[JMA DEBUG] office {office}: intersects allowed={len(inter)}; "
            f"examples={sorted(list(inter))[:5]}"
        )

        added_before = len(entries)

        for pub_iso, areas in _iter_area_blocks(doc):
            published_str = _utc_pub(pub_iso) or _utc_pub(doc.get("reportDatetime") or "")

            for area in areas:
                area_code = str(area.get("code", ""))
                if not area_code or area_code not in allowed_codes:
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

                    pheno = _phenomenon_name(code, w, ts_variants=ts_hr_variants.get(area_code))
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

        logging.warning(f"[JMA DEBUG] office {office}: added so far={len(entries) - added_before}")

    logging.warning(f"[JMA DEBUG] FINAL parsed entries={len(entries)}")
    return {"entries": entries, "source": "JMA offices"}
