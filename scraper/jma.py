import json
import logging
from typing import Dict, List, Tuple
from datetime import datetime, timezone
from dateutil import parser as dateparser

import httpx

# ---- Constants -------------------------------------------------------------

JMA_AREA_JSON = "https://www.jma.go.jp/bosai/common/const/area.json"
JMA_OFFICE_AREA_JSON = "https://www.jma.go.jp/bosai/warning/data/warning/{office}.json"
JMA_LINK_FOR_AREA = "https://www.jma.go.jp/bosai/warning/#area_type=class10s&area_code={code}"

# Default minimal weather code mapping (can be overridden by conf["weather"])
DEFAULT_WEATHER_MAP = {
    "10": "Heavy Rain (Landslide)",    # 土砂災害
    "14": "Thunderstorm",              # 雷
    "18": "Flood",                     # 洪水
    "19": "Storm Surge",               # 高潮
    "20": "Dense Fog",                 # 濃霧
    # Add more if you wish
}

# Status strings in JMA JSON
STATUS_ACTIVE = {"発表", "継続"}   # issue/continue
STATUS_CANCEL = {"解除"}          # cancel

# ---- Helpers --------------------------------------------------------------

def _read_local_region_map(conf: dict) -> Dict[str, str]:
    """
    Read your static display-name -> code mapping file if provided.
    Keys (names) are your source of truth.
    """
    path = conf.get("region_map_file")
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("region_map_file JSON must be an object of {name: code}")
        return data
    except Exception as e:
        logging.warning("[JMA] Failed to read region_map_file '%s': %s", path, e)
        return {}


def _build_live_region_map(area_json: dict) -> Dict[str, str]:
    """
    Build a display-name -> class10 code mapping from live area.json
    to match your local naming style:
      - Most: "Prefecture: Region"
      - Special (single-bucket prefectures like Osaka/Kagawa): "Osaka Prefecture"
    """
    offices = area_json.get("offices", {})
    class10s = area_json.get("class10s", {})
    live_map: Dict[str, str] = {}

    for office_code, office in offices.items():
        en_office = office.get("enName") or office.get("name") or ""
        children = office.get("children") or []

        # Some prefectures (e.g., 270000 Osaka, 370000 Kagawa) have a single class10 node equal to office
        is_single_bucket = (len(children) == 1 and children[0] == office_code)

        if is_single_bucket:
            # Use "Osaka Prefecture", "Kagawa Prefecture", etc.
            if en_office:
                live_map[en_office] = office_code
            continue

        for c10_code in children:
            c10 = class10s.get(c10_code, {})
            en_region = c10.get("enName") or c10.get("name") or ""
            if not en_office or not en_region:
                continue
            display = f"{en_office}: {en_region}"
            live_map[display] = c10_code

    return live_map


def _reconcile_region_codes(local_map: Dict[str, str], live_map: Dict[str, str]) -> Tuple[Dict[str, str], List[Tuple[str, str, str]], List[str], List[str]]:
    """
    Keep *exactly* the keys from local_map (your display names).
    For each key, use the live code if present; otherwise keep the local code.
    Returns (result_map, changed_pairs, missing_in_live, missing_locally_but_present_live)
    """
    result = {}
    changed: List[Tuple[str, str, str]] = []
    missing_in_live: List[str] = []

    for name, local_code in local_map.items():
        live_code = live_map.get(name)
        if live_code:
            result[name] = live_code
            if live_code != local_code:
                changed.append((name, local_code, live_code))
        else:
            # Keep your code, but note mismatch
            result[name] = local_code
            missing_in_live.append(name)

    # (Optional) what live knows that you don't track (just for visibility)
    missing_locally = sorted(set(live_map.keys()) - set(local_map.keys()))
    return result, changed, missing_in_live, missing_locally


def _invert_map(m: Dict[str, str]) -> Dict[str, str]:
    """Invert {name: code} -> {code: name} (last in wins if duplicates)."""
    return {v: k for k, v in m.items()}


def _fmt_utc(dt_s: str) -> str:
    """
    Convert JMA local datetime string (ISO with +09:00) to a friendly UTC stamp like:
    'Fri, 08 Aug 2025 11:58 UTC'
    If parsing fails, return the original string.
    """
    try:
        dt = dateparser.parse(dt_s)
        if not dt.tzinfo:
            # Assume JST if tz missing
            dt = dt.replace(tzinfo=timezone.utc)
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%a, %d %b %Y %H:%M UTC")
    except Exception:
        return dt_s


def _hazard_title(code: str, weather_map: Dict[str, str]) -> str:
    return weather_map.get(code, f"Code {code}")


def _collect_active_area_warnings(area_obj: dict) -> List[Tuple[str, str]]:
    """
    From an 'area' block, return list of (hazard_code, status) where status is active.
    Structure example:
      {"code":"280010","warnings":[{"code":"14","status":"継続"}, ...]}
    """
    out: List[Tuple[str, str]] = []
    for w in area_obj.get("warnings", []) or []:
        code = str(w.get("code", "")).strip()
        status = (w.get("status") or "").strip()
        if status in STATUS_ACTIVE:
            out.append((code, status))
    return out


# ---- Main async scraper ----------------------------------------------------

async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Fetches JMA warnings per office and returns generic entries for your UI.
    Respects your region display names as the master list; only codes are reconciled.
    """
    # 1) Load weather map (overrides default if provided via loader)
    weather_map = dict(DEFAULT_WEATHER_MAP)
    if isinstance(conf.get("weather"), dict):
        weather_map.update({str(k): v for k, v in conf["weather"].items()})

    # 2) Read your local (display-name -> code) mapping
    local_map = _read_local_region_map(conf)
    if not local_map:
        logging.warning("[JMA] No local region_map_file found or empty; continuing with live only (no filtering by your names).")

    # 3) Fetch live area.json, build live mapping
    try:
        resp = await client.get(JMA_AREA_JSON, timeout=20)
        resp.raise_for_status()
        area = resp.json()
    except Exception as e:
        logging.warning("[JMA] Failed to fetch live area.json: %s", e)
        area = {}

    live_map = _build_live_region_map(area) if area else {}

    # 4) Reconcile: keep your names, update codes if live differs
    if local_map:
        region_map, changed, missing_in_live, missing_locally = _reconcile_region_codes(local_map, live_map or {})
        if changed:
            c0 = changed[0]
            logging.warning("[JMA] region code updates: %d changed (e.g. %s: %s→%s)", len(changed), c0[0], c0[1], c0[2])
        if missing_in_live:
            logging.warning("[JMA] %d local keys not found in live area.json (kept local codes). Examples: %s", len(missing_in_live), missing_in_live[:3])
        if missing_locally:
            logging.warning("[JMA] %d live regions not tracked locally (ignored). Examples: %s", len(missing_locally), missing_locally[:3])
    else:
        # No local constraints → use full live map (if any). If both are empty, we’ll still parse, but won’t filter.
        region_map = live_map

    # Build reverse map for display names
    code_to_name = _invert_map(region_map)

    # 5) Decide which office files to fetch
    office_codes = conf.get("office_codes") or []
    if not office_codes:
        # Fallback: derive offices from the codes we care about
        office_codes = sorted({c[:6] + "000" for c in region_map.values() if len(c) >= 6})

    # 6) Fetch each office’s warning JSON and filter to your regions
    entries: List[dict] = []
    total_considered = 0
    total_kept = 0

    for office in office_codes:
        url = JMA_OFFICE_AREA_JSON.format(office=office)
        try:
            res = await client.get(url, timeout=20)
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            logging.warning("[JMA FETCH ERROR] office %s: %s", office, e)
            continue

        report_dt = data.get("reportDatetime") or data.get("reportDatetimeText") or ""

        # Prefer the first areaTypes block that contains class10 'areas'
        area_types = data.get("areaTypes") or []
        if not area_types:
            continue

        # Count present class10 areas for debug
        present = area_types[0].get("areas") or []
        present_codes = {str(a.get("code", "")).strip() for a in present}
        logging.warning("[JMA DEBUG] office %s: present area rows=%d, unique area codes=%d",
                        office, len(present), len(present_codes))

        # Region filter = only your reconciled codes
        allowed_codes = set(code_to_name.keys())
        intersects = sorted(present_codes & allowed_codes)
        logging.warning("[JMA DEBUG] office %s: intersects allowed=%d; examples=%s",
                        office, len(intersects), intersects[:3])

        added_before = len(entries)
        for area_obj in present:
            code = str(area_obj.get("code", "")).strip()
            if allowed_codes and code not in allowed_codes:
                continue

            active = _collect_active_area_warnings(area_obj)
            if not active:
                continue

            # Emit one entry per active hazard for this area code
            region_name = code_to_name.get(code, code)
            for hazard_code, status in active:
                title = f"Warning – {_hazard_title(hazard_code, weather_map)}"
                entries.append({
                    "title": title,
                    "region": region_name,    # used by your generic renderer
                    "summary": "",            # could add headlineText if you want
                    "link": JMA_LINK_FOR_AREA.format(code=code),
                    "published": _fmt_utc(report_dt),
                })

        logging.warning("[JMA DEBUG] office %s: added so far=%d", office, len(entries) - added_before)
        total_considered += len(present)
        total_kept += len(entries) - added_before

    logging.warning("[JMA DEBUG] FINAL parsed entries=%d (kept %d of %d areas scanned)", len(entries), total_kept, total_considered)

    return {
        "entries": entries,
        "source": {
            "area_json": JMA_AREA_JSON,
            "offices": office_codes,
        }
    }
