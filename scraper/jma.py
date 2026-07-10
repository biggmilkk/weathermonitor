import asyncio
import json
import logging
from typing import Dict, List, Optional, Set, Tuple

import httpx

JMA_AREA_JSON = "https://www.jma.go.jp/bosai/common/const/area.json"
JMA_WARNING_BASE = "https://www.jma.go.jp/bosai/warning/data/r8"

# English messages
HEAVY_RAIN = "Heavy Rain"
HEAVY_RAIN_INUNDATION = "Heavy Rain (Inundation)"
HEAVY_RAIN_LANDSLIDE = "Heavy Rain (Landslide)"
HEAVY_SNOW = "Heavy Snow"
FLOOD = "Flood"
STORM_GALE = "Storm/Gale"
SNOWSTORM = "Snowstorm/Blizzard"
HIGH_WAVES = "High Waves"
STORM_SURGE = "Storm Surge"
LANDSLIDE = "Landslide"

# Only warning-or-higher codes. Advisories are intentionally excluded.
# 43/48/49 cover danger-warning style codes introduced around the Reiwa 8 transition.
# Advisory codes such as 10, 19, 29, 12, 13, 15, and 16 are intentionally excluded.
CODE_TO_MESSAGE = {
    # Reiwa 8 warning-or-higher codes. Advisories are intentionally excluded.
    # Heavy rain
    "33": "Level 5 Heavy Rain Special Warning Emergency",
    "43": "Level 4 Heavy Rain Danger Warning",
    "03": "Level 3 Heavy Rain Warning",

    # Landslide disaster
    "39": "Level 5 Landslide Disaster Special Warning Emergency",
    "49": "Level 4 Landslide Disaster Danger Warning",
    "09": "Level 3 Landslide Disaster Warning",

    # Storm surge
    "38": "Level 5 Storm Surge Special Warning Emergency",
    "48": "Level 4 Storm Surge Danger Warning",
    "08": "Level 3 Storm Surge Warning",

    # Other warning/special-warning categories kept in the r8 system
    "32": f"{SNOWSTORM} Special Warning Emergency",
    "02": f"{SNOWSTORM} Warning",
    "35": "Storm Special Warning Emergency",
    "05": "Storm Warning",
    "36": f"{HEAVY_SNOW} Special Warning Emergency",
    "06": f"{HEAVY_SNOW} Warning",
    "37": f"{HIGH_WAVES} Special Warning Emergency",
    "07": f"{HIGH_WAVES} Warning",

    # Legacy fallback only. Flood warnings are no longer handled as the same
    # independent warning/advisory item in the r8 weather-warning JSON.
    "04": f"{FLOOD} Warning",
}
INCLUDE_CODES = set(CODE_TO_MESSAGE)
ACTIVE_STATUSES = {"発表", "継続"}
INACTIVE_STATUSES = {
    "",
    "解除",
    "発表警報・注意報はなし",
    "なし",
    "発表なし",
    "警報から注意報",
}


def _load_region_map_from_file(path: str) -> Dict[str, str]:
    with open(path, "r", encoding="utf-8") as f:
        return {str(k): str(v) for k, v in json.load(f).items()}


async def _fetch_area_json(client: httpx.AsyncClient) -> Optional[dict]:
    try:
        r = await client.get(JMA_AREA_JSON, timeout=20)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, dict) else None
    except Exception as e:
        logging.warning(f"[JMA VALIDATION] Could not fetch area.json: {e}")
        return None


def _valid_area_codes(area_json: dict) -> Set[str]:
    """Accept office/class10/class15/class20 codes while validating the region map."""
    valid: Set[str] = set()
    for key in ("offices", "class10s", "class15s", "class20s"):
        valid.update(str(code) for code in (area_json.get(key) or {}).keys())
    return valid


def _validate_region_map(region_map: Dict[str, str], area_json: Optional[dict]) -> Dict[str, str]:
    if not area_json:
        return region_map

    valid = _valid_area_codes(area_json)
    out: Dict[str, str] = {}
    for name, code in region_map.items():
        code = str(code)
        if code in valid:
            out[name] = code
        else:
            logging.warning(
                f"[JMA VALIDATION] Dropping '{name}' "
                f"(unknown office/class10/class15/class20 code {code}) per area.json"
            )
    return out


def _office_json_url(office_code: str) -> str:
    return f"{JMA_WARNING_BASE}/{office_code}.json"


def _office_frontend_url(office_code: str) -> str:
    return f"https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={office_code}"


def _build_code_to_name(region_map: Dict[str, str]) -> Dict[str, str]:
    return {str(code): name for name, code in region_map.items()}


def _parent_code(area_json: Optional[dict], code: str) -> Optional[str]:
    if not area_json:
        return None

    code = str(code or "")
    for table in ("class20s", "class15s", "class10s"):
        node = (area_json.get(table) or {}).get(code)
        if isinstance(node, dict):
            parent = node.get("parent")
            return str(parent) if parent else None
    return None


def _resolve_region_name(
    area_code: str,
    allowed_code_to_name: Dict[str, str],
    area_json: Optional[dict],
) -> Optional[str]:
    """
    Match the emitted warning areaCode to region_area_codes.json.
    Exact class10 match is expected, but this also walks child -> parent for safety.
    """
    cur = str(area_code or "")
    seen: Set[str] = set()

    while cur and cur not in seen:
        if cur in allowed_code_to_name:
            return allowed_code_to_name[cur]
        seen.add(cur)
        cur = _parent_code(area_json, cur) or ""

    return None


def _office_for_code(code: str, area_json: Optional[dict]) -> Optional[str]:
    """Return the /r8 office code for an office/class10/class15/class20 code."""
    code = str(code or "")
    if not code:
        return None

    if not area_json:
        return code

    offices = area_json.get("offices") or {}
    if code in offices:
        return code

    cur = code
    seen: Set[str] = set()
    while cur and cur not in seen:
        if cur in offices:
            return cur
        seen.add(cur)
        cur = _parent_code(area_json, cur) or ""

    return None


def _dedupe_preserve_order(codes: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for code in codes:
        code = str(code or "")
        if code and code not in seen:
            seen.add(code)
            out.append(code)
    return out


def _derive_office_codes(
    region_map: Dict[str, str],
    area_json: Optional[dict],
    configured_codes: Optional[List[str]] = None,
) -> List[str]:
    """
    /r8 files are by office code. region_area_codes.json is mostly class10 codes.
    Derive the parent office code automatically so config cannot go stale.
    """
    raw_codes = [str(c) for c in (configured_codes or []) if str(c).strip()]
    if not raw_codes:
        raw_codes = [str(code) for code in region_map.values()]

    offices: List[str] = []
    for code in raw_codes:
        office = _office_for_code(code, area_json)
        if office:
            offices.append(office)
        else:
            logging.warning(f"[JMA VALIDATION] Could not resolve office for code {code}")

    return _dedupe_preserve_order(offices)


def _parse_heavy_rain_conditions(cond_text: Optional[str]) -> List[str]:
    out: List[str] = []
    text = str(cond_text or "")
    if not text:
        return out
    if "浸水" in text:
        out.append(HEAVY_RAIN_INUNDATION)
    if "土砂" in text:
        out.append(HEAVY_RAIN_LANDSLIDE)
    return out


def _kind_code(kind: dict) -> str:
    code = kind.get("code")
    if isinstance(code, dict):
        code = code.get("code")
    if not code and isinstance(kind.get("kind"), dict):
        code = kind["kind"].get("code")
    return str(code or "")


def _kind_status(kind: dict) -> str:
    status = kind.get("status")
    if isinstance(status, dict):
        status = status.get("status") or status.get("name")
    return str(status or "")


def _kind_condition(kind: dict) -> str:
    condition = kind.get("condition")
    if isinstance(condition, dict):
        condition = condition.get("name") or condition.get("text")
    return str(condition or "")


def _is_active_status(status: str) -> bool:
    s = str(status or "").strip()
    if s in INACTIVE_STATUSES:
        return False
    if s in ACTIVE_STATUSES:
        return True

    # Unknown status strings are treated as active only for known warning codes,
    # to avoid missing warnings if JMA adds a new active status label.
    logging.debug(f"[JMA] Treating unknown warning status as active: {s}")
    return True


def _messages_for_kind(kind: dict) -> List[str]:
    code = _kind_code(kind)
    status = _kind_status(kind)

    if code not in INCLUDE_CODES:
        return []
    if not _is_active_status(status):
        return []

    msg = CODE_TO_MESSAGE.get(code)
    return [msg] if msg else []


def _area_code_from_item(item: dict) -> str:
    code = item.get("areaCode") or item.get("code")
    if code:
        return str(code)

    area = item.get("area")
    if isinstance(area, dict):
        return str(area.get("code") or "")

    return ""


def _title_for_message(msg: str) -> str:
    if "Emergency" in msg:
        base = msg.replace(" Emergency", "")
        return f"Emergency – {base}"
    return f"Warning – {msg}"


def _make_entry(msg: str, region_name: str, frontend_url: str, report_dt: str) -> dict:
    return {
        "title": _title_for_message(msg),
        "region": region_name,
        "summary": "",
        "link": frontend_url,
        "published": report_dt,
    }


def _parse_r8_warning_schema(
    data: list,
    *,
    frontend_url: str,
    allowed_code_to_name: Dict[str, str],
    area_json: Optional[dict],
) -> List[dict]:
    entries: List[dict] = []
    seen: Set[Tuple[str, str, str]] = set()

    for record in data:
        if not isinstance(record, dict):
            continue

        report_dt = (
            record.get("reportDatetime")
            or record.get("reportDateTime")
            or record.get("targetDatetime")
            or ""
        )
        warning_block = record.get("warning") or {}
        if not isinstance(warning_block, dict):
            continue

        # Prefer class10Items because region_area_codes.json is class10-level.
        # Also read class15/class20 defensively and resolve upward, then dedupe.
        for item_key in ("class10Items", "class15Items", "class20Items"):
            for item in warning_block.get(item_key, []) or []:
                if not isinstance(item, dict):
                    continue

                area_code = _area_code_from_item(item)
                region_name = _resolve_region_name(area_code, allowed_code_to_name, area_json)
                if not region_name:
                    continue

                for kind in item.get("kinds", []) or []:
                    if not isinstance(kind, dict):
                        continue
                    for msg in _messages_for_kind(kind):
                        sig = (region_name, msg, report_dt)
                        if sig in seen:
                            continue
                        seen.add(sig)
                        entries.append(_make_entry(msg, region_name, frontend_url, report_dt))

    return entries


def _parse_old_warning_schema(
    data: dict,
    *,
    frontend_url: str,
    allowed_code_to_name: Dict[str, str],
    area_json: Optional[dict],
) -> List[dict]:
    report_dt = data.get("reportDatetime") or data.get("reportDateTime") or ""
    entries: List[dict] = []
    seen: Set[Tuple[str, str, str]] = set()

    for area_type in data.get("areaTypes", []) or []:
        for area in area_type.get("areas", []) or []:
            area_code = str(area.get("code", ""))
            region_name = _resolve_region_name(area_code, allowed_code_to_name, area_json)
            if not region_name:
                continue

            for warning in area.get("warnings", []) or []:
                if not isinstance(warning, dict):
                    continue
                for msg in _messages_for_kind(warning):
                    sig = (region_name, msg, report_dt)
                    if sig in seen:
                        continue
                    seen.add(sig)
                    entries.append(_make_entry(msg, region_name, frontend_url, report_dt))

    return entries


async def _fetch_office_json(
    client: httpx.AsyncClient,
    office: str,
    allowed_code_to_name: Dict[str, str],
    area_json: Optional[dict],
) -> List[dict]:
    """Fetch and parse a single office JSON; return normalized entries."""
    url = _office_json_url(office)
    try:
        r = await client.get(url, timeout=25)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {office}: {e}")
        return []

    frontend_url = _office_frontend_url(office)

    try:
        if isinstance(data, list):
            return _parse_r8_warning_schema(
                data,
                frontend_url=frontend_url,
                allowed_code_to_name=allowed_code_to_name,
                area_json=area_json,
            )
        if isinstance(data, dict):
            return _parse_old_warning_schema(
                data,
                frontend_url=frontend_url,
                allowed_code_to_name=allowed_code_to_name,
                area_json=area_json,
            )

        logging.warning(f"[JMA PARSE] {office}: unknown JSON root type {type(data).__name__}")
        return []
    except Exception as e:
        logging.warning(f"[JMA PARSE ERROR] {office}: {e}")
        return []


async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Fetch all JMA /r8 office JSONs concurrently and return normalized entries.
    Output shape intentionally matches the existing renderer:
    title, region, summary, link, published.
    """
    try:
        region_map = _load_region_map_from_file(conf["region_map_file"])
    except Exception as e:
        logging.warning(f"[JMA] Failed to load region_map_file: {e}")
        return {"entries": [], "error": str(e), "source": conf}

    area_json = await _fetch_area_json(client)
    region_map = _validate_region_map(region_map, area_json)
    allowed_code_to_name = _build_code_to_name(region_map)

    office_codes = _derive_office_codes(
        region_map,
        area_json,
        configured_codes=conf.get("office_codes"),
    )
    if not office_codes:
        logging.warning("[JMA] No office codes resolved; returning empty result")
        return {"entries": [], "source": "JMA (/r8 office JSONs)"}

    tasks = [
        _fetch_office_json(client, office, allowed_code_to_name, area_json)
        for office in office_codes
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    entries: List[dict] = []
    for office, result in zip(office_codes, results):
        if isinstance(result, Exception):
            logging.warning(f"[JMA TASK ERROR] {office}: {result}")
            continue
        entries.extend(result)

    entries.sort(key=lambda x: x.get("published", ""), reverse=True)

    logging.warning(
        f"[JMA DEBUG] Parsed {len(entries)} active warnings from {len(office_codes)} offices"
    )
    return {"entries": entries, "source": "JMA (/r8 office JSONs)"}
