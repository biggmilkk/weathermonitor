# scraper/jma.py
import asyncio
import datetime as dt
import logging
from typing import Dict, List, Any, Tuple

import httpx

JMA_BASE = "https://www.jma.go.jp/bosai/warning/data/warning"

# JP → EN level mapping, matching JMA English UI wording
JP_TO_EN_LEVEL = {
    "注意報": "Advisory",
    "警報": "Warning",
    "特別警報": "Alert",      # JMA English UI shows this as "Alert"
    "緊急警報": "Emergency",   # very rare
}

# Normalize phenomena to the nine English labels visible on the front page.
# We map by Japanese keywords we see in JSON (defensive: multiple keys → one label).
PHENOMENON_MAP = {
    # Heavy rain
    "大雨（土砂災害）": "Heavy Rain (Landslide)",
    "大雨（浸水害）": "Heavy Rain (Inundation)",
    "大雨": "Heavy Rain (Inundation)",  # fallback if subtype missing
    # Flood / River flood
    "洪水": "Flood",
    # Wind
    "暴風": "Storm",
    "強風": "Gale",
    # Waves / Storm surge
    "波浪": "High Wave",
    "高潮": "Storm Surge",
    # Thunder
    "雷": "Thunder Storm",
    # Fog
    "濃霧": "Dense Fog",
}

KEEP_LEVELS = {"Warning", "Alert", "Emergency"}

def _iso_utc(dt_obj: dt.datetime) -> str:
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
    return dt_obj.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")

def _fmt_from_utc(dt_obj: dt.datetime) -> str:
    # Example: "From 00:55 UTC August 8"
    dt_utc = dt_obj.astimezone(dt.timezone.utc)
    return dt_utc.strftime("From %H:%M UTC %B %-d")

def _best_start_time(area_warn: Dict[str, Any], office_updated: dt.datetime) -> dt.datetime:
    """
    Try to extract the earliest 'valid from' time from a timeSeries block.
    Fall back to office update/report time.
    """
    # Many JMA files have timeSeries[...]["timeDefines"] as list of ISO strings
    ts = area_warn.get("timeSeries")
    if isinstance(ts, list):
        for block in ts:
            if isinstance(block, dict) and "timeDefines" in block:
                tds = block.get("timeDefines") or []
                if tds:
                    try:
                        # pick the first timeDefine as start
                        start = dt.datetime.fromisoformat(tds[0].replace("Z", "+00:00"))
                        return start
                    except Exception:
                        pass
    return office_updated

def _normalize_phenomenon(jp: str) -> str:
    # exact match first
    if jp in PHENOMENON_MAP:
        return PHENOMENON_MAP[jp]
    # attempt partial/fallback matches by contained key
    for key, label in PHENOMENON_MAP.items():
        if key in (jp or ""):
            return label
    # If unknown, just return the JP string (last resort so user sees *something*)
    return jp or "Unknown"

async def _fetch_json(client: httpx.AsyncClient, url: str) -> Any:
    r = await client.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

async def _fetch_office(client: httpx.AsyncClient, code: str) -> Tuple[str, Dict[str, Any]]:
    url = f"{JMA_BASE}/{code}.json"
    try:
        data = await _fetch_json(client, url)
        return code, data
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] office {code}: {e}")
        return code, {}

async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Scrape JMA warnings purely from the JSON endpoints.

    Output entries shaped like:
      {
        "title": "Warning - Heavy Rain (Landslide) – Hokkaido: Soya Region",
        "region": "Hokkaido: Soya Region",
        "level": "Warning",
        "type": "Heavy Rain (Landslide)",
        "summary": "From 00:55 UTC August 8",
        "published": "2025-08-08T00:55:00Z",
        "link": "https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code=011000",
      }
    """
    entries: List[Dict[str, Any]] = []

    # 1) Load map.json to discover all "office" area codes
    map_url = f"{JMA_BASE}/map.json"
    try:
        jmap = await _fetch_json(client, map_url)
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] map.json: {e}")
        return {"entries": [], "error": str(e), "source": map_url}

    # map.json structure varies slightly, but typically has "offices" array with codes
    office_list = []
    for k in ("offices", "areaTypes", "areas"):
        # be tolerant to structure, but standard is jmap["offices"] = [{code, name, ...}]
        if isinstance(jmap, dict) and "offices" in jmap and isinstance(jmap["offices"], list):
            office_list = [o.get("code") for o in jmap["offices"] if isinstance(o, dict)]
            break
    if not office_list:
        # Fallback: try to collect anything that *looks like* office codes
        office_list = [a.get("code") for a in (jmap.get("areas") or []) if isinstance(a, dict)]

    office_list = [c for c in office_list if isinstance(c, str) and c.endswith("000")]
    office_list = list(dict.fromkeys(office_list))  # dedupe, keep order

    if not office_list:
        logging.warning("[JMA] No office codes found in map.json")
        return {"entries": [], "source": map_url}

    # 2) Fetch each office JSON concurrently
    tasks = [_fetch_office(client, code) for code in office_list]
    results = await asyncio.gather(*tasks)

    for office_code, office_json in results:
        if not office_json:
            continue

        # Try to get a "report/update" time for this office (fallback for "From …")
        # Common fields seen: "reportDatetime", "reportTime", "publishingOffice" etc.
        office_updated = None
        for key in ("reportDatetime", "reportTime", "targetDateTime", "updateTime", "publishingDatetime"):
            val = office_json.get(key)
            if isinstance(val, str):
                try:
                    office_updated = dt.datetime.fromisoformat(val.replace("Z", "+00:00"))
                    break
                except Exception:
                    pass
        if office_updated is None:
            office_updated = dt.datetime.now(dt.timezone.utc)

        # Build the pretty region prefix like "Hokkaido: Soya Region"
        # We usually have an "office name" (prefecture-level) + per-area names under it.
        office_name = office_json.get("officeNameEn") or office_json.get("officeName") or ""
        # some JSONs have only JP officeName; that's fine

        # The data tree tends to have one or more "timeSeries" blocks with areas[] each carrying warnings[]
        time_series = office_json.get("timeSeries")
        if not isinstance(time_series, list):
            # fallback: some files use "areaTypes" → blocks → "areas" with warnings inline
            time_series = [office_json]  # try to treat the root as a single block

        for block in time_series:
            areas = []
            if isinstance(block, dict):
                areas = block.get("areas") or block.get("areaTypes") or []
            if not isinstance(areas, list):
                continue

            # normalize areas list to iterable of dicts with name/code and warnings
            normalized_areas = []
            for a in areas:
                if not isinstance(a, dict):
                    continue
                # try typical keys
                area_name = a.get("nameEn") or a.get("name") or a.get("areaName") or ""
                area_code = a.get("code") or a.get("areaCode") or ""
                warnings = a.get("warnings") or a.get("warningCodes") or a.get("warning") or []
                normalized_areas.append({
                    "name": area_name,
                    "code": area_code,
                    "warnings": warnings,
                    "raw": a,
                })

            for a in normalized_areas:
                reg_label = a["name"] or ""
                # Compose "Prefecture: Region" if office_name looks like a prefecture name
                region_display = f"{office_name}: {reg_label}" if office_name and reg_label else (reg_label or office_name)

                # "warnings" can be a list of dicts, each with something like:
                #   {"status": "警報", "event": "大雨", "kind": "土砂災害"} OR merged "event" string
                warns = a.get("warnings") or []
                if not isinstance(warns, list):
                    continue

                for w in warns:
                    if not isinstance(w, dict):
                        continue

                    # Resolve level
                    level_jp = w.get("status") or w.get("level") or ""
                    level_en = JP_TO_EN_LEVEL.get(level_jp, level_jp)
                    if level_en not in KEEP_LEVELS:
                        continue

                    # Resolve phenomenon
                    # Sometimes "event" is just "大雨", and subtype is under "kind" (e.g., "土砂災害")
                    event_jp = (w.get("event") or "") + (w.get("kind") or "")
                    event_jp = event_jp.strip() or w.get("name") or ""
                    phenomenon_en = _normalize_phenomenon(event_jp)

                    # Pick a start time
                    start_time = _best_start_time(w, office_updated)
                    published_iso = _iso_utc(start_time)
                    from_str = _fmt_from_utc(start_time)

                    # Build link to the office
                    link = f"https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={office_code}"

                    # Compose title
                    title = f"{level_en} - {phenomenon_en} – {region_display}" if region_display else f"{level_en} - {phenomenon_en}"

                    entries.append({
                        "title": title,
                        "region": region_display or "Unknown",
                        "level": level_en,
                        "type": phenomenon_en,
                        "summary": from_str,
                        "published": published_iso,
                        "link": link,
                    })

    # Sort newest first (by published time string)
    entries.sort(key=lambda e: e.get("published", ""), reverse=True)

    logging.warning(f"[JMA DEBUG] Parsed {len(entries)} alerts (Warning/Alert/Emergency only)")
    return {"entries": entries, "source": f"{JMA_BASE}/map.json"}
