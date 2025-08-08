import logging
import json
from typing import Dict, List, Tuple
from datetime import datetime, timezone
import httpx

MAP_URL = "https://www.jma.go.jp/bosai/warning/data/warning/map.json"
OFFICE_LINK = "https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={code}"

# Phenomena names to match the JMA English UI
PHENOMENON_MAP = {
    "04": "Flood",
    "14": "High Wave",
    "15": "Storm Surge",
    "18": "Thunderstorm",
    "20": "Dense Fog",
    # "03" is special-cased (Heavy Rain -> Landslide / Inundation)
    # "10" is Heavy Rain (Advisory in many cases) — we ignore unless escalated to Warning level (shouldn’t be needed for class10)
}

def _iso_utc(dt_jst_str: str) -> str:
    # JMA gives JST with +09:00; we’ll keep it but emit ISO string
    try:
        return datetime.fromisoformat(dt_jst_str).astimezone(timezone.utc).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def _load_areacodes(conf: dict) -> Dict:
    """
    Try to load areacode.json from:
      1) conf['area_code_file'] if provided
      2) scraper/areacode.json (your repo’s path)
    """
    paths = []
    if conf and conf.get("area_code_file"):
        paths.append(conf["area_code_file"])
    paths.append("scraper/areacode.json")
    for p in paths:
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            continue
    logging.warning("[JMA DEBUG] areacode.json not found; using codes as names.")
    return {"class10s": {}, "offices": {}}

def _is_class10(code: str, ac: Dict) -> bool:
    # class10s keys are the “office-level regions”
    return code in (ac.get("class10s") or {})

def _region_names(code: str, ac: Dict) -> Tuple[str, str]:
    """
    Return (pref_en, region_en) for class10 code.
    class10s[code]['parent'] -> office code like '020000', then offices[that]['enName'] is the prefecture name.
    """
    class10s = ac.get("class10s") or {}
    offices = ac.get("offices") or {}
    if code in class10s:
        reg_en = class10s[code].get("enName", code)
        parent_office = class10s[code].get("parent")
        pref_en = offices.get(parent_office, {}).get("enName", parent_office or "")
        return pref_en or "", reg_en
    # Fallback
    return "", code

def _level_from_jp(condition: str, attentions: List[str]) -> str:
    """
    Decide Advisory/Warning/Alert/Emergency from Japanese strings.
    - If any contains '特別警報' => Alert
    - Else if any contains '警戒'   => Warning
    - Else if any contains '注意'   => Advisory
    - Else default to Warning (conservative for class10 feed)
    """
    hay = " ".join([condition or ""] + (attentions or []))
    if "特別警報" in hay:
        return "Alert"
    if "警戒" in hay:
        return "Warning"
    if "注意" in hay:
        return "Advisory"
    return "Warning"

def _heavy_rain_subtypes(condition: str, attentions: List[str]) -> List[str]:
    """
    For code '03' heavy rain: split to Landslide / Inundation based on mentions.
    """
    hay = " ".join([condition or ""] + (attentions or []))
    subs = []
    if ("土砂" in hay) or ("土砂災害" in hay):
        subs.append("Heavy Rain (Landslide)")
    if ("浸水" in hay) or ("浸水害" in hay):
        subs.append("Heavy Rain (Inundation)")
    # If neither recognized (rare), fall back to generic Heavy Rain
    if not subs:
        subs.append("Heavy Rain")
    return subs

async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Build JMA office-level Warning/Alert/Emergency items from map.json.
    """
    areacode = _load_areacodes(conf)

    # Fetch map.json
    try:
        resp = await client.get(conf.get("url", MAP_URL), timeout=15)
        resp.raise_for_status()
        map_data = resp.json()
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {e}")
        return {"entries": [], "error": str(e), "source": conf.get("url", MAP_URL)}

    # Keep most-recent report per (class10_code, phenomenon_name)
    latest: Dict[Tuple[str, str], dict] = {}

    # map.json is a list of reports
    for report in map_data:
        report_ts = report.get("reportDatetime")
        published_iso = _iso_utc(report_ts) if report_ts else datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z")

        for at in report.get("areaTypes", []):
            for area in at.get("areas", []):
                code = str(area.get("code", ""))
                if not _is_class10(code, areacode):
                    # Ignore municipalities/class15, etc.
                    continue

                warnings = area.get("warnings", []) or []
                for w in warnings:
                    pcode = str(w.get("code", ""))
                    condition = w.get("condition") or ""
                    attentions = w.get("attentions") or []

                    # Decide level and drop advisories
                    level = _level_from_jp(condition, attentions)
                    if level == "Advisory":
                        continue

                    # Figure phenomenon(s)
                    phenos: List[str] = []
                    if pcode == "03":
                        phenos = _heavy_rain_subtypes(condition, attentions)
                    else:
                        name = PHENOMENON_MAP.get(pcode)
                        if not name:
                            # Unknown or not in our allow-list
                            continue
                        phenos = [name]

                    for ph in phenos:
                        key = (code, ph)
                        entry = latest.get(key)
                        # Always prefer the newest report time
                        if (not entry) or (published_iso > entry["published"]):
                            pref_en, region_en = _region_names(code, areacode)
                            region_str = f"{pref_en}: {region_en}" if pref_en else region_en

                            latest[key] = {
                                "title": f"{level} – {ph}",
                                "region": region_str or code,
                                "level": level,
                                "type": ph,
                                "summary": "",
                                "published": published_iso,
                                "link": OFFICE_LINK.format(code=code),
                            }

    entries = sorted(latest.values(), key=lambda x: (x["published"], x["region"], x["type"]), reverse=True)
    logging.warning(f"[JMA DEBUG] Parsed {len(entries)} office warnings/alerts")
    return {"entries": entries, "source": conf.get("url", MAP_URL)}
