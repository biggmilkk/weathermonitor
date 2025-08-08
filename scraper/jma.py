import datetime
import logging
import httpx

JMA_MAP_URL = "https://www.jma.go.jp/bosai/warning/data/warning/map.json"
OFFICE_URL_PREFIX = "https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code="

# JP -> EN level
JP_TO_EN_LEVEL = {
    "特別警報": "Alert",
    "警報": "Warning",
    # we will explicitly EXCLUDE 注意報 (Advisory)
}

# Phenomenon codes seen in map.json → English label (front-page terms)
PHENOMENON = {
    "03": "Heavy Rain (Landslide)",   # 土砂災害
    "04": "Flood",                    # 洪水
    "10": "Heavy Rain (Inundation)",  # 大雨(浸水) — often Advisory; we’ll filter by level below
    "11": "Gale",                     # 風
    "13": "Storm",                    # 暴風
    "14": "High Wave",                # 波浪
    "15": "Storm Surge",              # 高潮
    "18": "Thunderstorm",             # 雷
    "20": "Dense Fog",                # 濃霧
}

def _infer_level(status: str, attentions: list | None) -> str | None:
    """Return 'Alert' or 'Warning' when clearly indicated. Otherwise None (treat as Advisory)."""
    status = status or ""
    attns = attentions or []

    # Explicit in status
    if "特別警報" in status:
        return "Alert"
    if "警報" in status and "注意報" not in status:
        # includes transitions like "警報から注意報" (downgrade) → that *contains* 注意報,
        # which we should treat as not Warning anymore.
        return "Warning"

    # Heuristic via attentions:
    # Any '...警戒' (e.g., 土砂災害警戒 / 浸水警戒) is presented on JMA as warning-level.
    if any("警戒" in a for a in attns):
        return "Warning"

    # Otherwise treat as Advisory (we drop)
    return None

async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Parse JMA map.json, keep only Warning / Alert (and any 'Emergency' if ever present),
    and emit office-level human labels like 'Hokkaido: Soya Region'.
    """
    url = conf.get("url", JMA_MAP_URL)

    # Optional area code → name mapping file if you have it (won’t break if missing)
    area_names = {}
    for key in ("area_code_file", "areacode_file", "areacode_path"):
        path = conf.get(key)
        if path:
            try:
                import json, os
                with open(path, "r", encoding="utf-8") as f:
                    area_names = json.load(f)
            except Exception as e:
                logging.warning("[JMA DEBUG] failed loading areacode mapping: %s", e)
            break

    try:
        r = await client.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logging.warning("[JMA FETCH ERROR] %s", e)
        return {"entries": [], "error": str(e), "source": url}

    entries = []
    now_iso = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    # Each item => a “reportDatetime” bundle with areaTypes (offices + municipalities)
    for report in data:
        report_dt = report.get("reportDatetime")
        area_types = report.get("areaTypes", [])
        for at in area_types:
            for area in at.get("areas", []):
                code = str(area.get("code", "")).strip()
                warnings = area.get("warnings", []) or []
                if not code or not warnings:
                    continue

                # Friendly name like "Hokkaido: Soya Region" if you’ve mapped it; else show the code.
                region_name = area_names.get(code, code)

                for w in warnings:
                    pcode = str(w.get("code", "")).strip()
                    status = w.get("status", "")  # e.g., 継続 / 発表 / 警報から注意報 など
                    attentions = w.get("attentions", [])  # list of JP strings
                    condition = w.get("condition", "")    # may include 土砂災害/浸水害, etc.

                    level = _infer_level(status, attentions)
                    if level not in ("Warning", "Alert", "Emergency"):
                        # Drop Advisories
                        continue

                    # Phenomenon English
                    phen = PHENOMENON.get(pcode, pcode)
                    # If condition clarifies Landslide/Inundation, prefer that for heavy rain
                    if pcode == "10":
                        # 10 can be landslide/inundation — use condition text to pick:
                        if "土砂" in condition and "浸水" in condition:
                            phen = "Heavy Rain (Landslide/Inundation)"
                        elif "土砂" in condition:
                            phen = "Heavy Rain (Landslide)"
                        elif "浸水" in condition:
                            phen = "Heavy Rain (Inundation)"

                    title = f"{level} – {phen}"
                    entries.append({
                        "title": title,
                        "region": region_name,
                        "level": level,
                        "type": phen,
                        "summary": condition or "",
                        "published": (report_dt or now_iso).replace("+09:00", "Z"),
                        "link": OFFICE_URL_PREFIX + code
                    })

    logging.warning("[JMA DEBUG] Parsed %d warning/alert items", len(entries))
    return {"entries": entries, "source": url}
