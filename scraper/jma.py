import httpx
import datetime
from dateutil import parser as dateparser

# JMA JSON feed (English names are included in this feed).
JMA_MAP_URL = "https://www.jma.go.jp/bosai/warning/data/warning/map.json"

# Keep only these levels (as shown on the English site)
# Advisory is shown on UI but the user wants Warning/Alert/Emergency only.
ACCEPT_LEVELS = {"Warning", "Alert", "Emergency"}

# Normalize Japanese status, just in case the feed uses JP in some branches.
JP_TO_EN_LEVEL = {
    "注意報": "Advisory",
    "警報": "Warning",
    "特別警報": "Alert",  # JMA shows this as "Alert" on the English UI
    "緊急警報": "Emergency",  # rarely used; included for completeness
}

# Phenomenon normalization: map a few known alternates to the exact UI labels.
# (We only show the nine the user listed)
PHENOMENON_NORMALIZE = {
    "Heavy Rain (Inundation)": "Heavy Rain (Inundation)",
    "Heavy Rain (Landslide)": "Heavy Rain (Landslide)",
    "Flood": "Flood",
    "Storm": "Storm",
    "Gale": "Gale",
    "High Wave": "High Wave",
    "Storm Surge": "Storm Surge",
    "Thunder Storm": "Thunder Storm",
    "Thunderstorm": "Thunder Storm",  # normalize alternate spacing
    "Dense Fog": "Dense Fog",
}

def _norm_level(level: str) -> str:
    if not level:
        return ""
    level = level.strip()
    if level in ACCEPT_LEVELS or level == "Advisory":
        return level
    return JP_TO_EN_LEVEL.get(level, level)

def _norm_phenomenon(name: str) -> str:
    if not name:
        return ""
    name = name.strip()
    return PHENOMENON_NORMALIZE.get(name, name)

def _office_link(area_code: str) -> str:
    # Link that opens the office/region panel on the English UI
    return f"https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={area_code}"

async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Parse JMA warning JSON (map.json) and emit entries for Warning/Alert/Emergency.
    Each entry is per (region, phenomenon, level), with a 'from' timestamp if present.
    """
    url = conf.get("url", JMA_MAP_URL)
    try:
        resp = await client.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as ex:
        # Mirror your other scrapers' failure pattern
        return {"entries": [], "error": str(ex), "source": url}

    entries = []
    now_iso = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    # The structure in map.json is nested. We’ll be defensive and walk it:
    # Top-level likely has an "offices" (or similar) dict keyed by office code.
    # Each office has a name and a list of phenomena with status and times.
    # Because JMA occasionally tweaks field names, we probe keys safely.

    # Try common containers that have office-level records
    candidates = []
    if isinstance(data, dict):
        for key in ("offices", "areas", "features", "centers", "list", "items", "data"):
            val = data.get(key)
            if isinstance(val, dict):
                candidates.append(val)
            elif isinstance(val, list):
                # Convert list of office-like objects into dict-like iteration
                candidates.append({str(i): v for i, v in enumerate(val)})

    # If nothing matched, maybe the top-level itself is office-like
    if not candidates:
        if isinstance(data, dict):
            candidates = [data]
        else:
            candidates = []

    def iter_offices(container):
        # Yield tuples: (area_code, office_obj)
        if isinstance(container, dict):
            for k, v in container.items():
                if isinstance(v, dict):
                    yield k, v

    seen = set()  # de-dup (area_code, region, phenomenon, level, from_time)

    for container in candidates:
        for area_code, office in iter_offices(container):
            # Office/region name
            region = (
                office.get("name_en")
                or office.get("nameEn")
                or office.get("name")
                or office.get("enName")
                or ""
            )
            # Some feeds have "officeName", etc.
            if not region and isinstance(office.get("officeName"), str):
                region = office["officeName"]

            # Find phenomena lists. Common keys to probe:
            phen_lists = []
            for k in ("types", "phenomena", "warnings", "items", "list"):
                v = office.get(k)
                if isinstance(v, list) and v:
                    phen_lists.append(v)

            if not phen_lists:
                # Some structures nest under "details" or "statusList"
                for k in ("details", "statusList", "entries"):
                    v = office.get(k)
                    if isinstance(v, list) and v:
                        phen_lists.append(v)

            if not phen_lists:
                continue

            # Flatten one level
            phen_items = [item for sub in phen_lists for item in (sub or []) if isinstance(sub, list)]

            for item in phen_items:
                if not isinstance(item, dict):
                    continue

                # Phenomenon English label
                phenomenon = (
                    item.get("phenomenon_en")
                    or item.get("phenomenonEn")
                    or item.get("name_en")
                    or item.get("nameEn")
                    or item.get("phenomenon")
                    or item.get("name")
                    or ""
                )
                phenomenon = _norm_phenomenon(phenomenon)

                # Status / level
                level = (
                    item.get("status_en")
                    or item.get("statusEn")
                    or item.get("status")
                    or ""
                )
                level = _norm_level(level)

                if level not in ACCEPT_LEVELS:
                    continue
                if phenomenon not in PHENOMENON_NORMALIZE.values():
                    # Keep only the 9 the UI shows
                    continue

                # Start time (if provided)
                start_raw = (
                    item.get("from")
                    or item.get("startTime")
                    or item.get("issued")
                    or item.get("reportDatetime")
                    or ""
                )
                start_iso = ""
                if start_raw:
                    try:
                        start_iso = dateparser.parse(start_raw).replace(microsecond=0).isoformat() + "Z"
                    except Exception:
                        start_iso = ""

                key = (area_code, region, phenomenon, level, start_iso)
                if key in seen:
                    continue
                seen.add(key)

                entries.append({
                    "title": f"{level} – {phenomenon}",
                    "region": region or area_code,
                    "province": "",  # unused in your renderer, but keep shape consistent
                    "level": level,  # Alert / Warning / Emergency
                    "type": phenomenon,  # e.g., "Heavy Rain (Landslide)"
                    "summary": f"From {start_iso or now_iso}",
                    "published": start_iso or now_iso,
                    "link": _office_link(area_code),
                })

    return {"entries": entries, "source": url}
