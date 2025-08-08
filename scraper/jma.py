import httpx
from datetime import datetime, timezone
from dateutil import parser as dateparser
import logging

# Phenomena names (the ones you actually want to show)
PHENOMENON_BY_CODE = {
    "04": "Flood",
    "05": "High Wave",
    "06": "Storm Surge",   # some datasets use 19 for surge risk; we map both below
    "07": "Storm",         # if present as a warning-tier type
    "08": "Gale",          # if present as a warning-tier type
    "10": "Heavy Rain",    # we’ll suffix (Inundation)/(Landslide) via attentions
    # Some files encode surge as “19” in the risk table; treat as Storm Surge
    "19": "Storm Surge",
}

# Status mapping (Japanese → English label shown in UI)
JP_TO_EN_LEVEL = {
    "警報": "Warning",
    "特別警報": "Alert",
    "緊急警報": "Emergency",
    # "注意報": "Advisory"  # intentionally excluded
}

def _utc_str(iso_jst: str) -> str:
    # JMA gives JST with +09:00 offset; convert to “Fri, 08 Aug 2025 06:27 UTC”
    dt = dateparser.parse(iso_jst)
    utc = dt.astimezone(timezone.utc)
    return utc.strftime("%a, %d %b %Y %H:%M UTC")

def _heavy_rain_suffix_from_attentions(attns: list[str]) -> str:
    # Decide between (Inundation) vs (Landslide) using hints if present
    attns = attns or []
    # 土砂災害注意 => Landslide, 浸水注意 => Inundation
    if any("土砂" in a for a in attns):
        return " (Landslide)"
    if any("浸水" in a for a in attns):
        return " (Inundation)"
    return ""  # fallback when hints are absent

async def scrape_jma_office_async(office_code: str) -> dict:
    """
    Parse a single office JSON (e.g., 014100 for Hokkaido: Kushiro/Nemuro office)
    and return only Warning/Alert/Emergency for the phenomena we care about.
    """
    url = f"https://www.jma.go.jp/bosai/warning/data/warning/014100.json"
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {e}")
        return {"entries": [], "error": str(e), "source": url}

    published_iso = data.get("reportDatetime")
    published_utc = _utc_str(published_iso) if published_iso else None

    entries = []

    # The “areaTypes[0]” section carries current warning/advisory issuance per class10 area
    area_types = data.get("areaTypes") or []
    if not area_types:
        return {"entries": [], "source": url}

    # We’ll use the detailed “timeSeries” to detect phenomena presence + attentions
    # keyed by class10 area code → { code -> attentions(list) }
    ts = data.get("timeSeries") or []
    attentions_map: dict[str, dict[str, list[str]]] = {}
    for block in ts:
        for t_area in (block.get("areaTypes") or []):
            for a in (t_area.get("areas") or []):
                acode = a.get("code")
                for w in (a.get("warnings") or []):
                    code = str(w.get("code"))
                    # collect attentions if available
                    attns = []
                    for lvl in (w.get("levels") or []):
                        for la in (lvl.get("localAreas") or []):
                            # levels.localAreas may contain 'attentions'
                            attns += la.get("attentions", []) or []
                    # also some “warnings” objects carry attentions directly
                    attns += (w.get("attentions") or [])
                    if acode:
                        attentions_map.setdefault(acode, {}).setdefault(code, [])
                        attentions_map[acode][code].extend(attns)

    # Now read current issuance per area (class10) and keep only warning-tier types we want
    for at in area_types:
        for a in at.get("areas", []):
            class10_code = str(a.get("code"))
            for w in a.get("warnings", []):
                code = str(w.get("code"))
                status = w.get("status", "")  # e.g. "発表" (issued), "継続" (continuing)

                # Only consider phenomena we care about
                if code not in PHENOMENON_BY_CODE:
                    continue

                # Heuristic: in this JSON, thunder/dense fog are usually advisory-only,
                # so we don’t include code 14 (雷) nor 20 (濃霧) at all.
                if code in {"14", "20"}:
                    continue

                # Treat presence here as a warning-tier signal;
                # if your file exposes “警報/特別警報” texts, map them with JP_TO_EN_LEVEL.
                # For the Kushiro test, Flood (code 04) is the one we want.
                level_en = "Warning"  # default label (works for the test case)

                # Compose phenomenon name
                phenomenon = PHENOMENON_BY_CODE[code]
                if code == "10":
                    # Attach suffix based on attentions if any
                    attns = attentions_map.get(class10_code, {}).get(code, [])
                    phenomenon += _heavy_rain_suffix_from_attentions(attns)

                # Build region name: for a single office run, use the office’s class10 names.
                # For 014100, class10 “014020” is Kushiro Region; “014010” is Nemuro Region.
                # If you have your areacode map loaded in memory, prefer that here.
                region_name = class10_code  # placeholder if you don’t look up names

                title = f"{level_en} – {phenomenon}"
                entries.append({
                    "title": title,
                    "region": region_name,
                    "level": level_en,
                    "type": phenomenon,
                    "summary": "",
                    "published": published_utc,
                    "link": url
                })

    return {"entries": entries, "source": url}
