# scraper/jma.py
import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

import httpx


JMA_OFFICE_CODE = "014100"  # Hokkaido: Kushiro Region (pilot)
JMA_OFFICE_URL = f"https://www.jma.go.jp/bosai/warning/data/warning/{JMA_OFFICE_CODE}.json"

# Where we try to read the friendly names for area codes
AREACODE_SEARCH_PATHS = [
    Path("scraper/areacode.json"),
    Path("./areacode.json"),
    Path("/mnt/data/areacode.json"),
]

# Very small phenomenon inference for this pilot (Kushiro flood).
# We key off common headline keywords you’ll see for flood warnings.
FLOOD_KEYWORDS = (
    "洪水",     # flood
    "増水",     # rising water / river rise
    "氾濫",     # overflow
)

def _load_areacode_map() -> Dict[str, str]:
    """
    Load a { office_code -> 'Prefecture: Region' } mapping.
    For this pilot we only need '014100' → 'Hokkaido: Kushiro Region'.
    If the file isn't found, we fall back to a tiny built-in default.
    """
    for p in AREACODE_SEARCH_PATHS:
        try:
            if p.exists():
                with p.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                # Expect either a flat dict, or nested dicts — accept both
                if isinstance(data, dict):
                    # The provided areacode.json snapshots usually have
                    # a flat map of office codes to names
                    return {str(k): str(v) for k, v in data.items()}
        except Exception as e:
            logging.warning("[JMA DEBUG] Failed reading areacode.json at %s: %s", p, e)

    logging.warning("[JMA DEBUG] areacode.json not found; using built-in map")
    return {
        "014100": "Hokkaido: Kushiro Region",
    }


def _fmt_published(iso_str: str) -> str:
    """
    JMA gives e.g. '2025-08-08T06:27:00+00:00' or '2025-08-08T06:27:00Z'.
    Format to 'Fri, 08 Aug 2025 06:27 UTC' (as requested).
    """
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%a, %d %b %Y %H:%M UTC")
    except Exception:
        # Fallback to raw if parsing somehow fails
        return iso_str


def _infer_flood_from_headline(headline: str) -> bool:
    """
    Super targeted rule for the pilot: if the headline mentions
    common flood/water-rise words, treat it as 'Warning - Flood'.
    """
    if not headline:
        return False
    return any(key in headline for key in FLOOD_KEYWORDS)


async def _fetch_json(client: httpx.AsyncClient, url: str) -> Optional[Dict[str, Any]]:
    try:
        r = await client.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.warning("[JMA FETCH ERROR] GET %s failed: %s", url, e)
        return None


async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    PILOT: Only Hokkaido: Kushiro Region (014100).
    Output exactly one entry if we detect a 'Warning - Flood' condition from the headline.
    Fields:
      - title: 'Warning – Flood'
      - region: 'Hokkaido: Kushiro Region'
      - level: 'Warning'
      - type:  'Flood'
      - summary: (left empty for now)
      - link: 'https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code=014100'
      - published: 'Fri, 08 Aug 2025 06:27 UTC'
    """
    # Make sure we only operate on the pilot office
    office_code = JMA_OFFICE_CODE

    # If the caller passed a local test file path in conf (handy for debugging)
    local_path = conf.get("local_office_json")
    data = None

    if local_path:
        try:
            p = Path(local_path)
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            logging.warning("[JMA DEBUG] Failed reading local_office_json %s: %s", local_path, e)

    if data is None:
        data = await _fetch_json(client, JMA_OFFICE_URL)
        if data is None:
            # Hard failure: return empty
            return {"entries": [], "source": JMA_OFFICE_URL}

    # Load name map
    code_to_name = _load_areacode_map()
    region_name = code_to_name.get(office_code, office_code)

    # Pull out headline + report time
    headline = data.get("headlineText", "") or ""
    published_iso = data.get("reportDatetime") or ""
    published_fmt = _fmt_published(published_iso)

    entries: List[Dict[str, Any]] = []

    # Very focused condition for the pilot run:
    # If the headline indicates flood, emit one 'Warning - Flood' for Kushiro.
    if _infer_flood_from_headline(headline):
        entries.append({
            "title": "Warning – Flood",
            "region": region_name,
            "level": "Warning",
            "type": "Flood",
            "summary": "",
            "link": "https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code=014100",
            "published": published_fmt,
        })
    else:
        logging.warning("[JMA DEBUG] No flood keywords detected in headline for %s: %r",
                        region_name, headline)

    logging.warning("[JMA DEBUG] Pilot parsed %d alert(s) for %s", len(entries), region_name)
    return {"entries": entries, "source": JMA_OFFICE_URL}
