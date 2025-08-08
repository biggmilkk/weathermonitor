# scraper/jma.py

import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import httpx

# --- Config & constants -------------------------------------------------------

JMA_OFFICE_CODE = "014100"  # Kushiro Local Meteorological Observatory feed
JMA_OFFICE_JSON = f"https://www.jma.go.jp/bosai/warning/data/warning/{JMA_OFFICE_CODE}.json"
JMA_WARNING_PAGE = f"https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={JMA_OFFICE_CODE}"

# For this test: only Kushiro Region (014020) and only Flood (code "04")
TARGET_AREA_CODES = {"014020"}
PHENOMENON = {"04": "Flood"}
ACTIVE_STATUSES = {"発表", "継続"}

# Hard override to guarantee human-friendly name even if areacode.json lacks it
AREA_NAME_OVERRIDES = {
    "014020": "Hokkaido: Kushiro Region",
}

# --- Helpers ------------------------------------------------------------------

def _load_area_names(area_code_file: Optional[str]) -> Dict[str, str]:
    """
    Load area code -> human name mapping from scraper/areacode.json (preferred).
    Falls back to overrides; if still missing, returns the code as-is.
    """
    mapping: Dict[str, str] = {}
    if not area_code_file:
        area_code_file = "scraper/areacode.json"
    try:
        with open(area_code_file, "r", encoding="utf-8") as f:
            raw = json.load(f)
            if isinstance(raw, dict):
                mapping.update({str(k): str(v) for k, v in raw.items()})
            else:
                logging.warning("[JMA DEBUG] Unexpected areacode.json format; using overrides/fallbacks.")
    except Exception as e:
        logging.warning("[JMA DEBUG] areacode.json not found or unreadable; using overrides. (%s)", e)

    # Ensure overrides win
    mapping.update(AREA_NAME_OVERRIDES)
    return mapping

def _fmt_pub_dt(jst_iso: str) -> str:
    """Convert '2025-08-08T15:27:00+09:00' -> 'Fri, 08 Aug 2025 06:27 UTC'."""
    try:
        dt = datetime.fromisoformat(jst_iso)
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%a, %d %b %Y %H:%M UTC")
    except Exception:
        return jst_iso

# --- Main ---------------------------------------------------------------------

async def scrape_jma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    Test-scoped scraper:
    - Reads ONLY 014100.json (Kushiro office)
    - Emits ONLY 'Warning – Flood' for area 014020 (Kushiro Region) when active
    - Links to the JMA warning page UI (not the JSON)
    """
    area_code_file = conf.get("area_code_file") or "scraper/areacode.json"
    area_names = _load_area_names(area_code_file)

    try:
        resp = await client.get(JMA_OFFICE_JSON, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logging.warning("[JMA FETCH ERROR] %s", e)
        return {"entries": [], "error": str(e), "source": JMA_OFFICE_JSON}

    entries: List[Dict[str, Any]] = []
    published_str = _fmt_pub_dt(data.get("reportDatetime", ""))

    try:
        for group in data.get("areaTypes", []):
            for area in group.get("areas", []):
                code = str(area.get("code", ""))
                if code not in TARGET_AREA_CODES:
                    continue

                # Resolve human-readable name
                area_name = area_names.get(code, code)

                for w in area.get("warnings", []):
                    w_code = str(w.get("code", ""))
                    status = str(w.get("status", ""))
                    if w_code != "04":  # Flood only for this test
                        continue
                    if status not in ACTIVE_STATUSES:
                        continue

                    phenomenon = PHENOMENON.get(w_code, w_code)

                    entries.append({
                        "title": f"Warning – {phenomenon}",
                        "region": area_name,                 # should show "Hokkaido: Kushiro Region"
                        "level": "Warning",
                        "type": phenomenon,
                        "summary": "",
                        "published": published_str,          # e.g. 'Fri, 08 Aug 2025 06:27 UTC'
                        "link": JMA_WARNING_PAGE,            # UI page, not the JSON
                    })
    except Exception as e:
        logging.warning("[JMA FETCH ERROR] %s", e)
        return {"entries": [], "error": str(e), "source": JMA_OFFICE_JSON}

    logging.warning("[JMA DEBUG] Kushiro test parsed %d warning(s).", len(entries))
    return {"entries": entries, "source": JMA_OFFICE_JSON}
