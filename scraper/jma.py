import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import httpx

# --- Config & constants -------------------------------------------------------

JMA_OFFICE_CODE = "014100"  # Hokkaido: Kushiro Local Meteorological Observatory feed
JMA_OFFICE_URL = f"https://www.jma.go.jp/bosai/warning/data/warning/{JMA_OFFICE_CODE}.json"

# Only emit "Warning / Alert / Emergency" levels (we'll infer level by code/status for this test).
# For this Kushiro test, we only produce "Warning – Flood" (code 04) for the Kushiro Region area code 014020.
TARGET_AREA_CODES = {"014020"}  # Kushiro Region

# Phenomenon name mapping (subset for the test)
PHENOMENON = {
    "04": "Flood",
    # We'll add more later for the full rollout
}

# Statuses in the per-office JSON:
# - "発表" (issued) / "継続" (continued) should be considered active; "解除" (canceled) is inactive.
ACTIVE_STATUSES = {"発表", "継続"}


# --- Helpers ------------------------------------------------------------------

def _load_area_names(area_code_file: Optional[str]) -> Dict[str, str]:
    """
    Load area code -> human name mapping from scraper/areacode.json (preferred),
    otherwise fallback to naive code echo.
    """
    if not area_code_file:
        # Default to repo path the user said they use
        area_code_file = "scraper/areacode.json"
    try:
        with open(area_code_file, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        logging.warning("[JMA DEBUG] areacode.json not found or unreadable; using codes as names. (%s)", e)
        return {}

    # The uploaded areacode.json uses a flat mapping in most setups: { "014020": "Hokkaido: Kushiro Region", ... }
    # If yours is nested, adapt here. For now, assume flat.
    if isinstance(raw, dict):
        # Ensure values are strings
        return {str(k): str(v) for k, v in raw.items()}
    logging.warning("[JMA DEBUG] Unexpected areacode.json format; using codes as names.")
    return {}


def _fmt_pub_dt(jst_iso: str) -> str:
    """
    Convert JMA's JST ISO string (e.g., '2025-08-08T15:27:00+09:00') to 'Fri, 08 Aug 2025 06:27 UTC'.
    """
    try:
        dt = datetime.fromisoformat(jst_iso)
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%a, %d %b %Y %H:%M UTC")
    except Exception:
        return jst_iso  # fallback


# --- Main scraper -------------------------------------------------------------

async def scrape_jma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    Test-scoped scraper:
    - Fetches ONLY the Kushiro Local Office JSON (014100.json)
    - Emits a single entry per TARGET_AREA_CODES with active Flood (code '04') warnings
    - Formats: "Hokkaido: Kushiro Region", "Warning – Flood", Published: <UTC>
    """
    area_code_file = conf.get("area_code_file") or "scraper/areacode.json"
    area_names = _load_area_names(area_code_file)

    try:
        # Fetch the office JSON
        resp = await client.get(JMA_OFFICE_URL, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logging.warning("[JMA FETCH ERROR] %s", e)
        return {"entries": [], "error": str(e), "source": JMA_OFFICE_URL}

    entries: List[Dict[str, Any]] = []

    # Published time
    pub_jst = data.get("reportDatetime", "")
    published_str = _fmt_pub_dt(pub_jst)

    # Structure of the office JSON:
    # {
    #   "reportDatetime": "...+09:00",
    #   "areaTypes": [
    #       { "areas": [ { "code":"014010", "warnings":[ {"code":"14","status":"継続"}, ... ] }, ... ] },
    #       { "areas": [ { "code":"0120601", "warnings":[ ... ] }, ... ] }
    #   ],
    #   ...
    # }
    #
    # We need to scan all areaTypes[].areas[]. For our test, we only care about area code "014020" (Kushiro Region).
    try:
        area_types = data.get("areaTypes", [])
        for group in area_types:
            for area in group.get("areas", []):
                code = str(area.get("code", ""))
                if code not in TARGET_AREA_CODES:
                    continue  # Only Kushiro Region for this test

                # The area name: prefer areacode mapping; fallback to the code
                area_name = area_names.get(code, code)

                # Each area has a 'warnings' array with dicts like {"code":"04","status":"発表", ...}
                for w in area.get("warnings", []):
                    w_code = str(w.get("code", ""))
                    status = str(w.get("status", ""))

                    # Only active & only Flood (code '04') for this test
                    if w_code != "04":
                        continue
                    if status not in ACTIVE_STATUSES:
                        continue

                    phenomenon = PHENOMENON.get(w_code, w_code)

                    entries.append({
                        "title": f"Warning – {phenomenon}",
                        "region": area_name,  # should read "Hokkaido: Kushiro Region" from areacode.json
                        "level": "Warning",
                        "type": phenomenon,
                        "summary": "",
                        "published": published_str,
                        "link": JMA_OFFICE_URL,
                    })

    except Exception as e:
        logging.warning("[JMA FETCH ERROR] %s", e)
        return {"entries": [], "error": str(e), "source": JMA_OFFICE_URL}

    logging.warning("[JMA DEBUG] Kushiro test parsed %d warning(s).", len(entries))
    return {"entries": entries, "source": JMA_OFFICE_URL}
