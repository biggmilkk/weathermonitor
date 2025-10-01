# scraper/imd_india.py
import asyncio
import re
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup

IMD_MC = "https://mausam.imd.gov.in/imd_latest/contents/subdivisionwise-warning_mc.php?id={id}"

# -------------------------------------------------------------------
# Severity mapping
# NOTE:
#  - You asked to explicitly support Orange as rgb(255, 165, 0).
#  - We keep existing hex handling, including Red (#ff0000),
#    but we DO NOT add rgb(...) detection for Red yet.
#  - If IMD uses slightly different orange tints, we accept common hexes too.
# -------------------------------------------------------------------

HEX_TO_SEVERITY = {
    "#ff0000": "Red",      # keep hex red support (no rgb red yet)
    "#ff9900": "Orange",
    "#ffa500": "Orange",   # canonical orange
    "#ff8c00": "Orange",   # sometimes used
    "#f90":    "Orange",   # short hex (rare, but cheap to support)
    # ignored:
    "#ffff00": None,       # Yellow
    "#7cfc00": None,       # Green
    "#4dff4d": None,       # Green (alt)
}

# rgb(...) recognition — ONLY Orange for now, per your request
RGB_TO_SEVERITY = {
    (255, 165, 0): "Orange",
    # no red rgb here yet — you'll add when ready
}

# Nationwide coverage: confirmed IDs 1..34 (35+ are empty)
DEFAULT_ID_RANGE = list(range(1, 35))

# ----------------- helpers -----------------

def _extract_bgcolor(style: Optional[str]) -> Optional[str]:
    """
    Extract background-color from an inline style.
    Returns a normalized token:
      - hex in lowercase (e.g., '#ffa500' or '#f90')
      - or 'rgb(r,g,b)' with canonical spacing removed (lowercase)
    """
    if not style:
        return None

    # Try hex first (#RGB or #RRGGBB)
    m_hex = re.search(r"background-color\s*:\s*([#A-Fa-f0-9]{3,7})\b", style)
    if m_hex:
        return m_hex.group(1).strip().lower()

    # Try rgb() — allow arbitrary whitespace
    m_rgb = re.search(
        r"background-color\s*:\s*rgb\s*\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})\s*\)",
        style,
        flags=re.IGNORECASE,
    )
    if m_rgb:
        r, g, b = (int(m_rgb.group(1)), int(m_rgb.group(2)), int(m_rgb.group(3)))
        # Constrain to valid byte range
        r = max(0, min(255, r))
        g = max(0, min(255, g))
        b = max(0, min(255, b))
        return f"rgb({r},{g},{b})"

    return None

def _severity_from_tr(tr) -> Optional[str]:
    """
    Map a <tr> style's background-color to severity.
    Supports:
      - hex colors via HEX_TO_SEVERITY
      - rgb(...) for Orange only: rgb(255,165,0)
    """
    token = _extract_bgcolor(tr.get("style"))
    if not token:
        return None

    # Hex path
    if token.startswith("#"):
        return HEX_TO_SEVERITY.get(token)

    # rgb(...) path — only Orange for now
    if token.startswith("rgb(") and token.endswith(")"):
        try:
            parts = token[4:-1].split(",")
            rgb = (int(parts[0]), int(parts[1]), int(parts[2]))
            return RGB_TO_SEVERITY.get(rgb)
        except Exception:
            return None

    return None

def _clean_text(el) -> str:
    """
    Convert <br> to ", ", collapse whitespace, and remove duplicate commas.
    """
    for br in el.find_all("br"):
        br.replace_with(", ")
    txt = el.get_text(" ", strip=True)
    txt = re.sub(r"\s+", " ", txt).strip(" ,")
    # Collapse duplicate commas/spaces that sometimes appear after replacements
    txt = re.sub(r"(,\s*){2,}", ", ", txt)
    return txt

def _parse_section(table) -> List[Dict[str, Any]]:
    """
    A section looks like:
      <tr><th>Warnings for <Region></th></tr>
      <tr><th>Date of Issue: ...</th></tr>
      <tr style="background-color:..."><td>Day 1: ...</td><td>Hazards</td></tr>
      ...
    Return a single entry if Day 1 is Orange/Red (per current mapping), else [].
    """
    rows = table.find_all("tr")
    region = None
    published = None

    # Find region and date header rows first
    for tr in rows:
        th = tr.find("th")
        if th:
            text = _clean_text(th)
            if text.startswith("Warnings for"):
                region = text.replace("Warnings for", "", 1).strip()
            elif "Date of Issue" in text:
                m = re.search(r"Date of Issue\s*:\s*(.+)$", text, re.I)
                if m:
                    published = m.group(1).strip()

    # Now find Day 1 row
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) >= 2:
            day_label = _clean_text(tds[0])
            if re.match(r"^Day\s*1\s*:", day_label, re.I):
                sev = _severity_from_tr(tr)
                # Keep only Orange/Red per current requirement
                if sev not in ("Orange", "Red"):
                    return []
                hazards = _clean_text(tds[1])
                title = f"{sev} • {hazards or 'Weather'} — {region or 'IMD Sub-division'}"
                return [{
                    "title": title,
                    "region": region,
                    "severity": sev,
                    "event": hazards or None,
                    "description": f"{day_label} — {hazards}" if hazards else day_label,
                    "published": published,   # IMD's Date of Issue
                    "source_url": None,       # filled by caller
                    "is_new": False,
                }]
    return []

def _parse_mc_html(html: str, source_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    entries: List[Dict[str, Any]] = []

    # Find all tables that contain "Warnings for"
    for tbl in soup.find_all("table"):
        if "Warnings for" in tbl.get_text(" ", strip=True):
            items = _parse_section(tbl)
            for it in items:
                it["source_url"] = source_url
            entries.extend(items)

    return entries

# ----------------- async fetch -----------------

async def _fetch_one(client, idx: int) -> List[Dict[str, Any]]:
    url = IMD_MC.format(id=idx)
    r = await client.get(url, timeout=20.0)
    if r.status_code != 200 or not r.text or "Warnings for" not in r.text:
        return []
    try:
        return _parse_mc_html(r.text, url)
    except Exception:
        return []

async def _crawl_ids(client, ids: List[int]) -> List[Dict[str, Any]]:
    sem = asyncio.Semaphore(12)

    async def one(i: int):
        async with sem:
            try:
                return await _fetch_one(client, i)
            except Exception:
                return []

    results: List[Dict[str, Any]] = []
    chunks = await asyncio.gather(*[one(i) for i in ids])
    for ch in chunks:
        results.extend(ch)
    return results

# ----------------- public entry -----------------

async def scrape_imd_current_orange_red_async(conf: dict, client) -> dict:
    """
    Scrape ids 1..34 for 'Sub-division-wise warnings'.
    Keep only Day 1 rows where background-color maps to Orange/Red.
    - Orange supports: hex (#ff9900, #ffa500, #ff8c00, #f90) and rgb(255,165,0)
    - Red supports: hex (#ff0000) only for now (no rgb red handling yet)
    Output: { "entries": [...], "source": {...} }
    """
    ids = conf.get("ids") or DEFAULT_ID_RANGE
    entries = await _crawl_ids(client, ids)

    # Sort by Date of Issue if present (desc)
    entries.sort(key=lambda e: e.get("published") or "", reverse=True)
    return {"entries": entries, "source": {"type": "imd_mc_pages"}}
