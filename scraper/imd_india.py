# scraper/imd_india.py
import asyncio
import re
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup

IMD_MC = "https://mausam.imd.gov.in/imd_latest/contents/subdivisionwise-warning_mc.php?id={id}"

# Map inline background-color -> severity we care about.
# Add shades here if IMD changes tints.
HEX_TO_SEVERITY = {
    "#ff0000": "Red",
    "#ff9900": "Orange",
    "#ffa500": "Orange",
    # ignored:
    "#ffff00": None,   # Yellow
    "#7cfc00": None,   # Green
    "#4dff4d": None,   # Green (alt)
}

# Nationwide coverage: confirmed IDs 1..34 (35+ are empty)
DEFAULT_ID_RANGE = list(range(1, 35))

# ----------------- helpers -----------------

def _normalize_hex_from_style(style: Optional[str]) -> Optional[str]:
    """Extract and normalize hex from style='background-color: #xxxxxx'."""
    if not style:
        return None
    m = re.search(r"background-color\s*:\s*([#A-Fa-f0-9]{4,7})", style)
    if not m:
        return None
    return m.group(1).strip().lower()

def _severity_from_tr(tr) -> Optional[str]:
    return HEX_TO_SEVERITY.get(_normalize_hex_from_style(tr.get("style")))

def _clean_text(el) -> str:
    # Convert <br> to ", " and normalize whitespace
    for br in el.find_all("br"):
        br.replace_with(", ")
    txt = el.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", txt).strip(" ,")

def _parse_section(table) -> List[Dict[str, Any]]:
    """
    A section looks like:
      <tr><th>Warnings for <Region></th></tr>
      <tr><th>Date of Issue: ...</th></tr>
      <tr style="background-color:#xxxxxx"><td>Day 1: ...</td><td>Hazards</td></tr>
      ...
    Return a single entry if Day 1 is Orange/Red, else [].
    """
    rows = table.find_all("tr")
    region = None
    published = None

    # find region + date first
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

    # now find Day 1 row
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) >= 2:
            day_label = _clean_text(tds[0])
            if re.match(r"^Day\s*1\s*:", day_label, re.I):
                sev = _severity_from_tr(tr)
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
    Output: { "entries": [...], "source": {...} }
    """
    ids = conf.get("ids") or DEFAULT_ID_RANGE
    entries = await _crawl_ids(client, ids)

    # Sort by Date of Issue if present (desc)
    entries.sort(key=lambda e: e.get("published") or "", reverse=True)
    return {"entries": entries, "source": {"type": "imd_mc_pages"}}
