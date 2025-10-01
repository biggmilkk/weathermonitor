# scraper/imd_india.py
import asyncio
import re
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup

IMD_MC = "https://mausam.imd.gov.in/imd_latest/contents/subdivisionwise-warning_mc.php?id={id}"

# -------------------------------------------------------------------
# Severity mapping
#  - Only trust Orange when it's given as rgb(255,165,0)
#  - Keep Red support only for hex (#ff0000) for now
# -------------------------------------------------------------------

HEX_TO_SEVERITY = {
    "#ff0000": "Red",   # Red (hex only, no rgb red yet)
    # all other hexes ignored
}

RGB_TO_SEVERITY = {
    (255, 165, 0): "Orange",  # official orange
}

DEFAULT_ID_RANGE = list(range(1, 35))

# ----------------- helpers -----------------

def _extract_bgcolor(style: Optional[str]) -> Optional[str]:
    if not style:
        return None

    # Hex (#RGB or #RRGGBB)
    m_hex = re.search(r"background-color\s*:\s*([#A-Fa-f0-9]{3,7})\b", style)
    if m_hex:
        return m_hex.group(1).strip().lower()

    # rgb(r, g, b)
    m_rgb = re.search(
        r"background-color\s*:\s*rgb\s*\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})\s*\)",
        style,
        flags=re.IGNORECASE,
    )
    if m_rgb:
        r, g, b = (int(m_rgb.group(1)), int(m_rgb.group(2)), int(m_rgb.group(3)))
        return f"rgb({r},{g},{b})"

    return None

def _severity_from_tr(tr) -> Optional[str]:
    token = _extract_bgcolor(tr.get("style"))
    if not token:
        return None

    if token.startswith("#"):  # hex
        return HEX_TO_SEVERITY.get(token)

    if token.startswith("rgb(") and token.endswith(")"):  # rgb
        try:
            parts = token[4:-1].split(",")
            rgb = (int(parts[0]), int(parts[1]), int(parts[2]))
            return RGB_TO_SEVERITY.get(rgb)
        except Exception:
            return None

    return None

def _clean_text(el) -> str:
    for br in el.find_all("br"):
        br.replace_with(", ")
    txt = el.get_text(" ", strip=True)
    txt = re.sub(r"\s+", " ", txt).strip(" ,")
    txt = re.sub(r"(,\s*){2,}", ", ", txt)
    return txt

def _split_hazards(text: str) -> List[str]:
    parts = [p.strip(" ,;") for p in re.split(r",", text) if p.strip(" ,;")]
    seen, out = set(), []
    for p in parts:
        if p.lower() not in seen:
            seen.add(p.lower())
            out.append(p)
    return out

# ----------------- parsing -----------------

def _parse_sections_scoped(table) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    rows = table.find_all("tr")

    current_region: Optional[str] = None
    current_issue: Optional[str] = None
    section_rows: List = []

    def _flush_if_any(section_rows: List) -> None:
        nonlocal entries, current_region, current_issue
        for tr in section_rows:
            tds = tr.find_all("td")
            if len(tds) >= 2:
                day_label = _clean_text(tds[0])
                if re.match(r"^Day\s*1\s*:", day_label, re.I):
                    sev = _severity_from_tr(tr)
                    if sev not in ("Orange", "Red"):
                        return
                    hazards_text = _clean_text(tds[1])
                    hazards_list = _split_hazards(hazards_text)
                    m_date = re.search(r"Day\s*1\s*:\s*(.+)$", day_label, re.I)
                    day1_date = m_date.group(1).strip() if m_date else None

                    entries.append({
                        "title": f"IMD — {current_region or 'Sub-division'}",
                        "region": current_region,
                        "severity": sev,
                        "hazards": hazards_list,
                        "day1_date": day1_date,
                        "description": None,
                        "published": current_issue,
                        "source_url": None,
                        "is_new": False,
                    })
                    return

    for tr in rows:
        th = tr.find("th")
        if th:
            text = _clean_text(th)
            if text.startswith("Warnings for"):
                if current_region is not None:
                    _flush_if_any(section_rows)
                current_region = text.replace("Warnings for", "", 1).strip()
                current_issue = None
                section_rows = []
                continue
            elif "Date of Issue" in text:
                m = re.search(r"Date of Issue\s*:\s*(.+)$", text, re.I)
                if m:
                    current_issue = m.group(1).strip()
                continue
        if current_region is not None:
            section_rows.append(tr)

    if current_region is not None:
        _flush_if_any(section_rows)

    return entries

def _parse_mc_html(html: str, source_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, Any]] = []
    for tbl in soup.find_all("table"):
        if "Warnings for" in tbl.get_text(" ", strip=True):
            items = _parse_sections_scoped(tbl)
            for it in items:
                it["source_url"] = source_url
            out.extend(items)
    return out

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

    - Orange supports only rgb(255,165,0)
    - Red supports hex #ff0000 only (no rgb red yet)

    Output entry fields:
      - title: "IMD — <Region>"
      - region: "<Region>"
      - severity: "Orange" | "Red"
      - hazards: [list of hazards]
      - day1_date: "October 1, 2025"
      - published: "<Date of Issue>"
      - source_url: page URL
      - is_new: bool
    """
    ids = conf.get("ids") or DEFAULT_ID_RANGE
    entries = await _crawl_ids(client, ids)
    entries.sort(key=lambda e: e.get("published") or "", reverse=True)
    return {"entries": entries, "source": {"type": "imd_mc_pages"}}
