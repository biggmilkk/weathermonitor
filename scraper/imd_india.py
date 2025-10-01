# scraper/imd_india.py
import asyncio
import re
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup

IMD_MC = "https://mausam.imd.gov.in/imd_latest/contents/subdivisionwise-warning_mc.php?id={id}"

# IDs 1..34 are valid; 35+ are empty
DEFAULT_ID_RANGE = list(range(1, 35))

# --- Severity policy (as requested) ---------------------------------
# Orange must be exactly rgb(255,165,0). Red is hex-only (#ff0000).
ORANGE_RGB = (255, 165, 0)
RED_HEX_LOWER = "#ff0000"


# ----------------- style parsing helpers -----------------

_RGB_RE = re.compile(
    r"(?:background(?:-color)?|bgcolor)\s*:\s*rgb\s*\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})\s*\)",
    re.IGNORECASE,
)
_HEX_RE = re.compile(
    r"(?:background(?:-color)?|bgcolor)\s*:\s*([#A-Fa-f0-9]{3,7})\b",
    re.IGNORECASE,
)

def _rgb_tuple_from_style(style: Optional[str]) -> Optional[tuple]:
    if not style:
        return None
    m = _RGB_RE.search(style)
    if not m:
        return None
    r, g, b = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return (max(0, min(255, r)), max(0, min(255, g)), max(0, min(255, b)))

def _hex_from_style(style: Optional[str]) -> Optional[str]:
    if not style:
        return None
    m = _HEX_RE.search(style)
    if not m:
        return None
    return m.group(1).strip().lower()

def _bgcolor_attr(node) -> Optional[str]:
    # Some pages may use a legacy bgcolor attribute
    val = node.get("bgcolor")
    if not val:
        return None
    s = str(val).strip()
    # Normalize hex-like values
    if s.startswith("#") and (len(s) in (4, 7)):
        return s.lower()
    # Normalize rgb(...) values if present
    m = re.match(r"rgb\s*\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})\s*\)", s, re.I)
    if m:
        r, g, b = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"rgb({r},{g},{b})"
    return s.lower()


def _severity_from_row_or_cells(tr) -> Optional[str]:
    """
    Determine severity from the Day 1 row. We check, in order:
      1) <tr style="..."> for rgb(...) or hex (#...).
      2) Each <td> style (first 2 cells).
      3) bgcolor attribute on tr/td.
    Accept only:
      - Orange if rgb == (255,165,0)
      - Red    if hex == #ff0000
    """
    # 1) TR style
    rgb = _rgb_tuple_from_style(tr.get("style"))
    if rgb == ORANGE_RGB:
        return "Orange"
    hx = _hex_from_style(tr.get("style"))
    if hx == RED_HEX_LOWER:
        return "Red"

    # 2) TD styles
    tds = tr.find_all("td")
    for td in tds[:2]:
        rgb = _rgb_tuple_from_style(td.get("style"))
        if rgb == ORANGE_RGB:
            return "Orange"
        hx = _hex_from_style(td.get("style"))
        if hx == RED_HEX_LOWER:
            return "Red"

    # 3) bgcolor attribute on tr / td
    bg_tr = _bgcolor_attr(tr)
    if bg_tr == RED_HEX_LOWER:
        return "Red"
    if bg_tr == f"rgb({ORANGE_RGB[0]},{ORANGE_RGB[1]},{ORANGE_RGB[2]})":
        return "Orange"

    for td in tds[:2]:
        bg_td = _bgcolor_attr(td)
        if bg_td == RED_HEX_LOWER:
            return "Red"
        if bg_td == f"rgb({ORANGE_RGB[0]},{ORANGE_RGB[1]},{ORANGE_RGB[2]})":
            return "Orange"

    return None


# ----------------- text cleaning -----------------

def _clean_text(el) -> str:
    # Convert <br> to ", " and normalize whitespace/commas
    for br in el.find_all("br"):
        br.replace_with(", ")
    txt = el.get_text(" ", strip=True)
    txt = re.sub(r"\s+", " ", txt).strip(" ,")
    txt = re.sub(r"(,\s*){2,}", ", ", txt)
    return txt

def _split_hazards(text: str) -> List[str]:
    parts = [p.strip(" ,;") for p in text.split(",") if p.strip(" ,;")]
    seen, out = set(), []
    for p in parts:
        low = p.lower()
        if low not in seen:
            seen.add(low)
            out.append(p)
    return out


# ----------------- section-scoped parsing -----------------

def _parse_sections_scoped(table) -> List[Dict[str, Any]]:
    """
    Each 'Warnings for <Region>' block is a section.
    Within a section:
      - capture 'Date of Issue'
      - find the Day 1 row; read severity from row/cell colors
      - if Orange/Red, emit a single entry for that region
    """
    entries: List[Dict[str, Any]] = []
    rows = table.find_all("tr")

    current_region: Optional[str] = None
    current_issue: Optional[str] = None
    section_rows: List = []

    def _flush_section() -> None:
        nonlocal entries, current_region, current_issue, section_rows
        if current_region is None:
            section_rows = []
            return
        # look for Day 1 inside the accumulated rows
        for tr in section_rows:
            tds = tr.find_all("td")
            if len(tds) >= 2:
                day_label = _clean_text(tds[0])
                if re.match(r"^Day\s*1\s*:", day_label, re.I):
                    severity = _severity_from_row_or_cells(tr)
                    if severity not in ("Orange", "Red"):
                        break  # not high enough level; skip section
                    hazards_text = _clean_text(tds[1])
                    hazards_list = _split_hazards(hazards_text)
                    m_date = re.search(r"Day\s*1\s*:\s*(.+)$", day_label, re.I)
                    day1_date = m_date.group(1).strip() if m_date else None

                    entries.append({
                        "title": f"IMD — {current_region}",
                        "region": current_region,
                        "severity": severity,
                        "hazards": hazards_list,
                        "day1_date": day1_date,
                        "description": None,          # avoid duplication
                        "published": current_issue,   # "Date of Issue"
                        "source_url": None,           # filled by caller
                        "is_new": False,
                    })
                    break
        section_rows = []

    for tr in rows:
        th = tr.find("th")
        if th:
            text = _clean_text(th)
            if text.startswith("Warnings for"):
                # close previous section
                if current_region is not None:
                    _flush_section()
                # start new section
                current_region = text.replace("Warnings for", "", 1).strip()
                current_issue = None
                section_rows = []
                continue
            if "Date of Issue" in text:
                m = re.search(r"Date of Issue\s*:\s*(.+)$", text, re.I)
                if m:
                    current_issue = m.group(1).strip()
                continue

        # Collect potential day rows for current section
        if current_region is not None:
            section_rows.append(tr)

    # flush last open section
    if current_region is not None:
        _flush_section()

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
    Crawl sub-division pages (ids 1..34).
    For each region section on a page:
      - read 'Date of Issue'
      - read Day 1 row
      - determine severity from the row/cell color:
          Orange -> rgb(255,165,0) only
          Red    -> #ff0000 (hex) only
      - keep only Orange/Red
    Emit one entry per region that meets the threshold.
    """
    ids = conf.get("ids") or DEFAULT_ID_RANGE
    entries = await _crawl_ids(client, ids)
    entries.sort(key=lambda e: e.get("published") or "", reverse=True)
    return {"entries": entries, "source": {"type": "imd_mc_pages"}}
