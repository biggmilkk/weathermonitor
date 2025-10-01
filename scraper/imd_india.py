# scraper/imd_india.py
import asyncio
import re
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup

IMD_MC = "https://mausam.imd.gov.in/imd_latest/contents/subdivisionwise-warning_mc.php?id={id}"

# Valid sub-division IDs: 1..34 (35+ are empty)
DEFAULT_ID_RANGE = list(range(1, 35))

# Severity policy (as requested):
# - Orange -> rgb(255,165,0) only
# - Red    -> #ff0000 (hex) only
ORANGE_RGB = (255, 165, 0)
RED_HEX = "#ff0000"

# -------------------------------------------------------------------
# Style parsing helpers (robust to extra CSS like darkreader vars)
# -------------------------------------------------------------------

# Capture rgb(...) anywhere in style/border/background, ignore spacing & case
RGB_ANY_RE = re.compile(
    r"rgb\s*\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})\s*\)",
    re.IGNORECASE,
)

# Capture any hex like #RRGGBB or #RGB anywhere in style
HEX_ANY_RE = re.compile(r"#([0-9A-Fa-f]{3}|[0-9A-Fa-f]{6})")

def _rgb_from_style(style: Optional[str]) -> Optional[tuple]:
    if not style:
        return None
    m = RGB_ANY_RE.search(style)
    if not m:
        return None
    r, g, b = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return (max(0, min(255, r)), max(0, min(255, g)), max(0, min(255, b)))

def _hex_candidates_from_style(style: Optional[str]) -> List[str]:
    """Return ALL hex codes present (lowercased) — we will only accept #ff0000 for Red."""
    if not style:
        return []
    return [f"#{h.lower()}" for h in HEX_ANY_RE.findall(style)]

def _bgcolor_attr(node) -> Optional[str]:
    """
    Normalize legacy 'bgcolor' attribute (could be hex or rgb()) to:
      - 'rgb(r,g,b)' if rgb form
      - lowercase hex '#rrggbb' or '#rgb' if hex form
    """
    val = node.get("bgcolor")
    if not val:
        return None
    s = str(val).strip()
    m_rgb = RGB_ANY_RE.fullmatch(s) or RGB_ANY_RE.search(s)
    if m_rgb:
        r, g, b = int(m_rgb.group(1)), int(m_rgb.group(2)), int(m_rgb.group(3))
        return f"rgb({r},{g},{b})"
    m_hex = HEX_ANY_RE.search(s)
    if m_hex:
        return f"#{m_hex.group(0).lower()}" if not s.startswith("#") else s.lower()
    return s.lower()

def _is_orange_rgb_token(token: Optional[str]) -> bool:
    """Accept only exact rgb(255,165,0), insensitive to spaces/case."""
    if not token:
        return False
    m = RGB_ANY_RE.fullmatch(token) or RGB_ANY_RE.search(token)
    if not m:
        return False
    r, g, b = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return (r, g, b) == ORANGE_RGB

def _is_red_hex_token(token: Optional[str]) -> bool:
    """Accept only #ff0000 (exact), case-insensitive input normalized to lower."""
    if not token:
        return False
    return token.strip().lower() == RED_HEX

def _severity_from_row_or_cells(tr) -> Optional[str]:
    """
    Determine severity from the Day 1 row. We check, in priority:
      1) <tr style="..."> rgb/hex
      2) <td> style (first two cells)
      3) bgcolor attribute on tr/td
    Accept only:
      - Orange if any style token resolves to rgb(255,165,0)
      - Red    if any style/bgcolor token equals #ff0000
    """
    # 1) TR style
    style_tr = tr.get("style") or ""
    if _is_orange_rgb_token(style_tr):
        return "Orange"
    # Accept Red only if #ff0000 appears (ignore other hexes like #cc8400 from darkreader)
    for hx in _hex_candidates_from_style(style_tr):
        if _is_red_hex_token(hx):
            return "Red"

    # 2) TD styles
    tds = tr.find_all("td")
    for td in tds[:2]:
        style_td = td.get("style") or ""
        if _is_orange_rgb_token(style_td):
            return "Orange"
        for hx in _hex_candidates_from_style(style_td):
            if _is_red_hex_token(hx):
                return "Red"

    # 3) bgcolor attr
    bg_tr = _bgcolor_attr(tr)
    if _is_orange_rgb_token(bg_tr):
        return "Orange"
    if _is_red_hex_token(bg_tr):
        return "Red"

    for td in tds[:2]:
        bg_td = _bgcolor_attr(td)
        if _is_orange_rgb_token(bg_td):
            return "Orange"
        if _is_red_hex_token(bg_td):
            return "Red"

    return None

# -------------------------------------------------------------------
# Text cleaning
# -------------------------------------------------------------------

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

# -------------------------------------------------------------------
# Section-scoped parsing
# -------------------------------------------------------------------

def _parse_sections_scoped(table) -> List[Dict[str, Any]]:
    """
    Each 'Warnings for <Region>' block is a section.
    Within a section:
      - capture 'Date of Issue'
      - find the Day 1 row; read severity from row/cell colors
      - if Orange/Red, emit one entry for that region
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
        # Search the collected rows for Day 1
        for tr in section_rows:
            tds = tr.find_all("td")
            if len(tds) >= 2:
                day_label = _clean_text(tds[0])
                if re.match(r"^Day\s*1\s*:", day_label, re.I):
                    severity = _severity_from_row_or_cells(tr)
                    if severity not in ("Orange", "Red"):
                        break  # below threshold → skip this section
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
                # close previous section and start a new one
                if current_region is not None:
                    _flush_section()
                current_region = text.replace("Warnings for", "", 1).strip()
                current_issue = None
                section_rows = []
                continue
            if "Date of Issue" in text:
                m = re.search(r"Date of Issue\s*:\s*(.+)$", text, re.I)
                if m:
                    current_issue = m.group(1).strip()
                continue

        if current_region is not None:
            section_rows.append(tr)

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

# -------------------------------------------------------------------
# Async fetch
# -------------------------------------------------------------------

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

# -------------------------------------------------------------------
# Public entry
# -------------------------------------------------------------------

async def scrape_imd_current_orange_red_async(conf: dict, client) -> dict:
    """
    Crawl sub-division pages (ids 1..34). For each region section:
      - read 'Date of Issue'
      - read Day 1 row
      - severity from row/cell color:
          Orange -> rgb(255,165,0) only
          Red    -> #ff0000 (hex) only
      - keep only Orange/Red
    Emit one entry per region that meets the threshold.
    """
    ids = conf.get("ids") or DEFAULT_ID_RANGE
    entries = await _crawl_ids(client, ids)
    # Sort by Date of Issue (desc) if present
    entries.sort(key=lambda e: e.get("published") or "", reverse=True)
    return {"entries": entries, "source": {"type": "imd_mc_pages"}}
