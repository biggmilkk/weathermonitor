# scraper/imd_india.py
import asyncio
import re
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup

IMD_MC = "https://mausam.imd.gov.in/imd_latest/contents/subdivisionwise-warning_mc.php?id={id}"

# -------------------------------------------------------------------
# IDs: 1..34 are valid; 32 and 33 are duplicates of 31 → EXCLUDE
# -------------------------------------------------------------------
EXCLUDED_IDS = {32, 33}
DEFAULT_ID_RANGE = [i for i in range(1, 35) if i not in EXCLUDED_IDS]

# -------------------------------------------------------------------
# Severity policy (per your instructions):
# - Orange -> HEX #FFA500 only
# - Red    -> HEX #FF0000 only (placeholder until you confirm)
# -------------------------------------------------------------------
ORANGE_HEX = "#ffa500"
RED_HEX    = "#ff0000"

# -------------------------------------------------------------------
# Regex helpers
# -------------------------------------------------------------------
HEX_ANY_RE = re.compile(r"#([0-9A-Fa-f]{6})")
DAY1_LABEL_RE = re.compile(r"^Day\s*1\s*:", re.I)

def _norm(s: Optional[str]) -> str:
    return (s or "").strip()

def _extract_hex_set(s: Optional[str]) -> set[str]:
    """Return a set of lowercase #rrggbb codes found anywhere in s (style strings etc.)."""
    if not s:
        return set()
    return {("#" + h.lower()) for h in HEX_ANY_RE.findall(s)}

def _bgcolor_attr_hex(node) -> Optional[str]:
    """Normalize legacy bgcolor attribute to lowercase hex #rrggbb if present."""
    val = node.get("bgcolor")
    if not val:
        return None
    s = str(val).strip()
    m = HEX_ANY_RE.search(s)
    return f"#{m.group(1).lower()}" if m else None

def _severity_from_row(tr) -> Optional[str]:
    """
    Accept severity based on hex found in:
      - <tr style="...">
      - first two <td> style="..."
      - bgcolor attr on tr/td
    """
    # 1) TR style hexes
    tr_hexes = _extract_hex_set(tr.get("style"))
    if ORANGE_HEX in tr_hexes:
        return "Orange"
    if RED_HEX in tr_hexes:
        return "Red"

    # 2) TD style hexes
    tds = tr.find_all("td")
    for td in tds[:2]:
        td_hexes = _extract_hex_set(td.get("style"))
        if ORANGE_HEX in td_hexes:
            return "Orange"
        if RED_HEX in td_hexes:
            return "Red"

    # 3) bgcolor attributes
    bg_tr = _bgcolor_attr_hex(tr)
    if bg_tr == ORANGE_HEX:
        return "Orange"
    if bg_tr == RED_HEX:
        return "Red"

    for td in tds[:2]:
        bg_td = _bgcolor_attr_hex(td)
        if bg_td == ORANGE_HEX:
            return "Orange"
        if bg_td == RED_HEX:
            return "Red"

    return None

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
# Parsing
# -------------------------------------------------------------------

def _parse_tbody(tb, source_id: int, source_url: str) -> List[Dict[str, Any]]:
    """
    Parse one <tbody> that may contain multiple 'Warnings for ...' sections:
      Warnings for <Region>
      Date of Issue: <...>
      Day 1: <...>   (this row decides severity via hex color)
      Day 2: ...
    Emit exactly one entry per section if Day 1 is Orange/Red.
    """
    entries: List[Dict[str, Any]] = []
    rows = tb.find_all("tr", recursive=False) or tb.find_all("tr")  # be liberal

    current_region: Optional[str] = None
    current_issue: Optional[str] = None
    i = 0
    n = len(rows)

    while i < n:
        tr = rows[i]
        th = tr.find("th")
        if th:
            text = _clean_text(th)
            # Start of a new section
            if text.startswith("Warnings for"):
                current_region = text.replace("Warnings for", "", 1).strip()
                current_issue = None

                # Expect next row to be Date of Issue
                if i + 1 < n:
                    th2 = rows[i + 1].find("th")
                    if th2:
                        t2 = _clean_text(th2)
                        if "Date of Issue" in t2:
                            m = re.search(r"Date of Issue\s*:\s*(.+)$", t2, re.I)
                            if m:
                                current_issue = m.group(1).strip()
                            i += 1  # consume DoI row

                # Expect next row to be Day 1; else fallback search
                day1_row = None
                if i + 1 < n:
                    candidate = rows[i + 1]
                    tds = candidate.find_all("td")
                    if len(tds) >= 2 and DAY1_LABEL_RE.match(_clean_text(tds[0])):
                        day1_row = candidate
                        i += 1  # consume Day 1 row
                if day1_row is None:
                    j = i + 1
                    scan_limit = min(n, i + 10)
                    while j < scan_limit:
                        tds = rows[j].find_all("td")
                        if len(tds) >= 2 and DAY1_LABEL_RE.match(_clean_text(tds[0])):
                            day1_row = rows[j]
                            i = j
                            break
                        j += 1

                if current_region and day1_row:
                    severity = _severity_from_row(day1_row)
                    if severity in ("Orange", "Red"):
                        tds = day1_row.find_all("td")
                        day_label = _clean_text(tds[0])
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
                            "published": current_issue,   # IMD "Date of Issue"
                            "source_url": source_url,
                            "source_id": source_id,       # keep the numeric id for tie-breaks
                            "is_new": False,
                        })
        i += 1

    return entries

def _parse_mc_html(html: str, source_id: int, source_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, Any]] = []

    # Prefer explicit TBODY blocks
    tbodys = soup.find_all("tbody")
    if tbodys:
        for tb in tbodys:
            out.extend(_parse_tbody(tb, source_id, source_url))
    else:
        # Fallback: some pages may lack <tbody>
        for tbl in soup.find_all("table"):
            if "Warnings for" in tbl.get_text(" ", strip=True):
                out.extend(_parse_tbody(tbl, source_id, source_url))
    return out

# -------------------------------------------------------------------
# Async fetch
# -------------------------------------------------------------------

async def _fetch_one(client, idx: int) -> List[Dict[str, Any]]:
    url = IMD_MC.format(id=idx)
    r = await client.get(url, timeout=20.0)
    if r.status_code != 200 or not r.text:
        return []
    try:
        return _parse_mc_html(r.text, idx, url)
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
    Crawl sub-division pages (ids 1..34, but exclude 32 & 33).
    For each region section:
      - read 'Date of Issue'
      - read Day 1 row
      - severity from hex color:
          Orange -> #FFA500 only
          Red    -> #FF0000 only
      - keep only Orange/Red
    Emit one entry per region that meets the threshold.
    Also: de-duplicate by region name (keep the lowest source_id).
    """
    # 1) Resolve and hard-filter ids
    ids = conf.get("ids") or DEFAULT_ID_RANGE
    ids = [i for i in ids if i not in EXCLUDED_IDS]  # enforce exclusion even if conf passes them

    # 2) Crawl
    entries = await _crawl_ids(client, ids)

    # 3) De-duplicate by region name: keep the lowest source_id (so 31 wins vs 32/33)
    dedup: Dict[str, Dict[str, Any]] = {}
    for e in entries:
        region = (e.get("region") or "").strip()
        if not region:
            continue
        sid = int(e.get("source_id") or 10**9)
        if region not in dedup or sid < int(dedup[region].get("source_id") or 10**9):
            dedup[region] = e

    final_entries = list(dedup.values())

    # 4) Sort by Date of Issue desc if present
    final_entries.sort(key=lambda e: e.get("published") or "", reverse=True)

    return {"entries": final_entries, "source": {"type": "imd_mc_pages", "excluded_ids": sorted(EXCLUDED_IDS)}}
