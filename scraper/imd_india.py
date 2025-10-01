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
# - Red    -> HEX #FF0000 only
# -------------------------------------------------------------------
ORANGE_HEX = "#ffa500"
RED_HEX    = "#ff0000"

# -------------------------------------------------------------------
# Regex helpers
# -------------------------------------------------------------------
HEX_ANY_RE = re.compile(r"#([0-9A-Fa-f]{6})")
DAY_LABEL_RE = re.compile(r"^Day\s*(\d+)\s*:\s*(.+)$", re.I)  # captures day number + date (e.g., "1" + "October 1, 2025")

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

def _emit_entry(entries: List[Dict[str, Any]], *, region: str, issue: Optional[str],
                day_num: int, day_date: Optional[str], tr, source_id: int, source_url: str):
    """Create an entry for a given day row if severity meets threshold."""
    severity = _severity_from_row(tr)
    if severity not in ("Orange", "Red"):
        return
    tds = tr.find_all("td")
    if len(tds) < 2:
        return
    hazards_text = _clean_text(tds[1])
    hazards_list = _split_hazards(hazards_text)

    entries.append({
        "title": f"IMD — {region}",
        "region": region,
        "severity": severity,
        "hazards": hazards_list,
        "day": ("today" if day_num == 1 else "tomorrow" if day_num == 2 else f"day{day_num}"),
        "day_num": day_num,
        "day_date": day_date,          # e.g., "October 1, 2025"
        "description": None,           # avoid duplication
        "published": issue,            # IMD "Date of Issue"
        "source_url": source_url,
        "source_id": source_id,        # keep the numeric id for tie-breaks
        "is_new": False,
    })

def _parse_tbody(tb, source_id: int, source_url: str) -> List[Dict[str, Any]]:
    """
    Parse one <tbody> that may contain multiple 'Warnings for ...' sections:
      Warnings for <Region>
      Date of Issue: <...>
      Day 1: <...>   (Orange/Red → emit Today)
      Day 2: <...>   (Orange/Red → emit Tomorrow)
    """
    out: List[Dict[str, Any]] = []
    rows = tb.find_all("tr", recursive=False) or tb.find_all("tr")  # be liberal

    i = 0
    n = len(rows)
    while i < n:
        tr = rows[i]
        th = tr.find("th")
        if th:
            text = _clean_text(th)
            if text.startswith("Warnings for"):
                region = text.replace("Warnings for", "", 1).strip()

                # Date of Issue (next row if th)
                issue = None
                if i + 1 < n and rows[i + 1].find("th"):
                    t2 = _clean_text(rows[i + 1].find("th"))
                    m_issue = re.search(r"Date of Issue\s*:\s*(.+)$", t2, re.I)
                    if m_issue:
                        issue = m_issue.group(1).strip()
                    i += 1  # consume DoI row

                # Walk forward through subsequent rows until next section/blank break
                j = i + 1
                while j < n:
                    trj = rows[j]
                    thj = trj.find("th")
                    # stop when a new section starts or spacer rows (td colspan breaks) are encountered
                    if thj and _clean_text(thj).startswith("Warnings for"):
                        break

                    tds = trj.find_all("td")
                    if len(tds) >= 2:
                        label = _clean_text(tds[0])
                        m_day = DAY_LABEL_RE.match(label)
                        if m_day:
                            day_num = int(m_day.group(1))
                            day_date = m_day.group(2).strip()
                            if day_num in (1, 2):
                                _emit_entry(
                                    out, region=region, issue=issue,
                                    day_num=day_num, day_date=day_date, tr=trj,
                                    source_id=source_id, source_url=source_url
                                )
                    j += 1

                i = j - 1  # jump to end of this section
        i += 1

    return out

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
    Crawl sub-division pages (ids 1..34, exclude 32 & 33).
    For each region section:
      - read 'Date of Issue'
      - read Day 1 (→ item with day='today') and Day 2 (→ item with day='tomorrow')
      - severity from hex color:
          Orange -> #FFA500 only
          Red    -> #FF0000 only
      - keep only Orange/Red
    Emit one entry per (region, day) that meets the threshold.
    De-duplicate by (region, day_num): keep the lowest source_id (e.g., 31 over 32/33).
    """
    ids = conf.get("ids") or DEFAULT_ID_RANGE
    ids = [i for i in ids if i not in EXCLUDED_IDS]  # enforce exclusion even if conf passes them

    entries = await _crawl_ids(client, ids)

    # Deduplicate by region + day_num, preferring lowest source_id
    dedup: Dict[tuple, Dict[str, Any]] = {}
    for e in entries:
        region = (e.get("region") or "").strip()
        day_num = int(e.get("day_num") or 0)
        if not region or day_num not in (1, 2):
            continue
        sid = int(e.get("source_id") or 10**9)
        key = (region, day_num)
        if key not in dedup or sid < int(dedup[key].get("source_id") or 10**9):
            dedup[key] = e

    final_entries = list(dedup.values())
    final_entries.sort(key=lambda e: (e.get("published") or "", e.get("region") or "", e.get("day_num") or 0), reverse=True)

    return {"entries": final_entries, "source": {"type": "imd_mc_pages", "excluded_ids": sorted(EXCLUDED_IDS)}}
