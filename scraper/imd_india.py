# scraper/imd_india.py
import asyncio
import re
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup

IMD_MC = "https://mausam.imd.gov.in/imd_latest/contents/subdivisionwise-warning_mc.php?id={id}"

# Exclude duplicates
EXCLUDED_IDS = {32, 33}
DEFAULT_ID_RANGE = [i for i in range(1, 35) if i not in EXCLUDED_IDS]

# Severity policy
ORANGE_HEX = "#ffa500"
RED_HEX    = "#ff0000"

# Regex helpers
HEX_ANY_RE = re.compile(r"#([0-9A-Fa-f]{6})")
DAY_LABEL_RE = re.compile(r"^Day\s*(\d+)\s*:\s*(.+)$", re.I)

def _extract_hex_set(s: Optional[str]) -> set[str]:
    if not s:
        return set()
    return {("#" + h.lower()) for h in HEX_ANY_RE.findall(s)}

def _bgcolor_attr_hex(node) -> Optional[str]:
    val = node.get("bgcolor")
    if not val:
        return None
    s = str(val).strip()
    m = HEX_ANY_RE.search(s)
    return f"#{m.group(1).lower()}" if m else None

def _severity_from_row(tr) -> Optional[str]:
    # tr.style
    tr_hex = _extract_hex_set(tr.get("style"))
    if ORANGE_HEX in tr_hex: return "Orange"
    if RED_HEX in tr_hex:    return "Red"
    # td.styles
    tds = tr.find_all("td")
    for td in tds[:2]:
        td_hex = _extract_hex_set(td.get("style"))
        if ORANGE_HEX in td_hex: return "Orange"
        if RED_HEX in td_hex:    return "Red"
    # bgcolor attrs
    bg_tr = _bgcolor_attr_hex(tr)
    if bg_tr == ORANGE_HEX: return "Orange"
    if bg_tr == RED_HEX:    return "Red"
    for td in tds[:2]:
        bg_td = _bgcolor_attr_hex(td)
        if bg_td == ORANGE_HEX: return "Orange"
        if bg_td == RED_HEX:    return "Red"
    return None

def _clean_text(el) -> str:
    for br in el.find_all("br"):
        br.replace_with(", ")
    import re as _re
    txt = el.get_text(" ", strip=True)
    txt = _re.sub(r"\s+", " ", txt).strip(" ,")
    txt = _re.sub(r"(,\s*){2,}", ", ", txt)
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

def _parse_region_block(rows: List, start_idx: int, source_id: int, source_url: str) -> (Optional[Dict[str, Any]], int):
    """Parse one 'Warnings for <Region>' section; return (entry_or_None, next_index)."""
    n = len(rows)
    # Region line
    th_text = _clean_text(rows[start_idx].find("th"))
    region = th_text.replace("Warnings for", "", 1).strip()

    # Date of Issue (next th row if present)
    issue = None
    i = start_idx + 1
    if i < n and rows[i].find("th"):
        t2 = _clean_text(rows[i].find("th"))
        m_issue = re.search(r"Date of Issue\s*:\s*(.+)$", t2, re.I)
        if m_issue:
            issue = m_issue.group(1).strip()
        i += 1

    # Walk day rows until next section or break
    days: Dict[str, Dict[str, Any]] = {}
    while i < n:
        tr = rows[i]
        th = tr.find("th")
        if th:
            # new section starts
            break
        tds = tr.find_all("td")
        if len(tds) >= 2:
            label = _clean_text(tds[0])
            m_day = DAY_LABEL_RE.match(label)
            if m_day:
                day_num = int(m_day.group(1))
                day_date = m_day.group(2).strip()
                if day_num in (1, 2):
                    sev = _severity_from_row(tr)
                    if sev in ("Orange", "Red"):
                        hazards = _split_hazards(_clean_text(tds[1]))
                        key = "today" if day_num == 1 else "tomorrow"
                        days[key] = {
                            "severity": sev,
                            "hazards": hazards,
                            "date": day_date,
                        }
        i += 1

    # Emit a single entry only if we have at least one day to keep
    if days:
        return ({
            "title": f"IMD — {region}",
            "region": region,
            "days": days,                 # {"today": {...}, "tomorrow": {...}} subset
            "published": issue,           # Date of Issue
            "source_url": source_url,
            "source_id": source_id,
            "is_new": False,
        }, i)

    return (None, i)

def _parse_tbody(tb, source_id: int, source_url: str) -> List[Dict[str, Any]]:
    rows = tb.find_all("tr", recursive=False) or tb.find_all("tr")
    out: List[Dict[str, Any]] = []
    i, n = 0, len(rows)
    while i < n:
        th = rows[i].find("th")
        if th:
            text = _clean_text(th)
            if text.startswith("Warnings for"):
                entry, next_i = _parse_region_block(rows, i, source_id, source_url)
                if entry:
                    out.append(entry)
                i = next_i
                continue
        i += 1
    return out

def _parse_mc_html(html: str, source_id: int, source_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, Any]] = []
    tbodys = soup.find_all("tbody")
    if tbodys:
        for tb in tbodys:
            out.extend(_parse_tbody(tb, source_id, source_url))
    else:
        for tbl in soup.find_all("table"):
            if "Warnings for" in tbl.get_text(" ", strip=True):
                out.extend(_parse_tbody(tbl, source_id, source_url))
    return out

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
    import asyncio as _asyncio
    sem = _asyncio.Semaphore(12)
    async def one(i: int):
        async with sem:
            try:
                return await _fetch_one(client, i)
            except Exception:
                return []
    results: List[Dict[str, Any]] = []
    chunks = await _asyncio.gather(*[one(i) for i in ids])
    for ch in chunks:
        results.extend(ch)
    return results

async def scrape_imd_current_orange_red_async(conf: dict, client) -> dict:
    """
    Crawl sub-division pages (ids 1..34, excluding 32 & 33).
    Emit ONE entry per region with days={"today":{...}, "tomorrow":{...}} (subset),
    keeping only Orange (#FFA500) and Red (#FF0000).
    Also de-duplicate by region, preferring the lowest source_id (31 over 32/33).
    """
    ids = conf.get("ids") or DEFAULT_ID_RANGE
    ids = [i for i in ids if i not in EXCLUDED_IDS]
    entries = await _crawl_ids(client, ids)

    # De-duplicate by region (keep lowest source id)
    dedup: Dict[str, Dict[str, Any]] = {}
    for e in entries:
        region = (e.get("region") or "").strip()
        if not region:
            continue
        sid = int(e.get("source_id") or 10**9)
        if region not in dedup or sid < int(dedup[region].get("source_id") or 10**9):
            dedup[region] = e

    final_entries = list(dedup.values())
    final_entries.sort(key=lambda e: (e.get("published") or "", e.get("region") or ""), reverse=True)
    return {"entries": final_entries, "source": {"type": "imd_mc_pages", "excluded_ids": sorted(EXCLUDED_IDS)}}
