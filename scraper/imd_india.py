# imd_india.py
from __future__ import annotations

import re
import requests
from bs4 import BeautifulSoup
from typing import Any, Mapping, Sequence
from dateutil import parser as dateparser

# --------------------------------------------------------------------
# Config
# --------------------------------------------------------------------

DEFAULT_SOURCE_URL = "https://mausam.imd.gov.in/imd_latest/contents/warnings.php"
TIMEOUT = 25

# --------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------

def _norm(s: Any) -> str:
    return (s or "").strip()

def _text(el) -> str:
    return _norm(el.get_text(" ", strip=True)) if el else ""

def _split_hazards(cell_text: str) -> list[str]:
    t = _norm(cell_text)
    if not t:
        return []
    # IMD uses <br> to join hazards; in plain text they’re comma/space separated
    parts = re.split(r"\s*[,|/]\s*|\s*br\s*", t, flags=re.I)
    # keep original order, drop empties, re-normalize
    out = []
    for p in parts:
        p = _norm(p)
        if p and p.lower() not in {"day 1", "day 2", "day 3", "no warning"}:
            out.append(p)
    # If “No warning” is the only thing, return []
    if not out and re.search(r"\bno warning\b", t, flags=re.I):
        return []
    return out

def _severity_from_style(style: str | None) -> str | None:
    s = (style or "").lower()
    # IMD uses background colors for severity; match by common rgb values.
    if "rgb(255, 0, 0)" in s or "background:#ff0000" in s:
        return "Red"
    if "rgb(255, 165, 0)" in s or "background:#ffa500" in s:
        return "Orange"
    if "rgb(255, 255, 0)" in s or "background:#ffff00" in s:
        return "Yellow"
    if "rgb(124, 252, 0)" in s or "background:#7cfc00" in s or "rgb(230, 255, 238)" in s:
        return "Green"
    return None

def _parse_issue_date(text: str | None) -> str | None:
    """
    Extract the 'Date of Issue: Month D, YYYY' to an ISO-ish string.
    We keep original formatting if parseable for downstream display.
    """
    t = _norm(text)
    if not t:
        return None
    # accept both "Date of Issue: October 5, 2025" and just "October 5, 2025"
    m = re.search(r"date of issue:\s*(.+)$", t, flags=re.I)
    candidate = _norm(m.group(1) if m else t)
    try:
        return dateparser.parse(candidate).isoformat()
    except Exception:
        return candidate  # fall back to raw string; renderer can still display it

def _parse_day_date(text: str | None) -> str | None:
    """
    Extract the 'Day N: Month D, YYYY' to ISO-ish string; fallback to raw.
    """
    t = _norm(text)
    if not t:
        return None
    m = re.search(r"day\s*\d+\s*:\s*(.+)$", t, flags=re.I)
    candidate = _norm(m.group(1) if m else t)
    try:
        return dateparser.parse(candidate).isoformat()
    except Exception:
        return candidate

# --------------------------------------------------------------------
# Core HTML parsing
# --------------------------------------------------------------------

def _parse_region_blocks(html_text: str, page_url: str) -> list[dict]:
    soup = BeautifulSoup(html_text, "html.parser")
    entries: list[dict] = []

    # Strategy:
    # - Find all TR headers like <th colspan="3">Warnings for <Region></th>
    # - The next <tr> is usually "Date of Issue: ...".
    # - Following rows contain "Day 1:", "Day 2:", ... with colored backgrounds.
    for th in soup.select("th[colspan='3']"):
        title = _text(th)
        m = re.search(r"warnings\s+for\s+(.+)$", title, flags=re.I)
        if not m:
            continue
        region = _norm(m.group(1))

        # Next row: issue date
        issue_row = th.find_next("tr")
        issue_iso: str | None = None
        if issue_row and issue_row.name == "tr":
            issue_iso = _parse_issue_date(_text(issue_row))

        # Collect day rows until next region header
        days: dict[str, dict] = {}
        cur = issue_row.find_next("tr") if issue_row else th.find_next("tr")
        while cur:
            # Stop at the next region header
            if cur.find("th", colspan=True) and re.search(r"warnings\s+for\s+", _text(cur), flags=re.I):
                break

            tds = cur.find_all("td")
            if len(tds) >= 2:
                day_label = _text(tds[0])     # e.g. "Day 2: October 6, 2025"
                hazards_cell = tds[1]
                sev = _severity_from_style(cur.get("style") or hazards_cell.get("style") or "")
                hazards = _split_hazards(hazards_cell.get_text("\n", strip=True).replace("\n", ", "))
                day_date_iso = _parse_day_date(day_label)

                if re.search(r"\bday\s*1\b", day_label, flags=re.I):
                    days["today"] = {
                        "severity": sev,
                        "hazards": hazards,
                        "date": day_date_iso,
                        "is_new": False,
                    }
                elif re.search(r"\bday\s*2\b", day_label, flags=re.I):
                    days["tomorrow"] = {
                        "severity": sev,
                        "hazards": hazards,
                        "date": day_date_iso,
                        "is_new": False,
                    }
            cur = cur.find_next("tr")

        # Build 'published' (FIX: prefer Date of Issue)
        day_dates: list[str] = []
        for k in ("today", "tomorrow"):
            if k in days and days[k].get("date"):
                day_dates.append(days[k]["date"])  # already ISO-ish or raw

        published_out = issue_iso or (max(day_dates) if day_dates else None)

        entry = {
            "region": region,
            "days": days,
            "published": published_out,
            "link": page_url,
        }
        entries.append(entry)

    return entries

# --------------------------------------------------------------------
# Public entrypoint
# --------------------------------------------------------------------

def fetch(conf: Mapping[str, Any]) -> dict:
    """
    Returns {"entries": [...]} where each entry:
      - region: str
      - days: { "today": {...}, "tomorrow": {...} }
      - published: Date-of-Issue (preferred) or newest day-date
      - link: source URL
    """
    url = _norm(conf.get("url") or DEFAULT_SOURCE_URL)
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    entries = _parse_region_blocks(r.text, url)
    # Keep only regions with at least today or tomorrow hazards (empty 'No warning' rows are filtered)
    entries = [e for e in entries if e.get("days")]
    return {"entries": entries}
