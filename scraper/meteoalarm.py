# scraper/meteoalarm.py
"""
Meteoalarm scraper.
- Fetches per-country RSS feeds and/or the EU overview
- Normalizes country names
- Aggregates alerts by country and day (today/tomorrow)
- Computes counts for "(n active)" and attaches a country-level 'link'
"""

from __future__ import annotations

import re
import time
import html
import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

import feedparser

# --------------------------------------------------------------------
# Constants / Normalization
# --------------------------------------------------------------------

# Canonicalize verbose / legacy country names from the EU feed
NORMALIZE_COUNTRY_NAMES = {
    # Existing examples (add any others you like)
    "Macedonia (the former Yugoslav Republic of)": "North Macedonia",
    "MeteoAlarm Macedonia (the former Yugoslav Republic of)": "North Macedonia",
    # ✅ FIX: shorten the UK's long official name
    "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
}

# Slugs to fetch per-country RSS (only a few shown here for brevity; keep your full table)
COUNTRY_TO_RSS_SLUG = {
    "Austria": "austria",
    "Belgium": "belgium",
    "Croatia": "croatia",
    "Czech Republic": "czech-republic",
    "Denmark": "denmark",
    "France": "france",
    "Germany": "germany",
    "Greece": "greece",
    "Hungary": "hungary",
    "Ireland": "ireland",
    "Italy": "italy",
    "Netherlands": "netherlands",
    "Norway": "norway",
    "Poland": "poland",
    "Portugal": "portugal",
    "Romania": "romania",
    "Serbia": "serbia",
    "Slovakia": "slovakia",
    "Slovenia": "slovenia",
    "Spain": "spain",
    "Sweden": "sweden",
    "Switzerland": "switzerland",
    "United Kingdom": "united-kingdom",
    # If the EU overview ever returns the long form even after normalization:
    "United Kingdom of Great Britain and Northern Ireland": "united-kingdom",
}

# ⚠️ This table is used ONLY for building the front-end "Read more" link
# (not for fetching). GB vs UK matters here. Keep UK as "UK".
COUNTRY_TO_CODE = {
    "Austria": "AT",
    "Belgium": "BE",
    "Croatia": "HR",
    "Czech Republic": "CZ",
    "Denmark": "DK",
    "France": "FR",
    "Germany": "DE",
    "Greece": "GR",
    "Hungary": "HU",
    "Ireland": "IE",
    "Italy": "IT",
    "Netherlands": "NL",
    "Norway": "NO",
    "Poland": "PL",
    "Portugal": "PT",
    "Romania": "RO",
    "Serbia": "RS",
    "Slovakia": "SK",
    "Slovenia": "SI",
    "Spain": "ES",
    "Sweden": "SE",
    "Switzerland": "CH",
    # ✅ FIX: Use UK (not GB) for the Meteoalarm front-end region code
    "United Kingdom": "UK",
    # If a long-form name ever slips through prior to normalization:
    "United Kingdom of Great Britain and Northern Ireland": "UK",
}

LEVEL_WHITELIST = {"Orange", "Red"}  # renderer focuses on severe levels

RSS_BASE = "https://www.meteoalarm.org/en_RSS/1.0"
FRONT_BASE = "https://meteoalarm.org/en/live/region"

UTC = timezone.utc


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------

def _norm(s: Any) -> str:
    return (s or "").strip()

def _parse_dt(s: str | None) -> str:
    if not s:
        return ""
    try:
        # feedparser already parses RFC2822-ish; still normalize to ISO-like for our app
        dt = datetime(*feedparser._parse_date(s)[:6], tzinfo=UTC)
        return dt.isoformat()
    except Exception:
        return s

def _country_name(raw: str) -> str:
    name = _norm(html.unescape(raw))
    return NORMALIZE_COUNTRY_NAMES.get(name, name)

def _front_end_url(country: str) -> str | None:
    code = COUNTRY_TO_CODE.get(country)
    if not code:
        return None
    return f"{FRONT_BASE}/{code}"

def _severity_from_title(title: str) -> str | None:
    t = title.lower()
    if "red " in t or t.startswith("red ") or " red" in t:
        return "Red"
    if "amber " in t or t.startswith("amber ") or " amber" in t:
        return "Amber"
    if "orange " in t or t.startswith("orange ") or " orange" in t:
        return "Orange"
    if "yellow " in t or t.startswith("yellow ") or " yellow" in t:
        return "Yellow"
    return None

def _type_from_title(title: str) -> str:
    # e.g., "Amber warning of wind affecting ..."
    m = re.search(r"warning of\s+([A-Za-z /-]+?)\s+affecting", title, flags=re.I)
    return _norm(m.group(1) if m else "")

def _day_from_dates(onset_iso: str, until_iso: str) -> str:
    # crude day bucketing: if onset date is today UTC → "today" else "tomorrow"
    try:
        on = datetime.fromisoformat(onset_iso.replace("Z", "+00:00"))
        now = datetime.now(tz=UTC)
        if on.date() == now.date():
            return "today"
        return "tomorrow"
    except Exception:
        return "today"

def _count_incr(d: dict, day: str, level: str, typ: str):
    d.setdefault("by_day", {}).setdefault(day, {}).setdefault(level, {}).setdefault(typ, 0)
    d["by_day"][day][level][typ] += 1
    d.setdefault("by_type", {}).setdefault(level, {}).setdefault(typ, 0)
    d["by_type"][level][typ] += 1
    d["total"] = d.get("total", 0) + 1


# --------------------------------------------------------------------
# Fetch / Parse per-country RSS
# --------------------------------------------------------------------

def _fetch_country(country: str) -> dict:
    slug = COUNTRY_TO_RSS_SLUG.get(country)
    if not slug:
        return {"country": country, "alerts": {}, "counts": {"total": 0}, "published": ""}

    url = f"{RSS_BASE}/{slug}.xml"
    feed = feedparser.parse(url)

    # country header/title from the channel title; normalize it
    title = ""
    published = ""
    if feed and feed.get("feed"):
        title = _country_name(feed["feed"].get("title") or country)
        published = _parse_dt(feed["feed"].get("published") or feed["feed"].get("updated"))

    alerts_by_day: dict[str, list[dict]] = {"today": [], "tomorrow": []}
    counts = {"total": 0, "by_day": {}, "by_type": {}}

    for entry in feed.get("entries", []):
        e_title = _norm(entry.get("title"))
        e_desc  = _norm(entry.get("description"))
        e_link  = _norm(entry.get("link"))

        level = _severity_from_title(e_title) or _severity_from_title(e_desc) or ""
        typ   = _type_from_title(e_title or e_desc)

        # dates: RSS often puts validity in the description; fall back to published
        # Try to scrape "valid from ... to ..." from description; fallback to published times
        onset_raw, until_raw = "", ""
        m = re.search(r"valid from\s+(.+?)\s+to\s+(.+)$", e_desc, flags=re.I)
        if m:
            onset_raw, until_raw = _norm(m.group(1)), _norm(m.group(2))
        onset_iso = _parse_dt(onset_raw or entry.get("published") or entry.get("updated"))
        until_iso = _parse_dt(until_raw or entry.get("updated") or entry.get("published"))

        day = _day_from_dates(onset_iso, until_iso)

        alert = {
            "level": level or "",
            "type": typ or "",
            "from": onset_iso,
            "until": until_iso,
            "published": _parse_dt(entry.get("published")),
            "link": e_link,
            # renderer fallbacks
            "title": e_title,
            "summary": e_desc,
            # enclosure (color-coded image) if we have it
            "enclosure": _norm((entry.get("enclosures") or [{}])[0].get("href") if entry.get("enclosures") else entry.get("image")),
        }

        # Only keep severe levels used by the UI counters (but keep your choice)
        if level in LEVEL_WHITELIST:
            alerts_by_day[day].append(alert)
            _count_incr(counts, day, level, typ)

    out = {
        "country": country,
        "title": title or country,
        "name": title or country,
        "alerts": alerts_by_day,
        "counts": counts,
        "total_alerts": counts.get("total", 0),
        "published": published,
        "link": _front_end_url(title or country),  # ✅ uses COUNTRY_TO_CODE with UK
    }
    return out


# --------------------------------------------------------------------
# Public entrypoint used by the fetcher
# --------------------------------------------------------------------

def fetch(conf: Mapping[str, Any]) -> dict:
    """
    Returns {"entries": [...]} where each entry is a per-country dict
    understood by renderers_meteoalarm.py.
    If conf includes a subset of countries, respect it; otherwise fetch a default set.
    """
    countries: Sequence[str] = conf.get("countries") or list(COUNTRY_TO_RSS_SLUG.keys())

    entries = []
    now = time.time()
    for c in countries:
        # Normalize incoming conf names (if any are long-form)
        cname = _country_name(c)
        entry = _fetch_country(cname)
        entries.append(entry)

    # Optional: sort countries by display name
    entries.sort(key=lambda e: (e.get("name") or e.get("title") or ""))

    return {"entries": entries, "fetched_at": now}
