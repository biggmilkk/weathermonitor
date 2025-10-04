# scraper/meteoalarm.py
from __future__ import annotations
import json
import re
import feedparser
import urllib.parse
import requests
from datetime import datetime, timezone as _UTC
from email.utils import parsedate_to_datetime
try:
    from dateutil import parser as dateparser
except Exception:
    dateparser = None

# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------

METEOALARM_RSS = "https://feeds.meteoalarm.org/rss/en/all.xml"
TIMEOUT = 20

COUNTRY_TO_CODE = {
    # Manual overrides for known differences
    "United Kingdom": "UK",  # override default GB code
    "United Kingdom of Great Britain and Northern Ireland": "UK",
}

NORMALIZE_COUNTRY_NAMES = {
    # Clean up country display names
    "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
}

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------

def _parse_dt(s: str | None) -> str:
    """Parse RSS dates safely; return ISO8601 UTC or the original string."""
    if not s:
        return ""
    # Try standard library first
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_UTC)
        return dt.astimezone(_UTC).isoformat()
    except Exception:
        pass
    # Try dateutil fallback
    if dateparser is not None:
        try:
            dt = dateparser.parse(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_UTC)
            return dt.astimezone(_UTC).isoformat()
        except Exception:
            pass
    return s


def _front_end_url(country: str) -> str:
    """Return the Meteoalarm live-region URL for a given country."""
    code = COUNTRY_TO_CODE.get(country)
    if not code:
        # fallback to ISO code logic (e.g., GB for UK)
        code = (country[:2].upper() if country else "EU")
    return f"https://meteoalarm.org/en/live/region/{code}"


def _country_name(raw: str) -> str:
    """Normalize country name."""
    if not raw:
        return ""
    raw = raw.strip()
    return NORMALIZE_COUNTRY_NAMES.get(raw, raw)


# --------------------------------------------------------------------
# Core fetch logic
# --------------------------------------------------------------------

def fetch() -> dict:
    """Fetch Meteoalarm RSS feed and return parsed structured data."""
    r = requests.get(METEOALARM_RSS, timeout=TIMEOUT)
    r.raise_for_status()
    feed = feedparser.parse(r.content)

    entries = []
    for e in feed.entries:
        # Region and country
        country = _country_name(e.get("cap_country") or e.get("author") or "")
        if not country:
            continue

        # Alerts per day (today/tomorrow)
        from_dt = _parse_dt(e.get("cap_effective") or e.get("published"))
        until_dt = _parse_dt(e.get("cap_expires") or e.get("updated"))

        alert = {
            "id": e.get("id") or e.get("guid") or e.get("link"),
            "type": e.get("cap_event") or e.get("title") or "",
            "level": e.get("cap_severity") or "",
            "from": from_dt,
            "until": until_dt,
            "published": _parse_dt(e.get("published")),
        }

        region = e.get("cap_areaDesc") or e.get("title") or country
        entry = {
            "country": country,
            "title": region,
            "name": region,
            "alerts": {"today": [alert]},
            "counts": {"total": 1, "by_day": {"today": {"total": 1}}},
            "total_alerts": 1,
            "published": _parse_dt(e.get("published")),
            "link": _front_end_url(country),
        }

        entries.append(entry)

    # Sort by name for UI stability
    entries.sort(key=lambda x: x.get("country") or "")

    return {"entries": entries, "fetched_at": datetime.now(_UTC).isoformat()}
