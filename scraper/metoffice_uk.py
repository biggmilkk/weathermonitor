import asyncio
import httpx
import logging
import re
from datetime import datetime
import feedparser
import streamlit as st

# Met Office UK-wide RSS
DEFAULT_URL = "https://www.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/UK"

# Your canonical region list (exact labels)
UK_REGIONS = [
    "Orkney & Shetland",
    "Highlands & Eilean Siar",
    "Grampian",
    "Strathclyde",
    "Central, Tayside & Fife",
    "SW Scotland, Lothian Borders",
    "Northern Ireland",
    "Wales",
    "North West England",
    "North East England",
    "Yorkshire & Humber",
    "West Midlands",
    "East Midlands",
    "East of England",
    "South West England",
    "London & South East England",
]

# Build a simple keyword map to your canonical regions.
# (Met Office item titles/summaries typically mention area strings.)
_REGION_KEYWORDS = {
    # Scotland blocks
    "orkney": "Orkney & Shetland",
    "shetland": "Orkney & Shetland",
    "eilean siar": "Highlands & Eilean Siar",
    "western isles": "Highlands & Eilean Siar",
    "outer hebrides": "Highlands & Eilean Siar",
    "highland": "Highlands & Eilean Siar",
    "grampian": "Grampian",
    "strathclyde": "Strathclyde",
    "tayside": "Central, Tayside & Fife",
    "fife": "Central, Tayside & Fife",
    "central": "Central, Tayside & Fife",
    "lothian": "SW Scotland, Lothian Borders",
    "borders": "SW Scotland, Lothian Borders",
    "scottish borders": "SW Scotland, Lothian Borders",
    "southwest scotland": "SW Scotland, Lothian Borders",
    "south west scotland": "SW Scotland, Lothian Borders",

    # Nations
    "northern ireland": "Northern Ireland",
    "wales": "Wales",

    # England regions
    "north west england": "North West England",
    "northwest england": "North West England",
    "north west": "North West England",
    "north east england": "North East England",
    "northeast england": "North East England",
    "north east": "North East England",
    "yorkshire": "Yorkshire & Humber",
    "humber": "Yorkshire & Humber",
    "west midlands": "West Midlands",
    "east midlands": "East Midlands",
    "east of england": "East of England",
    "east anglia": "East of England",
    "south west england": "South West England",
    "southwest england": "South West England",
    "south west": "South West England",
    "london & south east england": "London & South East England",
    "london and south east england": "London & South East England",
    "london": "London & South East England",
    "south east england": "London & South East England",
}

# Normalize warning "bucket" from the title, e.g.:
# "Yellow warning of Wind", "Amber warning of Rain", etc.
_BUCKET_PAT = re.compile(
    r"\b(yellow|amber|red)\s+warning\s+of\s+([a-z/ ]+)",
    re.IGNORECASE
)

# Browser-like headers to avoid 403s
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://www.metoffice.gov.uk/",
}

def _clean(s: str) -> str:
    return (s or "").replace("\r", " ").replace("\n", " ").strip()

def _find_regions(text: str) -> set[str]:
    t = text.lower()
    hits = set()
    for kw, region in _REGION_KEYWORDS.items():
        if kw in t:
            hits.add(region)
    return hits

def _bucket_from_title(title: str) -> str:
    """
    Returns a concise bucket label, e.g. 'Yellow – Wind', 'Amber – Rain', etc.
    Falls back to the raw title if no pattern matched.
    """
    m = _BUCKET_PAT.search(title or "")
    if not m:
        return (title or "Alert").strip()
    level = m.group(1).title()
    typ   = m.group(2).strip().title()
    return f"{level} – {typ}"

def _parse_published(pub: str) -> str:
    """
    Return a consistent published string. Feedparser may already give RFC822;
    we keep it as-is, your renderer will UTC-label it.
    """
    return _clean(pub)

def _expand_to_region_entries(e: dict) -> list[dict]:
    """
    An RSS item can mention multiple parts of the UK. Duplicate it per region hit,
    tagging each with that region and a compact 'bucket' (warning type).
    """
    title = _clean(getattr(e, "title", ""))
    summary = _clean(getattr(e, "summary", "")) or _clean(getattr(e, "description", ""))
    link = _clean(getattr(e, "link", "")) or _clean(getattr(e, "id", ""))
    published = _parse_published(getattr(e, "published", "") or getattr(e, "updated", "") or "")

    regions = _find_regions(f"{title} {summary}")
    if not regions:
        return []

    bucket = _bucket_from_title(title)

    out = []
    for region in regions:
        out.append({
            "title": title,
            "summary": summary,
            "link": link,
            "published": published,
            "region": region,          # top-level group key for renderer
            "bucket": bucket,          # 2nd-level toggle in grouped compact UI
        })
    return out

def _parse_feed(content: bytes) -> list[dict]:
    parsed = feedparser.parse(content)
    entries: list[dict] = []
    for e in parsed.entries:
        # Drop cancellations/final (if present)
        t = _clean(getattr(e, "title", ""))
        if re.search(r"\b(cancellation|cancelled|final)\b", t, re.IGNORECASE):
            continue
        entries.extend(_expand_to_region_entries(e))
    return entries

@st.cache_data(ttl=60, show_spinner=False)
def scrape_metoffice_uk(conf: dict) -> dict:
    url = conf.get("url", DEFAULT_URL)
    try:
        resp = httpx.get(url, headers=HEADERS, timeout=15, follow_redirects=True)
        resp.raise_for_status()
        entries = _parse_feed(resp.content)
    except Exception as e:
        logging.warning(f"[METOFFICE UK] Fetch failed: {e}")
        return {"entries": [], "error": str(e), "source": url}
    return {"entries": entries, "source": url}

async def scrape_metoffice_uk_async(conf: dict, client: httpx.AsyncClient) -> dict:
    url = conf.get("url", DEFAULT_URL)
    try:
        resp = await client.get(url, headers=HEADERS, timeout=15, follow_redirects=True)
        resp.raise_for_status()
        entries = _parse_feed(resp.content)
    except Exception as e:
        logging.warning(f"[METOFFICE UK] Async fetch failed: {e}")
        return {"entries": [], "error": str(e), "source": url}
    return {"entries": entries, "source": url}
