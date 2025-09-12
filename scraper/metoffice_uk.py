import asyncio
import streamlit as st
import httpx
import logging
import feedparser
import re

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

# Severity/type pattern
BUCKET_PAT = re.compile(r"\b(yellow|amber|red)\s+warning\s+of\s+([a-z/ ]+)", re.I)

def _norm(s: str) -> str:
    return (s or "").replace("\r", " ").replace("\n", " ").strip()

def _bucket_from_title(title: str) -> str:
    m = BUCKET_PAT.search(title or "")
    if not m:
        return _norm(title or "Alert")
    return f"{m.group(1).title()} – {m.group(2).strip().title()}"

def _parse_feed(content: bytes, region_label: str) -> list[dict]:
    parsed = feedparser.parse(content)
    out = []
    for e in parsed.entries:
        title = _norm(getattr(e, "title", ""))
        if re.search(r"\b(cancellation|cancelled|final)\b", title, re.I):
            continue
        out.append({
            "title": title,
            "summary": _norm(getattr(e, "summary", "") or getattr(e, "description", "")),
            "link": _norm(getattr(e, "link", "") or getattr(e, "id", "")),
            "published": _norm(getattr(e, "published", "") or getattr(e, "updated", "")),
            "region": region_label,
            "bucket": _bucket_from_title(title),
        })
    return out

@st.cache_data(ttl=60, show_spinner=False)
def scrape_metoffice_uk(conf: dict) -> dict:
    urls    = conf.get("urls", [])
    regions = conf.get("regions", [])
    entries = []
    for url, region in zip(urls, regions):
        try:
            resp = httpx.get(url, headers=HEADERS, timeout=15, follow_redirects=True)
            resp.raise_for_status()
            entries.extend(_parse_feed(resp.content, region))
        except Exception as e:
            logging.warning(f"[UK-MET] {region} {url} failed: {e}")
    return {"entries": entries, "source": "Met Office UK"}

async def scrape_metoffice_uk_async(conf: dict, client: httpx.AsyncClient) -> dict:
    urls    = conf.get("urls", [])
    regions = conf.get("regions", [])

    async def fetch_one(url, region):
        try:
            r = await client.get(url, headers=HEADERS, timeout=15, follow_redirects=True)
            r.raise_for_status()
            return _parse_feed(r.content, region)
        except Exception as e:
            logging.warning(f"[UK-MET async] {region} {url} failed: {e}")
            return []

    results = await asyncio.gather(
        *[fetch_one(u, r) for u, r in zip(urls, regions)]
    )

    entries = []
    for lst in results:
        entries.extend(lst)
    return {"entries": entries, "source": "Met Office UK"}
