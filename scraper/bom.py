import streamlit as st
import httpx
import logging
import feedparser
import re

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
    "Referer": "https://www.bom.gov.au/",
}

def _parse_feed(content: bytes, state: str) -> list[dict]:
    """
    Use feedparser to parse raw XML bytes and tag with state,
    skipping “Cancellation” or “Final” warnings.
    """
    parsed = feedparser.parse(content)
    entries = []
    for e in parsed.entries:
        title = getattr(e, "title", "").strip()
        # filter out cancellations & finals
        if re.search(r"\b(cancellation|final)\b", title, re.IGNORECASE):
            continue

        entries.append({
            "state":     state,
            "title":     title,
            "summary":   getattr(e, "summary", "").strip(),
            "link":      getattr(e, "link", "").strip(),
            "published": getattr(e, "published", "").strip(),
        })
    return entries

@st.cache_data(ttl=60, show_spinner=False)
def scrape_bom_multi(conf: dict) -> dict:
    """
    Synchronous fetch & parse of all BOM state feeds.
    """
    urls   = conf.get("urls", [])
    states = conf.get("states", [])
    entries = []

    for url, state in zip(urls, states):
        try:
            resp = httpx.get(
                url, headers=HEADERS, timeout=10, follow_redirects=True
            )
            resp.raise_for_status()
            entries.extend(_parse_feed(resp.content, state))
        except Exception as e:
            logging.warning(f"[BOM FETCH ERROR] sync {state} {url}: {e}")

    logging.warning(f"[BOM DEBUG] Parsed {len(entries)} alerts across {len(states)} states")
    return {"entries": entries, "source": "Australia BOM"}

async def scrape_bom_multi_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Asynchronous fetch & parse of all BOM state feeds.
    """
    urls   = conf.get("urls", [])
    states = conf.get("states", [])
    entries = []

    for url, state in zip(urls, states):
        try:
            resp = await client.get(
                url, headers=HEADERS, timeout=10, follow_redirects=True
            )
            resp.raise_for_status()
            entries.extend(_parse_feed(resp.content, state))
        except Exception as e:
            logging.warning(f"[BOM FETCH ERROR] async {state} {url}: {e}")

    logging.warning(f"[BOM DEBUG] Async parsed {len(entries)} alerts across {len(states)} states")
    return {"entries": entries, "source": "Australia BOM"}
