import streamlit as st
import httpx
import logging
import xml.etree.ElementTree as ET

# Cache duration for BOM feeds
CACHE_TTL = 60  # seconds

# XML namespace (if any) can be added here


def _parse_bom_root(content: bytes, state: str) -> list[dict]:
    """
    Parse a BOM warnings XML blob and tag each entry with its state.
    """
    entries = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        logging.warning(f"[BOM PARSE ERROR] {state}: {e}")
        return entries

    for warn in root.findall('.//warning'):
        title = warn.findtext('headline') or ''
        summary = warn.findtext('description') or ''
        link = warn.findtext('link') or ''
        published = warn.findtext('sent') or ''
        entries.append({
            'state': state,
            'title': title.strip(),
            'summary': summary.strip(),
            'link': link.strip(),
            'published': published.strip(),
        })
    return entries

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def scrape_bom_multi(conf: dict) -> dict:
    """
    Synchronous scraper: fetches and parses all BOM state feeds.
    Expects conf to contain:
      - 'urls': list of feed URLs
      - 'states': list of matching state labels
    Returns dict with 'entries' list and 'source' label.
    """
    urls = conf.get('urls', [])
    states = conf.get('states', [])
    entries = []

    for url, state in zip(urls, states):
        try:
            resp = httpx.get(url, timeout=10)
            resp.raise_for_status()
            entries.extend(_parse_bom_root(resp.content, state))
        except Exception as e:
            logging.warning(f"[BOM FETCH ERROR] {state} {url}: {e}")
    logging.warning(f"[BOM DEBUG] Parsed {len(entries)} alerts across {len(states)} states")
    return {'entries': entries, 'source': 'Australia BOM'}

async def scrape_bom_multi_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Async scraper: uses shared httpx.AsyncClient to fetch all BOM state feeds.
    """
    urls = conf.get('urls', [])
    states = conf.get('states', [])
    entries = []

    for url, state in zip(urls, states):
        try:
            resp = await client.get(url, timeout=10)
            resp.raise_for_status()
            entries.extend(_parse_bom_root(resp.content, state))
        except Exception as e:
            logging.warning(f"[BOM FETCH ERROR] async {state} {url}: {e}")
    logging.warning(f"[BOM DEBUG] Async parsed {len(entries)} alerts across {len(states)} states")
    return {'entries': entries, 'source': 'Australia BOM'}
