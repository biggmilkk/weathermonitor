import streamlit as st
import httpx
import logging
import xml.etree.ElementTree as ET

# Common HTTP headers for BOM feeds (mimic a browser or known agent)
HEADERS = {
    "User-Agent": "WeatherMonitorApp/1.0 (contact@example.com)",
    "Accept": "application/xml, text/xml, */*;q=0.1",
    "Referer": "https://www.bom.gov.au/"
}

def _parse_bom_root(content: bytes, state: str) -> list[dict]:
    """
    Parse BOM XML content and tag each alert with its state.
    """
    entries = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        logging.warning(f"[BOM PARSE ERROR] {state} - {e}")
        return entries

    # BOM uses <warning> elements under the root
    for warning in root.findall('.//warning'):
        title = warning.findtext('headline') or ''
        summary = warning.findtext('description') or ''
        link = warning.findtext('link') or ''
        sent = warning.findtext('sent') or ''
        entries.append({
            'title': title.strip(),
            'summary': summary.strip(),
            'link': link.strip(),
            'published': sent.strip(),
            'state': state
        })
    return entries

@st.cache_data(ttl=60, show_spinner=False)
def scrape_bom_multi(conf: dict) -> dict:
    """
    Synchronous scraper for all BOM state feeds.
    Returns {'entries': [...], 'source': 'Australia BOM'}
    """
    urls = conf.get('urls', [])
    states = conf.get('states', [])
    entries = []
    for url, state in zip(urls, states):
        try:
            resp = httpx.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            entries.extend(_parse_bom_root(resp.content, state))
        except Exception as e:
            logging.warning(f"[BOM FETCH ERROR] sync {state} {url}: {e}")
    return {'entries': entries, 'source': 'Australia BOM'}

async def scrape_bom_multi_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Asynchronous scraper for all BOM state feeds using shared HTTP client.
    Returns {'entries': [...], 'source': 'Australia BOM'}
    """
    urls = conf.get('urls', [])
    states = conf.get('states', [])
    entries = []
    for url, state in zip(urls, states):
        try:
            resp = await client.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            entries.extend(_parse_bom_root(resp.content, state))
        except Exception as e:
            logging.warning(f"[BOM FETCH ERROR] async {state} {url}: {e}")
    return {'entries': entries, 'source': 'Australia BOM'}
