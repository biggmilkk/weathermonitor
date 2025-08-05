import httpx
import feedparser
import logging
from dateutil import parser as dateparser
from datetime import datetime
import streamlit as st


def _parse_entries(feed):
    """
    Shared parsing logic for CMA feeds: filters out lifts/resolves/expired and
    only keeps Orange/Red alerts.
    """
    entries = []
    for entry in feed.entries:
        raw_title = entry.get('title', '') or ''
        summary = entry.get('summary', '') or ''
        raw_lower = raw_title.lower()
        sum_lower = summary.lower()

        # Skip lifts, removes, resolves
        if any(k in raw_lower for k in ('lift', 'remove', 'resolve')):
            continue
        if any(k in sum_lower for k in ('lift', 'remove', 'resolve')):
            continue

        # Skip expired alerts
        expires = entry.get('cap_expires') or entry.get('expires')
        if expires:
            try:
                exp_dt = dateparser.parse(expires)
                if exp_dt < datetime.utcnow():
                    continue
            except Exception:
                pass

        # Only Orange or Red
        if 'orange' in raw_lower:
            level = 'Orange'
        elif 'red' in raw_lower:
            level = 'Red'
        else:
            continue

        title = raw_title.strip()
        if not title:
            continue

        link = entry.get('link', '').strip()
        published = entry.get('cap_effective') or entry.get('published', '')
        region = (
            entry.get('cap_areadesc') or entry.get('cap_areaDesc') or entry.get('areaDesc') or 'China'
        ).strip()

        entries.append({
            'title': title,
            'level': level,
            'summary': summary.strip(),
            'link': link,
            'published': published,
            'region': region,
            'province': ''
        })

    return entries


@st.cache_data(ttl=60, show_spinner=False)
def scrape_cma(conf: dict) -> dict:
    """
    Synchronous wrapper for the CMA feed scraper, with caching.
    """
    url = conf.get('url')
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)

        entries = _parse_entries(feed)
        logging.warning(f"[CMA DEBUG] Parsed {len(entries)} alerts (Orange/Red only)")
        return {'entries': entries, 'source': url}

    except Exception as e:
        logging.warning(f"[CMA ERROR] Failed to fetch/parse CMA feed: {e}")
        return {'entries': [], 'error': str(e), 'source': url}


async def scrape_cma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Async CMA scraper using a shared httpx.AsyncClient.
    """
    url = conf.get('url')
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)

        entries = _parse_entries(feed)
        logging.warning(f"[CMA DEBUG] Parsed {len(entries)} alerts (Orange/Red only)")
        return {'entries': entries, 'source': url}

    except Exception as e:
        logging.warning(f"[CMA ERROR] Failed to fetch/parse CMA feed async: {e}")
        return {'entries': [], 'error': str(e), 'source': url}
