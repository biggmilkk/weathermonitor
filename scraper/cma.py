import streamlit as st
import feedparser
import logging
from dateutil import parser as dateparser
from datetime import datetime

# Cache this scraper for 60 seconds to reduce repeated parsing and memory churn
@st.cache_data(ttl=60, show_spinner=False)
def scrape_cma(conf):
    """
    Fetch and parse the CMA CAP RSS feed synchronously using feedparser.
    Skips expired and lifted/removed/resolve bulletins (in title or description).
    Determines alert severity by looking for 'orange' or 'red' in the title,
    and skips all others (including blue and yellow).
    Uses the RSS <title> as the alert title and cap:areaDesc for region.
    Returns dict with 'entries' and 'source'.
    """
    url = conf.get('url')
    try:
        feed = feedparser.parse(url)
        entries = []

        for entry in feed.entries:
            raw_title = entry.get('title', '') or ''
            summary = entry.get('summary', '') or ''
            raw_lower = raw_title.lower()
            sum_lower = summary.lower()

            # Skip bulletins indicating lifting or resolution
            if any(keyword in raw_lower for keyword in ('lift', 'remove', 'resolve')):
                continue
            if any(keyword in sum_lower for keyword in ('lift', 'remove', 'resolve')):
                continue

            # Skip expired alerts based on cap:expires or expires
            expires = entry.get('cap_expires') or entry.get('expires')
            if expires:
                try:
                    exp_dt = dateparser.parse(expires)
                    if exp_dt < datetime.utcnow():
                        continue
                except Exception:
                    pass

            # Determine alert level by color keyword in title
            if 'orange' in raw_lower:
                level = 'Orange'
            elif 'red' in raw_lower:
                level = 'Red'
            else:
                # skip blue and yellow and any unknown
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

        logging.warning(f"[CMA DEBUG] Successfully parsed {len(entries)} alerts (Orange/Red only)")
        return {'entries': entries, 'source': url}

    except Exception as e:
        logging.warning(f"[CMA ERROR] Failed to fetch/parse CMA feed: {e}")
        return {'entries': [], 'error': str(e), 'source': url}
