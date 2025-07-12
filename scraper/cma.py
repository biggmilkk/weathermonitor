import feedparser
import logging
from dateutil import parser as dateparser
from datetime import datetime

# Synchronous scraper for WMO CMA CAP RSS feed (China)
def scrape_cma(conf):
    """
    Fetch and parse the CMA CAP RSS feed synchronously using feedparser.
    Skips expired and lifted/removed/resolve bulletins.
    Returns dict with 'entries' and 'source'.
    """
    url = conf.get('url')
    try:
        feed = feedparser.parse(url)
        entries = []

        for entry in feed.entries:
            # Skip bulletins indicating lifting or resolution
            raw_title = entry.get('title', '')
            raw_lower = raw_title.lower()
            if any(keyword in raw_lower for keyword in ('lift', 'remove', 'resolve')):
                continue

            # Skip expired alerts based on cap:expires
            expires = entry.get('cap_expires')
            if expires:
                try:
                    exp_dt = dateparser.parse(expires)
                    if exp_dt < datetime.utcnow():
                        continue
                except Exception:
                    pass

            # Determine alert title: prefer CAP event, fallback to RSS title
            title = entry.get('cap_event', raw_title).strip()
            if not title:
                continue

            # Summary/description
            summary = entry.get('summary', '').strip()
            # Link to full alert
            link = entry.get('link', '').strip()

            # Published time: cap:effective or pubDate
            published = entry.get('cap_effective') or entry.get('published', '')

            # Area/region
            region = entry.get('cap_areaDesc', 'China').strip()

            entries.append({
                'title': title,
                'summary': summary,
                'link': link,
                'published': published,
                'region': region,
                'province': ''
            })

        logging.warning(f"[CMA DEBUG] Parsed {len(entries)} CMA alerts from {url}")
        return {'entries': entries, 'source': url}

    except Exception as e:
        logging.warning(f"[CMA ERROR] Failed to fetch/parse CMA feed: {e}")
        return {'entries': [], 'error': str(e), 'source': url}
