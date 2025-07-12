import feedparser
import logging
from dateutil import parser as dateparser
from datetime import datetime

# Synchronous scraper for WMO CMA CAP RSS feed (China)
def scrape_cma(conf):
    """
    Fetch and parse the CMA CAP RSS feed synchronously using feedparser.
    Skips expired and lifted/removed/resolve bulletins (in title or description).
    Uses the RSS <title> as the alert title and cap:areaDesc for region.
    Returns dict with 'entries' and 'source'.
    """
    url = conf.get('url')
    try:
        feed = feedparser.parse(url)
        entries = []

        for entry in feed.entries:
            # Extract raw title and description
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

            # Use the RSS title directly for alerts (preserves proper casing)
            title = raw_title.strip()
            if not title:
                continue

            # Link to full alert
            link = entry.get('link', '').strip()

            # Published time: cap:effective or pubDate
            published = entry.get('cap_effective') or entry.get('published', '')

            # Area/region: try CAP areaDesc or its variants
            region = (
                entry.get('cap_areaDesc') or entry.get('cap_areadesc') or
                entry.get('areaDesc') or entry.get('areadesc') or 'China'
            ).strip()

            entries.append({
                'title': title,
                'summary': summary.strip(),
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
