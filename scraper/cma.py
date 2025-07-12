import aiohttp
import asyncio
import xml.etree.ElementTree as ET
import logging
from datetime import datetime

# Namespace for CAP v1.2
CAP_NS = {'cap': 'urn:oasis:names:tc:emergency:cap:1.2'}

async def fetch_and_parse_cma(session, url):
    """
    Fetches the CMA CAP feed at `url` and returns a list of alert dicts.
    Skips expired alerts based on cap:expires.
    """
    try:
        async with session.get(url, timeout=10) as resp:
            text = await resp.text()
            root = ET.fromstring(text)
            channel = root.find('channel')
            entries = []

            for item in channel.findall('item'):
                title_elem = item.find('cap:event', CAP_NS) or item.find('title')
                title = title_elem.text.strip() if title_elem is not None and title_elem.text else ""
                if not title:
                    continue

                # Skip expired alerts
                expires_elem = item.find('cap:expires', CAP_NS)
                if expires_elem is not None:
                    try:
                        exp_dt = datetime.fromisoformat(expires_elem.text.replace('Z', '+00:00'))
                        if exp_dt < datetime.utcnow():
                            continue
                    except Exception:
                        pass

                # Summary/description
                summary_elem = item.find('description')
                summary = summary_elem.text.strip() if summary_elem is not None and summary_elem.text else ""

                # Link
                link_elem = item.find('link')
                link = link_elem.text.strip() if link_elem is not None else ""

                # Published time: cap:effective or pubDate
                eff_elem = item.find('cap:effective', CAP_NS)
                pub_elem = item.find('pubDate')
                published = eff_elem.text if eff_elem is not None else (pub_elem.text if pub_elem is not None else "")

                # Area/region
                area_elem = item.find('cap:areaDesc', CAP_NS)
                region = area_elem.text.strip() if area_elem is not None and area_elem.text else "China"

                entries.append({
                    'title': title,
                    'summary': summary,
                    'link': link,
                    'published': published,
                    'region': region,
                    'province': ''
                })

            return entries

    except Exception as e:
        logging.warning(f"[CMA FETCH ERROR] {url} - {e}")
        return []

async def scrape_cma(conf):
    """
    Async entrypoint: fetches and parses the CMA RSS/CAP feed.
    Returns dict with 'entries' and 'source'.
    """
    url = conf.get('url')
    async with aiohttp.ClientSession() as session:
        entries = await fetch_and_parse_cma(session, url) if url else []
        logging.warning(f"[CMA DEBUG] Fetched {len(entries)} alerts from {url}")
        return {
            'entries': entries,
            'source': url
        }
