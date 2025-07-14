import streamlit as st
import requests
import xml.etree.ElementTree as ET
import logging
import re
from datetime import datetime

# Cached entry point for Streamlit: synchronous fetch
def scrape_ec(sources):
    """
    Fetch and parse Environment Canada Atom feeds for multiple regions synchronously.
    Cached for 60 seconds to minimize repeated network and XML parsing.
    Returns a dict with 'entries' (list) and 'source' identifier.
    """
    entries = []
    for region in sources:
        url = region.get("ATOM URL")
        if not url:
            continue
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
        except Exception as e:
            logging.warning(f"[EC FETCH ERROR] {url} - {e}")
            continue

        for item in root.findall("{http://www.w3.org/2005/Atom}entry"):
            title_elem = item.find("{http://www.w3.org/2005/Atom}title")
            summary_elem = item.find("{http://www.w3.org/2005/Atom}summary")
            link_elem = item.find("{http://www.w3.org/2005/Atom}link")
            pub_elem = item.find("{http://www.w3.org/2005/Atom}published")

            title = title_elem.text or "" if title_elem is not None else ""
            if not title:
                continue

            up = title.upper()
            # skip ended or no-alert
            if 'ENDED' in up or up.startswith('NO ALERT'):
                continue

            # only warnings or specific watch
            alert_type = re.split(r",\s*", title)[0].strip().upper()
            if 'WARNING' not in alert_type and alert_type != 'SEVERE THUNDERSTORM WATCH':
                continue

            raw_pub = pub_elem.text or "" if pub_elem is not None else ""
            try:
                dt = datetime.strptime(raw_pub, "%Y-%m-%dT%H:%M:%SZ")
                pub_iso = dt.isoformat()
            except Exception:
                pub_iso = raw_pub

            entries.append({
                "title": alert_type,
                "summary": summary_elem.text[:500] if summary_elem is not None else "",
                "link": link_elem.attrib.get("href", "") if link_elem is not None else "",
                "published": pub_iso,
                "region": region.get("Region Name", ""),
                "province": region.get("Province-Territory", "")
            })
    logging.warning(f"[EC DEBUG] Parsed {len(entries)} EC alerts")
    return {"entries": entries, "source": "Environment Canada"}
