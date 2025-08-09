import streamlit as st
import requests
import logging
import httpx

# Only include these event types
ALLOWED_EVENTS = {
    "Severe Thunderstorm Warning",
    "Flash Flood Warning",
    "Tornado Warning",
    "Flood Warning",
    "Extreme Heat Warning",
    "Air Quality Alert",
}

@st.cache_data(ttl=60, show_spinner=False)
def scrape_nws(conf: dict) -> dict:
    """
    Synchronous scraper for NWS active alerts (expects conf dict with optional 'url').
    """
    url = conf.get("url", "https://api.weather.gov/alerts/active")
    headers = {"User-Agent": "WeatherMonitorApp (your@email.com)"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        feed = resp.json()
        logging.warning(f"[NWS DEBUG] Parsed {len(feed.get('features', []))} alerts")
    except Exception as e:
        logging.warning(f"[NWS SCRAPER ERROR] Fetch failed: {e}")
        return {"entries": [], "error": str(e), "source": url}

    entries = []
    for feature in feed.get("features", []):
        props = feature.get("properties", {}) or {}
        event_type = props.get("event", "")
        if event_type not in ALLOWED_EVENTS:
            continue
        entries.append({
            "title": props.get("headline", event_type),
            "summary": props.get("description", ""),
            "link": props.get("web", ""),
            "published": props.get("effective", ""),
        })
    return {"entries": entries, "source": url}

async def scrape_nws_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Async scraper for NWS active alerts using httpx.AsyncClient.
    """
    url = conf.get("url", "https://api.weather.gov/alerts/active")
    headers = {"User-Agent": "WeatherMonitorApp (your@email.com)"}
    try:
        resp = await client.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        feed = resp.json()
        logging.warning(f"[NWS DEBUG] Parsed {len(feed.get('features', []))} alerts")
    except Exception as e:
        logging.warning(f"[NWS SCRAPER ERROR] Async fetch failed: {e}")
        return {"entries": [], "error": str(e), "source": url}

    entries = []
    for feature in feed.get("features", []):
        props = feature.get("properties", {}) or {}
        event_type = props.get("event", "")
        if event_type not in ALLOWED_EVENTS:
            continue
        entries.append({
            "title": props.get("headline", event_type),
            "summary": props.get("description", ""),
            "link": props.get("web", ""),
            "published": props.get("effective", ""),
        })
    return {"entries": entries, "source": url}
