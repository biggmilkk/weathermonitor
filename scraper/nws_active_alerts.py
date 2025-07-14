import streamlit as st
import requests
import logging

# Only include these event types
ALLOWED_EVENTS = {
    "Severe Thunderstorm Warning",
    "Flash Flood Warning",
    "Tornado Warning",
    "Flood Warning",
    "Extreme Heat Warning",
    "Air Quality Alert",
}

# Cache this scraper for 60 seconds to reduce repeated parsing and memory churn
@st.cache_data(ttl=60, show_spinner=False)
def scrape_nws(url="https://api.weather.gov/alerts/active"):
    headers = {
        "User-Agent": "WeatherMonitorApp (your@email.com)"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        feed = response.json()
        logging.warning(f"[NWS DEBUG] Successfully parsed {len(feed.get('features', []))} alerts")
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
            "published": props.get("effective", "")
        })

    return {"entries": entries, "source": url}
