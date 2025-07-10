import requests
import logging

# ✅ Only keep alerts that match these event types
ALLOWED_EVENTS = {
    "Severe Thunderstorm Warning",
    "Evacuation Immediate",
    "Flood Warning",
    "Extreme Heat Warning",
    "Heat Advisory",
    "Flood Advisory",
    "Dense Fog Advisory",
    "Flood Watch",
    "Extreme Heat Watch",
    "Air Quality Alert"
}

def scrape(url="https://api.weather.gov/alerts/active"):
    headers = {
        "User-Agent": "WeatherMonitorApp (your@email.com)"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        feed = response.json()
        logging.warning(f"[NWS DEBUG] Successfully fetched JSON with {len(feed.get('features', []))} features")
    except Exception as e:
        logging.warning(f"[NWS SCRAPER ERROR] Fetch failed: {e}")
        return {
            "feed_title": "NWS Alerts",
            "entries": [],
            "error": str(e),
            "source": url
        }

    entries = []
    try:
        for feature in feed.get("features", []):
            props = feature.get("properties", {})
            if not isinstance(props, dict):
                continue

            event_type = props.get("event", "")
            if event_type not in ALLOWED_EVENTS:
                continue  # Skip anything not explicitly allowed

            entries.append({
                "title": props.get("headline", event_type or "No Title"),
                "summary": props.get("description", "")[:500],
                "link": props.get("web", ""),
                "published": props.get("effective", "")
            })

    except Exception as parse_err:
        logging.warning(f"[NWS SCRAPER ERROR] Parsing failed: {parse_err}")
        return {
            "feed_title": "NWS Alerts",
            "entries": [],
            "error": str(parse_err),
            "source": url
        }

    return {
        "feed_title": "National Weather Service - Active Alerts",
        "entries": entries,
        "source": url
    }
