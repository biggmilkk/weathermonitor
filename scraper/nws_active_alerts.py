import requests
import logging

def scrape(url="https://api.weather.gov/alerts/active"):
    headers = {"User-Agent": "WeatherMonitorApp (your@email.com)"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        feed = response.json()
        logging.warning(f"[NWS DEBUG] Fetched {len(feed.get('features', []))} features")
    except Exception as e:
        logging.warning(f"[NWS SCRAPER ERROR] {e}")
        return {
            "feed_title": "NWS Alerts",
            "entries": [],
            "error": str(e),
            "source": url
        }

    entries = []
    for feature in feed.get("features", []):
        props = feature.get("properties", {})
        entries.append({
            "title": props.get("headline", "No Title"),
            "summary": props.get("description", "")[:500],
            "link": props.get("web", ""),
            "published": props.get("effective", "")
        })

    return {  # <-- this must not be missing!
        "feed_title": "National Weather Service - Active Alerts",
        "entries": entries,
        "source": url
    }
