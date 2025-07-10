import requests
import logging

def scrape(url="https://api.weather.gov/alerts/active"):
    headers = {"User-Agent": "WeatherMonitorApp (danisliew@gmail.com)"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        feed = response.json()
    except Exception as e:
        logging.warning(f"[NWS] Error fetching alerts from {url}: {e}")
        return {
            "feed_title": "NWS Alerts",
            "entries": [],
            "error": str(e),
            "source": url
        }

    features = feed.get("features", [])
    logging.warning(f"[NWS] Fetched {len(features)} alert features")

    entries = []
    for feature in features:
        props = feature.get("properties", {})
        entries.append({
            "title": props.get("headline", "No Title"),
            "summary": props.get("description", "")[:500],
            "link": props.get("web", ""),
            "published": props.get("effective", "")
        })

    if not entries:
        logging.warning("[NWS] No entries extracted")
    else:
        logging.warning(f"[NWS] Sample alert: {entries[0].get('title', '')}")

    return {
        "feed_title": "National Weather Service - Active Alerts",
        "entries": entries,
        "source": url
    }
