import requests
import logging

def scrape(url="https://api.weather.gov/alerts/active"):
    headers = {"User-Agent": "WeatherMonitorApp (you@example.com)"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        feed = response.json()
        logging.warning("[NWS DEBUG] Successfully fetched JSON with %d features", len(feed.get("features", [])))
    except Exception as e:
        logging.warning(f"[NWS SCRAPER ERROR] Exception: {e}")
        return {
            "feed_title": "NWS Alerts",
            "entries": [],
            "error": str(e),
            "source": url
        }

    try:
        entries = []
        for feature in feed.get("features", []):
            props = feature.get("properties", {})
            entries.append({
                "title": props.get("headline", "No Title"),
                "summary": props.get("description", "")[:500],
                "link": props.get("web", ""),
                "published": props.get("effective", "")
            })
        logging.warning("[NWS DEBUG] Returning %d parsed alerts", len(entries))
        return {
            "feed_title": "National Weather Service - Active Alerts",
            "entries": entries,
            "source": url
        }
    except Exception as parse_error:
        logging.warning(f"[NWS SCRAPER ERROR] Parsing failed: {parse_error}")
        return {
            "feed_title": "NWS Alerts",
            "entries": [],
            "error": str(parse_error),
            "source": url
        }
