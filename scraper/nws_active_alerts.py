import requests
import logging

def scrape(url="https://api.weather.gov/alerts/active"):
    headers = {"User-Agent": "WeatherMonitorApp (your@email.com)"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        feed = response.json()

        if not isinstance(feed, dict) or "features" not in feed:
            raise ValueError("Response JSON missing 'features' key")

        logging.warning(f"[NWS DEBUG] Successfully fetched JSON with {len(feed['features'])} features")

    except Exception as e:
        logging.warning(f"[NWS SCRAPER ERROR] Fetching failed: {e}")
        return {
            "feed_title": "NWS Alerts",
            "entries": [],
            "error": str(e),
            "source": url
        }

    try:
        entries = []
        for feature in feed["features"]:
            props = feature.get("properties")
            if not props:
                continue
            entries.append({
                "title": props.get("headline", "No Title"),
                "summary": props.get("description", "")[:500],
                "link": props.get("web", ""),
                "published": props.get("effective", "")
            })

        return {
            "feed_title": "National Weather Service - Active Alerts",
            "entries": entries,
            "source": url
        }

    except Exception as e:
        logging.warning(f"[NWS SCRAPER ERROR] Parsing failed: {e}")
        return {
            "feed_title": "NWS Alerts",
            "entries": [],
            "error": str(e),
            "source": url
        }
