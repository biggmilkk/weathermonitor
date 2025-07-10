import requests
import logging

def scrape(url="https://api.weather.gov/alerts/active"):
    headers = {"User-Agent": "WeatherMonitorApp (your@email.com)"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        feed = response.json()
    except Exception as e:
        logging.warning(f"[NWS SCRAPER ERROR] {e}")
        return {"entries": [], "error": str(e), "source": url}

    features = feed.get("features", [])
    logging.warning(f"[NWS DEBUG] Fetched {len(features)} features")

    entries = []
    for feature in features:
        props = feature.get("properties", {})
        entries.append({
            "title": props.get("headline", "No Title"),
            "summary": props.get("description", "")[:500],
            "link": props.get("web", ""),
            "published": props.get("effective", "")
        })

    return {"entries": entries, "source": url}
