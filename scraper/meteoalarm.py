import feedparser
import logging
from bs4 import BeautifulSoup
import json
import os

CACHE_FILE = "data/meteoalarm_cache.json"

AWARENESS_TYPES = {
    "1": "Wind",
    "2": "Snow/Ice",
    "3": "Thunderstorms",
    "4": "Fog",
    "5": "Extreme high temperature",
    "6": "Extreme low temperature",
    "7": "Coastal event",
    "8": "Forestfire",
    "9": "Avalanche",
    "10": "Rain",
    "12": "Flood",
    "13": "Rain - Flood",
}

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"[METEOALARM CACHE] Failed to load cache: {e}")
    return {}

def save_cache(cache):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except Exception as e:
        logging.warning(f"[METEOALARM CACHE] Failed to save cache: {e}")

def scrape_meteoalarm(url="https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-europe"):
    try:
        feed = feedparser.parse(url)
        cache = load_cache()
        updated_cache = {}
        entries = []

        for entry in feed.entries:
            country = entry.get("title", "").replace("MeteoAlarm ", "").strip()
            pub_date = entry.get("published", "")
            description_html = entry.get("description", "")
            link = entry.get("link", "")

            soup = BeautifulSoup(description_html, "html.parser")
            rows = soup.find_all("tr")
            alert_blocks = []

            for row in rows:
                cell = row.find("td", attrs={"data-awareness-level": ["3", "4"]})
                if not cell:
                    continue

                awt = cell.get("data-awareness-type", "")
                level = cell.get("data-awareness-level", "")
                cells = row.find_all("td")
                if len(cells) == 2:
                    hazard = AWARENESS_TYPES.get(awt, f"Type {awt}")
                    time_info = cells[1].get_text(" ", strip=True)
                    alert_blocks.append(f"{hazard} (Level {level}) - {time_info}")

            if alert_blocks:
                summary = "\n".join(alert_blocks)
                is_new = pub_date != cache.get(country)

                entries.append({
                    "title": f"{country} Alerts",
                    "summary": summary[:500],
                    "link": link,
                    "published": pub_date,
                    "region": country,
                    "province": "Europe",
                    "is_new": is_new
                })

                updated_cache[country] = pub_date

        save_cache(updated_cache)
        logging.warning(f"[METEOALARM DEBUG] Processed {len(entries)} orange/red country alerts")

        return {
            "entries": sorted(entries, key=lambda x: x["published"], reverse=True),
            "source": url
        }

    except Exception as e:
        logging.warning(f"[METEOALARM ERROR] Failed to fetch feed: {e}")
        return {
            "entries": [],
            "error": str(e),
            "source": url
        }
