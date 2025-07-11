import feedparser
import logging
import re
import json
import os
from bs4 import BeautifulSoup

AWARENESS_LEVELS = {
    "2": "Yellow",
    "3": "Orange",
    "4": "Red",
}

AWARENESS_TYPES = {
    "1": "Wind",
    "2": "Snow/Ice",
    "3": "Thunderstorms",
    "4": "Fog",
    "5": "Extreme high temperature",
    "6": "Extreme low temperature",
    "7": "Coastal event",
    "8": "Forest fire",
    "9": "Avalanche",
    "10": "Rain",
    "12": "Flood",
    "13": "Rain/Flood",
}

CACHE_PATH = os.path.join("data", "meteoalarm_cache.json")

def load_cache():
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r") as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"[METEOALARM CACHE] Failed to load cache: {e}")
    return {}

def save_cache(cache):
    try:
        with open(CACHE_PATH, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        logging.warning(f"[METEOALARM CACHE] Failed to save cache: {e}")

def scrape_meteoalarm(url="https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-europe"):
    try:
        feed = feedparser.parse(url)
        entries = []
        old_cache = load_cache()
        new_cache = {}

        for entry in feed.entries:
            country = entry.get("title", "").replace("MeteoAlarm ", "").strip()
            pub_date = entry.get("published", "")
            description_html = entry.get("description", "")
            link = entry.get("link", "")

            soup = BeautifulSoup(description_html, "html.parser")
            rows = soup.find_all("tr")
            summary_lines = []
            fingerprint_blocks = []

            for row in rows:
                cells = row.find_all("td")
                if len(cells) != 2:
                    continue

                cell = cells[0]
                level = cell.get("data-awareness-level")
                awt = cell.get("data-awareness-type")

                if not level or not awt:
                    match = re.search(r"awt:(\d+)\s+level:(\d+)", cell.get_text(strip=True))
                    if match:
                        awt, level = match.groups()

                if level not in AWARENESS_LEVELS:
                    continue

                level_name = AWARENESS_LEVELS[level]
                type_name = AWARENESS_TYPES.get(awt, f"Type {awt}")
                time_info = cells[1].get_text(" ", strip=True)

                fingerprint = f"{level}:{awt}:{time_info}"
                fingerprint_blocks.append(fingerprint)

                prev_fps = old_cache.get(country, [])
                prefix = "[NEW] " if fingerprint not in prev_fps else ""
                summary_lines.append(f"{prefix}[{level_name}] {type_name} - {time_info}")

            if summary_lines:
                new_cache[country] = fingerprint_blocks
                entries.append({
                    "title": f"{country} Alerts",
                    "summary": "\n".join(summary_lines),
                    "link": link,
                    "published": pub_date,
                    "region": country,
                    "province": "Europe"
                })
        logging.warning(f"[METEOALARM DEBUG] New cache contents: {json.dumps(new_cache, indent=2)}")
        save_cache(new_cache)

        logging.warning(f"[METEOALARM DEBUG] Found {len(entries)} country alerts with yellow/orange/red levels")
        return {
            "entries": entries,
            "source": url
        }

    except Exception as e:
        logging.warning(f"[METEOALARM ERROR] Failed to fetch feed: {e}")
        return {
            "entries": [],
            "error": str(e),
            "source": url
        }
