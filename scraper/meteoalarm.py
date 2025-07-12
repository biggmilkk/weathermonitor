import feedparser
import logging
import re
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

def scrape_meteoalarm(conf):
    try:
        url = conf.get("url", "https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-europe")
        old_cache = conf.get("cache", {})

        # Convert old cache (from list) to dict if needed
        if isinstance(old_cache, list):
            converted = {}
            for fp in old_cache:
                # Assign to generic key since we don’t know country
                converted.setdefault("global", []).append(fp)
            old_cache = converted

        feed = feedparser.parse(url)
        entries = []
        new_fingerprints = {}

        for entry in feed.entries:
            country = entry.get("title", "").replace("MeteoAlarm ", "").strip()
            pub_date = entry.get("published", "")
            description_html = entry.get("description", "")
            link = entry.get("link", "")

            soup = BeautifulSoup(description_html, "html.parser")
            rows = soup.find_all("tr")
            summary_today = []
            summary_tomorrow = []
            fingerprints = []

            current_section = "Today"

            for row in rows:
                header = row.find("th")
                if header:
                    text = header.get_text(strip=True).lower()
                    if "tomorrow" in text:
                        current_section = "Tomorrow"
                    elif "today" in text:
                        current_section = "Today"
                    continue

                cells = row.find_all("td")
                if len(cells) != 2:
                    continue

                level = cells[0].get("data-awareness-level")
                awt = cells[0].get("data-awareness-type")

                if not level or not awt:
                    match = re.search(r"awt:(\d+)\s+level:(\d+)", cells[0].get_text(strip=True))
                    if match:
                        awt, level = match.groups()

                if level not in AWARENESS_LEVELS:
                    continue

                level_name = AWARENESS_LEVELS[level]
                type_name = AWARENESS_TYPES.get(awt, f"Type {awt}")
                time_info = cells[1].get_text(" ", strip=True)

                fingerprint = f"{level}:{awt}:{time_info}"
                fingerprints.append(fingerprint)

                prev_fps = old_cache.get(country, [])
                is_new = fingerprint not in prev_fps
                prefix = "[NEW] " if is_new else ""
                alert_line = f"{prefix}[{level_name}] {type_name} - {time_info}"

                if current_section == "Tomorrow":
                    summary_tomorrow.append(alert_line)
                else:
                    summary_today.append(alert_line)

            if summary_today or summary_tomorrow:
                new_fingerprints[country] = fingerprints

                summary_lines = []
                if summary_today:
                    summary_lines.append("Today")
                    summary_lines.extend(summary_today)
                    summary_lines.append("")  # extra spacing

                if summary_tomorrow:
                    summary_lines.append("Tomorrow")
                    summary_lines.extend(summary_tomorrow)

                entries.append({
                    "title": f"{country} Alerts",
                    "summary": "\n".join(summary_lines),
                    "link": link,
                    "published": pub_date,
                    "region": country,
                    "province": "Europe"
                })

        logging.warning(f"[METEOALARM DEBUG] Parsed {len(entries)} alert blocks")
        return {
            "entries": entries,
            "source": url,
            "fingerprints": new_fingerprints  # to be stored in session on click
        }

    except Exception as e:
        logging.warning(f"[METEOALARM ERROR] Failed to fetch feed: {e}")
        return {
            "entries": [],
            "error": str(e),
            "source": conf.get("url"),
            "fingerprints": {}
        }
