import feedparser
import logging
import re
from bs4 import BeautifulSoup

# Alert severity levels mapped from MeteoAlarm codes
AWARENESS_LEVELS = {
    "2": "Yellow",
    "3": "Orange",
    "4": "Red",
}

# Alert types mapped from MeteoAlarm codes
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
    """
    Fetch and parse the MeteoAlarm RSS feed for European countries.
    Only Orange and Red alerts are retained; countries without any are skipped.
    Returns a dict with 'entries' (list of alert dicts) and 'source' URL.
    """
    url = conf.get(
        "url", "https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-europe"
    )
    try:
        feed = feedparser.parse(url)
        entries = []

        for entry in feed.entries:
            country = entry.get("title", "").replace("MeteoAlarm", "").strip()
            pub_date = entry.get("published", "")
            description_html = entry.get("description", "")
            link = entry.get("link", "")

            soup = BeautifulSoup(description_html, "html.parser")
            rows = soup.find_all("tr")

            current_section = "today"
            alert_data = {"today": [], "tomorrow": []}

            for row in rows:
                header = row.find("th")
                if header:
                    text = header.get_text(strip=True).lower()
                    if "tomorrow" in text:
                        current_section = "tomorrow"
                    elif "today" in text:
                        current_section = "today"
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

                # Only keep recognized severity levels
                if level not in AWARENESS_LEVELS:
                    continue
                level_name = AWARENESS_LEVELS[level]
                # Filter: only Orange and Red
                if level_name not in ["Orange", "Red"]:
                    continue

                type_name = AWARENESS_TYPES.get(awt, f"Type {awt}")

                from_match = re.search(r"From:\s*</b>\s*<i>(.*?)</i>", str(cells[1]), re.IGNORECASE)
                until_match = re.search(r"Until:\s*</b>\s*<i>(.*?)</i>", str(cells[1]), re.IGNORECASE)
                from_time = from_match.group(1) if from_match else "?"
                until_time = until_match.group(1) if until_match else "?"

                alert_data[current_section].append({
                    "level": level_name,
                    "type": type_name,
                    "from": from_time,
                    "until": until_time,
                })

            # Skip country if no Orange/Red alerts present
            if not alert_data["today"] and not alert_data["tomorrow"]:
                continue

            entries.append({
                "title": f"{country} Alerts",
                "summary": "",  # structured alerts
                "alerts": alert_data,
                "link": link,
                "published": pub_date,
                "region": country,
                "province": "Europe",
            })

        logging.warning(f"[METEOALARM DEBUG] Parsed {len(entries)} alert summaries (Orange/Red only)")
        return {"entries": entries, "source": url}

    except Exception as e:
        logging.warning(f"[METEOALARM ERROR] Failed to fetch feed: {e}")
        return {"entries": [], "error": str(e), "source": url}
