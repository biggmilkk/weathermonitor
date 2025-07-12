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
    # ... (other awareness types)
}


def fetch_meteoalarm_feed(conf):
    url = conf.get("url")
    entries = []
    try:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            country = entry.get("title", "").replace("MeteoAlarm ", "")
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
                    continue

                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                level_match = re.search(r"awareness-level=\"(\d+)\"", cells[0].get("data-awareness-level", ""))
                awt = cells[0].get("data-awareness-type", "")
                level = level_match.group(1) if level_match else ""
                level_name = AWARENESS_LEVELS.get(level, f"Level {level}")
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

            # skip countries with no yellow-or-above alerts
            if not alert_data["today"] and not alert_data["tomorrow"]:
                continue

            summary_fallback = ""
            if not alert_data["today"] and not alert_data["tomorrow"]:
                summary_fallback = "No alerts available."

            entries.append({
                "title": f"{country} Alerts",
                "summary": summary_fallback,
                "alerts": alert_data,
                "link": link,
                "published": pub_date,
                "region": country,
                "province": "Europe"
            })

        logging.warning(f"[METEOALARM DEBUG] Parsed {len(entries)} alert summaries")
        return {
            "entries": entries,
            "source": url
        }

    except Exception as e:
        logging.warning(f"[METEOALARM ERROR] Failed to fetch feed: {e}")
        return {
            "entries": [],
            "error": str(e),
            "source": conf.get("url")
        }
