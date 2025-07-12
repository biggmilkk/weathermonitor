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
        feed = feedparser.parse(url)
        entries = []

        for entry in feed.entries:
            country = entry.get("title", "").replace("MeteoAlarm ", "").strip()
            pub_date = entry.get("published", "")
            description_html = entry.get("description", "")
            link = entry.get("link", "")

            soup = BeautifulSoup(description_html, "html.parser")
            rows = soup.find_all("tr")
            summary_today = []
            summary_tomorrow = []

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

                # Extract 'From' and 'Until' times
                from_match = re.search(r"From:\s*</b>\s*<i>(.*?)</i>", str(cells[1]), re.IGNORECASE)
                until_match = re.search(r"Until:\s*</b>\s*<i>(.*?)</i>", str(cells[1]), re.IGNORECASE)

                from_time = from_match.group(1) if from_match else "?"
                until_time = until_match.group(1) if until_match else "?"

                line = f"[{level_name}] {type_name} - From: {from_time} Until: {until_time}"

                if current_section == "Tomorrow":
                    summary_tomorrow.append(line)
                else:
                    summary_today.append(line)

            if summary_today or summary_tomorrow:
                summary_lines = []

                if summary_today:
                    summary_lines.append("Today")
                    summary_lines.extend(summary_today)

                if summary_tomorrow:
                    summary_lines.append("Tomorrow")
                    summary_lines.extend(summary_tomorrow)

                summary_text = "\n".join(summary_lines)  # Ensure each alert is on its own line

                entries.append({
                    "title": f"{country} Alerts",
                    "summary": summary_text,
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
