import feedparser
import logging
from bs4 import BeautifulSoup

AWARENESS_LEVELS = {
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


def scrape_meteoalarm(url="https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-europe"):
    try:
        feed = feedparser.parse(url)
        entries = []

        for entry in feed.entries:
            country = entry.get("title", "").replace("MeteoAlarm ", "").strip()
            pub_date = entry.get("published", "")
            description_html = entry.get("description", "")
            link = entry.get("link", "")

            soup = BeautifulSoup(description_html, "html.parser")  # Ensure HTML parser is used
            rows = soup.find_all("tr")

            alert_blocks = []
            for row in rows:
                cells = row.find_all("td")
                if len(cells) != 2:
                    continue

                cell = cells[0]
                print("DEBUG RAW CELL:", cell)
                print("data-awareness-level:", cell.get("data-awareness-level"))
                print("data-awareness-type:", cell.get("data-awareness-type"))
                if level in AWARENESS_LEVELS:
                    level_name = AWARENESS_LEVELS[level]
                    type_name = AWARENESS_TYPES.get(awt, f"Type {awt}")
                    time_info = cells[1].get_text(" ", strip=True)
                    alert_blocks.append(f"[{level_name}] {type_name} - {time_info}")

            if alert_blocks:
                summary = "\n".join(alert_blocks)
                entries.append({
                    "title": f"{country} Alerts",
                    "summary": summary[:500],
                    "link": link,
                    "published": pub_date,
                    "region": country,
                    "province": "Europe"
                })

        logging.warning(f"[METEOALARM DEBUG] Found {len(entries)} country alerts with orange/red levels")
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
