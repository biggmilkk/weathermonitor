import feedparser
import logging
from bs4 import BeautifulSoup

AWARENESS_LEVELS = {
    "1": "Green",
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


def scrape_meteoalarm(url="https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-europe"):
    try:
        feed = feedparser.parse(url)
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
                cells = row.find_all("td")
                if len(cells) != 2:
                    continue

                td = cells[0]
                level = td.get("data-awareness-level")
                awt = td.get("data-awareness-type")

                # Only keep orange (3) and red (4) alerts
                if level not in ("3", "4"):
                    continue

                level_name = AWARENESS_LEVELS.get(level, f"Level {level}")
                type_name = AWARENESS_TYPES.get(awt, f"Type {awt}")
                time_range = cells[1].get_text(" ", strip=True)

                alert_blocks.append(f"[{level_name}] {type_name} - {time_range}")

            if not alert_blocks:
                continue

            summary = "\n".join(alert_blocks)
            entries.append({
                "title": f"{country} Alerts",
                "summary": summary[:500],
                "link": link,
                "published": pub_date,
                "region": country,
                "province": "Europe"
            })

        logging.warning(f"[METEOALARM DEBUG] Found {len(entries)} entries with orange/red alerts")
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
