import feedparser
import logging
from bs4 import BeautifulSoup

def scrape(url="https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-europe"):
    try:
        feed = feedparser.parse(url)
        entries = []

        for entry in feed.entries:
            country = entry.get("title", "").replace("MeteoAlarm ", "").strip()
            pub_date = entry.get("published", "")
            description_html = entry.get("description", "")
            link = entry.get("link", "")

            # Parse HTML description
            soup = BeautifulSoup(description_html, "html.parser")
            rows = soup.find_all("tr")
            alert_blocks = []

            for row in rows:
                cells = row.find_all("td")
                if len(cells) == 2:
                    awt_text = cells[0].get_text(strip=True)
                    time_info = cells[1].get_text(" ", strip=True)
                    alert_blocks.append(f"{awt_text} - {time_info}")

            summary = "\n".join(alert_blocks) or "No active alerts."

            entries.append({
                "title": f"{country} Alerts",
                "summary": summary[:500],
                "link": link,
                "published": pub_date,
                "region": country,
                "province": "Europe"  # Optional placeholder
            })

        logging.warning(f"[METEOALARM DEBUG] Successfully fetched {len(entries)} country alerts")
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
