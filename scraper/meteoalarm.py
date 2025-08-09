import streamlit as st
import feedparser
import logging
import re
from bs4 import BeautifulSoup
import httpx

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

# Default feed URL
DEFAULT_URL = "https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-europe"


def _parse_feed(feed):
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

            # Only recognized severity levels
            if level not in AWARENESS_LEVELS:
                continue
            level_name = AWARENESS_LEVELS[level]
            # Only Orange and Red
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

        # Skip if no relevant alerts
        if not alert_data["today"] and not alert_data["tomorrow"]:
            continue

        entries.append({
            "title": f"{country} Alerts",
            "summary": "",
            "alerts": alert_data,
            "link": link,
            "published": pub_date,
            "region": country,
            "province": "Europe",
        })
    return entries

@st.cache_data(ttl=60, show_spinner=False)
def scrape_meteoalarm(conf):
    """
    Synchronous wrapper: fetch and parse MeteoAlarm RSS.
    """
    url = conf.get("url", DEFAULT_URL)
    try:
        feed = feedparser.parse(url)
        entries = _parse_feed(feed)
        logging.warning(f"[METEOALARM DEBUG] Parsed {len(entries)} alerts (Orange/Red)")
        return {"entries": entries, "source": url}
    except Exception as e:
        logging.warning(f"[METEOALARM ERROR] Failed to fetch feed: {e}")
        return {"entries": [], "error": str(e), "source": url}

async def scrape_meteoalarm_async(conf, client: httpx.AsyncClient):
    """
    Async MeteoAlarm scraper using httpx.AsyncClient.
    """
    url = conf.get("url", DEFAULT_URL)
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        entries = _parse_feed(feed)
        logging.warning(f"[METEOALARM DEBUG] Parsed {len(entries)} alerts")
        return {"entries": entries, "source": url}
    except Exception as e:
        logging.warning(f"[METEOALARM ERROR] Fetch failed: {e}")
        return {"entries": [], "error": str(e), "source": url}
