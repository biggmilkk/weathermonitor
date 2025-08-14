import streamlit as st
import feedparser
import logging
import re
from bs4 import BeautifulSoup
import httpx

# Alert severity levels mapped from MeteoAlarm numeric codes
AWARENESS_LEVELS = {
    "2": "Yellow",
    "3": "Orange",
    "4": "Red",
}

# Alert types mapped from MeteoAlarm numeric codes
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

# Country → 2-letter region code for the front-end “Read more” link
MA_COUNTRY_CODES = {
    "Austria": "AT",
    "Belgium": "BE",
    "Bosnia and Herzegovina": "BA",
    "Bulgaria": "BG",
    "Croatia": "HR",
    "Cyprus": "CY",
    "Czechia": "CZ",
    "Czech Republic": "CZ",  # alias seen on some lists
    "Denmark": "DK",
    "Estonia": "EE",
    "Finland": "FI",
    "France": "FR",
    "Germany": "DE",
    "Greece": "GR",
    "Hungary": "HU",
    "Iceland": "IS",
    "Ireland": "IE",
    "Israel": "IL",
    "Italy": "IT",
    "Latvia": "LV",
    "Lithuania": "LT",
    "Luxembourg": "LU",
    "Malta": "MT",
    "Moldova": "MD",
    "Montenegro": "ME",
    "Netherlands": "NL",
    "North Macedonia": "MK",
    "Norway": "NO",
    "Poland": "PL",
    "Portugal": "PT",
    "Romania": "RO",
    "Serbia": "RS",
    "Slovakia": "SK",
    "Slovenia": "SI",
    "Spain": "ES",
    "Sweden": "SE",
    "Switzerland": "CH",
    "Ukraine": "UA",
    "United Kingdom": "GB",
    "United Kingdom of Great Britain and Northern Ireland": "GB",
}

DEFAULT_URL = "https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-europe"


def _normalize_link(country_name: str, fallback_link: str) -> str:
    """
    Build the human-friendly front-end link for a country:
      https://meteoalarm.org/en/live/region/XX
    Falls back to the feed entry link if we don't recognize the country.
    """
    code = MA_COUNTRY_CODES.get(country_name)
    if code:
        return f"https://meteoalarm.org/en/live/region/{code}"
    return fallback_link or ""


def _parse_feed(feed):
    """
    Parse the MeteoAlarm RSS feed into entries, keeping only Orange/Red alerts.
    Adds:
      - 'total_alerts' (int): total Orange/Red across today+tomorrow
      - 'counts' (dict): per-day counts keyed by "LEVEL|TYPE"
        e.g., counts = {"today": {"Orange|Thunderstorms": 4}, "tomorrow": {...}}
      - 'link' points to the correct meteoalarm.org country page
    Keeps your existing 'alerts' structure for compatibility.
    """
    entries = []

    for entry in feed.entries:
        # Country name (e.g., "Austria")
        country = entry.get("title", "").replace("MeteoAlarm", "").strip()
        pub_date = entry.get("published", "")
        description_html = entry.get("description", "")
        entry_link = entry.get("link", "")

        soup = BeautifulSoup(description_html, "html.parser")
        rows = soup.find_all("tr")

        current_section = "today"
        alert_data = {"today": [], "tomorrow": []}

        # Per-day counts for (level, type) -> int. Use string key "LEVEL|TYPE" for portability.
        per_day_counts = {"today": {}, "tomorrow": {}}

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

            # Prefer attribute hints; fallback to regex in text when missing
            level = cells[0].get("data-awareness-level")
            awt = cells[0].get("data-awareness-type")
            if not level or not awt:
                m = re.search(r"awt:(\d+)\s+level:(\d+)", cells[0].get_text(strip=True))
                if m:
                    awt, level = m.groups()

            # Map severity; filter only Orange/Red
            if level not in AWARENESS_LEVELS:
                continue
            level_name = AWARENESS_LEVELS[level]
            if level_name not in ("Orange", "Red"):
                continue

            type_name = AWARENESS_TYPES.get(awt, f"Type {awt}")

            # Extract validity window (strings; renderer will format)
            from_match = re.search(r"From:\s*</b>\s*<i>(.*?)</i>", str(cells[1]), re.IGNORECASE)
            until_match = re.search(r"Until:\s*</b>\s*<i>(.*?)</i>", str(cells[1]), re.IGNORECASE)
            from_time = from_match.group(1) if from_match else "?"
            until_time = until_match.group(1) if until_match else "?"

            alert = {
                "level": level_name,
                "type": type_name,
                "from": from_time,
                "until": until_time,
            }
            alert_data[current_section].append(alert)

            # Increment per-day bucket count
            bucket_key = f"{level_name}|{type_name}"
            per_day_counts[current_section][bucket_key] = per_day_counts[current_section].get(bucket_key, 0) + 1

        # Skip if no Orange/Red alerts today or tomorrow
        if not alert_data["today"] and not alert_data["tomorrow"]:
            continue

        total_alerts = sum(per_day_counts["today"].values()) + sum(per_day_counts["tomorrow"].values())
        link = _normalize_link(country, entry_link)

        entries.append({
            "title": f"{country} Alerts",
            "summary": "",
            "alerts": alert_data,                 # kept as-is for your renderer
            "counts": {                           # NEW per-day (level|type) counts
                "today": per_day_counts["today"],
                "tomorrow": per_day_counts["tomorrow"],
            },
            "total_alerts": total_alerts,         # NEW total Orange/Red
            "link": link,                         # human-friendly country page
            "published": pub_date,
            "region": country,
            "province": "Europe",
            "country_code": MA_COUNTRY_CODES.get(country, ""),  # optional helper
        })

    # Alphabetical by country name (and only countries with active alerts are added)
    entries.sort(key=lambda e: e["region"].lower())
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
        logging.warning(f"[METEOALARM DEBUG] Parsed {len(entries)} countries with Orange/Red alerts")
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
        resp = await client.get(url, timeout=15)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        entries = _parse_feed(feed)
        logging.warning(f"[METEOALARM DEBUG] Parsed {len(entries)} countries with Orange/Red alerts (async)")
        return {"entries": entries, "source": url}
    except Exception as e:
        logging.warning(f"[METEOALARM ERROR] Fetch failed: {e}")
        return {"entries": [], "error": str(e), "source": url}
