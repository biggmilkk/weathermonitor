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

# Country → 2-letter region code used by meteoalarm.org “live” page
COUNTRY_TO_CODE = {
    "Austria": "AT",
    "Belgium": "BE",
    "Bosnia and Herzegovina": "BA",
    "Bulgaria": "BG",
    "Croatia": "HR",
    "Cyprus": "CY",
    "Czechia": "CZ",
    "Czech Republic": "CZ",  # alias
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
    "United Kingdom": "UK",  # use "UK" if their frontend expects it
}

def _normalize_country(name: str) -> str:
    return " ".join((name or "").split())

def _country_link(country_name: str) -> str | None:
    code = COUNTRY_TO_CODE.get(_normalize_country(country_name))
    if not code:
        return None
    return f"https://meteoalarm.org/en/live/region/{code}"

def _summarize_counts(alert_data: dict) -> dict:
    """
    Build totals of severe/extreme (Orange/Red) by type and by level.
    Returns:
      {
        "total": int,
        "by_level": {"Orange": n, "Red": m},
        "by_type": {
           "Thunderstorms": {"Orange": x, "Red": y, "total": x+y},
           ...
        }
      }
    """
    summary = {
        "total": 0,
        "by_level": {"Orange": 0, "Red": 0},
        "by_type": {}
    }
    for day in ("today", "tomorrow"):
        for a in alert_data.get(day, []):
            lvl = a.get("level")
            typ = a.get("type")
            if lvl not in ("Orange", "Red") or not typ:
                continue
            summary["total"] += 1
            summary["by_level"][lvl] = summary["by_level"].get(lvl, 0) + 1
            bt = summary["by_type"].setdefault(typ, {"Orange": 0, "Red": 0, "total": 0})
            bt[lvl] += 1
            bt["total"] += 1
    return summary

def _parse_feed(feed):
    entries = []
    for entry in feed.entries:
        # e.g., title like "MeteoAlarm Austria"
        country = entry.get("title", "").replace("MeteoAlarm", "").strip()
        pub_date = entry.get("published", "")
        description_html = entry.get("description", "")
        default_link = entry.get("link", "")

        # Prefer country-specific live page if we can map it
        link = _country_link(country) or default_link

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

            # Only recognized severity levels, and only Orange/Red
            if level not in AWARENESS_LEVELS:
                continue
            level_name = AWARENESS_LEVELS[level]
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

        country_norm = _normalize_country(country)
        counts = _summarize_counts(alert_data)

        entries.append({
            "title": f"{country_norm} Alerts",
            "summary": "",
            "alerts": alert_data,
            "counts": counts,   # <— NEW: totals by type/level + overall
            "link": link,
            "published": pub_date,
            "region": country_norm,
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
        logging.warning(f"[METEOALARM DEBUG] Parsed {len(entries)} country entries (Orange/Red only)")
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
        logging.warning(f"[METEOALARM DEBUG] Parsed {len(entries)} country entries (async)")
        return {"entries": entries, "source": url}
    except Exception as e:
        logging.warning(f"[METEOALARM ERROR] Fetch failed: {e}")
        return {"entries": [], "error": str(e), "source": url}
