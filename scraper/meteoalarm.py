import streamlit as st
import feedparser
import logging
import re
from bs4 import BeautifulSoup
import httpx
import asyncio

# ------------ Severity & type maps (unchanged) ------------
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

DEFAULT_URL = "https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-europe"

# ------------ Country code for front-end "Read more" ------------
COUNTRY_TO_CODE = {
    "Austria": "AT","Belgium": "BE","Bosnia and Herzegovina": "BA","Bulgaria": "BG",
    "Croatia": "HR","Cyprus": "CY","Czechia": "CZ","Czech Republic": "CZ","Denmark": "DK",
    "Estonia": "EE","Finland": "FI","France": "FR","Germany": "DE","Greece": "GR",
    "Hungary": "HU","Iceland": "IS","Ireland": "IE","Israel": "IL","Italy": "IT",
    "Latvia": "LV","Lithuania": "LT","Luxembourg": "LU","Malta": "MT","Moldova": "MD",
    "Montenegro": "ME","Netherlands": "NL","North Macedonia": "MK",
    "Republic of North Macedonia": "MK","Norway": "NO","Poland": "PL","Portugal": "PT",
    "Romania": "RO","Serbia": "RS","Slovakia": "SK","Slovenia": "SI","Spain": "ES",
    "Sweden": "SE","Switzerland": "CH","Ukraine": "UA",
    "United Kingdom": "GB","United Kingdom of Great Britain and Northern Ireland": "GB",
}

# ------------ Country RSS slug (from your list) ------------
COUNTRY_TO_RSS_SLUG = {
    "Austria": "austria",
    "Belgium": "belgium",
    "Bosnia and Herzegovina": "bosnia-herzegovina",
    "Bulgaria": "bulgaria",
    "Croatia": "croatia",
    "Cyprus": "cyprus",
    "Czechia": "czechia",
    "Czech Republic": "czechia",
    "Denmark": "denmark",
    "Estonia": "estonia",
    "Finland": "finland",
    "France": "france",
    "Germany": "germany",
    "Greece": "greece",
    "Hungary": "hungary",
    "Iceland": "iceland",
    "Ireland": "ireland",
    "Israel": "israel",
    "Italy": "italy",
    "Latvia": "latvia",
    "Lithuania": "lithuania",
    "Luxembourg": "luxembourg",
    "Malta": "malta",
    "Moldova": "moldova",
    "Montenegro": "montenegro",
    "Netherlands": "netherlands",
    "North Macedonia": "republic-of-north-macedonia",
    "Republic of North Macedonia": "republic-of-north-macedonia",
    "Norway": "norway",
    "Poland": "poland",
    "Portugal": "portugal",
    "Romania": "romania",
    "Serbia": "serbia",
    "Slovakia": "slovakia",
    "Slovenia": "slovenia",
    "Spain": "spain",
    "Sweden": "sweden",
    "Switzerland": "switzerland",
    "Ukraine": "ukraine",
    "United Kingdom": "united-kingdom",
    "United Kingdom of Great Britain and Northern Ireland": "united-kingdom",
}

def _front_end_url(country_name: str) -> str | None:
    code = COUNTRY_TO_CODE.get(country_name)
    return f"https://meteoalarm.org/en/live/region/{code}" if code else None

def _country_rss_url(country_name: str) -> str | None:
    slug = COUNTRY_TO_RSS_SLUG.get(country_name)
    return f"https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-{slug}" if slug else None

# ------------ Common RSS row parser (used for both EU and per-country feeds) ------------
def _parse_table_rows(description_html: str):
    soup = BeautifulSoup(description_html, "html.parser")
    rows = soup.find_all("tr")
    current = "today"
    items = {"today": [], "tomorrow": []}

    for row in rows:
        header = row.find("th")
        if header:
            txt = header.get_text(strip=True).lower()
            if "tomorrow" in txt:
                current = "tomorrow"
            elif "today" in txt:
                current = "today"
            continue

        cells = row.find_all("td")
        if len(cells) != 2:
            continue

        level = cells[0].get("data-awareness-level")
        awt   = cells[0].get("data-awareness-type")
        if not level or not awt:
            m = re.search(r"awt:(\d+)\s+level:(\d+)", cells[0].get_text(strip=True))
            if m:
                awt, level = m.groups()

        if level not in AWARENESS_LEVELS:
            continue
        level_name = AWARENESS_LEVELS[level]
        # keep only Orange/Red
        if level_name not in ("Orange", "Red"):
            continue

        type_name = AWARENESS_TYPES.get(awt, f"Type {awt}")

        from_m = re.search(r"From:\s*</b>\s*<i>(.*?)</i>", str(cells[1]), re.IGNORECASE)
        until_m = re.search(r"Until:\s*</b>\s*<i>(.*?)</i>", str(cells[1]), re.IGNORECASE)
        from_time = from_m.group(1) if from_m else "?"
        until_time = until_m.group(1) if until_m else "?"

        items[current].append({
            "level": level_name,
            "type": type_name,
            "from": from_time,
            "until": until_time,
        })
    return items

# ------------ Europe aggregate parser (your existing behavior) ------------
def _parse_europe(feed):
    entries = []
    for entry in feed.entries:
        country = entry.get("title", "").replace("MeteoAlarm", "").strip()  # e.g., "Austria"
        pub_date = entry.get("published", "")
        description_html = entry.get("description", "")
        alerts_by_day = _parse_table_rows(description_html)

        # Skip if no Orange/Red alerts
        if not alerts_by_day["today"] and not alerts_by_day["tomorrow"]:
            continue

        entries.append({
            "title": f"{country} Alerts",
            "summary": "",
            "alerts": alerts_by_day,
            "link": _front_end_url(country) or entry.get("link", ""),
            "published": pub_date,
            "region": country,         # used for alphabetical sort
            "province": "Europe",
            # counts will be injected by augmentation step
        })
    return entries

# ------------ Count helpers for per-country feeds ------------
def _count_from_country_feed(fp_obj) -> dict:
    """
    Returns:
      {
        "total": <int>,                     # total across today+tomorrow
        "by_type": { "<Type>": {"Orange": n, "Red": m, "total": n+m}, ... },
        "by_day":  {
           "today":    { "<Type>|<Level>": n, ... },
           "tomorrow": { "<Type>|<Level>": n, ... },
        }
      }
    Counts each table row (which typically corresponds to an affected-region bucket).
    """
    counts = {"total": 0, "by_type": {}, "by_day": {"today": {}, "tomorrow": {}}}
    for entry in fp_obj.entries:
        desc = entry.get("description", "")
        per_day = _parse_table_rows(desc)
        for day in ("today", "tomorrow"):
            for it in per_day[day]:
                level = it.get("level", "")
                typ   = it.get("type", "")
                if level not in ("Orange", "Red"):
                    continue
                # per-day key
                k = f"{level}|{typ}"
                counts["by_day"].setdefault(day, {})
                counts["by_day"][day][k] = counts["by_day"][day].get(k, 0) + 1
                # per-type summary
                bucket = counts["by_type"].setdefault(typ, {"Orange": 0, "Red": 0, "total": 0})
                bucket[level] += 1
                bucket["total"] += 1
                counts["total"] += 1
    return counts

# ------------ Public API ------------
@st.cache_data(ttl=60, show_spinner=False)
def scrape_meteoalarm(conf):
    """
    Synchronous: fetch Europe feed, then (sequentially) augment per-country counts.
    """
    url = conf.get("url", DEFAULT_URL)
    try:
        eu_fp = feedparser.parse(url)
        base_entries = _parse_europe(eu_fp)
    except Exception as e:
        logging.warning(f"[METEOALARM ERROR] Failed to fetch EU feed: {e}")
        return {"entries": [], "error": str(e), "source": url}

    # augment counts sequentially
    for item in base_entries:
        country = item.get("region", "")
        rss_url = _country_rss_url(country)
        if not rss_url:
            continue
        try:
            fp = feedparser.parse(rss_url)
            counts = _count_from_country_feed(fp)
            item["counts"] = counts
            item["total_alerts"] = counts.get("total", 0)
        except Exception as e:
            logging.warning(f"[METEOALARM WARN] Count fetch failed for {country}: {e}")

    # Only countries that still have alerts (already filtered), sort A–Z
    base_entries.sort(key=lambda x: (x.get("region") or x.get("title","")).lower())
    return {"entries": base_entries, "source": url}

async def scrape_meteoalarm_async(conf, client: httpx.AsyncClient):
    """
    Async: fetch EU feed, then concurrently fetch each present country's RSS for counts.
    """
    url = conf.get("url", DEFAULT_URL)
    try:
        # main EU feed via httpx for consistency
        eu_resp = await client.get(url, timeout=15)
        eu_resp.raise_for_status()
        eu_fp = feedparser.parse(eu_resp.content)
        base_entries = _parse_europe(eu_fp)
    except Exception as e:
        logging.warning(f"[METEOALARM ERROR] EU fetch failed: {e}")
        return {"entries": [], "error": str(e), "source": url}

    async def fetch_counts(country: str) -> tuple[str, dict] | None:
        rss_url = _country_rss_url(country)
        if not rss_url:
            return None
        try:
            r = await client.get(rss_url, timeout=12)
            r.raise_for_status()
            fp = feedparser.parse(r.content)
            return (country, _count_from_country_feed(fp))
        except Exception as e:
            logging.warning(f"[METEOALARM WARN] Count fetch failed for {country}: {e}")
            return None

    # Fire off all country count requests (unbounded for speed, as you prefer)
    tasks = [fetch_counts(item.get("region","")) for item in base_entries]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    # Merge counts back
    counts_by_country = {c: cnt for c, cnt in results if c and cnt}  # drop Nones
    for item in base_entries:
        ctry = item.get("region","")
        cnts = counts_by_country.get(ctry)
        if cnts:
            item["counts"] = cnts
            item["total_alerts"] = cnts.get("total", 0)

    # Sort A–Z; countries without alerts already excluded by _parse_europe
    base_entries.sort(key=lambda x: (x.get("region") or x.get("title","")).lower())
    logging.warning(f"[METEOALARM DEBUG] Parsed {len(base_entries)} countries with alerts")
    return {"entries": base_entries, "source": url}
