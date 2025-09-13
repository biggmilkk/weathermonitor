import streamlit as st
import feedparser
import logging
import re
from bs4 import BeautifulSoup
import httpx
import asyncio
from datetime import datetime, timezone

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

# ------------ Time helpers ------------
def _parse_iso8601_maybe(s: str):
    """
    Try to parse an ISO-8601-ish timestamp like '2025-09-06T18:59:59+00:00'.
    Return timezone-aware dt or None.
    """
    if not s:
        return None
    s = s.strip()
    try:
        # Python handles 'YYYY-MM-DDTHH:MM:SS+00:00' and with Z if converted
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        # Ensure timezone-aware; assume UTC if missing
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def _fmt_utc(dt):
    if not dt:
        return ""
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%b %d %H:%M UTC")

# ------------ Common RSS row parser (used for both EU and per-country feeds) ------------
def _parse_table_rows(description_html: str):
    soup = BeautifulSoup(description_html, "html.parser")
    rows = soup.find_all("tr")
    current = "today"
    items = {"today": [], "tomorrow": []}

    now_utc = datetime.now(timezone.utc)

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

        # Pull From/Until; normalize & filter expired
        # We accept either the <i> content or raw fallback
        cell_html = str(cells[1])
        from_m = re.search(r"From:\s*</b>\s*<i>(.*?)</i>", cell_html, re.IGNORECASE)
        until_m = re.search(r"Until:\s*</b>\s*<i>(.*?)</i>", cell_html, re.IGNORECASE)
        from_raw = from_m.group(1).strip() if from_m else ""
        until_raw = until_m.group(1).strip() if until_m else ""

        from_dt = _parse_iso8601_maybe(from_raw)
        until_dt = _parse_iso8601_maybe(until_raw)

        # If there's an until time and it's already passed, drop this row
        if until_dt and until_dt <= now_utc:
            continue

        # Format for display
        from_label = _fmt_utc(from_dt) if from_dt else ""
        until_label = _fmt_utc(until_dt) if until_dt else (until_raw or "")

        items[current].append({
            "level": level_name,
            "type": type_name,
            "from": from_label or None,
            "until": until_label or None,
        })
    return items

# ------------ Europe aggregate parser (your existing behavior) ------------
def _parse_europe(feed):
    entries = []
    for entry in feed.entries:
        country = entry.get("title", "").replace("MeteoAlarm", "").strip()
        pub_date = entry.get("published", "")
        description_html = entry.get("description", "")
        alerts_by_day = _parse_table_rows(description_html)

        # Skip if no Orange/Red (after expiry filtering)
        if not alerts_by_day["today"] and not alerts_by_day["tomorrow"]:
            continue

        entries.append({
            "title": f"{country} Alerts",
            "summary": "",
            "alerts": alerts_by_day,
            "link": _front_end_url(country) or entry.get("link", ""),
            "published": pub_date,
            "region": country,
            "province": "Europe",
        })
    return entries

# ------------ Count helpers for per-country feeds ------------
def _count_from_country_feed(fp_obj) -> dict:
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
                k = f"{level}|{typ}"
                counts["by_day"].setdefault(day, {})
                counts["by_day"][day][k] = counts["by_day"][day].get(k, 0) + 1
                bucket = counts["by_type"].setdefault(typ, {"Orange": 0, "Red": 0, "total": 0})
                bucket[level] += 1
                bucket["total"] += 1
                counts["total"] += 1
    return counts

# ------------ Public API (sync) ------------
@st.cache_data(ttl=60, show_spinner=False)
def scrape_meteoalarm(conf: dict):
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

    base_entries.sort(key=lambda x: (x.get("region") or x.get("title","")).lower())

    # Debug message
    logging.warning(f"[METEOALARM DEBUG] Parsed {len(base_entries)} alerts")

    return {"entries": base_entries, "source": url}

# ------------ Public API (async) ------------
async def scrape_meteoalarm_async(conf: dict, client: httpx.AsyncClient):
    url = conf.get("url", DEFAULT_URL)
    try:
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

    tasks = [fetch_counts(item.get("region","")) for item in base_entries]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    counts_by_country = {c: cnt for c, cnt in results if c and cnt}
    for item in base_entries:
        ctry = item.get("region","")
        cnts = counts_by_country.get(ctry)
        if cnts:
            item["counts"] = cnts
            item["total_alerts"] = cnts.get("total", 0)

    base_entries.sort(key=lambda x: (x.get("region") or x.get("title","")).lower())

    # Debug message
    logging.warning(f"[METEOALARM DEBUG] Parsed {len(base_entries)}")

    return {"entries": base_entries, "source": url}
