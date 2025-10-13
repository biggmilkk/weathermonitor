# scraper/meteoalarm.py

import feedparser
import logging
import re
from bs4 import BeautifulSoup
import httpx
import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, Tuple, List

# -------------------- Severity & type maps --------------------
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

# -------------------- Name normalization & URL overrides --------------------
NORMALIZE_COUNTRY_NAMES = {
    "Macedonia (the former Yugoslav Republic of)": "North Macedonia",
    "MeteoAlarm Macedonia (the former Yugoslav Republic of)": "North Macedonia",
    "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
    # add more edge cases here if encountered
}

FEED_URL_OVERRIDES = {
    "North Macedonia": "https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-republic-of-north-macedonia",
}

# Country code for "Read more" links in UI
COUNTRY_TO_CODE = {
    "Austria": "AT","Belgium": "BE","Bosnia and Herzegovina": "BA","Bulgaria": "BG",
    "Croatia": "HR","Cyprus": "CY","Czechia": "CZ","Czech Republic": "CZ","Denmark": "DK",
    "Estonia": "EE","Finland": "FI","France": "FR","Germany": "DE","Greece": "GR",
    "Hungary": "HU","Iceland": "IS","Ireland": "IE","Israel": "IL","Italy": "IT",
    "Latvia": "LV","Lithuania": "LT","Luxembourg": "LU","Malta": "MT","Moldova": "MD",
    "Montenegro": "ME","Netherlands": "NL","North Macedonia": "MK",
    "Norway": "NO","Poland": "PL","Portugal": "PT",
    "Romania": "RO","Serbia": "RS","Slovakia": "SK","Slovenia": "SI","Spain": "ES",
    "Sweden": "SE","Switzerland": "CH","Ukraine": "UA",
    "United Kingdom": "UK", "United Kingdom of Great Britain and Northern Ireland": "UK",
}

# Known slugs for country RSS endpoints
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

# -------------------- Helpers --------------------
def _front_end_url(country_name: str) -> Optional[str]:
    code = COUNTRY_TO_CODE.get(country_name)
    return f"https://meteoalarm.org/en/live/region/{code}" if code else None

def _country_rss_url(country_name: str) -> Optional[str]:
    # Prefer explicit override (e.g., North Macedonia)
    if country_name in FEED_URL_OVERRIDES:
        return FEED_URL_OVERRIDES[country_name]
    slug = COUNTRY_TO_RSS_SLUG.get(country_name)
    return f"https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-{slug}" if slug else None

def _parse_iso8601_maybe(s: str):
    if not s:
        return None
    s = s.strip()
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def _fmt_utc(dt):
    if not dt:
        return ""
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%b  %d %H:%M UTC").replace("  ", " ")

# Parse the HTML table in each RSS item (EU and per-country)
def _parse_table_rows(description_html: str):
    soup = BeautifulSoup(description_html, "html.parser")
    rows = soup.find_all("tr")
    current = "today"
    items = {"today": [], "tomorrow": []}

    now_utc = datetime.now(timezone.utc)

    for row in rows:
        # Consider all <th> cells in the row to robustly set the day context
        headers = row.find_all("th")
        if headers:
            header_text = " ".join(h.get_text(strip=True).lower() for h in headers)
            if "tomorrow" in header_text:
                current = "tomorrow"
            elif "today" in header_text:
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
        # Only Orange/Red
        if level_name not in ("Orange", "Red"):
            continue

        type_name = AWARENESS_TYPES.get(awt, f"Type {awt}")

        cell_html = str(cells[1])
        from_m = re.search(r"From:\s*</b>\s*<i>(.*?)</i>", cell_html, re.IGNORECASE)
        until_m = re.search(r"Until:\s*</b>\s*<i>(.*?)</i>", cell_html, re.IGNORECASE)
        from_raw = from_m.group(1).strip() if from_m else ""
        until_raw = until_m.group(1).strip() if until_m else ""

        from_dt = _parse_iso8601_maybe(from_raw)
        until_dt = _parse_iso8601_maybe(until_raw)

        # Drop expired
        if until_dt and until_dt <= now_utc:
            continue

        from_label = _fmt_utc(from_dt) if from_dt else ""
        until_label = _fmt_utc(until_dt) if until_dt else (until_raw or "")

        items[current].append({
            "level": level_name,
            "type": type_name,
            "from": from_label or None,
            "until": until_label or None,
        })
    return items

# Build synthetic minimal counts directly from a country's EU alert rows (fallback)
def _counts_from_eu_rows(alerts_by_day: Dict[str, List[Dict]]) -> Dict:
    counts = {"total": 0, "by_type": {}, "by_day": {"today": {}, "tomorrow": {}}}
    for day in ("today", "tomorrow"):
        for it in alerts_by_day.get(day, []):
            lvl = it.get("level", "")
            typ = it.get("type", "")
            k = f"{lvl}|{typ}"
            counts["by_day"][day][k] = counts["by_day"][day].get(k, 0) + 1
            bucket = counts["by_type"].setdefault(typ, {"Orange": 0, "Red": 0, "total": 0})
            if lvl in ("Orange", "Red"):
                bucket[lvl] += 1
                bucket["total"] += 1
                counts["total"] += 1
    return counts

def _is_national_summary(entry) -> bool:
    """
    Heuristic: per-country feeds include a national summary 'Spain'/'Israel' item
    with link containing '?region=XX'. Regional items include '?geocode='.
    """
    link = (entry.get("link") if isinstance(entry, dict) else getattr(entry, "link", "")) or ""
    if "region=" in link and "geocode=" not in link:
        return True
    title = (entry.get("title") if isinstance(entry, dict) else getattr(entry, "title", "")) or ""
    return bool(title.strip().lower().startswith("meteoalarm "))

# Parse per-country feed into counts (true multi-instance totals, per unique region)
def _count_from_country_feed(fp_obj) -> Dict:
    counts = {"total": 0, "by_type": {}, "by_day": {"today": {}, "tomorrow": {}}}

    # Track unique (region, day, level, type) to avoid double-counting duplicates
    seen_keys = set()

    for entry in fp_obj.entries:
        # Skip the national summary item; we only want regional instances
        if _is_national_summary(entry):
            continue

        desc = entry.get("description", "") or getattr(entry, "description", "") or ""
        per_day = _parse_table_rows(desc)

        # Region name (title of the entry is the administrative/zone region)
        region_title = (entry.get("title") if isinstance(entry, dict) else getattr(entry, "title", "")) or ""
        region = region_title.strip()

        for day in ("today", "tomorrow"):
            for it in per_day.get(day, []):
                level = (it.get("level", "") or "").strip()
                if level not in ("Orange", "Red"):
                    continue
                typ = (it.get("type", "") or "").strip()

                # Dedupe by region+day+level+type so a region contributes once per bucket
                skey = (region, day, level, typ)
                if skey in seen_keys:
                    continue
                seen_keys.add(skey)

                # Per-day bucket
                k = f"{level}|{typ}"
                day_map = counts["by_day"].setdefault(day, {})
                day_map[k] = day_map.get(k, 0) + 1

                # Per-type totals
                bucket = counts["by_type"].setdefault(typ, {"Orange": 0, "Red": 0, "total": 0})
                bucket[level] += 1
                bucket["total"] += 1

                counts["total"] += 1
    return counts

# -------------------- EU aggregate parser (with normalization) --------------------
def _parse_europe(feed):
    entries = []
    for entry in feed.entries:
        raw_title = entry.get("title", "") or ""
        t = raw_title.strip()
        if t.lower().startswith("meteoalarm "):
            t = t[len("MeteoAlarm "):].strip()
        country = NORMALIZE_COUNTRY_NAMES.get(t, t)

        pub_date = entry.get("published", "") or getattr(entry, "published", "") or ""
        description_html = entry.get("description", "") or getattr(entry, "description", "") or ""
        alerts_by_day = _parse_table_rows(description_html)

        # Skip if no Orange/Red after expiry filtering
        if not alerts_by_day["today"] and not alerts_by_day["tomorrow"]:
            continue

        entries.append({
            "title": f"{country} Alerts",
            "summary": "",
            "alerts": alerts_by_day,
            "link": _front_end_url(country) or entry.get("link", "") or getattr(entry, "link", "") or "",
            "published": pub_date,
            "region": country,
            "province": "Europe",
        })
    return entries

# -------------------- Public API (sync) --------------------
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
        alerts_by_day = item.get("alerts") or {}

        # Try override URL first, then slug URL
        tried = []
        found_counts = False
        for which in ("override", "slug"):
            rss_url = (
                FEED_URL_OVERRIDES.get(country)
                if which == "override" else _country_rss_url(country)
            )
            if not rss_url or rss_url in tried:
                continue
            tried.append(rss_url)
            try:
                fp = feedparser.parse(rss_url)
                counts = _count_from_country_feed(fp)
                # Use regional counts if any were found; this reflects "active regions" per bucket
                if counts.get("total", 0) > 0:
                    item["counts"] = counts
                    item["total_alerts"] = counts.get("total", 0)
                    found_counts = True
                    break
            except Exception as e:
                logging.warning(f"[METEOALARM WARN] Count fetch failed for {country} via {rss_url}: {e}")

        # Final fallback: synthesize minimal counts from EU rows so (x active) prints at least something
        if not found_counts:
            fallback_counts = _counts_from_eu_rows(alerts_by_day)
            item["counts"] = fallback_counts
            item["total_alerts"] = fallback_counts.get("total", 0)

        # --- DEBUG LOG FOR SPAIN ---
        if country.lower() == "spain":
            logging.warning(
                "🟢 [SPAIN DEBUG] counts.total=%s  by_day_today=%s  by_day_tomorrow=%s  by_type=%s",
                item.get("counts", {}).get("total"),
                item.get("counts", {}).get("by_day", {}).get("today"),
                item.get("counts", {}).get("by_day", {}).get("tomorrow"),
                item.get("counts", {}).get("by_type"),
            )
    
    base_entries.sort(key=lambda x: (x.get("region") or x.get("title","")).lower())
    logging.warning(f"[METEOALARM DEBUG] Parsed {len(base_entries)} alerts (sync)")
    return {"entries": base_entries, "source": url}

# -------------------- Public API (async) --------------------
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

    async def fetch_counts(country: str, alerts_by_day: Dict) -> Optional[Tuple[str, Dict]]:
        # Try override first, then slug URL
        tried = []
        for which in ("override", "slug"):
            rss_url = (
                FEED_URL_OVERRIDES.get(country)
                if which == "override" else _country_rss_url(country)
            )
            if not rss_url or rss_url in tried:
                continue
            tried.append(rss_url)
            try:
                r = await client.get(rss_url, timeout=12)
                r.raise_for_status()
                fp = feedparser.parse(r.content)
                counts = _count_from_country_feed(fp)
                if counts.get("total", 0) > 0:
                    return (country, counts)
            except Exception as e:
                logging.warning(f"[METEOALARM WARN] Count fetch failed for {country} via {rss_url}: {e}")

        # Final fallback: synthesize minimal counts from EU rows (so we still show '(x active)')
        return (country, _counts_from_eu_rows(alerts_by_day))

    tasks = [fetch_counts(item.get("region", ""), item.get("alerts") or {}) for item in base_entries]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    # Filter out malformed items carefully (shouldn't happen, but be safe)
    valid_pairs: List[Tuple[str, Dict]] = []
    for x in results:
        if isinstance(x, tuple) and len(x) == 2 and isinstance(x[0], str) and isinstance(x[1], dict):
            valid_pairs.append(x)

    counts_by_country = {c: cnt for (c, cnt) in valid_pairs}

    # Attach counts & totals
    for item in base_entries:
        country = item.get("region", "")
        cnt = counts_by_country.get(country)
        if isinstance(cnt, dict):
            item["counts"] = cnt
            item["total_alerts"] = int(cnt.get("total", 0))

    base_entries.sort(key=lambda x: (x.get("region") or x.get("title","")).lower())
    logging.warning(f"[METEOALARM DEBUG] Parsed {len(base_entries)}")
    return {"entries": base_entries, "source": url}
