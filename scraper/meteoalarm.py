import asyncio
import logging
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Literal, Optional, TypedDict

import feedparser
import httpx
import streamlit as st
from bs4 import BeautifulSoup

# Optional dateutil for robust parsing (falls back to strptime)
try:
    from dateutil import parser as dtparse
    _HAS_DATEUTIL = True
except Exception:
    _HAS_DATEUTIL = False

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

# ------------ Types ------------
Day = Literal["today", "tomorrow"]

class AlertRow(TypedDict, total=False):
    level_code: str
    level: str
    type_code: str
    type: str
    from_: str  # raw, original string
    until: str  # raw, original string
    start_iso: Optional[str]
    end_iso: Optional[str]

class AlertsByDay(TypedDict):
    today: list[AlertRow]
    tomorrow: list[AlertRow]

# ------------ Normalization helpers ------------
def _normalize_country(name: str) -> str:
    n = (name or "").replace("MeteoAlarm", "").strip()
    aliases = {
        "Czech Republic": "Czechia",
        "Republic of North Macedonia": "North Macedonia",
        "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
    }
    return aliases.get(n, n)

def _front_end_url(country_name: str) -> Optional[str]:
    code = COUNTRY_TO_CODE.get(country_name)
    return f"https://meteoalarm.org/en/live/region/{code}" if code else None

def _country_rss_url(country_name: str) -> Optional[str]:
    slug = COUNTRY_TO_RSS_SLUG.get(country_name)
    return f"https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-{slug}" if slug else None

# ------------ Time parsing & filtering ------------
def _parse_utc_legacy(ts: str, ref_year: int) -> Optional[datetime]:
    """
    Parse strings like 'Sep 05 16:00 UTC' (possibly missing year) to aware UTC datetime.
    """
    if not ts or not isinstance(ts, str):
        return None
    s = ts.strip()

    # Append year if missing
    if not re.search(r"\b\d{4}\b", s):
        s = f"{s} {ref_year}"

    # Ensure UTC label exists (many feeds already include it)
    if "UTC" not in s.upper():
        s = f"{s} UTC"

    if _HAS_DATEUTIL:
        try:
            dt = dtparse.parse(s, fuzzy=True)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        except Exception:
            pass

    for fmt in ("%b %d %H:%M %Z %Y", "%b %d %H:%M%Z %Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None

def _filter_expired_alerts(
    alerts_by_day: AlertsByDay,
    *,
    ref_year: int,
    now_utc: datetime,
    expiry_grace: timedelta = timedelta(minutes=0),
) -> AlertsByDay:
    """
    Drop alerts whose 'until' < (now_utc - expiry_grace).
    If 'until' is missing but 'from' is older than 2 days, drop conservatively.
    """
    keep: AlertsByDay = {"today": [], "tomorrow": []}
    cutoff = now_utc - expiry_grace

    for day in ("today", "tomorrow"):
        for it in alerts_by_day.get(day, []):
            raw_from = it.get("from_") or it.get("from") or it.get("start")
            raw_until = it.get("until") or it.get("end")
            dt_from = _parse_utc_legacy(raw_from, ref_year) if raw_from else None
            dt_until = _parse_utc_legacy(raw_until, ref_year) if raw_until else None

            if dt_until and dt_until < cutoff:
                continue
            if not dt_until and dt_from and (now_utc - dt_from) > timedelta(days=2):
                continue

            # Attach normalized ISO (if available) for downstream use
            it["start_iso"] = dt_from.isoformat() if dt_from else None
            it["end_iso"] = dt_until.isoformat() if dt_until else None
            keep[day].append(it)
    return keep

# ------------ Common RSS row parser (robust; language-agnostic labels) ------------
def _extract_times(cell_html: str) -> tuple[Optional[str], Optional[str]]:
    """
    Try to extract start/end strings from the right-hand <td>.
    Prefer <time datetime="...">; fall back to the first two <i> tags.
    Return raw strings (not yet normalized).
    """
    soup = BeautifulSoup(cell_html, "html.parser")
    times: list[str] = []

    for t in soup.select("time[datetime]"):
        dt = t.get("datetime")
        if dt:
            times.append(dt)

    if len(times) < 2:
        for it in soup.select("i"):
            txt = it.get_text(strip=True)
            if txt:
                times.append(txt)
            if len(times) >= 2:
                break

    start = times[0] if times else None
    end = times[1] if len(times) > 1 else None
    return start, end

def _parse_table_rows(description_html: str) -> AlertsByDay:
    soup = BeautifulSoup(description_html or "", "html.parser")
    rows = soup.select("tr")
    items: AlertsByDay = {"today": [], "tomorrow": []}
    current: Day = "today"

    for row in rows:
        th = row.find("th")
        if th:
            label = th.get_text(strip=True).lower()
            # Common languages: English, Spanish, German, French (prefix match for "aujourd'hui")
            if "tomorrow" in label or "mañana" in label or "morgen" in label or "demain" in label:
                current = "tomorrow"
            elif "today" in label or "hoy" in label or "heute" in label or "aujourd" in label:
                current = "today"
            continue

        tds = row.find_all("td")
        if len(tds) != 2:
            continue

        meta, detail = tds[0], tds[1]
        level_code = (meta.get("data-awareness-level") or "").strip()
        type_code = (meta.get("data-awareness-type") or "").strip()

        if not level_code or not type_code:
            # Fallback: e.g., "awt:10 level:4"
            text = meta.get_text(" ", strip=True)
            m = re.search(r"awt:(\d+)\s+level:(\d+)", text, re.IGNORECASE)
            if m:
                type_code, level_code = m.groups()

        level_name = AWARENESS_LEVELS.get(level_code, f"Unknown({level_code})")
        type_name = AWARENESS_TYPES.get(type_code, f"Unknown({type_code})")

        if level_name not in ("Orange", "Red"):
            continue

        start_raw, end_raw = _extract_times(str(detail))

        items[current].append({
            "level_code": level_code,
            "level": level_name,
            "type_code": type_code,
            "type": type_name,
            "from_": start_raw or "",
            "until": end_raw or "",
        })

    return items

# ------------ Europe aggregate parser (now filters expired) ------------
def _parse_europe(feed) -> list[dict]:
    entries: list[dict] = []
    now_utc = datetime.now(timezone.utc)

    for entry in getattr(feed, "entries", []):
        pub_date = entry.get("published", "") or ""
        ref_year = now_utc.year
        if pub_date:
            try:
                if _HAS_DATEUTIL:
                    ref_year = dtparse.parse(pub_date, fuzzy=True).year
                else:
                    m = re.search(r"\b(\d{4})\b", pub_date)
                    if m:
                        ref_year = int(m.group(1))
            except Exception:
                pass

        country = _normalize_country(entry.get("title", ""))
        description_html = entry.get("description", "")

        alerts_by_day = _parse_table_rows(description_html)
        alerts_by_day = _filter_expired_alerts(alerts_by_day, ref_year=ref_year, now_utc=now_utc)

        # Skip if no active Orange/Red after filtering
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
            # counts will be injected later
        })

    return entries

# ------------ Count helpers for per-country feeds (now filters expired) ------------
def _count_from_country_feed(fp_obj) -> dict:
    """
    Returns counts only for non-expired (active) Orange/Red rows.
    """
    counts = {"total": 0, "by_type": {}, "by_day": {"today": {}, "tomorrow": {}}}
    now_utc = datetime.now(timezone.utc)

    # Infer a reference year from feed metadata if possible
    ref_year = now_utc.year
    try:
        if getattr(fp_obj, "feed", None):
            pub = getattr(fp_obj.feed, "published", "") or getattr(fp_obj.feed, "updated", "") or ""
            if pub and _HAS_DATEUTIL:
                ref_year = dtparse.parse(pub, fuzzy=True).year
    except Exception:
        pass

    for entry in getattr(fp_obj, "entries", []):
        desc = entry.get("description", "") or ""
        per_day = _parse_table_rows(desc)
        per_day = _filter_expired_alerts(per_day, ref_year=ref_year, now_utc=now_utc)

        for day in ("today", "tomorrow"):
            for it in per_day[day]:
                level = it.get("level", "")
                typ = it.get("type", "")
                if level not in ("Orange", "Red"):
                    continue

                k = f"{level}|{typ}"
                counts["by_day"][day][k] = counts["by_day"][day].get(k, 0) + 1

                bucket = counts["by_type"].setdefault(typ, {"Orange": 0, "Red": 0, "total": 0})
                bucket[level] += 1
                bucket["total"] += 1
                counts["total"] += 1

    return counts

# ------------ Retry helper (async) ------------
async def _get_with_retries(
    client: httpx.AsyncClient,
    url: str,
    *,
    retries: int = 3,
    base_delay: float = 0.4,
    timeout: float = 12.0,
) -> Optional[bytes]:
    for attempt in range(retries):
        try:
            r = await client.get(url, timeout=timeout)
            r.raise_for_status()
            return r.content
        except Exception as e:
            if attempt == retries - 1:
                logging.warning(f"[METEOALARM RETRY-FAIL] {url}: {e}")
                return None
            await asyncio.sleep(base_delay * (2 ** attempt) + random.random() * 0.2)

# ------------ Public API (sync) ------------
@st.cache_data(ttl=60, show_spinner=False)
def scrape_meteoalarm(conf: dict):
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
            item["counts_error"] = str(e)

    # Only countries that still have active alerts (already filtered), sort A–Z
    base_entries.sort(key=lambda x: (x.get("region") or x.get("title","")).lower())
    return {"entries": base_entries, "source": url}

# ------------ Public API (async) ------------
async def scrape_meteoalarm_async(conf: dict, client: httpx.AsyncClient):
    """
    Async: fetch EU feed, then concurrently fetch each present country's RSS for counts.
    Includes concurrency limit and retries.
    """
    url = conf.get("url", DEFAULT_URL)
    try:
        blob = await _get_with_retries(client, url, timeout=15)
        if blob is None:
            raise RuntimeError("EU feed unavailable")
        eu_fp = feedparser.parse(blob)
        base_entries = _parse_europe(eu_fp)
    except Exception as e:
        logging.warning(f"[METEOALARM ERROR] EU fetch failed: {e}")
        return {"entries": [], "error": str(e), "source": url}

    sem = asyncio.Semaphore(conf.get("max_concurrency", 8))

    async def fetch_counts(country: str) -> tuple[str, Optional[dict]]:
        rss_url = _country_rss_url(country)
        if not rss_url:
            return (country, None)
        async with sem:
            blob = await _get_with_retries(client, rss_url)
        if blob is None:
            return (country, None)
        try:
            fp = feedparser.parse(blob)
            return (country, _count_from_country_feed(fp))
        except Exception as e:
            logging.warning(f"[METEOALARM WARN] Count parse failed for {country}: {e}")
            return (country, None)

    tasks = [fetch_counts(item.get("region", "")) for item in base_entries]
    results = await asyncio.gather(*tasks)

    for ctry, cnts in results:
        if not ctry:
            continue
        for item in base_entries:
            if item.get("region") == ctry:
                if cnts:
                    item["counts"] = cnts
                    item["total_alerts"] = cnts.get("total", 0)
                else:
                    item["counts_error"] = "No counts (fetch/parse failure)"
                break

    base_entries.sort(key=lambda x: (x.get("region") or x.get("title","")).lower())
    return {"entries": base_entries, "source": url}
