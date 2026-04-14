import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import feedparser
import httpx


CAP_SEVERITY_TO_LEVEL = {
    "moderate": "Yellow",
    "severe": "Orange",
    "extreme": "Red",
}

EVENT_TYPE_NORMALIZATION = {
    "Thunderstorm": "Thunderstorms",
    "Thunderstorms": "Thunderstorms",
    "Rain": "Rain",
    "Wind": "Wind",
    "Snow": "Snow/Ice",
    "Snow/Ice": "Snow/Ice",
    "Flood": "Flood",
    "Rain/Flood": "Rain/Flood",
    "Fog": "Fog",
    "Coastal Event": "Coastal event",
    "Forest Fire": "Forest fire",
    "Extreme High Temperature": "Extreme high temperature",
    "Extreme Low Temperature": "Extreme low temperature",
    "Avalanche": "Avalanche",
}

COUNTRY_TO_CODE = {
    "Andorra": "AD",
    "Austria": "AT",
    "Belgium": "BE",
    "Bosnia and Herzegovina": "BA",
    "Bulgaria": "BG",
    "Croatia": "HR",
    "Cyprus": "CY",
    "Czech Republic": "CZ",
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

COUNTRY_TO_ATOM_SLUG = {
    "Andorra": "andorra",
    "Austria": "austria",
    "Belgium": "belgium",
    "Bosnia and Herzegovina": "bosnia-herzegovina",
    "Bulgaria": "bulgaria",
    "Croatia": "croatia",
    "Cyprus": "cyprus",
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
    "United Kingdom of Great Britain and Northern Ireland": "united-kingdom",
    "United Kingdom": "united-kingdom",
}


def _country_atom_url(country_name: str) -> Optional[str]:
    slug = COUNTRY_TO_ATOM_SLUG.get(country_name)
    if not slug:
        return None
    return f"https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-atom-{slug}"


def _front_end_url(country_name: str) -> Optional[str]:
    code = COUNTRY_TO_CODE.get(country_name)
    if not code:
        return None
    return f"https://meteoalarm.org/en/live/region/{code}"


def _parse_dt(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _fmt_utc(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    return dt.astimezone(timezone.utc).strftime("%b %d %H:%M UTC")


def _extract_event_type(event_text: str) -> str:
    text = (event_text or "").strip()

    # Remove leading severity word if present
    for prefix in ("Yellow ", "Orange ", "Red "):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break

    # Remove trailing " Warning"
    if text.endswith(" Warning"):
        text = text[:-8].strip()

    return EVENT_TYPE_NORMALIZATION.get(text, text)


def _cap_get(entry, key: str, default: str = "") -> str:
    return entry.get(key, "") or getattr(entry, key, "") or default


def _entry_link(entry) -> str:
    links = entry.get("links", []) or []
    for link in links:
        href = link.get("href")
        if href and "meteoalarm.org" in href and "geocode=" in href:
            return href
    for link in links:
        href = link.get("href")
        if href:
            return href
    return entry.get("link", "") or ""


def _counts_from_alerts(alerts_by_day: Dict[str, List[Dict]]) -> Dict:
    counts = {"total": 0, "by_type": {}, "by_day": {"today": {}, "tomorrow": {}}}

    for day in ("today", "tomorrow"):
        for it in alerts_by_day.get(day, []):
            lvl = it.get("level", "")
            typ = it.get("type", "")
            if lvl not in ("Orange", "Red"):
                continue

            key = f"{lvl}|{typ}"
            counts["by_day"][day][key] = counts["by_day"][day].get(key, 0) + 1

            bucket = counts["by_type"].setdefault(
                typ, {"Orange": 0, "Red": 0, "total": 0}
            )
            bucket[lvl] += 1
            bucket["total"] += 1
            counts["total"] += 1

    return counts


def _classify_day(start_dt: Optional[datetime], now_utc: datetime) -> Optional[str]:
    if not start_dt:
        return None
    today = now_utc.date()
    start_date = start_dt.date()
    if start_date == today:
        return "today"
    if start_date == today + timedelta(days=1):
        return "tomorrow"
    return None


def _parse_country_feed(country_name: str, fp_obj) -> Optional[Dict]:
    now_utc = datetime.now(timezone.utc)
    alerts_by_day = {"today": [], "tomorrow": []}
    latest_updated: Optional[datetime] = None

    seen_ids = set()

    for entry in fp_obj.entries:
        severity_raw = _cap_get(entry, "cap_severity").strip().lower()
        level = CAP_SEVERITY_TO_LEVEL.get(severity_raw)

        # keep your internal threshold: only Orange/Red
        if level not in ("Orange", "Red"):
            continue

        expires_dt = _parse_dt(_cap_get(entry, "cap_expires"))
        if expires_dt and expires_dt <= now_utc:
            continue

        effective_dt = _parse_dt(_cap_get(entry, "cap_effective"))
        onset_dt = _parse_dt(_cap_get(entry, "cap_onset"))
        published_dt = _parse_dt(entry.get("published", "") or entry.get("updated", ""))
        start_dt = effective_dt or onset_dt or published_dt

        bucket = _classify_day(start_dt, now_utc)
        if bucket is None:
            continue

        identifier = _cap_get(entry, "cap_identifier") or entry.get("id", "")
        if identifier in seen_ids:
            continue
        seen_ids.add(identifier)

        event_text = _cap_get(entry, "cap_event")
        area_desc = _cap_get(entry, "cap_areadesc")
        type_name = _extract_event_type(event_text)

        alert = {
            "level": level,
            "type": type_name,
            "from": _fmt_utc(start_dt),
            "until": _fmt_utc(expires_dt),
            "area": area_desc or None,
            "identifier": identifier,
            "link": _entry_link(entry),
        }
        alerts_by_day[bucket].append(alert)

        updated_dt = _parse_dt(entry.get("updated", "") or entry.get("published", ""))
        if updated_dt and (latest_updated is None or updated_dt > latest_updated):
            latest_updated = updated_dt

    counts = _counts_from_alerts(alerts_by_day)
    if counts["total"] == 0:
        return None

    return {
        "title": f"{country_name} Alerts",
        "summary": "",
        "alerts": alerts_by_day,
        "counts": counts,
        "total_alerts": counts["total"],
        "link": _front_end_url(country_name) or _country_atom_url(country_name) or "",
        "published": latest_updated.isoformat() if latest_updated else "",
        "region": country_name,
        "province": "Europe",
    }


async def scrape_meteoalarm_async(conf: dict, client: httpx.AsyncClient):
    countries = conf.get("countries") or list(COUNTRY_TO_ATOM_SLUG.keys())
    timeout = conf.get("timeout", 12.0)

    async def fetch_country(country: str) -> Optional[Dict]:
        url = _country_atom_url(country)
        if not url:
            return None

        try:
            resp = await client.get(url, timeout=timeout)
            resp.raise_for_status()
            fp = feedparser.parse(resp.content)
            return _parse_country_feed(country, fp)
        except Exception as e:
            logging.warning(f"[METEOALARM WARN] Failed {country} via {url}: {e}")
            return None

    results = await asyncio.gather(*(fetch_country(c) for c in countries), return_exceptions=False)
    entries = [x for x in results if isinstance(x, dict)]
    entries.sort(key=lambda x: (x.get("region") or "").lower())

    logging.warning(f"[METEOALARM DEBUG] Parsed {len(entries)} country alerts (async)")
    return {"entries": entries, "source": "country_atom_feeds"}


def scrape_meteoalarm(conf: dict):
    async def _run():
        limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)
        headers = {"User-Agent": "weather-monitor/1.0"}
        async with httpx.AsyncClient(limits=limits, headers=headers, follow_redirects=True) as client:
            return await scrape_meteoalarm_async(conf, client)

    return asyncio.run(_run())
