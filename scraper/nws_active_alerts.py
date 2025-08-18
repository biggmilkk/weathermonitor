import streamlit as st
import requests
import logging
import httpx
import re

# Only include these event types (same as before; adjust as you like)
ALLOWED_EVENTS = {
    "Severe Thunderstorm Warning",
    "Flash Flood Warning",
    "Tornado Warning",
    "Flood Warning",
    "Extreme Heat Warning",
    "Air Quality Alert",
}

# State/territory codes -> full names (incl. DC + territories). Marine grouped as "Marine".
STATE_NAMES = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado",
    "CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho",
    "IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas","KY":"Kentucky","LA":"Louisiana",
    "ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan","MN":"Minnesota","MS":"Mississippi",
    "MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey",
    "NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma",
    "OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota",
    "TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington",
    "WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming","DC":"District of Columbia",
    "PR":"Puerto Rico","VI":"U.S. Virgin Islands","GU":"Guam","AS":"American Samoa","MP":"Northern Mariana Islands",
    "MAR":"Marine",
}

MARINE_PREFIXES = {"ANZ","AMZ","GMZ","PZZ","PHZ","PKZ","PMZ"}  # common marine groupings

_state_re = re.compile(r",\s*([A-Z]{2})(?:\s|$)")  # fallback extractor for ", XX" in areaDesc

def _infer_state_from_ugc(ugc_list) -> str | None:
    """
    Infer state/territory code from UGC codes.
    Prefer marine grouping if any marine prefix present.
    """
    ugc_list = ugc_list or []
    # Marine takes precedence if any zone is marine
    for code in ugc_list:
        if isinstance(code, str) and len(code) >= 3:
            if code[:3] in MARINE_PREFIXES:
                return "MAR"
    # Otherwise take the first alpha 2-letter prefix
    for code in ugc_list:
        if isinstance(code, str) and len(code) >= 2 and code[:2].isalpha():
            return code[:2]
    return None

def _fallback_state_from_area(area_desc: str) -> str | None:
    """
    Try to pull a trailing ', XX' state code from areaDesc as a weak fallback.
    """
    if not area_desc:
        return None
    m = _state_re.search(area_desc)
    return m.group(1) if m else None

def _enrich_entry_from_props(props: dict) -> dict | None:
    """
    Build a normalized entry with state + event bucket.
    Returns None if the event is not allowed.
    """
    event_type = props.get("event", "")
    if event_type not in ALLOWED_EVENTS:
        return None

    area_desc = props.get("areaDesc", "") or ""
    ugc = (props.get("geocode") or {}).get("UGC") or []

    state_code = _infer_state_from_ugc(ugc) or _fallback_state_from_area(area_desc)
    if not state_code:
        # If truly unknown, group under 'Marine' if headline hints marine;
        # else skip state (you can choose to drop or bucket as 'Unknown')
        state_code = "MAR" if "marine" in (props.get("headline","").lower()) else "Unknown"

    state_name = STATE_NAMES.get(state_code, state_code)

    return {
        "title": props.get("headline", event_type),
        "summary": props.get("description", ""),
        "link": props.get("web", "") or props.get("uri", ""),
        "published": props.get("effective", "") or props.get("sent", ""),
        "region": area_desc,                 # keep raw area text for display/context
        "state_code": state_code,            # NEW
        "state": state_name,                 # NEW (for grouping)
        "event": event_type,                 # NEW (use as bucket)
        "bucket": event_type,                # alias for renderer convenience
    }

@st.cache_data(ttl=60, show_spinner=False)
def scrape_nws(conf: dict) -> dict:
    """
    Synchronous scraper for NWS active alerts.
    """
    url = conf.get("url", "https://api.weather.gov/alerts/active")
    headers = {"User-Agent": "WeatherMonitorApp (your@email.com)"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        feed = resp.json()
        logging.warning(f"[NWS DEBUG] Parsed {len(feed.get('features', []))} alerts")
    except Exception as e:
        logging.warning(f"[NWS SCRAPER ERROR] Fetch failed: {e}")
        return {"entries": [], "error": str(e), "source": url}

    entries = []
    for feature in feed.get("features", []):
        props = feature.get("properties", {}) or {}
        enriched = _enrich_entry_from_props(props)
        if enriched:
            entries.append(enriched)
    return {"entries": entries, "source": url}

async def scrape_nws_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Async scraper for NWS active alerts using httpx.AsyncClient.
    """
    url = conf.get("url", "https://api.weather.gov/alerts/active")
    headers = {"User-Agent": "WeatherMonitorApp (your@email.com)"}
    try:
        resp = await client.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        feed = resp.json()
        logging.warning(f"[NWS DEBUG] Parsed {len(feed.get('features', []))} alerts")
    except Exception as e:
        logging.warning(f"[NWS SCRAPER ERROR] Async fetch failed: {e}")
        return {"entries": [], "error": str(e), "source": url}

    entries = []
    for feature in feed.get("features", []):
        props = feature.get("properties", {}) or {}
        enriched = _enrich_entry_from_props(props)
        if enriched:
            entries.append(enriched)
    return {"entries": entries, "source": url}
