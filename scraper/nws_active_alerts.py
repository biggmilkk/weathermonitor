import streamlit as st
import requests
import logging
import httpx
import re
import json

# Only include these event types
ALLOWED_EVENTS = {
    "Severe Thunderstorm Warning",
    "Flash Flood Warning",
    "Tornado Warning",
    "Flood Warning",
    "Extreme Heat Warning",
    "Air Quality Alert",
}

# State/territory names + Marine bucket
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

MARINE_PREFIXES = {"ANZ","AMZ","GMZ","PZZ","PHZ","PKZ","PMZ"}  # common marine prefixes
_state_re = re.compile(r",\s*([A-Z]{2})(?:\s|$)")

def _infer_state_from_ugc(ugc_list) -> str | None:
    ugc_list = ugc_list or []
    for code in ugc_list:
        if isinstance(code, str) and len(code) >= 3 and code[:3] in MARINE_PREFIXES:
            return "MAR"
    for code in ugc_list:
        if isinstance(code, str) and len(code) >= 2 and code[:2].isalpha():
            return code[:2]
    return None

def _fallback_state_from_area(area_desc: str) -> str | None:
    if not area_desc:
        return None
    m = _state_re.search(area_desc)
    return m.group(1) if m else None

def _enrich_entry_from_props(props: dict) -> dict | None:
    event_type = props.get("event", "")
    if event_type not in ALLOWED_EVENTS:
        return None

    area_desc = props.get("areaDesc", "") or ""
    ugc = (props.get("geocode") or {}).get("UGC") or []

    state_code = _infer_state_from_ugc(ugc) or _fallback_state_from_area(area_desc)
    if not state_code:
        state_code = "MAR" if "marine" in (props.get("headline","").lower()) else "Unknown"

    state_name = STATE_NAMES.get(state_code, state_code)

    return {
        "title": props.get("headline", event_type),
        "summary": props.get("description", ""),
        "link": props.get("web", "") or props.get("uri", ""),
        "published": props.get("effective", "") or props.get("sent", ""),
        "region": area_desc,
        "state_code": state_code,
        "state": state_name,
        "event": event_type,
        "bucket": event_type,
    }

def _parse_json_response_like(resp) -> dict:
    try:
        return resp.json()
    except Exception as ex:
        # Try manual loads so we can show a better error
        try:
            text = resp.text if hasattr(resp, "text") else resp.content.decode("utf-8", "replace")
        except Exception:
            text = "<unreadable>"
        ct = resp.headers.get("content-type", "unknown") if hasattr(resp, "headers") else "unknown"
        status = resp.status_code if hasattr(resp, "status_code") else "unknown"
        snippet = text[:400].replace("\n", " ")
        logging.warning(f"[NWS DEBUG] Non-JSON? status={status} content-type={ct} body[:400]={snippet!r}")
        raise

@st.cache_data(ttl=60, show_spinner=False)
def scrape_nws(conf: dict) -> dict:
    url = conf.get("url", "https://api.weather.gov/alerts/active")
    headers = {
        "User-Agent": "WeatherMonitorApp (support@weathermonitor.app)",
        "Accept": "application/geo+json",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        feed = _parse_json_response_like(resp)
        logging.info(f"[NWS] parsed {len(feed.get('features', []))} alerts")
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
    url = conf.get("url", "https://api.weather.gov/alerts/active")
    headers = {
        "User-Agent": "WeatherMonitorApp (support@weathermonitor.app)",
        "Accept": "application/geo+json",
    }
    try:
        resp = await client.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        feed = _parse_json_response_like(resp)
        logging.info(f"[NWS] parsed {len(feed.get('features', []))} alerts")
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
