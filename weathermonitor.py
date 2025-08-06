import time
import streamlit as st
import os
import sys
import logging
import gc
import asyncio
import nest_asyncio
from dateutil import parser as dateparser
from feeds import get_feed_definitions
from utils.scraper_registry import SCRAPER_REGISTRY
from streamlit_autorefresh import st_autorefresh
from computation import compute_counts
from renderer import RENDERERS
import httpx

# Allow nested asyncio loops under Streamlit
nest_asyncio.apply()

# Constants
FETCH_TTL = 60        # how often to refresh metadata
MAX_CONCURRENCY = 20  # parallel scrapers

# Ensure module path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Page config
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

# Auto-refresh every FETCH_TTL seconds
st_autorefresh(interval=FETCH_TTL * 1000, key="auto_refresh_main")

# Load feed definitions
FEED_CONFIG = get_feed_definitions()

# --- Session state: only last_seen values, no full data ---
now = time.time()
for key, conf in FEED_CONFIG.items():
    # timestamp for non‐rss_meteoalarm feeds
    st.session_state.setdefault(f"{key}_last_seen_time", 0.0)
    # for rss_meteoalarm, we keep a set of alert_ids
    if conf["type"] == "rss_meteoalarm":
        st.session_state.setdefault(f"{key}_last_seen_alerts", set())
st.session_state.setdefault("last_refreshed", now)
st.session_state.setdefault("active_feed", None)


# Unique ID for rss_meteoalarm alerts
def alert_id(e):
    return f"{e['level']}|{e['type']}|{e['from']}|{e['until']}"


# Async helper to fetch any number of feeds (used for metadata)
async def _fetch_all_feeds(configs: dict):
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    async with httpx.AsyncClient(timeout=30.0) as client:
        async def bound_fetch(key, conf):
            async with sem:
                try:
                    data = await SCRAPER_REGISTRY[conf["type"]](conf, client)
                except Exception as ex:
                    logging.warning(f"[{key.upper()} FETCH ERROR] {ex}")
                    data = {"entries": [], "error": str(ex), "source": conf}
                return key, data.get("entries", [])
        tasks = [bound_fetch(k, cfg) for k, cfg in configs.items()]
        return await asyncio.gather(*tasks)


# Run an async coroutine on the current loop
def run_async(coro):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro)


# --- 1) Metadata pass: fetch all feeds, compute badges ---
now = time.time()
to_fetch = {
    k: v for k, v in FEED_CONFIG.items()
    if now - st.session_state[f"{k}_last_fetch"] > FETCH_TTL
}
meta_entries = {}
if to_fetch:
    results = run_async(_fetch_all_feeds(to_fetch))
    for key, entries in results:
        # store nothing in session state except the timestamp
        st.session_state[f"{key}_last_fetch"] = now
        meta_entries[key] = entries
    st.session_state["last_refreshed"] = now

# For feeds not in to_fetch (cached), we still need entries for badges:
for key in FEED_CONFIG:
    if key not in meta_entries:
        # fetch from cache
        data = run_async(_fetch_all_feeds({key: FEED_CONFIG[key]}))[0][1]
        meta_entries[key] = data

# --- UI Header ---
st.title("Global Weather Monitor")
st.caption(
    f"Last refreshed: "
    f"{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}"
)
st.markdown("---")

# --- Buttons with badges ---
cols = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = meta_entries.get(key, [])
    # pick correct last_seen
    if conf["type"] == "rss_meteoalarm":
        last_seen = st.session_state[f"{key}_last_seen_alerts"]
    else:
        last_seen = st.session_state[f"{key}_last_seen_time"]
    # compute badge
    _, new_count = compute_counts(entries, conf, last_seen, alert_id_fn=alert_id)

    with cols[i]:
        clicked = st.button(conf["label"], key=f"btn_{key}_{i}", use_container_width=True)
        if new_count:
            st.markdown(
                f"<span style='margin-left:8px;"
                f"padding:2px 6px;border-radius:4px;background:#ffeecc;"
                f"font-size:0.9em;'>❗ {new_count} New</span>",
                unsafe_allow_html=True,
            )
        if clicked:
            st.session_state["active_feed"] = key

# --- 2) Detail pass: render only the active feed on click ---
active = st.session_state["active_feed"]
if active:
    conf = FEED_CONFIG[active]
    # fetch full entries (from cache or network)
    entries = run_async(_fetch_all_feeds({active: conf}))[0][1]

    # render with your existing grouped/simple renderers
    # pass 'key' so renderers can snapshot last_seen
    RENDERERS[conf["type"]](entries, {**conf, "key": active})

    # after rendering, advance last_seen
    if conf["type"] == "rss_meteoalarm":
        # snapshot set of alert_ids
        seen_ids = { alert_id(e) for e in entries }
        st.session_state[f"{active}_last_seen_alerts"] = seen_ids
    else:
        # snapshot by timestamp of latest entry
        try:
            newest_ts = max(
                dateparser.parse(e["published"]).timestamp() for e in entries
            )
        except:
            newest_ts = time.time()
        st.session_state[f"{active}_last_seen_time"] = newest_ts

    # separator and done
    st.markdown("---")
