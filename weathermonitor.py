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
import psutil

# Allow nested asyncio loops under Streamlit
nest_asyncio.apply()

# —— Autotuning constants —— 
MEMORY_LIMIT = 1 * 1024**3         # 1 GiB
MEMORY_HIGH_WATER = 0.85 * MEMORY_LIMIT
MEMORY_LOW_WATER  = 0.50 * MEMORY_LIMIT

MIN_CONC = 5
MAX_CONC = 50
STEP     = 5

# Initialize in session_state once
st.session_state.setdefault("concurrency", 20)

# Measure current process RSS
proc = psutil.Process(os.getpid())
rss  = proc.memory_info().rss

# Nudge concurrency based on memory
if rss > MEMORY_HIGH_WATER:
    # memory too high → back off
    st.session_state["concurrency"] = max(
        MIN_CONC, st.session_state["concurrency"] - STEP
    )
elif rss < MEMORY_LOW_WATER:
    # memory comfortably low → can push it up
    st.session_state["concurrency"] = min(
        MAX_CONC, st.session_state["concurrency"] + STEP
    )

# Use this dynamic value everywhere you used MAX_CONCURRENCY
MAX_CONCURRENCY = st.session_state["concurrency"]

# (Optional) Print for debugging
st.caption(f"Concurrency: {MAX_CONCURRENCY}, RSS: {rss//(1024*1024)} MB")

# Constants
FETCH_TTL = 60
MAX_CONCURRENCY = 20

# Ensure module path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Page config
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

# Auto-refresh
st_autorefresh(interval=FETCH_TTL * 1000, key="auto_refresh_main")

# Load feeds and init session state
FEED_CONFIG = get_feed_definitions()
now = time.time()
for key, conf in FEED_CONFIG.items():
    st.session_state.setdefault(f"{key}_data", [])
    st.session_state.setdefault(f"{key}_last_fetch", 0)
    st.session_state.setdefault(f"{key}_last_seen_time", 0.0)
    st.session_state.setdefault(f"{key}_pending_seen_time", None)
    if conf["type"] == "rss_meteoalarm":
        st.session_state.setdefault(f"{key}_last_seen_alerts", set())
st.session_state.setdefault("last_refreshed", now)
st.session_state.setdefault("active_feed", None)

# Unique ID for MeteoAlarm
def alert_id(e):
    return f"{e['level']}|{e['type']}|{e['from']}|{e['until']}"

# Async fetcher
async def _fetch_all_feeds(configs):
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    async with httpx.AsyncClient(timeout=30.0) as client:
        async def bound_fetch(key, conf):
            async with sem:
                try:
                    data = await SCRAPER_REGISTRY[conf["type"]](conf, client)
                except Exception as ex:
                    logging.warning(f"[{key.upper()} FETCH ERROR] {ex}")
                    data = {"entries": [], "error": str(ex), "source": conf}
                return key, data
        tasks = [bound_fetch(k, cfg) for k, cfg in configs.items()]
        return await asyncio.gather(*tasks)

# Run async on current loop
def run_async(coro):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro)

# Refresh stale feeds
now = time.time()
to_fetch = {
    k: v for k, v in FEED_CONFIG.items()
    if now - st.session_state[f"{k}_last_fetch"] > FETCH_TTL
}
if to_fetch:
    results = run_async(_fetch_all_feeds(to_fetch))
    for key, raw in results:
        entries = raw.get("entries", [])
        st.session_state[f"{key}_data"] = entries
        st.session_state[f"{key}_last_fetch"] = now
        st.session_state["last_refreshed"] = now
        conf = FEED_CONFIG[key]
        if st.session_state.get("active_feed") == key:
            last_seen = (
                st.session_state[f"{key}_last_seen_alerts"]
                if conf["type"] == "rss_meteoalarm"
                else st.session_state[f"{key}_last_seen_time"]
            )
            _, new_count = compute_counts(entries, conf, last_seen, alert_id_fn=alert_id)
            if new_count == 0:
                if conf["type"] == "rss_meteoalarm":
                    snap = {
                        alert_id(e)
                        for country in entries
                        for alerts in country.get("alerts", {}).values()
                        for e in alerts
                    }
                    st.session_state[f"{key}_last_seen_alerts"] = snap
                else:
                    st.session_state[f"{key}_last_seen_time"] = now
        gc.collect()

# Header
st.title("Global Weather Monitor")
st.caption(
    f"Last refreshed: "
    f"{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}"
)
st.markdown("---")

# Feed buttons
cols = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = st.session_state[f"{key}_data"]
    seen = (
        st.session_state[f"{key}_last_seen_alerts"]
        if conf["type"] == "rss_meteoalarm"
        else st.session_state[f"{key}_last_seen_time"]
    )
    _, new_count = compute_counts(entries, conf, seen, alert_id_fn=alert_id)
    with cols[i]:
        clicked = st.button(
            conf["label"], key=f"btn_{key}_{i}", use_container_width=True
        )
        if new_count > 0:
            st.markdown(
                "<span style='margin-left:8px;padding:2px 6px;"
                "border-radius:4px;background:#ffeecc;font-size:0.9em;'>"
                f"❗ {new_count} New</span>",
                unsafe_allow_html=True,
            )
        if clicked:
            if st.session_state["active_feed"] == key:
                if conf["type"] == "rss_meteoalarm":
                    snap = {
                        alert_id(e)
                        for country in entries
                        for alerts in country.get("alerts", {}).values()
                        for e in alerts
                    }
                    st.session_state[f"{key}_last_seen_alerts"] = snap
                else:
                    st.session_state[f"{key}_last_seen_time"] = time.time()
                st.session_state["active_feed"] = None
            else:
                st.session_state["active_feed"] = key
                st.session_state[f"{key}_pending_seen_time"] = time.time()

# Display details
active = st.session_state["active_feed"]
if active:
    st.markdown("---")
    conf = FEED_CONFIG[active]
    entries = st.session_state[f"{active}_data"]
    data_list = sorted(entries, key=lambda x: x.get("published", ""), reverse=True)

    if conf["type"] == "rss_bom_multi":
        # BOM
        RENDERERS["rss_bom_multi"](entries, {**conf, "key": active})

    elif conf["type"] == "ec_async":
        # Environment Canada: grouped & ordered renderer
        RENDERERS["ec_grouped"](entries, {**conf, "key": active})

    elif conf["type"] == "rss_meteoalarm":
        # MeteoAlarm rendering
        seen_ids = st.session_state[f"{active}_last_seen_alerts"]
        for country in data_list:
            for alerts in country.get("alerts", {}).values():
                for e in alerts:
                    e["is_new"] = alert_id(e) not in seen_ids

    elif conf["type"] == "rss_jma":
        # JMA
        for item in entries:
            RENDERERS["rss_jma"](item, conf)    

        # red-bar + render
        for country in data_list:
            alerts_flat = [
                e for alerts in country.get("alerts", {}).values() for e in alerts
            ]
            if any(e.get("is_new") for e in alerts_flat):
                st.markdown(
                    "<div style='height:4px;background:red;margin:8px 0;'></div>",
                    unsafe_allow_html=True,
                )
            RENDERERS.get(conf["type"], lambda i, c: None)(country, conf)

    else:
        # Generic rendering
        seen = st.session_state[f"{active}_last_seen_time"]
        for item in data_list:
            pub = item.get("published")
            try:
                ts = dateparser.parse(pub).timestamp() if pub else 0.0
            except:
                ts = 0.0
            if ts > seen:
                st.markdown(
                    "<div style='height:4px;background:red;margin:8px 0;'></div>",
                    unsafe_allow_html=True,
                )
            RENDERERS.get(conf["type"], lambda i, c: None)(item, conf)

    # Snapshot last seen timestamps or alerts
    pkey = f"{active}_pending_seen_time"
    if pkey in st.session_state:
        if conf["type"] == "rss_meteoalarm":
            snap = {
                alert_id(e)
                for country in data_list
                for alerts in country.get("alerts", {}).values()
                for e in alerts
            }
            st.session_state[f"{active}_last_seen_alerts"] = snap
        else:
            st.session_state[f"{active}_last_seen_time"] = st.session_state.pop(pkey)
        st.session_state.pop(pkey, None)
