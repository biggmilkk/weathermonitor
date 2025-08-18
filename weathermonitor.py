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
from renderer import RENDERERS, ec_remaining_new_total
import httpx
import psutil

# Allow nested asyncio loops under Streamlit
nest_asyncio.apply()

# Page config FIRST (avoid emitting UI before this)
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

# —— Autotuning constants ——
MEMORY_LIMIT = 1 * 1024**3
MEMORY_HIGH_WATER = 0.85 * MEMORY_LIMIT
MEMORY_LOW_WATER = 0.50 * MEMORY_LIMIT

MIN_CONC = 5
MAX_CONC = 50
STEP = 5

# Initialize in session_state once
st.session_state.setdefault("concurrency", 20)

# Measure current process RSS
proc = psutil.Process(os.getpid())
rss = proc.memory_info().rss

# Nudge concurrency based on memory
if rss > MEMORY_HIGH_WATER:
    st.session_state["concurrency"] = max(
        MIN_CONC, st.session_state["concurrency"] - STEP
    )
elif rss < MEMORY_LOW_WATER:
    st.session_state["concurrency"] = min(
        MAX_CONC, st.session_state["concurrency"] + STEP
    )

# Use this dynamic value everywhere you used MAX_CONCURRENCY
MAX_CONCURRENCY = st.session_state["concurrency"]

# (Optional) Print for debugging
st.caption(f"Concurrency: {MAX_CONCURRENCY}, RSS: {rss//(1024*1024)} MB")

# Constants
FETCH_TTL = 60

# Ensure module path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

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

# Unique ID for MeteoAlarm items
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

# Run async safely
def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            return asyncio.run_coroutine_threadsafe(coro, loop).result()
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)

# Refresh stale feeds
now = time.time()
to_fetch = {
    k: v
    for k, v in FEED_CONFIG.items()
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

        # When the active feed is open, decide whether to snapshot "seen"
        if st.session_state.get("active_feed") == key:
            if conf["type"] == "rss_meteoalarm":
                last_seen = st.session_state[f"{key}_last_seen_alerts"]
                _, new_count = compute_counts(
                    entries, conf, last_seen, alert_id_fn=alert_id
                )
                if new_count == 0:
                    snap = {
                        alert_id(e)
                        for country in entries
                        for alerts in country.get("alerts", {}).values()
                        for e in alerts
                    }
                    st.session_state[f"{key}_last_seen_alerts"] = snap
            elif conf["type"] == "ec_async":
                pass  # EC manages NEW state per bucket
            else:
                last_seen = st.session_state.get(f"{key}_last_seen_time") or 0.0
                _, new_count = compute_counts(
                    entries, conf, last_seen, alert_id_fn=alert_id
                )
                if new_count == 0:
                    st.session_state[f"{key}_last_seen_time"] = now

        # Keep the EC aggregate updated even if not opened yet
        if conf["type"] == "ec_async":
            st.session_state[f"{key}_remaining_new_total"] = ec_remaining_new_total(
                key, entries
            )

        gc.collect()

# Header
st.title("Global Weather Monitor")
st.caption(
    f"Last refreshed: "
    f"{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}"
)
st.markdown("---")

# --- helper to draw/update the "❗ N New" badge ---
def render_badge(ph, count: int):
    if count and count > 0:
        ph.markdown(
            "<span style='margin-left:8px;padding:2px 6px;"
            "border-radius:4px;background:#ffeecc;color:#000;font-size:0.9em;font-weight:bold;'>"
            f"❗ {count} New</span>",
            unsafe_allow_html=True,
        )
    else:
        ph.empty()

# Feed buttons
cols = st.columns(len(FEED_CONFIG))
badge_placeholders = {}

for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = st.session_state[f"{key}_data"]

    # Compute NEW count
    if conf["type"] == "rss_meteoalarm":
        seen = st.session_state[f"{key}_last_seen_alerts"]
        _, new_count = compute_counts(entries, conf, seen, alert_id_fn=alert_id)
    elif conf["type"] == "ec_async":
        ec_total = st.session_state.get(f"{key}_remaining_new_total")
        if isinstance(ec_total, int):
            new_count = ec_total
        else:
            new_count = ec_remaining_new_total(key, entries)
            st.session_state[f"{key}_remaining_new_total"] = int(new_count)
    else:
        seen = st.session_state.get(f"{key}_last_seen_time") or 0.0
        _, new_count = compute_counts(entries, conf, seen, alert_id_fn=alert_id)

    with cols[i]:
        clicked = st.button(conf["label"], key=f"btn_{key}_{i}", use_container_width=True)

        badge_ph = st.empty()
        badge_placeholders[key] = badge_ph
        render_badge(badge_ph, int(new_count) if new_count is not None else 0)

        if clicked:
            if st.session_state["active_feed"] == key:
                # Closing
                if conf["type"] == "rss_meteoalarm":
                    snap = {
                        alert_id(e)
                        for country in entries
                        for alerts in country.get("alerts", {}).values()
                        for e in alerts
                    }
                    st.session_state[f"{key}_last_seen_alerts"] = snap
                elif conf["type"] == "ec_async":
                    pass
                else:
                    st.session_state[f"{key}_last_seen_time"] = time.time()
                st.session_state["active_feed"] = None
            else:
                # Opening
                st.session_state["active_feed"] = key
                if conf["type"] == "rss_meteoalarm":
                    st.session_state[f"{key}_pending_seen_time"] = time.time()
                elif conf["type"] == "ec_async":
                    st.session_state[f"{key}_pending_seen_time"] = None
                else:
                    st.session_state[f"{key}_pending_seen_time"] = time.time()

# Display details
active = st.session_state["active_feed"]
if active:
    st.markdown("---")
    conf = FEED_CONFIG[active]
    entries = st.session_state[f"{active}_data"]
    data_list = sorted(entries, key=lambda x: x.get("published", ""), reverse=True)

    # --- BOM ---
    if conf["type"] == "rss_bom_multi":
        RENDERERS["rss_bom_multi"](entries, {**conf, "key": active})

    # --- Environment Canada ---
    elif conf["type"] == "ec_async":
        RENDERERS["ec_grouped_compact"](entries, {**conf, "key": active})
        ec_total_now = ec_remaining_new_total(active, entries)
        st.session_state[f"{active}_remaining_new_total"] = int(ec_total_now)
        ph = badge_placeholders.get(active)
        if ph is not None:
            render_badge(ph, int(ec_total_now))

    # --- Meteoalarm ---
    elif conf["type"] == "rss_meteoalarm":
        seen_ids = st.session_state[f"{active}_last_seen_alerts"]

        def _has_alerts(c):
            a = c.get("alerts", {})
            return any(a.get("today")) or any(a.get("tomorrow"))

        countries = [c for c in data_list if _has_alerts(c)]

        for country in countries:
            for alerts in country.get("alerts", {}).values():
                for e in alerts:
                    e["is_new"] = alert_id(e) not in seen_ids

        countries.sort(key=lambda c: (c.get("title", "").casefold()))

        for country in countries:
            alerts_flat = [
                e for alerts in country.get("alerts", {}).values() for e in alerts
            ]
            if any(e.get("is_new") for e in alerts_flat):
                st.markdown(
                    "<div style='height:4px;background:red;margin:8px 0;'></div>",
                    unsafe_allow_html=True,
                )
            RENDERERS["rss_meteoalarm"](country, {**conf, "key": active})

    # --- JMA ---
    elif conf["type"] == "rss_jma":
        RENDERERS["rss_jma"](entries, {**conf, "key": active})

    # --- Generic ---
    else:
        seen_ts = st.session_state.get(f"{active}_last_seen_time") or 0.0
        for item in data_list:
            pub = item.get("published")
            try:
                ts = dateparser.parse(pub).timestamp() if pub else 0.0
            except Exception:
                ts = 0.0
            if ts > seen_ts:
                st.markdown(
                    "<div style='height:4px;background:red;margin:8px 0;'></div>",
                    unsafe_allow_html=True,
                )
            RENDERERS.get(conf["type"], lambda i, c: None)(item, conf)

    # Snapshot last seen
    pkey = f"{active}_pending_seen_time"
    pending = st.session_state.get(pkey, None)

    if conf["type"] == "rss_meteoalarm":
        snap = {
            alert_id(e)
            for country in data_list
            for alerts in country.get("alerts", {}).values()
            for e in alerts
        }
        st.session_state[f"{active}_last_seen_alerts"] = snap
    elif conf["type"] == "ec_async":
        pass
    else:
        if pending is not None:
            st.session_state[f"{active}_last_seen_time"] = float(pending)

    st.session_state.pop(pkey, None)
