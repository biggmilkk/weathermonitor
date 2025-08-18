import os
import sys
import time
import gc
import logging
import asyncio
import httpx
import psutil

import streamlit as st
from dateutil import parser as dateparser
from streamlit_autorefresh import st_autorefresh
import nest_asyncio

from feeds import get_feed_definitions
from utils.scraper_registry import SCRAPER_REGISTRY
from computation import compute_counts
from renderer import (
    RENDERERS,
    ec_remaining_new_total,
    draw_badge,                    
    safe_int,                       
    alert_id,                       
    meteoalarm_country_has_alerts,  
    meteoalarm_mark_and_sort,       
    meteoalarm_snapshot_ids,        
    render_empty_state,             
)

# --------------------------------------------------------------------
# Environment + Streamlit setup
# --------------------------------------------------------------------

# Avoid Linux inotify watcher exhaustion (graceful fallback to polling)
os.environ.setdefault("STREAMLIT_WATCHER_TYPE", "poll")

# Allow nested asyncio loops under Streamlit
nest_asyncio.apply()

# Page config FIRST (avoid emitting UI before this)
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

# --------------------------------------------------------------------
# Adaptive memory & concurrency autotuning
# --------------------------------------------------------------------

vm = psutil.virtual_memory()
# Use up to 50% of system RAM with a hard cap at 4 GiB for our internal limit
MEMORY_LIMIT = int(min(0.5 * vm.total, 4 * 1024**3))
MEMORY_HIGH_WATER = 0.85 * MEMORY_LIMIT
MEMORY_LOW_WATER  = 0.50 * MEMORY_LIMIT

MIN_CONC = 5
MAX_CONC = 50
STEP     = 5

def _rss_bytes() -> int:
    return psutil.Process(os.getpid()).memory_info().rss

# Initialize concurrency in session_state once
st.session_state.setdefault("concurrency", 20)

# Pre-fetch memory check nudge
rss_before = _rss_bytes()
if rss_before > MEMORY_HIGH_WATER:
    st.session_state["concurrency"] = max(MIN_CONC, st.session_state["concurrency"] - STEP)
elif rss_before < MEMORY_LOW_WATER:
    st.session_state["concurrency"] = min(MAX_CONC, st.session_state["concurrency"] + STEP)

MAX_CONCURRENCY = st.session_state["concurrency"]

# Small caption to help debug under load
st.caption(f"Concurrency: {MAX_CONCURRENCY}, RSS: {rss_before // (1024*1024)} MB")

# --------------------------------------------------------------------
# Constants & state
# --------------------------------------------------------------------

FETCH_TTL = 60  # seconds
HTTP2_ENABLED = False  # set True only if 'h2' is installed; otherwise HTTP/1.1 keeps it simple

# Ensure module path (prefer a package layout in the future)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Auto-refresh
st_autorefresh(interval=FETCH_TTL * 1000, key="auto_refresh_main")

# Cache feed definitions if they do any I/O
@st.cache_data(ttl=3600)
def load_feeds():
    return get_feed_definitions()

FEED_CONFIG = load_feeds()

now = time.time()
for key, conf in FEED_CONFIG.items():
    st.session_state.setdefault(f"{key}_data", [])
    st.session_state.setdefault(f"{key}_last_fetch", 0)
    st.session_state.setdefault(f"{key}_last_seen_time", 0.0)
    st.session_state.setdefault(f"{key}_pending_seen_time", None)
    if conf["type"] == "rss_meteoalarm":
        # store as tuple for deterministic serialization across processes
        st.session_state.setdefault(f"{key}_last_seen_alerts", tuple())
st.session_state.setdefault("last_refreshed", now)
st.session_state.setdefault("active_feed", None)

# --------------------------------------------------------------------
# Async HTTP fetching with tuned limits/retries
# --------------------------------------------------------------------

async def with_retries(fn, *, attempts=3, base_delay=0.5):
    for i in range(attempts):
        try:
            return await fn()
        except Exception as ex:
            if i == attempts - 1:
                raise
            await asyncio.sleep(base_delay * (2 ** i))

async def _fetch_all_feeds(configs: dict):
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    limits = httpx.Limits(
        max_connections=MAX_CONCURRENCY,
        max_keepalive_connections=MAX_CONCURRENCY
    )
    # lightweight retry at transport level
    transport = httpx.AsyncHTTPTransport(retries=3)

    timeout = httpx.Timeout(30.0)
    headers = {"User-Agent": "weathermonitor.app/1.0 (+support@weathermonitor.app)"}

    async with httpx.AsyncClient(
        timeout=timeout,
        limits=limits,
        transport=transport,
        http2=HTTP2_ENABLED,  # keep False unless httpx[http2] (h2) is installed
        headers=headers,
    ) as client:
        async def bound_fetch(key, conf):
            async with sem:
                async def call():
                    t0 = time.perf_counter()
                    data = await SCRAPER_REGISTRY[conf["type"]](conf, client)
                    dt_ms = (time.perf_counter() - t0) * 1000
                    logging.info(f"[{key}] {len(data.get('entries', []))} entries in {dt_ms:.1f} ms")
                    return data

                try:
                    data = await with_retries(call)
                except Exception as ex:
                    logging.warning(f"[{key.upper()} FETCH ERROR] {ex}")
                    data = {"entries": [], "error": str(ex), "source": conf}
                return key, data

        tasks = [bound_fetch(k, cfg) for k, cfg in configs.items()]
        return await asyncio.gather(*tasks)

def run_async(coro):
    """Safer event loop handling within Streamlit reruns: create/close per call."""
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        asyncio.set_event_loop(None)

def _immediate_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()

# --------------------------------------------------------------------
# Refresh stale feeds
# --------------------------------------------------------------------

now = time.time()
to_fetch = {k: v for k, v in FEED_CONFIG.items() if now - st.session_state[f"{k}_last_fetch"] > FETCH_TTL}
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
                last_seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
                _, new_count = compute_counts(entries, conf, last_seen_ids, alert_id_fn=alert_id)
                if new_count == 0:
                    # snapshot of all visible alerts
                    st.session_state[f"{key}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
            elif conf["type"] == "ec_async":
                # EC compact renderer manages NEW state per bucket; no auto snapshot here
                pass
            else:
                last_seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
                _, new_count = compute_counts(entries, conf, last_seen_ts)
                if new_count == 0:
                    st.session_state[f"{key}_last_seen_time"] = now

        # Keep the EC aggregate (warnings + Severe Thunderstorm Watch) up-to-date even if EC isn't opened yet
        if conf["type"] == "ec_async":
            st.session_state[f"{key}_remaining_new_total"] = ec_remaining_new_total(key, entries)

        gc.collect()

# Post-fetch memory nudge (react within the same minute)
rss_after = _rss_bytes()
if rss_after > MEMORY_HIGH_WATER:
    st.session_state["concurrency"] = max(MIN_CONC, st.session_state["concurrency"] - STEP)

# --------------------------------------------------------------------
# Header
# --------------------------------------------------------------------

st.title("Global Weather Monitor")
st.caption(
    f"Last refreshed: "
    f"{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}"
)
st.markdown("---")

# --------------------------------------------------------------------
# Feed buttons row — professional highlight using Streamlit's primary style
# --------------------------------------------------------------------

if not FEED_CONFIG:
    st.info("No feeds configured.")
    st.stop()

cols = st.columns(len(FEED_CONFIG))
badge_placeholders = {}
_toggled = False  # track whether we toggled so we can rerun exactly once

for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = st.session_state[f"{key}_data"]

    # Compute baseline NEW count
    if conf["type"] == "rss_meteoalarm":
        seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
        _, new_count = compute_counts(entries, conf, seen_ids, alert_id_fn=alert_id)
    elif conf["type"] == "ec_async":
        ec_total = st.session_state.get(f"{key}_remaining_new_total")
        if isinstance(ec_total, int):
            new_count = ec_total
        else:
            new_count = ec_remaining_new_total(key, entries)
            st.session_state[f"{key}_remaining_new_total"] = int(new_count or 0)
    else:
        seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
        _, new_count = compute_counts(entries, conf, seen_ts)

    with cols[i]:
        is_active = (st.session_state.get("active_feed") == key)

        # Professional, minimal highlight: use primary button type for the active feed
        clicked = st.button(
            conf.get("label", key.upper()),
            key=f"btn_{key}_{i}",
            use_container_width=True,
            type=("primary" if is_active else "secondary"),
        )

        # Badge placeholder and draw (moved style in renderer.draw_badge)
        badge_ph = st.empty()
        badge_placeholders[key] = badge_ph
        draw_badge(badge_ph, safe_int(new_count))

        # Click handling with immediate rerun to avoid "extra click" artifacts
        if clicked:
            if st.session_state.get("active_feed") == key:
                # Closing an open feed → snapshot "seen" where appropriate
                if conf["type"] == "rss_meteoalarm":
                    st.session_state[f"{key}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
                elif conf["type"] == "ec_async":
                    # EC per-bucket close/open handled inside renderer
                    pass
                else:
                    st.session_state[f"{key}_last_seen_time"] = time.time()
                st.session_state["active_feed"] = None
            else:
                # Opening a feed
                st.session_state["active_feed"] = key
                if conf["type"] == "rss_meteoalarm":
                    st.session_state[f"{key}_pending_seen_time"] = time.time()
                elif conf["type"] == "ec_async":
                    # EC: renderer manages per-bucket pending snapshots
                    st.session_state[f"{key}_pending_seen_time"] = None
                else:
                    st.session_state[f"{key}_pending_seen_time"] = time.time()
            _toggled = True

# Force a single rerun if any button toggled, so the highlight/badges are fresh immediately
if _toggled:
    _immediate_rerun()

# --------------------------------------------------------------------
# Display details for the active feed
# --------------------------------------------------------------------

active = st.session_state["active_feed"]
if active:
    st.markdown("---")
    conf = FEED_CONFIG[active]
    entries = st.session_state[f"{active}_data"]
    data_list = sorted(entries, key=lambda x: x.get("published", ""), reverse=True)

    # --- BOM (grouped) ---
    if conf["type"] == "rss_bom_multi":
        RENDERERS["rss_bom_multi"](entries, {**conf, "key": active})

    # --- Environment Canada compact grouped (warnings+watch only) ---
    elif conf["type"] == "ec_async":
        RENDERERS["ec_grouped_compact"](entries, {**conf, "key": active})

        # After rendering, recompute aggregate NEW using renderer's per-bucket last_seen map
        ec_total_now = ec_remaining_new_total(active, entries)
        st.session_state[f"{active}_remaining_new_total"] = int(ec_total_now)

        # Repaint the main badge now so closing a bucket updates the count immediately
        ph = badge_placeholders.get(active)
        if ph is not None:
            draw_badge(ph, safe_int(ec_total_now))

    # --- Meteoalarm (countries) ---
    elif conf["type"] == "rss_meteoalarm":
        seen_ids = set(st.session_state[f"{active}_last_seen_alerts"])

        countries = [c for c in data_list if meteoalarm_country_has_alerts(c)]
        if not countries:
            render_empty_state()
        else:
            countries = meteoalarm_mark_and_sort(countries, seen_ids)
            for country in countries:
                RENDERERS["rss_meteoalarm"](country, {**conf, "key": active})

            # Commit snapshot of all currently visible alerts
            st.session_state[f"{active}_last_seen_alerts"] = meteoalarm_snapshot_ids(countries)

    # --- JMA ---
    elif conf["type"] == "rss_jma":
        RENDERERS["rss_jma"](entries, {**conf, "key": active})

    # --- NWS (grouped compact, US) ---
    elif conf["type"] == "nws_grouped_compact":
        RENDERERS["nws_grouped_compact"](entries, {**conf, "key": active})
    
    else:
        # Generic item-per-row renderer (JSON/NWS/CMA etc.)
        seen_ts = st.session_state.get(f"{active}_last_seen_time") or 0.0
        if not data_list:
            render_empty_state()
        else:
            for item in data_list:
                pub = item.get("published")
                try:
                    ts = dateparser.parse(pub).timestamp() if pub else 0.0
                except Exception:
                    ts = 0.0
                item["is_new"] = bool(ts > seen_ts)  # renderer will draw left stripe if True
                RENDERERS.get(conf["type"], lambda i, c: None)(item, conf)

            # Snapshot last seen timestamp for generic feeds
            pkey = f"{active}_pending_seen_time"
            pending = st.session_state.get(pkey, None)
            if pending is not None:
                st.session_state[f"{active}_last_seen_time"] = float(pending)
            st.session_state.pop(pkey, None)
