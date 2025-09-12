import os
import sys
import time
import gc
import logging
import asyncio
import httpx
import psutil
import traceback

import streamlit as st
from dateutil import parser as dateparser
from streamlit_autorefresh import st_autorefresh
import nest_asyncio

from feeds import get_feed_definitions
from utils.scraper_registry import SCRAPER_REGISTRY
from computation import compute_counts
from renderer import (
    RENDERERS,
    # Aggregates used for main-button "❗ New" counters
    ec_remaining_new_total,
    nws_remaining_new_total,
    ec_bucket_from_title,          # <-- needed for EC "mark all as seen"
    # Small UI + ID helpers
    draw_badge,
    safe_int,
    alert_id,
    meteoalarm_country_has_alerts,
    meteoalarm_mark_and_sort,
    meteoalarm_snapshot_ids,
    # Empty state renderer
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
        except Exception:
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
                    # ---- Build a merged conf for the scraper ----
                    # Merge top-level keys (except label/type) + expand nested "conf" dict.
                    call_conf = {}
                    for k, v in conf.items():
                        if k in ("label", "type"):
                            continue
                        if k == "conf" and isinstance(v, dict):
                            call_conf.update(v)
                        else:
                            call_conf[k] = v

                    t0 = time.perf_counter()
                    data = await SCRAPER_REGISTRY[conf["type"]](call_conf, client)
                    dt_ms = (time.perf_counter() - t0) * 1000
                    logging.info(f"[{key}] {len(data.get('entries', []))} entries in {dt_ms:.1f} ms")
                    return data

                try:
                    data = await with_retries(call)
                except Exception as ex:
                    logging.warning(f"[{key.upper()} FETCH ERROR] {ex}")
                    logging.warning(traceback.format_exc())
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
                    # If nothing new, snapshot all currently visible IDs
                    st.session_state[f"{key}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
            elif conf["type"] == "ec_async":
                # EC compact renderer manages NEW state per bucket; no auto snapshot here
                pass
            elif conf["type"] == "nws_grouped_compact":
                # NWS compact renderer manages per-bucket snapshots internally
                pass
            else:
                last_seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
                _, new_count = compute_counts(entries, conf, last_seen_ts)
                if new_count == 0:
                    st.session_state[f"{key}_last_seen_time"] = now

        # Keep EC and NWS aggregates up-to-date even if the feed isn't opened yet
        if conf["type"] == "ec_async":
            st.session_state[f"{key}_remaining_new_total"] = ec_remaining_new_total(key, entries)
        elif conf["type"] == "nws_grouped_compact":
            st.session_state[f"{key}_remaining_new_total"] = nws_remaining_new_total(key, entries)

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

# ====================================================================
# RESPONSIVE LAYOUT
#   Desktop = two-column master/detail (radio list → details on right)
#   Mobile  = accordion (each feed expands below its own button)
# ====================================================================

# Viewport width (if your component sets this, great; otherwise default to desktop)
vw = st.session_state.get("vw", 1200)
IS_MOBILE = bool(vw <= 768)

# Optional quick filter (useful with many feeds)
filter_q = st.text_input("Filter feeds", "", placeholder="Type to filter…")

def _matches(label: str) -> bool:
    return (filter_q.strip().lower() in label.lower()) if filter_q else True

# Compute per-feed "new" counters for labels/badges
def _count_for(key, conf, entries):
    if conf["type"] == "rss_meteoalarm":
        seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
        _, new_count = compute_counts(entries, conf, seen_ids, alert_id_fn=alert_id)
        return new_count
    if conf["type"] == "ec_async":
        val = st.session_state.get(f"{key}_remaining_new_total")
        return int(val) if isinstance(val, int) else int(ec_remaining_new_total(key, entries) or 0)
    if conf["type"] == "nws_grouped_compact":
        val = st.session_state.get(f"{key}_remaining_new_total")
        return int(val) if isinstance(val, int) else int(nws_remaining_new_total(key, entries) or 0)
    seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
    _, new_count = compute_counts(entries, conf, seen_ts)
    return new_count

# Render the selected feed (shared by desktop & mobile)
def _render_selected_feed(selected_key: str):
    if not selected_key:
        return
    conf = FEED_CONFIG[selected_key]
    entries = st.session_state[f"{selected_key}_data"]
    data_list = sorted(entries, key=lambda x: x.get("published", ""), reverse=True)

    # --- BOM (grouped) ---
    if conf["type"] == "rss_bom_multi":
        RENDERERS["rss_bom_multi"](entries, {**conf, "key": selected_key})

    # --- Environment Canada compact grouped ---
    elif conf["type"] == "ec_async":
        _PROVINCE_NAMES = {
            "AB": "Alberta", "BC": "British Columbia", "MB": "Manitoba",
            "NB": "New Brunswick", "NL": "Newfoundland and Labrador",
            "NT": "Northwest Territories", "NS": "Nova Scotia", "NU": "Nunavut",
            "ON": "Ontario", "PE": "Prince Edward Island", "QC": "Quebec",
            "SK": "Saskatchewan", "YT": "Yukon",
        }
        top_cols = st.columns([0.25, 0.75])
        with top_cols[0]:
            if st.button("Mark all as seen", key=f"{selected_key}_mark_all_seen"):
                lastseen_key = f"{selected_key}_bucket_last_seen"
                bucket_lastseen = st.session_state.get(lastseen_key, {}) or {}
                now_ts = time.time()

                # Update any existing keys
                for k2 in list(bucket_lastseen.keys()):
                    bucket_lastseen[k2] = now_ts

                # Ensure current entries' buckets are set
                for e in entries:
                    bucket = ec_bucket_from_title(e.get("title","") or "")
                    if not bucket:
                        continue
                    code = e.get("province", "")
                    prov_name = _PROVINCE_NAMES.get(code, code) if isinstance(code, str) else str(code)
                    bkey2 = f"{prov_name}|{bucket}"
                    bucket_lastseen[bkey2] = now_ts

                st.session_state[lastseen_key] = bucket_lastseen
                st.session_state[f"{selected_key}_remaining_new_total"] = 0
                _immediate_rerun()

        RENDERERS["ec_grouped_compact"](entries, {**conf, "key": selected_key})

        # Refresh EC aggregate count
        ec_total_now = ec_remaining_new_total(selected_key, entries)
        st.session_state[f"{selected_key}_remaining_new_total"] = int(ec_total_now)

    # --- NWS grouped compact (US) ---
    elif conf["type"] == "nws_grouped_compact":
        top_cols = st.columns([0.25, 0.75])
        with top_cols[0]:
            if st.button("Mark all as seen", key=f"{selected_key}_mark_all_seen"):
                lastseen_key = f"{selected_key}_bucket_last_seen"
                bucket_lastseen = st.session_state.get(lastseen_key, {}) or {}
                now_ts = time.time()
                for a in entries:
                    state = (a.get("state") or a.get("state_name") or a.get("state_code") or "Unknown")
                    bucket = (a.get("bucket") or a.get("event") or a.get("title") or "Alert")
                    bkey2 = f"{state}|{bucket}"
                    bucket_lastseen[bkey2] = now_ts
                st.session_state[lastseen_key] = bucket_lastseen
                st.session_state[f"{selected_key}_remaining_new_total"] = 0
                _immediate_rerun()

        RENDERERS["nws_grouped_compact"](entries, {**conf, "key": selected_key})
        nws_total_now = nws_remaining_new_total(selected_key, entries)
        st.session_state[f"{selected_key}_remaining_new_total"] = int(nws_total_now)

    # --- Meteoalarm (countries) ---
    elif conf["type"] == "rss_meteoalarm":
        seen_ids = set(st.session_state[f"{selected_key}_last_seen_alerts"])
        countries = [c for c in data_list if meteoalarm_country_has_alerts(c)]
        countries = meteoalarm_mark_and_sort(countries, seen_ids)
        for country in countries:
            RENDERERS["rss_meteoalarm"](country, {**conf, "key": selected_key})
        # Snapshot after rendering
        st.session_state[f"{selected_key}_last_seen_alerts"] = meteoalarm_snapshot_ids(countries)

    # --- JMA ---
    elif conf["type"] == "rss_jma":
        RENDERERS["rss_jma"](entries, {**conf, "key": selected_key})

    # --- Generic item-per-row renderer ---
    else:
        seen_ts = st.session_state.get(f"{selected_key}_last_seen_time") or 0.0

        if not data_list:
            render_empty_state()
            pkey = f"{selected_key}_pending_seen_time"
            pending = st.session_state.get(pkey, None)
            if pending is not None:
                st.session_state[f"{selected_key}_last_seen_time"] = float(pending)
            st.session_state.pop(pkey, None)
        else:
            for item in data_list:
                pub = item.get("published")
                try:
                    ts = dateparser.parse(pub).timestamp() if pub else 0.0
                except Exception:
                    ts = 0.0
                item["is_new"] = bool(ts > seen_ts)
                RENDERERS.get(conf["type"], lambda i, c: None)(item, conf)

            # Snapshot last seen timestamp
            pkey = f"{selected_key}_pending_seen_time"
            pending = st.session_state.get(pkey, None)
            if pending is not None:
                st.session_state[f"{selected_key}_last_seen_time"] = float(pending)
            st.session_state.pop(pkey, None)

# Build visible/ordered keys (respecting filter)
ordered_keys = [k for k in FEED_CONFIG.keys() if _matches(FEED_CONFIG[k].get("label", k.upper()))]
if not ordered_keys:
    st.info("No feeds match your filter.")
    st.stop()

# Keep a stable selection
st.session_state.setdefault("active_feed", ordered_keys[0])
if st.session_state["active_feed"] not in ordered_keys:
    st.session_state["active_feed"] = ordered_keys[0]

# ---------------- DESKTOP: two-column master/detail ----------------
if not IS_MOBILE:
    left, right = st.columns([0.33, 0.67])

    with left:
        # Vertical radio selector with counts (stable UX, keyboard-friendly)
        def fmt(k):
            cnt = _count_for(k, FEED_CONFIG[k], st.session_state[f"{k}_data"])
            lab = FEED_CONFIG[k].get("label", k.upper())
            return f"{lab}  {'• ' if cnt else ''}{cnt if cnt else ''}"

        sel = st.radio(
            "Feeds",
            options=ordered_keys,
            index=ordered_keys.index(st.session_state["active_feed"]),
            format_func=fmt,
            label_visibility="collapsed",
        )
        if sel != st.session_state["active_feed"]:
            st.session_state["active_feed"] = sel
            _immediate_rerun()

    with right:
        st.markdown(
            f"### {FEED_CONFIG[st.session_state['active_feed']].get('label', st.session_state['active_feed'].upper())}"
        )
        _render_selected_feed(st.session_state["active_feed"])

# ---------------- MOBILE: accordion (expand under the tapped button) ----------------
else:
    for i, key in enumerate(ordered_keys):
        conf = FEED_CONFIG[key]
        entries = st.session_state[f"{key}_data"]
        cnt = _count_for(key, conf, entries)

        with st.container():
            is_active = (st.session_state["active_feed"] == key)
            clicked = st.button(
                conf.get("label", key.upper()) + (f"  · {cnt}" if cnt else ""),
                key=f"btn_m_{key}_{i}",
                use_container_width=True,
                type=("primary" if is_active else "secondary"),
            )
            if clicked and not is_active:
                st.session_state["active_feed"] = key
                _immediate_rerun()

            # Inline details directly beneath this button
            if is_active:
                st.markdown("---")
                _render_selected_feed(key)

        st.markdown("")  # spacing between accordion blocks
