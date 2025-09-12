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

# For mobile drill-in UX
st.session_state.setdefault("mobile_view", "list")  # "list" or "detail"

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

# Subtle toggle (top-right). No header, no hint.
st.session_state.setdefault("layout_mode", "Desktop")
spacer, toggle_col = st.columns([0.85, 0.15])
with toggle_col:
    layout_mode = st.radio(
        "",
        options=["Desktop", "Mobile"],
        index=(0 if st.session_state["layout_mode"] == "Desktop" else 1),
        label_visibility="collapsed",
        horizontal=True,
    )
    if layout_mode != st.session_state["layout_mode"]:
        st.session_state["layout_mode"] = layout_mode
        # Reset drill-in when switching modes
        st.session_state["mobile_view"] = "list"
        _immediate_rerun()

IS_MOBILE = (st.session_state["layout_mode"] == "Mobile")

st.markdown("---")

# --------------------------------------------------------------------
# Shared details renderer used by both layouts
# --------------------------------------------------------------------
def _render_feed_details(active, conf, entries, badge_placeholders=None):
    """Render the details block for a specific feed (used by desktop & mobile)."""
    data_list = sorted(entries, key=lambda x: x.get("published", ""), reverse=True)

    if conf["type"] == "rss_bom_multi":
        RENDERERS["rss_bom_multi"](entries, {**conf, "key": active})

    elif conf["type"] == "ec_async":
        # ---------------- EC: Mark all as seen button ----------------
        _PROVINCE_NAMES = {
            "AB": "Alberta", "BC": "British Columbia", "MB": "Manitoba",
            "NB": "New Brunswick", "NL": "Newfoundland and Labrador",
            "NT": "Northwest Territories", "NS": "Nova Scotia", "NU": "Nunavut",
            "ON": "Ontario", "PE": "Prince Edward Island", "QC": "Quebec",
            "SK": "Saskatchewan", "YT": "Yukon",
        }
        top_cols = st.columns([0.25, 0.75])
        with top_cols[0]:
            if st.button("Mark all as seen", key=f"{active}_mark_all_seen"):
                lastseen_key = f"{active}_bucket_last_seen"
                bucket_lastseen = st.session_state.get(lastseen_key, {}) or {}
                now_ts = time.time()

                # 1) Update all existing keys in the map (if any)
                for k in list(bucket_lastseen.keys()):
                    bucket_lastseen[k] = now_ts

                # 2) Also make sure current entries' buckets are present & set
                for e in entries:
                    bucket = ec_bucket_from_title(e.get("title","") or "")
                    if not bucket:
                        continue
                    code = e.get("province", "")
                    prov_name = _PROVINCE_NAMES.get(code, code) if isinstance(code, str) else str(code)
                    bkey = f"{prov_name}|{bucket}"
                    bucket_lastseen[bkey] = now_ts

                st.session_state[lastseen_key] = bucket_lastseen
                st.session_state[f"{active}_remaining_new_total"] = 0
                if badge_placeholders is not None:
                    ph = badge_placeholders.get(active)
                    if ph is not None:
                        draw_badge(ph, 0)
                _immediate_rerun()
        # -------------------------------------------------------------
        RENDERERS["ec_grouped_compact"](entries, {**conf, "key": active})

        # After rendering, recompute aggregate NEW using renderer's per-bucket last_seen map
        ec_total_now = ec_remaining_new_total(active, entries)
        st.session_state[f"{active}_remaining_new_total"] = int(ec_total_now)

        if badge_placeholders is not None:
            ph = badge_placeholders.get(active)
            if ph is not None:
                draw_badge(ph, safe_int(ec_total_now))

    elif conf["type"] == "nws_grouped_compact":
        # ---------------- NWS: Mark all as seen button ----------------
        top_cols = st.columns([0.25, 0.75])
        with top_cols[0]:
            if st.button("Mark all as seen", key=f"{active}_mark_all_seen"):
                lastseen_key = f"{active}_bucket_last_seen"
                bucket_lastseen = st.session_state.get(lastseen_key, {}) or {}
                now_ts = time.time()

                # Compute current buckets from entries: "State|Bucket"
                for a in entries:
                    state = (a.get("state") or a.get("state_name") or a.get("state_code") or "Unknown")
                    bucket = (a.get("bucket") or a.get("event") or a.get("title") or "Alert")
                    bkey = f"{state}|{bucket}"
                    bucket_lastseen[bkey] = now_ts

                st.session_state[lastseen_key] = bucket_lastseen

                # Also zero-out the badge immediately for UX consistency
                st.session_state[f"{active}_remaining_new_total"] = 0
                if badge_placeholders is not None:
                    ph = badge_placeholders.get(active)
                    if ph is not None:
                        draw_badge(ph, 0)

                _immediate_rerun()
        # -------------------------------------------------------------
        RENDERERS["nws_grouped_compact"](entries, {**conf, "key": active})
        nws_total_now = nws_remaining_new_total(active, entries)
        st.session_state[f"{active}_remaining_new_total"] = int(nws_total_now)
        if badge_placeholders is not None:
            ph = badge_placeholders.get(active)
            if ph is not None:
                draw_badge(ph, safe_int(nws_total_now))

    elif conf["type"] == "rss_meteoalarm":
        seen_ids = set(st.session_state[f"{active}_last_seen_alerts"])
        countries = [c for c in data_list if meteoalarm_country_has_alerts(c)]
        countries = meteoalarm_mark_and_sort(countries, seen_ids)
        for country in countries:
            RENDERERS["rss_meteoalarm"](country, {**conf, "key": active})
        st.session_state[f"{active}_last_seen_alerts"] = meteoalarm_snapshot_ids(countries)

    elif conf["type"] == "rss_jma":
        RENDERERS["rss_jma"](entries, {**conf, "key": active})

    else:
        # Generic item-per-row renderer (JSON/NWS legacy/CMA etc.)
        seen_ts = st.session_state.get(f"{active}_last_seen_time") or 0.0

        if not data_list:
            render_empty_state()
            pkey = f"{active}_pending_seen_time"
            pending = st.session_state.get(pkey, None)
            if pending is not None:
                st.session_state[f"{active}_last_seen_time"] = float(pending)
            st.session_state.pop(pkey, None)
        else:
            for item in data_list:
                pub = item.get("published")
                try:
                    ts = dateparser.parse(pub).timestamp() if pub else 0.0
                except Exception:
                    ts = 0.0
                item["is_new"] = bool(ts > seen_ts)  # renderer will draw left stripe if True
                RENDERERS.get(conf["type"], lambda i, c: None)(item, conf)
            pkey = f"{active}_pending_seen_time"
            pending = st.session_state.get(pkey, None)
            if pending is not None:
                st.session_state[f"{active}_last_seen_time"] = float(pending)
            st.session_state.pop(pkey, None)

# ======================================
# MOBILE MODE: Reddit-style drill-in UX
# ======================================
def _new_count_for(key, conf, entries):
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

if IS_MOBILE:
    if not FEED_CONFIG:
        st.info("No feeds configured.")
        st.stop()

    # Two states: "list" (feed selector) or "detail" (full-screen feed)
    if st.session_state["mobile_view"] == "list":
        # Stacked feed buttons with small counters
        for i, (key, conf) in enumerate(FEED_CONFIG.items()):
            entries = st.session_state[f"{key}_data"]
            cnt = _new_count_for(key, conf, entries)

            with st.container():
                cols = st.columns([0.75, 0.25])
                with cols[0]:
                    clicked = st.button(
                        conf.get("label", key.upper()),
                        key=f"m_list_btn_{key}_{i}",
                        use_container_width=True,
                        type="secondary",
                    )
                with cols[1]:
                    # Simple dot + number on the right
                    ph = st.empty()
                    draw_badge(ph, safe_int(cnt))

                if clicked:
                    st.session_state["active_feed"] = key
                    st.session_state["mobile_view"] = "detail"
                    _immediate_rerun()

            st.markdown("")  # spacing

    else:
        # DETAIL VIEW: takes full window; top bar with ✕ to go back
        active = st.session_state.get("active_feed")
        if not active:
            st.session_state["mobile_view"] = "list"
            _immediate_rerun()

        conf = FEED_CONFIG[active]
        entries = st.session_state[f"{active}_data"]

        # Top bar with close (✕) on the left and title centered
        st.markdown(
            """
            <style>
              .topbar { position: sticky; top: 0; z-index: 3;
                        background: var(--background-color, white);
                        padding: 8px 4px 6px 4px; border-bottom: 1px solid rgba(0,0,0,0.1);}
            </style>
            """,
            unsafe_allow_html=True,
        )
        with st.container():
            st.markdown('<div class="topbar">', unsafe_allow_html=True)
            tb = st.columns([0.15, 0.70, 0.15])
            with tb[0]:
                if st.button("✕", key="m_detail_close", use_container_width=True):
                    # Snapshot on close for generic + meteoalarm types
                    if conf["type"] == "rss_meteoalarm":
                        st.session_state[f"{active}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
                    elif conf["type"] not in ("ec_async", "nws_grouped_compact"):
                        st.session_state[f"{active}_last_seen_time"] = time.time()
                    st.session_state["mobile_view"] = "list"
                    st.session_state["active_feed"] = None
                    _immediate_rerun()
            with tb[1]:
                st.markdown(
                    f"#### {conf.get('label', active.upper())}",
                )
            with tb[2]:
                pass
            st.markdown("</div>", unsafe_allow_html=True)

        # Full-screen details
        _render_feed_details(active, conf, entries, badge_placeholders=None)

# ======================================
# DESKTOP MODE: Original row + details below
# ======================================
else:
    if not FEED_CONFIG:
        st.info("No feeds configured.")
        st.stop()

    cols = st.columns(len(FEED_CONFIG))
    badge_placeholders = {}
    _toggled = False  # to rerun once after interactions

    for i, (key, conf) in enumerate(FEED_CONFIG.items()):
        entries = st.session_state[f"{key}_data"]

        # Compute NEW counts
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
        elif conf["type"] == "nws_grouped_compact":
            nws_total = st.session_state.get(f"{key}_remaining_new_total")
            if isinstance(nws_total, int):
                new_count = nws_total
            else:
                new_count = nws_remaining_new_total(key, entries)
                st.session_state[f"{key}_remaining_new_total"] = int(new_count or 0)
        else:
            seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
            _, new_count = compute_counts(entries, conf, seen_ts)

        with cols[i]:
            is_active = (st.session_state.get("active_feed") == key)

            clicked = st.button(
                conf.get("label", key.upper()),
                key=f"btn_{key}_{i}",
                use_container_width=True,
                type=("primary" if is_active else "secondary"),
            )

            badge_ph = st.empty()
            badge_placeholders[key] = badge_ph
            draw_badge(badge_ph, safe_int(new_count))

            if clicked:
                if st.session_state.get("active_feed") == key:
                    # Closing snapshot
                    if conf["type"] == "rss_meteoalarm":
                        st.session_state[f"{key}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
                    elif conf["type"] in ("ec_async", "nws_grouped_compact"):
                        pass
                    else:
                        st.session_state[f"{key}_last_seen_time"] = time.time()
                    st.session_state["active_feed"] = None
                else:
                    st.session_state["active_feed"] = key
                    if conf["type"] == "rss_meteoalarm":
                        st.session_state[f"{key}_pending_seen_time"] = time.time()
                    elif conf["type"] in ("ec_async", "nws_grouped_compact"):
                        st.session_state[f"{key}_pending_seen_time"] = None
                    else:
                        st.session_state[f"{key}_pending_seen_time"] = time.time()
                _toggled = True

    if _toggled:
        _immediate_rerun()

    # Display details for the active feed (unchanged desktop behavior)
    active = st.session_state["active_feed"]
    if active:
        st.markdown("---")
        conf = FEED_CONFIG[active]
        entries = st.session_state[f"{active}_data"]
        _render_feed_details(active, conf, entries, badge_placeholders)
