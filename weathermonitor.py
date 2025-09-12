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

# NEW: custom component for viewport width
from mobile_detect import mobile_viewport_width, is_mobile_width

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

st.markdown(
    """
    <style>
    /* Hide skeleton placeholder divs inserted by Streamlit */
    [data-testid="stSkeleton"] {
        display: none !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

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

# --------------------------------------------------------------------
# ONE-LAYOUT SWITCH with AUTO-DETECT (custom component)
#   Priority: query (?view=mobile/desktop) > env/secrets > auto width (component)
# --------------------------------------------------------------------

# Ask the invisible component for viewport width; returns int after first render
vw = mobile_viewport_width(default=None)  # None until first value arrives

# Read overrides
try:
    qp = st.query_params  # Streamlit >= 1.31
    view_override = (qp.get("view") or "").lower()
except Exception:
    try:
        view_override = (st.experimental_get_query_params().get("view", [""])[0] or "").lower()
    except Exception:
        view_override = ""

FORCE_MOBILE_ENV = os.environ.get("FORCE_MOBILE", "")
FORCE_MOBILE_SECRET = (st.secrets.get("FORCE_MOBILE", "") if hasattr(st, "secrets") else "")

if view_override == "mobile" or FORCE_MOBILE_ENV == "1" or FORCE_MOBILE_SECRET == "1":
    IS_MOBILE_COMPACT = True
elif view_override == "desktop":
    IS_MOBILE_COMPACT = False
else:
    # AUTO: fall back to component-reported width (works in iOS Safari)
    IS_MOBILE_COMPACT = is_mobile_width(vw, threshold=768)

# Optional smoothing: remember last decision until component reports
if vw is None and "last_layout_mobile" in st.session_state:
    IS_MOBILE_COMPACT = st.session_state["last_layout_mobile"]
else:
    st.session_state["last_layout_mobile"] = IS_MOBILE_COMPACT

# --------------------------------------------------------------------
# Shared helpers
# --------------------------------------------------------------------

def _compute_new_count_for_feed(key, conf, entries):
    # Compute baseline NEW count (per-feed strategy) — mirrors original logic
    if conf["type"] == "rss_meteoalarm":
        seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
        _, new_count = compute_counts(entries, conf, seen_ids, alert_id_fn=alert_id)
        return new_count
    elif conf["type"] == "ec_async":
        ec_total = st.session_state.get(f"{key}_remaining_new_total")
        if isinstance(ec_total, int):
            return ec_total
        new_count = ec_remaining_new_total(key, entries)
        st.session_state[f"{key}_remaining_new_total"] = int(new_count or 0)
        return new_count
    elif conf["type"] == "nws_grouped_compact":
        nws_total = st.session_state.get(f"{key}_remaining_new_total")
        if isinstance(nws_total, int):
            return nws_total
        new_count = nws_remaining_new_total(key, entries)
        st.session_state[f"{key}_remaining_new_total"] = int(new_count or 0)
        return new_count
    else:
        seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
        _, new_count = compute_counts(entries, conf, seen_ts)
        return new_count

def _render_feed_inline(key, conf, entries, badge_placeholders):
    """Inline renderer under the active button, reusing your existing per-feed logic."""
    data_list = sorted(entries, key=lambda x: x.get("published", ""), reverse=True)

    if conf["type"] == "rss_bom_multi":
        RENDERERS["rss_bom_multi"](entries, {**conf, "key": key})

    elif conf["type"] == "ec_async":
        # Mark all as seen (inline)
        _PROVINCE_NAMES = {
            "AB": "Alberta", "BC": "British Columbia", "MB": "Manitoba",
            "NB": "New Brunswick", "NL": "Newfoundland and Labrador",
            "NT": "Northwest Territories", "NS": "Nova Scotia", "NU": "Nunavut",
            "ON": "Ontario", "PE": "Prince Edward Island", "QC": "Quebec",
            "SK": "Saskatchewan", "YT": "Yukon",
        }
        top_cols = st.columns([0.5, 0.5])
        with top_cols[0]:
            if st.button("Mark all as seen", key=f"{key}_mark_all_seen_inline"):
                lastseen_key = f"{key}_bucket_last_seen"
                bucket_lastseen = st.session_state.get(lastseen_key, {}) or {}
                now_ts = time.time()
                for k2 in list(bucket_lastseen.keys()):
                    bucket_lastseen[k2] = now_ts
                for e in entries:
                    bucket = ec_bucket_from_title(e.get("title","") or "")
                    if not bucket:
                        continue
                    code = e.get("province", "")
                    prov_name = _PROVINCE_NAMES.get(code, code) if isinstance(code, str) else str(code)
                    bkey2 = f"{prov_name}|{bucket}"
                    bucket_lastseen[bkey2] = now_ts
                st.session_state[lastseen_key] = bucket_lastseen
                st.session_state[f"{key}_remaining_new_total"] = 0
                ph2 = badge_placeholders.get(key)
                if ph2 is not None:
                    draw_badge(ph2, 0)
                _immediate_rerun()

        RENDERERS["ec_grouped_compact"](entries, {**conf, "key": key})
        ec_total_now = ec_remaining_new_total(key, entries)
        st.session_state[f"{key}_remaining_new_total"] = int(ec_total_now)
        ph2 = badge_placeholders.get(key)
        if ph2 is not None:
            draw_badge(ph2, safe_int(ec_total_now))

    elif conf["type"] == "nws_grouped_compact":
        top_cols = st.columns([0.5, 0.5])
        with top_cols[0]:
            if st.button("Mark all as seen", key=f"{key}_mark_all_seen_inline"):
                lastseen_key = f"{key}_bucket_last_seen"
                bucket_lastseen = st.session_state.get(lastseen_key, {}) or {}
                now_ts = time.time()
                for a in entries:
                    state = (a.get("state") or a.get("state_name") or a.get("state_code") or "Unknown")
                    bucket = (a.get("bucket") or a.get("event") or a.get("title") or "Alert")
                    bkey2 = f"{state}|{bucket}"
                    bucket_lastseen[bkey2] = now_ts
                st.session_state[lastseen_key] = bucket_lastseen
                st.session_state[f"{key}_remaining_new_total"] = 0
                ph2 = badge_placeholders.get(key)
                if ph2 is not None:
                    draw_badge(ph2, 0)
                _immediate_rerun()

        RENDERERS["nws_grouped_compact"](entries, {**conf, "key": key})
        nws_total_now = nws_remaining_new_total(key, entries)
        st.session_state[f"{key}_remaining_new_total"] = int(nws_total_now)
        ph2 = badge_placeholders.get(key)
        if ph2 is not None:
            draw_badge(ph2, safe_int(nws_total_now))

    elif conf["type"] == "rss_meteoalarm":
        seen_ids_inline = set(st.session_state[f"{key}_last_seen_alerts"])
        countries = [c for c in data_list if meteoalarm_country_has_alerts(c)]
        countries = meteoalarm_mark_and_sort(countries, seen_ids_inline)
        for country in countries:
            RENDERERS["rss_meteoalarm"](country, {**conf, "key": key})
        st.session_state[f"{key}_last_seen_alerts"] = meteoalarm_snapshot_ids(countries)

    elif conf["type"] == "rss_jma":
        RENDERERS["rss_jma"](entries, {**conf, "key": key})

    else:
        # Generic item-per-row renderer (JSON/NWS legacy/CMA etc.)
        seen_ts_inline = st.session_state.get(f"{key}_last_seen_time") or 0.0
        if not data_list:
            render_empty_state()
            pkey = f"{key}_pending_seen_time"
            pending = st.session_state.get(pkey, None)
            if pending is not None:
                st.session_state[f"{key}_last_seen_time"] = float(pending)
            st.session_state.pop(pkey, None)
        else:
            for item in data_list:
                pub = item.get("published")
                try:
                    ts = dateparser.parse(pub).timestamp() if pub else 0.0
                except Exception:
                    ts = 0.0
                item["is_new"] = bool(ts > seen_ts_inline)
                RENDERERS.get(conf["type"], lambda i, c: None)(item, conf)
            pkey = f"{key}_pending_seen_time"
            pending = st.session_state.get(pkey, None)
            if pending is not None:
                st.session_state[f"{key}_last_seen_time"] = float(pending)
            st.session_state.pop(pkey, None)

# --------------------------------------------------------------------
# Render either MOBILE (stacked + inline) OR DESKTOP (original row)
# --------------------------------------------------------------------

if IS_MOBILE_COMPACT:
    # ---- MOBILE: stacked list; active feed renders inline under its button ----
    if not FEED_CONFIG:
        st.info("No feeds configured.")
        st.stop()

    badge_placeholders = {}
    _toggled = False

    for i, (key, conf) in enumerate(FEED_CONFIG.items()):
        entries = st.session_state[f"{key}_data"]
        new_count = _compute_new_count_for_feed(key, conf, entries)

        with st.container():
            is_active = (st.session_state.get("active_feed") == key)
            clicked = st.button(
                conf.get("label", key.upper()),
                key=f"btn_mobile_{key}_{i}",
                use_container_width=True,
                type=("primary" if is_active else "secondary"),
            )

            ph = st.empty()
            badge_placeholders[key] = ph
            draw_badge(ph, safe_int(new_count))

            if clicked:
                if is_active:
                    if conf["type"] == "rss_meteoalarm":
                        st.session_state[f"{key}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
                    elif conf["type"] not in ("ec_async", "nws_grouped_compact"):
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

            if st.session_state.get("active_feed") == key:
                st.markdown("---")
                _render_feed_inline(key, conf, entries, badge_placeholders)

    if _toggled:
        _immediate_rerun()

else:
    # ---- DESKTOP: original row of buttons; content rendered after all buttons ----
    if not FEED_CONFIG:
        st.info("No feeds configured.")
        st.stop()

    cols = st.columns(len(FEED_CONFIG))
    badge_placeholders = {}
    _toggled = False

    for i, (key, conf) in enumerate(FEED_CONFIG.items()):
        entries = st.session_state[f"{key}_data"]
        new_count = _compute_new_count_for_feed(key, conf, entries)

        with cols[i]:
            is_active = (st.session_state.get("active_feed") == key)

            # Professional, minimal highlight: use primary button type for the active feed
            clicked = st.button(
                conf.get("label", key.upper()),
                key=f"btn_{key}_{i}",
                use_container_width=True,
                type=("primary" if is_active else "secondary"),
            )

            # Badge placeholder and draw
            badge_ph = st.empty()
            badge_placeholders[key] = badge_ph
            draw_badge(badge_ph, safe_int(new_count))

            # Click handling with immediate rerun to avoid "extra click" artifacts
            if clicked:
                if st.session_state.get("active_feed") == key:
                    # Closing an open feed → snapshot "seen" where appropriate
                    if conf["type"] == "rss_meteoalarm":
                        st.session_state[f"{key}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
                    elif conf["type"] in ("ec_async", "nws_grouped_compact"):
                        # EC/NWS per-bucket close/open handled inside their renderers
                        pass
                    else:
                        st.session_state[f"{key}_last_seen_time"] = time.time()
                    st.session_state["active_feed"] = None
                else:
                    # Opening a feed
                    st.session_state["active_feed"] = key
                    if conf["type"] == "rss_meteoalarm":
                        st.session_state[f"{key}_pending_seen_time"] = time.time()
                    elif conf["type"] in ("ec_async", "nws_grouped_compact"):
                        # Renderers manage per-bucket pending snapshots
                        st.session_state[f"{key}_pending_seen_time"] = None
                    else:
                        st.session_state[f"{key}_pending_seen_time"] = time.time()
                _toggled = True

    # Force a single rerun if any button toggled, so the highlight/badges are fresh immediately
    if _toggled:
        _immediate_rerun()

    # --------------------------------------------------------------------
    # Display details for the active feed (original desktop behavior)
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
            # ---------------- EC: Mark all as seen button ----------------
            # Keys used by the EC renderer: "Province|Warning"
            # We replicate that here to set all last-seen to 'now'.
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

                    # Zero-out badge immediately and rerun for fresh UI
                    st.session_state[f"{active}_remaining_new_total"] = 0
                    ph = badge_placeholders.get(active)
                    if ph is not None:
                        draw_badge(ph, 0)
                    _immediate_rerun()
            # -------------------------------------------------------------

            RENDERERS["ec_grouped_compact"](entries, {**conf, "key": active})

            # After rendering, recompute aggregate NEW using renderer's per-bucket last_seen map
            ec_total_now = ec_remaining_new_total(active, entries)
            st.session_state[f"{active}_remaining_new_total"] = int(ec_total_now)

            # Repaint the main badge now so closing a bucket updates the count immediately
            ph = badge_placeholders.get(active)
            if ph is not None:
                draw_badge(ph, safe_int(ec_total_now))

        # --- NWS (grouped compact, US) ---
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
                    ph = badge_placeholders.get(active)
                    if ph is not None:
                        draw_badge(ph, 0)

                    _immediate_rerun()
            # -------------------------------------------------------------

            RENDERERS["nws_grouped_compact"](entries, {**conf, "key": active})

            # After rendering, recompute aggregate NEW using renderer's per-bucket last_seen map
            nws_total_now = nws_remaining_new_total(active, entries)
            st.session_state[f"{active}_remaining_new_total"] = int(nws_total_now)

            # Repaint the main badge now so closing a bucket updates the count immediately
            ph = badge_placeholders.get(active)
            if ph is not None:
                draw_badge(ph, safe_int(nws_total_now))

        # --- Meteoalarm (countries) ---
        elif conf["type"] == "rss_meteoalarm":
            seen_ids = set(st.session_state[f"{active}_last_seen_alerts"])

            # Filter to countries that actually have alerts
            countries = [c for c in data_list if meteoalarm_country_has_alerts(c)]

            # Mark new vs seen (per-alert) and sort by country title
            countries = meteoalarm_mark_and_sort(countries, seen_ids)

            # Render per country
            for country in countries:
                RENDERERS["rss_meteoalarm"](country, {**conf, "key": active})

            # Commit snapshot of all currently visible alerts
            st.session_state[f"{active}_last_seen_alerts"] = meteoalarm_snapshot_ids(countries)

        # --- JMA ---
        elif conf["type"] == "rss_jma":
            RENDERERS["rss_jma"](entries, {**conf, "key": active})

        else:
            # Generic item-per-row renderer (JSON/NWS legacy/CMA etc.)
            seen_ts = st.session_state.get(f"{active}_last_seen_time") or 0.0

            # Show standard empty-state when there are no entries
            if not data_list:
                render_empty_state()

                # Snapshot "seen" to avoid perpetual NEW highlight on next non-empty refresh
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

                # Snapshot last seen timestamp for generic feeds
                pkey = f"{active}_pending_seen_time"
                pending = st.session_state.get(pkey, None)
                if pending is not None:
                    st.session_state[f"{active}_last_seen_time"] = float(pending)
                st.session_state.pop(pkey, None)
