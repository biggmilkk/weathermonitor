# weathermonitor.py

import os
# Use a portable watcher to avoid inotify limits on some hosts/containers.
os.environ.setdefault("STREAMLIT_SERVER_FILE_WATCHER_TYPE", "poll")

import time
import sys
import gc
import psutil
import httpx
import asyncio
import logging
import nest_asyncio
import streamlit as st
from dateutil import parser as dateparser
from streamlit_autorefresh import st_autorefresh

from feeds import get_feed_definitions
from utils.scraper_registry import SCRAPER_REGISTRY
from computation import compute_counts
from renderer import RENDERERS, ec_remaining_new_total

# Allow nested asyncio loops under Streamlit
nest_asyncio.apply()

# ---------- Page + logging ----------
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

# ---------- Adaptive memory targets + concurrency autotune ----------
vm = psutil.virtual_memory()
# Budget up to half the system RAM (cap at 4 GiB)
MEMORY_LIMIT = int(min(0.5 * vm.total, 4 * 1024**3))
MEMORY_HIGH_WATER = 0.85 * MEMORY_LIMIT
MEMORY_LOW_WATER  = 0.50 * MEMORY_LIMIT

MIN_CONC = 5
MAX_CONC = 50
STEP     = 5

def _rss_bytes() -> int:
    return psutil.Process(os.getpid()).memory_info().rss

# Initialize in session_state once
st.session_state.setdefault("concurrency", 20)

# Pre-fetch memory check
rss_before = _rss_bytes()
if rss_before > MEMORY_HIGH_WATER:
    st.session_state["concurrency"] = max(MIN_CONC, st.session_state["concurrency"] - STEP)
elif rss_before < MEMORY_LOW_WATER:
    st.session_state["concurrency"] = min(MAX_CONC, st.session_state["concurrency"] + STEP)

# Use this dynamic value everywhere for concurrency
MAX_CONCURRENCY = st.session_state["concurrency"]

# Debug caption
st.caption(f"Concurrency: {MAX_CONCURRENCY}, RSS: {rss_before//(1024*1024)} MB")

# ---------- Constants ----------
FETCH_TTL = 60  # seconds

# Ensure module path (keep for now to avoid import issues if not packaged)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Auto-refresh
st_autorefresh(interval=FETCH_TTL * 1000, key="auto_refresh_main")

# Cache feed definitions (if they do I/O/config parsing)
@st.cache_data(ttl=3600)
def load_feeds():
    return get_feed_definitions()

FEED_CONFIG = load_feeds()

# Guard: no feeds configured
if not FEED_CONFIG:
    st.title("Global Weather Monitor")
    st.info("No feeds configured.")
    st.stop()

# Initialize session state for feeds
now = time.time()
for key, conf in FEED_CONFIG.items():
    st.session_state.setdefault(f"{key}_data", [])
    st.session_state.setdefault(f"{key}_last_fetch", 0)
    st.session_state.setdefault(f"{key}_last_seen_time", 0.0)
    st.session_state.setdefault(f"{key}_pending_seen_time", None)
    if conf["type"] == "rss_meteoalarm":
        # Deterministic storage (tuple); convert to set on read
        st.session_state.setdefault(f"{key}_last_seen_alerts", tuple())
st.session_state.setdefault("last_refreshed", now)
st.session_state.setdefault("active_feed", None)

# Unique ID for Meteoalarm items
def alert_id(e):
    return f"{e['level']}|{e['type']}|{e['from']}|{e['until']}"

# ---------- Networking helpers ----------

async def with_retries(coro_fn, *, attempts=3, base_delay=0.5):
    """
    Retry wrapper for transient fetch errors.
    `coro_fn` should be a callable returning an awaitable (no args).
    """
    for i in range(attempts):
        try:
            return await coro_fn()
        except Exception as ex:
            if i == attempts - 1:
                raise
            await asyncio.sleep(base_delay * (2 ** i))

async def _fetch_all_feeds(configs):
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    limits = httpx.Limits(
        max_connections=MAX_CONCURRENCY,
        max_keepalive_connections=MAX_CONCURRENCY,
    )
    transport = httpx.AsyncHTTPTransport(retries=3)
    headers = {"User-Agent": "weathermonitor.app/1.0 (+contact@example.com)"}
    timeout = httpx.Timeout(30.0)

    # Keep http2 disabled unless 'h2' is installed; RSS/JSON works great over HTTP/1.1
    async with httpx.AsyncClient(
        timeout=timeout,
        limits=limits,
        transport=transport,
        http2=False,
        headers=headers,
    ) as client:

        async def bound_fetch(key, conf):
            async with sem:
                async def call():
                    return await SCRAPER_REGISTRY[conf["type"]](conf, client)

                t0 = time.perf_counter()
                try:
                    data = await with_retries(call)
                except Exception as ex:
                    logging.warning(f"[{key.upper()} FETCH ERROR] {ex}")
                    data = {"entries": [], "error": str(ex), "source": conf}
                dt_ms = (time.perf_counter() - t0) * 1000.0
                n = len(data.get("entries", [])) if isinstance(data.get("entries", []), list) else -1
                logging.info(f"[{key}] {n} entries in {dt_ms:.1f} ms")
                return key, data

        tasks = [bound_fetch(k, cfg) for k, cfg in configs.items()]
        return await asyncio.gather(*tasks)

# Run async on a fresh event loop (safer under Streamlit reruns)
def run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        asyncio.set_event_loop(None)

# ---------- Refresh stale feeds ----------
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
                last_seen_tuple = st.session_state[f"{key}_last_seen_alerts"]
                last_seen = set(last_seen_tuple) if isinstance(last_seen_tuple, (list, tuple, set)) else set()
                _, new_count = compute_counts(entries, conf, last_seen, alert_id_fn=alert_id)
                if new_count == 0:
                    snap = tuple(sorted(
                        alert_id(e)
                        for country in entries
                        for alerts in (country.get("alerts", {}) or {}).values()
                        for e in (alerts or [])
                    ))
                    st.session_state[f"{key}_last_seen_alerts"] = snap
            elif conf["type"] == "ec_async":
                # EC compact renderer manages NEW state per bucket; no auto snapshot here
                pass
            else:
                last_seen_ts = float(st.session_state.get(f"{key}_last_seen_time") or 0.0)
                # For generic feeds, no custom ID function
                _, new_count = compute_counts(entries, conf, last_seen_ts, alert_id_fn=None)
                if new_count == 0:
                    st.session_state[f"{key}_last_seen_time"] = now

        # Keep the EC aggregate (warnings + Severe Thunderstorm Watch) up-to-date even if EC isn't opened yet
        if conf["type"] == "ec_async":
            st.session_state[f"{key}_remaining_new_total"] = ec_remaining_new_total(key, entries)

        gc.collect()

    # Post-fetch memory check: if we spiked, dampen concurrency immediately
    rss_after = _rss_bytes()
    if rss_after > MEMORY_HIGH_WATER:
        st.session_state["concurrency"] = max(MIN_CONC, st.session_state["concurrency"] - STEP)

# ---------- Header ----------
st.title("Global Weather Monitor")
st.caption(
    f"Last refreshed: "
    f"{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}"
)
st.markdown("---")

# ---------- Badge helper ----------
def _safe_int(x):
    try:
        return max(0, int(x))
    except Exception:
        return 0

def render_badge(ph, count: int):
    n = _safe_int(count)
    if n > 0:
        ph.markdown(
            "<span style='margin-left:8px;padding:2px 6px;"
            "border-radius:4px;background:#ffeecc;color:#000;font-size:0.9em;font-weight:bold;'>"
            f"❗ {n} New</span>",
            unsafe_allow_html=True,
        )
    else:
        ph.empty()

# ---------- Feed buttons ----------
cols = st.columns(len(FEED_CONFIG))
badge_placeholders = {}  # key -> placeholder

for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = st.session_state[f"{key}_data"]

    # Compute baseline NEW count
    if conf["type"] == "rss_meteoalarm":
        last_seen_tuple = st.session_state[f"{key}_last_seen_alerts"]
        seen = set(last_seen_tuple) if isinstance(last_seen_tuple, (list, tuple, set)) else set()
        _, new_count = compute_counts(entries, conf, seen, alert_id_fn=alert_id)
    elif conf["type"] == "ec_async":
        # Always use warnings(+watch)-only aggregate that matches bucket math
        ec_total = st.session_state.get(f"{key}_remaining_new_total")
        if isinstance(ec_total, int):
            new_count = ec_total
        else:
            new_count = ec_remaining_new_total(key, entries)
            st.session_state[f"{key}_remaining_new_total"] = int(new_count)
    else:
        seen_ts = float(st.session_state.get(f"{key}_last_seen_time") or 0.0)
        _, new_count = compute_counts(entries, conf, seen_ts, alert_id_fn=None)

    with cols[i]:
        clicked = st.button(conf["label"], key=f"btn_{key}_{i}", use_container_width=True)

        # draw badge via placeholder so we can repaint it later in the same run
        badge_ph = st.empty()
        badge_placeholders[key] = badge_ph
        render_badge(badge_ph, _safe_int(new_count))

        if clicked:
            if st.session_state["active_feed"] == key:
                # Closing an open feed
                if conf["type"] == "rss_meteoalarm":
                    snap = tuple(sorted(
                        alert_id(e)
                        for country in entries
                        for alerts in (country.get("alerts", {}) or {}).values()
                        for e in (alerts or [])
                    ))
                    st.session_state[f"{key}_last_seen_alerts"] = snap
                elif conf["type"] == "ec_async":
                    # EC per-bucket close/open handled inside renderer
                    # Recompute top-row badge now for immediate feedback
                    ec_total_now = ec_remaining_new_total(key, entries)
                    st.session_state[f"{key}_remaining_new_total"] = int(ec_total_now)
                    ph = badge_placeholders.get(key)
                    if ph is not None:
                        render_badge(ph, _safe_int(ec_total_now))
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

# ---------- Display details ----------
active = st.session_state["active_feed"]
if active:
    st.markdown("---")
    conf = FEED_CONFIG[active]
    entries = st.session_state[f"{active}_data"]
    data_list = sorted(entries, key=lambda x: x.get("published", ""), reverse=True)

    # --- BOM ---
    if conf["type"] == "rss_bom_multi":
        RENDERERS["rss_bom_multi"](entries, {**conf, "key": active})

    # --- Environment Canada grouped (compact warnings/watch view) ---
    elif conf["type"] == "ec_async":
        RENDERERS["ec_grouped_compact"](entries, {**conf, "key": active})

        # Immediately recompute aggregate NEW using renderer's per-bucket last_seen map
        ec_total_now = ec_remaining_new_total(active, entries)
        st.session_state[f"{active}_remaining_new_total"] = int(ec_total_now)

        # Repaint the main badge now so closing a bucket updates the count immediately
        ph = badge_placeholders.get(active)
        if ph is not None:
            render_badge(ph, _safe_int(ec_total_now))

    # --- Meteoalarm ---
    elif conf["type"] == "rss_meteoalarm":
        last_seen_tuple = st.session_state[f"{active}_last_seen_alerts"]
        seen_ids = set(last_seen_tuple) if isinstance(last_seen_tuple, (list, tuple, set)) else set()

        # Filter to countries that actually have alerts (belt + suspenders)
        def _has_alerts(c):
            a = c.get("alerts", {})
            return any(a.get("today")) or any(a.get("tomorrow"))

        countries = [c for c in data_list if _has_alerts(c)]

        # Mark new vs seen by unique id
        for country in countries:
            for alerts in (country.get("alerts", {}) or {}).values():
                for e in (alerts or []):
                    e["is_new"] = alert_id(e) not in seen_ids

        # Sort alphabetically by country title (case-insensitive)
        countries.sort(key=lambda c: (c.get("title", "").casefold()))

        # Red-bar + render per country
        for country in countries:
            alerts_flat = [
                e
                for alerts in (country.get("alerts", {}) or {}).values()
                for e in (alerts or [])
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

    # --- Generic item-per-row renderer ---
    else:
        seen_ts = float(st.session_state.get(f"{active}_last_seen_time") or 0.0)
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
            # dispatch by conf type if available, else no-op
            RENDERERS.get(conf["type"], lambda i, c: None)(item, conf)

    # Snapshot last seen timestamps or alerts for *non-EC* feeds
    pkey = f"{active}_pending_seen_time"
    pending = st.session_state.get(pkey, None)

    if conf["type"] == "rss_meteoalarm":
        snap = tuple(sorted(
            alert_id(e)
            for country in data_list
            for alerts in (country.get("alerts", {}) or {}).values()
            for e in (alerts or [])
        ))
        st.session_state[f"{active}_last_seen_alerts"] = snap
    elif conf["type"] == "ec_async":
        # EC compact view manages per-bucket snapshots internally
        pass
    else:
        if pending is not None:
            st.session_state[f"{active}_last_seen_time"] = float(pending)

    st.session_state.pop(pkey, None)
