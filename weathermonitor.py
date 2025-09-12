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
    ec_remaining_new_total,
    nws_remaining_new_total,
    uk_remaining_new_total,   # <-- added
    ec_bucket_from_title,
    draw_badge,
    safe_int,
    alert_id,
    meteoalarm_country_has_alerts,
    meteoalarm_mark_and_sort,
    meteoalarm_snapshot_ids,
    render_empty_state,
)

# --------------------------------------------------------------------
# Setup
# --------------------------------------------------------------------
os.environ.setdefault("STREAMLIT_WATCHER_TYPE", "poll")
nest_asyncio.apply()
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

vm = psutil.virtual_memory()
MEMORY_LIMIT = int(min(0.5 * vm.total, 4 * 1024**3))
MEMORY_HIGH_WATER = 0.85 * MEMORY_LIMIT
MEMORY_LOW_WATER = 0.50 * MEMORY_LIMIT
MIN_CONC, MAX_CONC, STEP = 5, 50, 5

def _rss_bytes():
    return psutil.Process(os.getpid()).memory_info().rss

st.session_state.setdefault("concurrency", 20)
rss_before = _rss_bytes()
if rss_before > MEMORY_HIGH_WATER:
    st.session_state["concurrency"] = max(MIN_CONC, st.session_state["concurrency"] - STEP)
elif rss_before < MEMORY_LOW_WATER:
    st.session_state["concurrency"] = min(MAX_CONC, st.session_state["concurrency"] + STEP)
MAX_CONCURRENCY = st.session_state["concurrency"]
st.caption(f"Concurrency: {MAX_CONCURRENCY}, RSS: {rss_before // (1024*1024)} MB")

# --------------------------------------------------------------------
# State & Config
# --------------------------------------------------------------------
FETCH_TTL = 60
HTTP2_ENABLED = False
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
st_autorefresh(interval=FETCH_TTL * 1000, key="auto_refresh_main")

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
        st.session_state.setdefault(f"{key}_last_seen_alerts", tuple())
st.session_state.setdefault("last_refreshed", now)
st.session_state.setdefault("active_feed", None)
st.session_state.setdefault("layout_mode", "Desktop")
st.session_state.setdefault("mobile_view", "list")

# --------------------------------------------------------------------
# Fetching
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
    limits = httpx.Limits(max_connections=MAX_CONCURRENCY, max_keepalive_connections=MAX_CONCURRENCY)
    transport = httpx.AsyncHTTPTransport(retries=3)
    timeout = httpx.Timeout(30.0)
    headers = {"User-Agent": "weathermonitor.app/1.0 (+support@weathermonitor.app)"}
    async with httpx.AsyncClient(
        timeout=timeout, limits=limits, transport=transport, http2=HTTP2_ENABLED, headers=headers
    ) as client:
        async def bound_fetch(key, conf):
            async with sem:
                async def call():
                    call_conf = {}
                    for k, v in conf.items():
                        if k in ("label", "type"):
                            continue
                        if k == "conf" and isinstance(v, dict):
                            call_conf.update(v)
                        else:
                            call_conf[k] = v
                    return await SCRAPER_REGISTRY[conf["type"]](call_conf, client)

                try:
                    data = await with_retries(call)
                except Exception as ex:
                    logging.warning(f"[{key.upper()} FETCH ERROR] {ex}")
                    logging.warning(traceback.format_exc())
                    data = {"entries": [], "error": str(ex), "source": conf}
                return key, data

        tasks = [bound_fetch(k, cfg) for k, cfg in FEED_CONFIG.items() if k in configs]
        return await asyncio.gather(*tasks)

def run_async(coro):
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
# Refresh
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

        if st.session_state.get("active_feed") == key:
            if conf["type"] == "rss_meteoalarm":
                last_seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
                _, new_count = compute_counts(entries, conf, last_seen_ids, alert_id_fn=alert_id)
                if new_count == 0:
                    st.session_state[f"{key}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
            elif conf["type"] == "ec_async":
                pass
            elif conf["type"] == "nws_grouped_compact":
                pass
            elif conf["type"] == "uk_grouped_compact":
                pass
            else:
                last_seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
                _, new_count = compute_counts(entries, conf, last_seen_ts)
                if new_count == 0:
                    st.session_state[f"{key}_last_seen_time"] = now

        if conf["type"] == "ec_async":
            st.session_state[f"{key}_remaining_new_total"] = ec_remaining_new_total(key, entries)
        elif conf["type"] == "nws_grouped_compact":
            st.session_state[f"{key}_remaining_new_total"] = nws_remaining_new_total(key, entries)
        elif conf["type"] == "uk_grouped_compact":
            st.session_state[f"{key}_remaining_new_total"] = uk_remaining_new_total(key, entries)

        gc.collect()

rss_after = _rss_bytes()
if rss_after > MEMORY_HIGH_WATER:
    st.session_state["concurrency"] = max(MIN_CONC, st.session_state["concurrency"] - STEP)

# --------------------------------------------------------------------
# Header
# --------------------------------------------------------------------
st.title("Global Weather Monitor")
st.caption(
    f"Last refreshed: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}"
)
st.markdown("---")

# --------------------------------------------------------------------
# Shared Rendering
# --------------------------------------------------------------------
def _render_feed_details(active, conf, entries, badge_placeholders=None):
    data_list = sorted(entries, key=lambda x: x.get("published", ""), reverse=True)

    if conf["type"] == "rss_bom_multi":
        RENDERERS["rss_bom_multi"](entries, {**conf, "key": active})

    elif conf["type"] == "ec_async":
        if not entries:
            render_empty_state("No active alerts at the moment.")
            st.session_state[f"{active}_remaining_new_total"] = 0
            return
        RENDERERS["ec_async"](entries, {**conf, "key": active})
        ec_total_now = ec_remaining_new_total(active, entries)
        st.session_state[f"{active}_remaining_new_total"] = int(ec_total_now)
        if badge_placeholders:
            ph = badge_placeholders.get(active)
            if ph:
                draw_badge(ph, safe_int(ec_total_now))

    elif conf["type"] == "nws_grouped_compact":
        if not entries:
            st.info("No active warnings that meet thresholds at the moment.")
            st.session_state[f"{active}_remaining_new_total"] = 0
            return

        cols = st.columns([0.25, 0.75])
        with cols[0]:
            if st.button("Mark all as seen", key=f"{active}_mark_all_seen"):
                lastseen_key = f"{active}_bucket_last_seen"
                bucket_lastseen = st.session_state.get(lastseen_key, {}) or {}
                now_ts = time.time()
                for a in entries:
                    state = a.get("state") or "Unknown"
                    bucket = a.get("bucket") or "Alert"
                    bkey = f"{state}|{bucket}"
                    bucket_lastseen[bkey] = now_ts
                st.session_state[lastseen_key] = bucket_lastseen
                st.session_state[f"{active}_remaining_new_total"] = 0
                if badge_placeholders:
                    ph = badge_placeholders.get(active)
                    if ph:
                        draw_badge(ph, 0)
                _immediate_rerun()

        RENDERERS["nws_grouped_compact"](entries, {**conf, "key": active})

        nws_total_now = nws_remaining_new_total(active, entries)
        st.session_state[f"{active}_remaining_new_total"] = int(nws_total_now)
        if badge_placeholders:
            ph = badge_placeholders.get(active)
            if ph:
                draw_badge(ph, safe_int(nws_total_now))

    elif conf["type"] == "uk_grouped_compact":
        if not entries:
            st.info("No active warnings that meet thresholds at the moment.")
            st.session_state[f"{active}_remaining_new_total"] = 0
            return

        cols = st.columns([0.25, 0.75])
        with cols[0]:
            if st.button("Mark all as seen", key=f"{active}_mark_all_seen"):
                lastseen_key = f"{active}_bucket_last_seen"
                bucket_lastseen = st.session_state.get(lastseen_key, {}) or {}
                now_ts = time.time()
                for a in entries:
                    region = a.get("state") or a.get("region") or "Unknown"
                    bucket = a.get("bucket") or a.get("event") or a.get("title") or "Alert"
                    bkey = f"{region}|{bucket}"
                    bucket_lastseen[bkey] = now_ts
                st.session_state[lastseen_key] = bucket_lastseen
                st.session_state[f"{active}_remaining_new_total"] = 0
                if badge_placeholders:
                    ph = badge_placeholders.get(active)
                    if ph:
                        draw_badge(ph, 0)
                _immediate_rerun()

        RENDERERS["uk_grouped_compact"](entries, {**conf, "key": active})

        uk_total_now = uk_remaining_new_total(active, entries)
        st.session_state[f"{active}_remaining_new_total"] = int(uk_total_now)
        if badge_placeholders:
            ph = badge_placeholders.get(active)
            if ph:
                draw_badge(ph, safe_int(uk_total_now))

    elif conf["type"] == "rss_meteoalarm":
        if not entries:
            st.info("No active alerts at the moment.")
            st.session_state[f"{active}_remaining_new_total"] = 0
            return
        entries = meteoalarm_mark_and_sort(
            entries,
            st.session_state.get(f"{active}_last_seen_alerts", tuple()),
            conf,
        )
        RENDERERS["rss_meteoalarm"](entries, {**conf, "key": active})
        if meteoalarm_country_has_alerts(entries):
            st.session_state[f"{active}_remaining_new_total"] = 1
        else:
            st.session_state[f"{active}_remaining_new_total"] = 0

    else:
        if not entries:
            st.info("No active alerts at the moment.")
            st.session_state[f"{active}_remaining_new_total"] = 0
            return
        RENDERERS[conf["type"]](entries, {**conf, "key": active})
        last_seen_ts = st.session_state.get(f"{active}_last_seen_time") or 0.0
        _, new_count = compute_counts(entries, conf, last_seen_ts)
        st.session_state[f"{active}_remaining_new_total"] = int(new_count)
        if badge_placeholders:
            ph = badge_placeholders.get(active)
            if ph:
                draw_badge(ph, safe_int(new_count))

def _new_count_for(key, conf, entries):
    if conf["type"] == "rss_meteoalarm":
        if meteoalarm_country_has_alerts(entries):
            return 1
        return 0
    if conf["type"] == "ec_async":
        val = st.session_state.get(f"{key}_remaining_new_total")
        return int(val) if isinstance(val, int) else int(ec_remaining_new_total(key, entries) or 0)
    if conf["type"] == "nws_grouped_compact":
        val = st.session_state.get(f"{key}_remaining_new_total")
        return int(val) if isinstance(val, int) else int(nws_remaining_new_total(key, entries) or 0)
    if conf["type"] == "uk_grouped_compact":
        val = st.session_state.get(f"{key}_remaining_new_total")
        return int(val) if isinstance(val, int) else int(uk_remaining_new_total(key, entries) or 0)
    seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
    _, new_count = compute_counts(entries, conf, seen_ts)
    return new_count

# --------------------------------------------------------------------
# Mobile + Desktop rendering
# --------------------------------------------------------------------
cols = st.columns(len(FEED_CONFIG))
badge_placeholders = {k: cols[i].empty() for i, k in enumerate(FEED_CONFIG)}

for key, conf in FEED_CONFIG.items():
    entries = st.session_state.get(f"{key}_data", [])
    new_count = _new_count_for(key, conf, entries)
    ph = badge_placeholders.get(key)
    if ph:
        draw_badge(ph, safe_int(new_count))

for key, conf in FEED_CONFIG.items():
    with st.expander(conf["label"], expanded=False):
        entries = st.session_state.get(f"{key}_data", [])
        _render_feed_details(key, conf, entries, badge_placeholders)
