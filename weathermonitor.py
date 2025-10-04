# weathermonitor.py

import os, sys, time, gc, logging, psutil
import streamlit as st
from dateutil import parser as dateparser
from streamlit_autorefresh import st_autorefresh

from feeds import get_feed_definitions
from utils.fetcher import run_fetch_round

# Computation helpers
from computation import (
    compute_counts,
    meteoalarm_unseen_active_instances,
    meteoalarm_snapshot_ids,
    parse_timestamp,
    compute_imd_timestamps,
    ec_remaining_new_total as ec_new_total,
    nws_remaining_new_total as nws_new_total,
)

# Renderers
from renderers import RENDERERS


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def render_empty_state():
    st.info("No active warnings at this time.")

def _immediate_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()


# --------------------------------------------------------------------
# Setup
# --------------------------------------------------------------------
os.environ.setdefault("STREAMLIT_WATCHER_TYPE", "poll")
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

vm = psutil.virtual_memory()
MEMORY_LIMIT = int(min(0.5 * vm.total, 4 * 1024**3))
MEMORY_HIGH_WATER = 0.85 * MEMORY_LIMIT
MEMORY_LOW_WATER  = 0.50 * MEMORY_LIMIT
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
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
tick_counter = st_autorefresh(interval=FETCH_TTL * 1000, key="auto_refresh_main")

@st.cache_data(ttl=3600)
def load_feeds():
    return get_feed_definitions()

@st.cache_data(ttl=FETCH_TTL, show_spinner=False)
def cached_fetch_round(to_fetch: dict, max_conc: int):
    return run_fetch_round(to_fetch, max_concurrency=max_conc)

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


# --------------------------------------------------------------------
# Cold boot: fetch ALL feeds
# --------------------------------------------------------------------
do_cold_boot = not st.session_state.get("_cold_boot_done", False)
if not do_cold_boot:
    do_cold_boot = all(len(st.session_state.get(f"{k}_data", [])) == 0 for k in FEED_CONFIG)

if do_cold_boot:
    all_results = cached_fetch_round(FEED_CONFIG, MAX_CONCURRENCY)
    now_ts = time.time()
    for key, raw in all_results:
        entries = raw.get("entries", [])
        conf = FEED_CONFIG[key]

        if conf["type"] == "imd_current_orange_red":
            fp_key = f"{key}_fp_by_region"
            ts_key = f"{key}_ts_by_region"
            prev_fp = dict(st.session_state.get(fp_key, {}) or {})
            prev_ts = dict(st.session_state.get(ts_key, {}) or {})
            entries, fp_by_region, ts_by_region = compute_imd_timestamps(
                entries=entries, prev_fp=prev_fp, prev_ts=prev_ts, now_ts=now_ts
            )
            st.session_state[fp_key] = fp_by_region
            st.session_state[ts_key] = ts_by_region

        st.session_state[f"{key}_data"] = entries
        st.session_state[f"{key}_last_fetch"] = now_ts
    st.session_state["last_refreshed"] = now_ts
    st.session_state["_cold_boot_done"] = True


# --------------------------------------------------------------------
# Scheduler logic (unchanged)
# --------------------------------------------------------------------
# ... [omitted for brevity: your group scheduling, batch fetch, IMD special handling] ...


# --------------------------------------------------------------------
# Details Panel
# --------------------------------------------------------------------
def _render_feed_details(active, conf, entries):
    data_list = sorted(entries, key=lambda x: x.get("published", ""), reverse=True)

    if conf["type"] == "rss_bom_multi":
        RENDERERS["rss_bom_multi"](entries, {**conf, "key": active}); return
    if conf["type"] == "ec_async":
        RENDERERS["ec_grouped_compact"](entries, {**conf, "key": active}); return
    if conf["type"] == "nws_grouped_compact":
        RENDERERS["nws_grouped_compact"](entries, {**conf, "key": active}); return
    if conf["type"] == "rss_meteoalarm":
        RENDERERS["rss_meteoalarm"](entries, {**conf, "key": active}); return
    if conf["type"] == "uk_grouped_compact":
        # UK renderer is self-sufficient. DO NOT clear 'seen' here.
        RENDERERS["uk_grouped_compact"](entries, {**conf, "key": active}); return
    if conf["type"] == "rss_jma":
        RENDERERS["rss_jma"](entries, {**conf, "key": active}); return

    # Generic fallback
    seen_ts = st.session_state.get(f"{active}_last_seen_time") or 0.0
    if not data_list:
        render_empty_state()
    else:
        for item in data_list:
            ts = item.get("timestamp") or parse_timestamp(item.get("published"))
            item["is_new"] = bool(ts and ts > seen_ts)
            RENDERERS.get(conf["type"], lambda i, c: None)(item, conf)


# --------------------------------------------------------------------
# Buttons + Clear-on-close contract
# --------------------------------------------------------------------
# ⚠️ TWO-CLICK CONTRACT (DO NOT CHANGE):
#   - Click 1: open panel, do NOT commit 'seen'.
#   - Click 2: close panel, commit 'seen' now.
# If you add pending/auto commits in details, you will reintroduce triple-click bug.

# ... [rest of UI button loop unchanged, with clear-on-close commits for non-renderer feeds] ...
