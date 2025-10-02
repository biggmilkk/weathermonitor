import os, sys, time, gc, logging, psutil
import streamlit as st
from dateutil import parser as dateparser
from streamlit_autorefresh import st_autorefresh

from feeds import get_feed_definitions
from computation import (
    compute_counts,
    meteoalarm_unseen_active_instances,  # moved from local helper to computation.py
)
from services.fetcher import run_fetch_round  # new: centralized async fetching

from renderer import (
    RENDERERS,
    ec_remaining_new_total,
    nws_remaining_new_total,
    ec_bucket_from_title,
    draw_badge,
    safe_int,
    meteoalarm_country_has_alerts,
    meteoalarm_mark_and_sort,
    meteoalarm_snapshot_ids,
    render_empty_state,
)

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

# --------------------------------------------------------------------
# Refresh (uses centralized fetcher)
# --------------------------------------------------------------------
now = time.time()
to_fetch = {
    k: v for k, v in FEED_CONFIG.items()
    if now - st.session_state[f"{k}_last_fetch"] > FETCH_TTL
}

if to_fetch:
    # `run_fetch_round` should accept a dict of feed configs to fetch
    # and return an iterable of (key, data) like the old _fetch_all_feeds().
    results = run_fetch_round(to_fetch)

    for key, raw in results:
        entries = raw.get("entries", [])
        st.session_state[f"{key}_data"] = entries
        st.session_state[f"{key}_last_fetch"] = now
        st.session_state["last_refreshed"] = now
        conf = FEED_CONFIG[key]

        # When the details pane is open, snapshot seen (feed-dependent)
        if st.session_state.get("active_feed") == key:
            if conf["type"] == "rss_meteoalarm":
                last_seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
                new_count = meteoalarm_unseen_active_instances(entries, last_seen_ids)
                if new_count == 0:
                    st.session_state[f"{key}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
            elif conf["type"] in ("ec_async", "nws_grouped_compact"):
                # grouped feeds use per-bucket last-seen inside renderer
                pass
            elif conf["type"] == "uk_grouped_compact":
                # UK uses a single feed-level last_seen_time (like BOM/JMA)
                last_seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
                _, new_count = compute_counts(entries, conf, last_seen_ts)
                if new_count == 0:
                    st.session_state[f"{key}_last_seen_time"] = now
            else:
                last_seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
                _, new_count = compute_counts(entries, conf, last_seen_ts)
                if new_count == 0:
                    st.session_state[f"{key}_last_seen_time"] = now

        # Precompute remaining NEW totals for badge row (where applicable)
        if conf["type"] == "ec_async":
            st.session_state[f"{key}_remaining_new_total"] = ec_remaining_new_total(key, entries)
        elif conf["type"] == "nws_grouped_compact":
            st.session_state[f"{key}_remaining_new_total"] = nws_remaining_new_total(key, entries)

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
# Details renderer (per feed panel)
# --------------------------------------------------------------------
def _immediate_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()

def _render_feed_details(active, conf, entries, badge_placeholders=None):
    data_list = sorted(entries, key=lambda x: x.get("published", ""), reverse=True)

    if conf["type"] == "rss_bom_multi":
        RENDERERS["rss_bom_multi"](entries, {**conf, "key": active})

    elif conf["type"] == "ec_async":
        if not entries:
            st.info("No active warnings that meet thresholds at the moment.")
            st.session_state[f"{active}_remaining_new_total"] = 0
            return

        _PROVINCE_NAMES = {
            "AB": "Alberta", "BC": "British Columbia", "MB": "Manitoba",
            "NB": "New Brunswick", "NL": "Newfoundland and Labrador",
            "NT": "Northwest Territories", "NS": "Nova Scotia", "NU": "Nunavut",
            "ON": "Ontario", "PE": "Prince Edward Island", "QC": "Quebec",
            "SK": "Saskatchewan", "YT": "Yukon",
        }
        cols = st.columns([0.25, 0.75])
        with cols[0]:
            if st.button("Mark all as seen", key=f"{active}_mark_all_seen"):
                lastseen_key = f"{active}_bucket_last_seen"
                bucket_lastseen = st.session_state.get(lastseen_key, {}) or {}
                now_ts = time.time()
                # Mark every existing bucket as seen now
                for k in list(bucket_lastseen.keys()):
                    bucket_lastseen[k] = now_ts
                # Snapshot current EC entries
                for e in entries:
                    bucket = ec_bucket_from_title(e.get("title", "") or "")
                    if not bucket:
                        continue
                    code = e.get("province", "")
                    prov_name = _PROVINCE_NAMES.get(code, code) if isinstance(code, str) else str(code)
                    bkey = f"{prov_name}|{bucket}"
                    bucket_lastseen[bkey] = now_ts
                st.session_state[lastseen_key] = bucket_lastseen
                st.session_state[f"{active}_remaining_new_total"] = 0
                if badge_placeholders:
                    ph = badge_placeholders.get(active)
                    if ph:
                        draw_badge(ph, 0)
                _immediate_rerun()

        RENDERERS["ec_grouped_compact"](entries, {**conf, "key": active})
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

        lastseen_key = f"{active}_bucket_last_seen"
        bucket_lastseen = st.session_state.get(lastseen_key, {}) or {}

        cols = st.columns([0.25, 0.75])
        with cols[0]:
            if st.button("Mark all as seen", key=f"{active}_mark_all_seen"):
                now_ts = time.time()
                for a in entries:
                    state = (a.get("state") or a.get("state_name") or a.get("state_code") or "Unknown")
                    bucket = (a.get("bucket") or a.get("event") or a.get("title") or "Alert")
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
            return

        RENDERERS["uk_grouped_compact"](entries, {**conf, "key": active})

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
        # Generic feeds: per-entry "is_new" vs a single last_seen_ts
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
                item["is_new"] = bool(ts > seen_ts)
                RENDERERS.get(conf["type"], lambda i, c: None)(item, conf)
            pkey = f"{active}_pending_seen_time"
            pending = st.session_state.get(pkey, None)
            if pending is not None:
                st.session_state[f"{active}_last_seen_time"] = float(pending)
            st.session_state.pop(pkey, None)

# --------------------------------------------------------------------
# New count helper for the badge row
# --------------------------------------------------------------------
def _new_count_for(key, conf, entries):
    if conf["type"] == "rss_meteoalarm":
        seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
        return meteoalarm_unseen_active_instances(entries, seen_ids)
    if conf["type"] == "ec_async":
        val = st.session_state.get(f"{key}_remaining_new_total")
        return int(val) if isinstance(val, int) else int(ec_remaining_new_total(key, entries) or 0)
    if conf["type"] == "nws_grouped_compact":
        val = st.session_state.get(f"{key}_remaining_new_total")
        return int(val) if isinstance(val, int) else int(nws_remaining_new_total(key, entries) or 0)
    if conf["type"] == "uk_grouped_compact":
        # UK uses feed-level last_seen_time like BOM/JMA
        seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
        _, new_count = compute_counts(entries, conf, seen_ts)
        return new_count
    seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
    _, new_count = compute_counts(entries, conf, seen_ts)
    return new_count

# --------------------------------------------------------------------
# Desktop (buttons row + details)  —  8 buttons per row
# --------------------------------------------------------------------
if not FEED_CONFIG:
    st.info("No feeds configured.")
    st.stop()

MAX_BTNS_PER_ROW = 8
items = list(FEED_CONFIG.items())

badge_placeholders = {}
_toggled = False
global_idx = 0  # ensure unique button keys across all rows

def _new_count_for_feed(key, conf, entries):
    if conf["type"] == "rss_meteoalarm":
        seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
        return meteoalarm_unseen_active_instances(entries, seen_ids)
    if conf["type"] == "ec_async":
        val = st.session_state.get(f"{key}_remaining_new_total")
        return int(val) if isinstance(val, int) else int(ec_remaining_new_total(key, entries) or 0)
    if conf["type"] == "nws_grouped_compact":
        val = st.session_state.get(f"{key}_remaining_new_total")
        return int(val) if isinstance(val, int) else int(nws_remaining_new_total(key, entries) or 0)
    if conf["type"] == "uk_grouped_compact":
        seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
        _, new_count = compute_counts(entries, conf, seen_ts)
        return new_count
    seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
    _, new_count = compute_counts(entries, conf, seen_ts)
    return new_count

for start in range(0, len(items), MAX_BTNS_PER_ROW):
    row_items = items[start : start + MAX_BTNS_PER_ROW]
    # keep a constant 8-column layout so the last item doesn't stretch full width
    cols = st.columns(MAX_BTNS_PER_ROW)
    for ci, (key, conf) in enumerate(row_items):
        entries = st.session_state[f"{key}_data"]
        new_count = _new_count_for_feed(key, conf, entries)

        with cols[ci]:
            is_active = (st.session_state.get("active_feed") == key)
            clicked = st.button(
                conf.get("label", key.upper()),
                key=f"btn_{key}_{global_idx}",
                use_container_width=True,
                type=("primary" if is_active else "secondary"),
            )
            badge_ph = st.empty()
            badge_placeholders[key] = badge_ph
            draw_badge(badge_ph, safe_int(new_count))

            if clicked:
                # toggle behavior unchanged
                if st.session_state.get("active_feed") == key:
                    if conf["type"] == "rss_meteoalarm":
                        st.session_state[f"{key}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
                    elif conf["type"] in ("ec_async", "nws_grouped_compact"):
                        pass
                    elif conf["type"] == "uk_grouped_compact":
                        st.session_state[f"{key}_last_seen_time"] = time.time()
                    else:
                        st.session_state[f"{key}_last_seen_time"] = time.time()
                    st.session_state["active_feed"] = None
                else:
                    st.session_state["active_feed"] = key
                    if conf["type"] == "rss_meteoalarm":
                        st.session_state[f"{key}_pending_seen_time"] = time.time()
                    elif conf["type"] in ("ec_async", "nws_grouped_compact"):
                        st.session_state[f"{key}_pending_seen_time"] = None
                    elif conf["type"] == "uk_grouped_compact":
                        st.session_state[f"{key}_pending_seen_time"] = time.time()
                    else:
                        st.session_state[f"{key}_pending_seen_time"] = time.time()
                _toggled = True

        global_idx += 1

if _toggled:
    _immediate_rerun()

active = st.session_state["active_feed"]
if active:
    st.markdown("---")
    conf = FEED_CONFIG[active]
    entries = st.session_state[f"{active}_data"]
    _render_feed_details(active, conf, entries, badge_placeholders)
