# weathermonitor.py

# --------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------
import os, sys, time, gc, logging, psutil
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from feeds import get_feed_definitions
from utils.fetcher import run_fetch_round

from computation import (
    compute_counts,
    meteoalarm_unseen_active_instances,
    meteoalarm_snapshot_ids,
    compute_imd_timestamps,
    ec_remaining_new_total as ec_new_total,
    nws_remaining_new_total as nws_new_total,
    snapshot_imd_seen,
    meteoalarm_total_active_instances,
)

from renderers import RENDERERS


# --------------------------------------------------------------------
# UI helpers
# --------------------------------------------------------------------
def render_empty_state():
    st.info("No active warnings at this time.")

def _immediate_rerun():
    if hasattr(st, "rerun"): st.rerun()
    elif hasattr(st, "experimental_rerun"): st.experimental_rerun()

def commit_seen_for_feed(prev_key: str):
    """Commit 'seen' when closing/switching away from a feed."""
    if not prev_key: return
    conf = FEED_CONFIG.get(prev_key)
    if not conf: return

    entries = st.session_state.get(f"{prev_key}_data", [])

    if conf["type"] == "rss_meteoalarm":
        st.session_state[f"{prev_key}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)

    elif conf["type"] in ("ec_async", "ec_grouped_compact", "nws_grouped_compact"):
        pass  # handled in renderers

    elif conf["type"] == "imd_current_orange_red":
        fp_key, ts_key = f"{prev_key}_fp_by_region", f"{prev_key}_ts_by_region"
        fp_by_region, ts_by_region, cleared = snapshot_imd_seen(entries, now_ts=time.time())
        st.session_state[fp_key] = fp_by_region
        st.session_state[ts_key] = ts_by_region
        st.session_state[f"{prev_key}_data"] = cleared
        st.session_state[f"{prev_key}_last_seen_time"] = time.time()

    else:
        st.session_state[f"{prev_key}_last_seen_time"] = time.time()


# --------------------------------------------------------------------
# App setup
# --------------------------------------------------------------------
os.environ.setdefault("STREAMLIT_WATCHER_TYPE", "poll")
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

vm = psutil.virtual_memory()
MEMORY_LIMIT = int(min(0.5 * vm.total, 4 * 1024**3))
MEMORY_HIGH_WATER = 0.85 * MEMORY_LIMIT
MEMORY_LOW_WATER  = 0.50 * MEMORY_LIMIT
MIN_CONC, MAX_CONC, STEP = 5, 50, 5

def _rss_bytes(): return psutil.Process(os.getpid()).memory_info().rss

st.session_state.setdefault("concurrency", 20)
rss_before = _rss_bytes()
if rss_before > MEMORY_HIGH_WATER:
    st.session_state["concurrency"] = max(MIN_CONC, st.session_state["concurrency"] - STEP)
elif rss_before < MEMORY_LOW_WATER:
    st.session_state["concurrency"] = min(MAX_CONC, st.session_state["concurrency"] + STEP)
MAX_CONCURRENCY = st.session_state["concurrency"]
st.caption(f"Concurrency: {MAX_CONCURRENCY}, RSS: {rss_before // (1024*1024)} MB")


# --------------------------------------------------------------------
# State & config
# --------------------------------------------------------------------
FETCH_TTL = 60
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
tick_counter = st_autorefresh(interval=FETCH_TTL * 1000, key="auto_refresh_main")

@st.cache_data(ttl=3600)
def load_feeds():
    return get_feed_definitions()

# NOTE: normalized cache key: use stable tuple of keys instead of raw dict
@st.cache_data(ttl=FETCH_TTL, show_spinner=False)
def cached_fetch_round(keys: tuple[str, ...], max_conc: int):
    # Rebuild subset dict deterministically from FEED_CONFIG at call time
    subset = {k: FEED_CONFIG[k] for k in keys if k in FEED_CONFIG}
    return run_fetch_round(subset, max_concurrency=max_conc)

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
# Cold boot: fetch all feeds once
# --------------------------------------------------------------------
do_cold_boot = not st.session_state.get("_cold_boot_done", False) or \
               all(len(st.session_state.get(f"{k}_data", [])) == 0 for k in FEED_CONFIG)

if do_cold_boot:
    # normalized: pass tuple of keys
    all_results = cached_fetch_round(tuple(sorted(FEED_CONFIG.keys())), MAX_CONCURRENCY)
    now_ts = time.time()
    for key, raw in all_results:
        entries = raw.get("entries", [])
        conf = FEED_CONFIG[key]

        if conf["type"] == "imd_current_orange_red":
            fp_key, ts_key = f"{key}_fp_by_region", f"{key}_ts_by_region"
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
# Scheduler (fetch on minute tick)
# --------------------------------------------------------------------
current_minute_index = int(time.time() // 60)
prev_minute_index = st.session_state.get("_last_minute_index")
is_timer_tick = (prev_minute_index != current_minute_index)
st.session_state["_last_minute_index"] = current_minute_index

minute_in_cycle_4 = (current_minute_index % 4) + 1

def group_is_due(group_code: str, m: int) -> bool:
    g = (group_code or "g1").lower()
    if g == "g1": return True
    if g == "g2_even": return m in (2, 4)
    if g == "g2_odd":  return m in (1, 3)
    if g == "g4_1":    return m == 1
    if g == "g4_2":    return m == 2
    if g == "g4_3":    return m == 3
    if g == "g4_4":    return m == 4
    return True

GROUP_MIN_SPACING = {"g1": 60, "g2_even": 120, "g2_odd": 120, "g4_1": 240, "g4_2": 240, "g4_3": 240, "g4_4": 240}

to_fetch = {}
if is_timer_tick:
    now = time.time()
    for key, conf in FEED_CONFIG.items():
        grp = (conf.get("group") or "g1").lower()
        if group_is_due(grp, minute_in_cycle_4):
            last = float(st.session_state.get(f"{key}_last_fetch", 0))
            if (now - last) >= (GROUP_MIN_SPACING.get(grp, 60) - 1):
                to_fetch[key] = conf

BATCH_SIZE = 10
if len(to_fetch) > BATCH_SIZE:
    to_fetch = dict(sorted(
        to_fetch.items(),
        key=lambda kv: float(st.session_state.get(f"{kv[0]}_last_fetch", 0))
    )[:BATCH_SIZE])

if to_fetch:
    # normalized: pass tuple of keys
    results = cached_fetch_round(tuple(sorted(to_fetch.keys())), MAX_CONCURRENCY)
    now = time.time()
    for key, raw in results:
        entries = raw.get("entries", [])
        conf = FEED_CONFIG[key]

        if conf["type"] == "imd_current_orange_red":
            fp_key, ts_key = f"{key}_fp_by_region", f"{key}_ts_by_region"
            prev_fp = dict(st.session_state.get(fp_key, {}) or {})
            prev_ts = dict(st.session_state.get(ts_key, {}) or {})
            now_ts  = time.time()
            entries, fp_by_region, ts_by_region = compute_imd_timestamps(
                entries=entries, prev_fp=prev_fp, prev_ts=prev_ts, now_ts=now_ts
            )
            st.session_state[fp_key] = fp_by_region
            st.session_state[ts_key] = ts_by_region

        st.session_state[f"{key}_data"] = entries
        st.session_state[f"{key}_last_fetch"] = now
        st.session_state["last_refreshed"] = now

        if st.session_state.get("active_feed") == key:
            if conf["type"] == "rss_meteoalarm":
                last_seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
                new_count = meteoalarm_unseen_active_instances(entries, last_seen_ids)
                if new_count == 0:
                    pass
            elif conf["type"] in ("ec_async", "ec_grouped_compact", "nws_grouped_compact"):
                pass
            elif conf["type"] == "uk_grouped_compact":
                last_seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
                _, new_count = compute_counts(entries, conf, last_seen_ts)
                if new_count == 0:
                    st.session_state[f"{key}_last_seen_time"] = now
            else:
                last_seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
                _, new_count = compute_counts(entries, conf, last_seen_ts)
                if new_count == 0:
                    st.session_state[f"{key}_last_seen_time"] = now

    gc.collect()

rss_after = _rss_bytes()
if rss_after > MEMORY_HIGH_WATER:
    st.session_state["concurrency"] = max(MIN_CONC, st.session_state["concurrency"] - STEP)


# --------------------------------------------------------------------
# Header
# --------------------------------------------------------------------
st.title("Global Weather Monitor")
st.caption(f"Last refreshed: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}")
st.markdown("---")


# --------------------------------------------------------------------
# Desktop (buttons row + details)
# --------------------------------------------------------------------
if not FEED_CONFIG:
    st.info("No feeds configured.")
    st.stop()

MAX_BTNS_PER_ROW = 6

FEED_POSITIONS = {
    "ec":               (0, 0),
    "metoffice_uk":     (0, 1),
    "nws":              (1, 0),
    "meteoalarm":       (1, 1),
    "imd_india_today":  (1, 3),
    "cma_china":        (0, 3),
    "jma":              (0, 4),
    "pagasa":           (1, 4),
    "bom_multi":        (1, 5),
}

pinned_keys = set(FEED_POSITIONS.keys())
items = [(k, v) for k, v in FEED_CONFIG.items() if k not in pinned_keys]

_toggled = False
global_idx = 0  # retained for layout flow only; no longer used in widget keys

def _new_count_for_feed(key, conf, entries):
    if conf["type"] == "rss_meteoalarm":
        seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
        from computation import meteoalarm_unseen_active_instance_total
        return meteoalarm_unseen_active_instance_total(entries, seen_ids)

    if conf["type"] in ("ec_async", "ec_grouped_compact"):
        last_map = st.session_state.get(f"{key}_bucket_last_seen", {}) or {}
        return int(ec_new_total(entries, last_seen_bkey_map=last_map))

    if conf["type"] == "nws_grouped_compact":
        last_map = st.session_state.get(f"{key}_bucket_last_seen", {}) or {}
        return int(nws_new_total(entries, last_seen_bkey_map=last_map))

    if conf["type"] == "uk_grouped_compact":
        seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
        _, new_count = compute_counts(entries, conf, seen_ts)
        return new_count

    seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
    _, new_count = compute_counts(entries, conf, seen_ts)
    return new_count

seq_rows = (len(items) + MAX_BTNS_PER_ROW - 1) // MAX_BTNS_PER_ROW
pinned_rows = max((r for r, _ in FEED_POSITIONS.values()), default=-1) + 1 if FEED_POSITIONS else 0
num_rows = max(seq_rows, pinned_rows)
seq_iter = iter(items)

for row in range(num_rows):
    col_widths = [v for _ in range(MAX_BTNS_PER_ROW) for v in (1.5, 0.7)]
    row_cols = st.columns(col_widths, gap="small")

    for col in range(MAX_BTNS_PER_ROW):
        feed_key = None
        for k, (r, c) in FEED_POSITIONS.items():
            if r == row and c == col:
                feed_key = k; break
        if not feed_key:
            try:
                feed_key, conf = next(seq_iter)
            except StopIteration:
                feed_key = None

        btn_col = row_cols[col * 2]
        badge_col = row_cols[col * 2 + 1]

        if feed_key:
            conf = FEED_CONFIG[feed_key]
            entries = st.session_state[f"{feed_key}_data"]
            new_count = _new_count_for_feed(feed_key, conf, entries)

            with btn_col:
                is_active = (st.session_state.get("active_feed") == feed_key)
                clicked = st.button(
                    conf.get("label", feed_key.upper()),
                    key=f"btn_{feed_key}",  # ✅ stable identity-only key
                    use_container_width=True,
                    type=("primary" if is_active else "secondary"),
                )

            with badge_col:
                cnt = int(new_count or 0)
                if cnt > 0:
                    badge_col.markdown(
                        "<span style='display:inline-block;background:#FFEB99;color:#000;"
                        "padding:2px 8px;border-radius:6px;font-weight:700;font-size:0.90em;"
                        "white-space:nowrap;'>"
                        f"❗&nbsp;{cnt}&nbsp;New</span>",
                        unsafe_allow_html=True,
                    )
                else:
                    badge_col.markdown("&nbsp;", unsafe_allow_html=True)

            if clicked:
                is_open = (st.session_state.get("active_feed") == feed_key)
                if is_open:
                    commit_seen_for_feed(feed_key)
                    st.session_state["active_feed"] = None
                else:
                    prev_active = st.session_state.get("active_feed")
                    if prev_active and prev_active != feed_key:
                        commit_seen_for_feed(prev_active)
                    st.session_state["active_feed"] = feed_key
                _toggled = True

            global_idx += 1
        else:
            with btn_col: st.write("")
            with badge_col: st.markdown("&nbsp;", unsafe_allow_html=True)

if _toggled:
    _immediate_rerun()


# --------------------------------------------------------------------
# Details panel
# --------------------------------------------------------------------
active = st.session_state["active_feed"]
if active:
    st.markdown("---")
    conf = FEED_CONFIG[active]
    entries = st.session_state[f"{active}_data"]
    renderer = RENDERERS.get(conf["type"])

    if renderer:
        renderer(entries, {**conf, "key": active})
    else:
        render_empty_state()
