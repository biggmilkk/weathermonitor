import os, sys, time, gc, logging, psutil
import streamlit as st
from dateutil import parser as dateparser
from streamlit_autorefresh import st_autorefresh

from feeds import get_feed_definitions
from utils.fetcher import run_fetch_round

# Pure logic imports
from computation import (
    compute_counts,
    meteoalarm_unseen_active_instances,
    meteoalarm_mark_and_sort,
    meteoalarm_snapshot_ids,
    ec_bucket_from_title,
    parse_timestamp,
    compute_imd_timestamps,
)

# Renderers map (from your renderers/__init__.py)
from renderers import RENDERERS


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def draw_badge(placeholder, count: int):
    if not placeholder:
        return
    if count > 0:
        placeholder.markdown(
            "<span style='display:inline-block;background:#FFEB99;color:#000;"
            "padding:2px 8px;border-radius:6px;font-weight:700;font-size:0.90em;"
            "white-space:nowrap;'>❗ {} New</span>".format(count),
            unsafe_allow_html=True,
        )
    else:
        placeholder.markdown("&nbsp;", unsafe_allow_html=True)

def render_empty_state():
    st.info("No active warnings at this time.")

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
FETCH_TTL = 60  # one scheduler tick = 60 seconds
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
# Controller-local helpers
# --------------------------------------------------------------------
def _entry_ts(e: dict) -> float:
    ts = e.get("timestamp")
    if isinstance(ts, (int, float)):
        return float(ts)
    return parse_timestamp(e.get("published"))

def ec_remaining_new_total(feed_key: str, entries: list) -> int:
    lastseen_map = st.session_state.get(f"{feed_key}_bucket_last_seen", {}) or {}
    total = 0
    for e in entries or []:
        bucket = ec_bucket_from_title((e.get("title") or "") or "")
        if not bucket:
            continue
        prov_name = (e.get("province_name") or str(e.get("province") or "")) or "Unknown"
        bkey = f"{prov_name}|{bucket}"
        last_seen = float(lastseen_map.get(bkey, 0.0))
        if _entry_ts(e) > last_seen:
            total += 1
    return total

def nws_remaining_new_total(feed_key: str, entries: list) -> int:
    lastseen_map = st.session_state.get(f"{feed_key}_bucket_last_seen", {}) or {}
    total = 0
    for e in entries or []:
        state  = (e.get("state") or e.get("state_name") or e.get("state_code") or "Unknown")
        bucket = (e.get("bucket") or e.get("event") or e.get("title") or "Alert")
        if not state or not bucket:
            continue
        bkey = f"{state}|{bucket}"
        last_seen = float(lastseen_map.get(bkey, 0.0))
        if _entry_ts(e) > last_seen:
            total += 1
    return total

# --------------------------------------------------------------------
# Cold boot: fetch ALL feeds once, ignoring groups
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
# Minute-group scheduler (fetch ONLY on the timer tick)
# --------------------------------------------------------------------
current_minute_index = int(time.time() // 60)
prev_minute_index = st.session_state.get("_last_minute_index")
is_timer_tick = (prev_minute_index != current_minute_index)
st.session_state["_last_minute_index"] = current_minute_index

minute_in_cycle_4 = (current_minute_index % 4) + 1  # 1..4

def group_is_due(group_code: str, minute_1_to_4: int) -> bool:
    g = (group_code or "g1").lower()
    if g == "g1":        return True
    if g == "g2_even":   return minute_1_to_4 in (2, 4)
    if g == "g2_odd":    return minute_1_to_4 in (1, 3)
    if g == "g4_1":      return minute_1_to_4 == 1
    if g == "g4_2":      return minute_1_to_4 == 2
    if g == "g4_3":      return minute_1_to_4 == 3
    if g == "g4_4":      return minute_1_to_4 == 4
    return True

GROUP_MIN_SPACING = {
    "g1": 60,
    "g2_even": 120, "g2_odd": 120,
    "g4_1": 240, "g4_2": 240, "g4_3": 240, "g4_4": 240,
}

to_fetch = {}
if is_timer_tick:
    now = time.time()
    for key, conf in FEED_CONFIG.items():
        grp = (conf.get("group") or "g1").lower()
        if group_is_due(grp, minute_in_cycle_4):
            last = float(st.session_state.get(f"{key}_last_fetch", 0))
            min_gap = GROUP_MIN_SPACING.get(grp, 60)
            if (now - last) >= (min_gap - 1):
                to_fetch[key] = conf

BATCH_SIZE = 10
if len(to_fetch) > BATCH_SIZE:
    to_fetch = dict(sorted(
        to_fetch.items(),
        key=lambda kv: float(st.session_state.get(f"{kv[0]}_last_fetch", 0))
    )[:BATCH_SIZE])

if to_fetch:
    results = cached_fetch_round(to_fetch, MAX_CONCURRENCY)  # no spinner
    now = time.time()
    for key, raw in results:
        entries = raw.get("entries", [])
        conf = FEED_CONFIG[key]

        if conf["type"] == "imd_current_orange_red":
            fp_key = f"{key}_fp_by_region"
            ts_key = f"{key}_ts_by_region"
            prev_fp = dict(st.session_state.get(fp_key, {}) or {})
            prev_ts = dict(st.session_state.get(ts_key, {}) or {})
            now_ts  = time.time()
            entries, fp_by_region, ts_by_region = compute_imd_timestamps(
                entries=entries, prev_fp=prev_fp, prev_ts=prev_ts, now_ts=now_ts,
            )
            st.session_state[fp_key] = fp_by_region
            st.session_state[ts_key] = ts_by_region

        st.session_state[f"{key}_data"] = entries
        st.session_state[f"{key}_last_fetch"] = now
        st.session_state["last_refreshed"] = now

        # Active feed bookkeeping (renderer-specific logic stays in renderers)
        if st.session_state.get("active_feed") == key:
            if conf["type"] == "rss_meteoalarm":
                last_seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
                new_count = meteoalarm_unseen_active_instances(entries, last_seen_ids)
                if new_count == 0:
                    st.session_state[f"{key}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
            elif conf["type"] in ("ec_async", "nws_grouped_compact"):
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

        # Button badge counts (pre-panel)
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
# Details panel — keep Meteoalarm/UK/JMA logic; EC/NWS delegated
# --------------------------------------------------------------------
def _render_feed_details(active, conf, entries):
    data_list = sorted(entries, key=lambda x: x.get("published", ""), reverse=True)

    if conf["type"] == "rss_bom_multi":
        RENDERERS["rss_bom_multi"](entries, {**conf, "key": active})
        return

    if conf["type"] == "ec_async":
        # EC fully lives in its renderer now
        RENDERERS["ec_grouped_compact"](entries, {**conf, "key": active})
        return

    if conf["type"] == "nws_grouped_compact":
        # NWS fully lives in its renderer now
        RENDERERS["nws_grouped_compact"](entries, {**conf, "key": active})
        return

    if conf["type"] == "uk_grouped_compact":
        if not entries:
            st.info("No active warnings that meet thresholds at the moment.")
            return
        RENDERERS["uk_grouped_compact"](entries, {**conf, "key": active})
        return

    if conf["type"] == "rss_meteoalarm":
        # Keep legacy controller logic until we move it into the renderer
        seen_ids = set(st.session_state[f"{active}_last_seen_alerts"])
        countries = [c for c in data_list if (c.get("alerts") or {}).get("today") or (c.get("alerts") or {}).get("tomorrow")]
        countries = meteoalarm_mark_and_sort(countries, seen_ids)
        for country in countries:
            RENDERERS["rss_meteoalarm"](country, {**conf, "key": active})
        st.session_state[f"{active}_last_seen_alerts"] = meteoalarm_snapshot_ids(countries)
        return

    if conf["type"] == "rss_jma":
        RENDERERS["rss_jma"](entries, {**conf, "key": active})
        return

    # Generic fallback
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
            ts = item.get("timestamp")
            if not isinstance(ts, (int, float)):
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
# Buttons row + details
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
global_idx = 0

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

seq_rows = (len(items) + MAX_BTNS_PER_ROW - 1) // MAX_BTNS_PER_ROW
pinned_rows = max(r for r, _ in FEED_POSITIONS.values()) + 1 if FEED_POSITIONS else 0
num_rows = max(seq_rows, pinned_rows)

seq_iter = iter(items)
for row in range(num_rows):
    col_widths = []
    for _ in range(MAX_BTNS_PER_ROW):
        col_widths.extend([1.5, 0.7])
    row_cols = st.columns(col_widths, gap="small")

    for col in range(MAX_BTNS_PER_ROW):
        feed_key = None
        for k, (r, c) in FEED_POSITIONS.items():
            if r == row and c == col:
                feed_key = k
                break

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
                    key=f"btn_{feed_key}_{global_idx}",
                    use_container_width=True,
                    type=("primary" if is_active else "secondary"),
                )

            with badge_col:
                try:
                    cnt = int(new_count)
                except Exception:
                    cnt = 0
                if cnt > 0:
                    badge_col.markdown(
                        "<span style='display:inline-block;background:#FFEB99;color:#000;"
                        "padding:2px 8px;border-radius:6px;font-weight:700;font-size:0.90em;"
                        "white-space:nowrap;'>❗&nbsp;{}&nbsp;New</span>".format(cnt),
                        unsafe_allow_html=True,
                    )
                else:
                    badge_col.markdown("&nbsp;", unsafe_allow_html=True)

            if clicked:
                if st.session_state.get("active_feed") == feed_key:
                    if conf["type"] == "rss_meteoalarm":
                        st.session_state[f"{feed_key}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
                    elif conf["type"] == "uk_grouped_compact":
                        st.session_state[f"{feed_key}_last_seen_time"] = time.time()
                    else:
                        st.session_state[f"{feed_key}_last_seen_time"] = time.time()
                    st.session_state["active_feed"] = None
                else:
                    st.session_state["active_feed"] = feed_key
                    if conf["type"] == "rss_meteoalarm":
                        st.session_state[f"{feed_key}_pending_seen_time"] = time.time()
                    elif conf["type"] in ("ec_async", "nws_grouped_compact"):
                        st.session_state[f"{feed_key}_pending_seen_time"] = None
                    elif conf["type"] == "uk_grouped_compact":
                        st.session_state[f"{feed_key}_pending_seen_time"] = time.time()
                    else:
                        st.session_state[f"{feed_key}_pending_seen_time"] = time.time()
                _toggled = True

            global_idx += 1
        else:
            with btn_col:
                st.write("")
            with badge_col:
                st.markdown("&nbsp;", unsafe_allow_html=True)

if _toggled:
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()

active = st.session_state["active_feed"]
if active:
    st.markdown("---")
    conf = FEED_CONFIG[active]
    entries = st.session_state[f"{active}_data"]
    _render_feed_details(active, conf, entries)
