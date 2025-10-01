import os, sys, time, gc, logging, asyncio, httpx, psutil, traceback
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
    ec_bucket_from_title,
    draw_badge,
    safe_int,
    alert_id,
    meteoalarm_country_has_alerts,
    meteoalarm_mark_and_sort,
    meteoalarm_snapshot_ids,
    render_empty_state,
)

nest_asyncio.apply()

# --------------------------------------------------------------------
# Logging & perf guardrails
# --------------------------------------------------------------------
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
HTTP2_ENABLED = False
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
st_autorefresh(interval=FETCH_TTL * 1000, key="auto")

FEED_CONFIG = get_feed_definitions()
for key, conf in FEED_CONFIG.items():
    st.session_state.setdefault(f"{key}_data", [])
    if conf["type"] == "rss_meteoalarm":
        st.session_state.setdefault(f"{key}_last_seen_alerts", tuple())
        st.session_state.setdefault(f"{key}_pending_seen_time", None)
    elif conf["type"] in ("ec_async", "nws_grouped_compact"):
        st.session_state.setdefault(f"{key}_remaining_new_total", 0)
    else:
        st.session_state.setdefault(f"{key}_last_seen_time", 0.0)

st.session_state.setdefault("active_feed", None)

# --------------------------------------------------------------------
# HTTP & fetch helpers
# --------------------------------------------------------------------
def _client():
    return httpx.AsyncClient(http2=HTTP2_ENABLED, timeout=20.0)

async def _fetch_one(session, key, conf):
    try:
        scraper = SCRAPER_REGISTRY[conf["scraper"]]
        return key, await scraper(session, conf)
    except Exception as e:
        logging.warning("Fetch failed for %s: %s", key, e)
        traceback.print_exc()
        return key, None

async def _fetch_all():
    tasks = []
    async with _client() as session:
        for key, conf in FEED_CONFIG.items():
            tasks.append(_fetch_one(session, key, conf))
        res = await asyncio.gather(*tasks, return_exceptions=True)
    return res

# --------------------------------------------------------------------
# Poll + store
# --------------------------------------------------------------------
def _store_entries(key, conf, entries):
    if entries is None:
        return
    st.session_state[f"{key}_data"] = entries

# --------------------------------------------------------------------
# Rendering of details panel
# --------------------------------------------------------------------
def _render_feed_details(active, conf, entries, badge_placeholders):
    data_list = entries or []

    if conf["type"] == "ec_async":
        # ec grouped renderer handles remaining new calculation internally
        RENDERERS["ec_grouped"](entries, {**conf, "key": active})

    elif conf["type"] == "nws_grouped_compact":
        RENDERERS["nws_grouped_compact"](entries, {**conf, "key": active})

    elif conf["type"] == "uk_grouped_compact":
        if not data_list:
            render_empty_state()
            return
        # Filter by threshold; renderer splits and draws per-bucket
        if not any(ec_bucket_from_title(e.get("title","")) for e in data_list):
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
            return

        RENDERERS.get(conf["type"], RENDERERS["generic"])(entries, {**conf, "key": active})

# --------------------------------------------------------------------
# Count-down / maintenance actions during refresh
# --------------------------------------------------------------------
def _immediate_rerun():
    st.experimental_rerun()

# --------------------------------------------------------------------
# New count helper for the badge row
# --------------------------------------------------------------------
# Count new active Meteoalarm alerts (per-alert basis, not per-type)
def _meteoalarm_new_active_alerts(entries: list[dict], seen_ids: set[str]) -> int:
    """Count *alerts* (today+tomorrow) that are currently active and not yet seen.
    Uses renderer.alert_id(e) which keys by level|type|from|until.
    """
    total = 0
    for country in entries or []:
        alerts = (country.get("alerts") or {})
        for day in ("today", "tomorrow"):
            for e in alerts.get(day, []) or []:
                try:
                    aid = alert_id(e)
                except Exception:
                    aid = None
                if aid and aid not in seen_ids:
                    total += 1
    return int(max(0, total))

def _new_count_for(key, conf, entries):
    if conf["type"] == "rss_meteoalarm":
        seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
        return _meteoalarm_new_active_alerts(entries, seen_ids)
    if conf["type"] == "ec_async":
        val = st.session_state.get(f"{key}_remaining_new_total")
        return int(val) if isinstance(val, int) else int(ec_remaining_new_total(key, entries) or 0)
    if conf["type"] == "nws_grouped_compact":
        val = st.session_state.get(f"{key}_remaining_new_total")
        return int(val) if isinstance(val, int) else int(nws_remaining_new_total(key, entries) or 0)
    if conf["type"] == "uk_grouped_compact":
        # UK now uses feed-level last_seen_time like BOM/JMA
        seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
        _, new_count = compute_counts(entries, conf, seen_ts)
        return new_count
    seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
    _, new_count = compute_counts(entries, conf, seen_ts)
    return new_count

# --------------------------------------------------------------------
# Desktop (buttons row + details)
# --------------------------------------------------------------------
MAX_BTNS_PER_ROW = 8
items = list(FEED_CONFIG.items())

badge_placeholders = {}
_toggled = False
global_idx = 0  # ensure unique button keys across all rows

def _new_count_for_feed(key, conf, entries):
    if conf["type"] == "rss_meteoalarm":
        seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
        return _meteoalarm_new_active_alerts(entries, seen_ids)
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
    cols = st.columns(len(row_items))
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
            draw_badge(badge_ph, new_count)

            if clicked:
                if is_active:
                    # Toggle off -> mark seen
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
                        st.session_state[f"{key}_remaining_new_total"] = safe_int(new_count)
                    elif conf["type"] == "uk_grouped_compact":
                        st.session_state[f"{key}_last_seen_time"] = time.time()
                    else:
                        st.session_state[f"{key}_last_seen_time"] = time.time()
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

# --------------------------------------------------------------------
# Background refresh
# --------------------------------------------------------------------
async def _refresh_loop():
    try:
        results = await _fetch_all()
    except Exception as e:
        logging.warning("Batch fetch failed: %s", e)
        traceback.print_exc()
        return

    for key, entries in results:
        conf = FEED_CONFIG.get(key)
        if conf is None:
            continue
        _store_entries(key, conf, entries)

        # Update “new” counters opportunistically when the panel is open
        if st.session_state.get("active_feed") == key:
            if conf["type"] == "rss_meteoalarm":
                last_seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
                new_count = _meteoalarm_new_active_alerts(entries, last_seen_ids)
                if new_count == 0:
                    st.session_state[f"{key}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
            elif conf["type"] in ("ec_async", "nws_grouped_compact"):
                # grouped feeds use per-bucket last_seen that renderers maintain
                pass
            elif conf["type"] == "uk_grouped_compact":
                # UK uses per-feed last_seen_time
                pass
            else:
                # generic: when open, keep last_seen_time stale until user toggles off
                pass

# --------------------------------------------------------------------
# Drive the refresh
# --------------------------------------------------------------------
try:
    asyncio.run(_refresh_loop())
except RuntimeError:
    # In notebooks/Streamlit reruns, event loop may already be running
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_refresh_loop())

# --------------------------------------------------------------------
# Post-refresh memory check + dynamic concurrency hint
# --------------------------------------------------------------------
rss_after = _rss_bytes()
if rss_after > MEMORY_HIGH_WATER and st.session_state["concurrency"] > MIN_CONC:
    st.session_state["concurrency"] = max(MIN_CONC, st.session_state["concurrency"] - STEP)
elif rss_after < MEMORY_LOW_WATER and st.session_state["concurrency"] < MAX_CONC:
    st.session_state["concurrency"] = min(MAX_CONC, st.session_state["concurrency"] + STEP)
