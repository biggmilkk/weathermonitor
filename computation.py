import time
import streamlit as st
import os
import sys
import logging
from feeds import get_feed_definitions
from utils.scraper_registry import SCRAPER_REGISTRY
from streamlit_autorefresh import st_autorefresh
from computation import compute_counts, advance_seen
from renderer import RENDERERS

# Ensure scrapers are on path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

# Auto-refresh every minute
st_autorefresh(interval=60 * 1000, key="autorefresh")

# Timing constants
now = time.time()
REFRESH_INTERVAL = 60  # seconds

# Load feed definitions
FEED_CONFIG = get_feed_definitions()

# Initialize session state for feeds
for key, conf in FEED_CONFIG.items():
    st.session_state.setdefault(f"{key}_data", [])
    st.session_state.setdefault(f"{key}_last_fetch", 0)
    st.session_state.setdefault(f"{key}_last_seen_time", 0.0)
    st.session_state.setdefault(f"{key}_pending_seen_time", None)
    if conf["type"] == "rss_meteoalarm":
        st.session_state.setdefault(f"{key}_last_seen_alerts", set())

st.session_state.setdefault("last_refreshed", now)
st.session_state.setdefault("active_feed", None)

# Unique identifier for MeteoAlarm alert entries
def alert_id(entry):
    return f"{entry['level']}|{entry['type']}|{entry['from']}|{entry['until']}"

# Fetch fresh data and advance seen markers
for key, conf in FEED_CONFIG.items():
    last_fetch = st.session_state[f"{key}_last_fetch"] or 0
    if now - last_fetch > REFRESH_INTERVAL:
        try:
            scraper = SCRAPER_REGISTRY.get(conf["type"])
            if not scraper:
                raise ValueError(f"No scraper for type '{conf['type']}'")
            entries = scraper(conf).get("entries", [])
            st.session_state[f"{key}_data"] = entries
            st.session_state[f"{key}_last_fetch"] = now
            st.session_state["last_refreshed"] = now

            # If this feed is open, advance its seen marker on idle refresh
            if st.session_state.get("active_feed") == key:
                if conf["type"] == "rss_meteoalarm":
                    last_seen = st.session_state[f"{key}_last_seen_alerts"]
                else:
                    last_seen = st.session_state[f"{key}_last_seen_time"]

                new_marker = advance_seen(
                    conf,
                    entries,
                    last_seen,
                    now,
                    alert_id_fn=alert_id,
                )
                if new_marker is not None:
                    if conf["type"] == "rss_meteoalarm":
                        st.session_state[f"{key}_last_seen_alerts"] = new_marker
                    else:
                        st.session_state[f"{key}_last_seen_time"] = new_marker

        except Exception as e:
            logging.warning(f"[{key.upper()} FETCH ERROR] {e}")
            st.session_state[f"{key}_data"] = []

# Main layout
st.title("Global Weather Monitor")
st.caption(
    f"Last refreshed: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}"
)
st.markdown("---")

# Feed selection buttons
cols = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    with cols[i]:
        if st.button(conf["label"], key=f"btn_{key}", use_container_width=True):
            if st.session_state["active_feed"] == key:
                # Closing feed: mark all as seen
                if conf["type"] == "rss_meteoalarm":
                    flat = [e for country in st.session_state[f"{key}_data"] for alerts in country.get('alerts', {}).values() for e in alerts]
                    st.session_state[f"{key}_last_seen_alerts"] = set(alert_id(e) for e in flat)
                else:
                    st.session_state[f"{key}_last_seen_time"] = time.time()
                st.session_state["active_feed"] = None
            else:
                # Opening feed: defer marking seen until end of render
                st.session_state["active_feed"] = key
                st.session_state[f"{key}_pending_seen_time"] = time.time()

# New/total counters
tabs = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = st.session_state[f"{key}_data"]
    if conf["type"] == "rss_meteoalarm":
        last_seen = st.session_state[f"{key}_last_seen_alerts"]
    else:
        last_seen = st.session_state[f"{key}_last_seen_time"]

    total, new_count = compute_counts(
        entries, conf, last_seen, alert_id_fn=alert_id
    )

    with tabs[i]:
        if new_count:
            st.markdown(
                f"""
                <div style="padding:8px;border-radius:6px;background-color:#ffeecc;">
                    ❗ {total} total / <strong>{new_count} new</strong>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"""
                <div style="padding:8px;border-radius:6px;">
                    {total} total / {new_count} new
                </div>
                """,
                unsafe_allow_html=True,
            )

# Display selected feed details
active = st.session_state.get("active_feed")
if active:
    st.markdown("---")
    conf = FEED_CONFIG[active]
    entries = st.session_state[f"{active}_data"]

    # For MeteoAlarm, annotate each alert with is_new
    if conf["type"] == "rss_meteoalarm":
        seen_ids = st.session_state[f"{active}_last_seen_alerts"]
        for country in entries:
            for alerts in country.get("alerts", {}).values():
                for e in alerts:
                    e["is_new"] = alert_id(e) not in seen_ids

    # Render each item via registry
    for item in entries:
        RENDERERS.get(conf["type"], lambda i, c: None)(item, conf)

    # Snapshot last seen after rendering
    pending_key = f"{active}_pending_seen_time"
    if pending_key in st.session_state:
        if conf["type"] == "rss_meteoalarm":
            flat = [e for country in entries for alerts in country.get('alerts', {}).values() for e in alerts]
            st.session_state[f"{active}_last_seen_alerts"] = set(alert_id(e) for e in flat)
        else:
            st.session_state[f"{active}_last_seen_time"] = st.session_state[pending_key]
        st.session_state.pop(pending_key, None)
