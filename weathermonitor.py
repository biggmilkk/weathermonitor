import time
import streamlit as st
import os
import sys
import logging
from feeds import get_feed_definitions
from utils.scraper_registry import SCRAPER_REGISTRY
from streamlit_autorefresh import st_autorefresh
from dateutil import parser as dateparser

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

# Unique identifier for a MeteoAlarm alert entry
def alert_id(entry):
    return f"{entry['level']}|{entry['type']}|{entry['from']}|{entry['until']}"

# Helper to parse timestamp string to epoch
def parse_timestamp(ts):
    try:
        return dateparser.parse(ts).timestamp()
    except Exception:
        return 0

# Fetch data if stale
for key, conf in FEED_CONFIG.items():
    last_fetch = st.session_state.get(f"{key}_last_fetch") or 0
    if now - last_fetch > REFRESH_INTERVAL:
        try:
            scraper = SCRAPER_REGISTRY.get(conf["type"])
            if not scraper:
                raise ValueError(f"No scraper for type '{conf['type']}'")
            data = scraper(conf)
            st.session_state[f"{key}_data"] = data.get("entries", [])
            st.session_state[f"{key}_last_fetch"] = now
            st.session_state["last_refreshed"] = now
        except Exception as e:
            st.session_state[f"{key}_data"] = []
            logging.warning(f"[{key.upper()} FETCH ERROR] {e}")

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
                st.session_state["active_feed"] = None
            else:
                st.session_state["active_feed"] = key
                st.session_state[f"{key}_pending_seen_time"] = time.time()

# New/total counts per feed
tabs = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = st.session_state[f"{key}_data"]
    if conf["type"] == "rss_meteoalarm":
        seen_alerts = st.session_state.get(f"{key}_last_seen_alerts", set())
        flat = [
            e
            for country in entries
            for alerts in country.get("alerts", {}).values()
            for e in alerts
            if e["level"] in ["Orange", "Red"]
        ]
        total = len(flat)
        new_count = sum(1 for e in flat if alert_id(e) not in seen_alerts)
    else:
        last_seen = st.session_state.get(f"{key}_last_seen_time") or 0.0
        total = len(entries)
        new_count = sum(
            1
            for alert in entries
            if alert.get("published") and parse_timestamp(alert["published"]) > last_seen
        )

    with tabs[i]:
        if new_count > 0:
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
active = st.session_state["active_feed"]
if active:
    st.markdown("---")
    conf = FEED_CONFIG[active]
    st.subheader(f"{conf['label']} Feed")

    data_list = sorted(
        st.session_state[f"{active}_data"], key=lambda x: x.get("published", ""), reverse=True
    )

    # Prepare seen tracking
    if conf["type"] == "rss_meteoalarm":
        seen_alerts = st.session_state.get(f"{active}_last_seen_alerts", set())
    else:
        last_seen_time = st.session_state.get(f"{active}_last_seen_time") or 0.0

    # CMA color map
    cma_color_map = {
        'Red':    '#E60026',  # Red
        'Orange': '#FF7F00',  # Orange
    }

    for item in data_list:
        # New indicator
        pub_ts = parse_timestamp(item.get("published", ""))
        if conf["type"] == "rss_meteoalarm":
            country_alerts = [e for alerts in item.get("alerts", {}).values() for e in alerts]
            if any(alert_id(e) not in seen_alerts for e in country_alerts):
                st.markdown(
                    "<div style='height:4px;background:red;margin:10px 0;border-radius:2px;'></div>",
                    unsafe_allow_html=True,
                )
        else:
            if pub_ts > last_seen_time:
                st.markdown(
                    "<div style='height:4px;background:red;margin:10px 0;border-radius:2px;'></div>",
                    unsafe_allow_html=True,
                )

        # Rendering per feed
        if conf["type"] == "rss_meteoalarm":
            st.markdown(f"<h3 style='margin-bottom:4px'>{item.get('title', '')}</h3>", unsafe_allow_html=True)
            for day in ['today','tomorrow']:
                entries_day = item['alerts'].get(day, [])
                if entries_day:
                    st.markdown(f"<h4 style='margin-top:16px'>{day.capitalize()}</h4>", unsafe_allow_html=True)
                    for e in entries_day:
                        try:
                            dt_from = dateparser.parse(e['from'])
                            dt_until = dateparser.parse(e['until'])
                            fmt_from = dt_from.strftime("%H:%M UTC %B %d")
                            fmt_until = dt_until.strftime("%H:%M UTC %B %d")
                        except:
                            fmt_from, fmt_until = e['from'], e['until']
                        is_new = alert_id(e) not in seen_alerts
                        prefix = '[NEW] ' if is_new else ''
                        color = {'orange':'#FF7F00','red':'#E60026'}.get(e['level'].lower(),'#888')
                        st.markdown(
                            f"<div style='margin-bottom:6px;'>"
                            f"<span style='color:{color};font-size:16px;'>&#9679;</span> {prefix}[{e['level']}] {e['type']} - {fmt_from} - {fmt_until}"
                            f"</div>", unsafe_allow_html=True
                        )
        elif conf["type"] == "rss_cma":
            level = item.get('level') or 'Orange'
            color = cma_color_map.get(level, '#888')
            st.markdown(
                f"<div style='margin-bottom:8px;'>"
                f"<span style='color:{color};font-size:18px;'>&#9679;</span> **{item['title']}**"
                f"</div>", unsafe_allow_html=True
            )
            st.caption(f"Region: {item['region']}")
            st.markdown(item['summary'])
            if item.get('link'):
                st.markdown(f"[Read more]({item['link']})")
            if item.get('published'):
                st.caption(f"Published: {item['published']}")
            st.markdown("---")
        else:
            st.markdown(item.get('summary','_No summary available._'))
            if item.get('link'):
                st.markdown(f"[Read more]({item['link']})")
            if item.get('published'):
                st.caption(f"Published: {item['published']}")
            st.markdown("---")

    # Update last seen
    pending_key = f"{active}_pending_seen_time"
    if pending_key in st.session_state:
        if conf['type'] == 'rss_meteoalarm':
            snapshot = set()
            for country in st.session_state[f"{active}_data"]:
                for alerts_list in country.get("alerts", {}).values():
                    for e in alerts_list:
                        snapshot.add(alert_id(e))
            st.session_state[f"{active}_last_seen_alerts"] = snapshot
        else:
            st.session_state[f"{active}_last_seen_time"] = st.session_state[pending_key]
        st.session_state.pop(pending_key, None)
