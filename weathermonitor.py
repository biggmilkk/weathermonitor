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

# Unique identifier for MeteoAlarm alert entries
def alert_id(entry):
    return f"{entry['level']}|{entry['type']}|{entry['from']}|{entry['until']}"

# Helper to parse timestamps
def parse_timestamp(ts):
    try:
        return dateparser.parse(ts).timestamp()
    except Exception:
        return 0

# Fetch fresh data if stale
for key, conf in FEED_CONFIG.items():
    last_fetch = st.session_state[f"{key}_last_fetch"] or 0
    last_seen_time = st.session_state[f"{key}_last_seen_time"]
    if now - last_fetch > REFRESH_INTERVAL:
        try:
            scraper = SCRAPER_REGISTRY.get(conf["type"])
            if not scraper:
                raise ValueError(f"No scraper for type '{conf['type']}'")
            result = scraper(conf)
            entries = result.get("entries", [])
            st.session_state[f"{key}_data"] = entries
            st.session_state[f"{key}_last_fetch"] = now
            st.session_state["last_refreshed"] = now
            # If this feed is open and there are no new alerts on refresh, advance the last_seen_time
            if st.session_state.get("active_feed") == key:
                if conf["type"] != "rss_meteoalarm":
                    # for non-MeteoAlarm feeds, clear out seen if no new
                    if not any(
                        parse_timestamp(a.get("published", "")) > last_seen_time
                        for a in entries
                    ):
                        st.session_state[f"{key}_last_seen_time"] = now
                else:
                    # for MeteoAlarm, snapshot only if no new alerts
                    seen = st.session_state.get(f"{key}_last_seen_alerts", set())
                    flat = [e for country in entries for alerts in country.get('alerts', {}).values() for e in alerts]
                    if not any(alert_id(e) not in seen for e in flat):
                        st.session_state[f"{key}_last_seen_alerts"] = set(alert_id(e) for e in flat)
        except Exception as e:
            logging.warning(f"[{key.upper()} FETCH ERROR] {e}")
            st.session_state[f"{key}_data"] = []

# Main UI
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
            # If clicking the open feed, close and mark as seen
            if st.session_state["active_feed"] == key:
                st.session_state[f"{key}_last_seen_time"] = time.time()
                st.session_state["active_feed"] = None
            else:
                # Opening new feed: pending seen for later snapshot
                st.session_state["active_feed"] = key
                st.session_state[f"{key}_pending_seen_time"] = time.time()

# New/total counters
tabs = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = st.session_state[f"{key}_data"]
    if conf["type"] == "rss_meteoalarm":
        seen = st.session_state[f"{key}_last_seen_alerts"]
        flat = [e for country in entries for alerts in country['alerts'].values() for e in alerts]
        total = len(flat)
        new_count = sum(1 for e in flat if alert_id(e) not in seen)
    else:
        last_seen = st.session_state[f"{key}_last_seen_time"]
        total = len(entries)
        new_count = sum(
            1 for a in entries if a.get("published") and parse_timestamp(a['published']) > last_seen
        )
    with tabs[i]:
        if new_count:
            st.markdown(
                f"""
                <div style="padding:8px;border-radius:6px;background-color:#ffeecc;">
                    ❗ {total} total / <strong>{new_count} new</strong>
                </div>
                """, unsafe_allow_html=True
            )
        else:
            st.markdown(
                f"""
                <div style="padding:8px;border-radius:6px;">
                    {total} total / {new_count} new
                </div>
                """, unsafe_allow_html=True
            )

# Display feed details
active = st.session_state['active_feed']
if active:
    st.markdown("---")
    conf = FEED_CONFIG[active]
    st.subheader(f"{conf['label']} Feed")

    data_list = sorted(
        st.session_state[f"{active}_data"], key=lambda x: x.get('published',''), reverse=True
    )

    # Prepare seen trackers
    if conf['type'] == 'rss_meteoalarm':
        seen = st.session_state[f"{active}_last_seen_alerts"]
    else:
        last_seen_time = st.session_state[f"{active}_last_seen_time"]

    cma_colors = {'Orange':'#FF7F00','Red':'#E60026'}

    for item in data_list:
        # Red bar for new
        pub_ts = parse_timestamp(item.get('published',''))
        if conf['type']=='rss_meteoalarm':
            flat = [e for alerts in item['alerts'].values() for e in alerts]
            if any(alert_id(e) not in seen for e in flat):
                st.markdown("<div style='height:4px;background:red;margin:10px 0;border-radius:2px;'></div>", unsafe_allow_html=True)
        else:
            if pub_ts>last_seen_time:
                st.markdown("<div style='height:4px;background:red;margin:10px 0;border-radius:2px;'></div>", unsafe_allow_html=True)

        # Render each feed
        if conf['type']=='rss_meteoalarm':
            st.markdown(f"<h3 style='margin-bottom:4px'>{item['title']}</h3>", unsafe_allow_html=True)
            for day in ['today','tomorrow']:
                alerts= item['alerts'].get(day,[])
                if alerts:
                    st.markdown(f"<h4 style='margin-top:16px'>{day.capitalize()}</h4>", unsafe_allow_html=True)
                    for e in alerts:
                        try:
                            f1=dateparser.parse(e['from']).strftime('%H:%M UTC %B %d')
                            f2=dateparser.parse(e['until']).strftime('%H:%M UTC %B %d')
                        except:
                            f1,f2=e['from'],e['until']
                        is_new=alert_id(e) not in seen
                        pref='[NEW] ' if is_new else ''
                        col={'Orange':'#FF7F00','Red':'#E60026'}.get(e['level'],'#888')
                        st.markdown(f"<div style='margin-bottom:6px;'><span style='color:{col};font-size:16px;'>&#9679;</span> {pref}[{e['level']}] {e['type']} – {f1} – {f2}</div>", unsafe_allow_html=True)
        elif conf['type']=='rss_cma':
            lvl=item.get('level','Orange')
            col=cma_colors.get(lvl,'#888')
            st.markdown(f"<div style='margin-bottom:8px;'><span style='color:{col};font-size:18px;'>&#9679;</span> <strong>{item['title']}</strong></div>", unsafe_allow_html=True)
            st.caption(f"Region: {item.get('region','')}")
            st.markdown(item.get('summary',''))
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

    # Snapshot last seen after rendering
    pk=f"{active}_pending_seen_time"
    if pk in st.session_state:
        if conf['type']=='rss_meteoalarm':
            snap=set(alert_id(e) for country in st.session_state[f"{active}_data"] for alerts in country['alerts'].values() for e in alerts)
            st.session_state[f"{active}_last_seen_alerts"]=snap
        else:
            st.session_state[f"{active}_last_seen_time"]=st.session_state[pk]
        st.session_state.pop(pk, None)
