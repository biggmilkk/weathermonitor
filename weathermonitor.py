import streamlit as st
import os
import sys
import time
import logging
from feeds import get_feed_definitions
from utils.scraper_registry import SCRAPER_REGISTRY
from streamlit_autorefresh import st_autorefresh

# Extend import path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)
st_autorefresh(interval=60 * 1000, key="autorefresh")

now = time.time()
REFRESH_INTERVAL = 60  # seconds
FEED_CONFIG = get_feed_definitions()

# --- Session Defaults ---
st.session_state.setdefault("last_refreshed", now)
st.session_state.setdefault("active_feed", None)

for key, conf in FEED_CONFIG.items():
    st.session_state.setdefault(f"{key}_data", [])
    st.session_state.setdefault(f"{key}_last_fetch", 0)
    st.session_state.setdefault(f"{key}_last_seen_time", 0.0)

# --- Fetch Data ---
for key, conf in FEED_CONFIG.items():
    if now - st.session_state[f"{key}_last_fetch"] > REFRESH_INTERVAL:
        try:
            scraper_func = SCRAPER_REGISTRY.get(conf["type"])
            if not scraper_func:
                raise ValueError(f"No scraper registered for type '{conf['type']}'")
            data = scraper_func(conf)
            st.session_state[f"{key}_data"] = data.get("entries", [])
            st.session_state[f"{key}_last_fetch"] = now
            st.session_state["last_refreshed"] = now
        except Exception as e:
            st.session_state[f"{key}_data"] = []
            logging.warning(f"[{key.upper()} FETCH ERROR] {e}")

# --- Header ---
st.title("Global Weather Monitor")
st.caption(f"Last refreshed: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}")
st.markdown("---")

# --- Feed Buttons ---
cols = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    with cols[i]:
        if st.button(conf["label"], key=f"btn_{key}", use_container_width=True):
            if st.session_state["active_feed"] == key:
                st.session_state["active_feed"] = None
            else:
                st.session_state["active_feed"] = key
                st.session_state[f"{key}_last_seen_time"] = time.time()

# --- New Alert Counters ---
count_cols = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = st.session_state[f"{key}_data"]
    last_seen = st.session_state[f"{key}_last_seen_time"]
    new_count = sum(
        1 for alert in entries
        if alert.get("published") and time.mktime(time.strptime(alert["published"], "%Y-%m-%dT%H:%M:%S%z")) > last_seen
    )
    total = len(entries)

    with count_cols[i]:
        if new_count > 0:
            st.markdown(f"""
                <div style="padding:8px;border-radius:6px;background-color:#ffeecc;">
                    ❗ {total} total / <strong>{new_count} new</strong>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div style="padding:8px;border-radius:6px;">
                    {total} total / {new_count} new
                </div>
            """, unsafe_allow_html=True)

# --- Feed Display ---
active = st.session_state["active_feed"]
if active:
    st.markdown("---")
    st.subheader(f"{FEED_CONFIG[active]['label']} Feed")
    alerts = sorted(
        st.session_state[f"{active}_data"],
        key=lambda x: x.get("published", ""),
        reverse=True
    )
    last_seen = st.session_state[f"{active}_last_seen_time"]

    for alert in alerts:
        pub_time = alert.get("published", "")
        is_new = False
        try:
            if pub_time:
                is_new = time.mktime(time.strptime(pub_time, "%Y-%m-%dT%H:%M:%S%z")) > last_seen
        except Exception:
            pass

        if is_new:
            st.markdown(
                "<div style='height:4px;background:red;margin:10px 0;border-radius:2px;'></div>",
                unsafe_allow_html=True
            )

        st.markdown(f"**{alert.get('title', '')}**")
        if "region" in alert and active != "rss_meteoalarm":
            st.caption(f"Region: {alert.get('region', '')}, {alert.get('province', '')}")

        summary = alert.get("summary", "")
        if summary:
            if active == "rss_meteoalarm":
                for line in summary.split("\n"):
                    line = line.strip()
                    if not line:
                        continue

                    if line.startswith("[") or line.startswith("[NEW] ["):
                        color = "gray"
                        if "[Yellow]" in line:
                            color = "#FFFF00"
                        elif "[Orange]" in line:
                            color = "#FF8C00"
                        elif "[Red]" in line:
                            color = "#FF0000"
                        st.markdown(
                            f"<span style='color:{color};font-size:18px'>&#9679;</span> {line}",
                            unsafe_allow_html=True
                        )
                    else:
                        st.markdown(f"**{line}**")
            else:
                st.markdown(summary)
        else:
            st.markdown("_No summary available._")

        if alert.get("link"):
            st.markdown(f"[Read more]({alert['link']})")
        if pub_time:
            st.caption(f"Published: {pub_time}")
        st.markdown("---")
