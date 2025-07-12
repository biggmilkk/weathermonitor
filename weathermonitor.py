import time
import streamlit as st
import os
import sys
import logging
from feeds import get_feed_definitions
from utils.scraper_registry import SCRAPER_REGISTRY
from streamlit_autorefresh import st_autorefresh

# Extend import path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Page setup
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

# Auto-refresh every 60 seconds
st_autorefresh(interval=60 * 1000, key="autorefresh")

now = time.time()
REFRESH_INTERVAL = 60  # seconds

FEED_CONFIG = get_feed_definitions()

# --- Session State Defaults ---
for key in FEED_CONFIG.keys():
    st.session_state.setdefault(f"{key}_data", [])
    st.session_state.setdefault(f"{key}_last_fetch", 0)
    st.session_state.setdefault(f"{key}_last_seen_time", 0.0)
    st.session_state.setdefault(f"{key}_pending_seen_time", None)

st.session_state.setdefault("last_refreshed", now)
st.session_state.setdefault("active_feed", None)

# --- Fetch Feed Data ---
for key, conf in FEED_CONFIG.items():
    last_fetch = st.session_state.get(f"{key}_last_fetch") or 0
    if now - last_fetch > REFRESH_INTERVAL:
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

# --- UI Header ---
st.title("Global Weather Monitor")
st.caption(
    f"Last refreshed: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}"
)
st.markdown("---")

# --- Handle Button Clicks ---
cols = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    with cols[i]:
        if st.button(conf["label"], key=f"btn_{key}", use_container_width=True):
            if st.session_state["active_feed"] == key:
                st.session_state["active_feed"] = None
            else:
                st.session_state["active_feed"] = key
                st.session_state[f"{key}_pending_seen_time"] = time.time()

# --- Counters ---
count_cols = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = st.session_state[f"{key}_data"]
    last_seen = st.session_state.get(f"{key}_last_seen_time") or 0.0

    def parse_timestamp(ts):
        try:
            from dateutil import parser
            return parser.parse(ts).timestamp()
        except Exception:
            return 0

    new_count = sum(
        1 for alert in entries
        if alert.get("published") and parse_timestamp(alert["published"]) > last_seen
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

    last_seen = st.session_state.get(f"{active}_last_seen_time") or 0.0

    def parse_timestamp(ts):
        try:
            from dateutil import parser
            return parser.parse(ts).timestamp()
        except Exception:
            return 0

    for alert in alerts:
        is_new = False
        pub_time = parse_timestamp(alert.get("published", ""))
        if pub_time > last_seen:
            is_new = True

        if is_new:
            st.markdown(
                "<div style='height:4px;background:red;margin:10px 0;border-radius:2px;'></div>",
                unsafe_allow_html=True
            )

        st.markdown(f"**{alert.get('title', '')}**")
        if "region" in alert:
            st.caption(f"Region: {alert.get('region', '')}, {alert.get('province', '')}")

        summary = alert.get("summary", "")
        if summary:
            if active == "rss_meteoalarm":
                for line in summary.split("\n"):
                    line = line.strip()
                    if not line:
                        continue

                    if line.lower() in {"today", "tomorrow"}:
                        st.markdown(f"<h4 style='margin-top:16px'>{line}</h4>", unsafe_allow_html=True)
                        continue

                    color = "#888"
                    level_text = ""
                    if line.startswith("[Yellow]"):
                        color = "#FFD700"
                        level_text = "[Yellow]"
                    elif line.startswith("[Orange]"):
                        color = "#FFA500"
                        level_text = "[Orange]"
                    elif line.startswith("[Red]"):
                        color = "#FF4500"
                        level_text = "[Red]"

                    text_only = line.replace(level_text, "").strip()

                    st.markdown(
                        f"<div style='margin-bottom:6px;'>"
                        f"<span style='color:{color};font-size:16px;'>&#9679;</span> {text_only}"
                        f"</div>",
                        unsafe_allow_html=True
                    )

            else:
                st.markdown(summary)
        else:
            st.markdown("_No summary available._")

        if alert.get("link"):
            st.markdown(f"[Read more]({alert['link']})")
        if alert.get("published"):
            st.caption(f"Published: {alert['published']}")
        st.markdown("---")

    # Apply pending seen time AFTER rendering
    pending_key = f"{active}_pending_seen_time"
    if pending_key in st.session_state:
        st.session_state[f"{active}_last_seen_time"] = st.session_state.pop(pending_key)
