import streamlit as st
import os
import sys
import json
import time
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

    # Seen tracking
    feed_type = FEED_CONFIG[key]["type"]
    if feed_type == "rss_meteoalarm":
        st.session_state.setdefault(f"{key}_seen_fingerprints", [])
    else:
        st.session_state.setdefault(f"{key}_seen_ids", set())

st.session_state.setdefault("last_refreshed", now)
st.session_state.setdefault("active_feed", None)

# --- Fetch Feed Data ---
for key, conf in FEED_CONFIG.items():
    last_fetch = st.session_state[f"{key}_last_fetch"]
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

                # Mark all alerts in this feed as seen
                entries = st.session_state[f"{key}_data"]
                feed_type = FEED_CONFIG[key]["type"]

                if feed_type == "rss_meteoalarm":
                    fingerprints = []
                    for alert in entries:
                        for line in alert.get("summary", "").split("\n"):
                            match = (
                                line.replace("[NEW] ", "").strip()
                                if line.startswith("[NEW] ")
                                else line.strip()
                            )
                            if match.startswith("["):
                                fingerprints.append(match)
                    st.session_state[f"{key}_seen_fingerprints"] = fingerprints
                else:
                    ids = {
                        alert.get("id")
                        or alert.get("guid")
                        or alert.get("link")
                        or alert.get("title")
                        for alert in entries
                    }
                    st.session_state[f"{key}_seen_ids"] = ids

# --- Counters (HTML highlight when new) ---
count_cols = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = st.session_state[f"{key}_data"]
    total = len(entries)

    feed_type = conf["type"]
    if feed_type == "rss_meteoalarm":
        seen = set(st.session_state[f"{key}_seen_fingerprints"])
        all_fps = set()
        for alert in entries:
            for line in alert.get("summary", "").split("\n"):
                line = line.strip().replace("[NEW] ", "")
                if line.startswith("["):
                    all_fps.add(line)
        new = len(all_fps - seen)
    else:
        seen_ids = st.session_state[f"{key}_seen_ids"]
        current_ids = {
            alert.get("id")
            or alert.get("guid")
            or alert.get("link")
            or alert.get("title")
            for alert in entries
        }
        new = len(current_ids - seen_ids)

    with count_cols[i]:
        if new > 0:
            st.markdown(f"""
                <div style="padding:8px;border-radius:6px;background-color:#ffeecc;">
                    ❗ {total} total / <strong>{new} new</strong>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div style="padding:8px;border-radius:6px;">
                    {total} total / {new} new
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

    feed_type = FEED_CONFIG[active]["type"]
    seen_set = (
        set(st.session_state[f"{active}_seen_fingerprints"])
        if feed_type == "rss_meteoalarm"
        else st.session_state[f"{active}_seen_ids"]
    )

    for alert in alerts:
        is_new = False

        if feed_type == "rss_meteoalarm":
            lines = alert.get("summary", "").split("\n")
            for line in lines:
                line_clean = line.replace("[NEW] ", "").strip()
                if line_clean.startswith("[") and line_clean not in seen_set:
                    is_new = True
                    break
        else:
            alert_id = (
                alert.get("id")
                or alert.get("guid")
                or alert.get("link")
                or alert.get("title")
            )
            if alert_id not in seen_set:
                is_new = True

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
                max_len = 500
                is_truncated = len(summary) > max_len
                truncated = summary[:max_len] + ("..." if is_truncated else "")
                st.markdown(truncated)
        else:
            st.markdown("_No summary available._")

        if alert.get("link"):
            st.markdown(f"[Read more]({alert['link']})")
        if alert.get("published"):
            st.caption(f"Published: {alert['published']}")
        st.markdown("---")
