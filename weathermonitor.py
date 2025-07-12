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
st_autorefresh(interval=60 * 1000, key="autorefresh")

now = time.time()
REFRESH_INTERVAL = 60  # seconds
FEED_CONFIG = get_feed_definitions()

# Cache loader/saver (generic per-feed)
def load_feed_cache(feed_key):
    path = os.path.join("data", f"{feed_key}_cache.json")
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_feed_cache(feed_key, data):
    path = os.path.join("data", f"{feed_key}_cache.json")
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logging.warning(f"[CACHE ERROR:{feed_key}] {e}")

# --- Session State Defaults ---
for key in FEED_CONFIG.keys():
    st.session_state.setdefault(f"{key}_seen_count", 0)
    st.session_state.setdefault(f"{key}_data", [])
    st.session_state.setdefault(f"{key}_last_fetch", 0)
    st.session_state.setdefault(f"{key}_fingerprints", {})  # for cache support

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
            
            # Load cache and pass to scraper (if supported)
            cache = load_feed_cache(key)
            conf["cache"] = cache
            
            data = scraper_func(conf)
            st.session_state[f"{key}_data"] = data.get("entries", [])
            st.session_state[f"{key}_last_fetch"] = now
            st.session_state["last_refreshed"] = now

            # Store fingerprints for possible caching
            if "fingerprints" in data:
                st.session_state[f"{key}_fingerprints"] = data["fingerprints"]

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
                st.session_state[f"{key}_seen_count"] = len(st.session_state[f"{key}_data"])
                st.session_state["active_feed"] = None
            else:
                prev = st.session_state["active_feed"]
                if prev:
                    st.session_state[f"{prev}_seen_count"] = len(st.session_state[f"{prev}_data"])
                    
                    # Save cache if fingerprints available
                    if st.session_state.get(f"{prev}_fingerprints"):
                        save_feed_cache(prev, st.session_state[f"{prev}_fingerprints"])

                st.session_state["active_feed"] = key

# --- Counters (HTML highlight when new) ---
count_cols = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    data = st.session_state[f"{key}_data"]
    total = len(data)
    new = max(0, total - st.session_state[f"{key}_seen_count"])
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
    seen_count = st.session_state[f"{active}_seen_count"]
    for i, alert in enumerate(alerts):
        is_new = i < (len(alerts) - seen_count)
        if is_new:
            st.markdown(
                "<div style='height:4px;background:red;margin:10px 0;border-radius:2px;'></div>",
                unsafe_allow_html=True
            )
        st.markdown(f"**{alert.get('title', '')}**")
        if "region" in alert and active != "meteoalarm":
            st.caption(f"Region: {alert.get('region', '')}, {alert.get('province', '')}")

        summary = alert.get("summary", "")
        if summary:
            if active == "meteoalarm":
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
