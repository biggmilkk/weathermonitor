# renderers/jma.py
import html
import time
from collections import OrderedDict

import streamlit as st
from dateutil import parser as dateparser

# Logic helpers (no UI)
from computation import attach_timestamp, sort_newest, mark_is_new_ts


# --------------------------
# Local UI helpers
# --------------------------

def _norm(s: str | None) -> str:
    return (s or "").strip()

def _fmt_utc(ts: float) -> str:
    return time.strftime("%a, %d %b %y %H:%M:%S UTC", time.gmtime(ts))

def _stripe_wrap(content: str, is_new: bool) -> str:
    """Add red stripe for NEW sections."""
    if not is_new:
        return content
    return (
        "<div style='border-left:4px solid #e40000;"
        "padding-left:10px;margin:8px 0;'>"
        f"{content}</div>"
    )

def render_empty_state():
    st.info("No active warnings that meet thresholds at the moment.")


# --------------------------
# JMA-specific rendering
# --------------------------

JMA_COLORS = {"Warning": "#FF7F00", "Emergency": "#E60026"}

def render(entries, conf):
    """
    JMA (Japan) – grouped by region, deduplicated titles with colored bullets.
    NEW is determined via a single feed-level last_seen timestamp.
    """
    feed_key = conf.get("key", "jma")

    items = entries if isinstance(entries, list) else (entries or [])
    if not items:
        render_empty_state()
        return

    # Newest first + ensure timestamps
    items = sort_newest(attach_timestamp(items))

    # Mark _is_new against a single last-seen per feed
    last_seen_key = f"{feed_key}_last_seen_time"
    last_seen = float(st.session_state.get(last_seen_key) or 0.0)
    items = mark_is_new_ts(items, last_seen_ts=last_seen)

    # Group by region
    groups = OrderedDict()
    for e in items:
        region = _norm(e.get("region", "")) or "(Unknown Region)"
        groups.setdefault(region, []).append(e)

    any_rendered = False
    for region, alerts in groups.items():
        if not alerts:
            continue
        any_rendered = True

        # Region header; stripe if any alert is new
        region_header = _stripe_wrap(
            f"<h2>{html.escape(region)}</h2>",
            any(a.get("_is_new") for a in alerts),
        )
        st.markdown(region_header, unsafe_allow_html=True)

        # Deduplicate by title; keep "NEW" if any instance is new
        title_new_map = OrderedDict()
        for a in alerts:
            t = _norm(a.get("title", ""))
            if not t:
                continue
            title_new_map[t] = title_new_map.get(t, False) or bool(a.get("_is_new"))

        for t, is_new_any in title_new_map.items():
            # Color based on inferred level keyword in title
            level = "Emergency" if "Emergency" in t else ("Warning" if "Warning" in t else None)
            color = JMA_COLORS.get(level, "#888")
            prefix = "[NEW] " if is_new_any else ""
            st.markdown(
                f"<div style='margin-bottom:4px;'>"
                f"<span style='color:{color};font-size:16px;'>&#9679;</span> {prefix}{html.escape(t)}"
                f"</div>",
                unsafe_allow_html=True
            )

        # Footer info from newest in this region
        newest = alerts[0]
        ts = float(newest.get("timestamp") or 0.0)
        if ts:
            st.caption(f"Published: {_fmt_utc(ts)}")
        link = _norm(newest.get("link"))
        if link:
            st.markdown(f"[Read more]({link})")

        st.markdown("---")

    if not any_rendered:
        render_empty_state()

    # Commit last_seen at end
    st.session_state[last_seen_key] = time.time()
