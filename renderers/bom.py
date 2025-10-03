# renderers/bom.py
import html
import time
from collections import OrderedDict

import streamlit as st
from dateutil import parser as dateparser

# logic helpers only (no UI)
from computation import attach_timestamp, sort_newest, mark_is_new_ts


# --------------------------
# Local UI helpers (no deps)
# --------------------------

def _norm(s: str | None) -> str:
    return (s or "").strip()

def _to_utc_label(pub: str | None) -> str | None:
    if not pub:
        return None
    try:
        dt = dateparser.parse(pub)
        if dt:
            return dt.astimezone().strftime("%a, %d %b %y %H:%M:%S UTC")
    except Exception:
        pass
    return pub

def _stripe_wrap(content: str, is_new: bool) -> str:
    if not is_new:
        return content
    return (
        "<div style='border-left:4px solid #e40000;"
        "padding-left:10px;margin:8px 0;'>"
        f"{content}</div>"
    )


# --------------------------
# BoM-specific rendering
# --------------------------

_BOM_ORDER = [
    "NSW & ACT",
    "Northern Territory",
    "Queensland",
    "South Australia",
    "Tasmania",
    "Victoria",
    "Western Australia",
]

def render(entries, conf):
    """
    BoM (Australia) – grouped by state.
    Uses a single feed-level last_seen_time to mark NEW items.
    """
    feed_key = conf.get("key", "bom")

    # normalize -> newest first
    items = sort_newest(attach_timestamp(entries if isinstance(entries, list) else (entries or [])))

    # mark new vs last_seen
    last_seen_key = f"{feed_key}_last_seen_time"
    last_seen = float(st.session_state.get(last_seen_key) or 0.0)
    items = mark_is_new_ts(items, last_seen_ts=last_seen)

    # group by state
    groups = OrderedDict()
    for e in items:
        st_name = _norm(e.get("state", ""))
        groups.setdefault(st_name, []).append(e)

    any_rendered = False
    for state in _BOM_ORDER:
        alerts = groups.get(state, [])
        if not alerts:
            continue
        any_rendered = True

        # state header (striped if any new)
        state_header = _stripe_wrap(
            f"<h2>{html.escape(state)}</h2>",
            any(a.get("_is_new") for a in alerts),
        )
        st.markdown(state_header, unsafe_allow_html=True)

        for a in alerts:
            prefix = "[NEW] " if a.get("_is_new") else ""
            title  = _norm(a.get("title", "")) or "(no title)"
            link   = _norm(a.get("link"))

            if title and link:
                st.markdown(f"{prefix}**[{title}]({link})**")
            else:
                st.markdown(f"{prefix}**{title}**")

            summary = a.get("summary")
            if summary:
                st.write(summary)

            pub_label = _to_utc_label(a.get("published"))
            if pub_label:
                st.caption(f"Published: {pub_label}")

        st.markdown("---")

    if not any_rendered:
        st.info("No active warnings that meet thresholds at the moment.")

    # commit last_seen at end
    st.session_state[last_seen_key] = time.time()
