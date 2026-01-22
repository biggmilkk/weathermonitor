# renderers/cma.py
import html
import time
from collections import OrderedDict
from datetime import timezone as _tz

import streamlit as st
from dateutil import parser as dateparser

from computation import (
    attach_timestamp,
    sort_newest,
    alphabetic_with_last,
    compute_remaining_new_by_region,
)

# --------------------------
# Helpers
# --------------------------

def _as_list(entries):
    if not entries:
        return []
    return entries if isinstance(entries, list) else [entries]

def _norm(s: str | None) -> str:
    return (s or "").strip()

def _to_utc_label(pub: str | None) -> str | None:
    if not pub:
        return None
    try:
        dt = dateparser.parse(pub)
        if dt:
            return dt.astimezone(_tz.utc).strftime("%a, %d %b %y %H:%M:%S UTC")
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

def _safe_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()

def _render_empty_state():
    st.info("No active warnings that meet thresholds at the moment.")

CMA_COLORS = {
    "Yellow": "#FFD400",
    "Orange": "#FF7F00",
    "Red":    "#E60026",
    "Blue":   "#1E90FF",
}

def _render_alert(item: dict, *, is_new: bool) -> None:
    title = _norm(item.get("title")) or _norm(item.get("headline")) or "(no title)"
    level = _norm(item.get("level"))
    color = CMA_COLORS.get(level, "#888")

    prefix = "[NEW] " if is_new else ""
    title_html = (
        f"{prefix}<span style='color:{color};font-size:18px;'>&#9679;</span> "
        f"<strong>{html.escape(title)}</strong>"
    )
    st.markdown(_stripe_wrap(title_html, is_new), unsafe_allow_html=True)

    text_block = _norm(item.get("summary") or item.get("description") or item.get("body"))
    if text_block:
        st.markdown(html.escape(text_block).replace("\n", "  \n"))

    link = _norm(item.get("link"))
    if link:
        st.markdown(f"[Read more]({link})")

    published = _to_utc_label(item.get("published"))
    if published:
        st.caption(f"Published: {published}")

    st.markdown("---")


# --------------------------
# Public renderer
# --------------------------

def render(entries, conf):
    """
    CMA renderer with *province buckets* (single-level toggle).

    IMPORTANT:
    - last_seen is tracked per province (region) only.
    - Opening/closing a province marks ALL alerts in that province as seen.
    - Total remaining new count is computed via compute_remaining_new_by_region. :contentReference[oaicite:3]{index=3}
    """
    feed_key = conf.get("key", "cma")

    open_key        = f"{feed_key}_active_region"          # province currently open
    pending_key     = f"{feed_key}_region_pending_seen"    # { province: opened_ts }
    lastseen_key    = f"{feed_key}_region_last_seen"       # { province: last_seen_ts }
    rerun_guard_key = f"{feed_key}_rerun_guard"

    # Init state
    st.session_state.setdefault(open_key, None)
    st.session_state.setdefault(pending_key, {})
    st.session_state.setdefault(lastseen_key, {})
    st.session_state.setdefault(f"{feed_key}_remaining_new_total", 0)

    active_region = st.session_state[open_key]
    pending_seen  = st.session_state[pending_key]
    last_seen_map = st.session_state[lastseen_key]

    # Normalize timestamps + sort
    items = sort_newest(attach_timestamp(_as_list(entries)))

    # Ensure every item has a province-like region
    norm_items = []
    for e in items:
        region = _norm(e.get("region") or "全国")
        d = dict(e)
        d["region"] = region
        norm_items.append(d)

    if not norm_items:
        st.session_state[f"{feed_key}_remaining_new_total"] = 0
        _render_empty_state()
        return

    # Group by province
    groups: OrderedDict[str, list[dict]] = OrderedDict()
    for e in norm_items:
        groups.setdefault(e["region"], []).append(e)

    # Province ordering (put 全国 last)
    regions = alphabetic_with_last(groups.keys(), last_value="全国")

    # Update the total remaining new count for the top-level (country) badge
    total_new = compute_remaining_new_by_region(
        norm_items,
        region_field="region",
        last_seen_map=last_seen_map,
        ts_key="timestamp",
    )
    st.session_state[f"{feed_key}_remaining_new_total"] = int(total_new)

    # Action row
    cols_actions = st.columns([1, 6])
    with cols_actions[0]:
        if st.button("Mark all as seen", key=f"{feed_key}_mark_all_seen"):
            now_ts = time.time()
            for r in groups.keys():
                last_seen_map[r] = now_ts
            pending_seen.clear()
            st.session_state[open_key] = None
            st.session_state[lastseen_key] = last_seen_map
            st.session_state[f"{feed_key}_remaining_new_total"] = 0
            _safe_rerun()
            return

    # Render province buckets
    for region in regions:
        region_items = groups.get(region, [])
        if not region_items:
            continue

        last_seen = float(last_seen_map.get(region, 0.0) or 0.0)
        new_count = sum(1 for x in region_items if float(x.get("timestamp") or 0.0) > last_seen)

        cols = st.columns([0.7, 0.3])

        with cols[0]:
            # Province header button
            if st.button(region, key=f"{feed_key}:{region}:btn", use_container_width=True):
                prev = active_region

                # Commit last-seen for previous open region if switching
                if prev and prev != region:
                    ts_opened_prev = float(pending_seen.pop(prev, time.time()))
                    last_seen_map[prev] = ts_opened_prev

                # Toggle close/open
                if active_region == region:
                    ts_opened = float(pending_seen.pop(region, time.time()))
                    last_seen_map[region] = ts_opened
                    st.session_state[open_key] = None
                else:
                    st.session_state[open_key] = region
                    pending_seen[region] = time.time()

                # Persist maps + rerun
                st.session_state[pending_key] = pending_seen
                st.session_state[lastseen_key] = last_seen_map
                _safe_rerun()
                return

        with cols[1]:
            active_count = len(region_items)
            badges_html = (
                "<span style='margin-left:6px;padding:2px 6px;"
                "border-radius:4px;background:#eef0f3;color:#000;font-size:0.9em;"
                "font-weight:600;display:inline-block;'>"
                f"{active_count} Active</span>"
            )
            if new_count > 0:
                badges_html += (
                    "<span style='margin-left:8px;padding:2px 6px;"
                    "border-radius:4px;background:#FFEB99;color:#000;font-size:0.9em;"
                    "font-weight:bold;display:inline-block;'>"
                    f"❗ {new_count} New</span>"
                )
            st.markdown(badges_html, unsafe_allow_html=True)

        # If open, render all alerts in this province
        if st.session_state.get(open_key) == region:
            # Ensure newest first inside province too
            region_items_sorted = sort_newest(attach_timestamp(region_items))
            for a in region_items_sorted:
                is_new = float(a.get("timestamp") or 0.0) > last_seen
                _render_alert(a, is_new=is_new)

        st.markdown("---")
