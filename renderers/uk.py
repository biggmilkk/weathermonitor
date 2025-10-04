# renderers/uk.py
import html
from collections import OrderedDict

import streamlit as st
from dateutil import parser as dateparser
from datetime import timezone as _tz

# Logic helpers (no UI)
from computation import attach_timestamp, sort_newest


# --------------------------
# Local, UI-only helpers
# --------------------------

def _norm(s: str | None) -> str:
    return (s or "").strip()

def _to_utc_label(pub: str | None) -> str | None:
    """Return a uniform UTC label for display, falling back to the original string."""
    if not pub:
        return None
    try:
        dt = dateparser.parse(pub)
        if dt:
            # Force UTC label for consistency across environments
            return dt.astimezone(_tz.utc).strftime("%a, %d %b %y %H:%M:%S UTC")
    except Exception:
        pass
    return pub

def _as_list(entries):
    if not entries:
        return []
    return entries if isinstance(entries, list) else [entries]

def _stripe_wrap(content: str, is_new: bool) -> str:
    """
    Wrap content with a red left border if is_new is True.
    Uses HTML so it can wrap any inline markdown.
    """
    if not is_new:
        return content
    return (
        "<div style='border-left:4px solid #e40000;"
        "padding-left:10px;margin:8px 0;'>"
        f"{content}</div>"
    )

def _render_empty_state():
    st.info("No active warnings that meet thresholds at the moment.")


# --------------------------
# Public renderer entrypoint
# --------------------------

def render(entries, conf):
    """
    Met Office (UK) — grouped by region, flat list of alerts.

    Behavior:
      - Uses a single feed-level last_seen_time stored in st.session_state (READ-ONLY here).
      - Highlights region headers with a red stripe if any alert in that region is NEW.
      - Marks individual items with a [NEW] prefix when newer than last_seen_time.
      - DOES NOT write/commit 'seen' state: clear-on-close is handled in the controller.
    """
    feed_key = conf.get("key", "uk")

    items = _as_list(entries)
    if not items:
        _render_empty_state()
        return

    # Normalize order: newest first, and ensure each item has 'timestamp'
    items = sort_newest(attach_timestamp(items))

    # Single last-seen timestamp for the whole feed (READ-ONLY)
    last_seen_key = f"{feed_key}_last_seen_time"
    last_seen = float(st.session_state.get(last_seen_key) or 0.0)

    # Group by region
    groups = OrderedDict()
    for e in items:
        region = _norm(e.get("region") or "Unknown")
        groups.setdefault(region, []).append(e)

    any_rendered = False
    for region, alerts in groups.items():
        if not alerts:
            continue
        any_rendered = True

        # Region header (striped if any NEW)
        has_new = any(float(a.get("timestamp") or 0.0) > last_seen for a in alerts)
        region_header = _stripe_wrap(f"<h2>{html.escape(region)}</h2>", has_new)
        st.markdown(region_header, unsafe_allow_html=True)

        # Region items
        for a in alerts:
            is_new = float(a.get("timestamp") or 0.0) > last_seen
            prefix = "[NEW] " if is_new else ""
            title  = a.get("bucket") or _norm(a.get("title", "")) or "(no title)"
            link   = _norm(a.get("link"))

            if title and link:
                st.markdown(f"{prefix}**[{title}]({link})**")
            else:
                st.markdown(f"{prefix}**{title}**")

            if a.get("summary"):
                st.write(a["summary"])

            pub_label = _to_utc_label(a.get("published"))
            if pub_label:
                st.caption(f"Published: {pub_label}")

        st.markdown("---")

    if not any_rendered:
        _render_empty_state()
