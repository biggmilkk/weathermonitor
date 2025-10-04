# renderers/uk.py
import html
from collections import OrderedDict

import streamlit as st
from dateutil import parser as dateparser
from datetime import timezone as _tz

# Logic helpers (no UI)
from computation import attach_timestamp, sort_newest

# -------------------------------------------------
# Local UI helpers
# -------------------------------------------------

# Match IMD's look-and-feel for colored bullets
_UK_DOT = {
    "yellow": "#FFCC00",
    "amber":  "#FF9900",
    "red":    "#FF0000",
}

def _norm(s: str | None) -> str:
    return (s or "").strip()

def _to_utc_label(pub: str | None) -> str | None:
    """Return a uniform UTC label for display, falling back to the original string."""
    if not pub:
        return None
    try:
        dt = dateparser.parse(pub)
        if dt:
            return dt.astimezone(_tz.utc).strftime("%a, %d %b %y %H:%M:%S UTC")
    except Exception:
        pass
    return pub

def _as_list(entries):
    if not entries:
        return []
    return entries if isinstance(entries, list) else [entries]

def _stripe_wrap(content: str, is_new: bool) -> str:
    """Red left border for 'new' blocks (same pattern as other feeds)."""
    if not is_new:
        return content
    return (
        "<div style='border-left:4px solid #e40000;"
        "padding-left:10px;margin:8px 0;'>"
        f"{content}</div>"
    )

def _extract_severity(alert: dict) -> str | None:
    """
    Try to pull a UK severity label (Yellow/Amber/Red) from known fields,
    falling back to a case-insensitive search in the text fields.
    """
    for key in ("severity", "level", "bucket"):
        v = _norm(alert.get(key))
        if v:
            v_low = v.lower()
            if any(x in v_low for x in ("yellow", "amber", "red")):
                return v_low.title()

    # Fallback: look inside title/summary text
    text = " ".join(
        t for t in [
            _norm(alert.get("title")),
            _norm(alert.get("summary")),
        ] if t
    ).lower()

    if "amber" in text:
        return "Amber"
    if "red" in text:
        return "Red"
    if "yellow" in text:
        return "Yellow"
    return None

def _severity_dot(sev: str | None) -> str:
    """
    Return a colored • span matching the severity (defaults to neutral gray).
    """
    hexcolor = _UK_DOT.get((sev or "").lower(), "#888")
    return f"<span style='color:{hexcolor};font-size:16px;'>&#9679;</span>"

def _render_empty_state():
    st.info("No active warnings that meet thresholds at the moment.")


# -------------------------------------------------
# Public renderer entrypoint
# -------------------------------------------------

def render(entries, conf):
    """
    Met Office (UK) — compact list by region.

    - Region header (striped if any alert is NEW).
    - Each alert is ONE line: colored bullet + [optional NEW] + linked summary sentence.
    - Published line underneath.
    - Renderer is read-only; controller handles seen-state commits.
    """
    feed_key = conf.get("key", "metoffice_uk")

    items = _as_list(entries)
    if not items:
        _render_empty_state()
        return

    # Normalize & sort newest-first
    items = sort_newest(attach_timestamp(items))  # parse/add 'timestamp' as needed

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

        # Region header (striped if any NEW items)
        has_new = any(float(a.get("timestamp") or 0.0) > last_seen for a in alerts)
        region_header = _stripe_wrap(f"<h2>{html.escape(region)}</h2>", has_new)
        st.markdown(region_header, unsafe_allow_html=True)

        # Render alerts
        for a in alerts:
            is_new = float(a.get("timestamp") or 0.0) > last_seen
            prefix_new = "[NEW] " if is_new else ""

            summary_line = _norm(a.get("summary")) or _norm(a.get("bucket") or a.get("title") or "(no title)")
            link = _norm(a.get("link"))

            # Colored bullet per severity (Yellow/Amber/Red)
            sev = _extract_severity(a)
            dot = _severity_dot(sev)

            if summary_line and link:
                # dot + [NEW] + linked summary
                st.markdown(f"{dot} {prefix_new}[{summary_line}]({link})", unsafe_allow_html=True)
            else:
                st.markdown(f"{dot} {prefix_new}**{summary_line}**", unsafe_allow_html=True)

            pub_label = _to_utc_label(a.get("published"))
            if pub_label:
                st.caption(f"Published: {pub_label}")

        st.markdown("---")

    if not any_rendered:
        _render_empty_state()
