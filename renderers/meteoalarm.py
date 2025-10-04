# meteoalarm.py
import html
from collections import OrderedDict
from datetime import timezone as _tz
from dateutil import parser as dateparser
import streamlit as st

# Logic helpers from computation.py
from computation import (
    attach_timestamp,
    sort_newest,
    meteoalarm_snapshot_ids,
    meteoalarm_unseen_active_instances,
)

# --------------------------------------------------------------------
# Utility helpers
# --------------------------------------------------------------------

def _norm(s: str | None) -> str:
    return (s or "").strip()

def _to_utc_label(pub: str | None) -> str | None:
    """Return UTC label for display."""
    if not pub:
        return None
    try:
        dt = dateparser.parse(pub)
        if dt:
            return dt.astimezone(_tz.utc).strftime("%a, %d %b %y %H:%M:%S UTC")
    except Exception:
        pass
    return pub

def _as_list(x):
    return x if isinstance(x, list) else ([x] if x else [])

def _stripe_wrap(content: str, is_new: bool) -> str:
    """Add red left border for new sections."""
    if not is_new:
        return content
    return (
        "<div style='border-left:4px solid #e40000;"
        "padding-left:10px;margin:8px 0;'>"
        f"{content}</div>"
    )

def _any_new(alerts_map: dict) -> bool:
    """True if any alert in the map is new."""
    for alerts in (alerts_map or {}).values():
        for a in alerts or []:
            if a.get("_is_new") or a.get("is_new"):
                return True
    return False

def _alerts_for_day(alerts_map: dict, day: str):
    """Return list of alerts for a given day key ('today', 'tomorrow')."""
    return _as_list(alerts_map.get(day))

def _day_level_type_count(by_day: dict, by_type: dict, day: str, level: str, typ: str):
    """Optional counter extraction for '(x active)' suffix."""
    try:
        return by_day.get(day, {}).get(level, {}).get(typ)
    except Exception:
        return None

# --------------------------------------------------------------------
# Main render logic
# --------------------------------------------------------------------

def _render_country(country: dict):
    """Render a single country section, with striped header if any alert is new."""
    title = _norm(country.get("title") or country.get("name") or "")
    # --- Special case: shorten overly long names ---
    if title.strip().lower() == "united kingdom of great britain and northern ireland":
        title = "United Kingdom"

    total_severe = 0
    counts = country.get("counts") or {}
    if isinstance(counts, dict):
        try:
            total_severe = int(counts.get("total") or country.get("total_alerts") or 0)
        except Exception:
            total_severe = int(country.get("total_alerts") or 0)

    header = title or "Meteoalarm"
    if total_severe > 0:
        header = f"{header} ({total_severe} active)"

    alerts_map = country.get("alerts") or {}
    st.markdown(
        _stripe_wrap(f"<h2>{html.escape(header)}</h2>", _any_new(alerts_map)),
        unsafe_allow_html=True,
    )

    by_day  = counts.get("by_day")  if isinstance(counts, dict) else {}
    by_type = counts.get("by_type") if isinstance(counts, dict) else {}

    for day in ("today", "tomorrow"):
        alerts = _alerts_for_day(alerts_map, day)
        if not alerts:
            continue

        st.markdown(f"<h4 style='margin-top:16px'>{day.capitalize()}</h4>", unsafe_allow_html=True)
        for e in alerts:
            try:
                dt1 = dateparser.parse(e.get("from", "")).astimezone(_tz.utc).strftime("%b %d %H:%M UTC")
                dt2 = dateparser.parse(e.get("until", "")).astimezone(_tz.utc).strftime("%b %d %H:%M UTC")
            except Exception:
                dt1, dt2 = _norm(e.get("from", "")), _norm(e.get("until", ""))

            level = _norm(e.get("level", ""))
            typ   = _norm(e.get("type", ""))
            color = {"Orange": "#FF7F00", "Red": "#E60026"}.get(level, "#888")
            prefix = "[NEW] " if ((e or {}).get("_is_new") or (e or {}).get("is_new")) else ""

            n = _day_level_type_count(by_day, by_type, day, level, typ)
            count_str = f" ({n} active)" if isinstance(n, int) and n > 0 else ""

            text = f"{prefix}[{level}] {typ}{count_str} – {dt1} to {dt2}"
            st.markdown(
                f"<div style='margin-bottom:6px;'>"
                f"<span style='color:{color};font-size:16px;'>&#9679;</span> {text}</div>",
                unsafe_allow_html=True,
            )

    # --- Fix region link handling for UK ---
    link = _norm(country.get("link"))
    if title == "United Kingdom" and link.endswith("/GB"):
        link = link[:-2] + "UK"

    if link and title:
        st.markdown(f"[Read more]({link})")

    published = _to_utc_label(country.get("published"))
    if published:
        st.caption(f"Published: {published}")

    st.markdown("---")

# --------------------------------------------------------------------
# Public entrypoint
# --------------------------------------------------------------------

def render(entries, conf):
    """
    Meteoalarm feed renderer.
    Groups alerts by country, shows per-day breakdown, and marks new entries.
    """
    items = _as_list(entries)
    if not items:
        st.info("No Meteoalarm alerts at this time.")
        return

    # Normalize timestamps and sort
    items = sort_newest(attach_timestamp(items))

    # Sort by name
    items.sort(key=lambda c: str(c.get("name") or c.get("title") or ""))

    for c in items:
        _render_country(c)
