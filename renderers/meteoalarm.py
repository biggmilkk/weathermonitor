# renderers/meteoalarm.py
import html
from datetime import timezone as _tz
import streamlit as st
from dateutil import parser as dateparser

# Pure logic helpers
from computation import (
    meteoalarm_mark_and_sort,
)

# --------------------------
# Local UI helpers
# --------------------------

def _norm(s: str | None) -> str:
    return (s or "").strip()

def _to_utc_label(s: str | None) -> str | None:
    if not s:
        return None
    try:
        dt = dateparser.parse(s)
        if dt:
            return dt.astimezone(_tz.utc).strftime("%a, %d %b %y %H:%M:%S UTC")
    except Exception:
        pass
    return s

def _stripe_wrap(content: str, is_new: bool) -> str:
    if not is_new:
        return content
    return (
        "<div style='border-left:4px solid #e40000;"
        "padding-left:10px;margin:8px 0;'>"
        f"{content}</div>"
    )

def _alerts_for_day(alerts_map: dict, day: str):
    """Case-insensitive access for 'today'/'tomorrow' keys."""
    return (
        (alerts_map or {}).get(day)
        or (alerts_map or {}).get(day.capitalize())
        or (alerts_map or {}).get(day.title())
        or []
    )

def _any_new(alerts_map: dict) -> bool:
    for day in ("today", "tomorrow"):
        for e in _alerts_for_day(alerts_map, day):
            if (e or {}).get("_is_new") or (e or {}).get("is_new"):
                return True
    return False

def _day_level_type_count(by_day: dict, by_type: dict, day: str, level: str, typ: str) -> int | None:
    """Prefer exact per-day count; fall back to per-type totals."""
    if isinstance(by_day, dict):
        d = by_day.get(day) or by_day.get(day.capitalize()) or by_day.get(day.title())
        if isinstance(d, dict):
            n = d.get(f"{level}|{typ}")
            if isinstance(n, int) and n > 0:
                return n
    if isinstance(by_type, dict):
        bucket = by_type.get(typ)
        if isinstance(bucket, dict):
            n = bucket.get(level) or bucket.get("total")
            if isinstance(n, int) and n > 0:
                return n
    return None

def _render_country(country: dict):
    """Render a single country section, with striped header if any alert is new."""
    title = _norm(country.get("title") or country.get("name") or "")
    counts = country.get("counts") or {}

    alerts_map = country.get("alerts") or {}
    by_day  = counts.get("by_day")  if isinstance(counts, dict) else {}
    by_type = counts.get("by_type") if isinstance(counts, dict) else {}

    # Compute the total strictly from what is currently visible (rows we'll render).
    # If we have no visible rows, fall back to counts.total / total_alerts.
    visible_total = 0
    for day in ("today", "tomorrow"):
        rows = _alerts_for_day(alerts_map, day)
        for e in rows or []:
            level = _norm(e.get("level", ""))
            typ   = _norm(e.get("type", ""))
            n = _day_level_type_count(by_day, by_type, day, level, typ)
            if not isinstance(n, int) or n <= 0:
                n = 1  # treat a visible category row with unknown count as one active
            visible_total += n

    # Build header using the visible_total; if there are no visible rows, fall back.
    fallback_total = 0
    try:
        fallback_total = int(counts.get("total") or country.get("total_alerts") or 0)
    except Exception:
        fallback_total = int(country.get("total_alerts") or 0)

    header_total = visible_total if visible_total > 0 else fallback_total
    header = title or "Meteoalarm"
    if header_total > 0:
        header = f"{header} ({header_total} active)"

    st.markdown(
        _stripe_wrap(f"<h2>{html.escape(header)}</h2>", _any_new(alerts_map)),
        unsafe_allow_html=True,
    )

    for day in ("today", "tomorrow"):
        alerts = _alerts_for_day(alerts_map, day)
        if not alerts:
            continue

        st.markdown(f"<h4 style='margin-top:16px'>{day.capitalize()}</h4>", unsafe_allow_html=True)
        for e in alerts:
            # Time window
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

    link = _norm(country.get("link"))
    if link and title:
        st.markdown(f"[Read more]({link})")

    published = _to_utc_label(country.get("published"))
    if published:
        st.caption(f"Published: {published}")

    st.markdown("---")


# --------------------------
# Public renderer entrypoint
# --------------------------

def render(entries: list[dict], conf: dict) -> None:
    """
    Meteoalarm renderer (standalone).

    Responsibilities:
      - Derive 'new' flags and sort groups via computation.meteoalarm_mark_and_sort().
      - Render each country block.
      - DOES NOT auto-commit “seen” on open.
      - DOES NOT provide a 'mark all as seen' button (clear-on-close is handled in controller).
    """
    feed_key = conf.get("key", "meteoalarm")
    st.session_state.setdefault(f"{feed_key}_last_seen_alerts", tuple())

    seen_ids = set(st.session_state[f"{feed_key}_last_seen_alerts"])
    countries = [c for c in (entries or []) if (c.get("alerts") or {}).get("today") or (c.get("alerts") or {}).get("tomorrow")]

    # Mark and sort (adds _is_new and sorts by severity/time per day)
    countries = meteoalarm_mark_and_sort(countries, seen_ids)

    if not countries:
        st.info("No active warnings that meet thresholds at the moment.")
        return

    for country in countries:
        _render_country(country)
