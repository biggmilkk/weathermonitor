# renderers/meteoalarm.py
import html
import streamlit as st
from dateutil import parser as dateparser

# --------------------------
# Local UI helpers
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

def render_empty_state():
    st.info("No active warnings that meet thresholds at the moment.")

def _is_new_flag(obj) -> bool:
    """Accept either '_is_new' (from computation.meteoalarm_mark_and_sort) or 'is_new'."""
    return bool((obj or {}).get("_is_new") or (obj or {}).get("is_new"))

def _alerts_for_day(alerts_map: dict, day: str):
    """Case-insensitive access for 'today'/'tomorrow' keys."""
    return (
        (alerts_map or {}).get(day)
        or (alerts_map or {}).get(day.capitalize())
        or (alerts_map or {}).get(day.title())
        or []
    )

# --------------------------
# Public renderer
# --------------------------

def render(item: dict, conf: dict) -> None:
    """
    Render a single Meteoalarm country block.
    Stripes the country header if any alert (today/tomorrow) is marked as new.

    Expected 'item' shape (from your scraper/computation):
      {
        "title": "...", "link": "...", "published": "...",
        "total_alerts": int,
        "counts": {"by_day": {...}, "by_type": {...}} (optional),
        "alerts": {"today": [...], "tomorrow": [...]}
      }
    """
    def _any_new(country) -> bool:
        alerts_dict = (country.get("alerts") or {})
        for day in ("today", "tomorrow"):
            for e in _alerts_for_day(alerts_dict, day):
                if _is_new_flag(e):
                    return True
        return False

    # Country header (with total severe from scraper)
    try:
        total_severe = int(item.get("total_alerts") or 0)
    except Exception:
        total_severe = 0

    title = _norm(item.get("title", "")) or "Meteoalarm"
    header_txt  = f"{title} ({total_severe} active)" if total_severe > 0 else title
    header_html = _stripe_wrap(f"<h2>{html.escape(header_txt)}</h2>", _any_new(item))
    st.markdown(header_html, unsafe_allow_html=True)

    counts   = item.get("counts") or {}
    by_day   = counts.get("by_day")  if isinstance(counts, dict) else {}
    by_type  = counts.get("by_type") if isinstance(counts, dict) else {}

    def _day_level_type_count(day: str, level: str, typ: str) -> int | None:
        """Prefer exact per-day count; fall back to per-type bucket totals if missing."""
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

    for day in ["today", "tomorrow"]:
        alerts = _alerts_for_day(item.get("alerts") or {}, day)
        if alerts:
            st.markdown(f"<h4 style='margin-top:16px'>{day.capitalize()}</h4>", unsafe_allow_html=True)
            for e in alerts:
                # Time window
                try:
                    dt1 = dateparser.parse(e.get("from", "")).strftime("%b %d %H:%M UTC")
                    dt2 = dateparser.parse(e.get("until", "")).strftime("%b %d %H:%M UTC")
                except Exception:
                    dt1, dt2 = _norm(e.get("from", "")), _norm(e.get("until", ""))

                level = _norm(e.get("level", ""))
                typ   = _norm(e.get("type", ""))
                color = {"Orange": "#FF7F00", "Red": "#E60026"}.get(level, "#888")
                prefix = "[NEW] " if _is_new_flag(e) else ""

                n = _day_level_type_count(day, level, typ)
                count_str = f" ({n} active)" if isinstance(n, int) and n > 0 else ""

                text = f"{prefix}[{level}] {typ}{count_str} – {dt1} to {dt2}"
                st.markdown(
                    f"<div style='margin-bottom:6px;'>"
                    f"<span style='color:{color};font-size:16px;'>&#9679;</span> {text}</div>",
                    unsafe_allow_html=True,
                )

    link = _norm(item.get("link"))
    if link and title:
        st.markdown(f"[Read more]({link})")

    published = _to_utc_label(item.get("published"))
    if published:
        st.caption(f"Published: {published}")

    st.markdown('---')
