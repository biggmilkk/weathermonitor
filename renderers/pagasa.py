# renderers/pagasa.py
import html
import streamlit as st
from dateutil import parser as dateparser
from datetime import timezone as _tz

# Pure logic helpers (no UI side effects)
from computation import attach_timestamp, sort_newest

# --------------------------
# Local UI helpers (no deps)
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
            return dt.astimezone(_tz.utc).strftime("%a, %d %b %y %H:%M:%S UTC")
    except Exception:
        pass
    return pub

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
# Single-card renderer
# --------------------------

def _render_card(item: dict, *, is_new: bool) -> None:
    """
    PAGASA card with colored bullets:
      - Severe   -> red (#E60026)
      - Moderate -> amber (#FF7F00)

    Expected item fields:
      - title/bucket, severity, region, summary, link, published
    """
    severity = (_norm(item.get("severity")) or "").title()
    color = "#E60026" if severity == "Severe" else "#FF7F00"  # amber for Moderate

    title = _norm(item.get("title") or item.get("bucket") or "PAGASA Alert")
    title_html = (
        f"<div><span style='color:{color};font-size:16px;'>&#9679;</span> "
        f"<strong>{html.escape(title)}</strong></div>"
    )

    st.markdown(_stripe_wrap(title_html, is_new), unsafe_allow_html=True)

    region = _norm(item.get("region", ""))
    if region:
        st.caption(f"Region: {region}")

    summary = item.get("summary")
    if summary:
        st.markdown(summary)

    link = _norm(item.get("link"))
    if link and title:
        st.markdown(f"[Read more]({link})")

    pub_label = _to_utc_label(item.get("published"))
    if pub_label:
        st.caption(f"Published: {pub_label}")

    st.markdown("---")

# --------------------------
# Public renderer entrypoint
# --------------------------

def render(entries: list[dict], conf: dict) -> None:
    """
    PAGASA renderer (list-aware, read-only).
    - Accepts the full entries list from the controller.
    - Normalizes timestamps and sorts newest-first.
    - Highlights each item as NEW if its timestamp > feed-level last_seen_time.
    - DOES NOT commit 'seen' state; controller handles clear-on-close.
    """
    feed_key = conf.get("key", "pagasa")
    items = entries or []

    if not items:
        _render_empty_state()
        return

    # Normalize & order
    items = sort_newest(attach_timestamp(items))

    # Read-only 'seen' reference (controller commits on CLOSE)
    last_seen_ts = float(st.session_state.get(f"{feed_key}_last_seen_time") or 0.0)

    for item in items:
        ts = float(item.get("timestamp") or 0.0)
        if ts <= 0.0:
            # Fallback if any item missed normalization
            try:
                ts = dateparser.parse(item.get("published") or "").timestamp()
            except Exception:
                ts = 0.0
        is_new = ts > last_seen_ts
        _render_card(item, is_new=is_new)
