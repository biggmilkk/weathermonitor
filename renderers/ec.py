# renderers/ec.py
import streamlit as st
import html
from dateutil import parser as dateparser
from computation import attach_timestamp, sort_newest, ec_bucket_from_title

# --------------------------------------------------------------------
# Local helpers
# --------------------------------------------------------------------
def _fmt_short_day(pub: str | None) -> str | None:
    if not pub:
        return None
    try:
        dt = dateparser.parse(pub)
        return dt.strftime("%a, %-d %b %y")
    except Exception:
        return pub

def _render_empty_state():
    st.info("No active EC warnings at this time.")

def _render_warning_block(item: dict) -> None:
    """
    Render a single EC warning entry.
    """
    title   = item.get("title") or "(no title)"
    link    = item.get("link")
    summary = item.get("summary")
    pub     = _fmt_short_day(item.get("published"))
    is_new  = bool(item.get("is_new"))

    # Stripe for new
    header_html = title
    if is_new:
        header_html = (
            "<div style='border-left:4px solid #e40000;"
            "padding-left:10px;margin:8px 0;'>"
            f"{html.escape(title)}</div>"
        )
    else:
        header_html = f"<h4>{html.escape(title)}</h4>"

    st.markdown(header_html, unsafe_allow_html=True)

    if summary:
        st.markdown(html.escape(summary), unsafe_allow_html=False)
    if link:
        st.markdown(f"[Read more]({link})")
    if pub:
        st.caption(f"Published: {pub}")

    st.markdown("---")

# --------------------------------------------------------------------
# Public renderer
# --------------------------------------------------------------------
def render(entries: list[dict], conf: dict) -> None:
    """
    Environment Canada renderer.
    - Relies on the scraper to already filter only 'warnings' and 'watches'.
    - Groups items loosely by province + bucket (but defaults to 'Warning' if none).
    - Read-only (does not commit seen state); controller handles clear-on-close.
    """
    items = attach_timestamp(entries or [])
    if not items:
        _render_empty_state()
        return

    # Sort newest first
    items = sort_newest(items)

    filtered: list[dict] = []
    for e in items:
        bucket = ec_bucket_from_title(e.get("title", "")) or "Warning"
        code = e.get("province", "")
        prov_name = (e.get("province_name") or str(code) or "Unknown")
        d = dict(e, province_name=prov_name, bucket=bucket,
                 bkey=f"{prov_name}|{bucket}")
        filtered.append(d)

    if not filtered:
        _render_empty_state()
        return

    # Render each warning block
    for item in filtered:
        _render_warning_block(item)
