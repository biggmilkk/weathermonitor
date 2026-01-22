# renderers/ec.py
import html
import time
from collections import OrderedDict

import streamlit as st
from dateutil import parser as dateparser

from computation import (
    attach_timestamp,
    sort_newest,
    ec_bucket_from_title,
)

# ============================================================
# Helpers
# ============================================================

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

def _norm(s: str | None) -> str:
    return (s or "").strip()

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

def render_empty_state():
    st.info("No active warnings that meet thresholds at the moment.")

def _entry_title(e: dict) -> str:
    """
    EC feeds occasionally change naming: some emit 'title', others 'headline'.
    Use a safe fallback chain so bucketing + display never breaks.
    """
    return _norm(
        e.get("title")
        or e.get("headline")
        or e.get("name")
        or e.get("summary")
    )

def _entry_province(e: dict) -> str:
    """
    Normalize province field with fallbacks.
    """
    return _norm(
        e.get("province_name")
        or e.get("province")
        or e.get("region")
    ) or "Unknown"

# ============================================================
# Province ordering
# ============================================================

_PROVINCE_ORDER = [
    "Alberta", "British Columbia", "Manitoba", "New Brunswick",
    "Newfoundland and Labrador", "Northwest Territories", "Nova Scotia",
    "Nunavut", "Ontario", "Prince Edward Island", "Quebec",
    "Saskatchewan", "Yukon",
]

# ============================================================
# EC Grouped Compact Renderer
# ============================================================

def render(entries, conf):
    """
    Grouped compact renderer for Environment Canada:
      Province → Warning bucket → list of alerts
    """
    feed_key = conf.get("key", "ec")

    open_key        = f"{feed_key}_active_bucket"
    pending_map_key = f"{feed_key}_bucket_pending_seen"
    lastseen_key    = f"{feed_key}_bucket_last_seen"
    rerun_guard_key = f"{feed_key}_rerun_guard"

    # clear one-shot guard (prevents double rerun loops)
    if st.session_state.get(rerun_guard_key):
        st.session_state.pop(rerun_guard_key, None)

    st.session_state.setdefault(open_key, None)
    st.session_state.setdefault(pending_map_key, {})
    st.session_state.setdefault(lastseen_key, {})
    st.session_state.setdefault(f"{feed_key}_remaining_new_total", 0)

    active_bucket   = st.session_state[open_key]
    pending_seen    = st.session_state[pending_map_key]
    bucket_lastseen = st.session_state[lastseen_key]

    # normalize + sort newest-first
    items = sort_newest(attach_timestamp(entries or []))

    # precompute normalized alerts with province + bucket keys
    filtered = []
    for e in items:
        title_txt = _entry_title(e)
        bucket = ec_bucket_from_title(title_txt)
        if not bucket:
            continue

        prov_name = _entry_province(e)
        d = dict(e, bucket=bucket, province_name=prov_name, bkey=f"{prov_name}|{bucket}")
        filtered.append(d)

    if not filtered:
        render_empty_state()
        return

    # ---------- Actions ----------
    cols_actions = st.columns([1, 6])
    with cols_actions[0]:
        if st.button("Mark all as seen", key=f"{feed_key}_mark_all_seen"):
            now_ts = time.time()
            for a in filtered:
                bucket_lastseen[a["bkey"]] = now_ts
            pending_seen.clear()
            st.session_state[open_key] = None
            st.session_state[lastseen_key] = bucket_lastseen
            # ensure the button badges zero instantly
            st.session_state[f"{feed_key}_remaining_new_total"] = 0
            _safe_rerun()
            return

    # group by province; order by canonical list first, then any extras
    groups: OrderedDict[str, list[dict]] = OrderedDict()
    for e in filtered:
        groups.setdefault(e["province_name"], []).append(e)

    provinces = [p for p in _PROVINCE_ORDER if p in groups] + [
        p for p in groups if p not in _PROVINCE_ORDER
    ]

    # ---------- Provinces ----------
    for prov in provinces:
        alerts = groups.get(prov, []) or []
        if not alerts:
            continue

        def _prov_has_new() -> bool:
            for a in alerts:
                last_seen = float(bucket_lastseen.get(a["bkey"], 0.0))
                if float(a.get("timestamp") or 0.0) > last_seen:
                    return True
            return False

        st.markdown(
            _stripe_wrap(f"<h2>{html.escape(prov)}</h2>", _prov_has_new()),
            unsafe_allow_html=True
        )

        # group by warning bucket inside the province
        buckets: OrderedDict[str, list[dict]] = OrderedDict()
        for a in alerts:
            buckets.setdefault(a["bucket"], []).append(a)

        # ---------- Buckets ----------
        for label, bucket_items in buckets.items():
            bkey = f"{prov}|{label}"
            cols = st.columns([0.7, 0.3])

            # bucket toggle + pending/seen bookkeeping
            with cols[0]:
                if st.button(label, key=f"{feed_key}:{bkey}:btn", use_container_width=True):
                    state_changed = False
                    prev = active_bucket

                    # if switching buckets, commit the previously open bucket as seen
                    if prev and prev != bkey:
                        ts_opened_prev = float(pending_seen.pop(prev, time.time()))
                        bucket_lastseen[prev] = ts_opened_prev

                    if active_bucket == bkey:
                        # closing the same bucket -> commit as seen
                        ts_opened = float(pending_seen.pop(bkey, time.time()))
                        bucket_lastseen[bkey] = ts_opened
                        st.session_state[open_key] = None
                        active_bucket = None
                        state_changed = True
                    else:
                        # opening a new bucket -> start pending timer
                        st.session_state[open_key] = bkey
                        pending_seen[bkey] = time.time()
                        active_bucket = bkey
                        state_changed = True

                    if state_changed and not st.session_state.get(rerun_guard_key, False):
                        st.session_state[rerun_guard_key] = True
                        _safe_rerun()
                        return

            # counts (active + new since last_seen)
            last_seen = float(bucket_lastseen.get(bkey, 0.0))
            new_count = sum(1 for x in bucket_items if float(x.get("timestamp") or 0.0) > last_seen)

            with cols[1]:
                active_count = len(bucket_items)
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

            # expanded bucket content
            if st.session_state.get(open_key) == bkey:
                for a in bucket_items:
                    is_new = float(a.get("timestamp") or 0.0) > last_seen
                    prefix = "[NEW] " if is_new else ""
                    title  = _entry_title(a) or "(no title)"

                    st.markdown(_stripe_wrap(f"{prefix}<strong>{html.escape(title)}</strong>", is_new), unsafe_allow_html=True)

                    summary = _norm(a.get("summary") or a.get("description") or a.get("body"))
                    if summary:
                        st.markdown(html.escape(summary).replace("\n", "  \n"))

                    pub_label = _to_utc_label(a.get("published"))
                    if pub_label:
                        st.caption(f"Published: {pub_label}")

                    link = _norm(a.get("link"))
                    if link:
                        st.markdown(f"[Read more]({link})")

                    st.markdown("---")

        st.markdown("---")
