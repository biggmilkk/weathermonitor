# renderers/nws.py
import html
import time
from collections import OrderedDict
from datetime import timezone as _tz

import streamlit as st
from dateutil import parser as dateparser

# Logic helpers from computation.py (no UI)
from computation import (
    attach_timestamp,
    sort_newest,
    alphabetic_with_last,
)

# --------------------------
# Local UI helpers (no deps)
# --------------------------

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

def _fmt_utc(ts: float) -> str:
    return time.strftime("%a, %d %b %y %H:%M:%S UTC", time.gmtime(ts))

def _as_list(entries):
    if not entries:
        return []
    return entries if isinstance(entries, list) else [entries]

def _norm(s: str | None) -> str:
    return (s or "").strip()

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

def render_empty_state():
    st.info("No active warnings that meet thresholds at the moment.")

def _safe_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()

# --------------------------
# Public renderer entrypoint
# --------------------------

def render(entries, conf):
    """
    NWS (US) – grouped compact:
      State (e.g., California, Texas, Marine)
        → Event bucket (e.g., Tornado Warning)
          → list of alerts

    Maintains per-bucket last-seen keyed by "State|Bucket" in st.session_state.
    """
    feed_key = conf.get("key", "nws")

    open_key        = f"{feed_key}_active_bucket"
    pending_map_key = f"{feed_key}_bucket_pending_seen"
    lastseen_key    = f"{feed_key}_bucket_last_seen"
    rerun_guard_key = f"{feed_key}_rerun_guard"

    # clear one-shot guard if set
    if st.session_state.get(rerun_guard_key):
        st.session_state.pop(rerun_guard_key, None)

    st.session_state.setdefault(open_key, None)
    st.session_state.setdefault(pending_map_key, {})
    st.session_state.setdefault(lastseen_key, {})
    st.session_state.setdefault(f"{feed_key}_remaining_new_total", 0)

    active_bucket   = st.session_state[open_key]
    pending_seen    = st.session_state[pending_map_key]
    bucket_lastseen = st.session_state[lastseen_key]

    # Normalize & sort newest-first
    items = sort_newest(attach_timestamp(_as_list(entries)))

    normalized = []
    for e in items:
        state  = _norm(e.get("state") or e.get("state_name") or e.get("state_code") or "Unknown")
        bucket = _norm(e.get("bucket") or e.get("event") or e.get("title") or "Alert")
        if not state or not bucket:
            continue
        normalized.append(dict(e, state=state, bucket=bucket, bkey=f"{state}|{bucket}"))

    if not normalized:
        render_empty_state()
        return

    # ---------- Actions ----------
    cols_actions = st.columns([1, 6])
    with cols_actions[0]:
        if st.button("Mark all as seen", key=f"{feed_key}_mark_all_seen"):
            now_ts = time.time()
            # mark every visible state|bucket pair as seen
            for a in normalized:
                bucket_lastseen[a["bkey"]] = now_ts
            # clear any "pending opened" bucket and close the active one
            pending_seen.clear()
            st.session_state[open_key] = None
            st.session_state[lastseen_key] = bucket_lastseen
            # ensure the button badges zero instantly
            st.session_state[f"{feed_key}_remaining_new_total"] = 0
            _safe_rerun()
            return

    # Group by state
    groups = OrderedDict()
    for e in normalized:
        groups.setdefault(e["state"], []).append(e)

    # Order states alphabetically, with Marine last if present
    states = alphabetic_with_last(list(groups.keys()), last_value="Marine")

    for state in states:
        alerts = groups.get(state, [])
        if not alerts:
            continue

        def _state_has_new() -> bool:
            for a in alerts:
                last_seen = float(bucket_lastseen.get(a["bkey"], 0.0))
                if float(a.get("timestamp") or 0.0) > last_seen:
                    return True
            return False

        # State header (striped if any new)
        st.markdown(
            _stripe_wrap(f"<h2>{html.escape(state)}</h2>", _state_has_new()),
            unsafe_allow_html=True
        )

        # Bucket by event type
        buckets = OrderedDict()
        for a in alerts:
            buckets.setdefault(a["bucket"], []).append(a)

        # Render each bucket with toggle and counts
        for label, items in buckets.items():
            bkey = f"{state}|{label}"
            cols = st.columns([0.7, 0.3])

            # Toggle button
            with cols[0]:
                if st.button(label, key=f"{feed_key}:{bkey}:btn", use_container_width=True):
                    state_changed = False
                    prev = active_bucket
                    if prev and prev != bkey:
                        ts_opened_prev = float(pending_seen.pop(prev, time.time()))
                        bucket_lastseen[prev] = ts_opened_prev

                    if active_bucket == bkey:
                        ts_opened = float(pending_seen.pop(bkey, time.time()))
                        bucket_lastseen[bkey] = ts_opened
                        st.session_state[open_key] = None
                        state_changed = True
                    else:
                        st.session_state[open_key] = bkey
                        pending_seen[bkey] = time.time()
                        state_changed = True

                    if state_changed and not st.session_state.get(rerun_guard_key, False):
                        st.session_state[rerun_guard_key] = True
                        _safe_rerun()
                        return

            # NEW count for this bucket (committed last_seen)
            last_seen = float(bucket_lastseen.get(bkey, 0.0))
            new_count = sum(1 for x in items if float(x.get("timestamp") or 0.0) > last_seen)

            # Badges (Active + New)
            with cols[1]:
                active_count = len(items)
                badges_html = (
                    "<span style='margin-left:6px;padding:2px 6px;"
                    "border-radius:4px;background:#eef0f3;color:#000;font-size:0.9em;"
                    "font-weight:600;display:inline-block;'>"
                    f"{active_count} Active</span>"
                )
                if new_count > 0:
                    badges_html += (
                        "<span style='margin-left:8px;padding:2px 6px;"
                        "border-radius:4px;background:#ffeecc;color:#000;font-size:0.9em;"
                        "font-weight:bold;display:inline-block;'>"
                        f"❗ {new_count} New</span>"
                    )
                st.markdown(badges_html, unsafe_allow_html=True)

            # List items if this bucket is open
            if st.session_state.get(open_key) == bkey:
                for a in items:
                    is_new = float(a.get("timestamp") or 0.0) > last_seen
                    prefix = "[NEW] " if is_new else ""
                    title  = _norm(a.get("title", "")) or "(no title)"
                    region = _norm(a.get("region", ""))
                    link   = _norm(a.get("link"))

                    if title and link:
                        st.markdown(f"{prefix}**[{title}]({link})**")
                    else:
                        st.markdown(f"{prefix}**{title}**")

                    if region:
                        st.caption(f"Region: {region}")

                    pub_label = _to_utc_label(a.get("published"))
                    if pub_label:
                        st.caption(f"Published: {pub_label}")

                    st.markdown("---")

        st.markdown("---")
