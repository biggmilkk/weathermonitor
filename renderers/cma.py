# renderers/cma.py
import html
import time
from collections import OrderedDict

import streamlit as st
from dateutil import parser as dateparser

from computation import (
    attach_timestamp,
    sort_newest,
    alphabetic_with_last,
    entry_ts,
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

# ============================================================
# CMA bucket labels — RED/ORANGE ONLY
# ============================================================

LEVEL_TO_BUCKET_LABEL = {
    "Red":    "Red Warning",
    "Orange": "Orange Warning",
    # Intentionally omit Yellow/Blue
}

_BUCKET_ORDER = [
    "Red Warning",
    "Orange Warning",
]

# Bullet colors for per-alert titles
LEVEL_TO_BULLET_COLOR = {
    "Red": "#E60026",
    "Orange": "#FF7F00",
}

def _cma_bucket_from_level(level: str | None) -> str | None:
    return LEVEL_TO_BUCKET_LABEL.get(_norm(level))

def _remaining_new_total(entries, bucket_lastseen) -> int:
    total = 0
    for e in entries or []:
        prov = _norm(e.get("province_name") or e.get("province") or e.get("region")) or "全国"
        bucket = e.get("bucket")
        if not bucket:
            continue
        bkey = f"{prov}|{bucket}"
        if entry_ts(e) > float(bucket_lastseen.get(bkey, 0.0)):
            total += 1
    return total

# ============================================================
# CMA Grouped Renderer
# ============================================================

def render(entries, conf):
    """
    CMA renderer (Environment Canada style):
      Province → (Red Warning / Orange Warning) → alerts

    Notes:
    - Uses headline for display.
    - Adds colored bullet before each alert title (not in bucket label).
    - Extra guard: will not render Yellow/Blue buckets.
    """
    feed_key = conf.get("key", "cma")

    open_key        = f"{feed_key}_active_bucket"
    pending_map_key = f"{feed_key}_bucket_pending_seen"
    lastseen_key    = f"{feed_key}_bucket_last_seen"
    rerun_guard_key = f"{feed_key}_rerun_guard"

    if st.session_state.get(rerun_guard_key):
        st.session_state.pop(rerun_guard_key, None)

    st.session_state.setdefault(open_key, None)
    st.session_state.setdefault(pending_map_key, {})
    st.session_state.setdefault(lastseen_key, {})
    st.session_state.setdefault(f"{feed_key}_remaining_new_total", 0)

    active_bucket   = st.session_state[open_key]
    pending_seen    = st.session_state[pending_map_key]
    bucket_lastseen = st.session_state[lastseen_key]

    items = sort_newest(attach_timestamp(entries or []))

    filtered = []
    for e in items:
        # Only Red/Orange buckets
        bucket = _cma_bucket_from_level(e.get("level"))
        if not bucket:
            continue

        prov = _norm(e.get("region") or e.get("province_name") or e.get("province")) or "全国"
        filtered.append(dict(
            e,
            bucket=bucket,
            province_name=prov,
            bkey=f"{prov}|{bucket}",
        ))

    if not filtered:
        st.session_state[f"{feed_key}_remaining_new_total"] = 0
        render_empty_state()
        return

    # Update country-level NEW badge
    st.session_state[f"{feed_key}_remaining_new_total"] = _remaining_new_total(filtered, bucket_lastseen)

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
            st.session_state[f"{feed_key}_remaining_new_total"] = 0
            _safe_rerun()
            return

    # Group by province
    groups: OrderedDict[str, list[dict]] = OrderedDict()
    for e in filtered:
        groups.setdefault(e["province_name"], []).append(e)

    provinces = alphabetic_with_last(groups.keys(), last_value="全国")

    # ---------- Provinces ----------
    for prov in provinces:
        alerts = groups.get(prov, [])
        if not alerts:
            continue

        def _prov_has_new() -> bool:
            return any(
                entry_ts(a) > float(bucket_lastseen.get(a["bkey"], 0.0))
                for a in alerts
            )

        st.markdown(
            _stripe_wrap(f"<h2>{html.escape(prov)}</h2>", _prov_has_new()),
            unsafe_allow_html=True,
        )

        # Group by bucket (severity)
        buckets: OrderedDict[str, list[dict]] = OrderedDict()
        for a in alerts:
            buckets.setdefault(a["bucket"], []).append(a)

        ordered_labels = [b for b in _BUCKET_ORDER if b in buckets]

        # ---------- Buckets ----------
        for label in ordered_labels:
            items_in_bucket = buckets[label]
            bkey = f"{prov}|{label}"

            cols = st.columns([0.7, 0.3])

            # Toggle button (NO bullet)
            with cols[0]:
                if st.button(label, key=f"{feed_key}:{bkey}:btn", use_container_width=True):
                    prev = active_bucket

                    # commit previous bucket if switching
                    if prev and prev != bkey:
                        bucket_lastseen[prev] = float(pending_seen.pop(prev, time.time()))

                    if active_bucket == bkey:
                        # closing: commit this bucket as seen at open time
                        bucket_lastseen[bkey] = float(pending_seen.pop(bkey, time.time()))
                        st.session_state[open_key] = None
                    else:
                        # opening: start pending timer
                        st.session_state[open_key] = bkey
                        pending_seen[bkey] = time.time()

                    if not st.session_state.get(rerun_guard_key):
                        st.session_state[rerun_guard_key] = True
                        st.session_state[f"{feed_key}_remaining_new_total"] = _remaining_new_total(filtered, bucket_lastseen)
                        _safe_rerun()
                        return

            last_seen = float(bucket_lastseen.get(bkey, 0.0))
            new_count = sum(1 for x in items_in_bucket if entry_ts(x) > last_seen)

            with cols[1]:
                badges = (
                    "<span style='margin-left:6px;padding:2px 6px;"
                    "border-radius:4px;background:#eef0f3;color:#000;font-size:0.9em;"
                    "font-weight:600;display:inline-block;'>"
                    f"{len(items_in_bucket)} Active</span>"
                )
                if new_count > 0:
                    badges += (
                        "<span style='margin-left:8px;padding:2px 6px;"
                        "border-radius:4px;background:#FFEB99;color:#000;font-size:0.9em;"
                        "font-weight:bold;display:inline-block;'>"
                        f"❗ {new_count} New</span>"
                    )
                st.markdown(badges, unsafe_allow_html=True)

            # Expanded bucket content
            if st.session_state.get(open_key) == bkey:
                # newest first within the open bucket
                for a in sort_newest(attach_timestamp(items_in_bucket)):
                    is_new = entry_ts(a) > last_seen
                    prefix = "[NEW] " if is_new else ""

                    # USE HEADLINE (fallback to title)
                    headline = _norm(a.get("headline") or a.get("title")) or "(no title)"

                    # Colored bullet based on level
                    lvl = _norm(a.get("level"))
                    bullet_color = LEVEL_TO_BULLET_COLOR.get(lvl, "#888")

                    title_html = (
                        f"{prefix}"
                        f"<span style='color:{bullet_color};font-size:16px;'>&#9679;</span> "
                        f"<strong>{html.escape(headline)}</strong>"
                    )
                    st.markdown(_stripe_wrap(title_html, is_new), unsafe_allow_html=True)

                    desc = _norm(a.get("summary") or a.get("description") or a.get("body"))
                    if desc:
                        st.markdown(html.escape(desc).replace("\n", "  \n"))

                    pub_label = _to_utc_label(a.get("published"))
                    if pub_label:
                        st.caption(f"Published: {pub_label}")

                    st.markdown("---")

        st.markdown("---")
