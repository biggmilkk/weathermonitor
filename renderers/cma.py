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
# CMA bucket ordering (severity)
# ============================================================

_SEVERITY_ORDER = ["Red", "Orange", "Yellow", "Blue"]

# Bullet colors (matches your old CMA renderer)
CMA_COLORS = {
    "Yellow": "#FFD400",
    "Orange": "#FF7F00",
    "Red":    "#E60026",
    "Blue":   "#1E90FF",
}

def _cma_bucket_from_level(level: str | None) -> str | None:
    lvl = _norm(level)
    return lvl if lvl in ("Red", "Orange", "Yellow", "Blue") else None

def _remaining_new_total(entries, bucket_lastseen) -> int:
    """
    Count unseen items using province|bucket keys (province|severity).
    Mirrors computation.ec_remaining_new_total pattern but for CMA. :contentReference[oaicite:5]{index=5}
    """
    total = 0
    for e in entries or []:
        prov = _norm(e.get("province_name") or e.get("province") or e.get("region") or "Unknown") or "Unknown"
        bucket = _cma_bucket_from_level(e.get("bucket") or e.get("level"))
        if not bucket:
            continue
        bkey = f"{prov}|{bucket}"
        last_seen = float(bucket_lastseen.get(bkey, 0.0))
        if entry_ts(e) > last_seen:
            total += 1
    return total

# ============================================================
# CMA EC-style Grouped Compact Renderer
# ============================================================

def render(entries, conf):
    """
    EC-style grouped renderer for CMA:
      Province → Severity bucket (Red/Orange/Yellow/Blue) → list of alerts

    Clear-on-close behavior:
      - Opening a bucket starts pending timer
      - Closing commits as seen for that province|severity bucket
    """
    feed_key = conf.get("key", "cma")

    open_key        = f"{feed_key}_active_bucket"
    pending_map_key = f"{feed_key}_bucket_pending_seen"
    lastseen_key    = f"{feed_key}_bucket_last_seen"
    rerun_guard_key = f"{feed_key}_rerun_guard"

    # clear one-shot guard
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

    # normalize alerts with province + severity bucket keys
    filtered = []
    for e in items:
        bucket = _cma_bucket_from_level(e.get("level"))
        if not bucket:
            continue
        prov_name = _norm(e.get("region") or e.get("province_name") or e.get("province")) or "全国"
        d = dict(e, bucket=bucket, province_name=prov_name, bkey=f"{prov_name}|{bucket}")
        filtered.append(d)

    if not filtered:
        st.session_state[f"{feed_key}_remaining_new_total"] = 0
        render_empty_state()
        return

    # Update the feed-level "remaining new" total for your country button
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

    # group by province; keep 全国 last
    groups: OrderedDict[str, list[dict]] = OrderedDict()
    for e in filtered:
        groups.setdefault(e["province_name"], []).append(e)

    provinces = alphabetic_with_last(groups.keys(), last_value="全国")

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

        st.markdown(_stripe_wrap(f"<h2>{html.escape(prov)}</h2>", _prov_has_new()), unsafe_allow_html=True)

        # group by severity bucket inside province, order by severity
        buckets: OrderedDict[str, list[dict]] = OrderedDict()
        for a in alerts:
            buckets.setdefault(a["bucket"], []).append(a)

        bucket_labels = [b for b in _SEVERITY_ORDER if b in buckets] + [b for b in buckets if b not in _SEVERITY_ORDER]

        # ---------- Buckets ----------
        for label in bucket_labels:
            items_in_bucket = buckets.get(label, []) or []
            if not items_in_bucket:
                continue

            bkey = f"{prov}|{label}"
            cols = st.columns([0.7, 0.3])

            # bucket toggle + pending/seen bookkeeping
            with cols[0]:
                # show colored bullet next to bucket label
                color = CMA_COLORS.get(label, "#888")
                btn_label = f"● {label}"
                if st.button(btn_label, key=f"{feed_key}:{bkey}:btn", use_container_width=True):
                    state_changed = False
                    prev = active_bucket

                    # if switching buckets, commit previously open bucket
                    if prev and prev != bkey:
                        ts_opened_prev = float(pending_seen.pop(prev, time.time()))
                        bucket_lastseen[prev] = ts_opened_prev

                    if active_bucket == bkey:
                        # closing -> commit as seen
                        ts_opened = float(pending_seen.pop(bkey, time.time()))
                        bucket_lastseen[bkey] = ts_opened
                        st.session_state[open_key] = None
                        state_changed = True
                    else:
                        # opening -> start pending timer
                        st.session_state[open_key] = bkey
                        pending_seen[bkey] = time.time()
                        state_changed = True

                    if state_changed and not st.session_state.get(rerun_guard_key, False):
                        st.session_state[rerun_guard_key] = True
                        # update totals immediately on UI action
                        st.session_state[f"{feed_key}_remaining_new_total"] = _remaining_new_total(filtered, bucket_lastseen)
                        _safe_rerun()
                        return

            # counts (active + new since last_seen)
            last_seen = float(bucket_lastseen.get(bkey, 0.0))
            new_count = sum(1 for x in items_in_bucket if float(x.get("timestamp") or 0.0) > last_seen)

            with cols[1]:
                active_count = len(items_in_bucket)
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

            # expanded bucket content (newest first)
            if st.session_state.get(open_key) == bkey:
                expanded = sort_newest(attach_timestamp(items_in_bucket))
                for a in expanded:
                    is_new = float(a.get("timestamp") or 0.0) > last_seen
                    prefix = "[NEW] " if is_new else ""

                    title = _norm(a.get("headline") or a.get("title") or "") or "(no title)"
                    desc  = _norm(a.get("summary") or a.get("description") or a.get("body") or "")

                    # alert title line with bullet color by severity
                    color = CMA_COLORS.get(label, "#888")
                    title_html = (
                        f"{prefix}<span style='color:{color};font-size:16px;'>&#9679;</span> "
                        f"<strong>{html.escape(title)}</strong>"
                    )
                    st.markdown(_stripe_wrap(title_html, is_new), unsafe_allow_html=True)

                    if desc:
                        st.markdown(html.escape(desc).replace("\n", "  \n"))

                    pub_label = _to_utc_label(a.get("published"))
                    if pub_label:
                        st.caption(f"Published: {pub_label}")

                    st.markdown("---")

        st.markdown("---")
