# renderers/bmkg.py
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

def _headline(e: dict) -> str:
    return _norm(
        e.get("headline")
        or e.get("title")
        or e.get("name")
        or e.get("event")
    )

def _province(e: dict) -> str:
    return _norm(
        e.get("province_name")
        or e.get("province")
        or "Indonesia"
    ) or "Indonesia"

def _location(e: dict) -> str:
    return _norm(
        e.get("region")
        or e.get("area")
        or (e.get("areas") or [None])[0]
        or _province(e)
    )

def _event(e: dict) -> str:
    return _norm(e.get("event")) or "Weather"

def _level(e: dict) -> str:
    return _norm(e.get("level"))

def _bullet_color(level: str) -> str:
    return {
        "Red": "#E60026",
        "Orange": "#FF7F00",
        "Yellow": "#D4AA00",
        "Blue": "#3B82F6",
    }.get(level, "#888")

def _bucket_label(e: dict) -> str | None:
    """
    Specific BMKG sub-bucket label for display and grouping.

    Examples:
      - Orange Warning - Thunderstorm
      - Yellow Warning - Heavy Rain
      - Red Warning - Extreme Weather
    """
    level = _level(e)
    event = _event(e)
    if not level:
        return None
    return f"{level} Warning - {event}"

def _remaining_new_total(entries, bucket_lastseen) -> int:
    """
    Remaining NEW across all BMKG entries using province|bucket_key.
    """
    total = 0
    for e in entries or []:
        prov = _province(e)
        bucket = e.get("bucket_key")
        if not bucket:
            continue
        bkey = f"{prov}|{bucket}"
        if entry_ts(e) > float(bucket_lastseen.get(bkey, 0.0)):
            total += 1
    return total

# ============================================================
# BMKG Province ordering
# ============================================================

# Keep "Indonesia" / nationwide-style rollups last if they appear
_LAST_PROVINCE = "Indonesia"

# ============================================================
# BMKG Grouped Renderer
# ============================================================

def render(entries, conf):
    """
    BMKG renderer:
      Province -> Specific sub-bucket -> alerts

    Example bucket labels:
      - Orange Warning - Thunderstorm
      - Yellow Warning - Heavy Rain
      - Red Warning - Extreme Weather

    Behavior:
      - opening a bucket starts a pending-seen timer
      - closing the bucket commits it as seen
      - switching buckets commits the previously open bucket as seen
    """
    feed_key = conf.get("key", "bmkg")

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

    # Normalize and precompute bucket keys
    filtered = []
    for e in items:
        bucket_label = _bucket_label(e)
        if not bucket_label:
            continue

        prov = _province(e)
        filtered.append(dict(
            e,
            bucket_key=bucket_label,
            bucket_label=bucket_label,
            province_name=prov,
            bkey=f"{prov}|{bucket_label}",
        ))

    if not filtered:
        st.session_state[f"{feed_key}_remaining_new_total"] = 0
        render_empty_state()
        return

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
            st.session_state[pending_map_key] = pending_seen
            st.session_state[f"{feed_key}_remaining_new_total"] = 0
            _safe_rerun()
            return

    # Group by province
    groups: OrderedDict[str, list[dict]] = OrderedDict()
    for e in filtered:
        groups.setdefault(e["province_name"], []).append(e)

    provinces = alphabetic_with_last(groups.keys(), last_value=_LAST_PROVINCE)

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

        # Group by bucket
        buckets: OrderedDict[str, dict] = OrderedDict()
        for a in alerts:
            bk = a["bucket_key"]
            if bk not in buckets:
                buckets[bk] = {
                    "label": a["bucket_label"],
                    "items": [],
                }
            buckets[bk]["items"].append(a)

        def _bucket_sort_key(label: str):
            ll = _norm(label).lower()
            if ll.startswith("red"):
                sev_rank = 0
            elif ll.startswith("orange"):
                sev_rank = 1
            elif ll.startswith("yellow"):
                sev_rank = 2
            elif ll.startswith("blue"):
                sev_rank = 3
            else:
                sev_rank = 4
            return (sev_rank, ll)

        ordered_bucket_keys = sorted(
            buckets.keys(),
            key=lambda bk: _bucket_sort_key(buckets[bk]["label"])
        )

        # ---------- Buckets ----------
        for bucket_key in ordered_bucket_keys:
            label = buckets[bucket_key]["label"]
            items_in_bucket = buckets[bucket_key]["items"]
            bkey = f"{prov}|{bucket_key}"

            cols = st.columns([0.7, 0.3])

            with cols[0]:
                if st.button(label, key=f"{feed_key}:{bkey}:btn", use_container_width=True):
                    prev = active_bucket

                    # Commit previous bucket if switching
                    if prev and prev != bkey:
                        bucket_lastseen[prev] = float(pending_seen.pop(prev, time.time()))

                    if active_bucket == bkey:
                        # Closing: commit this bucket as seen at open time
                        bucket_lastseen[bkey] = float(pending_seen.pop(bkey, time.time()))
                        st.session_state[open_key] = None
                    else:
                        # Opening: start pending timer
                        st.session_state[open_key] = bkey
                        pending_seen[bkey] = time.time()

                    if not st.session_state.get(rerun_guard_key):
                        st.session_state[rerun_guard_key] = True
                        st.session_state[lastseen_key] = bucket_lastseen
                        st.session_state[pending_map_key] = pending_seen
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
                for a in sort_newest(attach_timestamp(items_in_bucket)):
                    is_new = entry_ts(a) > last_seen
                    prefix = "[NEW] " if is_new else ""

                    headline = _headline(a) or "(no title)"
                    level = _level(a)
                    bullet_color = _bullet_color(level)
                    location = _location(a)
                    event = _event(a)

                    title_html = (
                        f"{prefix}"
                        f"<span style='color:{bullet_color};font-size:16px;'>&#9679;</span> "
                        f"<strong>{html.escape(headline)}</strong>"
                    )
                    st.markdown(_stripe_wrap(title_html, is_new), unsafe_allow_html=True)

                    if location:
                        st.markdown(f"**Location:** {html.escape(location)}")

                    if event:
                        st.markdown(f"**Type:** {html.escape(event)}")

                    summary = _norm(a.get("summary") or a.get("description"))
                    if summary:
                        st.markdown(html.escape(summary).replace("\n", "  \n"))

                    instruction = _norm(a.get("instruction"))
                    if instruction:
                        st.markdown(f"**Instruction:** {html.escape(instruction)}")

                    effective = _to_utc_label(a.get("effective"))
                    expires = _to_utc_label(a.get("expires"))
                    if effective:
                        st.caption(f"Effective: {effective}")
                    if expires:
                        st.caption(f"Expires: {expires}")

                    link = _norm(a.get("link"))
                    if link:
                        st.markdown(f"[Read more]({link})")

                    pub_label = _to_utc_label(a.get("published"))
                    if pub_label:
                        st.caption(f"Published: {pub_label}")

                    st.markdown("---")

        st.markdown("---")
