import html
import os
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


def _region(e: dict) -> str:
    return _norm(e.get("region") or "New Zealand") or "New Zealand"


def _areas_text(e: dict) -> str:
    areas = e.get("areas") or []
    if isinstance(areas, list):
        cleaned = [_norm(x) for x in areas if _norm(x)]
        if cleaned:
            return ", ".join(cleaned)

    region = _region(e)
    return "" if region == "New Zealand" else region


def _event(e: dict) -> str:
    return _norm(e.get("event")) or "Alert"


def _severity(e: dict) -> str:
    return _norm(e.get("severity"))


def _urgency(e: dict) -> str:
    return _norm(e.get("urgency"))


def _certainty(e: dict) -> str:
    return _norm(e.get("certainty"))


def _status(e: dict) -> str:
    return _norm(e.get("status"))


def _msg_type(e: dict) -> str:
    return _norm(e.get("msg_type"))


def _instruction(e: dict) -> str:
    return _norm(e.get("instruction"))


def _description(e: dict) -> str:
    return _norm(e.get("description") or e.get("summary"))


def _bucket_label(e: dict) -> str | None:
    severity = _severity(e)
    event = _event(e)
    if not severity:
        return None
    return f"{severity} - {event}"


def _bullet_color(severity: str) -> str:
    s = _norm(severity).lower()
    return {
        "extreme": "#E60026",
        "severe": "#FF7F00",
        "moderate": "#D4AA00",
        "minor": "#2E8B57",
        "unknown": "#888888",
    }.get(s, "#888")


def _severity_sort_rank(severity: str) -> int:
    return {
        "extreme": 0,
        "severe": 1,
        "moderate": 2,
        "minor": 3,
        "unknown": 4,
    }.get(_norm(severity).lower(), 5)


# ============================================================
# Translation
# ============================================================

@st.cache_data(ttl=7 * 24 * 3600, show_spinner=False)
def _translate_to_en_deepl(text: str) -> str | None:
    t = (text or "").strip()
    if not t:
        return None

    api_key = None
    try:
        api_key = st.secrets.get("DEEPL_API_KEY")
    except Exception:
        api_key = None
    if not api_key:
        api_key = os.getenv("DEEPL_API_KEY")

    if not api_key:
        return None

    try:
        import deepl
        translator = deepl.Translator(api_key)
        result = translator.translate_text(t, target_lang="EN-US")
        out = (result.text or "").strip()
        if not out or out == t:
            return None
        return out
    except Exception:
        return None


def _maybe_translate(text: str, *, enabled: bool) -> str | None:
    if not enabled:
        return None
    return _translate_to_en_deepl(text)


# ============================================================
# Remaining NEW total
# ============================================================

def _remaining_new_total(entries, bucket_lastseen) -> int:
    total = 0
    for e in entries or []:
        region = _region(e)
        bucket = e.get("bucket_key")
        if not bucket:
            continue
        bkey = f"{region}|{bucket}"
        if entry_ts(e) > float(bucket_lastseen.get(bkey, 0.0)):
            total += 1
    return total


# ============================================================
# Region ordering
# ============================================================

_LAST_REGION = "New Zealand"


# ============================================================
# MetService NZ Renderer
# ============================================================

def render(entries, conf):
    """
    MetService New Zealand renderer:
      Region -> Specific sub-bucket -> alerts

    Example bucket labels:
      - Severe - Heavy Rain
      - Extreme - Wind
    """
    feed_key = conf.get("key", "metservice_nz")
    translate_enabled = bool(
        (conf.get("conf") or {}).get("translate_to_en")
        or conf.get("translate_to_en")
    )

    open_key = f"{feed_key}_active_bucket"
    pending_map_key = f"{feed_key}_bucket_pending_seen"
    lastseen_key = f"{feed_key}_bucket_last_seen"
    rerun_guard_key = f"{feed_key}_rerun_guard"

    if st.session_state.get(rerun_guard_key):
        st.session_state.pop(rerun_guard_key, None)

    st.session_state.setdefault(open_key, None)
    st.session_state.setdefault(pending_map_key, {})
    st.session_state.setdefault(lastseen_key, {})
    st.session_state.setdefault(f"{feed_key}_remaining_new_total", 0)

    active_bucket = st.session_state[open_key]
    pending_seen = st.session_state[pending_map_key]
    bucket_lastseen = st.session_state[lastseen_key]

    items = sort_newest(attach_timestamp(entries or []))

    filtered = []
    for e in items:
        bucket_label = _bucket_label(e)
        if not bucket_label:
            continue

        region = _region(e)
        filtered.append(dict(
            e,
            bucket_key=bucket_label,
            bucket_label=bucket_label,
            region_name=region,
            bkey=f"{region}|{bucket_label}",
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

    # Group by region
    groups: OrderedDict[str, list[dict]] = OrderedDict()
    for e in filtered:
        groups.setdefault(e["region_name"], []).append(e)

    regions = alphabetic_with_last(groups.keys(), last_value=_LAST_REGION)

    # ---------- Regions ----------
    for region in regions:
        alerts = groups.get(region, [])
        if not alerts:
            continue

        def _region_has_new() -> bool:
            return any(
                entry_ts(a) > float(bucket_lastseen.get(a["bkey"], 0.0))
                for a in alerts
            )

        st.markdown(
            _stripe_wrap(f"<h2>{html.escape(region)}</h2>", _region_has_new()),
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

        def _bucket_sort_key(items_in_bucket: list[dict], label: str):
            first = items_in_bucket[0] if items_in_bucket else {}
            sev_rank = _severity_sort_rank(_severity(first))
            return (sev_rank, _norm(label).lower())

        ordered_bucket_keys = sorted(
            buckets.keys(),
            key=lambda bk: _bucket_sort_key(buckets[bk]["items"], buckets[bk]["label"])
        )

        # ---------- Buckets ----------
        for bucket_key in ordered_bucket_keys:
            label = buckets[bucket_key]["label"]
            items_in_bucket = buckets[bucket_key]["items"]
            bkey = f"{region}|{bucket_key}"

            cols = st.columns([0.7, 0.3])

            with cols[0]:
                if st.button(label, key=f"{feed_key}:{bkey}:btn", use_container_width=True):
                    prev = active_bucket

                    if prev and prev != bkey:
                        bucket_lastseen[prev] = float(pending_seen.pop(prev, time.time()))

                    if active_bucket == bkey:
                        bucket_lastseen[bkey] = float(pending_seen.pop(bkey, time.time()))
                        st.session_state[open_key] = None
                    else:
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

            if st.session_state.get(open_key) == bkey:
                for a in sort_newest(attach_timestamp(items_in_bucket)):
                    is_new = entry_ts(a) > last_seen
                    prefix = "[NEW] " if is_new else ""

                    headline = _headline(a) or "(untitled)"
                    severity = _severity(a)
                    bullet_color = _bullet_color(severity)
                    areas_text = _areas_text(a)
                    event = _event(a)
                    urgency = _urgency(a)
                    certainty = _certainty(a)
                    status = _status(a)
                    msg_type = _msg_type(a)

                    title_html = (
                        f"{prefix}"
                        f"<span style='color:{bullet_color};font-size:16px;'>&#9679;</span> "
                        f"<strong>{html.escape(headline)}</strong>"
                    )
                    st.markdown(_stripe_wrap(title_html, is_new), unsafe_allow_html=True)

                    headline_en = _maybe_translate(headline, enabled=translate_enabled)
                    if headline_en:
                        st.markdown(f"*English (auto):* {html.escape(headline_en)}")

                    if areas_text:
                        st.markdown(f"**Affected areas:** {html.escape(areas_text)}")

                    if event:
                        st.markdown(f"**Type:** {html.escape(event)}")

                    if severity:
                        st.markdown(f"**Severity:** {html.escape(severity)}")

                    if urgency:
                        st.markdown(f"**Urgency:** {html.escape(urgency)}")

                    if certainty:
                        st.markdown(f"**Certainty:** {html.escape(certainty)}")

                    if status:
                        st.markdown(f"**Status:** {html.escape(status)}")

                    if msg_type:
                        st.markdown(f"**Message Type:** {html.escape(msg_type)}")

                    desc = _description(a)
                    if desc:
                        st.markdown(html.escape(desc).replace("\n", "  \n"))

                        desc_en = _maybe_translate(desc, enabled=translate_enabled)
                        if desc_en:
                            st.markdown(f"*English (auto):* {html.escape(desc_en)}")

                    instruction = _instruction(a)
                    if instruction:
                        st.markdown(f"**Instruction:** {html.escape(instruction)}")

                        instruction_en = _maybe_translate(instruction, enabled=translate_enabled)
                        if instruction_en:
                            st.markdown(f"*English (auto):* {html.escape(instruction_en)}")

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
