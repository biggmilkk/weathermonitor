# renderers/cma.py
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
            # Convert to UTC for display
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
# Province name mapping (CN → ISO-style EN)
# ============================================================
# Notes:
# - For autonomous regions, use "X Autonomous Region" (and include ethnic group where standard/common).
# - For municipalities, use "Beijing Municipality", etc.
# - For SARs, use "Hong Kong SAR", "Macao SAR".
# - "Nationwide" for 全国.

PROVINCE_CN_TO_ISO_EN = {
    "北京": "Beijing Municipality",
    "天津": "Tianjin Municipality",
    "河北": "Hebei Province",
    "山西": "Shanxi Province",
    "内蒙古": "Inner Mongolia Autonomous Region",
    "辽宁": "Liaoning Province",
    "吉林": "Jilin Province",
    "黑龙江": "Heilongjiang Province",
    "上海": "Shanghai Municipality",
    "江苏": "Jiangsu Province",
    "浙江": "Zhejiang Province",
    "安徽": "Anhui Province",
    "福建": "Fujian Province",
    "江西": "Jiangxi Province",
    "山东": "Shandong Province",
    "河南": "Henan Province",
    "湖北": "Hubei Province",
    "湖南": "Hunan Province",
    "广东": "Guangdong Province",
    "广西": "Guangxi Zhuang Autonomous Region",
    "海南": "Hainan Province",
    "重庆": "Chongqing Municipality",
    "四川": "Sichuan Province",
    "贵州": "Guizhou Province",
    "云南": "Yunnan Province",
    "西藏": "Tibet Autonomous Region",
    "陕西": "Shaanxi Province",
    "甘肃": "Gansu Province",
    "青海": "Qinghai Province",
    "宁夏": "Ningxia Hui Autonomous Region",
    "新疆": "Xinjiang Uygur Autonomous Region",
    "香港": "Hong Kong SAR",
    "澳门": "Macao SAR",
    "台湾": "Taiwan",
    "全国": "Nationwide",
}

def _format_province_label(cn_name: str, *, translate_enabled: bool) -> str:
    """
    Display-only label:
      - If translate_enabled and CN name is known, show "云南 (Yunnan Province)"
      - Otherwise show CN only.
    """
    cn = (cn_name or "").strip()
    if not translate_enabled:
        return cn
    en = PROVINCE_CN_TO_ISO_EN.get(cn)
    return f"{cn} ({en})" if en else cn

# ============================================================
# Translation (DeepL, cached, on-demand)
# ============================================================

@st.cache_data(ttl=7 * 24 * 3600, show_spinner=False)
def _translate_to_en_deepl(text: str) -> str | None:
    """
    Translate Chinese → English using DeepL API (Free/Pro).
    Cached so identical strings translate once (saves quota).
    Returns None if missing key or translation fails.
    """
    t = (text or "").strip()
    if not t:
        return None

    # Skip mostly ASCII (already English-ish)
    try:
        if (sum(1 for ch in t if ord(ch) < 128) / max(1, len(t))) > 0.9:
            return None
    except Exception:
        pass

    # Prefer Streamlit secrets (Cloud), fallback to env var (local/Docker)
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
        import deepl  # pip install deepl
        translator = deepl.Translator(api_key)
        result = translator.translate_text(t, source_lang="ZH", target_lang="EN-US")
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
    """
    Remaining NEW across all entries (Red/Orange only), keyed by:
      prov = region or province_name or province or "全国"
      bkey = f"{prov}|{bucket}"
    """
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
    CMA renderer (EC-style):
      Province → (Red Warning / Orange Warning) → alerts

    - Uses 'headline' (fallback 'title') for display
    - Adds colored bullet before each alert title
    - Filters Yellow/Blue out defensively
    - Optional auto-translation (English (auto)) when translate_to_en is True
    - Province header shows ISO-style EN name in parentheses when translate is enabled
    """
    feed_key = conf.get("key", "cma")

    # Your feeds.py nests translate_to_en under feed["conf"]
    translate_enabled = bool((conf.get("conf") or {}).get("translate_to_en") or conf.get("translate_to_en"))

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

    # Filter to Red/Orange and normalize keys used for bucketing
    filtered = []
    for e in items:
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

    # Country-level remaining NEW badge
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

        prov_label = _format_province_label(prov, translate_enabled=translate_enabled)

        st.markdown(
            _stripe_wrap(f"<h2>{html.escape(prov_label)}</h2>", _prov_has_new()),
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

                    # Use headline (fallback title)
                    headline_cn = _norm(a.get("headline") or a.get("title")) or "(no title)"

                    # Colored bullet based on level
                    lvl = _norm(a.get("level"))
                    bullet_color = LEVEL_TO_BULLET_COLOR.get(lvl, "#888")

                    title_html = (
                        f"{prefix}"
                        f"<span style='color:{bullet_color};font-size:16px;'>&#9679;</span> "
                        f"<strong>{html.escape(headline_cn)}</strong>"
                    )
                    st.markdown(_stripe_wrap(title_html, is_new), unsafe_allow_html=True)

                    # Auto translate headline
                    headline_en = _maybe_translate(headline_cn, enabled=translate_enabled)
                    if headline_en:
                        st.markdown(f"*English (auto):* {html.escape(headline_en)}")

                    desc_cn = _norm(a.get("summary") or a.get("description") or a.get("body"))
                    if desc_cn:
                        st.markdown(html.escape(desc_cn).replace("\n", "  \n"))

                        # Auto translate description
                        desc_en = _maybe_translate(desc_cn, enabled=translate_enabled)
                        if desc_en:
                            st.markdown(f"*English (auto):* {html.escape(desc_en)}")

                    # ✅ Read more link (from scraper: entry["link"])
                    link = _norm(a.get("link"))
                    if link:
                        st.markdown(f"[Read more]({link})")

                    pub_label = _to_utc_label(a.get("published"))
                    if pub_label:
                        st.caption(f"Published: {pub_label}")

                    st.markdown("---")

        st.markdown("---")
