# renderers/cma.py
import html
import os
import re
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
# Province name mapping (CN → ISO-style EN)
# ============================================================

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
    t = (text or "").strip()
    if not t:
        return None

    try:
        if (sum(1 for ch in t if ord(ch) < 128) / max(1, len(t))) > 0.9:
            return None
    except Exception:
        pass

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
    "Red": "Red Warning",
    "Orange": "Orange Warning",
}

LEVEL_TO_BUCKET_LABEL_CN = {
    "Red": "红色预警",
    "Orange": "橙色预警",
}

_BUCKET_ORDER = [
    "Red Warning",
    "Orange Warning",
]

LEVEL_TO_BULLET_COLOR = {
    "Red": "#E60026",
    "Orange": "#FF7F00",
}

def _cma_bucket_from_level(level: str | None) -> str | None:
    return LEVEL_TO_BUCKET_LABEL.get(_norm(level))

# ============================================================
# CMA phenomenon parsing
# ============================================================

# Longer keys first matters, so we sort during matching.
CMA_PHENOMENON_CN_TO_EN = {
    "雷雨大风": "Thunderstorm Gale",
    "道路结冰": "Road Icing",
    "强对流": "Severe Convective Weather",
    "森林火险": "Forest Fire Risk",
    "地质灾害": "Geological Disaster",
    "暴风雪": "Blizzard",
    "暴雨": "Heavy Rain",
    "暴雪": "Snowstorm",
    "寒潮": "Cold Wave",
    "高温": "High Temperature",
    "低温": "Low Temperature",
    "大风": "Gale",
    "沙尘暴": "Sandstorm",
    "冰雹": "Hail",
    "大雾": "Fog",
    "霾": "Haze",
    "干旱": "Drought",
    "台风": "Typhoon",
    "洪水": "Flood",
    "雷电": "Lightning",
    "霜冻": "Frost",
    "寒冷": "Cold",
}

def _headline_cn(a: dict) -> str:
    return _norm(a.get("headline") or a.get("title"))

def _extract_cma_phenomenon_cn(text: str) -> str | None:
    t = _norm(text)
    if not t:
        return None

    for key in sorted(CMA_PHENOMENON_CN_TO_EN.keys(), key=len, reverse=True):
        if key in t:
            return key

    # fallback: try capture "<phenomenon><red/orange> warning"
    m = re.search(r"([\u4e00-\u9fff]{1,12})(红色|橙色)预警", t)
    if m:
        return _norm(m.group(1)) or None

    return None

def _cma_bucket_display_label(a: dict, *, translate_enabled: bool) -> str:
    """
    Pretty UI label for the button, e.g.
      - Orange Warning - Heavy Rain
      - Red Warning - Fog

    Falls back to generic Red/Orange Warning if no phenomenon can be extracted.
    """
    level = _norm(a.get("level"))
    generic_en = LEVEL_TO_BUCKET_LABEL.get(level) or "Warning"
    generic_cn = LEVEL_TO_BUCKET_LABEL_CN.get(level) or "预警"

    headline_cn = _headline_cn(a)
    phenomenon_cn = _extract_cma_phenomenon_cn(headline_cn)

    if not phenomenon_cn:
        return generic_en if translate_enabled else generic_cn

    phenomenon_en = CMA_PHENOMENON_CN_TO_EN.get(phenomenon_cn)
    if translate_enabled:
        return f"{generic_en} - {phenomenon_en or phenomenon_cn}"
    return f"{generic_cn} - {phenomenon_cn}"

# ============================================================
# Remaining NEW total (renderer-local)
# ============================================================

def _remaining_new_total(entries, bucket_lastseen) -> int:
    """
    Remaining NEW across all entries (Red/Orange only), keyed by:
      prov = region or province_name or province or "全国"
      bkey = f"{prov}|{bucket_key}"
    """
    total = 0
    for e in entries or []:
        prov = _norm(e.get("province_name") or e.get("province") or e.get("region")) or "全国"
        bucket_key = e.get("bucket_key")
        if not bucket_key:
            continue
        bkey = f"{prov}|{bucket_key}"
        if entry_ts(e) > float(bucket_lastseen.get(bkey, 0.0)):
            total += 1
    return total

# ============================================================
# CMA Grouped Renderer
# ============================================================

def render(entries, conf):
    """
    CMA renderer:
      Province -> Specific sub-bucket -> alerts

    Examples:
      - Orange Warning - Heavy Rain
      - Orange Warning - Gale
      - Red Warning - Fog

    Important:
      - bucket_key is the stable internal key for seen-state
      - bucket_label is the user-facing specific label
    """
    feed_key = conf.get("key", "cma")
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

    # Filter to Red/Orange and build stable + display bucket values
    filtered = []
    for e in items:
        level_bucket = _cma_bucket_from_level(e.get("level"))
        if not level_bucket:
            continue

        prov = _norm(e.get("region") or e.get("province_name") or e.get("province")) or "全国"

        bucket_label = _cma_bucket_display_label(e, translate_enabled=translate_enabled)
        bucket_key = bucket_label  # renderer-specific grouping key

        filtered.append(dict(
            e,
            bucket_key=bucket_key,
            bucket_label=bucket_label,
            province_name=prov,
            bkey=f"{prov}|{bucket_key}",
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

        # Group by specific bucket
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
            if ll.startswith("red") or ll.startswith("红色"):
                sev_rank = 0
            elif ll.startswith("orange") or ll.startswith("橙色"):
                sev_rank = 1
            else:
                sev_rank = 2
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

                    headline_cn = _headline_cn(a) or "(no title)"
                    lvl = _norm(a.get("level"))
                    bullet_color = LEVEL_TO_BULLET_COLOR.get(lvl, "#888")

                    title_html = (
                        f"{prefix}"
                        f"<span style='color:{bullet_color};font-size:16px;'>&#9679;</span> "
                        f"<strong>{html.escape(headline_cn)}</strong>"
                    )
                    st.markdown(_stripe_wrap(title_html, is_new), unsafe_allow_html=True)

                    # Optional auto-translation
                    headline_en = _maybe_translate(headline_cn, enabled=translate_enabled)
                    if headline_en:
                        st.markdown(f"*English (auto):* {html.escape(headline_en)}")

                    desc_cn = _norm(a.get("summary") or a.get("description") or a.get("body"))
                    if desc_cn:
                        st.markdown(html.escape(desc_cn).replace("\n", "  \n"))

                        desc_en = _maybe_translate(desc_cn, enabled=translate_enabled)
                        if desc_en:
                            st.markdown(f"*English (auto):* {html.escape(desc_en)}")

                    link = _norm(a.get("link"))
                    if link:
                        st.markdown(f"[Read more]({link})")

                    pub_label = _to_utc_label(a.get("published"))
                    if pub_label:
                        st.caption(f"Published: {pub_label}")

                    st.markdown("---")

        st.markdown("---")
