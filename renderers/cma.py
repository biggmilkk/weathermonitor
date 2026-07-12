# renderers/cma.py
import html
import os
import re
import time
from collections import OrderedDict
from typing import Any, Iterable, Optional, Set

import streamlit as st
from dateutil import parser as dateparser

from computation import (
    attach_timestamp,
    sort_newest,
    alphabetic_with_last,
    entry_ts,
    cma_bucket_label,
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


def _norm(s: Any) -> str:
    return str(s or "").strip()


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
# Config helpers
# ============================================================

DEFAULT_ALLOWED_LEVELS = {"Red", "Orange", "Yellow"}

CN_COLOR_TO_EN = {
    "红色": "Red",
    "橙色": "Orange",
    "黄色": "Yellow",
    "蓝色": "Blue",
}

EN_LEVELS = {"Red", "Orange", "Yellow", "Blue"}


def _conf_value(conf: dict, key: str, default: Any = None) -> Any:
    """
    Prefer nested feed config:
      {"conf": {"allowed_levels": [...]}}
    over stale top-level config:
      {"allowed_levels": [...]}
    """
    nested = conf.get("conf") if isinstance(conf.get("conf"), dict) else {}

    if isinstance(nested, dict) and key in nested:
        return nested.get(key)

    if key in conf:
        return conf.get(key)

    return default


def _normalise_level_name(value: Any) -> Optional[str]:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    compact = re.sub(r"\s+", "", text)

    if compact in CN_COLOR_TO_EN:
        return CN_COLOR_TO_EN[compact]

    lowered = compact.lower()
    for level in EN_LEVELS:
        if lowered == level.lower():
            return level

    return None


def _allowed_levels_from_conf(conf: dict) -> Set[str]:
    raw = _conf_value(conf, "allowed_levels", None)

    if raw is None:
        return set(DEFAULT_ALLOWED_LEVELS)

    if isinstance(raw, str):
        parts: Iterable[Any] = re.split(r"[,;\s]+", raw.strip())
    elif isinstance(raw, Iterable):
        parts = raw
    else:
        parts = [raw]

    out: Set[str] = set()

    for part in parts:
        level = _normalise_level_name(part)
        if level:
            out.add(level)

    return out or set(DEFAULT_ALLOWED_LEVELS)


# ============================================================
# Province name mapping (CN -> ISO-style EN)
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
    "China: National": "China: National",
}


def _format_province_label(name: str, *, translate_enabled: bool) -> str:
    raw = _norm(name)
    if not translate_enabled:
        return raw

    en = PROVINCE_CN_TO_ISO_EN.get(raw)
    return f"{raw} ({en})" if en and en != raw else raw


def _province_sort_key(name: str) -> tuple[int, str]:
    """
    Keep national bucket after province-style buckets.
    """
    n = _norm(name)
    if n in {"全国", "China: National", "Nationwide"}:
        return (1, n.lower())
    return (0, n.lower())


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
# CMA labels / colors
# ============================================================

LEVEL_TO_BULLET_COLOR = {
    "Red": "#E60026",
    "Orange": "#FF7F00",
    "Yellow": "#D6A700",
    "Blue": "#1E88E5",
}

LEVEL_TO_CN = {
    "Red": "红色",
    "Orange": "橙色",
    "Yellow": "黄色",
    "Blue": "蓝色",
}

LEVEL_SORT_RANK = {
    "Red": 0,
    "Orange": 1,
    "Yellow": 2,
    "Blue": 3,
}

HAZARD_CN_TO_EN = {
    "强对流天气": "Severe Convective Weather",
    "台风": "Typhoon",
    "暴雨": "Heavy Rain",
    "大风": "Gale",
    "大雾": "Heavy Fog",
    "沙尘暴": "Sandstorm",
    "暴雪": "Snowstorm",
    "寒潮": "Cold Wave",
    "冰冻": "Freezing",
    "高温": "High Temperature",
    "气象干旱": "Meteorological Drought",
    "干旱": "Drought",
    "低温": "Low Temperature",
    "山洪灾害气象": "Mountain Flood Risk",
    "山洪灾害": "Mountain Flood Risk",
    "地质灾害气象风险": "Geological Hazard Risk",
    "地质灾害": "Geological Hazard Risk",
    "中小河流洪水气象风险": "Small and Medium River Flood Risk",
    "中小河流洪水": "Small and Medium River Flood Risk",
    "渍涝风险气象": "Waterlogging Risk",
    "渍涝": "Waterlogging Risk",
    "农业气象灾害风险": "Agrometeorological Hazard Risk",
    "农业气象灾害": "Agrometeorological Hazard Risk",
}

# More-specific hazards first.
HAZARD_KEYS = sorted(HAZARD_CN_TO_EN.keys(), key=len, reverse=True)


def _headline_cn(a: dict) -> str:
    return _norm(a.get("headline") or a.get("title"))


def _entry_level(a: dict) -> Optional[str]:
    """
    Normalize level from scraper output, title, headline, or body.
    """
    direct = _normalise_level_name(a.get("level"))
    if direct:
        return direct

    text = " ".join(
        _norm(v)
        for v in (
            a.get("headline"),
            a.get("title"),
            a.get("summary"),
            a.get("description"),
            a.get("body"),
        )
        if v
    )
    compact = re.sub(r"\s+", "", text)

    for cn, en in CN_COLOR_TO_EN.items():
        if cn in compact:
            return en

    lowered = compact.lower()
    for level in EN_LEVELS:
        if level.lower() in lowered:
            return level

    return None


def _extract_hazard_cn(a: dict) -> Optional[str]:
    """
    Extract product/hazard name from national NMC titles and summaries.
    """
    text = " ".join(
        _norm(v)
        for v in (
            a.get("headline"),
            a.get("title"),
            a.get("summary"),
            a.get("description"),
            a.get("body"),
        )
        if v
    )
    compact = re.sub(r"\s+", "", text)

    for hazard in HAZARD_KEYS:
        if hazard in compact:
            return hazard

    # Fallback for titles like:
    #   中央气象台7月12日10时继续发布台风黄色预警
    title = re.sub(r"\s+", "", _headline_cn(a))
    title = re.sub(r"^.*?(继续发布|联合发布|发布)", "", title)
    title = re.sub(r"(红色|橙色|黄色|蓝色)(预警|预报).*$", "", title)
    title = title.strip("：:")

    return title or None


def _fallback_bucket_label(a: dict, *, translate_to_en: bool) -> Optional[str]:
    level = _entry_level(a)
    if not level:
        return None

    hazard_cn = _extract_hazard_cn(a) or "Warning"

    if translate_to_en:
        hazard_en = HAZARD_CN_TO_EN.get(hazard_cn, hazard_cn)
        return f"{level} Warning - {hazard_en}"

    level_cn = LEVEL_TO_CN.get(level, level)
    return f"{level_cn}预警 - {hazard_cn}"


def _bucket_label(a: dict, *, translate_to_en: bool) -> Optional[str]:
    """
    Use computation.cma_bucket_label when it works, but fall back to a local
    Red/Orange/Yellow/Blue-aware bucket label.

    This is important because older computation.cma_bucket_label versions often
    returned labels only for Red/Orange.
    """
    try:
        label = cma_bucket_label(a, translate_to_en=translate_to_en)
    except Exception:
        label = None

    if label:
        return label

    return _fallback_bucket_label(a, translate_to_en=translate_to_en)


def _bucket_sort_key(label: str, sample_entry: dict | None = None):
    level = _entry_level(sample_entry or {}) if sample_entry else None
    ll = _norm(label).lower()

    if not level:
        if ll.startswith("red") or ll.startswith("红色"):
            level = "Red"
        elif ll.startswith("orange") or ll.startswith("橙色"):
            level = "Orange"
        elif ll.startswith("yellow") or ll.startswith("黄色"):
            level = "Yellow"
        elif ll.startswith("blue") or ll.startswith("蓝色"):
            level = "Blue"

    sev_rank = LEVEL_SORT_RANK.get(level or "", 99)
    return (sev_rank, ll)


# ============================================================
# Remaining NEW total (renderer-local)
# ============================================================

def _remaining_new_total(
    entries,
    bucket_lastseen,
    *,
    translate_enabled: bool,
    allowed_levels: Set[str],
) -> int:
    """
    Remaining NEW across all CMA/NMC entries, including Yellow.

    Key:
      prov = region or province_name or province or "全国"
      bucket = local Yellow-aware bucket label
      bkey = f"{prov}|{bucket}"
    """
    total = 0

    for e in entries or []:
        level = _entry_level(e)
        if level not in allowed_levels:
            continue

        prov = _norm(e.get("province_name") or e.get("province") or e.get("region")) or "全国"
        bucket = _bucket_label(e, translate_to_en=translate_enabled)
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
    CMA/NMC renderer:
      Province/National -> Specific severity/hazard bucket -> alerts

    Examples:
      translate_to_en=True:
        Yellow Warning - Typhoon
        Orange Warning - Heavy Rain

      translate_to_en=False:
        黄色预警 - 台风
        橙色预警 - 暴雨
    """
    feed_key = conf.get("key", "cma")

    translate_enabled = bool(
        (conf.get("conf") or {}).get("translate_to_en")
        or conf.get("translate_to_en")
    )

    allowed_levels = _allowed_levels_from_conf(conf)

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

    # Filter to configured levels and build stable bucket keys.
    filtered = []
    for e in items:
        level = _entry_level(e)
        if level not in allowed_levels:
            continue

        bucket_label = _bucket_label(e, translate_to_en=translate_enabled)
        if not bucket_label:
            continue

        prov = _norm(e.get("region") or e.get("province_name") or e.get("province")) or "全国"

        filtered.append(dict(
            e,
            level=level,
            bucket_key=bucket_label,
            bucket_label=bucket_label,
            province_name=prov,
            bkey=f"{prov}|{bucket_label}",
        ))

    if not filtered:
        st.session_state[f"{feed_key}_remaining_new_total"] = 0
        render_empty_state()
        return

    st.session_state[f"{feed_key}_remaining_new_total"] = _remaining_new_total(
        filtered,
        bucket_lastseen,
        translate_enabled=translate_enabled,
        allowed_levels=allowed_levels,
    )

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

    # Group by province/national.
    groups: OrderedDict[str, list[dict]] = OrderedDict()
    for e in filtered:
        groups.setdefault(e["province_name"], []).append(e)

    provinces = sorted(groups.keys(), key=_province_sort_key)

    # Preserve old behavior if the only national label is exactly 全国.
    if "全国" in groups and "China: National" not in groups:
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

        # Group by specific bucket.
        buckets: OrderedDict[str, dict] = OrderedDict()
        for a in alerts:
            bk = a["bucket_key"]
            if bk not in buckets:
                buckets[bk] = {
                    "label": a["bucket_label"],
                    "items": [],
                    "sample": a,
                }
            buckets[bk]["items"].append(a)

        ordered_bucket_keys = sorted(
            buckets.keys(),
            key=lambda bk: _bucket_sort_key(
                buckets[bk]["label"],
                buckets[bk].get("sample"),
            ),
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

                    # Commit previous bucket if switching.
                    if prev and prev != bkey:
                        bucket_lastseen[prev] = float(pending_seen.pop(prev, time.time()))

                    if active_bucket == bkey:
                        # Closing: commit this bucket as seen at open time.
                        bucket_lastseen[bkey] = float(pending_seen.pop(bkey, time.time()))
                        st.session_state[open_key] = None
                    else:
                        # Opening: start pending timer.
                        st.session_state[open_key] = bkey
                        pending_seen[bkey] = time.time()

                    if not st.session_state.get(rerun_guard_key):
                        st.session_state[rerun_guard_key] = True
                        st.session_state[lastseen_key] = bucket_lastseen
                        st.session_state[pending_map_key] = pending_seen
                        st.session_state[f"{feed_key}_remaining_new_total"] = _remaining_new_total(
                            filtered,
                            bucket_lastseen,
                            translate_enabled=translate_enabled,
                            allowed_levels=allowed_levels,
                        )
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

            # Expanded bucket content.
            if st.session_state.get(open_key) == bkey:
                for a in sort_newest(attach_timestamp(items_in_bucket)):
                    is_new = entry_ts(a) > last_seen
                    prefix = "[NEW] " if is_new else ""

                    headline_cn = _headline_cn(a) or "(no title)"
                    lvl = _entry_level(a) or _norm(a.get("level"))
                    bullet_color = LEVEL_TO_BULLET_COLOR.get(lvl, "#888")

                    title_html = (
                        f"{prefix}"
                        f"<span style='color:{bullet_color};font-size:16px;'>&#9679;</span> "
                        f"<strong>{html.escape(headline_cn)}</strong>"
                    )
                    st.markdown(_stripe_wrap(title_html, is_new), unsafe_allow_html=True)

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
