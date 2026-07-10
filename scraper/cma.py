# scraper/cma.py
from __future__ import annotations

import asyncio
import html
import logging
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set
from urllib.parse import urljoin

import httpx

__all__ = ["scrape_cma_async", "scrape_async", "scrape"]

# ---------------------------------------------------------------------
# Primary automated source
# ---------------------------------------------------------------------
#
# weather.cma.cn is currently returning a WAF / human-verification page
# for automated requests. So this scraper uses www.nmc.cn as the primary
# machine-readable source for active China warning signals.
#
# NMC local warning JSON:
#   https://www.nmc.cn/rest/findAlarm
#
# Optional weather.cma.cn calls are left disabled by default. Enable only
# if those endpoints are reachable from your environment without the WAF.
# ---------------------------------------------------------------------

NMC_BASE = "https://www.nmc.cn"
NMC_FIND_ALARM_URL = f"{NMC_BASE}/rest/findAlarm"
NMC_ALARM_PAGE_URL = f"{NMC_BASE}/publish/alarm.html"

WEATHER_CMA_BASE = "https://weather.cma.cn"
WEATHER_CMA_MAP_ALARM_URL = f"{WEATHER_CMA_BASE}/api/map/alarm?adcode="
WEATHER_CMA_INDEX_ALARM_URL = f"{WEATHER_CMA_BASE}/api/alarm/newIndexalarm"
WEATHER_CMA_ALARM_MAP_PAGE_URL = f"{WEATHER_CMA_BASE}/web/alarm/map.html"

# NMC national product pages roughly replacing the daily "above-map"
# national warning products when weather.cma.cn is WAF-blocked.
NMC_NATIONAL_PAGES = [
    ("Rainstorm Warning", f"{NMC_BASE}/publish/country/warning/index.html"),
    ("Severe Convective Weather Warning", f"{NMC_BASE}/publish/country/warning/strongconvection.html"),
    ("Typhoon Warning", f"{NMC_BASE}/publish/country/warning/typhoon.html"),
    ("Gale Warning", f"{NMC_BASE}/publish/country/warning/gale.html"),
    ("Heavy Fog Warning", f"{NMC_BASE}/publish/country/warning/fog.html"),
    ("Sandstorm Warning", f"{NMC_BASE}/publish/country/warning/sand.html"),
    ("Snowstorm Warning", f"{NMC_BASE}/publish/country/warning/snow.html"),
    ("Cold Wave Warning", f"{NMC_BASE}/publish/country/warning/cold.html"),
    ("Freezing Warning", f"{NMC_BASE}/publish/country/warning/freeze.html"),
    ("High Temperature Warning", f"{NMC_BASE}/publish/country/warning/high-temperature.html"),
    ("Drought Warning", f"{NMC_BASE}/publish/country/warning/drought.html"),
    ("Low Temperature Warning", f"{NMC_BASE}/publish/country/warning/low-temperature.html"),
    ("Mountain Flood Risk Warning", f"{NMC_BASE}/publish/mountainflood.html"),
    ("Geological Hazard Risk Warning", f"{NMC_BASE}/publish/geohazard.html"),
    ("Waterlogging Risk Forecast", f"{NMC_BASE}/publish/waterlogging.html"),
]

# ---------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------

NMC_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Referer": NMC_ALARM_PAGE_URL,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

NMC_HTML_HEADERS = {
    **NMC_HEADERS,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

WEATHER_CMA_HEADERS = {
    "User-Agent": NMC_HEADERS["User-Agent"],
    "Referer": WEATHER_CMA_ALARM_MAP_PAGE_URL,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

# ---------------------------------------------------------------------
# Province mapping
# GB/T 2260 first two digits
# ---------------------------------------------------------------------

PROVINCE_CODE_TO_CN = {
    "11": "北京",
    "12": "天津",
    "13": "河北",
    "14": "山西",
    "15": "内蒙古",
    "21": "辽宁",
    "22": "吉林",
    "23": "黑龙江",
    "31": "上海",
    "32": "江苏",
    "33": "浙江",
    "34": "安徽",
    "35": "福建",
    "36": "江西",
    "37": "山东",
    "41": "河南",
    "42": "湖北",
    "43": "湖南",
    "44": "广东",
    "45": "广西",
    "46": "海南",
    "50": "重庆",
    "51": "四川",
    "52": "贵州",
    "53": "云南",
    "54": "西藏",
    "61": "陕西",
    "62": "甘肃",
    "63": "青海",
    "64": "宁夏",
    "65": "新疆",
    "71": "台湾",
    "81": "香港",
    "82": "澳门",
}

# ---------------------------------------------------------------------
# Severity handling
# ---------------------------------------------------------------------

DEFAULT_ALLOWED_LEVELS = {"Red", "Orange"}

CN_COLOR_TO_EN = {
    "红色": "Red",
    "橙色": "Orange",
    "黄色": "Yellow",
    "蓝色": "Blue",
}

LEVEL_DIGIT_TO_EN = {
    "1": "Red",
    "2": "Orange",
    "3": "Yellow",
    "4": "Blue",
}

RE_COLOR_STRONG = re.compile(
    r"(红\s*色|橙\s*色|黄\s*色|蓝\s*色)\s*(?:预警|预警信号|警报|警报信号)?"
)
RE_COLOR_COMPACT = re.compile(r"(红色|橙色|黄色|蓝色)")
RE_COLOR_CODE = re.compile(r"(?:_|\b)(RED|ORANGE|YELLOW|BLUE)\b", re.I)
RE_ICON_LEVEL = re.compile(r"p\d*([1-4])(?:\.png)?", re.I)
RE_TYPE_LEVEL = re.compile(r"p\d*([1-4])$", re.I)

# ---------------------------------------------------------------------
# HTML / WAF detection
# ---------------------------------------------------------------------

RE_TAGS = re.compile(r"<[^>]+>")
RE_SCRIPT_STYLE = re.compile(r"<(script|style)\b.*?</\1>", re.I | re.S)
RE_WS = re.compile(r"\s+")

WAF_MARKERS = (
    "WEB 应用防火墙",
    "人机识别检测",
    "向右滑动填充拼图",
    "js-challenge",
    "captcha",
)


class NonJsonResponse(RuntimeError):
    pass


def _looks_like_waf_or_html(text: str, content_type: str = "") -> bool:
    sample = text[:1000]
    if any(marker in sample for marker in WAF_MARKERS):
        return True

    if "text/html" in content_type.lower():
        return True

    return sample.lstrip().lower().startswith("<!doctype html") or sample.lstrip().lower().startswith("<html")


def _html_to_text(raw_html: str) -> str:
    cleaned = RE_SCRIPT_STYLE.sub(" ", raw_html or "")
    cleaned = RE_TAGS.sub(" ", cleaned)
    cleaned = html.unescape(cleaned)
    return RE_WS.sub(" ", cleaned).strip()


# ---------------------------------------------------------------------
# Time handling
# ---------------------------------------------------------------------

CST = timezone(timedelta(hours=8))


def _parse_pubtime(value: Optional[Any]) -> Optional[str]:
    """
    Parse China local time as UTC+8 and convert to UTC ISO 8601.
    """
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    formats = (
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y年%m月%d日%H时%M分",
        "%Y年%m月%d日%H时",
    )

    for fmt in formats:
        try:
            local_dt = datetime.strptime(text, fmt).replace(tzinfo=CST)
            return local_dt.astimezone(timezone.utc).isoformat()
        except Exception:
            continue

    return None


def _parse_nmc_national_time(text: str) -> Optional[str]:
    """
    Parse strings like:
      2026 年 07 月 10 日 18 时
      7月10日18时
    """
    now = datetime.now(CST)

    m = re.search(
        r"(?P<y>\d{4})\s*年\s*(?P<m>\d{1,2})\s*月\s*(?P<d>\d{1,2})\s*日\s*(?P<h>\d{1,2})\s*时",
        text,
    )
    if m:
        local_dt = datetime(
            int(m.group("y")),
            int(m.group("m")),
            int(m.group("d")),
            int(m.group("h")),
            0,
            tzinfo=CST,
        )
        return local_dt.astimezone(timezone.utc).isoformat()

    m = re.search(
        r"(?P<m>\d{1,2})\s*月\s*(?P<d>\d{1,2})\s*日\s*(?P<h>\d{1,2})\s*时",
        text,
    )
    if m:
        local_dt = datetime(
            now.year,
            int(m.group("m")),
            int(m.group("d")),
            int(m.group("h")),
            0,
            tzinfo=CST,
        )
        return local_dt.astimezone(timezone.utc).isoformat()

    return None


def _timestamp_from_iso(value: Optional[str], fallback: float) -> float:
    if not value:
        return fallback

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except Exception:
        return fallback


# ---------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------

def _conf_value(conf: Dict[str, Any], key: str, default: Any = None) -> Any:
    nested = conf.get("conf") if isinstance(conf.get("conf"), dict) else {}

    if key in conf:
        return conf.get(key)

    if isinstance(nested, dict) and key in nested:
        return nested.get(key)

    return default


def _conf_bool(conf: Dict[str, Any], key: str, default: bool) -> bool:
    value = _conf_value(conf, key, default)

    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}

    return bool(value)


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
    if lowered in {"red", "orange", "yellow", "blue"}:
        return lowered.capitalize()

    return text


def _allowed_levels_from_conf(conf: Dict[str, Any]) -> Set[str]:
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


# ---------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------

async def _get_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: Dict[str, str],
    timeout: float,
    params: Optional[Dict[str, Any]] = None,
    label: str = "json",
) -> Dict[str, Any]:
    resp = await client.get(url, headers=headers, params=params, timeout=timeout)
    resp.raise_for_status()

    content_type = resp.headers.get("content-type", "")
    text = resp.text or ""

    if _looks_like_waf_or_html(text, content_type):
        sample = text[:220].replace("\n", " ").replace("\r", " ")
        raise NonJsonResponse(f"{label}: non-JSON / WAF HTML response: {sample}")

    try:
        payload = resp.json()
    except Exception as exc:
        sample = text[:220].replace("\n", " ").replace("\r", " ")
        raise NonJsonResponse(f"{label}: could not parse JSON: {sample}") from exc

    if not isinstance(payload, dict):
        raise NonJsonResponse(f"{label}: expected JSON object, got {type(payload).__name__}")

    return payload


async def _get_text(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: Dict[str, str],
    timeout: float,
    label: str = "html",
) -> str:
    resp = await client.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()

    text = resp.text or ""

    if _looks_like_waf_or_html(text, resp.headers.get("content-type", "")) and "WEB 应用防火墙" in text:
        sample = text[:220].replace("\n", " ").replace("\r", " ")
        raise NonJsonResponse(f"{label}: WAF HTML response: {sample}")

    return text


# ---------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------

def _norm_text(value: Any) -> str:
    return RE_WS.sub(" ", str(value or "")).strip()


def _extract_level(item: Dict[str, Any]) -> Optional[str]:
    """
    Extract Red / Orange / Yellow / Blue from title/headline/pic/type.
    """
    pieces = [
        item.get("headline"),
        item.get("title"),
        item.get("description"),
        item.get("type"),
        item.get("severity"),
        item.get("level"),
        item.get("pic"),
        item.get("image"),
    ]

    text = " ".join(_norm_text(v) for v in pieces if v)
    compact = re.sub(r"\s+", "", text)

    m = RE_COLOR_STRONG.search(text)
    if m:
        return CN_COLOR_TO_EN.get(re.sub(r"\s+", "", m.group(1)))

    m = RE_COLOR_COMPACT.search(compact)
    if m:
        return CN_COLOR_TO_EN.get(m.group(1))

    m = RE_COLOR_CODE.search(text)
    if m:
        return m.group(1).capitalize()

    icon_text = " ".join(
        _norm_text(v)
        for v in (item.get("pic"), item.get("image"), item.get("type"))
        if v
    )

    m = RE_ICON_LEVEL.search(icon_text)
    if m:
        return LEVEL_DIGIT_TO_EN.get(m.group(1))

    m = RE_TYPE_LEVEL.search(_norm_text(item.get("type")))
    if m:
        return LEVEL_DIGIT_TO_EN.get(m.group(1))

    return None


def _province_from_id(value: Any) -> Optional[str]:
    if isinstance(value, str):
        m = re.match(r"^(\d{6,})", value.strip())
        if m:
            return PROVINCE_CODE_TO_CN.get(m.group(1)[:2])

    return None


def _province_from_text(text: str) -> Optional[str]:
    for province in PROVINCE_CODE_TO_CN.values():
        if province and province in text:
            return province

    return None


def _region_from_alarm(item: Dict[str, Any]) -> str:
    alarm_id = item.get("alertid") or item.get("id")
    text = " ".join(
        _norm_text(v)
        for v in (item.get("title"), item.get("headline"), item.get("description"))
        if v
    )

    return _province_from_id(alarm_id) or _province_from_text(text) or "全国"


def _absolute_nmc_url(path_or_url: Any) -> Optional[str]:
    if not path_or_url:
        return None

    return urljoin(NMC_BASE, str(path_or_url).strip())


def _absolute_weather_cma_url(path_or_url: Any) -> Optional[str]:
    if not path_or_url:
        return None

    return urljoin(WEATHER_CMA_BASE, str(path_or_url).strip())


def _alarm_link_from_cma_id(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None

    alarm_id = value.strip()
    if not alarm_id:
        return None

    return f"{WEATHER_CMA_BASE}/web/alarm/{alarm_id}.html"


def _entry_key(entry: Dict[str, Any]) -> str:
    for key in ("id", "link"):
        value = entry.get(key)
        if value:
            return f"{key}:{value}"

    return "|".join(
        str(entry.get(k) or "")
        for k in ("source_kind", "region", "title", "published")
    )


def _dedupe_entries(entries: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    out: List[Dict[str, Any]] = []

    for entry in entries:
        key = _entry_key(entry)
        if key in seen:
            continue

        seen.add(key)
        out.append(entry)

    return out


# ---------------------------------------------------------------------
# NMC local warnings
# ---------------------------------------------------------------------

async def _fetch_nmc_alarm_page(
    client: httpx.AsyncClient,
    *,
    page_no: int,
    page_size: int,
    timeout: float,
) -> Dict[str, Any]:
    params = {
        "pageNo": int(page_no),
        "pageSize": int(page_size),
        "signaltype": "",
        "signallevel": "",
        "province": "",
        "_": int(time.time() * 1000),
    }

    return await _get_json(
        client,
        NMC_FIND_ALARM_URL,
        headers=NMC_HEADERS,
        timeout=timeout,
        params=params,
        label=f"NMC findAlarm page {page_no}",
    )


def _local_entry_from_nmc_item(
    item: Dict[str, Any],
    *,
    allowed_levels: Set[str],
    now_ts: float,
) -> Optional[Dict[str, Any]]:
    level = _extract_level(item)

    if level not in allowed_levels:
        return None

    alert_id = item.get("alertid") or item.get("id")
    title = _norm_text(item.get("title")) or "China Weather Alert"
    published = _parse_pubtime(item.get("issuetime") or item.get("effective"))
    ts = _timestamp_from_iso(published, now_ts)
    link = _absolute_nmc_url(item.get("url"))

    return {
        "source": "CMA/NMC",
        "source_kind": "local",
        "id": alert_id,
        "headline": title,
        "title": title,
        "level": level,
        "region": _region_from_alarm(item),
        "summary": "",
        "published": published,
        "timestamp": ts,
        "link": link,
        "image": item.get("pic"),
    }


async def _fetch_nmc_local_entries(
    client: httpx.AsyncClient,
    conf: Dict[str, Any],
    *,
    allowed_levels: Set[str],
    timeout: float,
    now_ts: float,
) -> List[Dict[str, Any]]:
    page_size = int(_conf_value(conf, "nmc_page_size", 200) or 200)
    max_pages = int(_conf_value(conf, "nmc_max_pages", 20) or 20)

    first_payload = await _fetch_nmc_alarm_page(
        client,
        page_no=1,
        page_size=page_size,
        timeout=timeout,
    )

    data = first_payload.get("data") or {}
    page = data.get("page") or {}

    if not isinstance(page, dict):
        logging.warning("[CMA/NMC] Unexpected NMC page shape")
        return []

    total_page = int(page.get("totalPage") or 1)
    total_page = max(1, min(total_page, max_pages))

    items: List[Dict[str, Any]] = []

    first_list = page.get("list") or []
    if isinstance(first_list, list):
        items.extend(x for x in first_list if isinstance(x, dict))

    province_alarms = data.get("provinceAlarms") or []
    if isinstance(province_alarms, list):
        items.extend(x for x in province_alarms if isinstance(x, dict))

    if total_page > 1:
        tasks = [
            _fetch_nmc_alarm_page(
                client,
                page_no=page_no,
                page_size=page_size,
                timeout=timeout,
            )
            for page_no in range(2, total_page + 1)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for page_no, result in zip(range(2, total_page + 1), results):
            if isinstance(result, Exception):
                logging.warning("[CMA/NMC] Failed NMC page %s: %s", page_no, result)
                continue

            page_data = (result.get("data") or {}).get("page") or {}
            page_list = page_data.get("list") or []

            if isinstance(page_list, list):
                items.extend(x for x in page_list if isinstance(x, dict))

    entries: List[Dict[str, Any]] = []

    for item in items:
        try:
            entry = _local_entry_from_nmc_item(
                item,
                allowed_levels=allowed_levels,
                now_ts=now_ts,
            )
            if entry:
                entries.append(entry)
        except Exception:
            logging.exception("[CMA/NMC] Local warning parse error")

    return entries


# ---------------------------------------------------------------------
# NMC national products
# ---------------------------------------------------------------------

def _national_entry_from_nmc_html(
    raw_html: str,
    *,
    label: str,
    url: str,
    allowed_levels: Set[str],
    now_ts: float,
) -> Optional[Dict[str, Any]]:
    text = _html_to_text(raw_html)

    if not text:
        return None

    level = _extract_level({"title": text})

    if level not in allowed_levels:
        return None

    published = _parse_nmc_national_time(text)
    ts = _timestamp_from_iso(published, now_ts)

    # Make a compact summary around the main warning sentence.
    summary = ""
    m = re.search(r"(中央气象台.*?预警[:：].*?)(?:防御指南|相关产品|推荐服务|$)", text)
    if m:
        summary = _norm_text(m.group(1))[:700]

    title = label
    m_title = re.search(r"([\u4e00-\u9fffA-Za-z0-9\s]+预警)", text)
    if m_title:
        title = _norm_text(m_title.group(1)) or label

    return {
        "source": "CMA/NMC",
        "source_kind": "national",
        "headline": title,
        "title": f"{title} – {level}",
        "level": level,
        "region": "全国",
        "summary": summary,
        "published": published,
        "timestamp": ts,
        "link": url,
    }


async def _fetch_nmc_national_entries(
    client: httpx.AsyncClient,
    conf: Dict[str, Any],
    *,
    allowed_levels: Set[str],
    timeout: float,
    now_ts: float,
) -> List[Dict[str, Any]]:
    include_national = _conf_bool(conf, "include_nmc_national", True)

    if not include_national:
        return []

    tasks = [
        _get_text(
            client,
            url,
            headers=NMC_HTML_HEADERS,
            timeout=timeout,
            label=f"NMC national {label}",
        )
        for label, url in NMC_NATIONAL_PAGES
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    entries: List[Dict[str, Any]] = []

    for (label, url), result in zip(NMC_NATIONAL_PAGES, results):
        if isinstance(result, Exception):
            # Some optional national pages may move or 404. Do not fail the feed.
            logging.debug("[CMA/NMC] National page unavailable %s: %s", label, result)
            continue

        try:
            entry = _national_entry_from_nmc_html(
                result,
                label=label,
                url=url,
                allowed_levels=allowed_levels,
                now_ts=now_ts,
            )
            if entry:
                entries.append(entry)
        except Exception:
            logging.exception("[CMA/NMC] National warning parse error: %s", label)

    return entries


# ---------------------------------------------------------------------
# Optional weather.cma.cn support
# Disabled by default because these endpoints currently return WAF HTML.
# ---------------------------------------------------------------------

def _local_entry_from_weather_cma_alarm(
    item: Dict[str, Any],
    *,
    allowed_levels: Set[str],
    now_ts: float,
) -> Optional[Dict[str, Any]]:
    level = _extract_level(item)

    if level not in allowed_levels:
        return None

    alarm_id = item.get("id")
    headline = _norm_text(item.get("headline"))
    title = headline or _norm_text(item.get("title")) or "CMA Alert"
    published = _parse_pubtime(
        item.get("effective")
        or item.get("pubTime")
        or item.get("publishTime")
        or item.get("releaseTime")
    )
    ts = _timestamp_from_iso(published, now_ts)

    entry: Dict[str, Any] = {
        "source": "CMA",
        "source_kind": "local",
        "id": alarm_id,
        "headline": title,
        "title": title,
        "level": level,
        "region": _region_from_alarm(item),
        "summary": _norm_text(item.get("description")),
        "published": published,
        "timestamp": ts,
        "link": _alarm_link_from_cma_id(alarm_id),
        "raw_type": item.get("type"),
    }

    if item.get("longitude") is not None:
        entry["longitude"] = item.get("longitude")

    if item.get("latitude") is not None:
        entry["latitude"] = item.get("latitude")

    return entry


def _national_entry_from_weather_cma_index_item(
    item: Dict[str, Any],
    *,
    allowed_levels: Set[str],
    include_without_level: bool,
    now_ts: float,
) -> Optional[Dict[str, Any]]:
    title = _norm_text(item.get("title")) or "CMA National Product"
    description = _norm_text(item.get("description"))
    level = _extract_level(item)

    if level not in allowed_levels:
        if not include_without_level:
            return None
        level = level or "Info"

    published = _parse_pubtime(
        item.get("releaseTime")
        or item.get("effective")
        or item.get("pubTime")
        or item.get("publishTime")
    )
    ts = _timestamp_from_iso(published, now_ts)

    return {
        "source": "CMA",
        "source_kind": "national",
        "headline": description or title,
        "title": title,
        "level": level,
        "region": "全国",
        "summary": description,
        "published": published,
        "timestamp": ts,
        "link": _absolute_weather_cma_url(item.get("link")),
        "image": _absolute_weather_cma_url(item.get("image")),
    }


async def _fetch_weather_cma_entries_if_enabled(
    client: httpx.AsyncClient,
    conf: Dict[str, Any],
    *,
    allowed_levels: Set[str],
    timeout: float,
    now_ts: float,
) -> List[Dict[str, Any]]:
    try_weather_cma = _conf_bool(conf, "try_weather_cma", False)

    if not try_weather_cma:
        return []

    include_cma_national_without_level = _conf_bool(
        conf,
        "include_cma_national_without_level",
        True,
    )

    entries: List[Dict[str, Any]] = []

    # Local map alarms.
    try:
        adcode = str(_conf_value(conf, "adcode", "") or "")
        payload = await _get_json(
            client,
            f"{WEATHER_CMA_MAP_ALARM_URL}{adcode}",
            headers=WEATHER_CMA_HEADERS,
            timeout=timeout,
            label=f"weather.cma map/alarm adcode={adcode!r}",
        )
        data = payload.get("data") or []

        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue

                entry = _local_entry_from_weather_cma_alarm(
                    item,
                    allowed_levels=allowed_levels,
                    now_ts=now_ts,
                )
                if entry:
                    entries.append(entry)

    except NonJsonResponse as exc:
        logging.warning("[CMA FETCH ERROR] map/alarm: %s", exc)
    except Exception as exc:
        logging.warning("[CMA FETCH ERROR] map/alarm: %s", exc)

    # National above-map links.
    try:
        payload = await _get_json(
            client,
            WEATHER_CMA_INDEX_ALARM_URL,
            headers=WEATHER_CMA_HEADERS,
            timeout=timeout,
            label="weather.cma newIndexalarm",
        )
        data = payload.get("data") or {}

        if isinstance(data, dict):
            gj_items = data.get("gj") or []
            if isinstance(gj_items, list):
                for item in gj_items:
                    if not isinstance(item, dict):
                        continue

                    entry = _national_entry_from_weather_cma_index_item(
                        item,
                        allowed_levels=allowed_levels,
                        include_without_level=include_cma_national_without_level,
                        now_ts=now_ts,
                    )
                    if entry:
                        entries.append(entry)

    except NonJsonResponse as exc:
        logging.warning("[CMA FETCH ERROR] newIndexalarm: %s", exc)
    except Exception as exc:
        logging.warning("[CMA FETCH ERROR] newIndexalarm: %s", exc)

    return entries


# ---------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------

async def scrape_cma_async(
    conf: Dict[str, Any],
    client: httpx.AsyncClient,
) -> Dict[str, Any]:
    """
    CMA / China weather warning scraper.

    Default behavior:
      - Uses www.nmc.cn/rest/findAlarm as the main local-warning source.
      - Filters to Red/Orange by default.
      - Adds Red/Orange national product pages from NMC.
      - Does NOT hit weather.cma.cn by default, because it is currently
        returning a WAF challenge page to automated requests.

    Optional config keys:
      allowed_levels: ["Red", "Orange"]
      timeout: 15
      nmc_page_size: 200
      nmc_max_pages: 20
      include_nmc_national: true
      try_weather_cma: false
      include_cma_national_without_level: true
    """
    timeout = float(_conf_value(conf, "timeout", 15) or 15)
    allowed_levels = _allowed_levels_from_conf(conf)
    now_ts = datetime.now(timezone.utc).timestamp()

    entries: List[Dict[str, Any]] = []
    errors: List[str] = []

    # 1. Main working source: NMC active local warning list.
    try:
        entries.extend(
            await _fetch_nmc_local_entries(
                client,
                conf,
                allowed_levels=allowed_levels,
                timeout=timeout,
                now_ts=now_ts,
            )
        )
    except Exception as exc:
        logging.exception("[CMA/NMC FETCH ERROR] local warnings")
        errors.append(f"nmc local: {exc}")

    # 2. NMC national warning product pages.
    try:
        entries.extend(
            await _fetch_nmc_national_entries(
                client,
                conf,
                allowed_levels=allowed_levels,
                timeout=timeout,
                now_ts=now_ts,
            )
        )
    except Exception as exc:
        logging.exception("[CMA/NMC FETCH ERROR] national warnings")
        errors.append(f"nmc national: {exc}")

    # 3. Optional weather.cma.cn source.
    # Disabled by default because of WAF. Turn on only if the endpoint
    # is accessible from your deployment environment.
    try:
        entries.extend(
            await _fetch_weather_cma_entries_if_enabled(
                client,
                conf,
                allowed_levels=allowed_levels,
                timeout=timeout,
                now_ts=now_ts,
            )
        )
    except Exception as exc:
        logging.exception("[CMA FETCH ERROR] optional weather.cma.cn")
        errors.append(f"weather.cma optional: {exc}")

    entries = _dedupe_entries(entries)
    entries.sort(key=lambda x: float(x.get("timestamp") or 0.0), reverse=True)

    logging.warning(
        "[CMA DEBUG] Parsed %d entries; allowed_levels=%s; try_weather_cma=%s",
        len(entries),
        sorted(allowed_levels),
        _conf_bool(conf, "try_weather_cma", False),
    )

    result: Dict[str, Any] = {
        "source": "CMA/NMC",
        "entries": entries,
    }

    if errors:
        result["error"] = "; ".join(errors)

    return result


# ---------------------------------------------------------------------
# Registry aliases
# ---------------------------------------------------------------------

async def scrape_async(conf, client):
    return await scrape_cma_async(conf, client)


async def scrape(conf, client):
    return await scrape_cma_async(conf, client)
