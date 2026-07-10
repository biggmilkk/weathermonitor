# scraper/cma.py
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set
from urllib.parse import urljoin

import httpx

__all__ = ["scrape_cma_async", "scrape_async", "scrape"]

BASE_URL = "https://weather.cma.cn"
MAP_PAGE_URL = f"{BASE_URL}/web/alarm/map.html"
INDEX_ALARM_API_URL = f"{BASE_URL}/api/alarm/newIndexalarm"
MAP_ALARM_API_URL = f"{BASE_URL}/api/map/alarm?adcode={{adcode}}"

# Local alarms are filtered to warning-level colours by default.
# National channel links above the map are included by default even when
# they are Yellow/Blue/unknown, because the website itself chooses what
# to show above the map on that day.
DEFAULT_LOCAL_ALLOWED_LEVELS = {"Red", "Orange"}
DEFAULT_NATIONAL_ALLOWED_LEVELS = None  # None = include all visible national channels

# --------------------------
# HTTP headers
# --------------------------

CMA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Referer": MAP_PAGE_URL,
    "Origin": BASE_URL,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

# --------------------------
# Province mapping (GB/T 2260 first two digits)
# --------------------------

PROVINCE_CODE_TO_CN = {
    "11": "北京", "12": "天津", "13": "河北", "14": "山西", "15": "内蒙古",
    "21": "辽宁", "22": "吉林", "23": "黑龙江",
    "31": "上海", "32": "江苏", "33": "浙江", "34": "安徽", "35": "福建", "36": "江西", "37": "山东",
    "41": "河南", "42": "湖北", "43": "湖南", "44": "广东", "45": "广西", "46": "海南",
    "50": "重庆", "51": "四川", "52": "贵州", "53": "云南", "54": "西藏",
    "61": "陕西", "62": "甘肃", "63": "青海", "64": "宁夏", "65": "新疆",
    "71": "台湾", "81": "香港", "82": "澳门",
}

# --------------------------
# Severity handling
# --------------------------

CN_COLOR_TO_EN = {
    "红色": "Red",
    "橙色": "Orange",
    "黄色": "Yellow",
    "蓝色": "Blue",
}

# CMA icon/type codes commonly end with colour level:
#   1 = Red, 2 = Orange, 3 = Yellow, 4 = Blue
TYPE_SUFFIX_TO_LEVEL = {
    "1": "Red",
    "2": "Orange",
    "3": "Yellow",
    "4": "Blue",
}

RE_COLOR_STRONG = re.compile(r"(红色|橙色|黄色|蓝色)\s*(?:预警|预警信号|警报|警报信号|风险预警|风险)")
RE_COLOR_SIMPLE = re.compile(r"(红色|橙色|黄色|蓝色)")
RE_COLOR_CODE = re.compile(r"(?:_|-|\b)(RED|ORANGE|YELLOW|BLUE)\b", re.I)
RE_TYPE_SUFFIX = re.compile(r"p\d{6,}([1-4])$", re.I)

# Region extraction from Chinese titles/headlines.
RE_REGION_BEFORE_ACTION = re.compile(
    r"^(.{1,60}?)(?:气象台|气象局|应急管理局|自然资源部与中国气象局)?"
    r"(?:发布|更新|继续发布|变更|解除|取消|终止)"
)
RE_CANCELLED = re.compile(r"(?:解除|取消|终止).{0,12}(?:预警|警报|信号)")
RE_CHANNEL_LINK = re.compile(r"(?:href=[\"']|[\"'])(/web/channel-[^\"']+\.html)")

# --------------------------
# Time handling
# --------------------------

CST = timezone(timedelta(hours=8))  # China Standard Time


def _parse_pubtime(s: Optional[Any]) -> Optional[str]:
    """
    Parse CMA times as UTC+8 and convert to UTC ISO 8601.
    Returns None when a time is missing or unparseable.
    """
    if s is None:
        return None

    if isinstance(s, (int, float)):
        try:
            # Accept both seconds and milliseconds.
            ts = float(s)
            if ts > 10_000_000_000:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except Exception:
            return None

    text = str(s).strip()
    if not text:
        return None

    # Chinese date format, e.g. 2026年06月15日12时55分
    m = re.search(
        r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日\s*(\d{1,2})时\s*(?:(\d{1,2})分)?",
        text,
    )
    if m:
        year, month, day, hour, minute = m.groups()
        try:
            local_dt = datetime(
                int(year), int(month), int(day), int(hour), int(minute or 0), tzinfo=CST
            )
            return local_dt.astimezone(timezone.utc).isoformat()
        except Exception:
            pass

    for fmt in (
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d",
        "%Y-%m-%d",
    ):
        try:
            local_dt = datetime.strptime(text, fmt).replace(tzinfo=CST)
            return local_dt.astimezone(timezone.utc).isoformat()
        except Exception:
            continue

    return None


def _timestamp_from_iso(iso_dt: Optional[str], fallback_ts: float) -> float:
    if not iso_dt:
        return fallback_ts
    try:
        return datetime.fromisoformat(iso_dt.replace("Z", "+00:00")).timestamp()
    except Exception:
        return fallback_ts

# --------------------------
# Config helpers
# --------------------------


def _conf_get(conf: Dict[str, Any], key: str, default: Any = None) -> Any:
    """
    Accept both top-level feed config keys and nested conf={...} keys.
    """
    if not isinstance(conf, dict):
        return default
    if key in conf:
        return conf[key]
    inner = conf.get("conf")
    if isinstance(inner, dict) and key in inner:
        return inner[key]
    return default


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off", "none", ""}
    return bool(value)


def _normalise_level_name(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    lower = text.lower()
    if lower == "red":
        return "Red"
    if lower == "orange":
        return "Orange"
    if lower == "yellow":
        return "Yellow"
    if lower == "blue":
        return "Blue"
    # Also accept Chinese colour names in config.
    return CN_COLOR_TO_EN.get(text, text[:1].upper() + text[1:])


def _normalise_level_set(value: Any) -> Set[str]:
    if isinstance(value, str):
        values: Iterable[Any] = re.split(r"[,|\s]+", value.strip())
    elif isinstance(value, Iterable):
        values = value
    else:
        values = [value]

    out: Set[str] = set()
    for item in values:
        level = _normalise_level_name(item)
        if level:
            out.add(level)
    return out


def _configured_level_filter(
    conf: Dict[str, Any],
    key: str,
    default: Optional[Set[str]],
) -> Optional[Set[str]]:
    marker = object()
    raw = _conf_get(conf, key, marker)
    if raw is marker:
        return default
    if raw is None:
        return None
    return _normalise_level_set(raw)


def _configured_adcodes(conf: Dict[str, Any]) -> List[str]:
    raw = _conf_get(conf, "adcodes", None)
    if raw is None:
        raw = _conf_get(conf, "adcode", "")

    if raw is None or raw == "":
        return [""]

    if isinstance(raw, str):
        parts = re.split(r"[,|\s]+", raw.strip())
    elif isinstance(raw, Sequence):
        parts = [str(x).strip() for x in raw]
    else:
        parts = [str(raw).strip()]

    adcodes = [p for p in parts if p]
    return adcodes or [""]

# --------------------------
# Data helpers
# --------------------------


def _compact_text(*values: Any) -> str:
    return " ".join(str(v).strip() for v in values if v is not None and str(v).strip())


def _compact_text_no_space(*values: Any) -> str:
    return re.sub(r"\s+", "", _compact_text(*values))


def _absolute_url(url: Optional[Any]) -> Optional[str]:
    if url is None:
        return None
    text = str(url).strip()
    if not text:
        return None
    return urljoin(BASE_URL, text)


def _extract_level(item: Dict[str, Any]) -> Optional[str]:
    """
    Extract Red/Orange/Yellow/Blue from text fields or CMA type code.
    """
    text = _compact_text_no_space(
        item.get("headline"),
        item.get("title"),
        item.get("description"),
        item.get("type"),
        item.get("severity"),
        item.get("level"),
        item.get("signallevel"),
        item.get("signalLevel"),
    )

    m = RE_COLOR_STRONG.search(text)
    if m:
        return CN_COLOR_TO_EN.get(m.group(1))

    m = RE_COLOR_CODE.search(text)
    if m:
        return _normalise_level_name(m.group(1))

    m = RE_COLOR_SIMPLE.search(text)
    if m:
        return CN_COLOR_TO_EN.get(m.group(1))

    type_code = str(item.get("type") or "").strip()
    m = RE_TYPE_SUFFIX.search(type_code)
    if m:
        return TYPE_SUFFIX_TO_LEVEL.get(m.group(1))

    return None


def _level_is_allowed(level: Optional[str], allowed_levels: Optional[Set[str]]) -> bool:
    # None means no filtering.
    if allowed_levels is None:
        return True
    return bool(level and level in allowed_levels)


def _looks_cancelled(item: Dict[str, Any]) -> bool:
    text = _compact_text(item.get("headline"), item.get("title"))
    return bool(RE_CANCELLED.search(text))


def _province_from_id(item: Dict[str, Any]) -> str:
    iid = item.get("id")
    if isinstance(iid, str):
        m = re.match(r"^(\d{6,})", iid)
        if m:
            code = m.group(1)[:2]
            if code in PROVINCE_CODE_TO_CN:
                return PROVINCE_CODE_TO_CN[code]
    return "全国"


def _region_from_title_or_id(item: Dict[str, Any]) -> str:
    """
    Prefer the specific local name from title/headline, then fall back to province.
    Example: 广东省湛江市徐闻县发布雷雨大风黄色预警 -> 广东省湛江市徐闻县
    """
    for key in ("title", "headline"):
        text = str(item.get(key) or "").strip()
        if not text:
            continue
        m = RE_REGION_BEFORE_ACTION.search(text)
        if not m:
            continue
        region = m.group(1).strip(" ：:，,。")
        region = re.sub(r"(?:气象台|气象局)$", "", region).strip(" ：:，,。")
        if region:
            return region
    return _province_from_id(item)


def _alarm_link_from_id(iid: Optional[Any]) -> Optional[str]:
    """
    CMA detail pages follow:
      https://weather.cma.cn/web/alarm/<id>.html
    """
    if not iid or not isinstance(iid, str):
        return None
    iid = iid.strip()
    if not iid:
        return None
    return f"{BASE_URL}/web/alarm/{iid}.html"

# --------------------------
# Fetch helpers
# --------------------------


async def _fetch_json(client: httpx.AsyncClient, url: str, timeout: float = 15.0) -> Dict[str, Any]:
    resp = await client.get(
        url,
        headers=CMA_HEADERS,
        timeout=timeout,
        follow_redirects=True,
    )
    resp.raise_for_status()

    try:
        payload = resp.json()
    except Exception as exc:
        snippet = re.sub(r"\s+", " ", resp.text[:200])
        raise ValueError(f"Non-JSON response from {url}: {snippet}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected JSON root from {url}: {type(payload).__name__}")

    code = payload.get("code")
    if code not in (None, 0, "0"):
        logging.warning("[CMA] Non-zero response code from %s: %r", url, code)

    return payload


async def _fetch_map_page_channel_links(client: httpx.AsyncClient) -> List[str]:
    """
    Fallback only. The preferred source is INDEX_ALARM_API_URL data.gj.
    """
    resp = await client.get(
        MAP_PAGE_URL,
        headers={**CMA_HEADERS, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
        timeout=15,
        follow_redirects=True,
    )
    resp.raise_for_status()
    links = []
    seen = set()
    for rel in RE_CHANNEL_LINK.findall(resp.text):
        abs_url = _absolute_url(rel)
        if abs_url and abs_url not in seen:
            seen.add(abs_url)
            links.append(abs_url)
    return links

# --------------------------
# Normalisers
# --------------------------


def _normalise_local_alarm(
    item: Dict[str, Any],
    *,
    allowed_levels: Optional[Set[str]],
    now_ts: float,
    source_kind: str,
) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    if _looks_cancelled(item):
        return None

    level = _extract_level(item)
    if not _level_is_allowed(level, allowed_levels):
        return None

    iid = item.get("id")
    headline = str(item.get("headline") or "").strip()
    short_title = str(item.get("title") or "").strip()
    title = headline or short_title or "CMA Alert"
    summary = str(item.get("description") or "").strip()

    published = _parse_pubtime(
        item.get("effective")
        or item.get("pubTime")
        or item.get("publishTime")
        or item.get("releaseTime")
        or item.get("sendTime")
    )
    ts = _timestamp_from_iso(published, now_ts)

    return {
        "source": "CMA",
        "source_kind": source_kind,
        "id": str(iid).strip() if iid else None,
        "headline": headline,
        "title": title,
        "level": level or "Unknown",
        "region": _region_from_title_or_id(item),
        "summary": summary,
        "published": published,
        "timestamp": ts,
        "link": _alarm_link_from_id(iid),
        "longitude": item.get("longitude"),
        "latitude": item.get("latitude"),
        "raw_type": item.get("type"),
    }


def _normalise_national_channel(
    item: Dict[str, Any],
    *,
    allowed_levels: Optional[Set[str]],
    now_ts: float,
) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None

    level = _extract_level(item)
    if not _level_is_allowed(level, allowed_levels):
        return None

    title_text = str(item.get("title") or "").strip()
    description = str(item.get("description") or "").strip()
    title = title_text or description or "CMA National Alert"
    published = _parse_pubtime(
        item.get("releaseTime")
        or item.get("effective")
        or item.get("pubTime")
        or item.get("publishTime")
    )
    ts = _timestamp_from_iso(published, now_ts)

    link = _absolute_url(item.get("link"))
    image = _absolute_url(item.get("image"))

    return {
        "source": "CMA",
        "source_kind": "national_channel",
        "id": f"national:{link or title}:{published or ''}",
        "headline": description or title,
        "title": f"CMA National – {title}",
        "level": level or "National",
        "region": "China National",
        "summary": description,
        "published": published,
        "timestamp": ts,
        "link": link,
        "image": image,
    }


def _normalise_fallback_channel_link(link: str, *, now_ts: float) -> Dict[str, Any]:
    return {
        "source": "CMA",
        "source_kind": "national_channel_fallback",
        "id": f"national:{link}",
        "headline": "CMA national warning channel",
        "title": "CMA National Warning Channel",
        "level": "National",
        "region": "China National",
        "summary": "National channel link extracted from the CMA alarm map page.",
        "published": None,
        "timestamp": now_ts,
        "link": link,
    }

# --------------------------
# De-duplication
# --------------------------


def _entry_key(entry: Dict[str, Any]) -> str:
    eid = entry.get("id")
    if eid:
        return str(eid)
    link = entry.get("link")
    if link:
        return str(link)
    return "|".join(
        str(entry.get(k) or "") for k in ("source_kind", "region", "title", "published")
    )


def _dedupe_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        key = _entry_key(entry)
        if key not in out:
            out[key] = entry
            continue

        # Keep the version with the richer summary/link/image.
        old = out[key]
        if len(str(entry.get("summary") or "")) > len(str(old.get("summary") or "")):
            merged = {**old, **entry}
            out[key] = merged
        else:
            for field in ("link", "image", "longitude", "latitude"):
                if not old.get(field) and entry.get(field):
                    old[field] = entry[field]

    return list(out.values())

# --------------------------
# Main scraper
# --------------------------


async def scrape_cma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    CMA scraper that combines:
      1) Local/provincial active alarms from /api/map/alarm?adcode=
      2) Dynamic national channel links above the map from /api/alarm/newIndexalarm data.gj

    Default behaviour:
      - local map alarms: Red/Orange only
      - national channel links: include whatever CMA currently shows above the map
    """
    entries: List[Dict[str, Any]] = []
    errors: List[str] = []
    now_ts = datetime.now(timezone.utc).timestamp()

    include_local_alarms = _as_bool(_conf_get(conf, "include_local_alarms", True), True)
    include_national_channels = _as_bool(_conf_get(conf, "include_national_channels", True), True)
    include_index_pr = _as_bool(_conf_get(conf, "include_index_pr", True), True)
    html_fallback = _as_bool(_conf_get(conf, "html_fallback", True), True)

    local_allowed_levels = _configured_level_filter(
        conf,
        "allowed_levels",
        DEFAULT_LOCAL_ALLOWED_LEVELS,
    )
    national_allowed_levels = _configured_level_filter(
        conf,
        "national_allowed_levels",
        DEFAULT_NATIONAL_ALLOWED_LEVELS,
    )

    # 1) Local map alarms. Blank adcode means nationwide.
    if include_local_alarms:
        adcodes = _configured_adcodes(conf)
        tasks = [
            _fetch_json(client, MAP_ALARM_API_URL.format(adcode=adcode), timeout=15)
            for adcode in adcodes
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for adcode, result in zip(adcodes, results):
            if isinstance(result, Exception):
                msg = f"map/alarm adcode={adcode!r}: {result}"
                logging.warning("[CMA FETCH ERROR] %s", msg)
                errors.append(msg)
                continue

            alarms = result.get("data") or []
            if not isinstance(alarms, list):
                msg = f"map/alarm adcode={adcode!r}: expected list data, got {type(alarms).__name__}"
                logging.warning("[CMA PARSE ERROR] %s", msg)
                errors.append(msg)
                continue

            for item in alarms:
                try:
                    entry = _normalise_local_alarm(
                        item,
                        allowed_levels=local_allowed_levels,
                        now_ts=now_ts,
                        source_kind="local_map_alarm",
                    )
                    if entry:
                        entries.append(entry)
                except Exception:
                    logging.exception("[CMA PARSE ERROR] local map alarm")

    # 2) Index API. This is the source of the dynamic links above the map.
    index_payload: Optional[Dict[str, Any]] = None
    if include_national_channels or include_index_pr:
        try:
            index_payload = await _fetch_json(client, INDEX_ALARM_API_URL, timeout=15)
        except Exception as exc:
            msg = f"newIndexalarm: {exc}"
            logging.warning("[CMA FETCH ERROR] %s", msg)
            errors.append(msg)

    if index_payload:
        index_data = index_payload.get("data") or {}
        if not isinstance(index_data, dict):
            index_data = {}

        # data.pr is a short list of provincial/current alarms. Keep it as a fallback/supplement;
        # de-duplication removes overlaps with map/alarm by id.
        if include_index_pr:
            pr_items = index_data.get("pr") or []
            if isinstance(pr_items, list):
                for item in pr_items:
                    try:
                        entry = _normalise_local_alarm(
                            item,
                            allowed_levels=local_allowed_levels,
                            now_ts=now_ts,
                            source_kind="province_index_alarm",
                        )
                        if entry:
                            entries.append(entry)
                    except Exception:
                        logging.exception("[CMA PARSE ERROR] province index alarm")

        # data.gj is the dynamically-changing list of national channel links above the map.
        if include_national_channels:
            gj_items = index_data.get("gj") or []
            if isinstance(gj_items, list):
                for item in gj_items:
                    try:
                        entry = _normalise_national_channel(
                            item,
                            allowed_levels=national_allowed_levels,
                            now_ts=now_ts,
                        )
                        if entry:
                            entries.append(entry)
                    except Exception:
                        logging.exception("[CMA PARSE ERROR] national channel")

    # 3) Last-resort HTML fallback for national channel links.
    has_national = any(e.get("source_kind") == "national_channel" for e in entries)
    if include_national_channels and html_fallback and not has_national:
        try:
            for link in await _fetch_map_page_channel_links(client):
                entries.append(_normalise_fallback_channel_link(link, now_ts=now_ts))
        except Exception as exc:
            msg = f"map.html channel fallback: {exc}"
            logging.warning("[CMA FETCH ERROR] %s", msg)
            errors.append(msg)

    entries = _dedupe_entries(entries)
    entries.sort(key=lambda x: float(x.get("timestamp") or 0.0), reverse=True)

    logging.warning(
        "[CMA DEBUG] Parsed %d entries: local=%d national=%d errors=%d",
        len(entries),
        sum(1 for e in entries if str(e.get("source_kind") or "").startswith(("local", "province"))),
        sum(1 for e in entries if str(e.get("source_kind") or "").startswith("national")),
        len(errors),
    )

    result: Dict[str, Any] = {"source": "CMA", "entries": entries}
    if errors:
        result["errors"] = errors
    return result

# --------------------------
# Registry aliases
# --------------------------


async def scrape_async(conf, client):
    return await scrape_cma_async(conf, client)


async def scrape(conf, client):
    return await scrape_cma_async(conf, client)
