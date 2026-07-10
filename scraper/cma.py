# scraper/cma.py
from __future__ import annotations

import asyncio
import html
import logging
import re
from datetime import datetime, timezone, timedelta
from html.parser import HTMLParser
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin, urlparse

import httpx

__all__ = ["scrape_cma_async", "scrape_async", "scrape"]

# ---------------------------------------------------------------------
# National-only CMA / NMC scraper
# ---------------------------------------------------------------------
#
# Goal:
#   Read the national warning list from the top-right warning panel on:
#     https://www.nmc.cn/
#
# It intentionally does NOT fetch local warnings from:
#     https://weather.cma.cn/api/map/alarm?adcode=
#
# That local endpoint is too noisy for your app and is also often WAF-blocked.
# ---------------------------------------------------------------------

NMC_BASE = "https://www.nmc.cn"
NMC_HOME_URL = f"{NMC_BASE}/"
NMC_REFERER = NMC_HOME_URL

DEFAULT_ALLOWED_LEVELS = {"Red", "Orange"}

CST = timezone(timedelta(hours=8))

CN_COLOR_TO_EN = {
    "红色": "Red",
    "橙色": "Orange",
    "黄色": "Yellow",
    "蓝色": "Blue",
}

EN_LEVELS = {"Red", "Orange", "Yellow", "Blue"}

# Handles normal and spaced Chinese color text:
#   红色, 红 色, 橙色, 橙 色, etc.
RE_COLOR = re.compile(r"(红\s*色|橙\s*色|黄\s*色|蓝\s*色)")
RE_WS = re.compile(r"\s+")
RE_TAGS = re.compile(r"<[^>]+>")
RE_SCRIPT_STYLE = re.compile(r"<(script|style)\b.*?</\1>", re.I | re.S)

# Homepage national warning links can point to several NMC product sections.
# These are national products, not local city/province warning-signal pages.
NATIONAL_WARNING_PATH_HINTS = (
    "/publish/country/warning/",
    "/publish/mountainflood.html",
    "/publish/geohazard.html",
    "/publish/waterlogging.html",
    "/publish/swdz/zxhlhsqxyj.html",
    "/publish/nongyeqixiang/quanguonongyeqixiangzaihaifengxianyujing/",
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Referer": NMC_REFERER,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


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

    compact = RE_WS.sub("", text)

    if compact in CN_COLOR_TO_EN:
        return CN_COLOR_TO_EN[compact]

    lowered = compact.lower()
    for level in EN_LEVELS:
        if lowered == level.lower():
            return level

    return None


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
# HTML helpers
# ---------------------------------------------------------------------

class _AnchorCollector(HTMLParser):
    """
    Tiny stdlib-only anchor parser.
    Avoids requiring BeautifulSoup.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: List[Tuple[str, str]] = []
        self._stack: List[Dict[str, Any]] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag.lower() != "a":
            return

        attr_map = {k.lower(): (v or "") for k, v in attrs}
        href = attr_map.get("href", "").strip()

        if not href:
            return

        self._stack.append({"href": href, "text": []})

    def handle_data(self, data: str) -> None:
        if self._stack:
            self._stack[-1]["text"].append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self._stack:
            return

        item = self._stack.pop()
        href = str(item.get("href") or "").strip()
        text = "".join(item.get("text") or [])
        text = _norm_text(text)

        if href and text:
            self.links.append((href, text))


def _norm_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    return RE_WS.sub(" ", text).strip()


def _html_to_text(raw_html: str) -> str:
    cleaned = RE_SCRIPT_STYLE.sub(" ", raw_html or "")
    cleaned = RE_TAGS.sub(" ", cleaned)
    cleaned = html.unescape(cleaned)
    return RE_WS.sub(" ", cleaned).strip()


def _looks_like_bad_response(text: str) -> bool:
    sample = (text or "")[:1000]

    # NMC usually works, but this catches accidental proxy/login/challenge pages.
    bad_markers = (
        "WEB 应用防火墙",
        "人机识别检测",
        "向右滑动填充拼图",
        "captcha",
        "js-challenge",
    )

    return any(marker in sample for marker in bad_markers)


async def _get_text(
    client: httpx.AsyncClient,
    url: str,
    *,
    timeout: float,
    referer: str = NMC_REFERER,
) -> str:
    headers = {**HEADERS, "Referer": referer}
    resp = await client.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()

    text = resp.text or ""

    if _looks_like_bad_response(text):
        sample = text[:220].replace("\n", " ").replace("\r", " ")
        raise RuntimeError(f"NMC returned challenge/WAF HTML for {url}: {sample}")

    return text


# ---------------------------------------------------------------------
# Severity and time parsing
# ---------------------------------------------------------------------

def _extract_level(text_or_item: Any) -> Optional[str]:
    if isinstance(text_or_item, dict):
        text = " ".join(
            _norm_text(v)
            for v in (
                text_or_item.get("title"),
                text_or_item.get("headline"),
                text_or_item.get("summary"),
                text_or_item.get("description"),
                text_or_item.get("level"),
            )
            if v
        )
    else:
        text = _norm_text(text_or_item)

    compact = RE_WS.sub("", text)

    m = RE_COLOR.search(compact)
    if not m:
        return None

    color = RE_WS.sub("", m.group(1))
    return CN_COLOR_TO_EN.get(color)


def _parse_pubtime_from_text(text: str) -> Optional[str]:
    """
    Parse NMC national times such as:
      2026 年 07 月 10 日 18 时
      7月10日18时
    as China Standard Time, then return UTC ISO.
    """
    text = _norm_text(text)
    now_cst = datetime.now(CST)

    m = re.search(
        r"(?P<y>\d{4})\s*年\s*"
        r"(?P<m>\d{1,2})\s*月\s*"
        r"(?P<d>\d{1,2})\s*日\s*"
        r"(?P<h>\d{1,2})\s*时",
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
        r"(?P<m>\d{1,2})\s*月\s*"
        r"(?P<d>\d{1,2})\s*日\s*"
        r"(?P<h>\d{1,2})\s*时",
        text,
    )
    if m:
        local_dt = datetime(
            now_cst.year,
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
# National warning extraction
# ---------------------------------------------------------------------

def _absolute_nmc_url(href: str) -> str:
    return urljoin(NMC_BASE, href)


def _is_nmc_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False

    return parsed.netloc in {"www.nmc.cn", "nmc.cn", ""}


def _is_relevant_national_warning_url(url: str) -> bool:
    if not _is_nmc_url(url):
        return False

    parsed = urlparse(url)
    path = parsed.path

    if not path.endswith(".html") and not path.endswith("/"):
        return False

    return any(hint in path for hint in NATIONAL_WARNING_PATH_HINTS)


def _clean_homepage_title(text: str) -> str:
    """
    Homepage link text may include a badge like '预警' and a relative age.
    Keep the actual warning title readable.
    """
    text = _norm_text(text)

    # Remove common badge prefix duplicated into anchor text.
    text = re.sub(r"^(预警|警报|快讯)\s+", "", text)

    # Remove trailing relative time, if it appears inside the anchor text.
    text = re.sub(r"\s*\d+\s*(分钟前|小时前|天前)\s*$", "", text)

    return _norm_text(text)


def _extract_detail_summary(text: str, fallback_title: str) -> str:
    """
    Extract a compact useful paragraph from a detail page.
    """
    text = _norm_text(text)

    # Try to start around the official issuing sentence.
    patterns = (
        r"(中央气象台.*?预警[:：].*?)(?:防御指南|相关产品|推荐服务|$)",
        r"(水利部和中国气象局.*?预警[:：].*?)(?:相关产品|推荐服务|$)",
        r"(自然资源部与中国气象局.*?预警[:：].*?)(?:相关产品|推荐服务|$)",
        r"(.*?发布.*?预警[:：].*?)(?:防御指南|相关产品|推荐服务|$)",
    )

    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            return _norm_text(m.group(1))[:900]

    # Fallback: a small window around the title.
    idx = text.find(fallback_title[:20]) if fallback_title else -1
    if idx >= 0:
        return _norm_text(text[idx: idx + 900])

    return ""


def _homepage_entries_from_html(
    raw_html: str,
    *,
    allowed_levels: Set[str],
    now_ts: float,
) -> List[Dict[str, Any]]:
    parser = _AnchorCollector()
    parser.feed(raw_html)

    entries: List[Dict[str, Any]] = []
    seen_links: Set[str] = set()

    for order, (href, raw_title) in enumerate(parser.links):
        url = _absolute_nmc_url(href)

        if not _is_relevant_national_warning_url(url):
            continue

        title = _clean_homepage_title(raw_title)
        if not title:
            continue

        # This is the key filter:
        # keep only top-list national warnings that explicitly say Red/Orange.
        level = _extract_level(title)
        if level not in allowed_levels:
            continue

        # Avoid nav/menu duplicates and repeated page links.
        # For the homepage top-right list, the full title includes the issuing
        # sentence, while nav links usually do not include a color level.
        if url in seen_links:
            continue

        seen_links.add(url)

        published = _parse_pubtime_from_text(title)
        ts = _timestamp_from_iso(published, now_ts)

        entries.append(
            {
                "source": "CMA/NMC",
                "source_kind": "national",
                "id": url,
                "headline": title,
                "title": title,
                "level": level,
                "region": "China: National",
                "summary": "",
                "published": published,
                "timestamp": ts,
                "link": url,
                "_order": order,
            }
        )

    return entries


async def _enrich_entry_from_detail_page(
    client: httpx.AsyncClient,
    entry: Dict[str, Any],
    *,
    timeout: float,
) -> Dict[str, Any]:
    """
    Fetch detail page for better summary/published time.
    If anything fails, keep the homepage-derived entry.
    """
    url = str(entry.get("link") or "")
    if not url:
        return entry

    try:
        raw_html = await _get_text(client, url, timeout=timeout, referer=NMC_HOME_URL)
    except Exception as exc:
        logging.debug("[CMA/NMC DETAIL] Could not fetch %s: %s", url, exc)
        return entry

    text = _html_to_text(raw_html)

    if not text:
        return entry

    detail_level = _extract_level(text)
    if detail_level in EN_LEVELS:
        entry["level"] = detail_level

    detail_published = _parse_pubtime_from_text(text)
    if detail_published:
        entry["published"] = detail_published
        entry["timestamp"] = _timestamp_from_iso(
            detail_published,
            float(entry.get("timestamp") or 0.0),
        )

    summary = _extract_detail_summary(text, str(entry.get("title") or ""))
    if summary:
        entry["summary"] = summary

    return entry


def _entry_key(entry: Dict[str, Any]) -> str:
    return str(entry.get("link") or entry.get("id") or entry.get("title") or "")


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


def _sort_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Newest first, but preserve homepage order when timestamps tie.
    return sorted(
        entries,
        key=lambda e: (
            float(e.get("timestamp") or 0.0),
            -int(e.get("_order") or 0),
        ),
        reverse=True,
    )


# ---------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------

async def scrape_cma_async(
    conf: Dict[str, Any],
    client: httpx.AsyncClient,
) -> Dict[str, Any]:
    """
    National-only CMA/NMC scraper.

    It reads the warning links shown in the top-right panel of:
      https://www.nmc.cn/

    Default:
      allowed_levels = ["Red", "Orange"]

    Optional config:
      {
        "allowed_levels": ["Red", "Orange"],
        "timeout": 15,
        "fetch_detail_pages": true
      }
    """
    timeout = float(_conf_value(conf, "timeout", 15) or 15)
    allowed_levels = _allowed_levels_from_conf(conf)
    fetch_detail_pages = _conf_bool(conf, "fetch_detail_pages", True)

    now_ts = datetime.now(timezone.utc).timestamp()
    errors: List[str] = []

    try:
        homepage_html = await _get_text(
            client,
            NMC_HOME_URL,
            timeout=timeout,
            referer=NMC_HOME_URL,
        )
    except Exception as exc:
        logging.exception("[CMA/NMC FETCH ERROR] homepage")
        return {
            "source": "CMA/NMC",
            "entries": [],
            "error": f"homepage: {exc}",
        }

    entries = _homepage_entries_from_html(
        homepage_html,
        allowed_levels=allowed_levels,
        now_ts=now_ts,
    )

    if fetch_detail_pages and entries:
        tasks = [
            _enrich_entry_from_detail_page(
                client,
                entry,
                timeout=timeout,
            )
            for entry in entries
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        enriched: List[Dict[str, Any]] = []
        for entry, result in zip(entries, results):
            if isinstance(result, Exception):
                logging.debug("[CMA/NMC DETAIL ERROR] %s", result)
                enriched.append(entry)
                continue

            enriched.append(result)

        entries = enriched

    # Keep only configured levels after enrichment.
    entries = [
        entry
        for entry in entries
        if entry.get("level") in allowed_levels
    ]

    entries = _dedupe_entries(entries)
    entries = _sort_entries(entries)

    logging.warning(
        "[CMA/NMC DEBUG] Parsed %d national entries; allowed_levels=%s",
        len(entries),
        sorted(allowed_levels),
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
