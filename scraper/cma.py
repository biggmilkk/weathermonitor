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
# What this scraper does:
#   1. Fetches https://www.nmc.cn/
#   2. Reads the national warning links shown in the top-right warning list.
#   3. Keeps Red / Orange / Yellow national warnings by default.
#   4. Fetches each detail page and extracts the full article body.
#
# What this scraper intentionally does NOT do:
#   - It does not fetch local warnings from:
#       https://weather.cma.cn/api/map/alarm?adcode=
#   - It does not include local province/city warning-signal records.
#   - It does not include Blue warnings unless configured.
# ---------------------------------------------------------------------

NMC_BASE = "https://www.nmc.cn"
NMC_HOME_URL = f"{NMC_BASE}/"
NMC_REFERER = NMC_HOME_URL

# Updated default: include Yellow too.
DEFAULT_ALLOWED_LEVELS = {"Red", "Orange", "Yellow"}

CST = timezone(timedelta(hours=8))

CN_COLOR_TO_EN = {
    "红色": "Red",
    "橙色": "Orange",
    "黄色": "Yellow",
    "蓝色": "Blue",
}

EN_LEVELS = {"Red", "Orange", "Yellow", "Blue"}

# National warning/product paths that can appear in the homepage top-right list.
# Local warning-signal pages use different paths and are intentionally ignored.
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

RE_WS = re.compile(r"\s+")
RE_TAGS = re.compile(r"<[^>]+>")
RE_SCRIPT_STYLE = re.compile(r"<(script|style)\b.*?</\1>", re.I | re.S)
RE_COLOR = re.compile(r"(红\s*色|橙\s*色|黄\s*色|蓝\s*色)")
RE_RELATIVE_AGE = re.compile(r"\s*\d+\s*(分钟前|小时前|天前)\s*$")

WAF_MARKERS = (
    "WEB 应用防火墙",
    "人机识别检测",
    "向右滑动填充拼图",
    "captcha",
    "js-challenge",
)


# ---------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------

def _conf_value(conf: Dict[str, Any], key: str, default: Any = None) -> Any:
    """
    Support both:
      {"allowed_levels": [...]}
    and:
      {"conf": {"allowed_levels": [...]}}
    """
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
# General text / HTML helpers
# ---------------------------------------------------------------------

def _norm_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = text.replace("\xa0", " ")
    return RE_WS.sub(" ", text).strip()


def _looks_like_bad_response(text: str) -> bool:
    sample = (text or "")[:1000]
    return any(marker in sample for marker in WAF_MARKERS)


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


def _html_to_text(raw_html: str) -> str:
    """
    Convert NMC HTML to readable text while preserving useful paragraph breaks.
    """
    if not raw_html:
        return ""

    s = RE_SCRIPT_STYLE.sub("\n", raw_html)

    # Preserve common block boundaries before removing tags.
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"(?i)</(?:p|div|li|tr|td|h1|h2|h3|h4|h5|h6)>", "\n", s)
    s = re.sub(r"(?i)<(?:p|div|li|tr|td|h1|h2|h3|h4|h5|h6)\b[^>]*>", "\n", s)

    s = RE_TAGS.sub(" ", s)
    s = html.unescape(s)
    s = s.replace("\xa0", " ")

    lines: List[str] = []
    for line in s.splitlines():
        line = _norm_text(line)
        if line:
            lines.append(line)

    return "\n".join(lines)


def _repair_nmc_spacing(value: Any) -> str:
    """
    NMC article pages often expose text with odd spaces inserted between
    Chinese characters and digits, for example:
      台风 橙色 预警
      第 9 号
      83 0 公里
      20～2 5 公里
      250～9 00毫米

    This normalizes those display artifacts.
    """
    text = _norm_text(value)
    if not text:
        return ""

    # Remove spaces between Chinese characters.
    text = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", text)

    # Remove spaces between Chinese text and numbers.
    text = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=\d)", "", text)
    text = re.sub(r"(?<=\d)\s+(?=[\u4e00-\u9fff])", "", text)

    # Remove spaces inside numbers and ranges.
    text = re.sub(r"(?<=\d)\s+(?=\d)", "", text)
    text = re.sub(r"(?<=\d)\s+(?=[～~\-—])", "", text)
    text = re.sub(r"(?<=[～~\-—])\s+(?=\d)", "", text)

    # Remove spaces around Chinese punctuation.
    text = re.sub(r"\s+([，。！？、：；）】》])", r"\1", text)
    text = re.sub(r"([（【《])\s+", r"\1", text)

    # Tighten common numeric units.
    text = re.sub(
        r"(?<=\d)\s+(?=[年月日时分秒点号级度米公里百帕毫米公里/秒])",
        "",
        text,
    )

    return text.strip()


def _clean_article_text(text: str) -> str:
    """
    Normalize article text but preserve paragraph breaks.
    """
    lines: List[str] = []

    for line in (text or "").splitlines():
        cleaned = _repair_nmc_spacing(line)
        if cleaned:
            lines.append(cleaned)

    return "\n".join(lines).strip()


# ---------------------------------------------------------------------
# Anchor parsing
# ---------------------------------------------------------------------

class _AnchorCollector(HTMLParser):
    """
    stdlib-only anchor parser. It collects all <a href="...">text</a> pairs.
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


# ---------------------------------------------------------------------
# Severity and time parsing
# ---------------------------------------------------------------------

def _extract_level(text_or_item: Any) -> Optional[str]:
    """
    Extract Red / Orange / Yellow / Blue from Chinese warning text.
    """
    if isinstance(text_or_item, dict):
        text = " ".join(
            _norm_text(v)
            for v in (
                text_or_item.get("title"),
                text_or_item.get("headline"),
                text_or_item.get("summary"),
                text_or_item.get("description"),
                text_or_item.get("body"),
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
    Parse NMC issue times such as:
      2026年07月10日18时
      7月10日18时

    The parsed time is treated as China Standard Time and returned as UTC ISO.
    """
    clean = _repair_nmc_spacing(text)
    now_cst = datetime.now(CST)

    m = re.search(
        r"(?P<y>\d{4})年"
        r"(?P<m>\d{1,2})月"
        r"(?P<d>\d{1,2})日"
        r"(?P<h>\d{1,2})时",
        clean,
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
        r"(?P<m>\d{1,2})月"
        r"(?P<d>\d{1,2})日"
        r"(?P<h>\d{1,2})时",
        clean,
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
# Homepage national-warning extraction
# ---------------------------------------------------------------------

def _absolute_nmc_url(href: str) -> str:
    return urljoin(NMC_BASE, href)


def _is_nmc_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False

    return parsed.netloc in {"", "www.nmc.cn", "nmc.cn"}


def _is_relevant_national_warning_url(url: str) -> bool:
    if not _is_nmc_url(url):
        return False

    path = urlparse(url).path or ""

    if not path:
        return False

    return any(hint in path for hint in NATIONAL_WARNING_PATH_HINTS)


def _clean_homepage_title(text: str) -> str:
    """
    Homepage link text usually looks like:
      预警 中央气象台7月10日18时继续发布台风橙色预警 6小时前

    This removes the UI badge and trailing relative age.
    """
    title = _norm_text(text)
    title = re.sub(r"^(预警|警报|快讯)\s+", "", title)
    title = RE_RELATIVE_AGE.sub("", title)
    return _repair_nmc_spacing(title)


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

        level = _extract_level(title)
        if level not in allowed_levels:
            continue

        # Avoid repeated national category/menu links.
        # Menu links normally do not contain color levels, but this also
        # protects against duplicated homepage modules.
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
                "description": "",
                "body": "",
                "published": published,
                "timestamp": ts,
                "link": url,
                "_order": order,
            }
        )

    return entries


# ---------------------------------------------------------------------
# Detail-page article extraction
# ---------------------------------------------------------------------

def _find_article_start(clean: str, allowed_levels: Set[str]) -> int:
    """
    Locate the real article body, not the nav/menu text.

    Typical starts:
      中央气象台7月10日18时继续发布台风橙色预警：
      水利部和中国气象局7月10日18时联合发布红色山洪灾害气象预警：
      自然资源部与中国气象局7月10日18时联合发布橙色地质灾害气象风险预警：
    """
    if not clean:
        return -1

    allowed_cn_colors = []
    for cn, en in CN_COLOR_TO_EN.items():
        if en in allowed_levels:
            allowed_cn_colors.append(cn)

    color_alt = "|".join(re.escape(c) for c in allowed_cn_colors) or r"红色|橙色|黄色"
    issuer_alt = (
        r"中央气象台|"
        r"水利部和中国气象局|"
        r"自然资源部与中国气象局|"
        r"农业农村部和中国气象局|"
        r"国家防总办公室、应急管理部和中国气象局|"
        r"中国气象局"
    )

    patterns = (
        rf"(?:{issuer_alt})[^\n]{{0,140}}?(?:继续发布|联合发布|发布)"
        rf"[^\n]{{0,100}}?(?:{color_alt})[^\n]{{0,40}}?(?:预警|预报)[:：]",
        rf"[^\n]{{0,120}}?(?:继续发布|联合发布|发布)"
        rf"[^\n]{{0,100}}?(?:{color_alt})[^\n]{{0,40}}?(?:预警|预报)[:：]",
    )

    matches = []
    for pattern in patterns:
        for match in re.finditer(pattern, clean):
            matches.append(match)

    if not matches:
        return -1

    # Use the last match before the article end area. This avoids breadcrumb/menu
    # fragments and works well on NMC detail pages.
    return max(match.start() for match in matches)


def _find_article_end(clean: str, start: int) -> int:
    if start < 0:
        return len(clean)

    markers = (
        "\n防御指南",
        "防御指南：",
        "\n相关产品",
        "\n推荐服务",
        "\n国家气象中心版权所有",
        "国家气象中心 版权所有",
        "本站所刊登的信息",
    )

    candidates: List[int] = []

    for marker in markers:
        pos = clean.find(marker, start + 1)
        if pos > start:
            candidates.append(pos)

    return min(candidates) if candidates else len(clean)


def _fallback_start_from_title(clean: str, fallback_title: str) -> int:
    title = _repair_nmc_spacing(fallback_title)

    if not title:
        return -1

    # Try the first meaningful chunk of the homepage title.
    chunks = [
        title,
        title.replace("继续发布", "发布"),
    ]

    for chunk in chunks:
        short = chunk[:30]
        if short:
            pos = clean.find(short)
            if pos >= 0:
                return pos

    # Last fallback: look for the first line containing a colored warning.
    for match in re.finditer(r"[^\n]*(红色|橙色|黄色)[^\n]*(预警|预报)[：:]", clean):
        return match.start()

    return -1


def _extract_detail_article(
    detail_text: str,
    *,
    fallback_title: str,
    allowed_levels: Set[str],
) -> str:
    """
    Extract the full national warning body from an NMC detail page.

    The returned text intentionally excludes the 防御指南 section and footer.
    """
    clean = _clean_article_text(detail_text)
    if not clean:
        return ""

    start = _find_article_start(clean, allowed_levels)

    if start < 0:
        start = _fallback_start_from_title(clean, fallback_title)

    if start < 0:
        logging.warning(
            "[CMA/NMC DETAIL] Could not locate article start. "
            "title=%r sample=%r",
            fallback_title,
            clean[:400],
        )
        return ""

    end = _find_article_end(clean, start)
    if end <= start:
        end = len(clean)

    article = clean[start:end].strip()

    # Remove repeated small section headers if they appear immediately before body.
    article = re.sub(
        r"^(台风预警|暴雨预警|强对流天气预警|地质灾害气象风险预警|"
        r"山洪灾害气象预警|中小河流洪水气象风险预警|农业气象灾害风险预警)\s*",
        "",
        article,
    ).strip()

    # Keep enough text for the full warning body, but avoid accidentally
    # storing a full page/footer if extraction changes.
    return article[:8000].strip()


async def _enrich_entry_from_detail_page(
    client: httpx.AsyncClient,
    entry: Dict[str, Any],
    *,
    timeout: float,
    allowed_levels: Set[str],
) -> Dict[str, Any]:
    """
    Fetch the detail page and populate summary / description / body.
    If enrichment fails, keep the homepage-derived entry.
    """
    url = str(entry.get("link") or "").strip()
    title = str(entry.get("title") or "").strip()

    if not url:
        return entry

    try:
        raw_html = await _get_text(
            client,
            url,
            timeout=timeout,
            referer=NMC_HOME_URL,
        )
    except Exception as exc:
        logging.warning("[CMA/NMC DETAIL] Could not fetch %s: %s", url, exc)
        return entry

    detail_text = _html_to_text(raw_html)
    if not detail_text:
        logging.warning("[CMA/NMC DETAIL] Empty detail text for %s", url)
        return entry

    article = _extract_detail_article(
        detail_text,
        fallback_title=title,
        allowed_levels=allowed_levels,
    )

    if article:
        entry["summary"] = article
        entry["description"] = article
        entry["body"] = article
        logging.warning(
            "[CMA/NMC DETAIL] Enriched %r summary_len=%d",
            title,
            len(article),
        )
    else:
        logging.warning(
            "[CMA/NMC DETAIL] Detail fetched but no article extracted: %s title=%r",
            url,
            title,
        )

    detail_level = _extract_level(article or detail_text)
    if detail_level in EN_LEVELS:
        entry["level"] = detail_level

    detail_published = _parse_pubtime_from_text(article or detail_text)
    if detail_published:
        entry["published"] = detail_published
        entry["timestamp"] = _timestamp_from_iso(
            detail_published,
            float(entry.get("timestamp") or 0.0),
        )

    return entry


# ---------------------------------------------------------------------
# Dedupe / sorting
# ---------------------------------------------------------------------

def _entry_key(entry: Dict[str, Any]) -> str:
    return str(entry.get("link") or entry.get("id") or entry.get("title") or "")


def _dedupe_entries(entries: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    out: List[Dict[str, Any]] = []

    for entry in entries:
        key = _entry_key(entry)
        if not key:
            key = "|".join(
                str(entry.get(k) or "")
                for k in ("source_kind", "region", "title", "published")
            )

        if key in seen:
            continue

        seen.add(key)
        out.append(entry)

    return out


def _sort_entries(entries: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Newest first. Preserve homepage order for warnings with the same timestamp.
    """
    return sorted(
        entries,
        key=lambda e: (
            -float(e.get("timestamp") or 0.0),
            int(e.get("_order") or 999999),
        ),
    )


# ---------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------

async def scrape_cma_async(
    conf: Dict[str, Any],
    client: httpx.AsyncClient,
) -> Dict[str, Any]:
    """
    National-only CMA/NMC warning scraper.

    Recommended feed config:

      {
          "key": "cma_china",
          "type": "rss_cma",
          "label": "CMA China",
          "group": "g2_even",
          "conf": {
              "allowed_levels": ["Red", "Orange", "Yellow"],
              "fetch_detail_pages": true,
              "timeout": 15
          }
      }

    Optional config keys:
      allowed_levels: ["Red", "Orange", "Yellow"]  # default
      fetch_detail_pages: true                     # default
      timeout: 15                                  # default seconds
    """
    timeout = float(_conf_value(conf, "timeout", 15) or 15)
    allowed_levels = _allowed_levels_from_conf(conf)
    fetch_detail_pages = _conf_bool(conf, "fetch_detail_pages", True)

    now_ts = datetime.now(timezone.utc).timestamp()

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
                allowed_levels=allowed_levels,
            )
            for entry in entries
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        enriched: List[Dict[str, Any]] = []
        for entry, result in zip(entries, results):
            if isinstance(result, Exception):
                logging.warning("[CMA/NMC DETAIL ERROR] %s", result)
                enriched.append(entry)
            else:
                enriched.append(result)

        entries = enriched

    # Final threshold check after detail enrichment.
    entries = [
        entry
        for entry in entries
        if entry.get("level") in allowed_levels
    ]

    entries = _dedupe_entries(entries)
    entries = _sort_entries(entries)

    # Remove internal ordering helper before handing entries to the renderer.
    for entry in entries:
        entry.pop("_order", None)

    logging.warning(
        "[CMA/NMC DEBUG] Parsed %d national entries; allowed_levels=%s; detail_pages=%s",
        len(entries),
        sorted(allowed_levels),
        fetch_detail_pages,
    )

    return {
        "source": "CMA/NMC",
        "entries": entries,
    }


# ---------------------------------------------------------------------
# Registry aliases
# ---------------------------------------------------------------------

async def scrape_async(conf, client):
    return await scrape_cma_async(conf, client)


async def scrape(conf, client):
    return await scrape_cma_async(conf, client)
