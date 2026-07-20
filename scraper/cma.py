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
# Source:
#   https://www.nmc.cn/
#
# What this scraper does:
#   1. Fetches the NMC homepage.
#   2. Reads ONLY active national-warning product links from the homepage
#      warning panel.
#   3. Keeps Red / Orange / Yellow national warnings by default.
#   4. Rejects stale news/interview/CMS links such as:
#        /publish/cms/view/....html
#   5. Fetches each accepted product detail page and extracts the full
#      article body when available.
#
# What this scraper intentionally does NOT do:
#   - It does not fetch local warnings from weather.cma.cn.
#   - It does not include local province/city warning-signal records.
#   - It does not include Blue warnings unless configured.
# ---------------------------------------------------------------------

NMC_BASE = "https://www.nmc.cn"
NMC_HOME_URL = f"{NMC_BASE}/"
NMC_REFERER = NMC_HOME_URL

DEFAULT_ALLOWED_LEVELS = {"Red", "Orange", "Yellow"}

CST = timezone(timedelta(hours=8))

CN_COLOR_TO_EN = {
    "红色": "Red",
    "橙色": "Orange",
    "黄色": "Yellow",
    "蓝色": "Blue",
}

EN_LEVELS = {"Red", "Orange", "Yellow", "Blue"}

# Exact active national-warning product pages.
# Do NOT use broad "/publish/" matching, because the homepage also contains
# stale CMS/news/interview pages that can include warning words.
PRODUCT_BY_PATH: Dict[str, Dict[str, Any]] = {
    "/publish/country/warning/strong_convection.html": {
        "hazard_cn": "强对流天气",
        "hazard_en": "Severe Convective Weather",
        "aliases": ["强对流天气", "强对流"],
    },
    "/publish/country/warning/downpour.html": {
        "hazard_cn": "暴雨",
        "hazard_en": "Heavy Rain",
        "aliases": ["暴雨"],
    },
    "/publish/country/warning/typhoon.html": {
        "hazard_cn": "台风",
        "hazard_en": "Typhoon",
        "aliases": ["台风"],
    },
    "/publish/country/warning/gale.html": {
        "hazard_cn": "大风",
        "hazard_en": "Gale",
        "aliases": ["大风"],
    },
    "/publish/country/warning/fog.html": {
        "hazard_cn": "大雾",
        "hazard_en": "Heavy Fog",
        "aliases": ["大雾", "雾"],
    },
    "/publish/country/warning/sand.html": {
        "hazard_cn": "沙尘暴",
        "hazard_en": "Sandstorm",
        "aliases": ["沙尘暴", "沙尘"],
    },
    "/publish/country/warning/snow.html": {
        "hazard_cn": "暴雪",
        "hazard_en": "Snowstorm",
        "aliases": ["暴雪"],
    },
    "/publish/country/warning/cold.html": {
        "hazard_cn": "寒潮",
        "hazard_en": "Cold Wave",
        "aliases": ["寒潮"],
    },
    "/publish/country/warning/freeze.html": {
        "hazard_cn": "冰冻",
        "hazard_en": "Freezing",
        "aliases": ["冰冻"],
    },
    "/publish/country/warning/high-temperature.html": {
        "hazard_cn": "高温",
        "hazard_en": "High Temperature",
        "aliases": ["高温"],
    },
    "/publish/country/warning/drought.html": {
        "hazard_cn": "气象干旱",
        "hazard_en": "Meteorological Drought",
        "aliases": ["气象干旱", "干旱"],
    },
    "/publish/country/warning/low-temperature.html": {
        "hazard_cn": "低温",
        "hazard_en": "Low Temperature",
        "aliases": ["低温"],
    },
    "/publish/mountainflood.html": {
        "hazard_cn": "山洪灾害",
        "hazard_en": "Mountain Flood Risk",
        "aliases": ["山洪灾害气象", "山洪灾害", "山洪"],
    },
    "/publish/geohazard.html": {
        "hazard_cn": "地质灾害",
        "hazard_en": "Geological Hazard Risk",
        "aliases": ["地质灾害气象风险", "地质灾害"],
    },
    "/publish/swdz/zxhlhsqxyj.html": {
        "hazard_cn": "中小河流洪水",
        "hazard_en": "Small and Medium River Flood Risk",
        "aliases": ["中小河流洪水气象风险", "中小河流洪水"],
    },
    "/publish/waterlogging.html": {
        "hazard_cn": "渍涝",
        "hazard_en": "Waterlogging Risk",
        "aliases": ["渍涝风险气象", "渍涝"],
    },
    "/publish/nongyeqixiang/quanguonongyeqixiangzaihaifengxianyujing/": {
        "hazard_cn": "农业气象灾害",
        "hazard_en": "Agrometeorological Hazard Risk",
        "aliases": ["农业气象灾害风险", "农业气象灾害"],
    },
}

NATIONAL_ISSUER_PATTERN = (
    r"中央气象台|"
    r"水利部和中国气象局|"
    r"自然资源部与中国气象局|"
    r"农业农村部和中国气象局|"
    r"国家防总办公室、应急管理部和中国气象局|"
    r"中国气象局"
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
    Prefer nested feed config:
      {"conf": {"allowed_levels": [...]}}
    over stale top-level keys:
      {"allowed_levels": [...]}
    """
    nested = conf.get("conf") if isinstance(conf.get("conf"), dict) else {}

    if isinstance(nested, dict) and key in nested:
        return nested.get(key)

    if key in conf:
        return conf.get(key)

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
    Normalize NMC spacing artifacts:
      台风 黄 色 预警 -> 台风黄色预警
      第 9 号 -> 第9号
    """
    text = _norm_text(value)
    if not text:
        return ""

    text = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", text)
    text = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=\d)", "", text)
    text = re.sub(r"(?<=\d)\s+(?=[\u4e00-\u9fff])", "", text)
    text = re.sub(r"(?<=\d)\s+(?=\d)", "", text)
    text = re.sub(r"(?<=\d)\s+(?=[～~\-—])", "", text)
    text = re.sub(r"(?<=[～~\-—])\s+(?=\d)", "", text)
    text = re.sub(r"\s+([，。！？、：；）】》])", r"\1", text)
    text = re.sub(r"([（【《])\s+", r"\1", text)

    text = re.sub(
        r"(?<=\d)\s+(?=[年月日时分秒点号级度米公里百帕毫米公里/秒])",
        "",
        text,
    )

    return text.strip()


def _clean_article_text(text: str) -> str:
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
# URL / product helpers
# ---------------------------------------------------------------------

def _absolute_nmc_url(href: str) -> str:
    return urljoin(NMC_BASE, href)


def _is_nmc_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False

    return parsed.netloc in {"", "www.nmc.cn", "nmc.cn"}


def _product_for_url(url: str) -> Optional[Dict[str, Any]]:
    """
    Return the active national product definition for an NMC URL.

    This intentionally rejects /publish/cms/view/... and other news/interview
    articles, even if their titles contain warning colors.
    """
    if not _is_nmc_url(url):
        return None

    path = urlparse(url).path or ""

    if "/publish/cms/view/" in path:
        return None

    product = PRODUCT_BY_PATH.get(path)
    if product:
        return product

    # The agricultural warning path may behave like a directory.
    for known_path, known_product in PRODUCT_BY_PATH.items():
        if known_path.endswith("/") and path.startswith(known_path):
            return known_product

    return None


def _is_active_warning_list_title(raw_title: str) -> bool:
    """
    The active top warning panel uses a visible badge like:
      预警 中央气象台7月20日06时继续发布暴雨蓝色预警 3小时前

    News/interview titles usually do not start with this badge.
    """
    raw = _repair_nmc_spacing(raw_title)
    return raw.startswith("预警")


# ---------------------------------------------------------------------
# Severity and time parsing
# ---------------------------------------------------------------------

def _extract_first_level(text_or_item: Any) -> Optional[str]:
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


def _extract_product_level(text: str, product: Dict[str, Any]) -> Optional[str]:
    """
    Extract the level for this specific product.

    Example:
      title = 暴雨黄色和强对流天气蓝色预警
      product = 暴雨             -> Yellow
      product = 强对流天气       -> Blue

    This prevents a Blue product from passing just because the same title
    also mentions a Yellow product.
    """
    compact = _repair_nmc_spacing(text)
    compact = RE_WS.sub("", compact)

    aliases = product.get("aliases") or [product.get("hazard_cn")]

    for alias in aliases:
        alias = str(alias or "").strip()
        if not alias:
            continue

        # Product immediately before color, allowing short connective text.
        # This handles:
        #   暴雨黄色预警
        #   暴雨黄色和强对流天气蓝色预警
        #   地质灾害气象风险橙色预警
        pattern = rf"{re.escape(alias)}[^红橙黄蓝]{{0,16}}(红色|橙色|黄色|蓝色)"
        m = re.search(pattern, compact)
        if m:
            color = RE_WS.sub("", m.group(1))
            return CN_COLOR_TO_EN.get(color)

    # Fallback only when the text contains exactly one level.
    found: List[str] = []
    for m in RE_COLOR.finditer(compact):
        color = RE_WS.sub("", m.group(1))
        level = CN_COLOR_TO_EN.get(color)
        if level and level not in found:
            found.append(level)

    if len(found) == 1:
        return found[0]

    return None


def _parse_pubtime_from_text(text: str) -> Optional[str]:
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

def _clean_homepage_title(text: str) -> str:
    title = _norm_text(text)
    title = re.sub(r"^预警\s*", "", title)
    title = re.sub(r"^(警报|快讯)\s+", "", title)
    title = RE_RELATIVE_AGE.sub("", title)
    return _repair_nmc_spacing(title)


def _make_homepage_entry(
    *,
    title: str,
    url: str,
    level: str,
    product: Dict[str, Any],
    order: int,
    now_ts: float,
) -> Dict[str, Any]:
    published = _parse_pubtime_from_text(title)
    ts = _timestamp_from_iso(published, now_ts)

    return {
        "source": "CMA/NMC",
        "source_kind": "national",
        "id": url,
        "headline": title,
        "title": title,
        "level": level,
        "hazard_cn": product.get("hazard_cn"),
        "hazard_en": product.get("hazard_en"),
        "region": "China: National",
        "summary": "",
        "description": "",
        "body": "",
        "published": published,
        "timestamp": ts,
        "link": url,
        "_order": order,
    }


def _homepage_entries_from_html(
    raw_html: str,
    *,
    allowed_levels: Set[str],
    now_ts: float,
    require_active_badge: bool = True,
) -> List[Dict[str, Any]]:
    parser = _AnchorCollector()
    parser.feed(raw_html)

    entries: List[Dict[str, Any]] = []
    seen_links: Set[str] = set()

    for order, (href, raw_title) in enumerate(parser.links):
        url = _absolute_nmc_url(href)
        product = _product_for_url(url)

        if not product:
            continue

        if require_active_badge and not _is_active_warning_list_title(raw_title):
            # Reject nav links and stale page titles outside the active panel.
            continue

        title = _clean_homepage_title(raw_title)
        if not title:
            continue

        level = _extract_product_level(title, product)
        if level not in allowed_levels:
            continue

        if url in seen_links:
            continue
        seen_links.add(url)

        entries.append(
            _make_homepage_entry(
                title=title,
                url=url,
                level=level,
                product=product,
                order=order,
                now_ts=now_ts,
            )
        )

    logging.warning(
        "[CMA/NMC DEBUG] homepage_active_candidates=%s",
        [(e.get("level"), e.get("hazard_cn"), e.get("title"), e.get("link")) for e in entries],
    )

    return entries


# ---------------------------------------------------------------------
# Detail-page article extraction
# ---------------------------------------------------------------------

def _find_article_start(
    clean: str,
    *,
    product: Dict[str, Any],
) -> int:
    if not clean:
        return -1

    issuer_alt = NATIONAL_ISSUER_PATTERN
    alias_alt = "|".join(
        re.escape(str(alias))
        for alias in (product.get("aliases") or [product.get("hazard_cn")])
        if alias
    )

    if not alias_alt:
        alias_alt = r"[\u4e00-\u9fff]{1,24}"

    patterns = (
        rf"(?:{issuer_alt})[^\n]{{0,180}}?(?:继续发布|联合发布|发布)"
        rf"[^\n]{{0,120}}?(?:{alias_alt})[^\n]{{0,80}}?"
        rf"(?:红色|橙色|黄色|蓝色)[^\n]{{0,40}}?(?:预警|预报)[:：]",
        rf"[^\n]{{0,160}}?(?:继续发布|联合发布|发布)"
        rf"[^\n]{{0,120}}?(?:{alias_alt})[^\n]{{0,80}}?"
        rf"(?:红色|橙色|黄色|蓝色)[^\n]{{0,40}}?(?:预警|预报)[:：]",
    )

    matches = []
    for pattern in patterns:
        for match in re.finditer(pattern, clean):
            matches.append(match)

    if not matches:
        return -1

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

    for chunk in (title, title.replace("继续发布", "发布")):
        short = chunk[:30]
        if short:
            pos = clean.find(short)
            if pos >= 0:
                return pos

    return -1


def _extract_detail_article(
    detail_text: str,
    *,
    fallback_title: str,
    product: Dict[str, Any],
) -> str:
    clean = _clean_article_text(detail_text)
    if not clean:
        return ""

    start = _find_article_start(clean, product=product)

    if start < 0:
        start = _fallback_start_from_title(clean, fallback_title)

    if start < 0:
        logging.warning(
            "[CMA/NMC DETAIL] Could not locate article start. "
            "title=%r product=%r sample=%r",
            fallback_title,
            product.get("hazard_cn"),
            clean[:400],
        )
        return ""

    end = _find_article_end(clean, start)
    if end <= start:
        end = len(clean)

    article = clean[start:end].strip()

    article = re.sub(
        r"^(台风预警|暴雨预警|强对流天气预警|地质灾害气象风险预警|"
        r"山洪灾害气象预警|中小河流洪水气象风险预警|农业气象灾害风险预警|"
        r"大风预警|大雾预警|沙尘暴预警|暴雪预警|寒潮预警|冰冻预警|"
        r"高温预警|气象干旱预警|低温预警|渍涝风险气象预警)\s*",
        "",
        article,
    ).strip()

    return article[:8000].strip()


async def _enrich_entry_from_detail_page(
    client: httpx.AsyncClient,
    entry: Dict[str, Any],
    *,
    timeout: float,
    allowed_levels: Set[str],
) -> Dict[str, Any]:
    url = str(entry.get("link") or "").strip()
    title = str(entry.get("title") or "").strip()
    product = _product_for_url(url)

    if not url or not product:
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
        product=product,
    )

    if article:
        article_level = _extract_product_level(article, product)

        if article_level in EN_LEVELS:
            entry["level"] = article_level

        article_published = _parse_pubtime_from_text(article)
        if article_published:
            entry["published"] = article_published
            entry["timestamp"] = _timestamp_from_iso(
                article_published,
                float(entry.get("timestamp") or 0.0),
            )

        entry["summary"] = article
        entry["description"] = article
        entry["body"] = article

        logging.warning(
            "[CMA/NMC DETAIL] Enriched %r product=%s level=%s summary_len=%d",
            title,
            product.get("hazard_cn"),
            entry.get("level"),
            len(article),
        )
    else:
        # Important: do not extract level from the whole detail page.
        # Whole pages contain nav links/latest lists that can include stale
        # yellow/orange/red warning words.
        logging.warning(
            "[CMA/NMC DETAIL] Detail fetched but no article extracted: %s title=%r",
            url,
            title,
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
      allowed_levels: ["Red", "Orange", "Yellow"]
      fetch_detail_pages: true
      timeout: 15
      require_active_badge: true
    """
    timeout = float(_conf_value(conf, "timeout", 15) or 15)
    allowed_levels = _allowed_levels_from_conf(conf)
    fetch_detail_pages = _conf_bool(conf, "fetch_detail_pages", True)
    require_active_badge = _conf_bool(conf, "require_active_badge", True)

    logging.warning(
        "[CMA/NMC DEBUG] allowed_levels=%s require_active_badge=%s",
        sorted(allowed_levels),
        require_active_badge,
    )

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
        require_active_badge=require_active_badge,
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

    for entry in entries:
        entry.pop("_order", None)

    logging.warning(
        "[CMA/NMC DEBUG] returning_entries=%s",
        [
            (
                e.get("level"),
                e.get("hazard_cn"),
                e.get("title"),
                e.get("link"),
            )
            for e in entries
        ],
    )

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
