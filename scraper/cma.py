# scraper/cma.py
# -*- coding: utf-8 -*-
"""
CMA (China) scraper that mirrors exactly what's linked today on:
  https://weather.cma.cn/web/alarm/map.html

Flow:
  1) Fetch the map page, read #disasterWarning a[href] as the whitelist.
  2) For each whitelisted link, fetch the detail page and parse:
     - published (optional)
     - valid-time window (start/end)
     - level color (蓝/黄/橙/红 → Blue/Yellow/Orange/Red)
     - lead sentence from the article body (#text .xml) starting with “预计，…”
     - impacted regions extracted from that lead sentence
  3) Return an entry ONLY if start <= now < end (Asia/Shanghai)
     (Optional) keep within a small grace after end via conf["expiry_grace_minutes"].
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any

import httpx
from bs4 import BeautifulSoup

MAIN_PAGE = "https://weather.cma.cn/web/alarm/map.html"

# ------------------------------------------------------------
# Timezone helpers
# ------------------------------------------------------------
try:
    import zoneinfo  # Python 3.9+
    _HAS_ZONEINFO = True
except Exception:
    _HAS_ZONEINFO = False


def _tz(tz_name: str) -> timezone:
    if _HAS_ZONEINFO:
        try:
            return zoneinfo.ZoneInfo(tz_name)  # type: ignore
        except Exception:
            pass
    if tz_name == "Asia/Shanghai":
        return timezone(timedelta(hours=8), name=tz_name)
    return timezone.utc


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


# ------------------------------------------------------------
# Regex patterns
# ------------------------------------------------------------
RE_PUBLISHED = re.compile(r"发布时间：\s*(\d{4})年(\d{1,2})月(\d{1,2})日(\d{1,2})时")

RE_WINDOW = re.compile(
    r"(?P<mon>\d{1,2})月(?P<d1>\d{1,2})日(?P<h1>\d{1,2})(?:时|:)?(?P<m1>\d{2})?"
    r"\s*(?:至|到|—|–|-|~|～)\s*"
    r"(?:(?P<mon2>\d{1,2})月)?(?P<d2>\d{1,2})日(?P<h2>\d{1,2})(?:时|:)?(?P<m2>\d{2})?"
)

RE_LEVEL = re.compile(r"(蓝色|黄色|橙色|红色)\s*预警")
CN_COLOR_MAP = {"蓝色": "Blue", "黄色": "Yellow", "橙色": "Orange", "红色": "Red"}

RE_LEAD_SENTENCE = re.compile(r"(预计[，,].{10,400}?)(?:。|！|!|；|;|$)")

RE_IMPACT_MAIN = re.compile(
    r"预计[，,]\s*(?P<regions>[\u4e00-\u9fff、\s]{2,}?)(?:等地)?(?:部分地区)?(?:将)?发生",
    re.S,
)

RE_LINE_LOOKS_LIKE_WINDOW = re.compile(r"^\d{1,2}月\d{1,2}日.*(至|到|—|–|-|~|～).*$")


# ------------------------------------------------------------
# DOM helpers
# ------------------------------------------------------------
def _abs_url(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return f"https://weather.cma.cn{href if href.startswith('/') else '/' + href}"


def _extract_whitelist_links_from_map(html: str) -> list[str]:
    """Return today's links shown in #disasterWarning (SSR)."""
    soup = BeautifulSoup(html, "html.parser")
    box = soup.find(id="disasterWarning")
    if not box:
        return []
    urls = []
    for a in box.find_all("a", href=True):
        urls.append(_abs_url(a["href"]))
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _article_text(soup: BeautifulSoup) -> str:
    """Return the article body text (inside #text .xml) as fallback context."""
    node = soup.select_one("#text .xml")
    if node and node.get_text(strip=True):
        return node.get_text("\n", strip=True)
    for sel in (
        "main",
        "div#content",
        "div.detail",
        "div.article",
        "div#article",
        "div.container",
        "div#main",
    ):
        n = soup.select_one(sel)
        if n and n.get_text(strip=True):
            return n.get_text("\n", strip=True)
    return soup.get_text("\n", strip=True)


def _lead_paragraph_text(soup: BeautifulSoup) -> str:
    """Find the first <p> inside #text .xml that contains '预计'."""
    container = soup.select_one("#text .xml")
    if not container:
        return ""
    for p in container.find_all("p"):
        t = p.get_text(" ", strip=True)
        t = re.sub(r"\s+", "", t)
        if t.startswith("预计"):
            return t
    return ""


def _headline(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    raw = (
        h1.get_text(strip=True)
        if (h1 and h1.get_text(strip=True))
        else (soup.find("title").get_text(strip=True) if soup.find("title") else "气象预警")
    )
    parts = re.split(r"\s*(?:>>|›|＞)\s*", raw)
    last = (parts[-1] if parts else raw).replace("气象预警", "").strip()
    return last or raw


# ------------------------------------------------------------
# Summary helpers
# ------------------------------------------------------------
def _strip_window_fragment(s: str) -> str:
    return RE_WINDOW.sub("", s)


def _extract_impacted_regions(from_lead_sentence: str) -> List[str]:
    if not from_lead_sentence:
        return []
    text_wo_window = _strip_window_fragment(from_lead_sentence)
    m = RE_IMPACT_MAIN.search(text_wo_window)
    if not m:
        return []
    segment = m.group("regions")
    segment = re.sub(r"[^\u4e00-\u9fff、\s]", "", segment).strip()
    segment = re.sub(r"\s+", "", segment)
    parts = [p.strip() for p in segment.split("、") if p.strip()]
    cleaned: List[str] = []
    for p in parts:
        if re.search(r"[月日时至到—–\-~：:0-9]", p):
            continue
        p = re.sub(r"(等地|部分地区)$", "", p).strip()
        if re.search(r"[\u4e00-\u9fff]{2,}", p):
            cleaned.append(p)
    seen, out = set(), []
    for c in cleaned:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _summarize(
    article_text: str,
    win_start: Optional[datetime],
    win_end: Optional[datetime],
    lead_sentence: str,
) -> str:
    parts: List[str] = []
    if win_start and win_end:
        parts.append(
            f"有效期：{win_start.strftime('%m月%d日%H:%M')} 至 {win_end.strftime('%m月%d日%H:%M')}（北京时间）"
        )

    regions = _extract_impacted_regions(lead_sentence)
    if regions:
        parts.append("影响区域：" + "；".join(regions))

    text_line = lead_sentence
    if not text_line:
        for line in article_text.split("\n"):
            s = line.strip()
            if not s:
                continue
            if RE_LINE_LOOKS_LIKE_WINDOW.search(s):
                continue
            if re.search(r"[\u4e00-\u9fff]", s) and len(s) >= 8:
                text_line = s
                break

    if text_line:
        if len(text_line) > 140:
            text_line = text_line[:137] + "…"
        parts.append(text_line)

    return "  \n".join(parts) if parts else ""


# ------------------------------------------------------------
# Field parsers
# ------------------------------------------------------------
def _parse_published(text: str, tz) -> Optional[datetime]:
    m = RE_PUBLISHED.search(text)
    if not m:
        return None
    y, mo, d, h = map(int, m.groups())
    try:
        return datetime(y, mo, d, h, 0, tzinfo=tz)
    except Exception:
        return None


def _parse_window(
    text: str, pub_dt: Optional[datetime], tz
) -> Tuple[Optional[datetime], Optional[datetime]]:
    m = RE_WINDOW.search(text)
    if not m:
        return None, None
    mon = int(m.group("mon"))
    d1 = int(m.group("d1"))
    h1 = int(m.group("h1"))
    m1s = m.group("m1")
    min1 = int(m1s) if m1s else 0
    mon2 = m.group("mon2")
    d2 = int(m.group("d2"))
    h2 = int(m.group("h2"))
    m2s = m.group("m2")
    min2 = int(m2s) if m2s else 0

    year = pub_dt.year if pub_dt else datetime.now(tz).year
    m1 = mon
    m2 = int(mon2) if mon2 else m1

    start = end = None
    try:
        start = datetime(year, m1, d1, h1, min1, tzinfo=tz)
    except Exception:
        start = None
    try:
        end = datetime(year, m2, d2, h2, min2, tzinfo=tz)
        if start and end < start and not mon2:
            nm = m1 + 1
            ny = year + (1 if nm == 13 else 0)
            nm = 1 if nm == 13 else nm
            end = datetime(ny, nm, d2, h2, min2, tzinfo=tz)
    except Exception:
        end = None

    return start, end


def _parse_level(text: str) -> Optional[str]:
    m = RE_LEVEL.search(text)
    return CN_COLOR_MAP.get(m.group(1)) if m else None


# ------------------------------------------------------------
# Parse detail page
# ------------------------------------------------------------
def _parse_detail_html(html: str, url: str, tz) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    all_text = soup.get_text("\n", strip=True)

    pub_dt = _parse_published(all_text, tz)
    w_start, w_end = _parse_window(all_text, pub_dt, tz)
    if not (w_start and w_end):
        return None
    level = _parse_level(all_text)

    now = datetime.now(tz)
    if not (w_start <= now < w_end):
        return None

    article_text = _article_text(soup)
    lead = _lead_paragraph_text(soup)

    title = _headline(soup)
    summary = _summarize(article_text, w_start, w_end, lead)

    return {
        "title": title,
        "level": level,
        "summary": summary,
        "link": url,
        "published": _iso(pub_dt),
    }


# ------------------------------------------------------------
# Public entry
# ------------------------------------------------------------
async def scrape_cma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    tz = _tz(conf.get("tz_name", "Asia/Shanghai"))
    grace = int(conf.get("expiry_grace_minutes", 0))

    entries: List[Dict[str, Any]] = []
    errors: List[str] = []

    whitelist: List[str] = []
    try:
        resp = await client.get(MAIN_PAGE, timeout=15.0)
        resp.raise_for_status()
        whitelist = _extract_whitelist_links_from_map(resp.text)
    except Exception as e:
        errors.append(f"main: {e}")

    if not whitelist:
        whitelist = list(conf.get("urls") or [])

    for url in whitelist:
        try:
            r = await client.get(url, timeout=15.0)
            r.raise_for_status()
            item = _parse_detail_html(r.text, url, tz)
            if not item:
                continue
            if grace > 0:
                soup = BeautifulSoup(r.text, "html.parser")
                t_all = soup.get_text("\n", strip=True)
                pub_dt = _parse_published(t_all, tz)
                sdt, edt = _parse_window(t_all, pub_dt, tz)
                if edt and datetime.now(tz) >= (edt + timedelta(minutes=grace)):
                    continue
            entries.append(item)
        except Exception as e:
            errors.append(f"{url}: {e}")

    out: Dict[str, Any] = {"entries": entries, "source": "CMA"}
    if errors:
        out["error"] = "; ".join(errors)
    return out
