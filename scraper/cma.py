# cma.py
# -*- coding: utf-8 -*-
"""
CMA (China) scraper that visits specific channel pages, extracts the valid-time
window, and returns ONLY currently-active alerts (start <= now < end).

Interface matches your app:
  async def rss_cma(conf: dict, client: httpx.AsyncClient) -> dict
Returns {"entries": [...], "source": <str>, "error": <optional str>}

Expected entry fields (used by renderer.render_cma):
  - title: str
  - level: "Blue"|"Yellow"|"Orange"|"Red"|None
  - summary: str
  - link: str (absolute)
  - published: str (parseable by dateutil)
  - region: str (optional)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any

import httpx
from bs4 import BeautifulSoup

# ------------------------------------------------------------------------------------
# Defaults: if conf["urls"] isn’t provided, we’ll use this observed set.
# You can also pass conf["expiry_grace_minutes"] and conf["tz_name"].
# ------------------------------------------------------------------------------------
DEFAULT_URLS = [
    "https://weather.cma.cn/web/channel-374.html",  # 暴雨预警 (Rainstorm)
    "https://weather.cma.cn/web/channel-375.html",  # 高温预警 (Heat)
    "https://weather.cma.cn/web/channel-376.html",  # 台风预警 (Typhoon)
    "https://weather.cma.cn/web/channel-378.html",  # 强对流天气预警 (Severe convection)
    "https://weather.cma.cn/web/channel-fdb168519f08446088d6461b381b32b9.html",  # 地质灾害气象风险预警
    "https://weather.cma.cn/web/channel-2af8854da8874cd3b9fd28d10cf59ef4.html",  # 渍涝风险气象预报
    "https://weather.cma.cn/web/channel-e0d01629ac3643d4ac1da6fcc9e17ab5.html",  # 中小河流洪水气象风险预警
]

# ------------------------------------------------------------------------------------
# Timezone
# ------------------------------------------------------------------------------------
try:
    import zoneinfo  # py39+
    _TZ = zoneinfo.ZoneInfo
except Exception:
    _TZ = None

def _tz(name: str) -> timezone:
    if _TZ:
        try:
            return _TZ(name)  # type: ignore
        except Exception:
            pass
    # fallback fixed +08:00 if Asia/Shanghai not available
    if name == "Asia/Shanghai":
        return timezone(timedelta(hours=8), name=name)
    return timezone.utc

# ------------------------------------------------------------------------------------
# Regex patterns (detail page shape)
# ------------------------------------------------------------------------------------
RE_PUBLISHED = re.compile(r"发布时间：\s*(\d{4})年(\d{1,2})月(\d{1,2})日(\d{1,2})时")
RE_WINDOW = re.compile(
    r"(?P<mon>\d{1,2})月(?P<d1>\d{1,2})日(?P<h1>\d{1,2})时\s*至\s*(?:(?P<mon2>\d{1,2})月)?(?P<d2>\d{1,2})日(?P<h2>\d{1,2})时"
)
RE_LEVEL = re.compile(r"(蓝色|黄色|橙色|红色)\s*预警")
CN_COLOR_MAP = {"蓝色": "Blue", "黄色": "Yellow", "橙色": "Orange", "红色": "Red"}

TYPE_RULES = [
    (re.compile("暴雨预警"), "Rainstorm"),
    (re.compile("台风预警"), "Typhoon"),
    (re.compile("高温预警"), "Heat"),
    (re.compile("强对流天气预警|强对流天气"), "Severe convection"),
    (re.compile("地质灾害气象风险预警"), "Geo-hazard risk"),
    (re.compile("渍涝风险气象预报|渍涝风险"), "Waterlogging risk"),
    (re.compile("中小河流洪水气象风险预警|中小河流洪水"), "Small/medium river flood risk"),
]

# ------------------------------------------------------------------------------------
# Data shapes
# ------------------------------------------------------------------------------------
@dataclass
class CmaAlert:
    type_canon: str
    level: Optional[str]           # Blue/Yellow/Orange/Red
    title: str
    link: str
    published: Optional[str]       # ISO (+08:00), parseable by dateutil
    region: Optional[str]          # not always present
    summary: str                   # short human text (we fill with body+window)
    window_start_iso: Optional[str]
    window_end_iso: Optional[str]

# ------------------------------------------------------------------------------------
# Parsing utils
# ------------------------------------------------------------------------------------
def _canon_type(text: str) -> str:
    for pat, canon in TYPE_RULES:
        if pat.search(text):
            return canon
    return "Alert"

def _parse_published(text_blob: str, tz) -> Optional[datetime]:
    m = RE_PUBLISHED.search(text_blob)
    if not m:
        return None
    y, mo, d, h = map(int, m.groups())
    try:
        return datetime(y, mo, d, h, 0, tzinfo=tz)
    except Exception:
        return None

def _parse_window(text_blob: str, pub_dt: Optional[datetime], tz) -> Tuple[Optional[datetime], Optional[datetime]]:
    m = RE_WINDOW.search(text_blob)
    if not m:
        return None, None
    mon = int(m.group("mon"))
    d1  = int(m.group("d1")); h1 = int(m.group("h1"))
    mon2 = m.group("mon2")
    d2  = int(m.group("d2")); h2 = int(m.group("h2"))

    year = (pub_dt.year if pub_dt else datetime.now(tz).year)
    m1 = mon
    m2 = int(mon2) if mon2 else m1

    start = end = None
    try:
        start = datetime(year, m1, d1, h1, 0, tzinfo=tz)
    except Exception:
        start = None

    try:
        end = datetime(year, m2, d2, h2, 0, tzinfo=tz)
        if start and end < start and not mon2:
            # month rollover when month for end is omitted
            nm = m1 + 1
            ny = year + (1 if nm == 13 else 0)
            nm = 1 if nm == 13 else nm
            end = datetime(ny, nm, d2, h2, 0, tzinfo=tz)
    except Exception:
        end = None

    return start, end

def _extract_text_root(soup: BeautifulSoup) -> str:
    for sel in ["main", "div#content", "div.detail", "div.article", "div#article", "div.container", "div#main"]:
        node = soup.select_one(sel)
        if node and node.get_text(strip=True):
            return node.get_text("\n", strip=True)
    return soup.get_text("\n", strip=True)

def _headline(soup: BeautifulSoup, default_title: str) -> str:
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    tit = soup.find("title")
    return tit.get_text(strip=True) if tit else default_title

def _summarize(body: str, win_start: Optional[datetime], win_end: Optional[datetime]) -> str:
    out = []
    if win_start and win_end:
        out.append(f"有效期：{win_start.strftime('%m月%d日%H时')} 至 {win_end.strftime('%m月%d日%H时')}（北京时间）")
    # first sentence-ish
    first = body.split("\n", 1)[0]
    if len(first) > 140:
        first = first[:137] + "…"
    out.append(first)
    return "  \n".join(out)

# ------------------------------------------------------------------------------------
# Core: parse a single CMA channel detail page
# ------------------------------------------------------------------------------------
def _parse_detail_html(html: str, url: str, tz) -> Optional[CmaAlert]:
    soup = BeautifulSoup(html, "html.parser")
    text = _extract_text_root(soup)

    pub_dt = _parse_published(text, tz)
    w_start, w_end = _parse_window(text, pub_dt, tz)
    level = None
    m = RE_LEVEL.search(text)
    if m:
        level = CN_COLOR_MAP.get(m.group(1))

    # Must have an active window to include
    if not (w_start and w_end):
        return None

    now = datetime.now(tz)
    if not (w_start <= now < w_end):
        return None  # not currently active

    title = _headline(soup, default_title="气象预警")
    type_canon = _canon_type(title + " " + text)
    summary = _summarize(text, w_start, w_end)

    return CmaAlert(
        type_canon=type_canon,
        level=level,
        title=title,
        link=url,
        published=pub_dt.isoformat() if pub_dt else None,
        region=None,
        summary=summary,
        window_start_iso=w_start.isoformat() if w_start else None,
        window_end_iso=w_end.isoformat() if w_end else None,
    )

# ------------------------------------------------------------------------------------
# Public scraper entry point (async) — matches your registry call signature
# ------------------------------------------------------------------------------------
async def rss_cma(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    conf:
      - urls: list[str] (optional; defaults to DEFAULT_URLS)
      - tz_name: str (optional; default "Asia/Shanghai")
      - expiry_grace_minutes: int (optional; default 0)  # keeps alert slightly after end
    """
    urls: List[str] = conf.get("urls") or DEFAULT_URLS
    tz = _tz(conf.get("tz_name", "Asia/Shanghai"))
    grace_minutes = int(conf.get("expiry_grace_minutes", 0))

    entries: List[Dict[str, Any]] = []
    errors: List[str] = []

    for url in urls:
        try:
            r = await client.get(url, timeout=15)
            r.raise_for_status()
            alert = _parse_detail_html(r.text, url, tz)
            if alert:
                # Optional grace: if within grace after end, still include
                if alert.window_end_iso and grace_minutes > 0:
                    try:
                        end_dt = datetime.fromisoformat(alert.window_end_iso)
                        if datetime.now(tz) >= end_dt and datetime.now(tz) < (end_dt + timedelta(minutes=grace_minutes)):
                            pass  # still include
                    except Exception:
                        pass

                # Shape entries for renderer.render_cma:
                entries.append({
                    "title": alert.title,
                    "level": alert.level,        # renderer uses this for bullet color
                    "summary": alert.summary,
                    "link": alert.link,
                    "published": alert.published,
                    "region": alert.type_canon,  # show the type under "Region:" caption slot
                })
        except Exception as e:
            errors.append(f"{url}: {e}")

    out: Dict[str, Any] = {"entries": entries, "source": "CMA"}
    if errors:
        out["error"] = "; ".join(errors)
    return out
