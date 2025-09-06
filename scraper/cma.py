from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any

import httpx
from bs4 import BeautifulSoup

# ------------------------------------------------------------
# Defaults: observed channel pages that appear in #disasterWarning
# ------------------------------------------------------------
DEFAULT_URLS = [
    "https://weather.cma.cn/web/channel-374.html",  # 暴雨预警 (Rainstorm)
    "https://weather.cma.cn/web/channel-375.html",  # 高温预警 (Heat)
    "https://weather.cma.cn/web/channel-376.html",  # 台风预警 (Typhoon)
    "https://weather.cma.cn/web/channel-378.html",  # 强对流天气预警 (Severe convection)
    "https://weather.cma.cn/web/channel-fdb168519f08446088d6461b381b32b9.html",  # 地质灾害气象风险预警
    "https://weather.cma.cn/web/channel-2af8854da8874cd3b9fd28d10cf59ef4.html",  # 渍涝风险气象预报
    "https://weather.cma.cn/web/channel-e0d01629ac3643d4ac1da6fcc9e17ab5.html",  # 中小河流洪水气象风险预警
]

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
# Regex patterns (detail pages share this textual shape)
# ------------------------------------------------------------
# e.g., "发布时间：2025年09月05日10时"
RE_PUBLISHED = re.compile(r"发布时间：\s*(\d{4})年(\d{1,2})月(\d{1,2})日(\d{1,2})时")

# e.g., "9月5日14时至6日14时" or "9月5日20时至9月6日20时"
RE_WINDOW = re.compile(
    r"(?P<mon>\d{1,2})月(?P<d1>\d{1,2})日(?P<h1>\d{1,2})时\s*至\s*(?:(?P<mon2>\d{1,2})月)?(?P<d2>\d{1,2})日(?P<h2>\d{1,2})时"
)

# e.g., "蓝色预警" / "黄色预警" / "橙色预警" / "红色预警"
RE_LEVEL = re.compile(r"(蓝色|黄色|橙色|红色)\s*预警")
CN_COLOR_MAP = {"蓝色": "Blue", "黄色": "Yellow", "橙色": "Orange", "红色": "Red"}

# quick type canonization based on common keywords
TYPE_RULES = [
    (re.compile("暴雨预警"), "Rainstorm"),
    (re.compile("台风预警"), "Typhoon"),
    (re.compile("高温预警"), "Heat"),
    (re.compile("强对流天气预警|强对流天气"), "Severe convection"),
    (re.compile("地质灾害气象风险预警"), "Geo-hazard risk"),
    (re.compile("渍涝风险气象预报|渍涝风险"), "Waterlogging risk"),
    (re.compile("中小河流洪水气象风险预警|中小河流洪水"), "Small/medium river flood risk"),
]

def _canon_type(text: str) -> str:
    for pat, canon in TYPE_RULES:
        if pat.search(text):
            return canon
    return "Alert"

# ------------------------------------------------------------
# DOM helpers (robust to minor changes)
# ------------------------------------------------------------
_CONTENT_SELECTORS = ["main", "div#content", "div.detail", "div.article", "div#article", "div.container", "div#main"]

def _extract_text_root(soup: BeautifulSoup) -> str:
    for sel in _CONTENT_SELECTORS:
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

# ------------------------------------------------------------
# Field parsers
# ------------------------------------------------------------
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
        # handle month rollover where end month omitted and appears before start
        if start and end < start and not mon2:
            nm = m1 + 1
            ny = year + (1 if nm == 13 else 0)
            nm = 1 if nm == 13 else nm
            end = datetime(ny, nm, d2, h2, 0, tzinfo=tz)
    except Exception:
        end = None

    return start, end

def _parse_level(text_blob: str) -> Optional[str]:
    m = RE_LEVEL.search(text_blob)
    if not m:
        return None
    return CN_COLOR_MAP.get(m.group(1))

def _summarize(body: str, win_start: Optional[datetime], win_end: Optional[datetime]) -> str:
    out = []
    if win_start and win_end:
        out.append(f"有效期：{win_start.strftime('%m月%d日%H时')} 至 {win_end.strftime('%m月%d日%H时')}（北京时间）")
    first = body.split("\n", 1)[0]
    if len(first) > 140:
        first = first[:137] + "…"
    out.append(first)
    return "  \n".join(out)

# ------------------------------------------------------------
# Parse a single CMA channel page (detail-style)
# ------------------------------------------------------------
def _parse_detail_html(html: str, url: str, tz) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    text = _extract_text_root(soup)

    pub_dt = _parse_published(text, tz)
    w_start, w_end = _parse_window(text, pub_dt, tz)
    level = _parse_level(text)

    # Only include if the alert window exists and is currently active
    if not (w_start and w_end):
        return None
    now = datetime.now(tz)
    if not (w_start <= now < w_end):
        return None

    title = _headline(soup, default_title="气象预警")
    type_canon = _canon_type(title + " " + text)
    summary = _summarize(text, w_start, w_end)

    # Shape exactly for your CMA renderer (title/level/summary/link/published/region)
    return {
        "title": title,
        "level": level,                         # bullet color in renderer
        "summary": summary,
        "link": url,
        "published": _iso(pub_dt),              # renderer converts to UTC label
        "region": type_canon,                   # shows as "Region:" line (optional)
    }

# ------------------------------------------------------------
# Public entry (async) — exactly what your registry calls
# ------------------------------------------------------------
async def scrape_cma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    conf:
      - urls: list[str] (optional; falls back to DEFAULT_URLS)
      - tz_name: str (optional; default "Asia/Shanghai")
      - expiry_grace_minutes: int (optional; default 0)  # keep visible briefly after end
      - url: str (optional; if it *is* a CMA channel URL, we will include it)
    """
    tz = _tz(conf.get("tz_name", "Asia/Shanghai"))
    grace_minutes = int(conf.get("expiry_grace_minutes", 0))

    urls = list(conf.get("urls") or [])
    # If a single 'url' is provided and looks like a CMA channel page, include it too
    single = conf.get("url")
    if isinstance(single, str) and "weather.cma.cn/web/channel" in single:
        urls.append(single)
    if not urls:
        urls = DEFAULT_URLS

    entries: List[Dict[str, Any]] = []
    errors: List[str] = []

    for url in urls:
        try:
            r = await client.get(url, timeout=15.0)
            r.raise_for_status()
            item = _parse_detail_html(r.text, url, tz)
            if item:
                # Optional grace: include shortly after end time
                if grace_minutes > 0:
                    # We don’t have end_time inside entry; recompute once to check grace
                    text = _extract_text_root(BeautifulSoup(r.text, "html.parser"))
                    pub_dt = _parse_published(text, tz)
                    w_start, w_end = _parse_window(text, pub_dt, tz)
                    if w_end:
                        now = datetime.now(tz)
                        if not (now < (w_end + timedelta(minutes=grace_minutes))):
                            # beyond grace; drop
                            pass
                        else:
                            entries.append(item)
                    else:
                        entries.append(item)
                else:
                    entries.append(item)
        except Exception as e:
            errors.append(f"{url}: {e}")

    out: Dict[str, Any] = {"entries": entries, "source": "CMA"}
    if errors:
        out["error"] = "; ".join(errors)
    return out
