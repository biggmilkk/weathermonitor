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
# Patterns (detail pages share this text shape)
# ------------------------------------------------------------
# e.g., “发布时间：2025年09月05日10时”
RE_PUBLISHED = re.compile(r"发布时间：\s*(\d{4})年(\d{1,2})月(\d{1,2})日(\d{1,2})时")

# Robust window variants:
#   9月5日20时至6日20时
#   9月5日20:30至6日20:00
#   9月5日20时到9月6日20时
#   9月5日20时—6日20时 / - / ～ etc.
RE_WINDOW = re.compile(
    r"(?P<mon>\d{1,2})月(?P<d1>\d{1,2})日(?P<h1>\d{1,2})(?:时|:)?(?P<m1>\d{2})?"
    r"\s*(?:至|到|—|–|-|~|～)\s*"
    r"(?:(?P<mon2>\d{1,2})月)?(?P<d2>\d{1,2})日(?P<h2>\d{1,2})(?:时|:)?(?P<m2>\d{2})?"
)

# e.g., "蓝色预警" / "黄色预警" / "橙色预警" / "红色预警"
RE_LEVEL = re.compile(r"(蓝色|黄色|橙色|红色)\s*预警")
CN_COLOR_MAP = {"蓝色": "Blue", "黄色": "Yellow", "橙色": "Orange", "红色": "Red"}

# Impacted regions (best effort):
# We'll search after removing the window text, to avoid capturing dates as regions.
RE_IMPACT_MAIN = re.compile(
    r"预计[，,]\s*(?P<regions>[\u4e00-\u9fff、\s]{2,}?)(?:等地)?(?:部分地区)?(?:将)?发生",
    re.S,
)

# Content roots to try for nicer summaries
_CONTENT_SELECTORS = [
    "main", "div#content", "div.detail", "div.article", "div#article", "div.container", "div#main"
]

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
    # de-dupe keep order
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

def _page_text(soup: BeautifulSoup) -> str:
    for sel in _CONTENT_SELECTORS:
        node = soup.select_one(sel)
        if node and node.get_text(strip=True):
            return node.get_text("\n", strip=True)
    return soup.get_text("\n", strip=True)

def _headline(soup: BeautifulSoup) -> str:
    # Prefer <h1>, else <title>, then clean breadcrumb & generic label
    h1 = soup.find("h1")
    raw = h1.get_text(strip=True) if (h1 and h1.get_text(strip=True)) else (
        soup.find("title").get_text(strip=True) if soup.find("title") else "气象预警"
    )
    parts = re.split(r"\s*(?:>>|›|＞)\s*", raw)
    last = (parts[-1] if parts else raw).replace("气象预警", "").strip()
    return last or raw

# ------------------------------------------------------------
# Summary helpers
# ------------------------------------------------------------
_BLOCKLIST_SUBSTR = (
    "首页", "主站", "网站地图", "导航", "当前位置",
    "部门概况", "政务公开", "业务介绍", "新闻", "公告",
    "友情链接", "帮助", "地图", "联系我们"
)

RE_LINE_LOOKS_LIKE_WINDOW = re.compile(r"^\d{1,2}月\d{1,2}日.*(至|到|—|–|-|~|～).*$")
def _is_nav_line(s: str) -> bool:
    if any(b in s for b in _BLOCKLIST_SUBSTR):
        return True
    if RE_LINE_LOOKS_LIKE_WINDOW.search(s):
        return True
    return False

def _first_meaningful_line(text: str) -> str:
    # Prefer an informative sentence; skip crumbs, window-looking lines, and too-short fragments
    for line in text.split("\n"):
        s = line.strip()
        if not s or _is_nav_line(s):
            continue
        # Must contain Chinese and be reasonably long
        if re.search(r"[\u4e00-\u9fff]", s) and len(s) >= 8:
            return s
    # fallback: still avoid nav lines
    for line in text.split("\n"):
        s = line.strip()
        if s and not _is_nav_line(s):
            return s
    return ""

def _strip_window_fragment(s: str) -> str:
    """Remove any '…月…日至…月…日…' fragment to avoid mistaking it as region text."""
    return RE_WINDOW.sub("", s)

def _extract_impacted_regions(all_text: str) -> List[str]:
    """
    Best effort: pull the comma-separated region list from the opening sentence, e.g.
    '预计，四川东北部、重庆北部、陕西东南部等地部分地区发生…'
    Returns a list like ['四川东北部', '重庆北部', '陕西东南部'].
    """
    # Remove explicit time-window fragments before searching
    text_wo_window = _strip_window_fragment(all_text)
    m = RE_IMPACT_MAIN.search(text_wo_window)
    if not m:
        return []

    segment = m.group("regions")
    # Keep only Chinese, delimiter '、', and spaces, then collapse spaces
    segment = re.sub(r"[^\u4e00-\u9fff、\s]", "", segment).strip()
    segment = re.sub(r"\s+", "", segment)

    # Split by the Chinese list delimiter
    parts = [p.strip() for p in segment.split("、") if p.strip()]

    cleaned: List[str] = []
    for p in parts:
        # Drop anything that still looks like a time/date token
        if re.search(r"[月日时至到—–\-~：:0-9]", p):
            continue
        # Drop generic tails like “等地/部分地区” if they slipped in
        p = re.sub(r"(等地|部分地区)$", "", p).strip()
        # Keep only reasonably descriptive chunks (>= 2 Chinese chars)
        if re.search(r"[\u4e00-\u9fff]{2,}", p):
            cleaned.append(p)

    # Deduplicate preserving order
    seen, out = set(), []
    for c in cleaned:
        if c not in seen:
            seen.add(c); out.append(c)
    return out

def _summarize(body_text: str, win_start: Optional[datetime], win_end: Optional[datetime], all_text: str) -> str:
    parts = []
    if win_start and win_end:
        parts.append(f"有效期：{win_start.strftime('%m月%d日%H:%M')} 至 {win_end.strftime('%m月%d日%H:%M')}（北京时间）")

    # Impacted regions (if present)
    regions = _extract_impacted_regions(all_text)
    if regions:
        parts.append("影响区域：" + "；".join(regions))

    first = _first_meaningful_line(body_text)
    if len(first) > 140:
        first = first[:137] + "…"
    if first:
        parts.append(first)
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

def _parse_window(text: str, pub_dt: Optional[datetime], tz) -> Tuple[Optional[datetime], Optional[datetime]]:
    m = RE_WINDOW.search(text)
    if not m:
        return None, None
    mon = int(m.group("mon"))
    d1  = int(m.group("d1")); h1 = int(m.group("h1"))
    m1s = m.group("m1"); min1 = int(m1s) if m1s else 0
    mon2 = m.group("mon2")
    d2  = int(m.group("d2")); h2 = int(m.group("h2"))
    m2s = m.group("m2"); min2 = int(m2s) if m2s else 0

    year = (pub_dt.year if pub_dt else datetime.now(tz).year)
    m1 = mon
    m2 = int(mon2) if mon2 else m1

    start = end = None
    try:
        start = datetime(year, m1, d1, h1, min1, tzinfo=tz)
    except Exception:
        start = None
    try:
        end = datetime(year, m2, d2, h2, min2, tzinfo=tz)
        # If month omitted for end and end < start, assume rollover to next month
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
# Parse a single channel detail page (requires active window)
# ------------------------------------------------------------
def _parse_detail_html(html: str, url: str, tz) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    all_text = soup.get_text("\n", strip=True)   # robust match surface
    body_text = _page_text(soup)                 # nicer summary surface

    pub_dt = _parse_published(all_text, tz)      # optional
    w_start, w_end = _parse_window(all_text, pub_dt, tz)
    if not (w_start and w_end):
        return None  # cannot assert active
    level = _parse_level(all_text)

    now = datetime.now(tz)
    if not (w_start <= now < w_end):
        return None  # not currently active

    title = _headline(soup)
    summary = _summarize(body_text, w_start, w_end, all_text)

    # IMPORTANT: do NOT include "region" to avoid "Region:" in UI
    return {
        "title": title,
        "level": level,
        "summary": summary,
        "link": url,
        "published": _iso(pub_dt),
    }

# ------------------------------------------------------------
# Public entry — matches your registry: scrape_cma_async(conf, client)
# ------------------------------------------------------------
async def scrape_cma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    Conf:
      - tz_name: str (default 'Asia/Shanghai')
      - expiry_grace_minutes: int (default 0)
      - urls: list[str] (optional; used ONLY if #disasterWarning is empty)
    Behavior (strict-first):
      1) Read today's links from /web/alarm/map.html (#disasterWarning).
      2) Scrape ONLY those links for currently-active alerts.
      3) If the box is empty, optionally fall back to conf["urls"] (no static defaults).
    """
    tz = _tz(conf.get("tz_name", "Asia/Shanghai"))
    grace = int(conf.get("expiry_grace_minutes", 0))

    entries: List[Dict[str, Any]] = []
    errors: List[str] = []

    # Step 1: whitelist from main map page (source of truth)
    whitelist: List[str] = []
    try:
        resp = await client.get(MAIN_PAGE, timeout=15.0)
        resp.raise_for_status()
        whitelist = _extract_whitelist_links_from_map(resp.text)
    except Exception as e:
        errors.append(f"main: {e}")

    # Step 2: strict-first — only scrape live links; if empty, allow explicit conf["urls"]
    if not whitelist:
        whitelist = list(conf.get("urls") or [])

    # Step 3: fetch & parse each whitelisted link
    for url in whitelist:
        try:
            r = await client.get(url, timeout=15.0)
            r.raise_for_status()
            item = _parse_detail_html(r.text, url, tz)
            if not item:
                continue
            if grace > 0:
                # Optional grace: allow a brief post-expiry window
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
