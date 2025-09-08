# scraper/cma.py
from __future__ import annotations

import re
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any

import httpx
from bs4 import BeautifulSoup

MAIN_PAGE = "https://weather.cma.cn/web/alarm/map.html"

# ------------------------------------------------------------
# Timezone helpers
# ------------------------------------------------------------
try:
    import zoneinfo  # py3.9+
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

# 9月5日20时至6日20时 / 9月5日20:30至6日20:00 / 9月5日20时到9月6日20时 / 9月5日20时—6日20时 / - / ～ …
RE_WINDOW = re.compile(
    r"(?P<mon>\d{1,2})月(?P<d1>\d{1,2})日(?P<h1>\d{1,2})(?:时|:)?(?P<m1>\d{2})?"
    r"\s*(?:至|到|—|–|-|~|～)\s*"
    r"(?:(?P<mon2>\d{1,2})月)?(?P<d2>\d{1,2})日(?P<h2>\d{1,2})(?:时|:)?(?P<m2>\d{2})?"
)

RE_LEVEL = re.compile(r"(蓝色|黄色|橙色|红色)\s*预警")
CN_COLOR_MAP = {"蓝色": "Blue", "黄色": "Yellow", "橙色": "Orange", "红色": "Red"}


def _abs_url(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return f"https://weather.cma.cn{href if href.startswith('/') else '/' + href}"


def _extract_whitelist_links_from_map(html: str) -> list[str]:
    """
    Robustly discover the three 'active' category links from the CMA map page.

    The page used to expose <div id="disasterWarning">…</div>. It no longer does in SSR.
    So we:
      1) Prefer any element with anchors whose text ends with '预警'
      2) Filter to URLs under /web/channel-*.html (including hashed IDs)
      3) De-dup and absolutize
    """
    soup = BeautifulSoup(html, "html.parser")

    # Collect all anchors that *look* like category links
    candidates = []
    for a in soup.find_all("a", href=True):
        txt = (a.get_text(strip=True) or "")
        href = a["href"]
        if not txt:
            continue
        # The visible text is like: 暴雨预警 / 台风预警 / 地质灾害气象风险预警
        if not txt.endswith("预警"):
            continue
        # Accept both numeric channels and hashed channels
        if not re.search(r"/web/channel-[\w-]+\.html", href):
            continue
        candidates.append(_abs_url(href))

    # Fallback: if nothing matched (structure change), try broader scan
    if not candidates:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(r"/web/channel-[\w-]+\.html", href):
                candidates.append(_abs_url(href))

    # De-dup, keep order
    seen, out = set(), []
    for u in candidates:
        if u not in seen:
            seen.add(u)
            out.append(u)

    return out


# ------------------------------------------------------------
# Article text extraction (FULL text for summary)
# ------------------------------------------------------------
def _full_alert_text_from_article(soup: BeautifulSoup) -> str:
    container = soup.select_one("#text .xml")
    if not container:
        for sel in ("#text", "main", "div#content", "div.detail", "div.article", "article"):
            container = soup.select_one(sel)
            if container:
                break
    if not container:
        return soup.get_text("\n", strip=True)

    ps = list(container.find_all("p"))
    out_lines: List[str] = []
    for i, p in enumerate(ps):
        t = p.get_text(" ", strip=True)
        if not t and p.find("img"):
            continue
        if i == 0:
            b = p.find("b") or p.find("strong")
            if b:
                bt = b.get_text(" ", strip=True)
                if "联合发布" in bt and "预计" not in bt:
                    continue
        out_lines.append(t)
    return "\n".join([ln for ln in out_lines if ln]).strip()


# ------------------------------------------------------------
# Published time parsing
# ------------------------------------------------------------
def _parse_published(all_text: str, tz) -> Optional[datetime]:
    m = RE_PUBLISHED.search(all_text)
    if not m:
        return None
    try:
        y, mon, d, h = [int(g) for g in m.groups()]
        return datetime(y, mon, d, h, 0, tzinfo=tz)
    except Exception:
        return None


# ------------------------------------------------------------
# Forecast window parsing
# ------------------------------------------------------------
def _parse_window(all_text: str, pub_dt: Optional[datetime], tz) -> Tuple[Optional[datetime], Optional[datetime]]:
    m = RE_WINDOW.search(all_text.replace("\n", ""))
    if not m:
        return None, None

    def _int_or(groups: re.Match, key: str, default: Optional[int]) -> Optional[int]:
        v = groups.group(key)
        return int(v) if v and v.isdigit() else default

    if pub_dt:
        year = pub_dt.year
        base_mon = pub_dt.month
    else:
        now = datetime.now(tz)
        year = now.year
        base_mon = now.month

    m1 = _int_or(m, "mon", base_mon) or base_mon
    d1 = _int_or(m, "d1", None)
    h1 = _int_or(m, "h1", 0) or 0
    min1 = _int_or(m, "m1", 0) or 0
    mon2 = _int_or(m, "mon2", None)
    d2 = _int_or(m, "d2", None)
    h2 = _int_or(m, "h2", 0) or 0
    min2 = _int_or(m, "m2", 0) or 0

    try:
        start = datetime(year, m1, d1, h1, min1, tzinfo=tz)
    except Exception:
        start = None
    try:
        end = datetime(year, mon2 if mon2 else m1, d2, h2, min2, tzinfo=tz)
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
# Translation (optional)
# ------------------------------------------------------------
async def _translate_text_google(text: str, target_lang: str, client: httpx.AsyncClient) -> Optional[str]:
    try:
        chunks: List[str] = []
        acc = []
        total = 0
        for line in text.splitlines():
            if total + len(line) + 1 > 4500:
                chunks.append("\n".join(acc))
                acc = [line]
                total = len(line) + 1
            else:
                acc.append(line)
                total += len(line) + 1
        if acc:
            chunks.append("\n".join(acc))

        out_parts: List[str] = []
        for chunk in chunks:
            url = "https://translate.googleapis.com/translate_a/single"
            params = {"client": "gtx", "sl": "auto", "tl": target_lang, "dt": "t", "q": chunk}
            r = await client.get(url, params=params, timeout=10.0)
            r.raise_for_status()
            data = r.json()
            out_parts.append("".join(seg[0] for seg in data[0]))
        return "\n".join(out_parts).strip()
    except Exception as e:
        logging.warning(f"[CMA DEBUG] Translation failed: {e}")
        return None


# ------------------------------------------------------------
# Parse detail page (structured)
# ------------------------------------------------------------
def _parse_detail_html_struct(html: str, url: str, tz) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    all_text = soup.get_text("\n", strip=True)

    pub_dt = _parse_published(all_text, tz)
    w_start, w_end = _parse_window(all_text, pub_dt, tz)
    if not (w_start and w_end):
        return None
    level = _parse_level(all_text)

    full_alert_text = _full_alert_text_from_article(soup)
    title = _headline(soup)

    return {
        "title": title,
        "level": level,
        "full_text": full_alert_text,
        "window_start": w_start,
        "window_end": w_end,
        "link": url,
        "published": _iso(pub_dt),
    }


def _headline(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    raw = (
        h1.get_text(strip=True)
        if (h1 and h1.get_text(strip=True))
        else (soup.find("title").get_text(strip=True) if soup.find("title") else "气象预警")
    )
    parts = re.split(r"\s*(?:>>|›|＞)\s*", raw)
