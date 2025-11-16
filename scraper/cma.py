# scraper/cma.py
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx
from bs4 import BeautifulSoup

__all__ = ["scrape_cma_async", "scrape_async", "scrape"]

MAIN_PAGE = "https://weather.cma.cn/web/alarm/map.html"
TIMEOUT = httpx.Timeout(20.0)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

# ---------------- Timezone helpers ----------------
try:
    import zoneinfo
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
    return dt.astimezone(timezone.utc).isoformat() if dt else None


# ---------------- Regex patterns ----------------
# 发布时间：2025年11月16日10时 或 发布时间：2025年11月16日10时30分
RE_PUBLISHED = re.compile(
    r"发布时间：\s*(\d{4})年(\d{1,2})月(\d{1,2})日\s*(\d{1,2})(?:时|:)(\d{2})?"
)

# 9月5日20时至6日20时 / 9月5日20:30至6日20:00 / 9月5日20时到9月6日20时 / 9月5日20时—6日20时 / 9月5日20时~6日20时 …
RE_WINDOW = re.compile(
    r"(?P<mon>\d{1,2})月(?P<d1>\d{1,2})日(?P<h1>\d{1,2})(?:时|:)?(?P<m1>\d{2})?"
    r"\s*(?:至|到|—|–|-|~|～)\s*"
    r"(?:(?P<mon2>\d{1,2})月)?(?P<d2>\d{1,2})日(?P<h2>\d{1,2})(?:时|:)?(?P<m2>\d{2})?"
)

# robust color extraction (normalize whitespace first)
RE_LEVEL_CANON = re.compile(r"(蓝色|黄色|橙色|红色)预警")
RE_LEVEL_FALLBACK = re.compile(r"([蓝黄橙红])色.*?预警")
CN_COLOR_MAP = {"蓝色": "Blue", "黄色": "Yellow", "橙色": "Orange", "红色": "Red"}
CN_COLOR_CHAR_MAP = {"蓝": "Blue", "黄": "Yellow", "橙": "Orange", "红": "Red"}

# Canonical English for frequent titles (fallback to auto translate otherwise)
TITLE_EN_MAP: Dict[str, str] = {
    "暴雨预警": "Heavy Rain Warning",
    "强对流天气预警": "Severe Convective Weather Warning",
    "台风预警": "Typhoon Warning",
    "地质灾害气象风险预警": "Geological Disaster Meteorological Risk Warning",
    "渍涝风险气象预报": "Waterlogging Risk Forecast",
    "高温预警": "High Temperature Warning",
    "寒潮预警": "Cold Wave Warning",
    "大风预警": "Gale Warning",
    "沙尘暴预警": "Sandstorm Warning",
    "低温雨雪冰冻预警": "Low-Temperature Rain/Snow/Icing Warning",
}


def _abs_url(href: str) -> str:
    if href.startswith(("http://", "https://")):
        return href
    if href.startswith("/"):
        return f"https://weather.cma.cn{href}"
    return f"https://weather.cma.cn/web/{href.lstrip('./')}"


# -------- Discover links from the map page --------
def _extract_whitelist_links_from_map(html: str) -> List[str]:
    """
    Prefer grabbing ALL channel links inside #disasterWarning (CMA's own 'active' box).
    If that box isn't present in SSR, fall back to scanning the full page.
    Accept both numeric and hashed channel slugs.
    """
    soup = BeautifulSoup(html, "html.parser")
    urls: List[str] = []

    box = soup.find(id="disasterWarning")
    anchors = box.find_all("a", href=True) if box else soup.find_all("a", href=True)

    for a in anchors:
        href = (a.get("href") or "").strip()
        # Only keep CMA "channel" pages (numeric or hashed id), ending with .html
        if re.search(r"/web/channel-[\w-]+\.html$", href):
            urls.append(_abs_url(href))

    # De-dup while preserving order
    seen: set[str] = set()
    out: List[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# ---------------- Article extraction ----------------
def _headline(soup: BeautifulSoup) -> str:
    """
    Try to extract a good human-readable title for CMA warnings.

    Priority:
    1. First <p> inside <div class="xml"> (CMA detail body heading)
    2. <h1> (breadcrumb on some pages)
    3. <title> or generic fallback.
    """
    xml_div = soup.find("div", class_="xml")
    if xml_div:
        first_p = xml_div.find("p")
        if first_p:
            text = first_p.get_text("", strip=True)
            if text:
                # Strip trailing colon-like characters
                text = re.sub(r"[：:]\s*$", "", text)
                return text

    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        raw = h1.get_text(strip=True)
    else:
        raw = soup.title.get_text(strip=True) if soup.title else "气象预警"

    # Drop breadcrumb-ish prefixes like “气象预警 >> 暴雨预警”
    parts = re.split(r"\s*(?:>>|›|＞)\s*", raw)
    last = (parts[-1] if parts else raw).replace("气象预警", "").strip()
    return last or raw


def _full_alert_text_from_article(soup: BeautifulSoup) -> str:
    """
    Return the main alert text from inside #text .xml; fall back sanely if structure changes.

    For CMA detail pages like:
      <div id="text">
        <div class="xml">
           <p><b>中央气象台11月16日10时继续发布寒潮蓝色预警：</b></p>
           <p>受寒潮影响，预计...</p>
           <p>其中，11月16日14时至18日14时...</p>
           ...
    We treat the first <p> as the title/headline and skip it in the body.
    """
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
        # skip empty paragraphs
        if not t:
            continue

        # First <p> is the headline; we use it as title, not body
        if i == 0:
            continue

        # skip image-only paragraphs
        if p.find("img") and not t.strip():
            continue

        out_lines.append(t)
    return "\n".join(out_lines).strip()


# ---------------- Published & window parsing ----------------
def _parse_published(all_text: str, tz: timezone) -> Optional[datetime]:
    m = RE_PUBLISHED.search(all_text)
    if not m:
        return None
    y, mo, d, hh, mm = m.groups()
    try:
        year = int(y)
        month = int(mo)
        day = int(d)
        hour = int(hh)
        minute = int(mm) if mm is not None else 0
        return datetime(year, month, day, hour, minute, tzinfo=tz)
    except Exception:
        return None


def _parse_window(
    all_text: str, pub_dt: Optional[datetime], tz: timezone
) -> tuple[Optional[datetime], Optional[datetime]]:
    """
    Parse windows like “9月5日20时至6日20时” and variants with/without minutes or month on RHS.
    Anchor to pub_dt's year/month when omitted.
    """
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

    start: Optional[datetime]
    end: Optional[datetime]

    try:
        start = datetime(year, m1, d1, h1, min1, tzinfo=tz)
    except Exception:
        start = None
    try:
        end = datetime(year, mon2 if mon2 else m1, d2, h2, min2, tzinfo=tz)
        # if month omitted and end < start, roll to next month
        if start and end < start and not mon2:
            nm = m1 + 1
            ny = year + (1 if nm == 13 else 0)
            nm = 1 if nm == 13 else nm
            end = datetime(ny, nm, d2, h2, min2, tzinfo=tz)
    except Exception:
        end = None

    return start, end


def _parse_level(text: str) -> Optional[str]:
    """
    Robustly detect alert color. CMA sometimes inserts spaces like “黄 色预警”.
    """
    compact = re.sub(r"\s+", "", text)
    m = RE_LEVEL_CANON.search(compact)
    if m:
        return CN_COLOR_MAP.get(m.group(1))
    m2 = RE_LEVEL_FALLBACK.search(compact)
    if m2:
        return CN_COLOR_CHAR_MAP.get(m2.group(1))
    return None


# ---------------- Translation (optional) ----------------
async def _translate_text_google(
    text: str, target_lang: str, client: httpx.AsyncClient
) -> Optional[str]:
    """
    Lightweight use of Google's public translate endpoint (best-effort).
    """
    try:
        if not text:
            return text

        # Chunk to stay under URL length limits
        chunks: List[str] = []
        acc: List[str] = []
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
            params = {
                "client": "gtx",
                "sl": "auto",
                "tl": target_lang,
                "dt": "t",
                "q": chunk,
            }
            r = await client.get(url, params=params, timeout=10.0)
            r.raise_for_status()
            data = r.json()
            out_parts.append("".join(seg[0] for seg in data[0]))
        return "\n".join(out_parts).strip()
    except Exception as e:
        logging.warning("[CMA DEBUG] Translation failed: %s", e)
        return None


# ---------------- Parse a detail page → struct ----------------
def _parse_detail_html_struct(
    html: str, url: str, tz: timezone
) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    all_text = soup.get_text("\n", strip=True)

    pub_dt = _parse_published(all_text, tz)

    # NOTE: windows are optional—don't drop an entry just because we can't parse them
    w_start, w_end = _parse_window(all_text, pub_dt, tz)

    # Try level from <h1> first (often cleaner), then body
    h1_el = soup.find("h1")
    h1_text = h1_el.get_text(" ", strip=True) if h1_el else ""
    level = _parse_level(h1_text + all_text)

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


# ---------------- Public entry (concurrent) ----------------
async def scrape_cma_async(
    conf: Optional[Dict[str, Any]] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> Dict[str, Any]:
    """
    Optional conf:
      - tz_name: 'Asia/Shanghai' (default)
      - expiry_grace_minutes: int (default 0) → if window_end exists, drop after end+grace
      - urls: list[str] (fallback if discovery finds nothing)
      - translate_to_en: bool (default False) → add English body and bilingual title
    """
    conf = conf or {}
    tz = _tz(conf.get("tz_name", "Asia/Shanghai"))
    grace = int(conf.get("expiry_grace_minutes", 0))
    want_en = bool(conf.get("translate_to_en", False))

    close_client = False
    if client is None:
        client = httpx.AsyncClient(
            timeout=TIMEOUT, headers=HEADERS, follow_redirects=True
        )
        close_client = True

    errors: List[str] = []
    entries: List[Dict[str, Any]] = []

    try:
        # 1) discover today's links
        whitelist: List[str] = []
        try:
            resp = await client.get(MAIN_PAGE, timeout=15.0)
            resp.raise_for_status()
            whitelist = _extract_whitelist_links_from_map(resp.text)
        except Exception as e:
            errors.append(f"main: {e}")

        # fallback if explicitly configured
        urls_conf = conf.get("urls")
        if not whitelist and isinstance(urls_conf, list):
            whitelist = [str(u) for u in urls_conf if isinstance(u, str)]

        if not whitelist:
            out: Dict[str, Any] = {"entries": [], "source": "CMA"}
            if errors:
                out["error"] = "; ".join(errors)
            return out

        # 2) fetch + parse concurrently
        async def fetch_and_build(url: str) -> Optional[Dict[str, Any]]:
            try:
                r = await client.get(url, timeout=15.0)
                r.raise_for_status()
                data = _parse_detail_html_struct(r.text, url, tz)
                if not data:
                    return None

                # Optional drop after expiry (+grace) IF we know the window_end
                if grace > 0:
                    edt = data.get("window_end")
                    if isinstance(edt, datetime) and datetime.now(tz) >= (
                        edt + timedelta(minutes=grace)
                    ):
                        return None

                # ---- Build title (bilingual when translate_to_en=True) ----
                title_out = data["title"]
                if want_en:
                    title_en = TITLE_EN_MAP.get(title_out)
                    if not title_en:
                        title_en = await _translate_text_google(
                            title_out, "en", client
                        )
                    if title_en:
                        title_out = f"{title_out} ({title_en})"

                # ---- Build summary/body ----
                w_start = data.get("window_start")
                w_end = data.get("window_end")

                window_line = ""
                if isinstance(w_start, datetime) and isinstance(w_end, datetime):
                    window_line = (
                        f"有效期：{w_start.strftime('%m月%d日%H:%M')} 至 "
                        f"{w_end.strftime('%m月%d日%H:%M')}（北京时间）\n\n"
                    )

                full_text = data["full_text"]
                summary = f"{window_line}{full_text}".strip()
                body = summary  # for renderer's optional body field

                if want_en:
                    translated = await _translate_text_google(
                        full_text, "en", client
                    )
                    if translated:
                        summary = (
                            f"{summary}\n\n**English (auto):**\n{translated}"
                        )

                # Fallback for missing 'published' so 'new' highlighting still works
                pub = data.get("published") or _iso(w_start)

                return {
                    "title": title_out,
                    "level": data.get("level"),
                    "summary": summary,
                    "link": data["link"],
                    "published": pub,
                    "body": body,
                }
            except Exception as e:
                errors.append(f"{url}: {e}")
                return None

        results = await asyncio.gather(
            *(fetch_and_build(u) for u in whitelist), return_exceptions=False
        )
        entries = [r for r in results if r]
    finally:
        if close_client:
            await client.aclose()

    out = {"entries": entries, "source": "CMA"}
    if errors:
        out["error"] = "; ".join(errors)
    return out


# Compatibility aliases for registry variants
async def scrape_async(conf, client):
    return await scrape_cma_async(conf, client)


async def scrape(conf, client):
    return await scrape_cma_async(conf, client)
