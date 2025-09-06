# scraper/cma.py
from __future__ import annotations

import re
import logging
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

#  9月5日20时至6日20时 / 9月5日20:30至6日20:00 / 9月5日20时到9月6日20时 / 9月5日20时—6日20时 / - / ～ …
RE_WINDOW = re.compile(
    r"(?P<mon>\d{1,2})月(?P<d1>\d{1,2})日(?P<h1>\d{1,2})(?:时|:)?(?P<m1>\d{2})?"
    r"\s*(?:至|到|—|–|-|~|～)\s*"
    r"(?:(?P<mon2>\d{1,2})月)?(?P<d2>\d{1,2})日(?P<h2>\d{1,2})(?:时|:)?(?P<m2>\d{2})?"
)

RE_LEVEL = re.compile(r"(蓝色|黄色|橙色|红色)\s*预警")
CN_COLOR_MAP = {"蓝色": "Blue", "黄色": "Yellow", "橙色": "Orange", "红色": "Red"}

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
# Article text extraction (FULL text for summary)
# ------------------------------------------------------------
def _full_alert_text_from_article(soup: BeautifulSoup) -> str:
    """
    Return the full alert text from inside #text .xml:
    - Skip the first bold header paragraph if it contains '联合发布' and not '预计'
    - Skip paragraphs that are images-only
    - Join remaining <p> texts with newlines
    """
    container = soup.select_one("#text .xml")
    if not container:
        # Fallbacks if structure changes
        for sel in (
            "#text",
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
                container = n
                break
    if not container:
        return soup.get_text("\n", strip=True)

    out_lines: List[str] = []
    paragraphs = container.find_all("p")
    for idx, p in enumerate(paragraphs):
        # Skip image-only paragraphs
        if p.find("img"):
            continue
        text = p.get_text(" ", strip=True)
        # Normalize internal whitespace but keep natural sentence spacing
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        # Skip the bold header like “自然资源部与中国气象局…联合发布…预警：”
        if idx == 0 and ("联合发布" in text) and ("预计" not in text):
            continue
        out_lines.append(text)

    full_text = "\n".join(out_lines).strip()
    return full_text


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
# Translation (optional, with debug logs)
# ------------------------------------------------------------
async def _translate_text_google(text: str, target_lang: str, client: httpx.AsyncClient) -> Optional[str]:
    """
    Use Google's lightweight public translate endpoint.
    If it fails for any reason, return None (we'll fall back gracefully).
    """
    try:
        # Split long text into chunks (~4500 chars) to be safe
        chunks: List[str] = []
        buf: List[str] = []
        total = 0
        for line in text.split("\n"):
            if total + len(line) + 1 > 4500 and buf:
                chunks.append("\n".join(buf))
                buf = [line]
                total = len(line) + 1
            else:
                buf.append(line)
                total += len(line) + 1
        if buf:
            chunks.append("\n".join(buf))

        out_parts: List[str] = []
        for ch in chunks:
            params = {
                "client": "gtx",
                "sl": "auto",
                "tl": target_lang,
                "dt": "t",
                "q": ch,
            }
            r = await client.get(
                "https://translate.googleapis.com/translate_a/single",
                params=params,
                timeout=15.0,
            )
            r.raise_for_status()
            data = r.json()
            segs = data[0] if isinstance(data, list) and data else []
            out_parts.append("".join(seg[0] for seg in segs if isinstance(seg, list) and seg))
        result = "\n".join(out_parts).strip() if out_parts else None
        return result if result else None
    except Exception as e:
        logging.warning(f"[CMA DEBUG] Translation failed: {e}")
        return None


# ------------------------------------------------------------
# Parse detail page (returns structured pieces; summary built outside)
# ------------------------------------------------------------
def _parse_detail_html_struct(html: str, url: str, tz) -> Optional[Dict[str, Any]]:
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

    full_alert_text = _full_alert_text_from_article(soup)
    title = _headline(soup)

    return {
        "title": title,
        "level": level,
        "full_text": full_alert_text,  # return raw alert body so caller can translate/compose
        "window_start": w_start,
        "window_end": w_end,
        "link": url,
        "published": _iso(pub_dt),
    }


# ------------------------------------------------------------
# Public entry
# ------------------------------------------------------------
async def scrape_cma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    Optional conf:
      - tz_name: 'Asia/Shanghai' (default)
      - expiry_grace_minutes: int (default 0)
      - urls: list[str] (fallback if #disasterWarning is empty)
      - translate_to_en: bool (default False) → append automatic English translation paragraph
    """
    tz = _tz(conf.get("tz_name", "Asia/Shanghai"))
    grace = int(conf.get("expiry_grace_minutes", 0))
    want_en = bool(conf.get("translate_to_en", False))
    logging.warning(f"[CMA DEBUG] translate_to_en={want_en}")

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
            data = _parse_detail_html_struct(r.text, url, tz)
            if not data:
                continue

            # Optional grace after expiry
            if grace > 0:
                sdt = data.get("window_start")
                edt = data.get("window_end")
                if isinstance(edt, datetime) and datetime.now(tz) >= (edt + timedelta(minutes=grace)):
                    continue

            # Compose summary
            w_start: datetime = data["window_start"]  # type: ignore
            w_end: datetime = data["window_end"]      # type: ignore
            window_line = f"有效期：{w_start.strftime('%m月%d日%H:%M')} 至 {w_end.strftime('%m月%d日%H:%M')}（北京时间）"
            summary = f"{window_line}\n\n{data['full_text']}"

            # Optional automatic English translation as a second paragraph
            if want_en:
                logging.warning(f"[CMA DEBUG] Translating alert from {url}")
                translated = await _translate_text_google(data["full_text"], "en", client)
                if translated:
                    summary = f"{summary}\n\n**English (auto):**\n{translated}"
                    logging.warning("[CMA DEBUG] Translation success")
                else:
                    logging.warning("[CMA DEBUG] Translation empty or unavailable")

            entries.append({
                "title": data["title"],
                "level": data.get("level"),
                "summary": summary.strip(),
                "link": data["link"],
                "published": data["published"],
            })
        except Exception as e:
            errors.append(f"{url}: {e}")

    # Single debug line (match EC/BOM/JMA style)
    logging.warning(f"[CMA DEBUG] Parsed {len(entries)} alerts")

    out: Dict[str, Any] = {"entries": entries, "source": "CMA"}
    if errors:
        out["error"] = "; ".join(errors)
    return out
