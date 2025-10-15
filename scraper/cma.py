# scraper/cma.py
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
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

RE_PUBLISHED = re.compile(
    r"发布时间：\s*(\d{4})年(\d{1,2})月(\d{1,2})日\s*(\d{1,2}):(\d{2})"
)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.astimezone(timezone.utc).isoformat() if dt else None


def _abs_url(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"https://weather.cma.cn{href}"
    return f"https://weather.cma.cn/web/{href.lstrip('./')}"


def _discover_links_from_map(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    box = soup.find(id="disasterWarning")
    if not box:
        # Key change: return zero links when #disasterWarning is missing
        return []

    links: List[str] = []
    for a in box.find_all("a", href=True):
        href = a["href"].strip()
        if re.search(r"/web/channel-[\w-]+\.html$", href):
            links.append(_abs_url(href))
    return list(dict.fromkeys(links))


def _parse_title(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        raw = h1.get_text(strip=True)
    else:
        raw = soup.title.get_text(strip=True) if soup.title else "气象预警"
    return raw.strip()


def _parse_published(soup: BeautifulSoup) -> Optional[str]:
    text = soup.get_text("\n", strip=True)
    m = RE_PUBLISHED.search(text)
    if not m:
        return None
    y, mo, d, hh, mm = map(int, m.groups())
    try:
        dt = datetime(y, mo, d, hh, mm, tzinfo=timezone.utc)
        return _iso(dt)
    except Exception:
        return None


def _parse_body(soup: BeautifulSoup) -> Optional[str]:
    art = soup.find(id="text") or soup.find("article") or soup
    ps = [p.get_text(" ", strip=True) for p in art.find_all("p")] or [art.get_text(" ", strip=True)]
    body = "\n\n".join([t for t in ps if t])
    return body or None


async def _get(client: httpx.AsyncClient, url: str) -> str:
    r = await client.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.text


@dataclass
class Entry:
    title: str
    url: str
    published: Optional[str]
    body: Optional[str]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "url": self.url,
            "published": self.published,
            "body": self.body,
        }


async def _fetch_detail(client: httpx.AsyncClient, url: str) -> Optional[Entry]:
    try:
        html = await _get(client, url)
    except Exception as e:
        logging.warning("CMA detail fetch failed: %s", e)
        return None

    soup = BeautifulSoup(html, "html.parser")
    title = _parse_title(soup)
    published = _parse_published(soup)
    body = _parse_body(soup)
    return Entry(title=title, url=url, published=published, body=body)


async def scrape_cma_async(conf: Optional[Dict[str, Any]] = None, client: Optional[httpx.AsyncClient] = None) -> Dict[str, Any]:
    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True)
        close_client = True

    errors: List[str] = []
    entries: List[Dict[str, Any]] = []
    try:
        html = await _get(client, MAIN_PAGE)
        links = _discover_links_from_map(html)
        if links:
            results = await asyncio.gather(*[_fetch_detail(client, u) for u in links])
            entries = [e.as_dict() for e in results if e]
        else:
            entries = []
    except Exception as e:
        errors.append(f"fetch map: {e}")
    finally:
        if close_client:
            await client.aclose()

    out: Dict[str, Any] = {"entries": entries, "source": "CMA"}
    if errors:
        out["error"] = "; ".join(errors)
    return out


async def scrape_async(conf, client):
    return await scrape_cma_async(conf, client)


async def scrape(conf, client):
    return await scrape_cma_async(conf, client)
