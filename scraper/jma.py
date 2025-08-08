# scraper/jma.py
import asyncio
import datetime as dt
import logging
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup

try:
    # playwright is optional at install time; import inside so the app still boots without it
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
except Exception:  # pragma: no cover
    async_playwright = None
    PWTimeout = Exception

_ALLOWED_LEVELS = ("Warning", "Emergency", "Alert")  # keep only these
_JMA_ROOT = "https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices"

async def _extract_until_for_area(page, area_url: str, timeout_ms: int = 10000) -> Optional[str]:
    """
    Navigate to a specific area page and try to extract the "active until" text, if present.
    Returns a human-readable string or None.
    """
    try:
        await page.goto(area_url, wait_until="networkidle", timeout=timeout_ms)
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")

        # The English page typically includes phrasing like "… will be active until at least …"
        # Be flexible: scan visible text blobs for "until".
        text_chunks: List[str] = []
        for el in soup.find_all(["p", "div", "li", "span"]):
            t = el.get_text(" ", strip=True)
            if not t:
                continue
            if "until" in t.lower() and ("active" in t.lower() or "valid" in t.lower()):
                text_chunks.append(t)
        if text_chunks:
            # pick the first plausible line
            return text_chunks[0]

        # Fallback: grab any table cell that looks like validity/period
        for td in soup.select("table td"):
            t = td.get_text(" ", strip=True)
            if "until" in t.lower():
                return t
    except PWTimeout:
        logging.warning("[JMA DEBUG] Timeout while opening area page: %s", area_url)
    except Exception as e:  # be resilient, just skip the 'until'
        logging.warning("[JMA DEBUG] Failed to parse 'until' on %s: %s", area_url, e)
    return None


async def scrape_jma_async(conf: Dict[str, Any], client=None) -> Dict[str, Any]:
    """
    Scrape JMA warning table (English UI).

    Produces entries with:
      - title: "<Region>: <Level> – <Phenomenon>"
      - region
      - level
      - type: phenomenon
      - until: optional "active until" string if we can find it on the area page
      - link: area-specific URL if available else the root listing
      - published: ISO timestamp (UTC)

    Optional conf keys:
      - fetch_until: bool (default True) — whether to open each area page to grab "until"
      - max_regions: int (default None) — cap how many rows we enrich with "until" to keep it fast
      - headless: bool (default True)
      - root_url: str (debug override)
    """
    if async_playwright is None:
        logging.warning("[JMA DEBUG] Playwright not available; JMA scraper disabled.")
        return {"entries": [], "source": _JMA_ROOT}

    root_url = conf.get("root_url", _JMA_ROOT)
    fetch_until: bool = conf.get("fetch_until", True)
    max_regions: Optional[int] = conf.get("max_regions")  # e.g., 20 to cap enrichment
    headless: bool = conf.get("headless", True)

    entries: List[Dict[str, Any]] = []
    now_iso = dt.datetime.utcnow().isoformat() + "Z"

    # Safety args for sandboxed environments
    launch_args = {
        "headless": headless,
        "args": [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
        ],
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(**launch_args)
        try:
            page = await browser.new_page()
            await page.goto(root_url, wait_until="networkidle", timeout=30000)

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            # The list is a table; keep selectors generic so minor DOM tweaks don't break us
            rows = soup.select("table tr")
            for row in rows:
                tds = row.find_all("td")
                if len(tds) < 3:
                    continue

                # Heuristic: first three columns are Region | Phenomenon | Level
                region = tds[0].get_text(strip=True)
                phenomenon = tds[1].get_text(strip=True)
                level = tds[2].get_text(strip=True)

                # Keep only Warning/Emergency/Alert
                if not level or not any(l in level for l in _ALLOWED_LEVELS):
                    continue

                # Try to get a region-specific link (so we can fetch "until")
                area_href = None
                a = tds[0].find("a", href=True)
                if a:
                    # Make absolute if needed; JMA uses hash URLs with area_code param
                    if a["href"].startswith("http"):
                        area_href = a["href"]
                    else:
                        # normalize relative/hash to absolute root
                        area_href = "https://www.jma.go.jp" + a["href"] if a["href"].startswith("/") else _JMA_ROOT

                entries.append({
                    "title": f"{region}: {level} – {phenomenon}" if phenomenon else f"{region}: {level}",
                    "region": region,
                    "level": level,
                    "type": phenomenon,
                    "summary": "",
                    "published": now_iso,
                    "link": area_href or root_url,
                    # placeholder, may be filled below
                    "until": None,
                })

            # Optionally enrich with "until" by visiting area pages
            if fetch_until and entries:
                # Reuse one tab for speed
                detail_page = await browser.new_page()
                tasks = 0
                for item in entries:
                    if not item.get("link"):
                        continue
                    if max_regions is not None and tasks >= max_regions:
                        break
                    # Only try if it's an area URL with area_code
                    if "area_code=" not in item["link"]:
                        continue

                    tasks += 1
                    try:
                        until_text = await _extract_until_for_area(detail_page, item["link"])
                        if until_text:
                            item["until"] = until_text
                    except Exception as e:
                        logging.warning("[JMA DEBUG] Enrich 'until' failed on %s: %s", item["link"], e)

                await detail_page.close()

        finally:
            await browser.close()

    logging.warning("[JMA DEBUG] Async parsed %d alerts (filtered to Warning/Alert/Emergency)", len(entries))
    return {"entries": entries, "source": root_url}
