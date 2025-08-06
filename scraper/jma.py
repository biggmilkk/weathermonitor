import logging
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser as dateparser

async def scrape_jma_async(conf: dict, client) -> dict:
    """
    Async scraper for JMA warning page.
    Returns {'entries': [...], 'source': url}
    """
    url = conf.get("url", "https://www.jma.go.jp/bosai/warning/#lang=en")
    try:
        r = await client.get(url, timeout=10)
        r.raise_for_status()
        html = r.text
        soup = BeautifulSoup(html, "html.parser")

        entries = []
        # adjust these selectors if JMA changes their page
        panels = soup.select("div.panel-warning, div.panel-advisory")
        for p in panels:
            title_el = p.select_one(".panel-title")
            desc_el  = p.select_one(".panel-body")
            link_el  = p.select_one("a[href]")

            title   = title_el.get_text(strip=True) if title_el else "(no title)"
            summary = desc_el.get_text("\n", strip=True) if desc_el else ""
            link    = link_el["href"] if link_el and link_el["href"].startswith("http") else url
            published = datetime.utcnow().isoformat()

            entries.append({
                "title": title,
                "summary": summary,
                "link": link,
                "published": published,
            })

        logging.warning(f"[JMA DEBUG] Parsed {len(entries)} warnings")
        return {"entries": entries, "source": url}

    except Exception as e:
        logging.warning(f"[JMA ERROR] {e}")
        return {"entries": [], "error": str(e), "source": url}
