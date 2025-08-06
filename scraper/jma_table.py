import logging
import httpx
import streamlit as st
from bs4 import BeautifulSoup

# browser-like headers
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.jma.go.jp/bosai/warning/",
}

def _parse_jma_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="warning-table")
    if not table:
        return []
    entries = []
    # first header row defines columns (skip it)
    rows = table.find_all("tr")
    current_area = None
    for tr in rows:
        th = tr.find("th", class_="warning-clickable")
        if th and "contents-area" in tr.get("class", []):
            # this is a region header (e.g. “Hokkaido”) – store it
            current_area = th.get_text(strip=True)
            continue

        # data row: first TH is sub-region; TDs are advisory levels
        if th and not any(c.startswith("contents-header") for c in tr.get("class", [])):
            sub = th.get_text(strip=True)
            cells = tr.find_all("td")
            # for each column, if it’s not “contents-missing” record an alert
            for idx, td in enumerate(cells, start=1):
                if "contents-missing" in td.get("class", []):
                    continue
                level = td.get("title") or td.get_text(strip=True)
                entries.append({
                    "region": current_area,
                    "subregion": sub,
                    "type":    table.find_all("th")[idx].get_text(" ", strip=True),
                    "level":   level,
                })
    return entries

@st.cache_data(ttl=60, show_spinner=False)
def scrape_jma_table(conf: dict) -> dict:
    """
    Synchronous scraper for the JMA warning table.
    """
    url = conf.get("url", "https://www.jma.go.jp/bosai/warning/")
    try:
        resp = httpx.get(url, headers=HEADERS, timeout=10, follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {url} - {e}")
        return {"entries": [], "error": str(e), "source": url}

    entries = _parse_jma_table(resp.text)
    logging.warning(f"[JMA DEBUG] Parsed {len(entries)} entries")
    return {"entries": entries, "source": url}
