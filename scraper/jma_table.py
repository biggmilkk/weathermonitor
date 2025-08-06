import streamlit as st
import httpx
from bs4 import BeautifulSoup
import logging
from datetime import datetime

# Map the cell CSS classes to human levels
_CLASS_TO_LEVEL = {
    "contents-level20": "Advisory",
    "contents-level30": "Warning",
    "contents-level40": "Alert",
}

@st.cache_data(ttl=60, show_spinner=False)
async def scrape_jma_table_async(conf: dict, _client: httpx.AsyncClient) -> dict:
    """
    Fetch the JMA HTML warning page and extract the warning-table.
    """
    url = conf.get("url", "https://www.jma.go.jp/bosai/warning/")
    try:
        resp = await _client.get(url, timeout=10, follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        logging.warning(f"[JMA TABLE FETCH ERROR] {url} - {e}")
        return {"entries": [], "error": str(e), "source": url}

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="warning-table")
    if not table:
        logging.warning(f"[JMA TABLE] No <table.warning-table> found at {url}")
        return {"entries": [], "source": url}

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="warning-table")
    if not table:
        logging.warning(f"[JMA TABLE] No <table.warning-table> found at {url}")
        return {"entries": [], "source": url}

    # First header row gives the column names (skip the very first empty column)
    header = table.find("tr", class_="contents-header")
    type_names = [
        th.get_text(" ", strip=True)
        for th in header.find_all("th")[1:]
    ]

    entries = []
    current_group = None

    # Walk each row
    for tr in table.find_all("tr"):
        classes = tr.get("class", [])
        # group header row
        if "contents-header" in classes:
            area_th = tr.find("th", class_="contents-area")
            if area_th:
                current_group = area_th.get_text(strip=True)
            continue

        # region row: first <th> is region name
        if "contents-clickable" not in "".join(classes):
            continue
        region_th = tr.find("th", class_="contents-clickable")
        if not region_th or not current_group:
            continue

        region = region_th.get_text(strip=True)
        cells = tr.find_all("td")
        for idx, td in enumerate(cells):
            # pick out the level by CSS class
            level = None
            for cls in td.get("class", []):
                if cls in _CLASS_TO_LEVEL:
                    level = _CLASS_TO_LEVEL[cls]
                    break
            if not level:
                continue
            entries.append({
                "group":     current_group,
                "region":    region,
                "type":      type_names[idx],
                "level":     level,
                # timestamp so we can diff against last‐seen
                "timestamp": datetime.utcnow().isoformat() + "Z"
            })

    logging.warning(f"[JMA TABLE DEBUG] Parsed {len(entries)} alerts from {url}")
    return {"entries": entries, "source": url}
