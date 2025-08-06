import streamlit as st
import logging
import httpx
from bs4 import BeautifulSoup
from datetime import datetime

@st.cache_data(ttl=60, show_spinner=False)
async def scrape_jma_table_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Async scraper for the JMA warning table.
    Expects conf["url"] pointing at the JMA warnings page.
    Returns {"entries": [...], "source": url}.
    """
    url = conf.get("url")
    try:
        resp = await client.get(url, follow_redirects=True, timeout=10.0)
        resp.raise_for_status()
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {e}")
        return {"entries": [], "error": str(e), "source": url}

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="warning-table")
    if not table:
        logging.warning("[JMA PARSE ERROR] <table class='warning-table'> not found")
        return {"entries": [], "error": "warning-table not found", "source": url}

    # 1) extract the phenomenon names from the first header row (skip the very first empty/trick row)
    header = table.find("tr", class_="contents-header")
    phenoms = [th.get_text(" ", strip=True) for th in header.find_all("th")[1:]]

    entries = []
    current_group = None

    # 2) walk every <tr> in the table
    for row in table.find_all("tr"):
        classes = row.get("class", [])

        # detect group headers (they carry both classes "contents-header" and "contents-bold-top")
        if "contents-header" in classes and "contents-bold-top" in classes:
            current_group = row.find("th").get_text(strip=True)
            continue

        # skip the repeated phenomenon-name header rows
        if "contents-header" in classes and "contents-bold-top" not in classes:
            continue

        # now any row with a <th class="contents-clickable warning-clickable"> is a data row
        th = row.find("th", class_="contents-clickable")
        if not th:
            continue
        area = th.get_text(strip=True)

        # 3) loop each <td> in that row
        for idx, cell in enumerate(row.find_all("td")):
            # if it's missing or empty, skip
            if "contents-missing" in cell.get("class", []):
                continue

            # title attr holds the human level ("Advisory", "Warning", etc.)
            level = cell.get("title", "").strip() or "Unknown"

            # the phenomenon name is at the same index in phenoms
            phen = phenoms[idx] if idx < len(phenoms) else f"Phenomenon {idx}"

            entries.append({
                "group":       current_group,
                "area":        area,
                "phenomenon":  phen,
                "level":       level,
                "published":   datetime.utcnow().isoformat() + "Z",
                "link":        url,
            })

    logging.warning(f"[JMA DEBUG] Parsed {len(entries)} warnings")
    return {"entries": entries, "source": url}
