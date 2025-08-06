import httpx
from bs4 import BeautifulSoup
from datetime import datetime
import logging

async def scrape_jma_table_async(_client: httpx.AsyncClient, conf: dict) -> dict:
    """
    Async fetch & parse JMA warnings table.
    Returns {"entries": [...], "source": url}.
    """
    url = conf.get("url")
    entries = []
    try:
        resp = await _client.get(url, timeout=10, follow_redirects=True)
        logging.warning(resp.text[:10000])
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        table = soup.find("table", class_="warning-table")
        # find all region rows (skip header rows)
        rows = table.find_all("tr", class_=lambda c: c and "warning-clickable" in c)
        current_group = None
        for row in rows:
            # if this is a top-level <th> with a region name but no colon, treat it as a new group
            text = row.th.get_text(strip=True)
            if ":" not in text:
                current_group = text
                continue

            # otherwise split "Prefecture: Area"
            group, area = text.split(":", 1)
            # _client already fetched the page, so no new HTTP here
            # now for each cell in that row, if it has a class "contents-levelXX" it means a warning
            for idx, cell in enumerate(row.find_all("td")):
                cls = cell.get("class", [])
                level_cls = next((c for c in cls if c.startswith("contents-level")), None)
                if not level_cls:
                    continue
                level = cell.get("title", cell.get_text(strip=True))
                # map column idx→phenomenon name from the header row
                header = table.find_all("tr", class_="contents-header")[0]
                phen = header.find_all("th")[idx+1].get_text(" ", strip=True)
                entries.append({
                    "group":       current_group or group,
                    "area":        area.strip(),
                    "phenomenon":  phen,
                    "level":       level,
                    "published":   datetime.utcnow().isoformat() + "Z",
                    "link":        url
                })

        logging.warning(f"[JMA DEBUG] Parsed {len(entries)} warnings")
        return {"entries": entries, "source": url}

    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {e}")
        return {"entries": [], "error": str(e), "source": url}
