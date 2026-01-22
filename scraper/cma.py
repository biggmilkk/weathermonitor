# scraper/cma.py
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

API_URL = "https://weather.cma.cn/api/map/alarm?adcode="

# Browser-like headers (REQUIRED to avoid 403)
CMA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Referer": "https://weather.cma.cn/",
    "Accept": "application/json, text/plain, */*",
}

# CMA severity mapping (Chinese → English canonical)
LEVEL_MAP = {
    "蓝色": "Blue",
    "黄色": "Yellow",
    "橙色": "Orange",
    "红色": "Red",
}

# Only keep these levels
ALLOWED_LEVELS = {"Orange", "Red"}


def _parse_time(ts: str | None) -> str | None:
    """Parse CMA timestamps and normalize to RFC3339 UTC."""
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return ts


def _province_from_adcode(adcode: str | None) -> str:
    """
    Province bucket logic (same idea as NWS zones).
    CMA adcode: first 2 digits = province-level.
    """
    if not adcode or len(adcode) < 2:
        return "Unknown"
    return adcode[:2]


async def scrape_cma_async(conf: dict, client) -> List[Dict[str, Any]]:
    """
    CMA active alerts scraper (JSON API).

    Output fields per item:
      - title
      - level (English: Orange/Red)
      - region (province bucket)
      - summary
      - body
      - link
      - published (UTC ISO)
      - timestamp (float)
      - source = "CMA"
    """
    entries: List[Dict[str, Any]] = []

    try:
        resp = await client.get(
            API_URL,
            headers=CMA_HEADERS,
            timeout=15.0,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        logging.exception("[CMA FETCH ERROR]")
        return entries

    data = payload.get("data") or []
    now_ts = datetime.now(tz=timezone.utc).timestamp()

    for item in data:
        try:
            # Example fields observed in CMA API
            # item["signalLevel"] → "橙色"/"红色"/"黄色"/"蓝色"
            # item["signalTypeName"] → 暴雨 / 台风 / 强对流天气
            # item["province"] or item["adcode"]
            # item["title"], item["content"], item["pubTime"]

            level_cn = (item.get("signalLevel") or "").strip()
            level = LEVEL_MAP.get(level_cn)

            if level not in ALLOWED_LEVELS:
                continue  # skip blue/yellow entirely

            title = item.get("title") or item.get("signalTypeName") or "CMA Alert"
            body = item.get("content") or ""
            pub = _parse_time(item.get("pubTime"))

            adcode = item.get("adcode") or ""
            province = _province_from_adcode(adcode)

            link = item.get("url")
            if link and link.startswith("/"):
                link = "https://weather.cma.cn" + link

            entry = {
                "source": "CMA",
                "title": title,
                "level": level,
                "region": province,
                "summary": body[:280] + "…" if len(body) > 300 else body,
                "body": body,
                "link": link,
                "published": pub,
                "timestamp": (
                    datetime.fromisoformat(pub).timestamp()
                    if pub else now_ts
                ),
            }

            entries.append(entry)

        except Exception:
            logging.exception("[CMA PARSE ERROR]")
            continue

    logging.warning("[CMA DEBUG] Parsed %d", len(entries))
    return entries
