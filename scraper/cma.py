# scraper/cma.py
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import httpx

__all__ = ["scrape_cma_async", "scrape_async", "scrape"]

API_URL = "https://weather.cma.cn/api/map/alarm?adcode="

# --------------------------
# HTTP headers (avoid 403)
# --------------------------

CMA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Referer": "https://weather.cma.cn/",
    "Accept": "application/json, text/plain, */*",
}

# --------------------------
# Province mapping (GB/T 2260)
# --------------------------

PROVINCE_CODE_TO_CN = {
    "11": "北京", "12": "天津", "13": "河北", "14": "山西", "15": "内蒙古",
    "21": "辽宁", "22": "吉林", "23": "黑龙江",
    "31": "上海", "32": "江苏", "33": "浙江", "34": "安徽", "35": "福建", "36": "江西", "37": "山东",
    "41": "河南", "42": "湖北", "43": "湖南", "44": "广东", "45": "广西", "46": "海南",
    "50": "重庆", "51": "四川", "52": "贵州", "53": "云南", "54": "西藏",
    "61": "陕西", "62": "甘肃", "63": "青海", "64": "宁夏", "65": "新疆",
    "71": "台湾", "81": "香港", "82": "澳门",
}

# --------------------------
# Severity handling
# --------------------------

ALLOWED_LEVELS = {"Red", "Orange"}

CN_COLOR_TO_EN = {
    "红色": "Red",
    "橙色": "Orange",
    "黄色": "Yellow",
    "蓝色": "Blue",
}

RE_COLOR_STRONG = re.compile(r"(红色|橙色|黄色|蓝色)\s*(?:预警|预警信号|警报|警报信号)")
RE_COLOR_SIMPLE = re.compile(r"(红色|橙色|黄色|蓝色)")
RE_COLOR_CODE = re.compile(r"_(RED|ORANGE|YELLOW|BLUE)\b", re.I)

# --------------------------
# Time handling (FIXED)
# --------------------------

CST = timezone(timedelta(hours=8))  # China Standard Time

def _parse_pubtime(s: Optional[str]) -> Optional[str]:
    """
    Parse CMA times as UTC+8 and convert to UTC ISO 8601.
    """
    if not s:
        return None

    s = str(s).strip()
    for fmt in ("%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            local_dt = datetime.strptime(s, fmt).replace(tzinfo=CST)
            utc_dt = local_dt.astimezone(timezone.utc)
            return utc_dt.isoformat()
        except Exception:
            continue

    return None

# --------------------------
# Helpers
# --------------------------

def _extract_level(item: Dict[str, Any]) -> Optional[str]:
    text = " ".join(
        str(v) for v in (
            item.get("headline"),
            item.get("title"),
            item.get("description"),
            item.get("type"),
            item.get("severity"),
        ) if v
    )
    text = re.sub(r"\s+", "", text)

    m = RE_COLOR_STRONG.search(text)
    if m:
        return CN_COLOR_TO_EN.get(m.group(1))

    m = RE_COLOR_CODE.search(text)
    if m:
        return m.group(1).capitalize()

    m = RE_COLOR_SIMPLE.search(text)
    if m:
        return CN_COLOR_TO_EN.get(m.group(1))

    return None


def _province_from_id(item: Dict[str, Any]) -> str:
    iid = item.get("id")
    if isinstance(iid, str):
        m = re.match(r"^(\d{6,})", iid)
        if m:
            code = m.group(1)[:2]
            if code in PROVINCE_CODE_TO_CN:
                return PROVINCE_CODE_TO_CN[code]
    return "全国"

# --------------------------
# Main scraper
# --------------------------

async def scrape_cma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    entries: List[Dict[str, Any]] = []

    try:
        resp = await client.get(API_URL, headers=CMA_HEADERS, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        logging.exception("[CMA FETCH ERROR]")
        return {"source": "CMA", "entries": [], "error": str(e)}

    alarms = payload.get("data") or []
    if not isinstance(alarms, list):
        return {"source": "CMA", "entries": []}

    now_ts = datetime.now(timezone.utc).timestamp()

    for item in alarms:
        try:
            level = _extract_level(item)
            if level not in ALLOWED_LEVELS:
                continue

            headline = (item.get("headline") or "").strip()
            short_title = (item.get("title") or "").strip()
            title = headline or short_title or "CMA Alert"

            summary = (item.get("description") or "").strip()

            published = _parse_pubtime(
                item.get("effective")
                or item.get("pubTime")
                or item.get("publishTime")
            )

            ts = now_ts
            if published:
                try:
                    ts = datetime.fromisoformat(published.replace("Z", "+00:00")).timestamp()
                except Exception:
                    pass

            entries.append(
                {
                    "source": "CMA",
                    "headline": headline,
                    "title": title,
                    "level": level,  # Red / Orange only
                    "region": _province_from_id(item),
                    "summary": summary,
                    "published": published,
                    "timestamp": ts,
                    "link": None,
                }
            )

        except Exception:
            logging.exception("[CMA PARSE ERROR]")

    logging.warning("[CMA DEBUG] Parsed %d", len(entries))
    return {"source": "CMA", "entries": entries}

# --------------------------
# Registry aliases
# --------------------------

async def scrape_async(conf, client):
    return await scrape_cma_async(conf, client)

async def scrape(conf, client):
    return await scrape_cma_async(conf, client)
