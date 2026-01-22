# scraper/cma.py
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

import httpx

API_URL = "https://weather.cma.cn/api/map/alarm?adcode="

# Browser-like headers to avoid 403
CMA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ),
    "Referer": "https://weather.cma.cn/",
    "Accept": "application/json, text/plain, */*",
}

# Province CN→EN bucket map
PROVINCES = {
    "北京": "Beijing", "天津": "Tianjin", "上海": "Shanghai", "重庆": "Chongqing",
    "河北": "Hebei", "山西": "Shanxi", "辽宁": "Liaoning", "吉林": "Jilin",
    "黑龙江": "Heilongjiang", "江苏": "Jiangsu", "浙江": "Zhejiang", "安徽": "Anhui",
    "福建": "Fujian", "江西": "Jiangxi", "山东": "Shandong", "河南": "Henan",
    "湖北": "Hubei", "湖南": "Hunan", "广东": "Guangdong", "海南": "Hainan",
    "四川": "Sichuan", "贵州": "Guizhou", "云南": "Yunnan", "陕西": "Shaanxi",
    "甘肃": "Gansu", "青海": "Qinghai", "台湾": "Taiwan",
    "内蒙古": "Inner Mongolia", "广西": "Guangxi", "西藏": "Tibet",
    "宁夏": "Ningxia", "新疆": "Xinjiang", "香港": "Hong Kong", "澳门": "Macau",
}

LEVEL_MAP = {
    "RED": "Red",
    "ORANGE": "Orange",
}

def _parse_pubtime(text: str | None) -> str | None:
    if not text:
        return None
    try:
        # CMA example uses format like "2022/07/12 11:56"
        dt = datetime.strptime(text, "%Y/%m/%d %H:%M")
        return dt.isoformat()
    except Exception:
        return None

def _detect_level_and_type(item: dict[str, Any]) -> tuple[str | None, str | None]:
    """
    Detect level and event type from either:
      - type code like "11B09_RED"
      - title or headline text e.g., "高温橙色预警"
    """
    # 1. Try type code suffix
    type_code = item.get("type", "")
    if isinstance(type_code, str):
        parts = type_code.split("_")
        if len(parts) >= 2:
            lvl = parts[-1].upper()
            return LEVEL_MAP.get(lvl), parts[0]  # event_code

    # 2. Fallback: look in title text
    title = item.get("title", "")
    if "红色" in title:
        return "Red", None
    if "橙色" in title:
        return "Orange", None

    # 3. Nothing matched
    return None, None

def _infer_provinces_from_text(text: str) -> List[str]:
    hits: list[str] = []
    for cn, en in PROVINCES.items():
        if cn in text:
            hits.append(cn)
    return hits

async def scrape_cma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    entries: List[Dict[str, Any]] = []
    errors: List[str] = []

    try:
        resp = await client.get(API_URL, headers=CMA_HEADERS, timeout=15.0)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        logging.exception("[CMA FETCH ERROR]")
        return {"source": "CMA", "entries": [], "error": str(e)}

    alarms = payload.get("data") or []
    now_ts = datetime.utcnow().timestamp()

    for item in alarms:
        try:
            level, event_code = _detect_level_and_type(item)
            if level not in ("Orange", "Red"):
                continue

            title = item.get("title") or item.get("headline") or "(no title)"
            desc = item.get("description") or ""
            pub = _parse_pubtime(item.get("effective"))

            # Province inference from title/content
            text_blob = f"{title} {desc}"
            prov_cn_list = _infer_provinces_from_text(text_blob)
            if not prov_cn_list:
                prov_cn_list = ["全国"]

            for prov_cn in prov_cn_list:
                entries.append({
                    "source": "CMA",
                    "title": title,
                    "level": level,
                    "region": PROVINCES.get(prov_cn, prov_cn),
                    "summary": desc.strip(),
                    "link": None,
                    "published": pub,
                    "timestamp": datetime.fromisoformat(pub).timestamp() if pub else now_ts,
                })

        except Exception:
            logging.exception("[CMA PARSE ERROR]")
            continue

    return {"source": "CMA", "entries": entries}

# Registry compatibility
async def scrape_async(conf, client):
    return await scrape_cma_async(conf, client)

async def scrape(conf, client):
    return await scrape_cma_async(conf, client)
