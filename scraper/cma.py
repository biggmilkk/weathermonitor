# scraper/cma.py
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

import httpx

__all__ = ["scrape_cma_async", "scrape_async", "scrape"]

API_URL = "https://weather.cma.cn/api/map/alarm?adcode="

# Only keep severe alerts
ALLOWED_LEVELS = {"橙色", "红色"}

LEVEL_MAP = {
    "橙色": "Orange",
    "红色": "Red",
}

# Province buckets (CN → EN)
PROVINCES = {
    "北京": "Beijing",
    "天津": "Tianjin",
    "上海": "Shanghai",
    "重庆": "Chongqing",
    "河北": "Hebei",
    "山西": "Shanxi",
    "辽宁": "Liaoning",
    "吉林": "Jilin",
    "黑龙江": "Heilongjiang",
    "江苏": "Jiangsu",
    "浙江": "Zhejiang",
    "安徽": "Anhui",
    "福建": "Fujian",
    "江西": "Jiangxi",
    "山东": "Shandong",
    "河南": "Henan",
    "湖北": "Hubei",
    "湖南": "Hunan",
    "广东": "Guangdong",
    "海南": "Hainan",
    "四川": "Sichuan",
    "贵州": "Guizhou",
    "云南": "Yunnan",
    "陕西": "Shaanxi",
    "甘肃": "Gansu",
    "青海": "Qinghai",
    "台湾": "Taiwan",
    "内蒙古": "Inner Mongolia",
    "广西": "Guangxi",
    "西藏": "Tibet",
    "宁夏": "Ningxia",
    "新疆": "Xinjiang",
    "香港": "Hong Kong",
    "澳门": "Macau",
}

def _parse_pubtime(s: str) -> Optional[str]:
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").isoformat()
    except Exception:
        return None


def _infer_provinces_from_text(text: str) -> List[str]:
    """
    Return list of province CN names mentioned in text.
    """
    hits = []
    for cn in PROVINCES.keys():
        if cn in text:
            hits.append(cn)
    return hits


async def scrape_cma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    CMA JSON scraper with province buckets.
    - Uses official CMA alarm API
    - Emits ORANGE + RED only
    - Buckets alerts by province (like NWS states)
    """

    entries: List[Dict[str, Any]] = []
    errors: List[str] = []

    try:
        resp = await client.get(API_URL, timeout=15.0)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        logging.error(f"[CMA FETCH ERROR] {e}")
        return {
            "source": "CMA",
            "entries": [],
            "error": str(e),
        }

    alarms = payload.get("data") or []
    if not isinstance(alarms, list):
        return {"source": "CMA", "entries": []}

    for alarm in alarms:
        try:
            level_cn = alarm.get("level")
            if level_cn not in ALLOWED_LEVELS:
                continue

            title = alarm.get("title") or "气象预警"
            content = alarm.get("content") or ""
            pub = _parse_pubtime(alarm.get("pubTime", ""))

            url = alarm.get("url")
            if url and url.startswith("/"):
                url = "https://weather.cma.cn" + url

            # ---- Province inference ----
            provinces = []

            # 1) Structured region fields (if present)
            for key in ("areaName", "areas", "region"):
                v = alarm.get(key)
                if isinstance(v, str):
                    provinces.extend(_infer_provinces_from_text(v))

            # 2) Fallback: scan content
            if not provinces:
                provinces = _infer_provinces_from_text(content)

            # 3) Absolute fallback
            if not provinces:
                provinces = ["全国"]

            for prov_cn in provinces:
                prov_en = PROVINCES.get(prov_cn, prov_cn)

                entries.append(
                    {
                        "title": title,
                        "level": LEVEL_MAP[level_cn],
                        "summary": content.strip(),
                        "link": url,
                        "published": pub,
                        "region": prov_cn,
                        "state_code": prov_cn,
                        "state": prov_en,
                        "bucket": prov_en,
                        "event": title,
                    }
                )

        except Exception as e:
            errors.append(str(e))

    logging.warning(f"[CMA DEBUG] Parsed {len(entries)} province alerts")

    out = {
        "source": "CMA",
        "entries": entries,
    }
    if errors:
        out["error"] = "; ".join(errors)

    return out


# Registry compatibility
async def scrape_async(conf, client):
    return await scrape_cma_async(conf, client)


async def scrape(conf, client):
    return await scrape_cma_async(conf, client)
