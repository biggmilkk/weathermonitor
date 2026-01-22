# scraper/cma.py
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

__all__ = ["scrape_cma_async", "scrape_async", "scrape"]

API_URL = "https://weather.cma.cn/api/map/alarm?adcode="

# Required to avoid 403
CMA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Referer": "https://weather.cma.cn/",
    "Accept": "application/json, text/plain, */*",
}

# GB/T 2260: first 2 digits = province-level code
PROVINCE_CODE_TO_CN = {
    "11": "北京", "12": "天津", "13": "河北", "14": "山西", "15": "内蒙古",
    "21": "辽宁", "22": "吉林", "23": "黑龙江",
    "31": "上海", "32": "江苏", "33": "浙江", "34": "安徽", "35": "福建", "36": "江西", "37": "山东",
    "41": "河南", "42": "湖北", "43": "湖南", "44": "广东", "45": "广西", "46": "海南",
    "50": "重庆", "51": "四川", "52": "贵州", "53": "云南", "54": "西藏",
    "61": "陕西", "62": "甘肃", "63": "青海", "64": "宁夏", "65": "新疆",
    "71": "台湾", "81": "香港", "82": "澳门",
}

# Optional: nicer English bucket labels (your renderer currently shows region CN; keep both)
PROVINCE_CN_TO_EN = {
    "北京": "Beijing", "天津": "Tianjin", "河北": "Hebei", "山西": "Shanxi", "内蒙古": "Inner Mongolia",
    "辽宁": "Liaoning", "吉林": "Jilin", "黑龙江": "Heilongjiang",
    "上海": "Shanghai", "江苏": "Jiangsu", "浙江": "Zhejiang", "安徽": "Anhui", "福建": "Fujian", "江西": "Jiangxi", "山东": "Shandong",
    "河南": "Henan", "湖北": "Hubei", "湖南": "Hunan", "广东": "Guangdong", "广西": "Guangxi", "海南": "Hainan",
    "重庆": "Chongqing", "四川": "Sichuan", "贵州": "Guizhou", "云南": "Yunnan", "西藏": "Tibet",
    "陕西": "Shaanxi", "甘肃": "Gansu", "青海": "Qinghai", "宁夏": "Ningxia", "新疆": "Xinjiang",
    "台湾": "Taiwan", "香港": "Hong Kong", "澳门": "Macau",
}

ALLOWED_LEVELS = {"Orange", "Red"}


def _parse_pubtime(s: Optional[str]) -> Optional[str]:
    """
    CMA examples often use "YYYY/MM/DD HH:MM" in 'effective'.
    Return ISO8601 string (UTC) when we can.
    """
    if not s:
        return None
    s = str(s).strip()

    for fmt in ("%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            pass

    # Leave as-is if parse fails; renderer/controller may still parse it
    return s


def _extract_level(item: Dict[str, Any]) -> Optional[str]:
    """
    CMA payloads vary; reliably detect severity by scanning headline/title/description.
    Works with both '橙色/红色' and '橙/红'.
    """
    blob = " ".join(
        str(x) for x in (
            item.get("headline"),
            item.get("title"),
            item.get("description"),
            item.get("level"),
            item.get("severity"),
            item.get("signalLevelName"),
            item.get("type"),
        ) if x
    )
    blob = re.sub(r"\s+", "", blob)

    if "红" in blob:
        return "Red"
    if "橙" in blob:
        return "Orange"
    if "黄" in blob:
        return "Yellow"
    if "蓝" in blob:
        return "Blue"
    return None


def _province_from_id(item: Dict[str, Any]) -> Tuple[str, str]:
    """
    Reliable province bucketing from numeric prefix in 'id' (GB/T 2260-like).
    Example: "37011641600000_..." -> province code "37" -> 山东.
    Returns (province_cn, province_bucket_en)
    """
    iid = item.get("id")
    if isinstance(iid, str):
        m = re.match(r"^(\d{6,})", iid)
        if m:
            prov_code = m.group(1)[:2]
            prov_cn = PROVINCE_CODE_TO_CN.get(prov_code)
            if prov_cn:
                return prov_cn, PROVINCE_CN_TO_EN.get(prov_cn, prov_cn)

    return "全国", "National"


async def scrape_cma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    CMA v2 JSON scraper:
      - Fetches CMA alarm JSON
      - Filters only Orange/Red
      - Province buckets derived from numeric id prefix
      - No active-time filtering
    """
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
    if not isinstance(alarms, list):
        return {"source": "CMA", "entries": []}

    now_ts = datetime.now(timezone.utc).timestamp()

    for item in alarms:
        try:
            level = _extract_level(item)
            if level not in ALLOWED_LEVELS:
                continue

            title = item.get("title") or item.get("headline") or "CMA Alert"
            desc = item.get("description") or ""
            pub = _parse_pubtime(item.get("effective") or item.get("pubTime") or item.get("publishTime"))

            prov_cn, bucket_en = _province_from_id(item)

            ts = now_ts
            if pub:
                try:
                    ts = datetime.fromisoformat(str(pub).replace("Z", "+00:00")).timestamp()
                except Exception:
                    ts = now_ts

            entries.append(
                {
                    "source": "CMA",
                    "title": title,
                    "level": level,         # "Orange" / "Red"
                    "region": prov_cn,      # renderer shows this (CN)
                    "bucket": bucket_en,    # optional grouping (EN)
                    "summary": desc.strip(),
                    "link": None,           # JSON doesn't provide stable web link here
                    "published": pub,
                    "timestamp": ts,
                }
            )

        except Exception as e:
            errors.append(str(e))

    out: Dict[str, Any] = {"source": "CMA", "entries": entries}
    if errors:
        out["error"] = "; ".join(errors)
    logging.warning("[CMA DEBUG] Parsed %d (Orange/Red) alerts", len(entries))
    return out


# Registry compatibility aliases
async def scrape_async(conf, client):
    return await scrape_cma_async(conf, client)

async def scrape(conf, client):
    return await scrape_cma_async(conf, client)
