# scraper/pagasa.py
import asyncio
import re
from typing import List, Dict
from urllib.parse import urljoin

import httpx
from xml.etree import ElementTree as ET

ATOM_NS = {"a": "http://www.w3.org/2005/Atom"}
CAP_NS  = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

def _t(x: str | None) -> str:
    return (x or "").strip()

def _is_cap_url(url: str) -> bool:
    return bool(re.search(r"\.cap(?:$|\?)", url or "", flags=re.IGNORECASE))

def _abs(base: str, href: str) -> str:
    return urljoin(base, href)

async def _get(client: httpx.AsyncClient, url: str) -> httpx.Response:
    return await client.get(url, timeout=30)

def _parse_cap_xml(xml_bytes: bytes) -> Dict:
    """
    Parse a CAP 1.2 <alert> into the app's schema.
    Required by your renderer:
      - title (or headline)
      - summary/description
      - region
      - bucket (event)
      - published (ISO string)
      - link (filled by caller with the CAP URL)
    """
    root = ET.fromstring(xml_bytes)

    # simple helpers to find text with CAP namespace
    def cap_text(tag: str) -> str:
        return _t(root.findtext(tag, namespaces=CAP_NS))

    identifier = cap_text("cap:identifier")
    sent       = cap_text("cap:sent")

    info = root.find("cap:info", CAP_NS)
    event       = _t(info.findtext("cap:event", namespaces=CAP_NS)) if info is not None else ""
    headline    = _t(info.findtext("cap:headline", namespaces=CAP_NS)) if info is not None else ""
    description = _t(info.findtext("cap:description", namespaces=CAP_NS)) if info is not None else ""

    # Collect all <areaDesc> and optionally CAP "Region" parameter if present
    regions: List[str] = []
    if info is not None:
        # <parameter><valueName>layer:Google:Region:0.1</valueName><value>Region 11 (Davao Region)</value>
        for p in info.findall("cap:parameter", CAP_NS):
            vn = _t(p.findtext("cap:valueName", namespaces=CAP_NS))
            if "Region" in vn:
                vv = _t(p.findtext("cap:value", namespaces=CAP_NS))
                if vv:
                    regions.append(vv)

        for area in info.findall("cap:area", CAP_NS):
            desc = _t(area.findtext("cap:areaDesc", namespaces=CAP_NS))
            if desc:
                regions.append(desc)

    # Conservative, readable region label
    region = ", ".join(dict.fromkeys([r for r in regions if r]))  # dedupe while preserving order

    title = headline or event or identifier or "PAGASA Alert"

    return {
        "id": identifier or None,
        "title": title,
        "summary": description,      # renderer will show summary/description if present
        "region": region,
        "bucket": event,             # like NWS event; useful if you later add grouping
        "published": sent,           # your app parses ISO8601 already
        # "link" is assigned by caller to the CAP URL we fetched
    }

async def scrape_pagasa_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    conf:
      - url: index URL (default https://publicalert.pagasa.dost.gov.ph/feeds/)
      - per_feed_limit: max entries taken from the Atom (default 200)
      - max_caps: total cap files to fetch across discovered links (default 400)
    returns: {"entries": [...], "source": {...}}
    """
    index_url      = conf.get("url", "https://publicalert.pagasa.dost.gov.ph/feeds/")
    per_feed_limit = int(conf.get("per_feed_limit", 200))
    max_caps       = int(conf.get("max_caps", 400))

    # 1) fetch the index (Atom per your sample)
    r = await _get(client, index_url)
    r.raise_for_status()
    cap_urls: List[str] = []

    # Try Atom parse first
    try:
        root = ET.fromstring(r.content)
        # Collect <entry><link type="application/cap+xml" href="..."/>
        for entry in root.findall("a:entry", ATOM_NS):
            for link in entry.findall("a:link", ATOM_NS):
                typ  = (link.attrib.get("type") or "").lower()
                href = link.attrib.get("href", "")
                if "application/cap+xml" in typ or _is_cap_url(href):
                    cap_urls.append(_abs(index_url, href))
        # Defensive: cap the number coming from feed
        cap_urls = cap_urls[:per_feed_limit] or cap_urls
    except Exception:
        # If it wasn't Atom for some reason, regex .cap links from HTML
        cap_urls = re.findall(r'href=["\']([^"\']+\.cap[^"\']*)', r.text, flags=re.IGNORECASE)

    # Safety: unique + global cap
    seen = set()
    unique_caps = []
    for u in cap_urls:
        if u not in seen:
            seen.add(u)
            unique_caps.append(u)
    unique_caps = unique_caps[:max_caps]

    # 2) fetch each CAP concurrently and normalize
    async def _fetch_one(url: str):
        try:
            res = await _get(client, url)
            res.raise_for_status()
            entry = _parse_cap_xml(res.content)
            entry["link"] = url  # link to the actual CAP you opened in your browser
            return entry
        except Exception:
            return None

    entries_raw = await asyncio.gather(*[_fetch_one(u) for u in unique_caps])
    entries = [e for e in entries_raw if e]

    # 3) sort newest→oldest by CAP <sent>
    entries.sort(key=lambda e: e.get("published") or "", reverse=True)

    return {"entries": entries, "source": {"url": index_url, "total_caps": len(entries)}}
