from __future__ import annotations

import asyncio
import re
from typing import List, Dict
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

import httpx

# Namespaces
ATOM_NS = {"a": "http://www.w3.org/2005/Atom"}
CAP_NS  = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}


# ----------------------------- Helpers ---------------------------------------

def _t(x: str | None) -> str:
    """Trim helper."""
    return (x or "").strip()

def _is_cap_url(url: str) -> bool:
    """Heuristic: a CAP file URL ends with .cap (optionally with a query string)."""
    return bool(re.search(r"\.cap(?:$|\?)", (url or ""), flags=re.IGNORECASE))

def _abs(base: str, href: str) -> str:
    """Resolve relative URLs."""
    return urljoin(base, href)

async def _get(client: httpx.AsyncClient, url: str) -> httpx.Response:
    """HTTP GET with the shared AsyncClient (timeouts set by caller)."""
    return await client.get(url, timeout=30)

def _unique(seq: List[str]) -> List[str]:
    """Stable de-duplication."""
    seen = set()
    out = []
    for s in seq:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

def _cap_to_public_page(cap_url: str) -> str | None:
    """
    Convert a PAGASA CAP file URL to the public alert page on panahon.gov.ph.
    Example:
      input:  https://publicalert.pagasa.dost.gov.ph/output/gfa/<UUID>.cap
      output: https://www.panahon.gov.ph/public-alerts/<UUID>
    Fallback to None if we can't confidently extract the UUID.
    """
    try:
        path = urlparse(cap_url).path
        # Canonical form: /output/<bucket>/<uuid>.cap
        m = re.search(r"/output/[^/]+/([0-9a-fA-F-]{36})\.cap$", path)
        if not m:
            # Fallback: last path segment <slug>.cap (still try to use it as the slug)
            m = re.search(r"/([^/]+)\.cap$", path)
        if m:
            slug = m.group(1)
            # Only accept well-formed UUID; otherwise still try (some feeds may use non-UUID slugs)
            if re.fullmatch(r"[0-9a-fA-F-]{36}", slug):
                return f"https://www.panahon.gov.ph/public-alerts/{slug}"
            # If it's not a UUID but still a slug, you may choose to construct anyway:
            return f"https://www.panahon.gov.ph/public-alerts/{slug}"
    except Exception:
        pass
    return None


# --------------------------- CAP parsing -------------------------------------

def _parse_cap_xml(xml_bytes: bytes) -> Dict:
    """
    Parse a CAP 1.2 <alert> into the app's schema.

    Fields mapped:
      - id:        CAP <identifier>
      - title:     CAP <info><headline> or <event> or <identifier>
      - summary:   CAP <info><description>
      - region:    Join of Region parameter + all <areaDesc>
      - bucket:    CAP <info><event>   (useful for grouping, like NWS 'event')
      - published: CAP <sent>          (ISO8601; app will parse/format)
      - link:      (assigned by caller)
    """
    root = ET.fromstring(xml_bytes)

    def cap_text(tag: str) -> str:
        return _t(root.findtext(tag, namespaces=CAP_NS))

    identifier = cap_text("cap:identifier")
    sent       = cap_text("cap:sent")

    info = root.find("cap:info", CAP_NS)
    event       = _t(info.findtext("cap:event", namespaces=CAP_NS)) if info is not None else ""
    headline    = _t(info.findtext("cap:headline", namespaces=CAP_NS)) if info is not None else ""
    description = _t(info.findtext("cap:description", namespaces=CAP_NS)) if info is not None else ""

    # Build region from:
    #   - <parameter><valueName>...Region...</valueName><value>Region X (Name)</value>
    #   - Each <area><areaDesc>
    regions: List[str] = []
    if info is not None:
        for p in info.findall("cap:parameter", CAP_NS):
            vn = _t(p.findtext("cap:valueName", namespaces=CAP_NS))
            if vn and "Region" in vn:
                vv = _t(p.findtext("cap:value", namespaces=CAP_NS))
                if vv:
                    regions.append(vv)

        for area in info.findall("cap:area", CAP_NS):
            desc = _t(area.findtext("cap:areaDesc", namespaces=CAP_NS))
            if desc:
                regions.append(desc)

    # Deduplicate while preserving order
    region_str = ", ".join(dict.fromkeys(regions)) if regions else ""

    title = headline or event or identifier or "PAGASA Alert"

    return {
        "id": identifier or None,
        "title": title,
        "summary": description,
        "region": region_str,
        "bucket": event,
        "published": sent,
        # "link" filled by caller
    }


# ----------------------------- Scraper ---------------------------------------

async def scrape_pagasa_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Scrape PAGASA CAP alerts.

    conf:
      - url: base index URL (default: https://publicalert.pagasa.dost.gov.ph/feeds/)
      - per_feed_limit: number of <entry> CAP links to take from Atom (default: 200)
      - max_caps: hard cap on total CAP files fetched (default: 400)

    Returns:
      {"entries": [...], "source": {"url": index_url, "total_caps": len(entries)}}
    """
    index_url      = conf.get("url", "https://publicalert.pagasa.dost.gov.ph/feeds/")
    per_feed_limit = int(conf.get("per_feed_limit", 200))
    max_caps       = int(conf.get("max_caps", 400))

    # 1) Fetch the index (Atom per current site)
    r = await _get(client, index_url)
    r.raise_for_status()

    cap_urls: List[str] = []

    # Try to parse as Atom first (preferred)
    parsed_as_atom = False
    try:
        root = ET.fromstring(r.content)
        # Collect <entry><link type="application/cap+xml" href="..."/>
        for entry in root.findall("a:entry", ATOM_NS):
            for link in entry.findall("a:link", ATOM_NS):
                typ  = (link.attrib.get("type") or "").lower()
                href = link.attrib.get("href", "")
                if "application/cap+xml" in typ or _is_cap_url(href):
                    cap_urls.append(_abs(index_url, href))
        parsed_as_atom = True
    except Exception:
        # Will fallback to regex scrape from HTML below
        parsed_as_atom = False

    # If Atom parse worked, cap the number to per_feed_limit
    if parsed_as_atom:
        cap_urls = cap_urls[:per_feed_limit]
    else:
        # Fallback: HTML index — discover .cap links via regex
        cap_urls = re.findall(
            r'href=["\']([^"\']+\.cap[^"\']*)',
            r.text,
            flags=re.IGNORECASE,
        )

    # Unique + global cap
    cap_urls = _unique(cap_urls)[:max_caps]

    # 2) Fetch each CAP concurrently and normalize
    async def _fetch_one(url: str):
        try:
            res = await _get(client, url)
            res.raise_for_status()
            entry = _parse_cap_xml(res.content)
            # Prefer public alert page; fallback to raw CAP URL
            public_link = _cap_to_public_page(url)
            entry["link"] = public_link or url
            return entry
        except Exception:
            return None

    entries_raw = await asyncio.gather(*[_fetch_one(u) for u in cap_urls])
    entries = [e for e in entries_raw if e]

    # 3) Sort newest → oldest by CAP <sent> (ISO8601)
    entries.sort(key=lambda e: e.get("published") or "", reverse=True)

    return {"entries": entries, "source": {"url": index_url, "total_caps": len(entries)}}
