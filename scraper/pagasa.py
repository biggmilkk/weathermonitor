# scraper/pagasa.py

from __future__ import annotations

import asyncio
import re
from typing import List, Dict, Tuple
from urllib.parse import urljoin
from xml.etree import ElementTree as ET
from datetime import datetime
import httpx

# ----------------------- Namespaces / Filters -----------------------

ATOM_NS = {"a": "http://www.w3.org/2005/Atom"}
CAP_NS  = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}
ALLOWED_SEVERITIES = {"severe", "moderate"}

# ----------------------------- Helpers -----------------------------

def _t(x: str | None) -> str:
    return (x or "").strip()

def _is_cap_url(url: str) -> bool:
    return bool(re.search(r"\.cap(?:$|\?)", (url or ""), flags=re.IGNORECASE))

def _abs(base: str, href: str) -> str:
    return urljoin(base, href)

async def _get(client: httpx.AsyncClient, url: str) -> httpx.Response:
    return await client.get(url, timeout=30)

def _unique(seq: List[str]) -> List[str]:
    seen, out = set(), []
    for s in seq:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def _title_from_event_and_severity(event: str, severity: str, headline: str, identifier: str) -> str:
    def _has_level(s: str) -> bool:
        return bool(re.search(r"\((?:Severe|Moderate|Minor|Extreme|Intermediate|Final)\)", s or "", re.I))
    title = event or headline or identifier or "PAGASA Alert"
    if event and not _has_level(title):
        sev = (severity or "").strip()
        if sev and sev.lower() != "unknown":
            title = f"{event} ({sev.title()})"
    return title

def _to_ts(iso_str: str | None) -> float:
    if not iso_str: return 0.0
    try:
        return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0

def _norm_regions(region_str: str) -> Tuple[str, ...]:
    parts = [p.strip().lower() for p in (region_str or "").split(",") if p.strip()]
    return tuple(sorted(set(parts)))

def _content_key(e: Dict) -> Tuple:
    return (
        (e.get("bucket") or "").strip().lower(),
        (e.get("severity") or "").strip().lower(),
        (e.get("certainty") or "").strip().lower(),
        tuple(sorted((x or "").strip().lower() for x in e.get("response_types") or [])),
        _norm_regions(e.get("region") or ""),
    )

# --------------------------- CAP parsing ---------------------------

def _parse_cap_xml(xml_bytes: bytes) -> Dict:
    root = ET.fromstring(xml_bytes)

    def cap_text(tag: str) -> str:
        return _t(root.findtext(tag, namespaces=CAP_NS))

    identifier = cap_text("cap:identifier")
    sent       = cap_text("cap:sent")
    msg_type   = cap_text("cap:msgType")
    references = cap_text("cap:references")

    ref_ids = re.findall(r"[0-9a-fA-F-]{8,}", references) if references else []

    infos = root.findall("cap:info", CAP_NS)
    primary = infos[0] if infos else None

    event       = _t(primary.findtext("cap:event", namespaces=CAP_NS)) if primary is not None else ""
    headline    = _t(primary.findtext("cap:headline", namespaces=CAP_NS)) if primary is not None else ""
    description = _t(primary.findtext("cap:description", namespaces=CAP_NS)) if primary is not None else ""
    severity    = _t(primary.findtext("cap:severity", namespaces=CAP_NS)) if primary is not None else ""

    response_types: List[str] = []
    for inf in infos:
        for rt in inf.findall("cap:responseType", CAP_NS):
            v = _t(rt.text)
            if v:
                response_types.append(v)

    certainty = ""
    for inf in infos:
        c = _t(inf.findtext("cap:certainty", namespaces=CAP_NS))
        if c:
            certainty = c
            break

    regions: List[str] = []
    if primary is not None:
        for p in primary.findall("cap:parameter", CAP_NS):
            vn = _t(p.findtext("cap:valueName", namespaces=CAP_NS))
            if vn and "Region" in vn:
                vv = _t(p.findtext("cap:value", namespaces=CAP_NS))
                if vv:
                    regions.append(vv)
        for area in primary.findAll("cap:area", CAP_NS) if hasattr(primary, "findAll") else primary.findall("cap:area", CAP_NS):
            desc = _t(area.findtext("cap:areaDesc", namespaces=CAP_NS))
            if desc:
                regions.append(desc)

    region_str = ", ".join(dict.fromkeys(regions)) if regions else ""
    title = _title_from_event_and_severity(event, severity, headline, identifier)

    return {
        "id": identifier or None,
        "title": title,
        "summary": description,
        "region": region_str,
        "bucket": event,
        "severity": severity,
        "response_types": response_types,
        "certainty": certainty,
        "msg_type": msg_type,
        "published": sent,
        "references_ids": ref_ids,
    }

# ---------------------------- Dedup logic --------------------------

def _dedupe_updates(entries: List[Dict]) -> List[Dict]:
    id_to_ckey: Dict[str, Tuple] = {}
    ckeys: Dict[int, Tuple] = {}
    for i, e in enumerate(entries):
        ck = _content_key(e)
        ckeys[i] = ck
        if e.get("id"):
            id_to_ckey[e["id"]] = ck

    groups: Dict[Tuple, Dict] = {}
    for i, e in enumerate(entries):
        ck = ckeys[i]
        chain_ck = None
        for rid in e.get("references_ids") or []:
            if rid in id_to_ckey:
                chain_ck = id_to_ckey[rid]
                break
        if chain_ck is None:
            chain_ck = ck

        cur = groups.get(chain_ck)
        if not cur or _to_ts(e.get("published")) > _to_ts(cur.get("published")):
            groups[chain_ck] = e

    return list(groups.values())

# ------------------------------ Scraper ---------------------------

async def scrape_pagasa_async(conf: dict, client: httpx.AsyncClient) -> dict:
    index_url      = conf.get("url", "https://publicalert.pagasa.dost.gov.ph/feeds/")
    per_feed_limit = int(conf.get("per_feed_limit", 200))
    max_caps       = int(conf.get("max_caps", 400))

    r = await _get(client, index_url)
    r.raise_for_status()

    cap_urls: List[str] = []
    parsed_as_atom = False
    try:
        root = ET.fromstring(r.content)
        for entry in root.findall("a:entry", ATOM_NS):
            for link in entry.findall("a:link", ATOM_NS):
                typ  = (link.attrib.get("type") or "").lower()
                href = link.attrib.get("href", "")
                if "application/cap+xml" in typ or _is_cap_url(href):
                    cap_urls.append(_abs(index_url, href))
        parsed_as_atom = True
    except Exception:
        parsed_as_atom = False

    if parsed_as_atom:
        cap_urls = cap_urls[:per_feed_limit]
    else:
        cap_urls = re.findall(r'href=["\']([^"\']+\.cap[^"\']*)', r.text, flags=re.I)

    cap_urls = _unique(cap_urls)[:max_caps]

    async def _fetch_one(url: str):
        try:
            res = await _get(client, url)
            res.raise_for_status()
            e = _parse_cap_xml(res.content)

            if (e.get("msg_type") or "").strip().lower() == "cancel":
                return None

            sev = (e.get("severity") or "").strip().lower()
            if sev not in ALLOWED_SEVERITIES:
                return None

            e["link"] = url  # link to CAP XML (public page may not exist)
            return e
        except Exception:
            return None

    entries_raw = await asyncio.gather(*[_fetch_one(u) for u in cap_urls])
    entries = [e for e in entries_raw if e]

    entries = _dedupe_updates(entries)
    entries.sort(key=lambda e: e.get("published") or "", reverse=True)

    return {"entries": entries, "source": {"url": index_url, "total_caps": len(entries)}}
