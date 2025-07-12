import aiohttp
import asyncio
import xml.etree.ElementTree as ET
import logging
import re

async def fetch_and_parse(session, region):
    url = region.get("ATOM URL")
    if not url:
        return []

    try:
        async with session.get(url, timeout=10) as resp:
            text = await resp.text()
            root = ET.fromstring(text)
            entries = []

            for entry in root.findall("{http://www.w3.org/2005/Atom}entry"):
                title_elem = entry.find("{http://www.w3.org/2005/Atom}title")
                summary_elem = entry.find("{http://www.w3.org/2005/Atom}summary")
                link_elem = entry.find("{http://www.w3.org/2005/Atom}link")
                published_elem = entry.find("{http://www.w3.org/2005/Atom}published")

                title = title_elem.text if title_elem is not None else ""
                if not title:
                    continue

                # Skip ended alerts
                if "ENDED" in title.upper():
                    continue
                
                # Skip "No alert" entries
                if title.strip().upper().startswith("NO ALERT"):
                    continue

                alert_type = re.split(r",\s*", title)[0].strip().upper()
                if "WARNING" not in alert_type and alert_type != "SEVERE THUNDERSTORM WATCH":
                    continue
                
                entries.append({
                    "title": alert_type,
                    "summary": summary_elem.text[:500] if summary_elem is not None else "",
                    "link": link_elem.attrib.get("href", "") if link_elem is not None else "",
                    "published": published_elem.text if published_elem is not None else "",
                    "region": region.get("Region Name", ""),
                    "province": region.get("Province-Territory", "")
                })

            return entries

    except Exception as e:
        return []

async def scrape_ec(sources):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_and_parse(session, region) for region in sources if region.get("ATOM URL")]
        results = await asyncio.gather(*tasks)
        all_entries = []
        for result in results:
            all_entries.extend(result)
        logging.warning(f"[EC DEBUG] Successfully fetched {len(all_entries)} alerts")
        return {
            "entries": all_entries,
            "source": "Environment Canada"
        }
