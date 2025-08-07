import json
from typing import Any, Dict
from datetime import datetime


async def scrape_jma_async(conf: Dict[str, Any], client) -> Dict[str, Any]:
    """
    Scrapes JMA warnings from their map.json, extracts warning levels for specified phenomena,
    and returns a feed-like dict for weathermonitor.
    """
    # Configuration provides mappings from area codes to names and phenomena definitions
    area_codes: Dict[str, str] = conf.get('area_codes', {})
    content: Dict[str, Any] = conf.get('content', {})

    # Extract list of phenomenon mappings (value -> English name)
    values = content.get('values') or content.get('phenomena') or []
    ph_map = {item['value']: item['enName'] for item in values}

    # URL for JMA warning map
    url = 'https://www.jma.go.jp/bosai/warning/data/warning/map.json'
    resp = await client.get(url)
    data = await resp.json()

    # Determine report/update time
    rep_time = data.get('reportDatetime')
    if not rep_time and 'timeSeries' in data:
        ts = data['timeSeries'][0].get('timeDefines', [])
        rep_time = ts[0] if ts else None
    if rep_time:
        dt = datetime.fromisoformat(rep_time)
        updated = dt.strftime('%H:%M UTC %B %d')
    else:
        updated = ''

    # JMA may use key 'warnings' or 'warning'
    warns = data.get('warnings') or data.get('warning') or []

    # Phenomena of interest for warnings
    interested = {
        'inundation',  # Heavy Rain (Inundation)
        'landslide',   # Heavy Rain (Landslide)
        'flood',       # Flood
        'wind',        # Storm/Gale
        'wave',        # High Wave
        'tide',        # Storm Surge
        'thunder',     # Thunderstorm
        'fog'          # Dense Fog
    }

    items = []
    for warn in warns:
        code = warn.get('code') or warn.get('value')
        if code not in interested:
            continue
        level = warn.get('level')
        # Skip if no active warning
        if not level or level == '0':
            continue
        # Map numeric level to labels/colors
        if level == '2':
            lvl = 'Orange'
        elif level in ('3', '4'):
            lvl = 'Red'
        else:
            lvl = 'Yellow'
        ph = ph_map.get(code, code)

        for area in warn.get('areas', []):
            # Area may be dict with code/name or plain code
            if isinstance(area, dict):
                area_code = area.get('code')
            else:
                area_code = area
            name = area_codes.get(area_code) or area_code
            items.append({
                'title': f'{name}: {ph}',
                'level': lvl,
            })

    return {
        'title': 'JMA Warnings',
        'url': url,
        'updated': updated,
        'items': items
    }
