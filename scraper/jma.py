import datetime
from dateutil import tz
from feeds import FeedEntry
import httpx

async def scrape_jma_warning_async(conf, client: httpx.AsyncClient):
    """
    Scrape the JMA warning map JSON and return a dict with an 'entries' list.
    conf should include at least {'url': 'https://www.jma.go.jp/bosai/warning/data/warning/map.json'}.
    """
    url = conf.get("url") or "https://www.jma.go.jp/bosai/warning/data/warning/map.json"
    # 1) fetch
    resp = await client.get(url)
    resp.raise_for_status()
    payload = resp.json()

    # 2) parse into feed‐style entries
    entries = []
    for feature in payload.get("features", []):
        props = feature.get("properties", {})
        level = props.get("level")  # advisory=20, warning=30, etc.
        # only include active advisories/warnings:
        if level and level >= 20:
            # published time: use JMA timestamp if available, else now
            ts = props.get("time")
            if ts:
                # JMA times are in milliseconds since epoch
                published = datetime.datetime.fromtimestamp(ts / 1000, tz=tz.UTC)
            else:
                published = datetime.datetime.now(tz=tz.UTC)

            title = f"{props.get('office_name', 'JMA')} – {props.get('type', 'Unknown')}"
            description = props.get("status_text", "").strip() or props.get("text", "")
            link = url  # no per‐feature link; point users to the JSON endpoint or the page

            entries.append({
                "title": title,
                "description": description,
                "published": published.isoformat(),
                "link": link,
                # carry along raw fields in case you want to render them:
                "level": level,
                "office": props.get("office_name"),
                "region": props.get("area_name"),
                "type": props.get("type"),
            })

    return {
        "entries": entries,
        "source": conf
    }
