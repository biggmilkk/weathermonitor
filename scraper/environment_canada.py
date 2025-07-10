import feedparser

def is_red_warning(title: str) -> bool:
    title = title.strip().upper()
    if title.startswith("NO ALERTS"):
        return False
    if "WARNING" in title:
        return True
    if "SEVERE THUNDERSTORM WATCH" in title:
        return True
    return False

def scrape(url, region_name=None, province=None):
    feed = feedparser.parse(url)
    entries = []

    for entry in feed.entries:
        raw_title = entry.get("title", "")
        if not is_red_warning(raw_title):
            continue

        entries.append({
            "title": raw_title,
            "summary": entry.get("summary", "")[:500],
            "link": entry.get("link", ""),
            "published": entry.get("published", ""),
            "region": region_name or "Unknown",
            "province": province or "Unknown"
        })

    return {
        "entries": entries,
        "source": url
    }
