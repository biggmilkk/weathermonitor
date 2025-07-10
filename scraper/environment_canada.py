import feedparser

def is_red_warning(title: str) -> bool:
    title = title.strip().upper()
    return (
        "WARNING" in title and
        not title.startswith("NO ALERTS") and
        "WATCH" not in title
    ) or "SEVERE THUNDERSTORM WATCH" in title

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
            "region": region_name or "",
            "province": province or ""
        })

    return {
        "entries": entries,
        "source": url
    }
