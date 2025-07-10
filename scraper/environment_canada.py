import feedparser

def is_red_warning(title: str) -> bool:
    """Returns True if the title matches a red warning or Severe Thunderstorm Watch alert."""
    title = title.strip().upper()
    if title.startswith("NO ALERTS"):
        return False
    if "WARNING" in title:
        return True
    if "SEVERE THUNDERSTORM WATCH" in title:
        return True
    return False
    )

def scrape(url):
    feed = feedparser.parse(url)
    entries = []

    for entry in feed.entries:
        raw_title = entry.get("title", "")
        if not is_red_warning(raw_title):
            continue  # Skip non-red warnings

        entries.append({
            "title": raw_title,
            "summary": entry.get("summary", "")[:500],
            "link": entry.get("link", ""),
            "published": entry.get("published", "")
        })

    return {
        "entries": entries,
        "source": url
    }
