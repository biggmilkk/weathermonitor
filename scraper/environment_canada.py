import feedparser

def scrape(url):
    feed = feedparser.parse(url)
    entries = []
    for entry in feed.entries:
        entries.append({
            "title": entry.get("title", "No Title"),
            "summary": entry.get("summary", "")[:500],
            "link": entry.get("link", ""),
            "published": entry.get("published", "")
        })
    return {
        "entries": entries,
        "source": url
    }
