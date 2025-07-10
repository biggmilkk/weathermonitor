import feedparser

def scrape(url):
    feed = feedparser.parse(url)
    items = []
    for entry in feed.entries[:5]:  # Limit to 5 items
        items.append({
            "title": entry.title,
            "summary": entry.get("summary", ""),
            "published": entry.published,
            "link": entry.link
        })
    return {
        "source": url,
        "feed_title": feed.feed.get("title", "NWS Feed"),
        "entries": items
    }
