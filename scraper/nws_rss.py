import feedparser

def scrape(url):
    feed = feedparser.parse(url)
    entries = []

    for item in feed.entries[:10]:  # limit to 10 items
        entries.append({
            "title": item.get("title", "No Title"),
            "summary": item.get("summary", ""),
            "link": item.get("link", ""),
            "published": item.get("published", "")
        })

    return {
        "feed_title": feed.feed.get("title", "NWS RSS Feed"),
        "entries": entries,
        "source": url
    }
