import feedparser
import json
import os

def load_ec_sources():
    json_path = os.path.join(os.path.dirname(__file__), "../environment_canada_sources.json")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [entry["url"] for entry in data if entry.get("url")]

def scrape(_=None):
    feed_urls = load_ec_sources()
    entries = []

    for url in feed_urls:
        try:
            feed = feedparser.parse(url)
            for item in feed.entries:
                entries.append({
                    "title": item.get("title", "No title"),
                    "summary": item.get("summary", "")[:500],
                    "link": item.get("link", ""),
                    "published": item.get("published", "")
                })
        except Exception:
            continue

    entries = sorted(entries, key=lambda x: x.get("published", ""), reverse=True)

    return {
        "feed_title": "Environment Canada Alerts",
        "entries": entries[:50],  # Limit for display
        "source": "https://weather.gc.ca/rss/"
    }
