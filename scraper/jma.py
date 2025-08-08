import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

async def scrape_jma_async(conf, client=None):
    """
    Scrape JMA warning table from the HTML front-end (English version).
    Filters only 'Warning' level alerts.
    """
    url = "https://www.jma.go.jp/bosai/warning/#lang=en"
    entries = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle")

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")

        # Adjust selector to match actual table layout
        for row in soup.select("table tr"):
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) < 3:
                continue

            region, phenomenon, level = cols[0], cols[1], cols[2]
            if "Warning" not in level:
                continue

            entries.append({
                "title": f"{phenomenon} – {region}",
                "region": region,
                "level": level,
                "type": phenomenon,
                "summary": "",
                "published": datetime.datetime.utcnow().isoformat() + "Z",
                "link": url
            })

        await browser.close()

    return {"entries": entries}
