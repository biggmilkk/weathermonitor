import datetime
from playwright.async_api import async_playwright

async def scrape_jma_async(conf, client=None):
    entries = []
    url = "https://www.jma.go.jp/bosai/warning/#lang=en"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle")

        # Extract table rows
        rows = await page.query_selector_all("table tr")
        for row in rows:
            cells = await row.query_selector_all("td")
            if not cells:
                continue
            text_cells = [await c.inner_text() for c in cells]

            # Example filtering logic: look for "Warning"
            if not any("Warning" in t for t in text_cells):
                continue

            region = text_cells[0].strip()
            phenomenon = text_cells[1].strip()
            level = text_cells[2].strip()

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
