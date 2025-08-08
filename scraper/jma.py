import json
from typing import Any, Dict, List, Optional

"""
Playwright-based headless scraper for the JMA warnings SPA page.

This loads the page at https://www.jma.go.jp/bosai/warning/#lang=en, waits for
JMA's JavaScript to render the "warning-table-japan" table, and extracts the
same rows and statuses you see on the site (e.g., Advisory/Warning/Alert/Emergency).

Quick start:
  pip install playwright
  playwright install chromium
  python jma_headless.py --demo  # prints Kagoshima: Satsuma Region

Programmatic use:
  from jma_headless import scrape_warning_table_headless
  data = asyncio.run(scrape_warning_table_headless())

Output shape:
{
  "issued": "Issued at 10:26 JST, 08 Aug. 2025",  # if available
  "groups": [
    {
      "area": "Southern Kyushu and Amami",
      "headers": ["Heavy Rain (Inundation)", ...],
      "rows": [
        {
          "region": "Kagoshima: Satsuma Region",
          "cols": {
            "Heavy Rain (Inundation)": "Warning",
            "Heavy Rain (Landslide)": "Emergency",
            ...
          }
        }, ...
      ]
    }, ...
  ]
}
"""

async def scrape_warning_table_headless(lang: str = "en",
                                        url: Optional[str] = None,
                                        headless: bool = True,
                                        timeout_ms: int = 60000) -> Dict[str, Any]:
    """Return the rendered JMA warning table as structured data using Playwright.

    Requires `pip install playwright` and `playwright install chromium`.
    """
    try:
        from playwright.async_api import async_playwright
    except Exception as e:
        raise RuntimeError(
            "Playwright is required. Install with `pip install playwright` then run `playwright install chromium`."
        ) from e

    url = url or f"https://www.jma.go.jp/bosai/warning/#lang={lang}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(locale="en-US")
        page = await context.new_page()
        await page.goto(url, wait_until="domcontentloaded")

        # Wait for the scrollable table that contains the live data
        selector_table = "div#warning-table-japan .contents-wide-table-scroll table.warning-table"
        await page.wait_for_selector(selector_table, state="visible", timeout=timeout_ms)

        # Issued timestamp (best-effort)
        issued_text = None
        try:
            issued_text = await page.locator("#warning-table-japan .contents-header th").nth(0).inner_text()
            issued_text = issued_text.strip()
        except Exception:
            pass

        # Extract everything inside the page for robustness
        groups: List[Dict[str, Any]] = await page.evaluate(
            """
(() => {
  const LEVEL = {20: 'Advisory', 30: 'Warning', 40: 'Alert', 50: 'Emergency'};
  const tbl = document.querySelector('div#warning-table-japan .contents-wide-table-scroll table.warning-table');
  if (!tbl) return [];
  const groups = [];
  let current = null;
  let headers = [];
  const norm = (t) => (t || '').replace(/\s+/g, ' ').trim();

  for (const tr of tbl.querySelectorAll('tr')) {
    if (tr.classList.contains('contents-header') && tr.querySelector('.contents-area')) {
      const ths = Array.from(tr.querySelectorAll('th'));
      const area = norm(ths[0]?.innerText || '');
      headers = ths.slice(1).map(th => norm(th.innerText));
      current = { area, headers, rows: [] };
      groups.push(current);
      continue;
    }
    const th = tr.querySelector('th.contents-clickable');
    if (!th || !current) continue;
    const region = norm(th.innerText);
    const tds = Array.from(tr.querySelectorAll('td'));
    const cols = {};
    tds.forEach((td, i) => {
      let status = td.getAttribute('title') || norm(td.innerText) || '';
      if (td.classList.contains('contents-missing')) status = '';
      for (const cls of td.classList) {
        const m = /contents-level(\d+)/.exec(cls);
        if (m) {
          const lvl = Number(m[1]);
          status = LEVEL[lvl] || status;
        }
      }
      const header = headers[i] || `Col${i+1}`;
      cols[header] = status || '—';
    });
    current.rows.push({ region, cols });
  }
  return groups;
})();
            """
        )

        await browser.close()
        return {"issued": issued_text, "groups": groups}


# Small CLI for quick checks
if __name__ == "__main__":
    import argparse, asyncio
    parser = argparse.ArgumentParser(description="Scrape JMA warning table via Playwright headless browser")
    parser.add_argument("--lang", default="en")
    parser.add_argument("--headful", action="store_true", help="Run Chromium non-headless for debugging")
    parser.add_argument("--demo", action="store_true", help="Print Kagoshima: Satsuma Region row")
    args = parser.parse_args()

    async def _run():
        res = await scrape_warning_table_headless(lang=args.lang, headless=(not args.headful))
        if args.demo:
            block = next((g for g in res.get('groups', []) if g.get('area','').startswith('Southern Kyushu and Amami')), None)
            if block:
                row = next((r for r in block.get('rows', []) if r.get('region','').startswith('Kagoshima: Satsuma Region')), None)
                if row:
                    print(json.dumps({"issued": res.get("issued"), "region": row["region"], "cols": row["cols"]}, ensure_ascii=False, indent=2))
                    return
        print(json.dumps(res, ensure_ascii=False, indent=2))
    asyncio.run(_run())


# --- Streamlit demo helper (uses the existing async scraper) ---

def run_streamlit_demo(default_area: str = "Southern Kyushu and Amami"):
    import streamlit as st, asyncio, subprocess, sys, os
    import pandas as pd

    st.set_page_config(page_title="JMA Warnings", layout="wide")
    st.title("JMA Warnings (headless scrape)")

    @st.cache_data(ttl=300, show_spinner=False)
    def fetch():
        # Ensure Playwright browser is available (no-op if already installed)
        try:
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
        except Exception:
            pass
        return asyncio.run(scrape_warning_table_headless(lang="en", headless=True))

    data = fetch()
    st.caption(data.get("issued") or "Issued time unavailable")

    group_names = [g.get("area") for g in data.get("groups", [])]
    idx = group_names.index(default_area) if default_area in group_names else 0
    chosen = st.selectbox("Area group", group_names, index=idx)

    grp = next((g for g in data.get("groups", []) if g.get("area") == chosen), None)
    if grp:
        rows = [{"Region": r["region"], **r["cols"]} for r in grp.get("rows", [])]
        df = pd.DataFrame(rows)
        cols_order = ["Region"] + grp.get("headers", [])
        for c in cols_order:
            if c not in df.columns:
                df[c] = "—"
        st.dataframe(df[cols_order], use_container_width=True)
    else:
        st.info("No data for the chosen group.")

# If launched via `STREAMLIT_APP=1 streamlit run <this_file>.py`, open the demo UI
try:
    import os
    if os.environ.get("STREAMLIT_APP") == "1":
        run_streamlit_demo()
except Exception:
    pass
