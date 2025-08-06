import httpx
from dateutil import parser as dateparser

# 1) Load your lookup tables once at startup:

# A) Area codes → human name
#    You can grab this from JMA’s own index.json for class20s:
#    https://www.jma.go.jp/bosai/common/const/class20s/index.json
import requests
area_index = requests.get(
    "https://www.jma.go.jp/bosai/common/const/class20s/index.json"
).json()
AREA_NAME = { entry["code"]: entry["name"] for entry in area_index }

# B) Warning type keys → labels
TYPE_LABEL = {
    "rain_fall":       {"ja": "大雨警報",      "en": "Heavy Rain Warning"},
    "flood":           {"ja": "洪水警報",      "en": "Flood Warning"},
    "land_slide":      {"ja": "土砂災害警戒情報","en": "Landslide Advisory"},
    "storm_surge":     {"ja": "高潮警報",      "en": "Storm Surge Warning"},
    # … add whatever your feed actually uses …
}

# C) Status codes → English
STATUS_LABEL = {
    "発表": "Issued",
    "継続": "Continued",
    "解除": "Cancelled",
}

# 2) Fetch & render only true “警報” (warning) level items:
async def scrape_and_render_jma(conf: dict, client: httpx.AsyncClient, lang="en"):
    url = conf["url"]
    resp = await client.get(url, headers={"Referer": "https://www.jma.go.jp/bosai/warning/"})
    resp.raise_for_status()
    data = resp.json()

    for pref_code, region in data.items():
        ts = region.get("time")
        published = None
        try:
            published = dateparser.parse(ts).isoformat()
        except:
            pass

        for area_code, warns in region.get("areas", {}).items():
            # map the area code to name (falls back to code itself)
            name = AREA_NAME.get(area_code, area_code)

            for typ, level in warns.items():
                # numeric level > 0 only:
                if not isinstance(level, (int, float)) or level <= 0:
                    continue

                # only show real “警報” levels (JMA uses level>=30 for 警報)
                if level < 30:
                    continue

                # lookup human labels
                type_label = TYPE_LABEL.get(typ, {}).get(lang, typ)
                status     = region.get("status", "")   # if your JSON has status
                status_lbl = STATUS_LABEL.get(status, status)

                st.markdown(
                    f"**{name} — {type_label} (level {level})**  \n"
                    f"{status_lbl} at {published}"
                )
                st.caption(f"Updated: {published}")
                st.markdown("---")
