import httpx
from feeds import FeedEntry  # however you represent one feed item

async def scrape_jma_table_async(conf, client):
    # conf["url"] should be "https://www.jma.go.jp/bosai/warning/data/warning/map.json"
    resp = await client.get(conf["url"])
    resp.raise_for_status()
    data = resp.json()

    entries = []
    for feat in data.get("features", []):
        p = feat["properties"]
        # p["areaCode"], p["areaName"], then keys like:
        #   p["heavyRainInundationLevel"], p["floodLevel"], etc.
        # Only include ones that are non‐“missing” (e.g. level > 0)
        for phen_key, phen_label in [
            ("heavyRainInundationLevel", "Heavy Rain (Inundation)"),
            ("heavyRainLandslideLevel",   "Heavy Rain (Landslide)"),
            ("floodLevel",               "Flood"),
            ("stormGaleLevel",           "Storm / Gale"),
            ("highWaveLevel",            "High Wave"),
            ("stormSurgeLevel",          "Storm Surge"),
            ("thunderStormLevel",        "Thunderstorm"),
            ("denseFogLevel",            "Dense Fog"),
            ("dryAirLevel",              "Dry Air"),
        ]:
            lvl = p.get(phen_key)
            if lvl and int(lvl) > 0:
                entries.append(
                    FeedEntry(
                        title=f"{p['areaName']}: {phen_label} Level {lvl}",
                        description=(
                            f"{phen_label} advisory in {p['areaName']} "
                            f"(level {lvl})."
                        ),
                        published=p.get("timestamp"),  # or now()
                        link=conf["url"],
                        extra={"areaCode": p["areaCode"], "phenomenon": phen_key},
                    )
                )
    return {"entries": entries, "source": conf}
