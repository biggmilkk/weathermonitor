    entries = []
    now_iso = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    # Each item => a “reportDatetime” bundle with areaTypes (offices + municipalities)
    for report in data:
        report_dt = (report.get("reportDatetime") or now_iso).replace("+09:00", "Z")
        area_types = report.get("areaTypes", [])

        for at in area_types:
            # Keep ONLY the “offices” level (prefecture/regional office pages)
            # The API uses code/name "offices" for that level.
            if (at.get("code") or "").lower() != "offices" and (at.get("name") or "").lower() != "offices":
                continue

            for area in at.get("areas", []):
                code = str(area.get("code", "")).strip()
                warnings = area.get("warnings", []) or []
                if not code or not warnings:
                    continue

                # Friendly name like "Hokkaido: Tokachi" from areacode.json
                region_name = area_names.get(code, code)

                for w in warnings:
                    pcode = str(w.get("code", "")).strip()
                    status = w.get("status", "")          # 継続 / 発表 / 警報から注意報 etc.
                    attentions = w.get("attentions", [])  # list of JP strings
                    condition = w.get("condition", "")    # 土砂災害/浸水害 etc.

                    level = _infer_level(status, attentions)
                    if level not in ("Warning", "Alert", "Emergency"):
                        # Drop Advisories / anything not warning-grade
                        continue

                    # Phenomenon English label
                    phen = PHENOMENON.get(pcode, pcode)
                    if pcode == "10":  # Heavy Rain variants → refine by condition
                        if "土砂" in condition and "浸水" in condition:
                            phen = "Heavy Rain (Landslide/Inundation)"
                        elif "土砂" in condition:
                            phen = "Heavy Rain (Landslide)"
                        elif "浸水" in condition:
                            phen = "Heavy Rain (Inundation)"
                        else:
                            # leave as Heavy Rain (Inundation) by default or the mapped value
                            phen = PHENOMENON.get("10", "Heavy Rain")

                    entries.append({
                        "title": f"{level} – {phen}",
                        "region": region_name,
                        "level": level,
                        "type": phen,
                        "summary": condition or "",
                        "published": report_dt,
                        "link": f"https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={code}",
                    })
