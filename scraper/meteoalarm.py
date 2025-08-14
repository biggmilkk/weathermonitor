def _parse_feed(feed):
    # Country name → 2-letter region code used by MeteoAlarm frontend URLs
    MA_COUNTRY_CODES = {
        "Austria": "AT",
        "Belgium": "BE",
        "Bosnia and Herzegovina": "BA",
        "Bulgaria": "BG",
        "Croatia": "HR",
        "Cyprus": "CY",
        "Czechia": "CZ",
        "Czech Republic": "CZ",  # alias
        "Denmark": "DK",
        "Estonia": "EE",
        "Finland": "FI",
        "France": "FR",
        "Germany": "DE",
        "Greece": "GR",
        "Hungary": "HU",
        "Iceland": "IS",
        "Ireland": "IE",
        "Israel": "IL",
        "Italy": "IT",
        "Latvia": "LV",
        "Lithuania": "LT",
        "Luxembourg": "LU",
        "Malta": "MT",
        "Moldova": "MD",
        "Montenegro": "ME",
        "Netherlands": "NL",
        "North Macedonia": "MK",
        "Norway": "NO",
        "Poland": "PL",
        "Portugal": "PT",
        "Romania": "RO",
        "Serbia": "RS",
        "Slovakia": "SK",
        "Slovenia": "SI",
        "Spain": "ES",
        "Sweden": "SE",
        "Switzerland": "CH",
        "Ukraine": "UA",
        "United Kingdom": "GB",
        "United Kingdom of Great Britain and Northern Ireland": "GB",
    }

    entries = []

    for entry in feed.entries:
        # Country name (e.g., "Austria")
        country = entry.get("title", "").replace("MeteoAlarm", "").strip()
        pub_date = entry.get("published", "")
        description_html = entry.get("description", "")

        # Parse the HTML table
        soup = BeautifulSoup(description_html, "html.parser")
        rows = soup.find_all("tr")

        current_section = "today"
        alert_data = {"today": [], "tomorrow": []}

        # Per-day counts by (level, type)
        per_day_counts = {"today": {}, "tomorrow": {}}

        for row in rows:
            header = row.find("th")
            if header:
                text = header.get_text(strip=True).lower()
                if "tomorrow" in text:
                    current_section = "tomorrow"
                elif "today" in text:
                    current_section = "today"
                continue

            cells = row.find_all("td")
            if len(cells) != 2:
                continue

            # Try to get level/type from attributes, else fallback to text probe
            level = cells[0].get("data-awareness-level")
            awt = cells[0].get("data-awareness-type")
            if not level or not awt:
                m = re.search(r"awt:(\d+)\s+level:(\d+)", cells[0].get_text(strip=True))
                if m:
                    awt, level = m.groups()

            # Map to names and filter: only Orange/Red
            if level not in AWARENESS_LEVELS:
                continue
            level_name = AWARENESS_LEVELS[level]
            if level_name not in ("Orange", "Red"):
                continue

            type_name = AWARENESS_TYPES.get(awt, f"Type {awt}")

            # Window (keep as strings; renderer formats them)
            from_match = re.search(r"From:\s*</b>\s*<i>(.*?)</i>", str(cells[1]), re.IGNORECASE)
            until_match = re.search(r"Until:\s*</b>\s*<i>(.*?)</i>", str(cells[1]), re.IGNORECASE)
            from_time = from_match.group(1) if from_match else "?"
            until_time = until_match.group(1) if until_match else "?"

            # Store one alert entry (same structure you already use)
            alert = {
                "level": level_name,
                "type": type_name,
                "from": from_time,
                "until": until_time,
            }
            alert_data[current_section].append(alert)

            # Count by (level, type) for this day
            key = (level_name, type_name)
            per_day_counts[current_section][key] = per_day_counts[current_section].get(key, 0) + 1

        # Skip countries with no Orange/Red entries
        if not alert_data["today"] and not alert_data["tomorrow"]:
            continue

        # Total Orange/Red count across both days
        total_alerts = sum(per_day_counts["today"].values()) + sum(per_day_counts["tomorrow"].values())

        # Build the correct frontend URL (fallback to RSS link if we don't know the code)
        code = MA_COUNTRY_CODES.get(country)
        if code:
            link = f"https://meteoalarm.org/en/live/region/{code}"
        else:
            # fallback to the feed link in case of an unexpected country label
            link = entry.get("link", "")

        entries.append({
            "title": f"{country} Alerts",
            "summary": "",
            "alerts": alert_data,                 # keeps your existing structure
            "counts": {                           # NEW: per-day (level,type) counts
                "today": per_day_counts["today"],
                "tomorrow": per_day_counts["tomorrow"],
            },
            "total_alerts": total_alerts,         # NEW: country total
            "link": link,                         # fixed human-friendly URL
            "published": pub_date,
            "region": country,
            "province": "Europe",
        })

    # Alphabetical by country name
    entries.sort(key=lambda e: e["region"].lower())
    return entries
