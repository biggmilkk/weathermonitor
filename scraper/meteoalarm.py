from dateutil import parser as dateparser
from datetime import datetime, timezone, timedelta

def _parse_table_rows(description_html: str):
    """
    Parse Orange/Red rows from a Meteoalarm country/EU entry and
    keep only rows that are ACTIVE now or START within the next 24 hours.

    Active if: from_dt <= now < until_dt
    Starts soon if: now < from_dt <= now + 24h

    If times are missing:
      - If only 'until' exists: keep if now < until_dt (assume already started)
      - If only 'from' exists: keep if from_dt <= now+24h
      - If neither exists: drop
    """
    soup = BeautifulSoup(description_html, "html.parser")
    rows = soup.find_all("tr")
    current = "today"
    kept = {"today": [], "tomorrow": []}

    # Reference times
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=24)

    def _parse_when(s: str | None) -> datetime | None:
        if not s or s.strip() in ("?", ""):
            return None
        try:
            # Default date = today UTC (helps when feed omits a date)
            default_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
            dt = dateparser.parse(s, default=default_dt)
            if not dt:
                return None
            # Normalise to UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        except Exception:
            return None

    for row in rows:
        header = row.find("th")
        if header:
            txt = header.get_text(strip=True).lower()
            if "tomorrow" in txt:
                current = "tomorrow"
            elif "today" in txt:
                current = "today"
            continue

        cells = row.find_all("td")
        if len(cells) != 2:
            continue

        level = cells[0].get("data-awareness-level")
        awt   = cells[0].get("data-awareness-type")
        if not level or not awt:
            m = re.search(r"awt:(\d+)\s+level:(\d+)", cells[0].get_text(strip=True))
            if m:
                awt, level = m.groups()

        if level not in AWARENESS_LEVELS:
            continue
        level_name = AWARENESS_LEVELS[level]
        # Only Orange/Red
        if level_name not in ("Orange", "Red"):
            continue

        type_name = AWARENESS_TYPES.get(awt, f"Type {awt}")

        # Extract times (often like "Sep 05 16:00 UTC")
        from_m = re.search(r"From:\s*</b>\s*<i>(.*?)</i>", str(cells[1]), re.IGNORECASE)
        until_m = re.search(r"Until:\s*</b>\s*<i>(.*?)</i>", str(cells[1]), re.IGNORECASE)
        from_str = from_m.group(1) if from_m else None
        until_str = until_m.group(1) if until_m else None

        from_dt = _parse_when(from_str)
        until_dt = _parse_when(until_str)

        # If both parsed and 'until' ends up <= 'from', assume it crosses midnight → add 1 day
        if from_dt and until_dt and until_dt <= from_dt:
            until_dt = until_dt + timedelta(days=1)

        # Decide keep/drop
        keep = False
        if from_dt and until_dt:
            keep = (from_dt <= now < until_dt) or (now < from_dt <= cutoff)
        elif until_dt and not from_dt:
            keep = now < until_dt
        elif from_dt and not until_dt:
            keep = from_dt <= cutoff
        else:
            keep = False  # no usable timing

        if not keep:
            continue

        # Keep the original strings in the payload (renderer already formats them)
        kept[current].append({
            "level": level_name,
            "type": type_name,
            "from": from_str or "",
            "until": until_str or "",
        })

    return kept
