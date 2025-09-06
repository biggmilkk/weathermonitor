from dateutil import parser as dateparser
from datetime import datetime, timezone, timedelta

def _parse_table_rows(description_html: str):
    """
    Keep alerts that are:
      1) ACTIVE now:        from_dt <= now_utc < until_dt
      2) STARTING <= 24h:   now_utc < from_dt <= now_utc + 24h
    Also keep alerts with only 'until' if now_utc < until_dt (assume already started).
    Drop rows with unparseable times.
    """
    soup = BeautifulSoup(description_html, "html.parser")
    rows = soup.find_all("tr")
    current = "today"
    items = {"today": [], "tomorrow": []}

    now_utc = datetime.now(timezone.utc)
    soon_utc = now_utc + timedelta(hours=24)

    def _parse_dt(s: str) -> datetime | None:
        if not s:
            return None
        try:
            dt = dateparser.parse(s)
            if not dt:
                return None
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

        # Severe/Extreme only
        if level not in AWARENESS_LEVELS:
            continue
        level_name = AWARENESS_LEVELS[level]
        if level_name not in ("Orange", "Red"):
            continue

        type_name = AWARENESS_TYPES.get(awt, f"Type {awt}")

        from_m = re.search(r"From:\s*</b>\s*<i>(.*?)</i>", str(cells[1]), re.IGNORECASE)
        until_m = re.search(r"Until:\s*</b>\s*<i>(.*?)</i>", str(cells[1]), re.IGNORECASE)
        from_raw = from_m.group(1) if from_m else ""
        until_raw = until_m.group(1) if until_m else ""

        from_dt  = _parse_dt(from_raw)
        until_dt = _parse_dt(until_raw)

        keep = False
        if from_dt and until_dt:
            # active now OR starts within 24h
            keep = (from_dt <= now_utc < until_dt) or (now_utc < from_dt <= soon_utc)
        elif until_dt and not from_dt:
            # no start time: keep if still in future (assume started)
            keep = now_utc < until_dt
        elif from_dt and not until_dt:
            # has start only: keep if within next 24h or already started
            keep = (from_dt <= now_utc) or (now_utc < from_dt <= soon_utc)
        # else: both missing → drop

        if not keep:
            continue

        items[current].append({
            "level": level_name,
            "type": type_name,
            "from": from_raw,
            "until": until_raw,
        })

    return items
