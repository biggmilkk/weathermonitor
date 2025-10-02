from dateutil import parser as dateparser


def parse_timestamp(ts):
    """
    Parse an ISO timestamp string to UNIX epoch seconds.
    Returns 0 on failure.
    """
    try:
        return dateparser.parse(ts).timestamp()
    except Exception:
        return 0


def compute_counts(entries, conf, last_seen, alert_id_fn=None):
    """
    Compute total and new alert counts for a feed.

    - For 'rss_meteoalarm', flatten all Orange/Red alerts and count via `alert_id_fn` against `last_seen` set.
    - For other feeds, count entries and those with timestamp/published > last_seen timestamp.

    Returns: (total, new_count)
    """
    if conf['type'] == 'rss_meteoalarm':
        # Flatten all alerts
        flat = [
            e
            for country in entries
            for alerts in country.get('alerts', {}).values()
            for e in alerts
            if e.get('level') in ['Orange', 'Red']
        ]
        total = len(flat)
        # Count alerts whose ID is not in the seen set
        new_count = sum(1 for e in flat if alert_id_fn and alert_id_fn(e) not in last_seen)
    else:
        total = len(entries)
        # Ensure last_seen is numeric
        safe_last = last_seen if isinstance(last_seen, (int, float)) else 0.0

        def _ts(e):
            t = e.get("timestamp")
            if isinstance(t, (int, float)) and t > 0:
                return float(t)
            return parse_timestamp(e.get("published", ""))

        # Count those newer than last_seen timestamp
        new_count = sum(1 for e in entries if _ts(e) > safe_last)
    return total, new_count


def advance_seen(conf, entries, last_seen, now, alert_id_fn=None):
    """
    Determine a new 'seen' marker for an open feed when no new alerts arrive.

    - For 'rss_meteoalarm': if no alert IDs outside `last_seen`, return a new set of all IDs.
    - For other feeds: if no entries timestamp/published after last_seen, return `now` timestamp.

    If new marker should not advance, returns None.
    """
    if conf['type'] == 'rss_meteoalarm':
        flat = [
            e
            for country in entries
            for alerts in country.get('alerts', {}).values()
            for e in alerts
        ]
        # If all current alerts are already seen, snapshot them
        if alert_id_fn and not any(alert_id_fn(e) not in last_seen for e in flat):
            return set(alert_id_fn(e) for e in flat)
    else:
        # Ensure last_seen is numeric
        safe_last = last_seen if isinstance(last_seen, (int, float)) else 0.0

        def _ts(e):
            t = e.get("timestamp")
            if isinstance(t, (int, float)) and t > 0:
                return float(t)
            return parse_timestamp(e.get("published", ""))

        # If no entries are newer than last_seen, advance timestamp
        if not any(_ts(e) > safe_last for e in entries):
            return now
    return None


# --------------------------------------------------------------------
# Meteoalarm-specific helpers
# --------------------------------------------------------------------

def alert_id(entry):
    """
    Build a unique ID string for a Meteoalarm alert entry.
    """
    return "|".join([
        str(entry.get("id") or ""),
        str(entry.get("type") or ""),
        str(entry.get("level") or ""),
        str(entry.get("onset") or ""),
        str(entry.get("expires") or ""),
    ])


def meteoalarm_unseen_active_instances(entries, last_seen_ids):
    """
    Count unseen active Meteoalarm instances among Orange/Red alerts.

    entries: list of country dicts with 'alerts' mapping.
    last_seen_ids: set of previously seen alert IDs.
    """
    unseen = 0
    for country in entries:
        for alerts in country.get("alerts", {}).values():
            for a in alerts:
                if a.get("level") not in ["Orange", "Red"]:
                    continue
                aid = alert_id(a)
                if aid not in last_seen_ids:
                    unseen += 1
    return unseen
