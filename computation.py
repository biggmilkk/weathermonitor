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
    - For other feeds, count entries and those with published > last_seen timestamp.

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
        # Count those published after last_seen timestamp
        new_count = sum(
            1
            for e in entries
            if e.get('published') and parse_timestamp(e['published']) > last_seen
        )
    return total, new_count


def advance_seen(conf, entries, last_seen, now, alert_id_fn=None):
    """
    Determine a new 'seen' marker for an open feed when no new alerts arrive.

    - For 'rss_meteoalarm': if no alert IDs outside `last_seen`, return a new set of all IDs.
    - For other feeds: if no entries published after last_seen, return `now` timestamp.

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
        # If no entries are newer than last_seen, advance timestamp
        if not any(parse_timestamp(e.get('published', '')) > last_seen for e in entries):
            return now
    return None
