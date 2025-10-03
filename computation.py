# computation.py
"""
Pure data/logic helpers used by the app's renderers and controllers.
Keep this file framework-agnostic (no Streamlit imports, no session_state).

What this module provides:
- Robust timestamp parsing and helpers (attach/sort/mark-new).
- Generic grouping utilities.
- Feed-specific calculators for "remaining new" counts (EC/NWS-style).
- Meteoalarm utilities (ID building, mark/sort, snapshot, unseen counters).
- Backwards-compatible compute_counts/advance_seen used by top-level UI.

All functions are pure (no side effects) and easy to unit-test.
"""

from __future__ import annotations

from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Sequence

from dateutil import parser as dateparser


# --------------------------------------------------------------------
# Timestamp parsing & generic helpers
# --------------------------------------------------------------------

def parse_timestamp(ts: Any) -> float:
    """
    Parse a timestamp-like value into UNIX epoch seconds.

    Accepts:
      - float/int epoch seconds
      - datetime (naive or aware)
      - ISO8601-like strings
      - None / invalid -> returns 0.0

    NOTE: This is the canonical parser; use it everywhere.
    """
    if ts is None:
        return 0.0
    if isinstance(ts, (int, float)):
        try:
            return float(ts) if float(ts) > 0 else 0.0
        except Exception:
            return 0.0
    if isinstance(ts, datetime):
        # If tz-aware: timestamp() is UTC-based; naive assumed local—still okay for monotonic "newer than".
        try:
            return ts.timestamp()
        except Exception:
            return 0.0
    if isinstance(ts, str) and ts.strip():
        try:
            return dateparser.parse(ts).timestamp()
        except Exception:
            return 0.0
    return 0.0


def attach_timestamp(items: Sequence[Mapping[str, Any]], *, published_key: str = "published") -> list[dict]:
    """
    Return a new list where every item has a numeric 'timestamp' field.
    If an item already has a valid timestamp, it is reused.
    """
    out: list[dict] = []
    for e in items:
        t = e.get("timestamp")
        ts = parse_timestamp(t if t is not None else e.get(published_key))
        d = dict(e)
        d["timestamp"] = ts
        out.append(d)
    return out


def sort_newest(items: Sequence[Mapping[str, Any]], *, ts_key: str = "timestamp") -> list[dict]:
    """
    Return a new list sorted by timestamp (desc). Items lacking ts are treated as 0.
    """
    return sorted((dict(e) for e in items), key=lambda x: float(x.get(ts_key) or 0.0), reverse=True)


def mark_is_new_ts(
    items: Sequence[Mapping[str, Any]],
    *,
    last_seen_ts: float,
    ts_key: str = "timestamp",
    flag_key: str = "_is_new",
) -> list[dict]:
    """
    Return a new list with a boolean 'flag_key' indicating whether item is newer than last_seen_ts.
    """
    safe = float(last_seen_ts or 0.0)
    out: list[dict] = []
    for e in items:
        ts = float(e.get(ts_key) or 0.0)
        d = dict(e)
        d[flag_key] = ts > safe
        out.append(d)
    return out


def group_by(items: Sequence[Mapping[str, Any]], *, key: str) -> "OrderedDict[str, list[dict]]":
    """
    Group items by a string key into an OrderedDict with alphabetical key order.
    Missing/None keys are grouped under "Unknown".
    """
    buckets: dict[str, list[dict]] = defaultdict(list)
    for e in items:
        k = e.get(key)
        s = str(k).strip() if k is not None else "Unknown"
        buckets[s].append(dict(e))
    return OrderedDict(sorted(buckets.items(), key=lambda kv: kv[0]))


def alphabetic_with_last(keys: Iterable[str], *, last_value: str | None = None) -> list[str]:
    """
    Sort keys alphabetically, optionally moving `last_value` (if present) to the end.
    """
    ks = sorted(set(keys))
    if last_value and last_value in ks:
        ks.remove(last_value)
        ks.append(last_value)
    return ks


# --------------------------------------------------------------------
# Feed-agnostic "remaining new" calculators
# --------------------------------------------------------------------

def compute_remaining_new_by_region(
    entries: Sequence[Mapping[str, Any]],
    *,
    region_field: str,
    last_seen_map: Mapping[str, float],
    ts_key: str = "timestamp",
) -> int:
    """
    Generic helper: given entries that each belong to a 'region' (e.g., province/state/bucket),
    count how many entries are strictly newer than the per-region last_seen_map[region].
    Regions missing in `last_seen_map` default to 0.
    """
    total_new = 0
    for e in entries:
        region = str(e.get(region_field) or "Unknown")
        region_seen = float(last_seen_map.get(region, 0.0) or 0.0)
        ts = float(e.get(ts_key) or parse_timestamp(e.get("published")))
        if ts > region_seen:
            total_new += 1
    return total_new


# --------------------------------------------------------------------
# Environment Canada (EC) helpers
# --------------------------------------------------------------------

# You can extend this with additional known warning types if you wish to bucket by title.
EC_WARNING_TYPES: tuple[str, ...] = (
    "Warning", "Advisory", "Watch", "Statement", "Special Weather Statement",
    "Rainfall", "Snowfall", "Wind", "Thunderstorm", "Heat", "Cold"
)

def ec_bucket_from_title(title: str, *, patterns: Sequence[str] = EC_WARNING_TYPES) -> str | None:
    """
    Very lightweight bucketer: returns the first matching pattern contained in the title.
    If nothing matches, returns None.
    """
    t = (title or "").lower()
    for p in patterns:
        if p.lower() in t:
            return p
    return None


def ec_compute_new_total(
    entries: Sequence[Mapping[str, Any]],
    *,
    region_field: str = "province",
    last_seen_map: Mapping[str, float],
    ts_key: str = "timestamp",
) -> int:
    """
    EC-style 'remaining new' counter by province (or any provided region_field).
    This is a thin wrapper over the generic region calculator.
    """
    return compute_remaining_new_by_region(entries, region_field=region_field, last_seen_map=last_seen_map, ts_key=ts_key)


# --------------------------------------------------------------------
# NWS (US National Weather Service) helpers
# --------------------------------------------------------------------

def nws_compute_new_total(
    entries: Sequence[Mapping[str, Any]],
    *,
    region_field: str = "state",
    last_seen_map: Mapping[str, float],
    ts_key: str = "timestamp",
) -> int:
    """
    NWS-style 'remaining new' counter by state (or any provided region_field).
    """
    return compute_remaining_new_by_region(entries, region_field=region_field, last_seen_map=last_seen_map, ts_key=ts_key)


# --------------------------------------------------------------------
# Meteoalarm helpers
# --------------------------------------------------------------------

def alert_id(entry: Mapping[str, Any]) -> str:
    """
    Build a stable ID string for a Meteoalarm alert entry.
    Combines a few commonly-available fields; tolerate missing values.
    """
    return "|".join([
        str(entry.get("id") or ""),
        str(entry.get("type") or ""),
        str(entry.get("level") or ""),
        str(entry.get("onset") or ""),
        str(entry.get("expires") or ""),
    ])


def meteoalarm_unseen_active_instances(
    entries: Sequence[Mapping[str, Any]],
    last_seen_ids: set[str],
    *,
    levels_considered: Sequence[str] = ("Orange", "Red"),
) -> int:
    """
    Count unseen active Meteoalarm instances among entries' alerts for specified levels.

    entries: list of country dicts each having an 'alerts' mapping (e.g., {"Today":[...], "Tomorrow":[...]}).
    """
    unseen = 0
    for country in entries:
        alerts_map = country.get("alerts", {}) or {}
        for alerts in alerts_map.values():
            for a in alerts or []:
                if a.get("level") not in levels_considered:
                    continue
                if alert_id(a) not in last_seen_ids:
                    unseen += 1
    return unseen


def meteoalarm_mark_and_sort(
    countries: Sequence[Mapping[str, Any]],
    seen_ids: set[str],
    *,
    levels_considered: Sequence[str] = ("Orange", "Red"),
) -> list[dict]:
    """
    For each country, mark alerts with '_is_new' (if id not in seen_ids), keep only considered levels,
    and sort alerts by (level severity desc, onset desc).
    Returns a new list of countries with transformed 'alerts'.
    """
    # Severity order: Red > Orange > Yellow > Green -> map unseen to smaller number for easy sort
    severity_rank = {"Red": 3, "Orange": 2, "Yellow": 1, "Green": 0}

    out: list[dict] = []
    for country in countries:
        name = country.get("name") or country.get("country") or ""
        alerts_map = country.get("alerts", {}) or {}

        new_map: dict[str, list[dict]] = {}
        for day, alerts in alerts_map.items():
            filtered: list[dict] = []
            for a in alerts or []:
                lvl = a.get("level")
                if lvl not in levels_considered:
                    continue
                d = dict(a)
                d["_is_new"] = alert_id(a) not in seen_ids
                # Attach timestamps for sorting if not present
                d["timestamp"] = parse_timestamp(d.get("onset") or d.get("published"))
                filtered.append(d)

            filtered.sort(
                key=lambda x: (severity_rank.get(x.get("level"), 0), float(x.get("timestamp") or 0.0)),
                reverse=True,
            )
            new_map[day] = filtered

        c = dict(country)
        c["name"] = name
        c["alerts"] = new_map
        out.append(c)

    # Keep countries alphabetical by name for stable UI
    out.sort(key=lambda c: (str(c.get("name") or "")))
    return out


def meteoalarm_snapshot_ids(
    countries_or_entries: Sequence[Mapping[str, Any]],
    *,
    include_levels: Sequence[str] | None = None,
) -> tuple[str, ...]:
    """
    Snapshot all alert IDs (optionally filtering by levels) into a tuple for set-like comparisons.
    Works with either a list of countries (with 'alerts' maps) or a flat list of alert dicts.
    """
    ids: list[str] = []

    # Case 1: countries with 'alerts'
    if countries_or_entries and isinstance(countries_or_entries[0], Mapping) and "alerts" in countries_or_entries[0]:
        for country in countries_or_entries:  # type: ignore[index]
            alerts_map = country.get("alerts", {}) or {}
            for alerts in alerts_map.values():
                for a in alerts or []:
                    if include_levels and a.get("level") not in include_levels:
                        continue
                    ids.append(alert_id(a))
    else:
        # Case 2: flat entries
        for a in countries_or_entries:
            if include_levels and a.get("level") not in include_levels:
                continue
            ids.append(alert_id(a))

    return tuple(ids)


# --------------------------------------------------------------------
# Backwards-compatible counters for top-level badges
# --------------------------------------------------------------------

def compute_counts(
    entries: Sequence[Mapping[str, Any]],
    conf: Mapping[str, Any],
    last_seen: Any,
    alert_id_fn: Callable[[Mapping[str, Any]], str] | None = None,
) -> tuple[int, int]:
    """
    Compute total and new counts for a feed.

    - For 'rss_meteoalarm':
        Flatten all Orange/Red alerts and count via `alert_id_fn` against a set `last_seen`.
    - For others:
        Count entries and those with timestamp/published > last_seen timestamp.

    Returns: (total, new_count)
    """
    if conf.get("type") == "rss_meteoalarm":
        # Flatten all alerts of relevant levels
        flat = [
            e
            for country in entries
            for alerts in (country.get("alerts", {}) or {}).values()
            for e in (alerts or [])
            if e.get("level") in ("Orange", "Red")
        ]
        total = len(flat)
        new_count = sum(1 for e in flat if alert_id_fn and alert_id_fn(e) not in (last_seen or set()))
        return total, new_count

    # Non-meteoalarm path
    total = len(entries)
    safe_last = float(last_seen or 0.0)

    def _ts(e: Mapping[str, Any]) -> float:
        t = e.get("timestamp")
        return float(t) if isinstance(t, (int, float)) and float(t) > 0 else parse_timestamp(e.get("published"))

    new_count = sum(1 for e in entries if _ts(e) > safe_last)
    return total, new_count


def advance_seen(
    conf: Mapping[str, Any],
    entries: Sequence[Mapping[str, Any]],
    last_seen: Any,
    now: float,
    alert_id_fn: Callable[[Mapping[str, Any]], str] | None = None,
):
    """
    Suggest a new 'seen' marker for a feed when the user opens it.

    - For 'rss_meteoalarm':
        If all current alerts are already seen, return a snapshot set of their IDs.
    - For others:
        If no entries newer than last_seen, return `now` (epoch seconds).

    Returns:
        - set(str) for meteoalarm,
        - float for timestamp-based feeds,
        - or None if it should not advance.
    """
    if conf.get("type") == "rss_meteoalarm":
        flat = [
            e
            for country in entries
            for alerts in (country.get("alerts", {}) or {}).values()
            for e in (alerts or [])
        ]
        seen_ids = last_seen or set()
        if alert_id_fn and not any(alert_id_fn(e) not in seen_ids for e in flat):
            return set(alert_id_fn(e) for e in flat)
        return None

    # Timestamp-based feeds
    safe_last = float(last_seen or 0.0)

    def _ts(e: Mapping[str, Any]) -> float:
        t = e.get("timestamp")
        return float(t) if isinstance(t, (int, float)) and float(t) > 0 else parse_timestamp(e.get("published"))

    if not any(_ts(e) > safe_last for e in entries):
        return float(now)
    return None
