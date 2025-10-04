# computation.py
"""
Pure data/logic helpers used by the app's renderers and controllers.
Keep this file framework-agnostic (no Streamlit imports, no session_state).

What this module provides:
- Robust timestamp parsing and helpers (attach/sort/mark-new).
- Generic grouping utilities.
- Feed-specific calculators for "remaining new" counts (EC/NWS-style).
- Meteoalarm utilities (ID building, mark/sort, snapshot, unseen counters).
- IMD (India) utility to compute per-region timestamps/new flags.
- Backwards-compatible compute_counts/advance_seen used by the controller/UI.

All functions are pure (no side effects) and easy to unit-test.
"""

from __future__ import annotations

import json
import re
from collections import OrderedDict, defaultdict
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


def entry_ts(e: Mapping[str, Any]) -> float:
    """
    Canonical accessor for an entry's timestamp:
    uses numeric 'timestamp' if present, otherwise parses 'published'.
    """
    t = e.get("timestamp")
    if isinstance(t, (int, float)) and float(t) > 0:
        return float(t)
    return parse_timestamp(e.get("published"))


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

# Canonical EC warning buckets (strict, word-boundary matching)
EC_WARNING_TYPES: tuple[str, ...] = (
    "Arctic Outflow Warning",
    "Blizzard Warning",
    "Blowing Snow Warning",
    "Coastal Flooding Warning",
    "Dust Storm Warning",
    "Extreme Cold Warning",
    "Flash Freeze Warning",
    "Fog Warning",
    "Freezing Drizzle Warning",
    "Freezing Rain Warning",
    "Frost Warning",
    "Heat Warning",
    "Hurricane Warning",
    "Rainfall Warning",
    "Severe Thunderstorm Warning",
    "Severe Thunderstorm Watch",
    "Snowfall Warning",
    "Snow Squall Warning",
    "Tornado Warning",
    "Tropical Storm Warning",
    "Tsunami Warning",
    "Weather Warning",
    "Wind Warning",
    "Winter Storm Warning",
)
_EC_BUCKET_PATTERNS = {
    w: re.compile(rf"\b{re.escape(w)}\b", flags=re.IGNORECASE) for w in EC_WARNING_TYPES
}

def ec_bucket_from_title(title: str, *, patterns: Mapping[str, re.Pattern] = _EC_BUCKET_PATTERNS) -> str | None:
    """
    Return the canonical EC bucket by matching known warning names as whole words in the title.

    Behavior (restored to previous app semantics):
      1) Try strict match against the canonical set above (word-boundary, case-insensitive).
      2) If no strict match:
           - If the title contains the word "warning" (anywhere), treat it as a valid bucket.
             We try to extract the '<Something> Warning' phrase; otherwise return 'Warning'.
           - If the title contains 'severe thunderstorm watch', return that bucket.
      3) Else return None.

    This keeps button badges and renderer logic in sync with the scraper,
    which already filters EC entries down to Warnings (and Severe Thunderstorm Watch).
    """
    if not title:
        return None

    # 1) strict exact bucket match
    for canon, pat in patterns.items():
        if pat.search(title):
            return canon

    # normalize once for fallbacks
    t_low = title.lower()

    # 2a) any title containing "warning" should be treated as a warning
    if "warning" in t_low:
        # Try to capture a phrase ending with "warning" for a nicer bucket label
        m = re.search(r'([A-Za-z \-/]+warning)\b', title, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip().title()
        return "Warning"

    # 2b) explicit fallback for 'Severe Thunderstorm Watch'
    if "severe thunderstorm watch" in t_low:
        return "Severe Thunderstorm Watch"

    # 3) nothing recognized
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
    return compute_remaining_new_by_region(
        entries, region_field=region_field, last_seen_map=last_seen_map, ts_key=ts_key
    )


def ec_remaining_new_total(
    entries: Sequence[Mapping[str, Any]],
    *,
    last_seen_bkey_map: Mapping[str, float],
) -> int:
    """
    EC-specific 'remaining new' counter using composite keys 'province|bucket'
    to match your renderer/controller state map.
    """
    total = 0
    for e in entries or []:
        bucket = ec_bucket_from_title((e.get("title") or "") or "")
        if not bucket:
            continue
        prov_name = (e.get("province_name") or str(e.get("province") or "")).strip() or "Unknown"
        bkey = f"{prov_name}|{bucket}"
        last_seen = float(last_seen_bkey_map.get(bkey, 0.0))
        if entry_ts(e) > last_seen:
            total += 1
    return total


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
    return compute_remaining_new_by_region(
        entries, region_field=region_field, last_seen_map=last_seen_map, ts_key=ts_key
    )


def nws_remaining_new_total(
    entries: Sequence[Mapping[str, Any]],
    *,
    last_seen_bkey_map: Mapping[str, float],
) -> int:
    """
    NWS-specific 'remaining new' counter using composite keys 'state|bucket'
    to match your renderer/controller state map.
    """
    total = 0
    for e in entries or []:
        state = (e.get("state") or e.get("state_name") or e.get("state_code") or "Unknown")
        bucket = (e.get("bucket") or e.get("event") or e.get("title") or "Alert")
        if not state or not bucket:
            continue
        bkey = f"{state}|{bucket}"
        last_seen = float(last_seen_bkey_map.get(bkey, 0.0))
        if entry_ts(e) > last_seen:
            total += 1
    return total


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
        str(entry.get("onset") or entry.get("from") or ""),
        str(entry.get("expires") or entry.get("until") or ""),
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
        name = country.get("name") or country.get("country") or country.get("title") or ""
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
                # Attach timestamps for sorting if not present; tolerate either onset/from
                d["timestamp"] = parse_timestamp(d.get("onset") or d.get("from") or d.get("published"))
                filtered.append(d)

            filtered.sort(
                key=lambda x: (severity_rank.get(x.get("level"), 0), float(x.get("timestamp") or 0.0)),
                reverse=True,
            )
            new_map[day] = filtered

        c = dict(country)
        c["name"] = name
        c["title"] = c.get("title") or name  # ensure renderer can show a heading
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


def meteoalarm_total_active_instances(
    entries: Sequence[Mapping[str, Any]],
) -> int:
    """
    Return the total number of active Orange/Red alert *instances* across all countries.

    Preferred source is per-country `counts.total` (if present); fallback to `total_alerts`.
    Both fields are expected to include the "(n active)" multiplicity used in the UI.
    """
    total = 0
    for country in entries or []:
        counts = country.get("counts")
        # Prefer counts.total if available
        if isinstance(counts, dict) and ("total" in counts):
            try:
                total += int(counts.get("total") or 0)
                continue
            except Exception:
                pass
        # Fallback to total_alerts (kept for backward compatibility)
        try:
            total += int(country.get("total_alerts") or 0)
        except Exception:
            pass
    return total

# --------------------------------------------------------------------
# IMD (India) helpers
# --------------------------------------------------------------------

def compute_imd_timestamps(
    *,
    entries: Sequence[Mapping[str, Any]],
    prev_fp: Mapping[str, str] | None,
    prev_ts: Mapping[str, float] | None,
    now_ts: float,
) -> tuple[list[dict], dict[str, str], dict[str, float]]:
    """
    Given IMD entries (each with `region` and optional `days` containing `today`/`tomorrow`),
    compute per-region fingerprints to detect changes, assign `timestamp` and `is_new` at the
    item level, and propagate `is_new` to the day dicts.
    """
    prev_fp = dict(prev_fp or {})
    prev_ts = dict(prev_ts or {})

    updated: list[dict] = []
    fp_by_region: dict[str, str] = {}
    ts_by_region: dict[str, float] = {}

    for e in entries:
        region = (e.get("region") or "").strip()
        days = e.get("days") or {}

        norm = {
            "region": region,
            "today": {
                "severity": (days.get("today") or {}).get("severity"),
                "hazards":  (days.get("today") or {}).get("hazards") or [],
                "date":     (days.get("today") or {}).get("date"),
            },
            "tomorrow": {
                "severity": (days.get("tomorrow") or {}).get("severity"),
                "hazards":  (days.get("tomorrow") or {}).get("hazards") or [],
                "date":     (days.get("tomorrow") or {}).get("date"),
            },
        }

        # Fingerprint content
        fp = json.dumps(norm, sort_keys=True, separators=(",", ":"))
        changed = (prev_fp.get(region) != fp)

        # Timestamp: bump to now if changed; else keep old
        ts = now_ts if changed else float(prev_ts.get(region) or 0.0)
        if ts <= 0:
            ts = now_ts

        d = dict(e)
        d["timestamp"] = ts
        d["is_new"] = bool(changed)

        # propagate per-day is_new flags
        dd = dict(days)
        if "today" in dd and isinstance(dd["today"], dict):
            dd_today = dict(dd["today"])
            dd_today["is_new"] = bool(changed)
            dd["today"] = dd_today
        if "tomorrow" in dd and isinstance(dd["tomorrow"], dict):
            dd_tom = dict(dd["tomorrow"])
            dd_tom["is_new"] = bool(changed)
            dd["tomorrow"] = dd_tom
        d["days"] = dd

        updated.append(d)
        fp_by_region[region] = fp
        ts_by_region[region] = ts

    return updated, fp_by_region, ts_by_region


# --------------------------------------------------------------------
# IMD clear-on-close snapshot helper
# --------------------------------------------------------------------

def _imd_build_fingerprint(entry: Mapping[str, Any]) -> tuple[str, str]:
    """Return (region, fingerprint_json) for an IMD entry, using the same normalization as compute_imd_timestamps."""
    region = (entry.get("region") or "").strip()
    days = entry.get("days") or {}
    norm = {
        "region": region,
        "today": {
            "severity": (days.get("today") or {}).get("severity"),
            "hazards":  (days.get("today") or {}).get("hazards") or [],
            "date":     (days.get("today") or {}).get("date"),
        },
        "tomorrow": {
            "severity": (days.get("tomorrow") or {}).get("severity"),
            "hazards":  (days.get("tomorrow") or {}).get("hazards") or [],
            "date":     (days.get("tomorrow") or {}).get("date"),
        },
    }
    fp = json.dumps(norm, sort_keys=True, separators=(",", ":"))
    return region, fp


def snapshot_imd_seen(
    entries: Sequence[Mapping[str, Any]],
    *,
    now_ts: float | None = None,
) -> tuple[dict[str, str], dict[str, float], list[dict]]:
    """
    Build per-region fingerprints from the current entries and return:
      (fp_by_region, ts_by_region, cleared_entries)

    - fp_by_region / ts_by_region: persisted so the next fetch round won't
      re-mark unchanged regions as new.
    - cleared_entries: entries with is_new and per-day is_new flags cleared.
    """
    ts = float(now_ts or time.time())
    fp_by_region: dict[str, str] = {}
    ts_by_region: dict[str, float] = {}
    cleared: list[dict] = []

    for e in entries or []:
        region, fp = _imd_build_fingerprint(e)
        if not region:
            region = ""
        fp_by_region[region] = fp
        existing_ts = e.get("timestamp")
        ts_by_region[region] = float(existing_ts) if isinstance(existing_ts, (int, float)) and float(existing_ts) > 0 else ts

        d = dict(e)
        d["is_new"] = False
        days = d.get("days") or {}
        if isinstance(days, dict):
            dd = dict(days)
            if "today" in dd and isinstance(dd["today"], dict):
                t = dict(dd["today"]); t["is_new"] = False; dd["today"] = t
            if "tomorrow" in dd and isinstance(dd["tomorrow"], dict):
                t = dict(dd["tomorrow"]); t["is_new"] = False; dd["tomorrow"] = t
            d["days"] = dd
        cleared.append(d)

    return fp_by_region, ts_by_region, cleared

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
