# computation.py
from __future__ import annotations

import json
import re
import time
from collections import OrderedDict, defaultdict
from datetime import datetime
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Sequence

from dateutil import parser as dateparser


# --------------------------------------------------------------------
# Timestamp parsing & generic helpers
# --------------------------------------------------------------------

def parse_timestamp(ts: Any) -> float:
    """Parse many timestamp shapes to epoch seconds (invalid -> 0.0)."""
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
    """Return items with a numeric 'timestamp' (reusing if present)."""
    out: list[dict] = []
    for e in items:
        t = e.get("timestamp")
        ts = parse_timestamp(t if t is not None else e.get(published_key))
        d = dict(e); d["timestamp"] = ts
        out.append(d)
    return out


def sort_newest(items: Sequence[Mapping[str, Any]], *, ts_key: str = "timestamp") -> list[dict]:
    """Sort items by timestamp desc (missing treated as 0)."""
    return sorted((dict(e) for e in items), key=lambda x: float(x.get(ts_key) or 0.0), reverse=True)


def mark_is_new_ts(
    items: Sequence[Mapping[str, Any]],
    *,
    last_seen_ts: float,
    ts_key: str = "timestamp",
    flag_key: str = "_is_new",
) -> list[dict]:
    """Add boolean 'flag_key' if item ts > last_seen_ts."""
    safe = float(last_seen_ts or 0.0)
    out: list[dict] = []
    for e in items:
        ts = float(e.get(ts_key) or 0.0)
        d = dict(e); d[flag_key] = ts > safe
        out.append(d)
    return out


def group_by(items: Sequence[Mapping[str, Any]], *, key: str) -> "OrderedDict[str, list[dict]]":
    """Group items by key into an alphabetized OrderedDict."""
    buckets: dict[str, list[dict]] = defaultdict(list)
    for e in items:
        k = e.get(key)
        s = str(k).strip() if k is not None else "Unknown"
        buckets[s].append(dict(e))
    return OrderedDict(sorted(buckets.items(), key=lambda kv: kv[0]))


def alphabetic_with_last(keys: Iterable[str], *, last_value: str | None = None) -> list[str]:
    """Alphabetize keys, optionally moving `last_value` to the end."""
    ks = sorted(set(keys))
    if last_value and last_value in ks:
        ks.remove(last_value); ks.append(last_value)
    return ks


def entry_ts(e: Mapping[str, Any]) -> float:
    """Canonical timestamp accessor: numeric 'timestamp' else parsed 'published'."""
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
    """Count entries with ts newer than per-region last_seen_map[region]."""
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
_EC_BUCKET_PATTERNS = {w: re.compile(rf"\b{re.escape(w)}\b", flags=re.IGNORECASE) for w in EC_WARNING_TYPES}

def ec_bucket_from_title(title: str, *, patterns: Mapping[str, re.Pattern] = _EC_BUCKET_PATTERNS) -> str | None:
    """Return canonical EC bucket from title; strict match first, then '... Warning' fallback + 'Severe Thunderstorm Watch'."""
    if not title:
        return None
    for canon, pat in patterns.items():
        if pat.search(title):
            return canon
    t_low = title.lower()
    if "warning" in t_low:
        m = re.search(r'([A-Za-z \-/]+warning)\b', title, flags=re.IGNORECASE)
        return (m.group(1).strip().title() if m else "Warning")
    if "severe thunderstorm watch" in t_low:
        return "Severe Thunderstorm Watch"
    return None


def ec_compute_new_total(
    entries: Sequence[Mapping[str, Any]],
    *,
    region_field: str = "province",
    last_seen_map: Mapping[str, float],
    ts_key: str = "timestamp",
) -> int:
    """EC-style remaining-new counter by region_field."""
    return compute_remaining_new_by_region(entries, region_field=region_field, last_seen_map=last_seen_map, ts_key=ts_key)


def ec_remaining_new_total(
    entries: Sequence[Mapping[str, Any]],
    *,
    last_seen_bkey_map: Mapping[str, float],
) -> int:
    """EC-specific remaining-new counter using 'province|bucket' keys."""
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
    """NWS-style remaining-new counter by region_field."""
    return compute_remaining_new_by_region(entries, region_field=region_field, last_seen_map=last_seen_map, ts_key=ts_key)


def nws_remaining_new_total(
    entries: Sequence[Mapping[str, Any]],
    *,
    last_seen_bkey_map: Mapping[str, float],
) -> int:
    """NWS-specific remaining-new counter using 'state|bucket' keys."""
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
    """Build a stable ID for a Meteoalarm alert entry."""
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
    """Count unseen active Meteoalarm instances among entries' alerts for specified levels."""
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
    """Mark alerts with '_is_new', filter by level, sort by (severity desc, onset desc), keep countries alpha."""
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
                d["timestamp"] = parse_timestamp(d.get("onset") or d.get("from") or d.get("published"))
                filtered.append(d)
            filtered.sort(key=lambda x: (severity_rank.get(x.get("level"), 0), float(x.get("timestamp") or 0.0)), reverse=True)
            new_map[day] = filtered
        c = dict(country)
        c["name"] = name
        c["title"] = c.get("title") or name
        c["alerts"] = new_map
        out.append(c)
    out.sort(key=lambda c: (str(c.get("name") or "")))
    return out


def meteoalarm_snapshot_ids(
    countries_or_entries: Sequence[Mapping[str, Any]],
    *,
    include_levels: Sequence[str] | None = None,
) -> tuple[str, ...]:
    """Snapshot alert IDs (optionally filtered by levels) from countries-with-alerts or a flat list."""
    ids: list[str] = []
    if countries_or_entries and isinstance(countries_or_entries[0], Mapping) and "alerts" in countries_or_entries[0]:
        for country in countries_or_entries:  # type: ignore[index]
            alerts_map = country.get("alerts", {}) or {}
            for alerts in alerts_map.values():
                for a in alerts or []:
                    if include_levels and a.get("level") not in include_levels:
                        continue
                    ids.append(alert_id(a))
    else:
        for a in countries_or_entries:
            if include_levels and a.get("level") not in include_levels:
                continue
            ids.append(alert_id(a))
    return tuple(ids)


def meteoalarm_total_active_instances(entries: Sequence[Mapping[str, Any]]) -> int:
    """Sum per-country active instance totals: prefer counts.total, fallback total_alerts."""
    total = 0
    for country in entries or []:
        counts = country.get("counts")
        if isinstance(counts, dict) and ("total" in counts):
            try:
                total += int(counts.get("total") or 0)
                continue
            except Exception:
                pass
        try:
            total += int(country.get("total_alerts") or 0)
        except Exception:
            pass
    return total


def meteoalarm_unseen_active_instance_total(
    entries: Sequence[Mapping[str, Any]],
    last_seen_ids: set[str],
    *,
    levels_considered: Sequence[str] = ("Orange", "Red"),
) -> int:
    """
    Sum the active-instance counts for *unseen* Meteoalarm buckets.

    - We deduplicate by (day, level, type) so multiple alerts with the same bucket
      don't double-count.
    - We read counts preferentially from:
        counts.by_day[day]["{Level}|{Type}"]
      and fall back to:
        counts.by_type[type][level] or counts.by_type[type]["total"]
      if a day-level-type count isn't available.
    """
    def _bucket_count(counts: Mapping[str, Any] | None, day: str, level: str, typ: str) -> int:
        if not isinstance(counts, Mapping):
            return 0

        by_day = counts.get("by_day")
        if isinstance(by_day, Mapping):
            # try exact and a few normalized variants of 'day'
            for dkey in (day, str(day).capitalize(), str(day).title()):
                d = by_day.get(dkey)
                if isinstance(d, Mapping):
                    val = d.get(f"{level}|{typ}")
                    if isinstance(val, int) and val > 0:
                        return int(val)

        by_type = counts.get("by_type")
        if isinstance(by_type, Mapping):
            bucket = by_type.get(typ)
            if isinstance(bucket, Mapping):
                val = bucket.get(level)
                if isinstance(val, int) and val > 0:
                    return int(val)
                val = bucket.get("total")
                if isinstance(val, int) and val > 0:
                    return int(val)

        return 0

    total = 0
    for country in entries or []:
        counts = country.get("counts") if isinstance(country, Mapping) else None
        alerts_map = country.get("alerts", {}) or {}
        unseen_buckets: set[tuple[str, str, str]] = set()  # (day, level, type)

        if isinstance(alerts_map, Mapping):
            for day, alerts in alerts_map.items():
                for a in (alerts or []):
                    if not isinstance(a, Mapping):
                        continue
                    lvl = a.get("level")
                    if lvl not in levels_considered:
                        continue
                    if alert_id(a) not in last_seen_ids:
                        unseen_buckets.add((str(day), str(lvl), str(a.get("type"))))

        for (day, level, typ) in unseen_buckets:
            total += _bucket_count(counts, day, level, typ)

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
    """Fingerprint each region's content; if changed, bump timestamp + mark is_new on region and per-day."""
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
        fp = json.dumps(norm, sort_keys=True, separators=(",", ":"))
        changed = (prev_fp.get(region) != fp)
        ts = now_ts if changed else float(prev_ts.get(region) or 0.0)
        if ts <= 0:
            ts = now_ts

        d = dict(e); d["timestamp"] = ts; d["is_new"] = bool(changed)
        dd = dict(days)
        if "today" in dd and isinstance(dd["today"], dict):
            tdy = dict(dd["today"]); tdy["is_new"] = bool(changed); dd["today"] = tdy
        if "tomorrow" in dd and isinstance(dd["tomorrow"], dict):
            tom = dict(dd["tomorrow"]); tom["is_new"] = bool(changed); dd["tomorrow"] = tom
        d["days"] = dd

        updated.append(d)
        fp_by_region[region] = fp
        ts_by_region[region] = ts

    return updated, fp_by_region, ts_by_region


def imd_unseen_day_total(entries: Sequence[Mapping[str, Any]]) -> int:
    """
    Count unseen IMD 'alert units' as day-slots:
      - +1 for 'today' if present and marked is_new
      - +1 for 'tomorrow' if present and marked is_new
    Falls back to the item-level 'is_new' if day-level flags are absent.
    """
    total = 0
    for e in entries or []:
        days = e.get("days") or {}
        tdy = days.get("today") or {}
        tom = days.get("tomorrow") or {}

        used_day_flags = False
        if isinstance(tdy, dict) and "is_new" in tdy:
            if tdy.get("is_new"):
                total += 1
            used_day_flags = True
        if isinstance(tom, dict) and "is_new" in tom:
            if tom.get("is_new"):
                total += 1
            used_day_flags = True

        # Fallback if we only have entry-level is_new
        if not used_day_flags and e.get("is_new"):
            if isinstance(tdy, dict) and tdy:
                total += 1
            if isinstance(tom, dict) and tom:
                total += 1
    return total


# --------------------------------------------------------------------
# IMD clear-on-close snapshot helper
# --------------------------------------------------------------------

def _imd_build_fingerprint(entry: Mapping[str, Any]) -> tuple[str, str]:
    """(region, fingerprint_json) using same normalization as compute_imd_timestamps."""
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
    """Return (fp_by_region, ts_by_region, cleared_entries) and clear is_new flags immediately."""
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

        d = dict(e); d["is_new"] = False
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
    """Return (total, new_count) for a feed. Meteoalarm flattens Orange/Red and uses ID-based 'new'."""
    if conf.get("type") == "rss_meteoalarm":
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
    """Suggest a new 'seen' marker: Meteoalarm returns a set of IDs when all are already seen; others return timestamp."""
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

    safe_last = float(last_seen or 0.0)

    def _ts(e: Mapping[str, Any]) -> float:
        t = e.get("timestamp")
        return float(t) if isinstance(t, (int, float)) and float(t) > 0 else parse_timestamp(e.get("published"))

    if not any(_ts(e) > safe_last for e in entries):
        return float(now)
    return None
