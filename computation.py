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

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

import hashlib
import itertools
import json
import math
import re
import time


# ----------------------------------------------------------
# Generic helpers
# ----------------------------------------------------------

RFC3339 = "%Y-%m-%dT%H:%M:%SZ"

def _safe_int(x: Any, default: int | None = None) -> int | None:
    try:
        return int(x)
    except Exception:
        return default

def utcnow_ts() -> float:
    return time.time()

def parse_rfc3339(ts: str) -> float | None:
    try:
        return datetime.strptime(ts, RFC3339).replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return None

def to_rfc3339(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(RFC3339)


# ----------------------------------------------------------
# EC/NWS - style "new count since last seen" helpers
# ----------------------------------------------------------

def compute_counts(entries: Sequence[Mapping[str, Any]], last_seen_ts: float | None) -> dict[str, int]:
    """
    Returns a dict with counts of entries whose 'timestamp' is strictly greater than last_seen_ts.
    Missing timestamp entries are ignored.
    """
    if last_seen_ts is None:
        last_seen_ts = 0.0
    total = 0
    by_severity = {"warning": 0, "watch": 0, "advisory": 0, "other": 0}
    for e in entries:
        ts = e.get("timestamp")
        if not isinstance(ts, (int, float)):
            continue
        if ts > last_seen_ts:
            total += 1
            sev = (e.get("severity") or "").lower()
            if sev in by_severity:
                by_severity[sev] += 1
            else:
                by_severity["other"] += 1
    by_severity["total"] = total
    return by_severity


# ----------------------------------------------------------
# Meteoalarm utilities
# ----------------------------------------------------------

def meteoalarm_build_id(item: Mapping[str, Any]) -> str:
    """
    Build a stable ID for meteoalarm items from country+area+event+onset.
    """
    country = (item.get("country") or "").strip().lower()
    area = (item.get("area") or "").strip().lower()
    event = (item.get("event") or "").strip().lower()
    onset = (item.get("onset") or "").strip()
    base = f"{country}|{area}|{event}|{onset}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def meteoalarm_snapshot_ids(entries: Sequence[Mapping[str, Any]]) -> set[str]:
    """
    Make a snapshot set of current meteoalarm IDs.
    """
    ids = set()
    for e in entries:
        ids.add(meteoalarm_build_id(e))
    return ids

def meteoalarm_mark_unseen(entries: Sequence[Mapping[str, Any]], seen_ids: set[str] | None) -> list[dict]:
    """
    Mark entries with 'is_new' based on whether their ID is in the seen snapshot set.
    """
    seen_ids = seen_ids or set()
    out = []
    for e in entries:
        d = dict(e)
        d["is_new"] = meteoalarm_build_id(e) not in seen_ids
        out.append(d)
    return out


# ----------------------------------------------------------
# IMD (India) helpers
# ----------------------------------------------------------

def _canonicalize_imd_day(day: Mapping[str, Any] | None) -> dict:
    """
    Canonicalize one day block (today/tomorrow). We only keep fields relevant to "change" semantics.
    """
    day = day or {}
    severity = day.get("severity")
    hazards = day.get("hazards") or []
    date = day.get("date")

    # normalize hazards list: sort, dedupe, strip whitespace
    canon_hazards = sorted({(h or "").strip() for h in hazards if h is not None})

    return {
        "severity": severity,
        "hazards": canon_hazards,
        "date": date,
    }

def _imd_region_norm(region: str, days: Mapping[str, Any] | None) -> dict:
    days = days or {}
    today = _canonicalize_imd_day(days.get("today"))
    tomorrow = _canonicalize_imd_day(days.get("tomorrow"))
    return {
        "region": (region or "").strip(),
        "today": today,
        "tomorrow": tomorrow,
    }

def _imd_region_fp(norm: Mapping[str, Any]) -> str:
    """
    Deterministic serialization (stable keys, compact separators) -> SHA1 digest for compactness.
    """
    raw = json.dumps(norm, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

def _propagate_day_is_new(item: dict, changed_keys: set[str]) -> None:
    """
    Given an IMD item and a set of changed components ('today', 'tomorrow'), set per-day is_new.
    """
    days = item.get("days") or {}
    if not isinstance(days, dict):
        return
    if "today" in days and isinstance(days["today"], dict):
        days["today"] = dict(days["today"])
        days["today"]["is_new"] = "today" in changed_keys
    if "tomorrow" in days and isinstance(days["tomorrow"], dict):
        days["tomorrow"] = dict(days["tomorrow"])
        days["tomorrow"]["is_new"] = "tomorrow" in changed_keys
    item["days"] = days

def _diff_norm(old: Mapping[str, Any] | None, new: Mapping[str, Any]) -> set[str]:
    """
    Very small diff to identify which parts changed: returns a set among {"today","tomorrow"}.
    """
    changed = set()
    old = old or {}
    if (old.get("today") or {}) != (new.get("today") or {}):
        changed.add("today")
    if (old.get("tomorrow") or {}) != (new.get("tomorrow") or {}):
        changed.add("tomorrow")
    return changed

def compute_imd_timestamps(
    *,
    entries: Sequence[Mapping[str, Any]],
    prev_fp: Mapping[str, str] | None,
    prev_ts: Mapping[str, float] | None,
    now_ts: float,
) -> tuple[list[dict], dict[str, str], dict[str, float]]:
    """
    Given IMD entries (each with `region` and an optional `days` mapping containing `today`/`tomorrow`),
    compute per-region fingerprints to detect changes, assign `timestamp` and `is_new` at the item level,
    and propagate `is_new` to the day dicts.

    Returns:
        (updated_entries, fp_by_region, ts_by_region)
    """
    prev_fp = prev_fp or {}
    prev_ts = prev_ts or {}
    out: list[dict] = []
    fp_by_region: dict[str, str] = {}
    ts_by_region: dict[str, float] = {}

    # Build a lookup from previous normalized objects if you want per-day change diff
    # We don't persist the whole norm; reconstruct "old norm" from prev_fp is not possible,
    # so for per-day changed-keys we do a heuristic: mark both days as changed when FP differs.
    # If you want exact per-day diff, persist norms externally.
    # Here we keep it simple but still expose day flags sensibly.
    for e in entries:
        region = (e.get("region") or "").strip()
        days = e.get("days") or {}

        norm = _imd_region_norm(region, days)
        fp = json.dumps(norm, sort_keys=True, separators=(",", ":"))
        fp_digest = hashlib.sha1(fp.encode("utf-8")).hexdigest()

        fp_by_region[region] = fp_digest

        # Decide if changed
        prev_digest = prev_fp.get(region)
        changed = prev_digest != fp_digest

        # Timestamp: bump to now on change; otherwise keep previous ts if available, else 0
        prev_region_ts = prev_ts.get(region, 0.0)
        ts = now_ts if changed else prev_region_ts

        ts_by_region[region] = ts

        item = dict(e)
        item["timestamp"] = ts
        item["is_new"] = bool(changed)

        # Per-day flags: if changed, conservatively mark both days as new
        changed_keys = {"today", "tomorrow"} if changed else set()
        _propagate_day_is_new(item, changed_keys)

        out.append(item)

    return out, fp_by_region, ts_by_region


# Optional helper to "acknowledge" current IMD entries as seen (for clear-on-close UX),
# without putting logic into the controller. The controller can call this single pure function.
def snapshot_imd_seen(
    *,
    entries: Sequence[Mapping[str, Any]],
    now_ts: float | None = None,
) -> tuple[dict[str, str], dict[str, float], list[dict]]:
    """
    Produce a snapshot of current fingerprints and timestamps for IMD entries and
    return a copy of entries with all `is_new` flags cleared (including day flags).

    Returns:
        (fp_by_region, ts_by_region, cleared_entries)
    """
    now_ts = now_ts or utcnow_ts()
    fp_by_region: dict[str, str] = {}
    ts_by_region: dict[str, float] = {}
    cleared: list[dict] = []

    for e in entries:
        region = (e.get("region") or "").strip()
        norm = _imd_region_norm(region, e.get("days") or {})
        fp_raw = json.dumps(norm, sort_keys=True, separators=(",", ":"))
        fp_digest = hashlib.sha1(fp_raw.encode("utf-8")).hexdigest()
        fp_by_region[region] = fp_digest
        ts_by_region[region] = now_ts

        d = dict(e)
        d["is_new"] = False
        # clear day flags if present
        days = d.get("days")
        if isinstance(days, dict):
            dd = dict(days)
            if isinstance(dd.get("today"), dict):
                t = dict(dd["today"])
                t["is_new"] = False
                dd["today"] = t
            if isinstance(dd.get("tomorrow"), dict):
                t = dict(dd["tomorrow"])
                t["is_new"] = False
                dd["tomorrow"] = t
            d["days"] = dd
        cleared.append(d)

    return fp_by_region, ts_by_region, cleared


# ----------------------------------------------------------
# Backwards-compatible API used by controller code
# ----------------------------------------------------------

def advance_seen(last_seen_ts: float | None) -> float:
    """
    Move the 'last seen' pointer to now.
    """
    return utcnow_ts()

def attach_timestamp(items: Sequence[Mapping[str, Any]], default_ts: float | None = None) -> list[dict]:
    """
    Attach a 'timestamp' field to items that lack it, using default_ts or 0.0.
    """
    out = []
    default_ts = default_ts if default_ts is not None else 0.0
    for it in items:
        d = dict(it)
        if not isinstance(d.get("timestamp"), (int, float)):
            d["timestamp"] = default_ts
        out.append(d)
    return out

def sort_by_timestamp_desc(items: Sequence[Mapping[str, Any]]) -> list[dict]:
    """
    Sort entries newest-first by 'timestamp'.
    """
    return sorted((dict(x) for x in items), key=lambda d: d.get("timestamp") or 0.0, reverse=True)


# ----------------------------------------------------------
# Grouping utilities (used by various renderers)
# ----------------------------------------------------------

def group_by(items: Iterable[Mapping[str, Any]], key_fn) -> list[tuple[Any, list[dict]]]:
    """
    Group items by key_fn preserving order of first occurrence.
    """
    keys = []
    buckets = {}
    for it in items:
        k = key_fn(it)
        if k not in buckets:
            keys.append(k)
            buckets[k] = []
        buckets[k].append(dict(it))
    return [(k, buckets[k]) for k in keys]


# ----------------------------------------------------------
# END
# ----------------------------------------------------------
