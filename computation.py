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

# --------------------------------------------------------------------
# Timestamp helpers (robust parse/format/now)
# --------------------------------------------------------------------

RFC3339 = "%Y-%m-%dT%H:%M:%SZ"

def utcnow_ts() -> float:
    return time.time()

def to_rfc3339(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(RFC3339)

def parse_rfc3339(ts: str | None) -> float:
    if not ts:
        return 0.0
    try:
        return datetime.strptime(ts, RFC3339).replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return 0.0

def parse_timestamp(ts: Any) -> float:
    """Best-effort parser that accepts float/int/iso/rfc3339-ish strings; fallback 0.0."""
    if isinstance(ts, (int, float)):
        try:
            return float(ts)
        except Exception:
            return 0.0
    if isinstance(ts, datetime):
        try:
            return ts.timestamp()
        except Exception:
            return 0.0
    if isinstance(ts, str) and ts.strip():
        # try RFC3339 first
        r = parse_rfc3339(ts)
        if r > 0:
            return r
        # lenient: YYYY-MM-DD HH:MM (UTC) etc.
        try:
            from dateutil import parser as dateparser  # only used if present in env
            return dateparser.parse(ts).timestamp()
        except Exception:
            return 0.0
    return 0.0

def attach_timestamp(items: Sequence[Mapping[str, Any]], *, published_key: str = "published") -> list[dict]:
    """Attach a numeric 'timestamp' to each item; if missing, derive from published_key (or 0.0)."""
    out: list[dict] = []
    for it in items or []:
        d = dict(it)
        ts = d.get("timestamp")
        if not isinstance(ts, (int, float)):
            d["timestamp"] = parse_timestamp(d.get(published_key))
        out.append(d)
    return out

def sort_by_timestamp_desc(items: Sequence[Mapping[str, Any]]) -> list[dict]:
    return sorted((dict(x) for x in items or []), key=lambda d: float(d.get("timestamp") or 0.0), reverse=True)

def compute_counts(entries: Sequence[Mapping[str, Any]], last_seen_ts: float | None) -> dict[str, int]:
    """
    Returns a dict with counts of entries whose 'timestamp' is strictly greater than last_seen_ts.
    Missing timestamp entries are ignored.
    """
    if last_seen_ts is None:
        last_seen_ts = 0.0
    total = 0
    by_severity = {"warning": 0, "watch": 0, "advisory": 0, "other": 0}
    for e in entries or []:
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

def advance_seen(_: float | None = None) -> float:
    """Move the 'last seen' pointer to now."""
    return utcnow_ts()

# --------------------------------------------------------------------
# EC (Environment Canada) helpers for remaining-new totals
# --------------------------------------------------------------------

def ec_bucket_from_title(title: str) -> str | None:
    """Very small mapper from EC titles → buckets (warning/watch/advisory/other). Extend as needed."""
    t = (title or "").lower()
    if not t:
        return None
    if "warning" in t:
        return "warning"
    if "watch" in t:
        return "watch"
    if "advisory" in t:
        return "advisory"
    return "other"

def entry_ts(e: Mapping[str, Any]) -> float:
    ts = e.get("timestamp")
    if isinstance(ts, (int, float)) and float(ts) > 0:
        return float(ts)
    return parse_timestamp(e.get("published"))

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
# NWS (US National Weather Service) helpers for remaining-new totals
# --------------------------------------------------------------------

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

def meteoalarm_snapshot_ids(entries: Sequence[Mapping[str, Any]]) -> set[str]:
    """
    Make a snapshot set of current meteoalarm IDs from the nested country→area lists.
    Structure expected:
      entries = [
        { "country": "...", "alerts": { "AreaName": [ {alert}, {alert}, ... ], ... } },
        ...
      ]
    """
    ids: set[str] = set()
    for country in entries or []:
        alerts_map = country.get("alerts", {}) or {}
        for alerts in alerts_map.values():
            for a in alerts or []:
                ids.add(alert_id(a))
    return ids

def meteoalarm_unseen_active_instances(
    entries: Sequence[Mapping[str, Any]],
    last_seen_ids: set[str],
    *,
    levels_considered: tuple[str, ...] = ("Orange", "Red"),
) -> int:
    """
    Count how many current (nested) Meteoalarm alerts are not in `last_seen_ids`,
    considering only the specified severity levels.
    """
    unseen = 0
    for country in entries or []:
        alerts_map = country.get("alerts", {}) or {}
        for alerts in alerts_map.values():
            for a in alerts or []:
                if a.get("level") not in levels_considered:
                    continue
                if alert_id(a) not in last_seen_ids:
                    unseen += 1
    return unseen

def meteoalarm_mark_and_sort(
    entries: Sequence[Mapping[str, Any]],
    *,
    last_seen_ids: set[str] | None = None,
) -> list[dict]:
    """
    Flatten + mark 'is_new' on meteoalarm entries, then sort by severity/recency.
    (Kept here for completeness; your renderer may not need this flattening.)
    """
    last_seen_ids = last_seen_ids or set()
    flat: list[dict] = []
    for country in entries or []:
        cname = country.get("country") or ""
        alerts_map = country.get("alerts", {}) or {}
        for area_name, alerts in alerts_map.items():
            for a in alerts or []:
                d = dict(a)
                d["country"] = cname
                d["area"] = area_name
                d["is_new"] = alert_id(a) not in last_seen_ids
                # derive a numeric timestamp for sorting
                d["timestamp"] = entry_ts(a)
                flat.append(d)

    # severity rank: Red>Orange>Yellow>Other
    def _sev_rank(level: str | None) -> int:
        if (level or "").lower().startswith("red"):
            return 3
        if (level or "").lower().startswith("orange"):
            return 2
        if (level or "").lower().startswith("yellow"):
            return 1
        return 0

    return sorted(flat, key=lambda d: (_sev_rank(d.get("level")), float(d.get("timestamp") or 0.0)), reverse=True)

# --------------------------------------------------------------------
# IMD (India) content-change fingerprinting
# --------------------------------------------------------------------

def _canonicalize_imd_day(day: Mapping[str, Any] | None) -> dict:
    day = day or {}
    severity = day.get("severity")
    hazards = day.get("hazards") or []
    date = day.get("date")
    canon_hazards = sorted({(h or "").strip() for h in hazards if h is not None})
    return {"severity": severity, "hazards": canon_hazards, "date": date}

def _imd_region_norm(region: str, days: Mapping[str, Any] | None) -> dict:
    days = days or {}
    today = _canonicalize_imd_day(days.get("today"))
    tomorrow = _canonicalize_imd_day(days.get("tomorrow"))
    return {"region": (region or "").strip(), "today": today, "tomorrow": tomorrow}

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
    prev_fp = dict(prev_fp or {})
    prev_ts = dict(prev_ts or {})
    updated: list[dict] = []
    fp_by_region: dict[str, str] = {}
    ts_by_region: dict[str, float] = {}

    for e in entries or []:
        region = (e.get("region") or "").strip()
        days = e.get("days") or {}
        norm = _imd_region_norm(region, days)

        # deterministic JSON (stable keys + compact separators) as the content fingerprint
        fp = json.dumps(norm, sort_keys=True, separators=(",", ":"))
        changed = (prev_fp.get(region) != fp)

        # timestamp: bump to now on change; otherwise keep previous ts if available; else set to now
        ts = now_ts if changed else float(prev_ts.get(region) or 0.0)
        if ts <= 0:
            ts = now_ts

        d = dict(e)
        d["timestamp"] = float(ts)
        d["is_new"] = bool(changed)

        # propagate to day dicts if present
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
        ts_by_region[region] = float(ts)

    return updated, fp_by_region, ts_by_region

# --------------------------------------------------------------------
# Generic grouping utility (used by various renderers)
# --------------------------------------------------------------------

def group_by(items: Iterable[Mapping[str, Any]], key_fn) -> list[tuple[Any, list[dict]]]:
    keys = []
    buckets: dict[Any, list[dict]] = {}
    for it in items or []:
        k = key_fn(it)
        if k not in buckets:
            keys.append(k)
            buckets[k] = []
        buckets[k].append(dict(it))
    return [(k, buckets[k]) for k in keys]

# --------------------------------------------------------------------
# Optional: pure helper to snapshot IMD as "seen" (for clear-on-close UX)
# --------------------------------------------------------------------

def snapshot_imd_seen(
    *,
    entries: Sequence[Mapping[str, Any]],
    now_ts: float | None = None,
) -> tuple[dict[str, str], dict[str, float], list[dict]]:
    """
    Produce a snapshot of current fingerprints and timestamps for IMD entries and
    return a copy of entries with all `is_new` flags cleared (including per-day flags).

    Returns:
        (fp_by_region, ts_by_region, cleared_entries)
    """
    now_ts = float(now_ts or utcnow_ts())
    fp_by_region: dict[str, str] = {}
    ts_by_region: dict[str, float] = {}
    cleared: list[dict] = []

    for e in entries or []:
        region = (e.get("region") or "").strip()
        norm = _imd_region_norm(region, e.get("days") or {})
        fp_raw = json.dumps(norm, sort_keys=True, separators=(",", ":"))
        fp_by_region[region] = fp_raw
        ts_by_region[region] = now_ts

        d = dict(e)
        d["is_new"] = False
        if isinstance(d.get("days"), dict):
            dd = dict(d["days"])
            if isinstance(dd.get("today"), dict):
                t = dict(dd["today"]); t["is_new"] = False; dd["today"] = t
            if isinstance(dd.get("tomorrow"), dict):
                t = dict(dd["tomorrow"]); t["is_new"] = False; dd["tomorrow"] = t
            d["days"] = dd
        cleared.append(d)

    return fp_by_region, ts_by_region, cleared
