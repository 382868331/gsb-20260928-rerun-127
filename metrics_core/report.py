"""Snapshot reports over the multi-level window aggregates.

A report pins an observation range and the correction version it reflects,
captures every granularity in one consistent read, and is stored durably so
citations survive later corrections, restarts and background cleanup.
"""

from __future__ import annotations

import hashlib
import json
import time


def _window_key(level: int, metric: str, start: int) -> str:
    return f"{level}:{metric}:{start}"


def _bucket_index(bounds, value):
    for i, bound in enumerate(bounds):
        if value <= bound:
            return i
    return len(bounds)


def _range_aggregate(core, metric, start, end, lo, hi):
    """Aggregate only the observations inside [lo, hi) of window [start, end).

    A window the report range cuts through must contribute just its in-range
    observations, never the whole pre-aggregated window (which may include
    samples outside the pinned range, e.g. a 500s reading inside an hour
    window reported over 0-60s).
    """
    level0 = core.levels[0]
    rows = core.db.execute(
        "SELECT c.kind, c.value FROM contributions c "
        "JOIN samples s ON s.sample_id = c.sample_id "
        "WHERE c.level=? AND c.metric=? AND c.start>=? AND c.start<? "
        "AND c.retracted=0 AND s.ts>=? AND s.ts<?",
        (level0, metric, start, end, lo, hi)).fetchall()
    total = sum(value for _, value in rows)
    buckets = [0] * (len(core.buckets) + 1)
    for kind, value in rows:
        if kind == "dist":
            buckets[_bucket_index(core.buckets, value)] += 1
    return len(rows), total, buckets


def _entry(core, level, metric, start, row, range_start, range_end):
    end = start + level
    coverage = "full" if start >= range_start and end <= range_end else "partial"
    observed_start = max(start, range_start)
    observed_end = min(end, range_end)
    if row is None:
        # No window ever materialised: genuinely empty, distinct from a
        # window whose observed values happen to be zero.
        return {"level": level, "start": start, "end": end,
                "state": "empty", "coverage": coverage,
                "range_restricted": False,
                "observed_start": observed_start if coverage == "partial" else start,
                "observed_end": observed_end if coverage == "partial" else end,
                "version": None, "count": None, "sum": None, "buckets": None}
    status, version, count, total, buckets = row
    restricted = False
    if coverage == "partial":
        # Range cuts through this window: count only in-range observations.
        count, total, buckets = _range_aggregate(
            core, metric, start, end,
            max(range_start, start), min(range_end, end))
        restricted = True
    else:
        buckets = json.loads(buckets)
    return {"level": level, "start": start, "end": end,
            "state": status, "coverage": coverage,
            "range_restricted": restricted,
            "observed_start": observed_start if coverage == "partial" else start,
            "observed_end": observed_end if coverage == "partial" else end,
            "version": version, "count": count, "sum": total,
            "buckets": buckets}


def _observations(core, metric, range_start, range_end):
    level0 = core.levels[0]
    rows = core.db.execute(
        "SELECT s.ts, c.kind, c.value FROM contributions c "
        "JOIN samples s ON s.sample_id = c.sample_id "
        "WHERE c.level=? AND c.metric=? AND c.retracted=0 "
        "AND s.ts>=? AND s.ts<? ORDER BY s.ts, s.sample_id",
        (level0, metric, range_start, range_end)).fetchall()
    return [{"ts": ts, "kind": kind, "value": value,
             "bucket": _bucket_index(core.buckets, value) if kind == "dist" else None}
            for ts, kind, value in rows]


def _revision_snapshot(db, entries, metric):
    sources = []
    keys = sorted({(e["level"], e["start"]) for e in entries
                   if e.get("version") is not None})
    for level, start in keys:
        rows = db.execute(
            "SELECT version, reason FROM revisions WHERE window_key=? ORDER BY id",
            (_window_key(level, metric, start),)).fetchall()
        if rows:
            sources.append({"level": level, "start": start,
                            "revisions": [{"version": v, "reason": r}
                                           for v, r in rows]})
    return sources


def _aggregate_observations(report, lo, hi):
    rows = [o for o in report.get("observations", [])
            if lo <= o["ts"] < hi]
    if not rows:
        return None
    bounds = report.get("bucket_bounds")
    bucket_count = len(bounds) + 1 if bounds is not None else None
    buckets = [0] * bucket_count if bucket_count is not None else []
    for observation in rows:
        if observation["kind"] == "dist" and buckets:
            buckets[observation["bucket"]] += 1
    return {"count": len(rows), "sum": sum(o["value"] for o in rows),
            "buckets": buckets if buckets else None}


def _reconcile(entries, levels):
    """Check that each fully-covered parent window equals the sum of its
    fully-covered children (fine-grained <-> aggregate cross-check)."""
    discrepancies = []
    by_key = {(e["level"], e["start"]): e for e in entries}
    for i, parent_level in enumerate(levels[1:], 1):
        child_level = levels[i - 1]
        if parent_level % child_level:
            continue
        for e in entries:
            if e["level"] != parent_level or e["coverage"] != "full":
                continue
            if e["state"] == "empty":
                continue
            children = [by_key.get((child_level, s))
                        for s in range(e["start"], e["end"], child_level)]
            if any(c is None or c["state"] == "empty" for c in children):
                continue  # incomplete child coverage: cannot cross-check
            count = sum(c["count"] for c in children)
            total = sum(c["sum"] for c in children)
            buckets = [sum(c["buckets"][k] for c in children)
                       for k in range(len(children[0]["buckets"]))]
            if (count != e["count"] or abs(total - e["sum"]) > 1e-9
                    or buckets != e["buckets"]):
                discrepancies.append({
                    "level": parent_level, "start": e["start"],
                    "parent": {"count": e["count"], "sum": e["sum"],
                               "buckets": e["buckets"]},
                    "children_sum": {"count": count, "sum": total,
                                     "buckets": buckets}})
    return discrepancies


def _report_id(metric, range_start, range_end, correction_version, entries):
    canonical = json.dumps(
        {"metric": metric, "range": [range_start, range_end],
         "correction_version": correction_version, "entries": entries},
        sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def generate(core, metric, range_start, range_end, levels=None):
    """Build (or, after a crash/restart, rebuild identically) a pinned report.

    All reads happen inside one transaction, so corrections arriving during
    the read are never mixed into the snapshot.
    """
    if range_end <= range_start:
        raise ValueError("range_end must be after range_start")
    levels = tuple(sorted(levels or core.levels))
    db = core.db
    db.execute("BEGIN")
    try:
        correction_version = core.correction_version()
        revision_cursor = db.execute(
            "SELECT COALESCE(MAX(id),0) FROM revisions").fetchone()[0]
        entries = []
        observations = _observations(core, metric, range_start, range_end)
        for level in levels:
            first = int(range_start // level) * level
            start = first
            while start < range_end:
                row = db.execute(
                    "SELECT status, version, count, sum, buckets FROM windows "
                    "WHERE window_key=?",
                    (_window_key(level, metric, start),)).fetchone()
                entries.append(_entry(core, level, metric, start, row,
                                      range_start, range_end))
                start += level
        discrepancies = _reconcile(entries, levels)
        revision_sources = _revision_snapshot(db, entries, metric)
        report_id = _report_id(metric, range_start, range_end,
                               correction_version, entries)
        payload = {"levels": list(levels), "entries": entries,
                   "discrepancies": discrepancies,
                   "observations": observations,
                   "bucket_bounds": list(core.buckets),
                   "revision_sources": revision_sources}
        # INSERT OR IGNORE: regenerating after a mid-processing restart yields
        # the same deterministic id and never duplicates the stored report.
        db.execute(
            "INSERT OR IGNORE INTO reports(report_id,metric,range_start,range_end,"
            "correction_version,revision_cursor,created_at,adopted,payload) "
            "VALUES(?,?,?,?,?,?,?,0,?)",
            (report_id, metric, range_start, range_end, correction_version,
             revision_cursor, time.time(), json.dumps(payload, sort_keys=True)))
        db.execute("COMMIT")
    except Exception:
        db.execute("ROLLBACK")
        raise
    return get(core, report_id)


def _to_dict(row):
    report_id, metric, rs, re_, cv, rc, created, adopted, payload = row
    out = {"report_id": report_id, "metric": metric,
           "range_start": rs, "range_end": re_,
           "correction_version": cv, "revision_cursor": rc,
           "created_at": created, "adopted": bool(adopted)}
    out.update(json.loads(payload))
    # Incomplete coverage means a window is still open; an empty window is a
    # legitimate final state (no observations), not incomplete coverage.
    out["complete"] = all(
        e["state"] != "open" or e["coverage"] == "partial"
        for e in out["entries"])
    out["consistent"] = not out["discrepancies"]
    return out


def get(core, report_id):
    row = core.db.execute(
        "SELECT report_id,metric,range_start,range_end,correction_version,"
        "revision_cursor,created_at,adopted,payload FROM reports "
        "WHERE report_id=?", (report_id,)).fetchone()
    return _to_dict(row) if row else None


def adopt(core, report_id):
    """Formally adopt a report as the current reference. Older reports stay
    stored and remain citable; adoption never deletes anything."""
    cur = core.db.execute(
        "UPDATE reports SET adopted=1 WHERE report_id=?", (report_id,))
    core.db.commit()
    if cur.rowcount == 0:
        raise KeyError(f"unknown report {report_id!r}")
    return get(core, report_id)


def _changed(before, after):
    # A bare version bump (e.g. an out-of-range late sample revising a
    # partially covered window) does not change any reported figure, so it
    # must not show up as a difference.
    fields = ("state", "count", "sum", "buckets")
    return {f: {"before": before[f], "after": after[f]}
            for f in fields if before[f] != after[f]}


def diff(core, old_id, new_id):
    """Compare two reports on the same metric and range.

    Lists which aggregates changed inside the pinned range, and which
    corrections arrived for windows the range does not cover (those must not
    change the report figures).
    """
    old, new = get(core, old_id), get(core, new_id)
    if old is None or new is None:
        raise KeyError("both reports must exist")
    if (old["metric"], old["range_start"], old["range_end"]) != \
            (new["metric"], new["range_start"], new["range_end"]):
        raise ValueError("reports cover different metric or range")

    old_entries = {(e["level"], e["start"]): e for e in old["entries"]}
    changed = []
    for e in new["entries"]:
        before = old_entries.get((e["level"], e["start"]))
        if before is None:
            continue
        delta = _changed(before, e)
        if delta:
            changed.append({"level": e["level"], "start": e["start"],
                            "changes": delta})

    rs, re_ = old["range_start"], old["range_end"]
    # A revision only counts as in-range if it actually changed a reported
    # figure. Classifying by window-start overlap would misattribute
    # out-of-range corrections (e.g. a 510s late sample revising the hour
    # window that starts at 0s) to a range they do not affect.
    changed_keys = {(c["level"], c["start"]) for c in changed}
    in_range, out_of_range = [], []
    rows = core.db.execute(
        "SELECT window_key, version, reason FROM revisions "
        "WHERE id>? ORDER BY id", (old["revision_cursor"],)).fetchall()
    for window_key, version, reason in rows:
        level, metric, start = window_key.split(":")
        level, start = int(level), int(start)
        item = {"level": level, "metric": metric, "start": start,
                "version": version, "reason": reason}
        covered = (metric == old["metric"]
                   and (level, start) in changed_keys)
        (in_range if covered else out_of_range).append(item)

    return {"old_report": old_id, "new_report": new_id,
            "metric": old["metric"], "range": [rs, re_],
            "correction_version": {"before": old["correction_version"],
                                   "after": new["correction_version"]},
            "changed": changed,
            "revisions_in_range": in_range,
            "revisions_out_of_range": out_of_range}


def _observed_interval(entry):
    if entry is None or entry.get("state") == "empty":
        return None
    if entry.get("state") == "sealed":
        return entry["observed_start"], entry["observed_end"]
    return None


def _zero_value(report):
    bounds = report.get("bucket_bounds") or []
    return {"count": 0, "sum": 0.0, "buckets": [0] * (len(bounds) + 1)}


def _value_from(entry):
    if entry is None or entry["state"] == "empty":
        return None
    return {"count": entry["count"], "sum": entry["sum"],
            "buckets": entry["buckets"]}


def _delta(left, right):
    if left is None or right is None:
        return None
    out = {"count": right["count"] - left["count"],
           "sum": right["sum"] - left["sum"]}
    if len(left["buckets"]) == len(right["buckets"]):
        out["buckets"] = [b - a for a, b in zip(left["buckets"], right["buckets"])]
    else:
        out["buckets"] = None
    return out


def _entry_map(report, level):
    return {(e["level"], e["start"]): e for e in report["entries"]
            if e["level"] == level}


def _common_levels(left, right):
    return sorted(set(left["levels"]) & set(right["levels"]))


def _scope(level, left, right, lo, hi, kind, reason,
           left_value=None, right_value=None, include_change=True):
    item = {"level": level, "start": lo, "end": hi, "kind": kind,
            "reason": reason,
            "left_entry_version": left.get("version") if left else None,
            "right_entry_version": right.get("version") if right else None}
    if left_value is not None:
        item["left"] = left_value
    if right_value is not None:
        item["right"] = right_value
    if include_change and left_value is not None and right_value is not None:
        item["change"] = _delta(left_value, right_value)
    else:
        item["change"] = None
    return item


def compare(core, left_id, right_id, basis="intersection"):
    if basis not in ("intersection", "direct"):
        raise ValueError("basis must be 'intersection' or 'direct'")
    left, right = get(core, left_id), get(core, right_id)
    if left is None or right is None:
        raise KeyError("both reports must exist")

    levels = _common_levels(left, right)
    direct, intersection_only, no_basis = [], [], []
    changed_keys = set()
    comparable = left["metric"] == right["metric"] and bool(levels)

    if comparable:
        finest = levels[0]
        left_entries = _entry_map(left, finest)
        right_entries = _entry_map(right, finest)
        boundaries = {min(left["range_start"], right["range_start"])}
        boundaries.update(start + finest for _, start in left_entries)
        boundaries.update(start + finest for _, start in right_entries)
        boundaries.update({left["range_end"], right["range_end"]})
        range_min = min(left["range_start"], right["range_start"])
        range_max = max(left["range_end"], right["range_end"])
        boundaries = sorted(b for b in boundaries if range_min < b <= range_max)
        cursor = range_min
        for boundary in boundaries:
            if boundary <= cursor:
                continue
            window_start = int(cursor // finest) * finest
            left_entry = left_entries.get((finest, window_start))
            right_entry = right_entries.get((finest, window_start))
            left_value = _aggregate_observations(left, cursor, boundary)
            right_value = _aggregate_observations(right, cursor, boundary)
            left_interval = _observed_interval(left_entry)
            right_interval = _observed_interval(right_entry)
            left_full = left_interval == (window_start, window_start + finest)
            right_full = right_interval == (window_start, window_start + finest)
            left_has = left_full or left_value is not None
            right_has = right_full or right_value is not None
            if left_has and left_value is None:
                left_value = _zero_value(left)
            if right_has and right_value is None:
                right_value = _zero_value(right)
            if left_has and right_has:
                kind = "direct" if left_interval == right_interval else "intersection_only"
                reason = ("identical observed interval and granularity"
                          if kind == "direct" else "only the observed intersection has evidence in both reports")
                item = _scope(finest, left_entry, right_entry, cursor, boundary,
                              kind, reason, left_value, right_value)
                (direct if kind == "direct" else intersection_only).append(item)
            elif left_has or right_has:
                no_basis.append(_scope(
                    finest, left_entry, right_entry, cursor, boundary, "no_basis",
                    "one report lacks an observed basis for this interval",
                    left_value if left_has else None,
                    right_value if right_has else None, False))
            else:
                no_basis.append(_scope(
                    finest, left_entry, right_entry, cursor, boundary, "no_basis",
                    "neither report contains an observed basis", None, None, False))
            cursor = boundary

        if basis == "direct":
            moved = intersection_only
            intersection_only = []
            for item in moved:
                item["kind"] = "no_basis"
                item["reason"] = "direct comparison requires identical observed intervals"
                item["change"] = None
                no_basis.append(item)

    change_sources = []
    changed_keys = {
        (item["level"], item["start"])
        for item in direct + intersection_only
        if item["change"] is not None and (
            item["change"]["count"] or item["change"]["sum"]
            or item["change"]["buckets"])}
    for level, start in sorted(changed_keys):
        revision_rows = []
        for report in (left, right):
            for source in report.get("revision_sources", []):
                if source["level"] == level and source["start"] == start:
                    revision_rows.extend(
                        (r["version"], r["reason"]) for r in source["revisions"])
        if not revision_rows:
            key = _window_key(level, left["metric"], start)
            revision_rows = core.db.execute(
                "SELECT version, reason FROM revisions WHERE window_key=? ORDER BY id",
                (key,)).fetchall()
        revision_rows = sorted(set(revision_rows))
        change_sources.append({
            "level": level, "start": start,
            "revisions": [{"version": version, "reason": reason}
                           for version, reason in revision_rows],
            "left_report": {"report_id": left_id,
                             "correction_version": left["correction_version"]},
            "right_report": {"report_id": right_id,
                              "correction_version": right["correction_version"]}})
    payload = {
        "left_report": {"report_id": left_id,
                        "correction_version": left["correction_version"],
                        "adopted": left["adopted"],
                        "metric": left["metric"],
                        "range": [left["range_start"], left["range_end"]],
                        "levels": left["levels"]},
        "right_report": {"report_id": right_id,
                         "correction_version": right["correction_version"],
                         "adopted": right["adopted"],
                         "metric": right["metric"],
                         "range": [right["range_start"], right["range_end"]],
                         "levels": right["levels"]},
        "requested_basis": basis,
        "effective_basis": basis if comparable else "none",
        "directly_comparable": direct,
        "intersection_only": intersection_only,
        "no_comparable_basis": no_basis,
        "changed_windows": sorted(list(changed_keys)),
        "change_sources": change_sources,
    }
    canonical = json.dumps({"left": left_id, "right": right_id,
                            "basis": basis, "decision": payload},
                           sort_keys=True)
    comparison_id = hashlib.sha256(canonical.encode()).hexdigest()[:16]
    core.db.execute(
        "INSERT OR IGNORE INTO report_comparisons(comparison_id,left_report_id,"
        "right_report_id,requested_basis,created_at,payload) VALUES(?,?,?,?,?,?)",
        (comparison_id, left_id, right_id, basis, time.time(),
         json.dumps(payload, sort_keys=True)))
    core.db.commit()
    return get_comparison(core, comparison_id)


def get_comparison(core, comparison_id):
    row = core.db.execute(
        "SELECT comparison_id,left_report_id,right_report_id,requested_basis,"
        "created_at,payload FROM report_comparisons WHERE comparison_id=?",
        (comparison_id,)).fetchone()
    if row is None:
        return None
    cid, left_id, right_id, basis, created_at, payload = row
    out = {"comparison_id": cid, "left_report_id": left_id,
           "right_report_id": right_id, "requested_basis": basis,
           "created_at": created_at}
    out.update(json.loads(payload))
    return out
