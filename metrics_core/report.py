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
    if row is None:
        # No window ever materialised: genuinely empty, distinct from a
        # window whose observed values happen to be zero.
        return {"level": level, "start": start, "end": end,
                "state": "empty", "coverage": coverage,
                "range_restricted": False,
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
            "version": version, "count": count, "sum": total,
            "buckets": buckets}


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
        report_id = _report_id(metric, range_start, range_end,
                               correction_version, entries)
        payload = {"levels": list(levels), "entries": entries,
                   "discrepancies": discrepancies}
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
