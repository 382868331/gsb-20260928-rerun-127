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
        "AND c.retracted=0 AND s.ts>=? AND s.ts<? "
        "GROUP BY c.sample_id",
        (level0, metric, start, end, lo, hi)).fetchall()
    return _aggregate_observations(
        [{"kind": kind, "value": value} for kind, value in rows],
        core.buckets)


def _observations(core, metric, lo, hi):
    """Return the exact non-retracted level-zero observations in a range."""
    level0 = core.levels[0]
    rows = core.db.execute(
        "SELECT DISTINCT s.sample_id, s.ts, c.kind, c.value "
        "FROM contributions c JOIN samples s ON s.sample_id=c.sample_id "
        "WHERE c.level=? AND c.metric=? AND c.retracted=0 "
        "AND s.ts>=? AND s.ts<? ORDER BY s.sample_id",
        (level0, metric, lo, hi)).fetchall()
    return [{"sample_id": sid, "ts": ts, "kind": kind, "value": value}
            for sid, ts, kind, value in rows]


def _aggregate_observations(observations, bucket_bounds):
    total = sum(o["value"] for o in observations)
    buckets = [0] * (len(bucket_bounds) + 1)
    for observation in observations:
        if observation["kind"] == "dist":
            buckets[_bucket_index(bucket_bounds, observation["value"])] += 1
    return len(observations), total, buckets


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


def _decision_id(payload):
    canonical = json.dumps(payload, sort_keys=True)
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
        observations = _observations(core, metric, range_start, range_end)
        discrepancies = _reconcile(entries, levels)
        report_id = _report_id(metric, range_start, range_end,
                               correction_version, entries)
        payload = {"levels": list(levels),
                   "bucket_bounds": list(core.buckets), "entries": entries,
                   "observations": observations,
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


def _require_reports(core, old_id, new_id):
    old, new = get(core, old_id), get(core, new_id)
    if old is None or new is None:
        raise KeyError("both reports must exist")
    if old["metric"] != new["metric"]:
        raise ValueError("reports cover different metrics")
    return old, new


def _report_ref(report):
    return {"report_id": report["report_id"],
            "range": [report["range_start"], report["range_end"]],
            "correction_version": report["correction_version"],
            "revision_cursor": report["revision_cursor"],
            "created_at": report["created_at"]}


def _observations_in(report, level, start, end, lo, hi):
    return [o for o in report.get("observations", [])
            if lo <= o["ts"] < hi
            and start <= o["ts"] < start + level]


def _entry_ref(entry):
    if entry is None:
        return None
    return {"state": entry["state"], "coverage": entry["coverage"],
            "range_restricted": entry["range_restricted"],
            "version": entry["version"], "start": entry["start"],
            "end": entry["end"], "count": entry["count"],
            "sum": entry["sum"]}


def _scope_decision(old, new, level, window_start, old_entries, new_entries):
    window_end = window_start + level
    old_lo = max(old["range_start"], window_start)
    old_hi = min(old["range_end"], window_end)
    new_lo = max(new["range_start"], window_start)
    new_hi = min(new["range_end"], window_end)
    lo, hi = max(old_lo, new_lo), min(old_hi, new_hi)
    if lo >= hi:
        return None

    old_entry = old_entries[(level, window_start)]
    new_entry = new_entries[(level, window_start)]
    old_obs = _observations_in(old, level, window_start, window_end, lo, hi)
    new_obs = _observations_in(new, level, window_start, window_end, lo, hi)
    full = (old_entry["coverage"] == "full"
            and new_entry["coverage"] == "full"
            and lo == window_start and hi == window_end)
    old_sealed_zero = (old_entry["state"] == "sealed"
                       and old_entry["count"] is not None)
    new_sealed_zero = (new_entry["state"] == "sealed"
                       and new_entry["count"] is not None)
    if full and old_sealed_zero and new_sealed_zero:
        classification = "directly_comparable"
        reason = "both reports observe the complete aligned window"
    elif old_obs and new_obs:
        classification = "intersection_only"
        reason = "both reports have observations in the common intersection"
    else:
        classification = "no_basis"
        missing = []
        if not old_obs and not old_sealed_zero:
            missing.append("old")
        if not new_obs and not new_sealed_zero:
            missing.append("new")
        reason = "no observed facts on the " + " and ".join(missing) + " side"

    return {"level": level, "window_start": window_start,
            "window_end": window_end,
            "old_observed_range": [old_lo, old_hi],
            "new_observed_range": [new_lo, new_hi],
            "intersection_range": [lo, hi],
            "classification": classification, "reason": reason,
            "old": _entry_ref(old_entry), "new": _entry_ref(new_entry),
            "old_observation_count": len(old_obs),
            "new_observation_count": len(new_obs),
            "old_sample_ids": [o["sample_id"] for o in old_obs],
            "new_sample_ids": [o["sample_id"] for o in new_obs]}


def prepare_comparison(core, old_id, new_id, levels=None):
    """Persist the user's comparison basis before calculating any changes."""
    old, new = _require_reports(core, old_id, new_id)
    requested = tuple(sorted(levels or (
            set(old["levels"]) & set(new["levels"]))))
    available = set(old["levels"]) & set(new["levels"])
    adopted = tuple(level for level in requested if level in available)
    skipped = [{"level": level,
                "reason": "granularity is not present in both selected reports"}
               for level in requested if level not in available]
    old_entries = {(e["level"], e["start"]): e for e in old["entries"]}
    new_entries = {(e["level"], e["start"]): e for e in new["entries"]}
    scopes = []
    for level in adopted:
        start = int(min(old["range_start"], new["range_start"]) // level) * level
        end_limit = max(old["range_end"], new["range_end"])
        while start < end_limit:
            decision = _scope_decision(
                old, new, level, start, old_entries, new_entries)
            if decision is not None:
                scopes.append(decision)
            start += level

    payload = {"old_report": _report_ref(old),
               "new_report": _report_ref(new),
               "metric": old["metric"],
               "requested_levels": list(requested),
               "adopted_levels": list(adopted),
               "skipped_levels": skipped,
               "scopes": scopes}
    comparison_id = _decision_id(payload)
    payload = {"comparison_id": comparison_id, **payload,
               "created_at": time.time()}
    core.db.execute(
        "INSERT OR IGNORE INTO report_comparisons(comparison_id,old_report_id,"
        "new_report_id,requested_levels,adopted_levels,created_at,payload) "
        "VALUES(?,?,?,?,?,?,?)",
        (comparison_id, old_id, new_id, json.dumps(list(requested)),
         json.dumps(list(adopted)), payload["created_at"],
         json.dumps(payload, sort_keys=True)))
    core.db.commit()
    return payload


def _load_stored(core, table, comparison_id):
    row = core.db.execute(
        f"SELECT payload FROM {table} WHERE comparison_id=?",
        (comparison_id,)).fetchone()
    return json.loads(row[0]) if row else None


def get_comparison(core, comparison_id):
    return _load_stored(core, "report_comparisons", comparison_id)


def get_comparison_result(core, comparison_id):
    return _load_stored(core, "report_comparison_results", comparison_id)


def _observation_detail(report, observations):
    return [{"sample_id": o["sample_id"], "ts": o["ts"],
             "kind": o["kind"], "value": o["value"],
             "report_id": report["report_id"],
             "correction_version": report["correction_version"]}
            for o in observations]


def compare_intersection(core, comparison_id):
    """Calculate changes only over scopes with evidence on both sides."""
    decision = get_comparison(core, comparison_id)
    if decision is None:
        raise KeyError(f"unknown comparison {comparison_id!r}")
    old = get(core, decision["old_report"]["report_id"])
    new = get(core, decision["new_report"]["report_id"])
    changes = []
    for scope in decision["scopes"]:
        if scope["classification"] == "no_basis":
            continue
        lo, hi = scope["intersection_range"]
        level, window_start = scope["level"], scope["window_start"]
        window_end = scope["window_end"]
        old_obs = _observations_in(old, level, window_start, window_end, lo, hi)
        new_obs = _observations_in(new, level, window_start, window_end, lo, hi)
        bounds = new.get("bucket_bounds", core.buckets)
        old_count, old_sum, old_buckets = _aggregate_observations(old_obs, bounds)
        new_count, new_sum, new_buckets = _aggregate_observations(new_obs, bounds)
        old_ids = {o["sample_id"] for o in old_obs}
        new_ids = {o["sample_id"] for o in new_obs}
        changes.append({
            "level": level, "window_start": window_start,
            "window_end": window_end, "intersection_range": [lo, hi],
            "classification": scope["classification"],
            "before": {"count": old_count, "sum": old_sum,
                       "buckets": old_buckets},
            "after": {"count": new_count, "sum": new_sum,
                      "buckets": new_buckets},
            "delta": {"count": new_count - old_count,
                      "sum": new_sum - old_sum,
                      "buckets": [b - a for a, b in zip(old_buckets, new_buckets)]},
            "added_sample_ids": sorted(new_ids - old_ids),
            "removed_sample_ids": sorted(old_ids - new_ids),
            "source_observations": {
                "before": _observation_detail(old, old_obs),
                "after": _observation_detail(new, new_obs)}})

    payload = {"comparison_id": comparison_id,
               "decision": decision,
               "changed": changes,
               "created_at": time.time()}
    core.db.execute(
        "INSERT OR IGNORE INTO report_comparison_results(comparison_id,created_at,payload) "
        "VALUES(?,?,?)",
        (comparison_id, payload["created_at"], json.dumps(payload, sort_keys=True)))
    core.db.commit()
    return payload
