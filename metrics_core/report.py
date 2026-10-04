"""Snapshot reports spanning multiple time granularities.

A report pins the observation range and the correction epoch it was read
at, so fine-grained windows and their rollups can be cross-checked and a
read never mixes in corrections that arrive later. Late data produces a
new report plus a diff against the old one; both stay stored, so users
may formally adopt the new report or keep citing the old one -- citations
survive background cleanup of the underlying windows.
"""

from __future__ import annotations

import json
import time
from typing import Optional

REPORT_SCHEMA = """
CREATE TABLE IF NOT EXISTS reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric TEXT, range_start REAL, range_end REAL,
    epoch_revisions INTEGER, epoch_contributions INTEGER,
    adopted INTEGER DEFAULT 0,
    created REAL,
    body TEXT
);
"""


def _ensure_schema(core):
    core.db.executescript(REPORT_SCHEMA)
    core.db.commit()


def _epoch(core) -> dict:
    """Correction epoch: bumped by any revision (sealed-window correction)
    and by any new contribution (ingest / rollup). Two reports with equal
    epochs saw exactly the same corrections."""
    revisions = core.db.execute(
        "SELECT COALESCE(MAX(id), 0) FROM revisions").fetchone()[0]
    contributions = core.db.execute(
        "SELECT COUNT(*) FROM contributions").fetchone()[0]
    return {"revisions": revisions, "contributions": contributions}


def build_report(core, metric: str, start: float, end: float,
                 levels: Optional[tuple] = None) -> dict:
    """Consistent multi-granularity snapshot of `metric` over [start, end).

    Window states distinguish: 'sealed' (final), 'open' (still accepting,
    i.e. incomplete coverage), 'empty' (window exists, zero observations --
    not the same as missing), and 'missing' (no data at all -- values are
    None, never silently zero).
    """
    levels = tuple(sorted(levels)) if levels else core.levels
    windows = []
    for level in levels:
        s = int(start // level) * level
        while s < end:
            row = core.db.execute(
                "SELECT status, version, count, sum, buckets FROM windows "
                "WHERE window_key=?", (f"{level}:{metric}:{s}",)).fetchone()
            covered = s >= start and s + level <= end
            if row is None:
                windows.append({"level": level, "start": s, "end": s + level,
                                "metric": metric, "state": "missing",
                                "version": None, "count": None, "sum": None,
                                "buckets": None, "covered": covered})
            else:
                status, version, count, total, buckets = row
                if count == 0:
                    state = "empty" if status == "sealed" else "open"
                else:
                    state = status
                windows.append({"level": level, "start": s, "end": s + level,
                                "metric": metric, "state": state,
                                "version": version, "count": count,
                                "sum": total, "buckets": json.loads(buckets),
                                "covered": covered})
            s += level

    totals = {}
    for level in levels:
        present = [w for w in windows
                   if w["level"] == level and w["state"] != "missing"]
        complete = all(w["state"] in ("sealed", "empty")
                       for w in windows if w["level"] == level)
        buckets = [0] * (len(core.buckets) + 1)
        for w in present:
            for i, b in enumerate(w["buckets"]):
                buckets[i] += b
        totals[level] = {
            "count": sum(w["count"] for w in present),
            "sum": sum(w["sum"] for w in present),
            "buckets": buckets,
            "windows": len([w for w in windows if w["level"] == level]),
            "missing": len([w for w in windows
                            if w["level"] == level and w["state"] == "missing"]),
            "complete": complete,
        }

    return {"metric": metric, "range": [start, end], "levels": list(levels),
            "epoch": _epoch(core), "windows": windows, "totals": totals,
            "reconciliation": _reconcile(core, windows, levels)}


def _reconcile(core, windows, levels) -> list:
    """Cross-check every parent window against its child windows present in
    the report. Fine-grained and aggregate results must agree."""
    by_key = {(w["level"], w["start"]): w for w in windows}
    checks = []
    for parent in windows:
        idx = levels.index(parent["level"])
        if idx == 0 or parent["state"] == "missing":
            continue
        child_level = levels[idx - 1]
        children = []
        for s in range(parent["start"], parent["end"], child_level):
            child = by_key.get((child_level, s))
            if child is None or child["state"] == "missing":
                # fall back to the same-epoch database state; a child with
                # no window at all contributes zero
                row = core.db.execute(
                    "SELECT count, sum, buckets FROM windows WHERE window_key=?",
                    (f"{child_level}:{parent['metric']}:{s}",)).fetchone()
                child = ({"count": row[0], "sum": row[1],
                          "buckets": json.loads(row[2])} if row else
                         {"count": 0, "sum": 0.0,
                          "buckets": [0] * (len(core.buckets) + 1)})
            children.append(child)
        count = sum(c["count"] for c in children)
        total = sum(c["sum"] for c in children)
        buckets = [sum(c["buckets"][i] for c in children)
                   for i in range(len(core.buckets) + 1)]
        ok = (count == parent["count"]
              and abs(total - parent["sum"]) < 1e-9
              and buckets == parent["buckets"])
        checks.append({"level": parent["level"], "start": parent["start"],
                       "ok": ok,
                       "detail": "children match parent" if ok else
                       {"count": [count, parent["count"]],
                        "sum": [total, parent["sum"]],
                        "buckets": [buckets, parent["buckets"]]}})
    return checks


def diff_reports(old: dict, new: dict) -> dict:
    """Diff two reports. Aggregates inside the pinned range may legitimately
    change (late data / retractions); aggregates outside the covered range
    are listed separately and are expected NOT to change -- any change there
    is flagged."""
    def key(w):
        return (w["level"], w["start"])

    old_w = {key(w): w for w in old["windows"]}
    new_w = {key(w): w for w in new["windows"]}
    changed, appeared, out_of_scope = [], [], []
    for k in sorted(set(old_w) | set(new_w)):
        o, n = old_w.get(k), new_w.get(k)
        if o is None or n is None:
            appeared.append({"level": k[0], "start": k[1],
                             "before": _brief(o), "after": _brief(n)})
            continue
        delta = {}
        for f in ("state", "version", "count", "sum", "buckets"):
            if o[f] != n[f]:
                delta[f] = [o[f], n[f]]
        entry = {"level": k[0], "start": k[1], "changes": delta}
        if o["covered"] and n["covered"]:
            if delta:
                changed.append(entry)
        else:
            entry["changed"] = bool(delta)
            entry["expected"] = "unchanged (outside covered range)"
            out_of_scope.append(entry)
    return {"metric": new["metric"], "range": new["range"],
            "epoch": {"from": old["epoch"], "to": new["epoch"]},
            "changed": changed, "appeared": appeared,
            "out_of_scope": out_of_scope}


def _brief(w):
    if w is None:
        return None
    return {"state": w["state"], "version": w["version"],
            "count": w["count"], "sum": w["sum"]}


# ------------------------------------------------------------------ persistence
def save_report(core, report: dict) -> int:
    """Persist a report so it stays citable even after window cleanup."""
    _ensure_schema(core)
    cur = core.db.execute(
        "INSERT INTO reports(metric,range_start,range_end,epoch_revisions,"
        "epoch_contributions,adopted,created,body) VALUES(?,?,?,?,?,0,?,?)",
        (report["metric"], report["range"][0], report["range"][1],
         report["epoch"]["revisions"], report["epoch"]["contributions"],
         time.time(), json.dumps(report)))
    core.db.commit()
    return cur.lastrowid


def adopt_report(core, report_id: int) -> None:
    """Formally adopt a report as the current citable version."""
    _ensure_schema(core)
    core.db.execute("UPDATE reports SET adopted=1 WHERE report_id=?",
                    (report_id,))
    core.db.commit()


def get_report(core, report_id: int) -> Optional[dict]:
    """Fetch a stored report, including its pinned epoch and adoption flag.
    Independent of the windows tables, so background purges cannot remove
    the basis for interpreting a citation."""
    _ensure_schema(core)
    row = core.db.execute(
        "SELECT body, adopted FROM reports WHERE report_id=?",
        (report_id,)).fetchone()
    if row is None:
        return None
    report = json.loads(row[0])
    report["report_id"] = report_id
    report["adopted"] = bool(row[1])
    return report


def list_reports(core, metric: Optional[str] = None) -> list:
    _ensure_schema(core)
    q = ("SELECT report_id, metric, range_start, range_end, epoch_revisions,"
         " epoch_contributions, adopted, created FROM reports")
    args = ()
    if metric is not None:
        q += " WHERE metric=?"
        args = (metric,)
    return [{"report_id": rid, "metric": m, "range": [a, b],
             "epoch": {"revisions": er, "contributions": ec},
             "adopted": bool(ad), "created": ts}
            for rid, m, a, b, er, ec, ad, ts in
            core.db.execute(q + " ORDER BY report_id", args)]
