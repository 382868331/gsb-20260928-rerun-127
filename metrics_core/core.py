"""Metrics collection & aggregation core.

Features:
- Multi-level aligned time windows; sealed short windows roll up into longer ones.
- Idempotent ingestion: the same contribution is never counted twice, even
  across restarts or recomputation (durable dedup + versioned rollups).
- Counter reset detection and explicit source retraction boundaries.
- Sealed windows are never overwritten: revisions produce new, traceable
  versions while old versions remain queryable.
- Resource-limit rejections are recorded and queryable.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Optional

DEFAULT_LEVELS = (60, 300, 3600)          # seconds, ascending
DEFAULT_BUCKETS = (1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0)  # upper bounds; +inf implied

SCHEMA = """
CREATE TABLE IF NOT EXISTS samples (
    sample_id TEXT PRIMARY KEY,
    metric TEXT, source TEXT, ts REAL, kind TEXT, value REAL
);
CREATE TABLE IF NOT EXISTS counters (
    metric TEXT, source TEXT, last_value REAL,
    PRIMARY KEY (metric, source)
);
CREATE TABLE IF NOT EXISTS contributions (
    contrib_id TEXT PRIMARY KEY,
    window_key TEXT, level INTEGER, metric TEXT,
    start REAL, kind TEXT, value REAL, source TEXT,
    sample_id TEXT, child_key TEXT, child_version INTEGER,
    retracted INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS windows (
    window_key TEXT PRIMARY KEY,
    level INTEGER, metric TEXT, start REAL,
    status TEXT,            -- 'open' | 'sealed'
    version INTEGER,
    count INTEGER, sum REAL, buckets TEXT
);
CREATE TABLE IF NOT EXISTS revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    window_key TEXT, version INTEGER, reason TEXT, ts REAL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT, detail TEXT, ts REAL
);
CREATE TABLE IF NOT EXISTS rejections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id TEXT, metric TEXT, source TEXT, ts REAL, reason TEXT
);
"""


@dataclass(frozen=True)
class Sample:
    sample_id: str
    metric: str
    source: str
    ts: float
    kind: str          # 'counter' (cumulative) | 'dist' (distribution observation)
    value: float


def _window_key(level: int, metric: str, start: int) -> str:
    return f"{level}:{metric}:{start}"


class MetricsCore:
    def __init__(self, db_path: str = ":memory:",
                 levels=DEFAULT_LEVELS, buckets=DEFAULT_BUCKETS,
                 max_contributions_per_window: Optional[int] = None,
                 late_grace: float = 0.0):
        self.levels = tuple(sorted(levels))
        self.buckets = tuple(sorted(buckets))
        self.max_contributions = max_contributions_per_window
        self.late_grace = late_grace
        self.db = sqlite3.connect(db_path)
        self.db.executescript(SCHEMA)
        self.db.commit()

    # ------------------------------------------------------------------ ingest
    def ingest(self, s: Sample) -> str:
        """Returns 'accepted' | 'duplicate' | 'rejected'."""
        cur = self.db.cursor()
        if cur.execute("SELECT 1 FROM samples WHERE sample_id=?", (s.sample_id,)).fetchone():
            return "duplicate"
        if s.kind == "counter":
            row = cur.execute(
                "SELECT last_value FROM counters WHERE metric=? AND source=?",
                (s.metric, s.source)).fetchone()
            if row is not None and s.value < row[0]:
                self._event("counter_reset", {
                    "metric": s.metric, "source": s.source,
                    "prev": row[0], "value": s.value, "sample_id": s.sample_id})
                delta = s.value  # new epoch: count from zero
            else:
                delta = s.value - (row[0] if row else 0.0)
            cur.execute(
                "INSERT INTO counters(metric,source,last_value) VALUES(?,?,?) "
                "ON CONFLICT(metric,source) DO UPDATE SET last_value=excluded.last_value",
                (s.metric, s.source, s.value))
            value = delta
        elif s.kind == "dist":
            value = s.value
        else:
            raise ValueError(f"unknown kind {s.kind!r}")

        level0 = self.levels[0]
        start = int(s.ts // level0) * level0
        key = _window_key(level0, s.metric, start)
        if not self._admit(cur, key, s):
            return "rejected"
        cur.execute(
            "INSERT INTO samples(sample_id,metric,source,ts,kind,value) VALUES(?,?,?,?,?,?)",
            (s.sample_id, s.metric, s.source, s.ts, s.kind, s.value))
        cur.execute(
            "INSERT INTO contributions(contrib_id,window_key,level,metric,start,kind,"
            "value,source,sample_id) VALUES(?,?,?,?,?,?,?,?,?)",
            (f"sample:{s.sample_id}", key, level0, s.metric, start, s.kind,
             value, s.source, s.sample_id))
        self._touch(cur, key, level0, s.metric, start)
        self._recompute(cur, key, "ingest")
        self.db.commit()
        return "accepted"

    def _admit(self, cur, window_key: str, s: Sample) -> bool:
        if self.max_contributions is None:
            return True
        n = cur.execute(
            "SELECT COUNT(*) FROM contributions WHERE window_key=?",
            (window_key,)).fetchone()[0]
        if n >= self.max_contributions:
            cur.execute(
                "INSERT INTO rejections(sample_id,metric,source,ts,reason) VALUES(?,?,?,?,?)",
                (s.sample_id, s.metric, s.source, s.ts,
                 f"window contribution limit {self.max_contributions} exceeded"))
            self.db.commit()
            return False
        return True

    # ------------------------------------------------------------------ windows
    def _touch(self, cur, key, level, metric, start):
        cur.execute(
            "INSERT INTO windows(window_key,level,metric,start,status,version,count,sum,buckets) "
            "VALUES(?,?,?,?, 'open', 0, 0, 0.0, ?) "
            "ON CONFLICT(window_key) DO NOTHING",
            (key, level, metric, start, json.dumps([0] * (len(self.buckets) + 1))))

    def _aggregate(self, cur, key):
        """Sum non-retracted contributions; for rollups keep only the latest
        version of each child window (idempotent recompute)."""
        rows = cur.execute(
            "SELECT kind, value, child_key, MAX(child_version) FROM contributions "
            "WHERE window_key=? AND retracted=0 "
            "GROUP BY COALESCE(child_key, contrib_id)",
            (key,)).fetchall()
        count = 0
        total = 0.0
        buckets = [0] * (len(self.buckets) + 1)
        # bucket counts come from child windows at rollup time; for level-0
        # distribution samples we bucket each value directly.
        for kind, value, child_key, _ in rows:
            total += value
            if child_key is None:
                count += 1
                if kind == "dist":
                    buckets[self._bucket_index(value)] += 1
            else:
                child = cur.execute(
                    "SELECT count, buckets FROM windows WHERE window_key=?",
                    (child_key,)).fetchone()
                if child:
                    count += child[0]
                    for i, b in enumerate(json.loads(child[1])):
                        buckets[i] += b
        return count, total, buckets

    def _bucket_index(self, value: float) -> int:
        for i, bound in enumerate(self.buckets):
            if value <= bound:
                return i
        return len(self.buckets)

    def _recompute(self, cur, key, reason: str):
        count, total, buckets = self._aggregate(cur, key)
        row = cur.execute(
            "SELECT status, version FROM windows WHERE window_key=?", (key,)).fetchone()
        status, version = row
        if status == "sealed":
            version += 1  # never overwrite: a sealed window gets a new version
            cur.execute(
                "INSERT INTO revisions(window_key,version,reason,ts) VALUES(?,?,?,?)",
                (key, version, reason, time.time()))
            self._event("window_revision", {"window": key, "version": version, "reason": reason})
        cur.execute(
            "UPDATE windows SET count=?, sum=?, buckets=?, version=? WHERE window_key=?",
            (count, total, json.dumps(buckets), version, key))
        return status, version

    def seal_due(self, now: float):
        """Seal windows whose end (+grace) <= now. Cascades level by level so a
        freshly sealed child rolls up within the same call."""
        cur = self.db.cursor()
        for level in self.levels:
            rows = cur.execute(
                "SELECT window_key, metric, start FROM windows "
                "WHERE status='open' AND level=?", (level,)).fetchall()
            for key, metric, start in rows:
                if start + level + self.late_grace > now:
                    continue
                self._recompute(cur, key, "seal")
                cur.execute("UPDATE windows SET status='sealed' WHERE window_key=?",
                            (key,))
                self._rollup(cur, key, level, metric, start)
        self.db.commit()

    def _rollup(self, cur, child_key, level, metric, start):
        idx = self.levels.index(level)
        if idx + 1 >= len(self.levels):
            return
        parent_level = self.levels[idx + 1]
        parent_start = int(start // parent_level) * parent_level
        parent_key = _window_key(parent_level, metric, parent_start)
        child = cur.execute(
            "SELECT version, count, sum FROM windows WHERE window_key=?",
            (child_key,)).fetchone()
        version, count, total = child
        contrib_id = f"rollup:{child_key}:v{version}"
        # INSERT OR IGNORE: re-sealing / restart replays never double-count.
        cur.execute(
            "INSERT OR IGNORE INTO contributions(contrib_id,window_key,level,metric,"
            "start,kind,value,child_key,child_version) VALUES(?,?,?,?,?,?,?,?,?)",
            (contrib_id, parent_key, parent_level, metric, parent_start,
             "rollup", total, child_key, version))
        self._touch(cur, parent_key, parent_level, metric, parent_start)
        # A revision of an already-sealed parent must propagate as a new version.
        self._recompute(cur, parent_key, f"rollup of {child_key} v{version}")
        parent_status = cur.execute(
            "SELECT status FROM windows WHERE window_key=?",
            (parent_key,)).fetchone()[0]
        if parent_status == "sealed":
            # cascade the correction to higher levels
            self._rollup(cur, parent_key, parent_level, metric, parent_start)

    # ------------------------------------------------------------------ corrections
    def retract(self, source: str, metric: str, ts_start: float, ts_end: float,
                reason: str = "source retraction") -> int:
        """Withdraw a source's samples in [ts_start, ts_end]. Open windows are
        corrected in place; sealed windows get a new traceable version."""
        cur = self.db.cursor()
        rows = cur.execute(
            "SELECT c.contrib_id, c.window_key FROM contributions c "
            "JOIN samples s ON s.sample_id = c.sample_id "
            "WHERE c.source=? AND c.metric=? AND s.ts>=? AND s.ts<=? AND c.retracted=0",
            (source, metric, ts_start, ts_end)).fetchall()
        affected = sorted({k for _, k in rows})
        for cid, _ in rows:
            cur.execute("UPDATE contributions SET retracted=1 WHERE contrib_id=?", (cid,))
        self._event("retraction", {
            "source": source, "metric": metric,
            "range": [ts_start, ts_end], "samples": len(rows), "reason": reason})
        for key in affected:
            self._recompute(cur, key, reason)
            # propagate correction upward if the window had already rolled up
            level, metric_, start = cur.execute(
                "SELECT level, metric, start FROM windows WHERE window_key=?",
                (key,)).fetchone()
            status = cur.execute(
                "SELECT status FROM windows WHERE window_key=?", (key,)).fetchone()[0]
            if status == "sealed":
                self._rollup(cur, key, level, metric_, start)
        self.db.commit()
        return len(rows)

    def apply_late(self, s: Sample) -> str:
        """Ingest a sample whose window may already be sealed; sealed windows
        are revised (new version, propagated upward), never overwritten."""
        sealed = self._window_status(s) == "sealed"
        result = self.ingest(s)
        if result == "accepted" and sealed:
            level0 = self.levels[0]
            start = int(s.ts // level0) * level0
            cur = self.db.cursor()
            self._rollup(cur, _window_key(level0, s.metric, start), level0,
                         s.metric, start)
            self.db.commit()
        return result

    def _window_status(self, s: Sample):
        level0 = self.levels[0]
        start = int(s.ts // level0) * level0
        row = self.db.execute(
            "SELECT status FROM windows WHERE window_key=?",
            (_window_key(level0, s.metric, start),)).fetchone()
        return row[0] if row else None

    # ------------------------------------------------------------------ queries
    def window(self, level: int, metric: str, start: int) -> Optional[dict]:
        row = self.db.execute(
            "SELECT status, version, count, sum, buckets FROM windows WHERE window_key=?",
            (_window_key(level, metric, start),)).fetchone()
        if not row:
            return None
        status, version, count, total, buckets = row
        return {"level": level, "metric": metric, "start": start, "status": status,
                "version": version, "count": count, "sum": total,
                "buckets": json.loads(buckets)}

    def revisions(self, level: int, metric: str, start: int) -> list[dict]:
        key = _window_key(level, metric, start)
        return [{"version": v, "reason": r}
                for _, v, r in self.db.execute(
                    "SELECT id, version, reason FROM revisions WHERE window_key=? "
                    "ORDER BY id", (key,))]

    def rejections(self) -> list[dict]:
        return [{"sample_id": sid, "metric": m, "source": src, "ts": ts, "reason": why}
                for sid, m, src, ts, why in self.db.execute(
                    "SELECT sample_id, metric, source, ts, reason FROM rejections ORDER BY id")]

    def events(self, type_: Optional[str] = None) -> list[dict]:
        q = "SELECT type, detail FROM events" + (" WHERE type=?" if type_ else "") + " ORDER BY id"
        args = (type_,) if type_ else ()
        return [{"type": t, "detail": json.loads(d)} for t, d in self.db.execute(q, args)]

    def _event(self, type_: str, detail: dict):
        self.db.execute("INSERT INTO events(type,detail,ts) VALUES(?,?,?)",
                        (type_, json.dumps(detail), time.time()))

    def close(self):
        self.db.close()
