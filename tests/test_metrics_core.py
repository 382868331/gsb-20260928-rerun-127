import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metrics_core import MetricsCore, Sample, DEFAULT_BUCKETS

LEVELS = (60, 300, 3600)


def naive_windows(samples, retracted=(), levels=LEVELS, buckets=DEFAULT_BUCKETS):
    """Per-sample reference: apply counter/reset/retraction semantics directly
    and aggregate independently at every level."""
    out = {}
    last = {}
    for s in samples:
        if any(src == s.source and m == s.metric and a <= s.ts <= b
               for src, m, a, b in retracted):
            continue
        if s.kind == "counter":
            prev = last.get((s.metric, s.source))
            value = s.value if prev is None or s.value < prev else s.value - prev
            last[(s.metric, s.source)] = s.value
        else:
            value = s.value
        for level in levels:
            start = int(s.ts // level) * level
            w = out.setdefault((level, s.metric, start),
                               {"count": 0, "sum": 0.0,
                                "buckets": [0] * (len(buckets) + 1)})
            w["count"] += 1
            w["sum"] += value
            if s.kind == "dist":
                idx = next((i for i, b in enumerate(buckets) if value <= b),
                           len(buckets))
                w["buckets"][idx] += 1
    return out


def assert_matches_naive(core, expected, metric):
    for level in LEVELS:
        starts = {st for (lv, m, st) in expected if lv == level and m == metric}
        for start in starts:
            got = core.window(level, metric, start)
            want = expected[(level, metric, start)]
            assert got is not None, f"missing window {level}/{start}"
            assert got["count"] == want["count"], (level, start, "count")
            assert got["sum"] == pytest.approx(want["sum"]), (level, start, "sum")
            assert got["buckets"] == want["buckets"], (level, start, "buckets")


def counter(i, ts, value, source="A", metric="requests"):
    return Sample(f"c{i}", metric, source, ts, "counter", value)


def dist(i, ts, value, source="A", metric="latency"):
    return Sample(f"d{i}", metric, source, ts, "dist", value)


def test_counter_reset_and_multilevel_rollup(tmp_path):
    core = MetricsCore(str(tmp_path / "m.db"))
    samples = [counter(1, 5, 10), counter(2, 20, 15),      # deltas 10, 5
               counter(3, 40, 4),                           # reset -> delta 4
               counter(4, 70, 8),                           # delta 4 (post-reset epoch)
               counter(5, 130, 12)]                         # delta 4
    for s in samples:
        assert core.ingest(s) == "accepted"
    core.seal_due(now=10_000)

    resets = core.events("counter_reset")
    assert len(resets) == 1 and resets[0]["detail"]["prev"] == 15

    assert_matches_naive(core, naive_windows(samples), "requests")
    w1h = core.window(3600, "requests", 0)
    assert w1h["status"] == "sealed" and w1h["count"] == 5 and w1h["sum"] == 27


def test_restart_and_recompute_never_double_count(tmp_path):
    db = str(tmp_path / "m.db")
    samples = [counter(i, 10 * i, 10 * (i + 1)) for i in range(1, 6)]
    core = MetricsCore(db)
    for s in samples:
        core.ingest(s)
    core.seal_due(now=10_000)
    before = core.window(3600, "requests", 0)["sum"]
    core.close()

    reopened = MetricsCore(db)  # simulate restart
    for s in samples:           # replay / recompute
        assert reopened.ingest(s) == "duplicate"
    reopened.seal_due(now=20_000)
    assert reopened.window(3600, "requests", 0)["sum"] == before
    assert reopened.window(60, "requests", 0)["count"] == 5


def test_late_dist_sample_revises_sealed_window(tmp_path):
    core = MetricsCore(str(tmp_path / "m.db"))
    core.ingest(dist(1, 5, 3.0))
    core.ingest(dist(2, 10, 700.0))
    core.seal_due(now=10_000)
    sealed = core.window(60, "latency", 0)
    assert sealed["status"] == "sealed" and sealed["version"] == 0

    core.apply_late(dist(3, 30, 7.0))  # late sample into sealed window
    revised = core.window(60, "latency", 0)
    assert revised["version"] == 1            # new version, not overwritten
    assert revised["count"] == 3 and revised["sum"] == pytest.approx(710.0)
    assert revised["buckets"][1] == 1         # (1,5]: 3.0
    assert revised["buckets"][2] == 1         # (5,10]: 7.0
    assert revised["buckets"][6] == 1         # (500,1000]: 700.0
    assert core.revisions(60, "latency", 0)[0]["version"] == 1
    # revision propagated to parent levels
    assert core.window(300, "latency", 0)["count"] == 3
    assert core.window(3600, "latency", 0)["sum"] == pytest.approx(710.0)


def test_partial_source_retraction_boundaries(tmp_path):
    core = MetricsCore(str(tmp_path / "m.db"))
    samples = ([counter(i, 10 * i, 10 * (i + 1), source="A") for i in range(1, 4)]
               + [counter(10 + i, 10 * i, 100 * (i + 1), source="B") for i in range(1, 4)])
    for s in samples:
        core.ingest(s)
    core.seal_due(now=10_000)  # everything sealed

    # retract source B only -> sealed windows revised, source A untouched
    n = core.retract("B", "requests", 0, 1_000, reason="source B withdrew")
    assert n == 3
    expected = naive_windows(samples, retracted=[("B", "requests", 0, 1_000)])
    assert_matches_naive(core, expected, "requests")
    assert core.window(60, "requests", 0)["version"] >= 1
    assert core.events("retraction")[0]["detail"]["samples"] == 3


def test_resource_limit_rejections_are_queryable(tmp_path):
    core = MetricsCore(str(tmp_path / "m.db"), max_contributions_per_window=3)
    results = [core.ingest(dist(i, 5, float(i))) for i in range(5)]
    assert results == ["accepted"] * 3 + ["rejected"] * 2
    rej = core.rejections()
    assert len(rej) == 2
    assert rej[0]["sample_id"] == "d3" and "limit" in rej[0]["reason"]
    assert core.window(60, "latency", 0)["count"] == 3


def test_manual_sequence_matches_naive_at_all_levels(tmp_path):
    core = MetricsCore(str(tmp_path / "m.db"))
    samples = [
        counter(1, 3, 100, source="A"), counter(2, 61, 150, source="A"),
        counter(3, 62, 40, source="A"),                    # reset
        counter(4, 200, 55, source="A"),
        counter(5, 10, 7, source="B"), counter(6, 400, 9, source="B"),
        dist(1, 3, 0.5, source="A"), dist(2, 70, 12.0, source="A"),
        dist(3, 90, 250.0, source="B"), dist(4, 310, 8.0, source="B"),
        dist(5, 320, 8.0, source="B"),
    ]
    for s in samples:
        core.ingest(s)
    core.seal_due(now=1_000)
    core.apply_late(dist(6, 80, 42.0, source="A"))         # late into sealed 60s window
    core.retract("B", "latency", 300, 400, reason="B partial exit")

    retracted = [("B", "latency", 300, 400)]
    late = dist(6, 80, 42.0, source="A")
    for metric in ("requests", "latency"):
        ms = [s for s in samples if s.metric == metric]
        if metric == "latency":
            ms = ms + [late]
        assert_matches_naive(core, naive_windows(ms, retracted=retracted), metric)

    # late revision is traceable; bucket counts consistent after correction
    assert core.revisions(60, "latency", 60)
    w = core.window(3600, "latency", 0)
    assert w["count"] == sum(w["buckets"])
