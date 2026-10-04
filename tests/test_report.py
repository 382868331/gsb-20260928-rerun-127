import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metrics_core import MetricsCore, Sample, report

LEVELS = (60, 300, 3600)


def dist(i, ts, value, source="A", metric="latency"):
    return Sample(f"d{i}", metric, source, ts, "dist", value)


def counter(i, ts, value, source="A", metric="requests"):
    return Sample(f"c{i}", metric, source, ts, "counter", value)


def win(rep, level, start):
    for w in rep["windows"]:
        if w["level"] == level and w["start"] == start:
            return w
    raise KeyError((level, start))


def test_report_matches_original_entry_points(tmp_path):
    """New report entry and original window() entry agree on every level."""
    core = MetricsCore(str(tmp_path / "m.db"))
    for s in [dist(1, 5, 3.0), dist(2, 70, 12.0), dist(3, 130, 250.0),
              counter(1, 10, 100), counter(2, 80, 150)]:
        core.ingest(s)
    core.seal_due(now=10_000)

    rep = report.build_report(core, "latency", 0, 3600)
    assert rep["levels"] == list(LEVELS)          # all granularities at once
    for w in rep["windows"]:
        orig = core.window(w["level"], "latency", w["start"])
        if w["state"] == "missing":
            assert orig is None
        else:
            assert orig["count"] == w["count"] and orig["sum"] == w["sum"]
            assert orig["buckets"] == w["buckets"]
    # fine-grained and aggregate results cross-check
    assert all(c["ok"] for c in rep["reconciliation"])
    assert rep["totals"][3600]["count"] == 3
    assert rep["totals"][3600]["sum"] == pytest.approx(265.0)


def test_empty_and_incomplete_distinguished_from_zero(tmp_path):
    core = MetricsCore(str(tmp_path / "m.db"))
    core.ingest(dist(1, 5, 0.0))                    # genuine zero value
    core.seal_due(now=500)                          # seals 60s/300s, hour open
    rep = report.build_report(core, "latency", 0, 3600)

    w0 = win(rep, 60, 0)
    assert w0["state"] == "sealed" and w0["count"] == 1 and w0["sum"] == 0.0
    # a window with no observations at all is 'missing', values None, not 0
    w1 = win(rep, 60, 60)
    assert w1["state"] == "missing" and w1["count"] is None
    # open hour window = incomplete coverage, distinct from sealed/empty
    hour = win(rep, 3600, 0)
    assert hour["state"] == "open"
    assert rep["totals"][3600]["complete"] is False
    assert rep["totals"][60]["missing"] == 59

    # an existing-but-empty window reports 'empty', still not a zero value
    core.seal_due(now=10_000)
    core2 = MetricsCore(str(tmp_path / "m2.db"))
    core2.ingest(dist(1, 5, 7.0))
    core2.retract("A", "latency", 5, 5)             # withdraw the only obs
    core2.seal_due(now=10_000)
    rep2 = report.build_report(core2, "latency", 0, 60)
    assert win(rep2, 60, 0)["state"] == "empty"
    assert win(rep2, 60, 0)["count"] == 0


def test_late_data_diff_and_adoption(tmp_path):
    """Old report kept; diff shows which aggregates changed; user may adopt
    the new report or keep citing the old one."""
    db = str(tmp_path / "m.db")
    core = MetricsCore(db)
    core.ingest(dist(1, 5, 3.0))
    core.ingest(dist(2, 70, 12.0))
    core.seal_due(now=10_000)

    old = report.build_report(core, "latency", 0, 3600)
    old_id = report.save_report(core, old)

    # late data spanning two level-0 windows
    core.apply_late(dist(3, 30, 7.0))
    core.apply_late(dist(4, 100, 42.0))
    new = report.build_report(core, "latency", 0, 3600)
    new_id = report.save_report(core, new)

    diff = report.diff_reports(old, new)
    changed = {(c["level"], c["start"]) for c in diff["changed"]}
    assert (60, 0) in changed and (60, 60) in changed      # both late windows
    assert (300, 0) in changed and (3600, 0) in changed    # propagated rollups
    assert diff["epoch"]["from"] != diff["epoch"]["to"]
    # old report unchanged and still citable
    kept = report.get_report(core, old_id)
    assert win(kept, 3600, 0)["sum"] == pytest.approx(15.0)
    assert not kept["adopted"]

    report.adopt_report(core, new_id)
    assert report.get_report(core, new_id)["adopted"]
    assert not report.get_report(core, old_id)["adopted"]  # old stays citable
    ids = [r["report_id"] for r in report.list_reports(core, "latency")]
    assert ids == [old_id, new_id]


def test_out_of_range_aggregates_must_not_change(tmp_path):
    core = MetricsCore(str(tmp_path / "m.db"))
    core.ingest(dist(1, 5, 3.0))
    core.ingest(dist(2, 4000, 9.0))                  # outside the report range
    core.seal_due(now=10_000)
    old = report.build_report(core, "latency", 0, 3600)
    core.apply_late(dist(3, 30, 7.0))                # late, inside range
    new = report.build_report(core, "latency", 0, 3600)

    diff = report.diff_reports(old, new)
    assert diff["changed"]                            # in-range changes exist
    for entry in diff["out_of_scope"]:
        assert entry["changed"] is False             # uncovered: no change
        assert entry["expected"].startswith("unchanged")


def test_retract_single_observation_updates_report(tmp_path):
    core = MetricsCore(str(tmp_path / "m.db"))
    for s in [dist(1, 5, 3.0), dist(2, 10, 7.0), dist(3, 20, 9.0)]:
        core.ingest(s)
    core.seal_due(now=10_000)
    before = report.build_report(core, "latency", 0, 3600)

    core.retract("A", "latency", 10, 10, reason="withdraw one observation")
    after = report.build_report(core, "latency", 0, 3600)

    diff = report.diff_reports(before, after)
    w60 = [c for c in diff["changed"] if c["level"] == 60][0]
    assert w60["changes"]["count"] == [3, 2]
    assert w60["changes"]["sum"] == [19.0, 12.0]
    assert win(after, 60, 0)["version"] == 1         # sealed window revised
    assert all(c["ok"] for c in after["reconciliation"])


def test_reopen_mid_processing_and_cleanup_keeps_citations(tmp_path):
    db = str(tmp_path / "m.db")
    core = MetricsCore(db)
    core.ingest(dist(1, 5, 3.0))
    core.seal_due(now=10_000)
    first_id = report.save_report(core, report.build_report(core, "latency", 0, 3600))
    core.close()

    reopened = MetricsCore(db)                       # reopen mid-processing
    reopened.apply_late(dist(2, 30, 7.0))
    second_id = report.save_report(
        reopened, report.build_report(reopened, "latency", 0, 3600))
    diff = report.diff_reports(report.get_report(reopened, first_id),
                               report.get_report(reopened, second_id))
    assert any(c["level"] == 60 for c in diff["changed"])

    # background cleanup purges sealed windows; citations keep their basis
    assert reopened.purge_windows(before=10_000) > 0
    assert reopened.window(60, "latency", 0) is None
    cited = report.get_report(reopened, first_id)
    assert cited["epoch"]["revisions"] == 0
    assert win(cited, 60, 0)["sum"] == pytest.approx(3.0)
    assert report.get_report(reopened, second_id)["windows"]
