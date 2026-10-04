import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metrics_core import MetricsCore, Sample


def dist(i, ts, value, source="A", metric="latency"):
    return Sample(f"d{i}", metric, source, ts, "dist", value)


def counter(i, ts, value, source="A", metric="requests"):
    return Sample(f"c{i}", metric, source, ts, "counter", value)


def entry(report, level, start):
    for e in report["entries"]:
        if e["level"] == level and e["start"] == start:
            return e
    raise AssertionError(f"no entry {level}/{start}")


def test_report_multi_granularity_pinned_and_reconciled(tmp_path):
    core = MetricsCore(str(tmp_path / "m.db"))
    for s in [dist(1, 5, 3.0), dist(2, 70, 12.0), dist(3, 130, 250.0),
              dist(4, 310, 8.0)]:
        core.ingest(s)
    core.seal_due(now=1_000)

    report = core.generate_report("latency", 0, 600)
    assert report["levels"] == [60, 300, 3600]        # all granularities at once
    assert report["range_start"] == 0 and report["range_end"] == 600
    assert report["correction_version"] == core.correction_version()
    assert report["complete"] and report["consistent"]
    assert report["discrepancies"] == []

    # fine-grained entries cross-check the aggregate entries
    minute_sum = sum(entry(report, 60, s)["sum"] for s in (0, 60, 120))
    assert entry(report, 300, 0)["sum"] == pytest.approx(minute_sum)
    assert entry(report, 3600, 0)["count"] == 4

    # new entry point agrees with the pre-existing window() entry point
    for level, start in ((60, 0), (60, 120), (300, 0), (3600, 0)):
        w = core.window(level, "latency", start)
        e = entry(report, level, start)
        assert (e["count"], e["sum"], e["buckets"]) == \
            (w["count"], w["sum"], w["buckets"])


def test_late_arrival_keeps_old_report_and_diffs(tmp_path):
    db = str(tmp_path / "m.db")
    core = MetricsCore(db)
    core.ingest(dist(1, 5, 3.0))
    core.ingest(dist(2, 70, 12.0))
    core.ingest(dist(3, 5_000, 50.0))            # outside the future report range
    core.seal_due(now=10_000)

    old = core.generate_report("latency", 0, 300)
    old_id = old["report_id"]

    # late data arrives: two sealed in-range windows plus one out-of-range
    core.apply_late(dist(4, 30, 7.0))
    core.apply_late(dist(5, 90, 9.0))
    core.apply_late(dist(6, 5_030, 60.0))        # late into sealed window 4980

    new = core.generate_report("latency", 0, 300)
    assert new["report_id"] != old_id
    assert new["correction_version"] > old["correction_version"]

    # the old report is preserved verbatim
    assert core.get_report(old_id) == old

    diff = core.diff_reports(old_id, new["report_id"])
    changed = {(c["level"], c["start"]) for c in diff["changed"]}
    assert (60, 0) in changed and (60, 60) in changed      # both late windows
    assert (300, 0) in changed and (3600, 0) in changed    # cascaded rollups
    assert diff["correction_version"]["after"] > \
        diff["correction_version"]["before"]
    # the 5000s window is outside the pinned range: flagged, must not change
    out = diff["revisions_out_of_range"]
    assert any(i["start"] == 4980 for i in out)
    assert all(i["start"] < 300 for i in diff["revisions_in_range"])
    assert entry(core.get_report(old_id), 3600, 0)["sum"] == pytest.approx(15.0)


def test_adopt_new_or_cite_old_and_citation_survives_cleanup(tmp_path):
    db = str(tmp_path / "m.db")
    core = MetricsCore(db)
    core.ingest(dist(1, 5, 3.0))
    core.seal_due(now=1_000)
    old = core.generate_report("latency", 0, 60)

    core.apply_late(dist(2, 10, 700.0))
    new = core.generate_report("latency", 0, 60)
    adopted = core.adopt_report(new["report_id"])
    assert adopted["adopted"] is True
    assert core.get_report(old["report_id"])["adopted"] is False

    # background cleanup removes the underlying windows and samples...
    gone = core.purge_before(120)
    assert gone["windows"] > 0
    assert core.window(60, "latency", 0) is None

    # ...but both citations still resolve with their full explanation
    core.close()
    reopened = MetricsCore(db)
    still_old = reopened.get_report(old["report_id"])
    still_new = reopened.get_report(new["report_id"])
    assert still_old["entries"] == old["entries"]
    assert still_new["adopted"] is True
    assert entry(still_old, 60, 0)["sum"] == pytest.approx(3.0)
    assert entry(still_new, 60, 0)["sum"] == pytest.approx(703.0)


def test_retraction_visible_in_new_report_and_diff(tmp_path):
    core = MetricsCore(str(tmp_path / "m.db"))
    core.ingest(dist(1, 5, 3.0, source="A"))
    core.ingest(dist(2, 10, 700.0, source="B"))
    core.seal_due(now=1_000)
    old = core.generate_report("latency", 0, 60)

    core.retract("B", "latency", 0, 100, reason="B withdrew one observation")
    new = core.generate_report("latency", 0, 60)

    assert entry(new, 60, 0)["count"] == 1
    assert entry(new, 60, 0)["sum"] == pytest.approx(3.0)
    diff = core.diff_reports(old["report_id"], new["report_id"])
    changed = {(c["level"], c["start"]) for c in diff["changed"]}
    assert (60, 0) in changed and (3600, 0) in changed
    assert old["entries"] == core.get_report(old["report_id"])["entries"]


def test_regenerate_after_mid_processing_restart_is_idempotent(tmp_path):
    db = str(tmp_path / "m.db")
    core = MetricsCore(db)
    for s in [dist(1, 5, 3.0), dist(2, 70, 12.0)]:
        core.ingest(s)
    core.seal_due(now=1_000)
    first = core.generate_report("latency", 0, 300)
    core.close()                                   # crash/restart mid-processing

    reopened = MetricsCore(db)
    again = reopened.generate_report("latency", 0, 300)
    assert again["report_id"] == first["report_id"]
    assert again["entries"] == first["entries"]
    assert again["correction_version"] == first["correction_version"]


def test_empty_window_and_incomplete_coverage_vs_zero_values(tmp_path):
    core = MetricsCore(str(tmp_path / "m.db"))
    core.ingest(counter(1, 5, 10))               # primes the counter epoch
    # window [60,120): counter observed twice with no increase -> true zero
    core.ingest(counter(2, 65, 10))
    core.ingest(counter(3, 70, 10))
    # window [120,180): no observations at all -> empty
    # latency window [120,180): still open (not sealed) -> incomplete
    core.ingest(dist(1, 130, 4.0))
    core.seal_due(now=125)                          # seals only [0,60)

    report = core.generate_report("requests", 0, 180)
    zero = entry(report, 60, 60)
    empty = entry(report, 60, 120)
    assert zero["state"] == "sealed"
    assert zero["count"] == 2 and zero["sum"] == 0.0     # real zero, not empty
    assert empty["state"] == "empty"
    assert empty["count"] is None and empty["sum"] is None
    assert report["levels"] == [60, 300, 3600]

    latency = core.generate_report("latency", 0, 180)
    open_entry = entry(latency, 60, 120)
    assert open_entry["state"] == "open"
    assert latency["complete"] is False              # incomplete coverage flagged
    assert entry(latency, 60, 0)["state"] == "empty"

    core.seal_due(now=10_000)
    sealed = core.generate_report("latency", 0, 180)
    assert sealed["complete"] is True


def test_partial_coverage_marked_at_range_edges(tmp_path):
    core = MetricsCore(str(tmp_path / "m.db"))
    core.ingest(dist(1, 5, 3.0))
    core.seal_due(now=1_000)
    report = core.generate_report("latency", 30, 90)   # cuts through windows
    assert entry(report, 60, 0)["coverage"] == "partial"
    assert entry(report, 60, 60)["coverage"] == "partial"
    assert entry(report, 3600, 0)["coverage"] == "partial"


def test_partial_windows_count_only_in_range_observations(tmp_path):
    db = str(tmp_path / "m.db")
    core = MetricsCore(db)
    core.ingest(dist(1, 5, 3.0))
    core.ingest(dist(2, 500, 50.0))          # inside hour window, outside [0,60)
    core.seal_due(now=1_000)

    old = core.generate_report("latency", 0, 60)
    minute = entry(old, 60, 0)
    hour = entry(old, 3600, 0)
    assert minute["coverage"] == "full" and minute["sum"] == pytest.approx(3.0)
    # partial coverage: flagged, and restricted to the pinned range
    assert hour["coverage"] == "partial" and hour["range_restricted"] is True
    assert hour["count"] == 1 and hour["sum"] == pytest.approx(3.0)

    # late data still outside the range must not move any reported figure
    core.apply_late(dist(3, 510, 60.0))
    new = core.generate_report("latency", 0, 60)
    assert entry(new, 3600, 0)["sum"] == pytest.approx(3.0)
    diff = core.diff_reports(old["report_id"], new["report_id"])
    assert diff["changed"] == []
    assert diff["revisions_in_range"] == []
    # the out-of-range correction is reported as such, not misattributed by
    # the hour window's start (0) falling inside the range
    out = {(i["level"], i["start"]) for i in diff["revisions_out_of_range"]}
    assert (60, 480) in out

    # citations keep their justification after cleanup and reopen
    core.purge_before(600)
    core.close()
    reopened = MetricsCore(db)
    still_old = reopened.get_report(old["report_id"])
    assert still_old["entries"] == old["entries"]
    assert entry(still_old, 3600, 0)["sum"] == pytest.approx(3.0)
