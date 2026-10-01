package metriccore

import (
	"bytes"
	"errors"
	"math"
	"testing"
	"time"
)

func TestManualCounterResetDistributionAndDeduplication(t *testing.T) {
	base := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	r := newTestRegistry(base)

	samples := []Sample{
		{ID: "c1", Metric: "requests", Source: "srv-a", Timestamp: base.Add(5 * time.Second), Value: 100},
		{ID: "c2", Metric: "requests", Source: "srv-a", Timestamp: base.Add(15 * time.Second), Value: 110},
		{ID: "c2", Metric: "requests", Source: "srv-a", Timestamp: base.Add(16 * time.Second), Value: 111},
		{ID: "c3", Metric: "requests", Source: "srv-a", Timestamp: base.Add(25 * time.Second), Value: 5},
		{ID: "c4", Metric: "requests", Source: "srv-a", Timestamp: base.Add(55 * time.Second), Value: 7},
		{ID: "d1", Metric: "latency", Source: "srv-a", Timestamp: base.Add(10 * time.Second), Value: 4},
		{ID: "d2", Metric: "latency", Source: "srv-a", Timestamp: base.Add(20 * time.Second), Value: 11},
		{ID: "d3", Metric: "latency", Source: "srv-a", Timestamp: base.Add(40 * time.Second), Value: 16},
	}
	for _, sample := range samples {
		err := r.Ingest(sample)
		if sample.ID == "c2" && sample.Value == 111 && !errors.Is(err, ErrDuplicateSample) {
			t.Fatalf("expected duplicate sample, got %v", err)
		}
		if sample.ID != "c2" && err != nil {
			t.Fatalf("ingest %s: %v", sample.ID, err)
		}
	}

	w, ok := r.SnapshotWindow(base)
	if !ok {
		t.Fatal("window missing")
	}
	key := "srv-a\x00requests"
	gotCounter := w.Counters[key]
	if gotCounter.Samples != 4 || gotCounter.Total != 17 || gotCounter.Resets != 1 {
		t.Fatalf("counter = %+v, want samples=4 total=17 resets=1", gotCounter)
	}
	dist := w.Distributions["srv-a\x00latency"]
	if dist.Count != 3 || dist.Sum != 31 || dist.Min != 4 || dist.Max != 16 {
		t.Fatalf("distribution = %+v", dist)
	}
	wantBuckets := []uint64{1, 0, 1, 1}
	if !uint64sEqual(dist.Buckets, wantBuckets) {
		t.Fatalf("buckets = %v, want %v", dist.Buckets, wantBuckets)
	}
	if stats := r.Stats(); stats.DuplicateSamples != 1 {
		t.Fatalf("duplicate stats = %d, want 1", stats.DuplicateSamples)
	}
}

func TestLateSampleRejectedAfterWindowCloses(t *testing.T) {
	base := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	r := newTestRegistry(base)
	clock := base.Add(time.Minute)
	r.SetClock(func() time.Time { return clock })

	if err := r.Ingest(Sample{ID: "a", Metric: "requests", Source: "srv-a", Timestamp: base.Add(30 * time.Second), Value: 10}); err != nil {
		t.Fatal(err)
	}
	if closed := r.CloseWindows(clock); closed != 1 {
		t.Fatalf("closed = %d, want 1", closed)
	}
	err := r.Ingest(Sample{ID: "late", Metric: "requests", Source: "srv-a", Timestamp: base.Add(45 * time.Second), Value: 11})
	if !errors.Is(err, ErrLateSample) {
		t.Fatalf("late error = %v, want %v", err, ErrLateSample)
	}
	clock = base.Add(2 * time.Minute)
	if err := r.Ingest(Sample{ID: "next", Metric: "requests", Source: "srv-a", Timestamp: base.Add(70 * time.Second), Value: 12}); err != nil {
		t.Fatal(err)
	}
	if stats := r.Stats(); stats.LateSamples != 1 {
		t.Fatalf("late stats = %d, want 1", stats.LateSamples)
	}
}

func TestSourceAndMemoryLimitsVisible(t *testing.T) {
	base := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	r := NewRegistry(Config{WindowSize: time.Minute, MaxSources: 1, MaxMetrics: 1, MaxMemoryBytes: 700})
	r.SetClock(func() time.Time { return base })
	if err := r.AddMetric(Definition{Name: "requests", Type: Counter}); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterSource(Source{ID: "srv-a"}); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterSource(Source{ID: "srv-b"}); !errors.Is(err, ErrSourceLimit) {
		t.Fatalf("source limit error = %v, want %v", err, ErrSourceLimit)
	}
	err := r.Ingest(Sample{ID: "a", Metric: "requests", Source: "srv-a", Timestamp: base, Value: 1})
	if !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("memory limit error = %v, want %v", err, ErrMemoryLimit)
	}
	stats := r.Stats()
	if stats.LimitErrors < 2 || stats.EstimatedMemoryBytes == 0 || stats.LastLimitError == "" {
		t.Fatalf("limit stats not visible: %+v", stats)
	}
	rejections := r.Rejections()
	if len(rejections) < 2 || rejections[0].ReasonCode != "source_limit" || rejections[1].ReasonCode != "memory_limit" {
		t.Fatalf("resource rejections not queryable: %+v", rejections)
	}
}

func TestCheckpointResumeMatchesContinuousRun(t *testing.T) {
	base := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	samples := []Sample{
		{ID: "a1", Metric: "requests", Source: "srv-a", Timestamp: base.Add(10 * time.Second), Value: 100},
		{ID: "a2", Metric: "requests", Source: "srv-a", Timestamp: base.Add(20 * time.Second), Value: 110},
		{ID: "b1", Metric: "latency", Source: "srv-a", Timestamp: base.Add(30 * time.Second), Value: 12},
	}
	more := []Sample{
		{ID: "a3", Metric: "requests", Source: "srv-a", Timestamp: base.Add(time.Minute + 20*time.Second), Value: 3},
		{ID: "b2", Metric: "latency", Source: "srv-a", Timestamp: base.Add(time.Minute + 30*time.Second), Value: 17},
		{ID: "a2", Metric: "requests", Source: "srv-a", Timestamp: base.Add(21 * time.Second), Value: 999},
	}

	continuous := newTestRegistry(base)
	for _, sample := range samples {
		mustIngest(t, continuous, sample)
	}
	continuous.SetClock(func() time.Time { return base.Add(time.Minute) })
	continuous.CloseWindows(base.Add(time.Minute))
	continuous.SetClock(func() time.Time { return base.Add(2 * time.Minute) })
	for _, sample := range more {
		_ = continuous.Ingest(sample)
	}

	resumable := newTestRegistry(base)
	for _, sample := range samples {
		mustIngest(t, resumable, sample)
	}
	resumable.SetClock(func() time.Time { return base.Add(time.Minute) })
	resumable.CloseWindows(base.Add(time.Minute))
	var buffer bytes.Buffer
	if err := resumable.Save(&buffer); err != nil {
		t.Fatal(err)
	}
	resumed, err := Load(&buffer)
	if err != nil {
		t.Fatal(err)
	}
	resumed.SetClock(func() time.Time { return base.Add(2 * time.Minute) })
	for _, sample := range more {
		_ = resumed.Ingest(sample)
	}

	continuousResults := continuous.SnapshotWindows()
	resumedResults := resumed.SnapshotWindows()
	if len(continuousResults) != len(resumedResults) {
		t.Fatalf("window count = %d, want %d", len(resumedResults), len(continuousResults))
	}
	for i := range continuousResults {
		if !windowEqual(continuousResults[i], resumedResults[i]) {
			t.Fatalf("window %d mismatch:\ncontinuous=%+v\nresumed=%+v", i, continuousResults[i], resumedResults[i])
		}
	}
	if continuous.Stats() != resumed.Stats() {
		t.Fatalf("stats mismatch:\ncontinuous=%+v\nresumed=%+v", continuous.Stats(), resumed.Stats())
	}
}

func TestMultiLevelWindowsCorrectionsResetAndWithdrawal(t *testing.T) {
	base := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	r := NewRegistry(Config{
		WindowLevels:        []time.Duration{time.Minute, 2 * time.Minute},
		MaxSources:          10,
		MaxMetrics:          10,
		MaxMemoryBytes:      1 << 20,
		MaxRejectionRecords: 10,
	})
	r.SetClock(func() time.Time { return base.Add(2 * time.Hour) })
	must := func(sample Sample) {
		t.Helper()
		if err := r.Ingest(sample); err != nil {
			t.Fatalf("ingest %s: %v", sample.ID, err)
		}
	}
	if err := r.AddMetric(Definition{Name: "requests", Type: Counter}); err != nil {
		t.Fatal(err)
	}
	if err := r.AddMetric(Definition{Name: "latency", Type: Distribution, BucketBounds: []float64{5, 10, 15}}); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterSource(Source{ID: "srv-a"}); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterSource(Source{ID: "srv-b"}); err != nil {
		t.Fatal(err)
	}

	must(Sample{ID: "a1", Metric: "requests", Source: "srv-a", Timestamp: base.Add(10 * time.Second), Value: 100})
	must(Sample{ID: "a2", Metric: "requests", Source: "srv-a", Timestamp: base.Add(20 * time.Second), Value: 110})
	must(Sample{ID: "d1", Metric: "latency", Source: "srv-a", Timestamp: base.Add(15 * time.Second), Value: 4})
	must(Sample{ID: "d2", Metric: "latency", Source: "srv-a", Timestamp: base.Add(25 * time.Second), Value: 11})
	must(Sample{ID: "b1", Metric: "requests", Source: "srv-b", Timestamp: base.Add(30 * time.Second), Value: 7})
	if err := r.ResetSource("srv-a", base.Add(50*time.Second), "deploy"); err != nil {
		t.Fatal(err)
	}
	if closed := r.CloseWindows(base.Add(time.Minute)); closed != 1 {
		t.Fatalf("closed first minute = %d, want 1", closed)
	}

	late := []Sample{
		{ID: "a-late", Metric: "requests", Source: "srv-a", Timestamp: base.Add(25 * time.Second), Value: 115},
		{ID: "d-late", Metric: "latency", Source: "srv-a", Timestamp: base.Add(35 * time.Second), Value: 14},
	}
	revised, err := r.AmendWindow(0, base, late, "late backfill")
	if err != nil {
		t.Fatal(err)
	}
	if revised.Revision != 2 {
		t.Fatalf("revision = %d, want 2", revised.Revision)
	}
	old, ok := r.SnapshotWindowRevision(0, base, 1)
	if !ok || !old.Superseded {
		t.Fatalf("old revision not retained traceably: ok=%v result=%+v", ok, old)
	}

	must(Sample{ID: "a3", Metric: "requests", Source: "srv-a", Timestamp: base.Add(70 * time.Second), Value: 5})
	must(Sample{ID: "d3", Metric: "latency", Source: "srv-a", Timestamp: base.Add(80 * time.Second), Value: 16})
	if err := r.WithdrawSource("srv-b", base.Add(time.Minute), "partial source exit"); err != nil {
		t.Fatal(err)
	}
	err = r.Ingest(Sample{ID: "b2", Metric: "requests", Source: "srv-b", Timestamp: base.Add(70 * time.Second), Value: 8})
	if !errors.Is(err, ErrSourceWithdrawn) {
		t.Fatalf("withdrawn source ingest = %v, want %v", err, ErrSourceWithdrawn)
	}
	if closed := r.CloseWindows(base.Add(2 * time.Minute)); closed != 2 {
		t.Fatalf("closed second minute and parent = %d, want 2", closed)
	}

	current, ok := r.SnapshotWindowRevision(0, base, 2)
	if !ok {
		t.Fatal("revised window missing")
	}
	counter := current.Counters[ResultKey("srv-a", "requests")]
	if counter.Samples != 3 || counter.Total != 15 || counter.Resets != 0 {
		t.Fatalf("revised minute counter = %+v, want samples=3 total=15 resets=0", counter)
	}
	distribution := current.Distributions[ResultKey("srv-a", "latency")]
	if distribution.Count != 3 || distribution.Sum != 29 || !uint64sEqual(distribution.Buckets, []uint64{1, 0, 2, 0}) {
		t.Fatalf("revised minute distribution = %+v", distribution)
	}
	second, ok := r.SnapshotWindowRevision(0, base.Add(time.Minute), 1)
	if !ok {
		t.Fatal("second minute missing")
	}
	nextCounter := second.Counters[ResultKey("srv-a", "requests")]
	if nextCounter.Samples != 1 || nextCounter.Total != 5 || nextCounter.Resets != 1 {
		t.Fatalf("reset counter = %+v, want samples=1 total=5 resets=1", nextCounter)
	}
	parent, ok := r.SnapshotWindowRevision(1, base, 2)
	if !ok || parent.Open {
		t.Fatalf("parent revision missing or open: %+v ok=%v", parent, ok)
	}
	parentCounter := parent.Counters[ResultKey("srv-a", "requests")]
	if parentCounter.Samples != 4 || parentCounter.Total != 20 || parentCounter.Resets != 1 {
		t.Fatalf("parent counter = %+v, want samples=4 total=20 resets=1", parentCounter)
	}
	parentDistribution := parent.Distributions[ResultKey("srv-a", "latency")]
	if parentDistribution.Count != 4 || parentDistribution.Sum != 45 || !uint64sEqual(parentDistribution.Buckets, []uint64{1, 0, 2, 1}) {
		t.Fatalf("parent distribution = %+v", parentDistribution)
	}
	partial := parent.Counters[ResultKey("srv-b", "requests")]
	if partial.Samples != 1 || partial.Total != 0 {
		t.Fatalf("partial source counter = %+v, want samples=1 total=0", partial)
	}
	rejections := r.Rejections()
	if len(rejections) != 1 || rejections[0].ReasonCode != "source_withdrawn" || rejections[0].SampleID != "b2" {
		t.Fatalf("rejections = %+v, want queryable withdrawn b2", rejections)
	}

	var buffer bytes.Buffer
	if err := r.Save(&buffer); err != nil {
		t.Fatal(err)
	}
	resumed, err := Load(&buffer)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumed.Ingest(late[0]); !errors.Is(err, ErrDuplicateSample) {
		t.Fatalf("resumed corrected sample = %v, want duplicate", err)
	}
	resumedParent, ok := resumed.SnapshotWindowRevision(1, base, 2)
	if !ok || !windowEqual(resumedParent, parent) {
		t.Fatalf("resumed parent mismatch ok=%v:\n%+v\n%+v", ok, resumedParent, parent)
	}
	got := resumed.Rejections()
	found := false
	for _, rejection := range got {
		found = found || (rejection.SampleID == "b2" && rejection.ReasonCode == "source_withdrawn")
	}
	if !found {
		t.Fatalf("resumed rejection missing b2: %+v", got)
	}
}

func newTestRegistry(base time.Time) *Registry {
	r := NewRegistry(Config{WindowSize: time.Minute, MaxSources: 10, MaxMetrics: 10, MaxMemoryBytes: 1 << 20})
	r.SetClock(func() time.Time { return base.Add(time.Hour) })
	if err := r.AddMetric(Definition{Name: "requests", Type: Counter}); err != nil {
		panic(err)
	}
	if err := r.AddMetric(Definition{Name: "latency", Type: Distribution, BucketBounds: []float64{5, 10, 15}}); err != nil {
		panic(err)
	}
	if err := r.RegisterSource(Source{ID: "srv-a"}); err != nil {
		panic(err)
	}
	return r
}

func mustIngest(t *testing.T, r *Registry, sample Sample) {
	t.Helper()
	if err := r.Ingest(sample); err != nil {
		t.Fatalf("ingest %s: %v", sample.ID, err)
	}
}

func uint64sEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func windowEqual(a, b WindowResult) bool {
	if !a.Start.Equal(b.Start) || !a.End.Equal(b.End) || a.Open != b.Open || len(a.Counters) != len(b.Counters) || len(a.Distributions) != len(b.Distributions) {
		return false
	}
	for key := range a.Counters {
		if a.Counters[key] != b.Counters[key] {
			return false
		}
	}
	for key := range a.Distributions {
		left := a.Distributions[key]
		right := b.Distributions[key]
		if left.Count != right.Count || left.Sum != right.Sum || left.Min != right.Min || left.Max != right.Max || !uint64sEqual(left.Buckets, right.Buckets) {
			return false
		}
	}
	return true
}

func TestDistributionNoNaNOnEmptyResult(t *testing.T) {
	if math.IsNaN((DistributionResult{}).Min) {
		t.Fatal("empty result should use zero value, not NaN")
	}
}
