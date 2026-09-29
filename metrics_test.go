package metrics

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

var (
	defRequests = Definition{Name: "requests", Type: MetricCounter}
	defLatency  = Definition{Name: "latency_ms", Type: MetricDistribution, Buckets: []float64{2, 4}}
)

// manualSequence is the hand-verifiable sequence used by every scenario.
//
// requests, source=a (cumulative counter):
//
//	:05 v=10 baseline, :15 v=12 -> +2, :25 v=7 reset -> +7, :40 v=9 -> +2
//
// latency_ms:
//
//	:05 v=1, :10 v=3, :20 v=5 (:10 is intentionally repeated)
func manualSequence(t *testing.T, a *Aggregator) {
	t.Helper()
	samples := []Sample{
		{Source: "a", Metric: "requests", Timestamp: minTime(5), Value: 10},
		{Source: "a", Metric: "latency_ms", Timestamp: minTime(5), Value: 1},
		{Source: "a", Metric: "latency_ms", Timestamp: minTime(10), Value: 3},
		{Source: "a", Metric: "requests", Timestamp: minTime(15), Value: 12},
		{Source: "a", Metric: "latency_ms", Timestamp: minTime(10), Value: 3},
		{Source: "a", Metric: "requests", Timestamp: minTime(25), Value: 7},
		{Source: "b", Metric: "latency_ms", Timestamp: minTime(20), Value: 5},
		{Source: "a", Metric: "requests", Timestamp: minTime(40), Value: 9},
	}
	for _, sample := range samples {
		if err := a.Add(sample); err != nil {
			t.Fatalf("Add(%+v): %v", sample, err)
		}
	}
}

func minTime(minute int) time.Time {
	return time.Unix(int64(minute)*60, 0).UTC()
}

func newManualAggregator(t *testing.T) *Aggregator {
	t.Helper()
	a, err := New(Config{WindowSize: 30 * time.Minute}, defRequests, defLatency)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestManualCounterAndDistribution(t *testing.T) {
	a := newManualAggregator(t)
	manualSequence(t, a)

	results := a.Results()
	if len(results) != 2 {
		t.Fatalf("got %d windows, want 2", len(results))
	}

	first := results[0]
	gotCounter := first.Counters["requests"]
	wantCounter := CounterResult{Samples: 3, Increase: 9, ResetCount: 1}
	if gotCounter != wantCounter {
		t.Fatalf("counter = %+v, want %+v", gotCounter, wantCounter)
	}

	gotDist := first.Distributions["latency_ms"]
	if gotDist.Count != 3 || gotDist.Sum != 9 || gotDist.Min != 1 || gotDist.Max != 5 {
		t.Fatalf("distribution summary = %+v", gotDist)
	}
	if gotDist.Mean != 3 || gotDist.Variance != 4 {
		t.Fatalf("mean/variance = %v/%v, want 3/4", gotDist.Mean, gotDist.Variance)
	}
	wantBuckets := []int64{1, 1, 1}
	if !equalInt64(gotDist.BucketCounts, wantBuckets) {
		t.Fatalf("buckets = %v, want %v", gotDist.BucketCounts, wantBuckets)
	}

	second := results[1]
	gotSecond := second.Counters["requests"]
	if gotSecond.Increase != 2 || gotSecond.Samples != 1 || gotSecond.ResetCount != 0 {
		t.Fatalf("second-window counter = %+v", gotSecond)
	}

	stats := a.Stats()
	if stats.Received != 8 || stats.Accepted != 7 || stats.DroppedDuplicate != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if stats.CounterResets != 1 || stats.Sources != 2 || stats.OpenWindows != 2 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestClosedWindowDropsLateSamples(t *testing.T) {
	a := newManualAggregator(t)
	manualSequence(t, a)
	a.CloseBefore(minTime(30))

	late := Sample{Source: "a", Metric: "requests", Timestamp: minTime(0).Add(30 * time.Second), Value: 99}
	if err := a.Add(late); err != nil {
		t.Fatal(err)
	}
	stats := a.Stats()
	if stats.DroppedLate != 1 {
		t.Fatalf("DroppedLate = %d, want 1", stats.DroppedLate)
	}
	results := a.Results()
	if !results[0].Closed {
		t.Fatal("first window should be closed")
	}
	if got := results[0].Counters["requests"].Increase; got != 9 {
		t.Fatalf("late sample changed closed window: increase=%v", got)
	}

	current := Sample{Source: "a", Metric: "requests", Timestamp: minTime(30), Value: 15}
	if err := a.Add(current); err != nil {
		t.Fatal(err)
	}
}

func TestLimitsAreVisible(t *testing.T) {
	a, err := New(Config{WindowSize: 30 * time.Minute, MaxSources: 1}, defRequests, defLatency)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Add(Sample{Source: "a", Metric: "requests", Timestamp: minTime(0), Value: 1}); err != nil {
		t.Fatal(err)
	}
	err = a.Add(Sample{Source: "b", Metric: "requests", Timestamp: minTime(0), Value: 1})
	if !errors.Is(err, ErrSourceLimit) {
		t.Fatalf("err = %v, want ErrSourceLimit", err)
	}
	if got := a.Stats().DroppedSourceLimit; got != 1 {
		t.Fatalf("DroppedSourceLimit = %d, want 1", got)
	}

	bounded, err := New(Config{WindowSize: 30 * time.Minute, MaxMemoryEntries: 2}, defRequests)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := bounded.Add(Sample{Source: "a", Metric: "requests", Timestamp: minTime(i), Value: float64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	err = bounded.Add(Sample{Source: "a", Metric: "requests", Timestamp: minTime(2), Value: 2})
	if !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("err = %v, want ErrMemoryLimit", err)
	}
}

func TestUnknownMetric(t *testing.T) {
	a := newManualAggregator(t)
	if err := a.Add(Sample{Source: "a", Metric: "missing", Timestamp: minTime(0), Value: 1}); !errors.Is(err, ErrUnknownMetric) {
		t.Fatalf("err = %v, want ErrUnknownMetric", err)
	}
}

func TestCheckpointResumeMatchesFullRun(t *testing.T) {
	full := newManualAggregator(t)
	manualSequence(t, full)
	split := newManualAggregator(t)
	firstHalf := []Sample{
		{Source: "a", Metric: "requests", Timestamp: minTime(5), Value: 10},
		{Source: "a", Metric: "latency_ms", Timestamp: minTime(5), Value: 1},
		{Source: "a", Metric: "latency_ms", Timestamp: minTime(10), Value: 3},
		{Source: "a", Metric: "requests", Timestamp: minTime(15), Value: 12},
		{Source: "a", Metric: "latency_ms", Timestamp: minTime(10), Value: 3},
	}
	for _, sample := range firstHalf {
		if err := split.Add(sample); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if err := split.SaveCheckpoint(&buf); err != nil {
		t.Fatal(err)
	}
	resumed, err := LoadCheckpoint(&buf)
	if err != nil {
		t.Fatal(err)
	}

	remaining := []Sample{
		{Source: "a", Metric: "requests", Timestamp: minTime(25), Value: 7},
		{Source: "b", Metric: "latency_ms", Timestamp: minTime(20), Value: 5},
		{Source: "a", Metric: "requests", Timestamp: minTime(40), Value: 9},
		{Source: "a", Metric: "latency_ms", Timestamp: minTime(10), Value: 3}, // duplicate known before checkpoint
	}
	for _, sample := range remaining {
		if err := resumed.Add(sample); err != nil {
			t.Fatal(err)
		}
	}
	full.CloseBefore(minTime(30))
	resumed.CloseBefore(minTime(30))
	duplicate := Sample{Source: "a", Metric: "latency_ms", Timestamp: minTime(10), Value: 3}
	if err := full.Add(duplicate); err != nil {
		t.Fatal(err)
	}
	late := Sample{Source: "a", Metric: "requests", Timestamp: minTime(0).Add(30 * time.Second), Value: 99}
	if err := full.Add(late); err != nil {
		t.Fatal(err)
	}
	if err := resumed.Add(late); err != nil {
		t.Fatal(err)
	}

	if !resultsEqual(full.Results(), resumed.Results()) {
		t.Fatalf("results differ\nfull=%#v\nresumed=%#v", full.Results(), resumed.Results())
	}
	if full.Stats() != resumed.Stats() {
		t.Fatalf("stats differ\nfull=%+v\nresumed=%+v", full.Stats(), resumed.Stats())
	}
}

func equalInt64(a, b []int64) bool {
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

func resultsEqual(a, b []WindowResult) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Start != b[i].Start || a[i].End != b[i].End || a[i].Closed != b[i].Closed {
			return false
		}
		if !counterMapsEqual(a[i].Counters, b[i].Counters) {
			return false
		}
		if !distributionMapsEqual(a[i].Distributions, b[i].Distributions) {
			return false
		}
	}
	return true
}

func counterMapsEqual(a, b map[string]CounterResult) bool {
	if len(a) != len(b) {
		return false
	}
	for key, av := range a {
		if bv, ok := b[key]; !ok || av != bv {
			return false
		}
	}
	return true
}

func distributionMapsEqual(a, b map[string]DistributionResult) bool {
	if len(a) != len(b) {
		return false
	}
	for key, av := range a {
		bv, ok := b[key]
		if !ok || av.Count != bv.Count || av.Sum != bv.Sum || av.Mean != bv.Mean || av.Variance != bv.Variance || av.Min != bv.Min || av.Max != bv.Max {
			return false
		}
		if !equalInt64(av.BucketCounts, bv.BucketCounts) {
			return false
		}
	}
	return true
}
