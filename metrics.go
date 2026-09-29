package metrics

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

// MetricType selects how samples for a metric are aggregated.
type MetricType int

const (
	// MetricCounter treats sample values as points read from a cumulative
	// counter. Only the positive increase between two consecutive readings is
	// added to a window; a value smaller than the previous one is a reset.
	MetricCounter MetricType = iota + 1
	// MetricDistribution treats every sample as an independent observation.
	MetricDistribution
)

// Definition describes one metric. Buckets are upper-inclusive upper bounds
// and are only used by distributions, e.g. []float64{1, 5, 10} creates
// buckets (-inf,1], (1,5], (5,10], (10,+inf).
type Definition struct {
	Name    string
	Type    MetricType
	Buckets []float64
}

// Sample is one reading reported by a source.
type Sample struct {
	Source    string
	Metric    string
	Timestamp time.Time
	Value     float64
}

var (
	// ErrUnknownMetric is returned when a sample references an unregistered metric.
	ErrUnknownMetric = errors.New("metrics: unknown metric")
	// ErrSourceLimit is returned when a previously unseen source would exceed
	// Config.MaxSources.
	ErrSourceLimit = errors.New("metrics: source limit exceeded")
	// ErrMemoryLimit is returned when the deduplication table would exceed
	// Config.MaxMemoryEntries.
	ErrMemoryLimit = errors.New("metrics: memory limit exceeded")
)

// Stats makes overload and data-quality decisions visible to callers such as
// a future alerting layer.
type Stats struct {
	Received           int64
	Accepted           int64
	DroppedDuplicate   int64
	DroppedLate        int64
	DroppedSourceLimit int64
	DroppedMemoryLimit int64
	CounterResets      int64
	Sources            int
	OpenWindows        int
	ClosedWindows      int
}

// Config bounds an Aggregator. Zero means "no limit" for both limits.
type Config struct {
	WindowSize       time.Duration
	MaxSources       int
	MaxMemoryEntries int
}

// DistributionResult summarizes one distribution in one window.
type DistributionResult struct {
	Count        int64
	Sum          float64
	Min          float64
	Max          float64
	Mean         float64
	Variance     float64
	BucketBounds []float64
	BucketCounts []int64
}

// CounterResult summarizes one counter in one window.
type CounterResult struct {
	Samples    int64
	Increase   float64
	ResetCount int64
}

// WindowResult is the finalized or in-progress aggregation for one window.
type WindowResult struct {
	Start         time.Time
	End           time.Time
	Closed        bool
	Counters      map[string]CounterResult
	Distributions map[string]DistributionResult
}

type metricAgg struct {
	// counter fields
	Samples    int64   `json:"samples"`
	Increase   float64 `json:"increase"`
	ResetCount int64   `json:"reset_count"`

	// distribution fields
	Count        int64   `json:"count"`
	Sum          float64 `json:"sum"`
	Min          float64 `json:"min"`
	Max          float64 `json:"max"`
	M2           float64 `json:"m2"` // Welford sum of squared deviations
	Mean         float64 `json:"mean"`
	BucketCounts []int64 `json:"bucket_counts"`
}

type windowState struct {
	start   time.Time
	closed  bool
	metrics map[string]*metricAgg
}

type counterCursor struct {
	seen  bool
	value float64
}

// Aggregator is an in-process, dependency-free collector. It is safe for use
// by a single goroutine at a time.
type Aggregator struct {
	windowSize time.Duration
	maxSources int
	maxMemory  int

	definitions map[string]Definition
	sources     map[string]struct{}
	windows     map[int64]*windowState
	cursors     map[string]counterCursor
	seen        map[string]struct{}
	stats       Stats
}

// New creates an Aggregator. Definitions are copied and bucket bounds are
// sorted and deduplicated.
func New(cfg Config, defs ...Definition) (*Aggregator, error) {
	if cfg.WindowSize <= 0 {
		return nil, errors.New("metrics: WindowSize must be positive")
	}
	a := &Aggregator{
		windowSize:  cfg.WindowSize,
		maxSources:  cfg.MaxSources,
		maxMemory:   cfg.MaxMemoryEntries,
		definitions: make(map[string]Definition),
		sources:     make(map[string]struct{}),
		windows:     make(map[int64]*windowState),
		cursors:     make(map[string]counterCursor),
		seen:        make(map[string]struct{}),
	}
	for _, d := range defs {
		if err := a.Register(d); err != nil {
			return nil, err
		}
	}
	return a, nil
}

// Register adds or replaces a metric definition.
func (a *Aggregator) Register(d Definition) error {
	if d.Name == "" {
		return errors.New("metrics: metric name is required")
	}
	if d.Type == 0 {
		return fmt.Errorf("metrics: metric %q has no type", d.Name)
	}
	if d.Type == MetricDistribution {
		d.Buckets = normalizedBounds(d.Buckets)
	}
	a.definitions[d.Name] = d
	return nil
}

func normalizedBounds(in []float64) []float64 {
	if len(in) == 0 {
		return nil
	}
	work := append([]float64(nil), in...)
	sort.Float64s(work)
	out := work[:0]
	for _, v := range work {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		if len(out) == 0 || v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

// Add ingests one sample. Duplicate and late samples are counted in Stats and
// are not accumulated. Limit and validation failures return explicit errors
// and are also counted, so overload cannot pass silently.
func (a *Aggregator) Add(s Sample) error {
	a.stats.Received++
	d, ok := a.definitions[s.Metric]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownMetric, s.Metric)
	}
	if s.Source == "" {
		return errors.New("metrics: sample source is required")
	}
	if s.Timestamp.IsZero() {
		return errors.New("metrics: sample timestamp is required")
	}

	fingerprint := sampleFingerprint(s)
	if _, dup := a.seen[fingerprint]; dup {
		a.stats.DroppedDuplicate++
		return nil
	}

	if _, known := a.sources[s.Source]; !known {
		if a.maxSources > 0 && len(a.sources) >= a.maxSources {
			a.stats.DroppedSourceLimit++
			return fmt.Errorf("%w: %q", ErrSourceLimit, s.Source)
		}
		a.sources[s.Source] = struct{}{}
	}
	if a.maxMemory > 0 && len(a.seen) >= a.maxMemory {
		a.stats.DroppedMemoryLimit++
		return fmt.Errorf("%w: dedup table full (%d entries)", ErrMemoryLimit, a.maxMemory)
	}

	idx := a.windowIndex(s.Timestamp)
	w := a.windows[idx]
	if w == nil {
		w = &windowState{
			start:   a.windowStart(s.Timestamp),
			metrics: make(map[string]*metricAgg),
		}
		a.windows[idx] = w
	}
	if w.closed {
		a.stats.DroppedLate++
		return nil
	}

	agg := w.metrics[s.Metric]
	if agg == nil {
		agg = &metricAgg{}
		if d.Type == MetricDistribution {
			agg.Min = math.Inf(1)
			agg.Max = math.Inf(-1)
			agg.BucketCounts = make([]int64, len(d.Buckets)+1)
		}
		w.metrics[s.Metric] = agg
	}

	switch d.Type {
	case MetricCounter:
		a.addCounter(s, agg)
	case MetricDistribution:
		a.addDistribution(s, d, agg)
	default:
		return fmt.Errorf("metrics: unsupported metric type %d", d.Type)
	}

	a.seen[fingerprint] = struct{}{}
	a.stats.Accepted++
	return nil
}

func (a *Aggregator) addCounter(s Sample, agg *metricAgg) {
	key := s.Source + "\x00" + s.Metric
	prev := a.cursors[key]
	agg.Samples++
	if !prev.seen {
		// First reading is the baseline; the increase before it is unknown.
		prev = counterCursor{seen: true, value: s.Value}
	} else if s.Value < prev.value {
		// Cumulative counter reset: never report a negative increase.
		agg.Increase += s.Value
		agg.ResetCount++
		a.stats.CounterResets++
		prev.value = s.Value
	} else {
		agg.Increase += s.Value - prev.value
		prev.value = s.Value
	}
	a.cursors[key] = prev
}

func (a *Aggregator) addDistribution(s Sample, d Definition, agg *metricAgg) {
	agg.Count++
	agg.Sum += s.Value
	if s.Value < agg.Min {
		agg.Min = s.Value
	}
	if s.Value > agg.Max {
		agg.Max = s.Value
	}
	// Welford update for numerically stable variance.
	delta := s.Value - agg.Mean
	agg.Mean += delta / float64(agg.Count)
	agg.M2 += delta * (s.Value - agg.Mean)
	bucket := sort.SearchFloat64s(d.Buckets, s.Value)
	agg.BucketCounts[bucket]++
}

// CloseBefore marks windows ending at or before t as closed. Samples for a
// closed window are late and are counted in Stats.DroppedLate.
func (a *Aggregator) CloseBefore(t time.Time) {
	cutoff := a.windowIndex(t)
	for idx, w := range a.windows {
		if idx < cutoff && !w.closed {
			w.closed = true
		}
	}
}

// Stats returns a copy of the current collector counters.
func (a *Aggregator) Stats() Stats {
	s := a.stats
	s.Sources = len(a.sources)
	for _, w := range a.windows {
		if w.closed {
			s.ClosedWindows++
		} else {
			s.OpenWindows++
		}
	}
	return s
}

// Results returns all windows in chronological order. Closed windows are
// immutable and suitable for handing to an alerting system.
func (a *Aggregator) Results() []WindowResult {
	indices := make([]int64, 0, len(a.windows))
	for idx := range a.windows {
		indices = append(indices, idx)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
	out := make([]WindowResult, 0, len(indices))
	for _, idx := range indices {
		w := a.windows[idx]
		res := WindowResult{
			Start:         w.start,
			End:           w.start.Add(a.windowSize),
			Closed:        w.closed,
			Counters:      map[string]CounterResult{},
			Distributions: map[string]DistributionResult{},
		}
		names := make([]string, 0, len(w.metrics))
		for name := range w.metrics {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			agg := w.metrics[name]
			switch a.definitions[name].Type {
			case MetricCounter:
				res.Counters[name] = CounterResult{
					Samples:    agg.Samples,
					Increase:   agg.Increase,
					ResetCount: agg.ResetCount,
				}
			case MetricDistribution:
				variance := 0.0
				if agg.Count > 1 {
					variance = agg.M2 / float64(agg.Count-1)
				}
				res.Distributions[name] = DistributionResult{
					Count:        agg.Count,
					Sum:          agg.Sum,
					Min:          agg.Min,
					Max:          agg.Max,
					Mean:         agg.Mean,
					Variance:     variance,
					BucketBounds: append([]float64(nil), a.definitions[name].Buckets...),
					BucketCounts: append([]int64(nil), agg.BucketCounts...),
				}
			}
		}
		out = append(out, res)
	}
	return out
}

func (a *Aggregator) windowIndex(t time.Time) int64 {
	return t.UnixNano() / int64(a.windowSize)
}

func (a *Aggregator) windowStart(t time.Time) time.Time {
	idx := a.windowIndex(t)
	return time.Unix(0, idx*int64(a.windowSize))
}

func sampleFingerprint(s Sample) string {
	return fmt.Sprintf("%s\x00%s\x00%d\x00%g", s.Source, s.Metric, s.Timestamp.UnixNano(), s.Value)
}
