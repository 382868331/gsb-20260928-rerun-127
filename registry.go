package metriccore

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type Config struct {
	WindowSize     time.Duration `json:"window_size"`
	MaxSources     int           `json:"max_sources"`
	MaxMetrics     int           `json:"max_metrics"`
	MaxMemoryBytes int64         `json:"max_memory_bytes"`
}

type Registry struct {
	mu sync.RWMutex

	cfg           Config
	metrics       map[string]Definition
	sources       map[string]Source
	windows       map[time.Time]*windowState
	lastValue     map[string]counterState
	seen          map[string]time.Time
	closedThrough time.Time
	stats         Stats
	now           func() time.Time
}

type windowState struct {
	Start         time.Time
	End           time.Time
	Open          bool
	Counters      map[string]CounterResult
	Distributions map[string]DistributionResult
}

type counterState struct {
	Value  float64 `json:"value"`
	Exists bool    `json:"exists"`
}

func NewRegistry(cfg Config) *Registry {
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = time.Minute
	}
	if cfg.MaxMetrics == 0 {
		cfg.MaxMetrics = 1000
	}
	if cfg.MaxSources == 0 {
		cfg.MaxSources = 10000
	}
	if cfg.MaxMemoryBytes == 0 {
		cfg.MaxMemoryBytes = 64 << 20
	}
	return &Registry{
		cfg:       cfg,
		metrics:   make(map[string]Definition),
		sources:   make(map[string]Source),
		windows:   make(map[time.Time]*windowState),
		lastValue: make(map[string]counterState),
		seen:      make(map[string]time.Time),
		stats:     Stats{},
		now:       time.Now,
	}
}

func (r *Registry) AddMetric(def Definition) error {
	def = def.normalized()
	if err := def.validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.metrics[def.Name]; exists {
		return fmt.Errorf("metric %q already exists", def.Name)
	}
	if len(r.metrics) >= r.cfg.MaxMetrics {
		r.recordLimitLocked("metric limit reached")
		return ErrMetricLimit
	}
	additional := int64(96 + len(def.Name) + len(def.Description) + 8*len(def.BucketBounds))
	if r.stats.EstimatedMemoryBytes+additional > r.cfg.MaxMemoryBytes {
		r.recordLimitLocked("memory limit reached while adding metric")
		return ErrMemoryLimit
	}
	r.metrics[def.Name] = def
	r.refreshMemoryLocked()
	return nil
}

func (r *Registry) RegisterSource(source Source) error {
	if source.ID == "" {
		return fmt.Errorf("source id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.sources[source.ID]; exists {
		return fmt.Errorf("source %q already exists", source.ID)
	}
	if len(r.sources) >= r.cfg.MaxSources {
		r.recordLimitLocked(fmt.Sprintf("source limit reached: %d", r.cfg.MaxSources))
		return ErrSourceLimit
	}
	additional := int64(96 + len(source.ID))
	for k, v := range source.Labels {
		additional += int64(64 + len(k) + len(v))
	}
	if r.stats.EstimatedMemoryBytes+additional > r.cfg.MaxMemoryBytes {
		r.recordLimitLocked("memory limit reached while registering source")
		return ErrMemoryLimit
	}
	r.sources[source.ID] = cloneSource(source)
	r.refreshMemoryLocked()
	return nil
}

func (r *Registry) Ingest(sample Sample) error {
	if err := sample.validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now().UTC()
	ts := sample.Timestamp.UTC()
	if ts.After(now) {
		return ErrFutureSample
	}
	if _, ok := r.seen[sample.ID]; ok {
		r.stats.DuplicateSamples++
		return fmt.Errorf("%w: %s", ErrDuplicateSample, sample.ID)
	}
	def, ok := r.metrics[sample.Metric]
	if !ok {
		return fmt.Errorf("%w: %s", ErrMetricNotFound, sample.Metric)
	}
	if _, ok := r.sources[sample.Source]; !ok {
		return fmt.Errorf("%w: %s", ErrSourceNotFound, sample.Source)
	}

	start := ts.Truncate(r.cfg.WindowSize)
	end := start.Add(r.cfg.WindowSize)
	if !r.closedThrough.IsZero() && !end.After(r.closedThrough) {
		r.stats.LateSamples++
		return fmt.Errorf("%w: %s ends at %s", ErrLateSample, start.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano))
	}
	w, exists := r.windows[start]
	if exists && !w.Open {
		r.stats.LateSamples++
		return fmt.Errorf("%w: %s", ErrLateSample, start.Format(time.RFC3339Nano))
	}
	if !exists {
		if r.stats.EstimatedMemoryBytes+estimateWindowBytes(def) > r.cfg.MaxMemoryBytes {
			r.recordLimitLocked(fmt.Sprintf("memory limit reached while opening window %s", start.Format(time.RFC3339Nano)))
			return ErrMemoryLimit
		}
		w = newWindow(start, end)
		r.windows[start] = w
	}

	key := ResultKey(sample.Source, sample.Metric)
	switch def.Type {
	case Counter:
		r.applyCounter(w, key, sample.Value)
	case Distribution:
		w.Distributions[key] = applyDistribution(w.Distributions[key], def.BucketBounds, sample.Value)
	}
	r.lastValue[key] = counterState{Value: sample.Value, Exists: true}
	r.seen[sample.ID] = start
	r.refreshMemoryLocked()
	return nil
}

func ResultKey(sourceID, metricName string) string {
	return sourceID + "\x00" + metricName
}

func ParseResultKey(key string) (sourceID, metricName string, ok bool) {
	parts := strings.SplitN(key, "\x00", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func (r *Registry) applyCounter(w *windowState, key string, value float64) {
	result := w.Counters[key]
	result.Samples++
	previous, hasPrevious := r.lastValue[key]
	if !hasPrevious {
	} else if value >= previous.Value {
		result.Total += value - previous.Value
	} else if value < 0 {
		result.NegativeDrops++
	} else {
		result.Resets++
		result.Total += value
	}
	w.Counters[key] = result
}

func applyDistribution(result DistributionResult, bounds []float64, value float64) DistributionResult {
	if result.Count == 0 {
		result.Min = value
		result.Max = value
	} else {
		if value < result.Min {
			result.Min = value
		}
		if value > result.Max {
			result.Max = value
		}
	}
	if len(result.Buckets) != len(bounds)+1 {
		result.Buckets = make([]uint64, len(bounds)+1)
	}
	result.Count++
	result.Sum += value
	idx := sort.SearchFloat64s(bounds, value)
	result.Buckets[idx]++
	return result
}

func (r *Registry) CloseWindows(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	now = now.UTC()
	closed := 0
	for start, w := range r.windows {
		if w.Open && !w.End.After(now) {
			w.Open = false
			closed++
			if w.End.After(r.closedThrough) {
				r.closedThrough = w.End
			}
		}
		_ = start
	}
	return closed
}

func (r *Registry) PurgeWindows(before time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	purged := 0
	for start, w := range r.windows {
		if w.End.Before(before.UTC()) || w.End.Equal(before.UTC()) {
			delete(r.windows, start)
			purged++
		}
	}
	r.refreshMemoryLocked()
	return purged
}

func (r *Registry) SnapshotWindow(start time.Time) (WindowResult, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	w, ok := r.windows[start.UTC().Truncate(r.cfg.WindowSize)]
	if !ok {
		return WindowResult{}, false
	}
	return exportWindow(w), true
}

func (r *Registry) SnapshotWindows() []WindowResult {
	r.mu.RLock()
	defer r.mu.RUnlock()
	results := make([]WindowResult, 0, len(r.windows))
	for _, w := range r.windows {
		results = append(results, exportWindow(w))
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Start.Before(results[j].Start) })
	return results
}

func (r *Registry) Stats() Stats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s := r.stats
	s.Metrics = int64(len(r.metrics))
	s.Sources = int64(len(r.sources))
	s.Windows = int64(len(r.windows))
	s.DedupeEntries = int64(len(r.seen))
	return s
}

func (r *Registry) recordLimitLocked(message string) {
	r.stats.LimitErrors++
	r.stats.LastLimitError = message
}

func (r *Registry) refreshMemoryLocked() {
	var estimate int64 = 256
	for name := range r.metrics {
		estimate += int64(96 + len(name) + len(r.metrics[name].Description) + 8*len(r.metrics[name].BucketBounds))
	}
	for id, source := range r.sources {
		estimate += int64(96 + len(id))
		for k, v := range source.Labels {
			estimate += int64(64 + len(k) + len(v))
		}
	}
	for _, w := range r.windows {
		estimate += 128
		for k := range w.Counters {
			estimate += int64(96 + len(k))
		}
		for k, d := range w.Distributions {
			estimate += int64(96 + len(k) + 8*len(d.Buckets))
		}
	}
	for id := range r.seen {
		estimate += int64(48 + len(id))
	}
	for k := range r.lastValue {
		estimate += int64(48 + len(k))
	}
	r.stats.EstimatedMemoryBytes = estimate
}

func newWindow(start, end time.Time) *windowState {
	return &windowState{
		Start:         start,
		End:           end,
		Open:          true,
		Counters:      make(map[string]CounterResult),
		Distributions: make(map[string]DistributionResult),
	}
}

func exportWindow(w *windowState) WindowResult {
	out := WindowResult{
		Start: w.Start,
		End:   w.End,
		Open:  w.Open,
	}
	if len(w.Counters) > 0 {
		out.Counters = make(map[string]CounterResult, len(w.Counters))
		for k, v := range w.Counters {
			out.Counters[k] = v
		}
	}
	if len(w.Distributions) > 0 {
		out.Distributions = make(map[string]DistributionResult, len(w.Distributions))
		for k, v := range w.Distributions {
			out.Distributions[k] = v
		}
	}
	return out
}

func estimateWindowBytes(def Definition) int64 {
	return int64(256 + 8*len(def.BucketBounds))
}

func cloneSource(source Source) Source {
	clone := Source{ID: source.ID}
	if len(source.Labels) > 0 {
		clone.Labels = make(map[string]string, len(source.Labels))
		for k, v := range source.Labels {
			clone.Labels[k] = v
		}
	}
	return clone
}
