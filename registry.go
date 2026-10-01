package metriccore

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	WindowSize          time.Duration   `json:"window_size"`
	WindowLevels        []time.Duration `json:"window_levels,omitempty"`
	MaxSources          int             `json:"max_sources"`
	MaxMetrics          int             `json:"max_metrics"`
	MaxMemoryBytes      int64           `json:"max_memory_bytes"`
	MaxRejectionRecords int             `json:"max_rejection_records,omitempty"`
}

type windowKey struct {
	Level int
	Start time.Time
}

type eventKind int

const (
	sampleEvent eventKind = iota
	resetEvent
)

type windowEvent struct {
	Kind      eventKind
	ID        string
	Metric    string
	Source    string
	Timestamp time.Time
	Value     float64
	Reason    string
}

type counterCheckpoint struct {
	Timestamp time.Time
	Value     float64
	Reset     bool
}

type Registry struct {
	mu sync.RWMutex

	cfg           Config
	metrics       map[string]Definition
	sources       map[string]*sourceState
	windows       map[windowKey]*windowState
	history       map[windowKey][]*windowState
	checkpoints   map[string][]counterCheckpoint
	resetTimes    map[string][]time.Time
	seen          map[string]time.Time
	closedThrough time.Time
	stats         Stats
	rejections    []Rejection
	maxRejections int
	levels        []time.Duration
	now           func() time.Time
}

type windowState struct {
	Level          int
	Revision       int
	Start          time.Time
	End            time.Time
	Open           bool
	Superseded     bool
	RevisedAt      time.Time
	Reason         string
	Events         []windowEvent
	RolledChildren map[windowKey]struct{}
	Counters       map[string]CounterResult
	Distributions  map[string]DistributionResult
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
	r := &Registry{
		cfg:           cfg,
		metrics:       make(map[string]Definition),
		sources:       make(map[string]*sourceState),
		windows:       make(map[windowKey]*windowState),
		history:       make(map[windowKey][]*windowState),
		checkpoints:   make(map[string][]counterCheckpoint),
		resetTimes:    make(map[string][]time.Time),
		seen:          make(map[string]time.Time),
		rejections:    make([]Rejection, 0),
		maxRejections: 1000,
		levels:        []time.Duration{cfg.WindowSize},
		stats:         Stats{},
		now:           time.Now,
	}
	if len(cfg.WindowLevels) > 0 {
		r.levels = normalizeLevels(cfg.WindowLevels)
	}
	if cfg.MaxRejectionRecords > 0 {
		r.maxRejections = cfg.MaxRejectionRecords
	} else if cfg.MaxRejectionRecords < 0 {
		r.maxRejections = 0
	}
	r.cfg.WindowSize = r.levels[0]
	r.cfg.WindowLevels = append([]time.Duration(nil), r.levels...)
	r.cfg.MaxRejectionRecords = r.maxRejections
	return r
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
		r.pushRejectionLocked(Rejection{At: r.now().UTC(), Metric: def.Name, ReasonCode: reasonCode(ErrMetricLimit), Message: "metric limit reached"})
		return ErrMetricLimit
	}
	additional := int64(96 + len(def.Name) + len(def.Description) + 8*len(def.BucketBounds))
	if r.stats.EstimatedMemoryBytes+additional > r.cfg.MaxMemoryBytes {
		r.recordLimitLocked("memory limit reached while adding metric")
		r.pushRejectionLocked(Rejection{At: r.now().UTC(), Metric: def.Name, ReasonCode: reasonCode(ErrMemoryLimit), Message: "memory limit reached while adding metric"})
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
		r.pushRejectionLocked(Rejection{At: r.now().UTC(), Source: source.ID, ReasonCode: reasonCode(ErrSourceLimit), Message: fmt.Sprintf("source limit reached: %d", r.cfg.MaxSources)})
		return ErrSourceLimit
	}
	additional := int64(96 + len(source.ID))
	for k, v := range source.Labels {
		additional += int64(64 + len(k) + len(v))
	}
	if r.stats.EstimatedMemoryBytes+additional > r.cfg.MaxMemoryBytes {
		r.recordLimitLocked("memory limit reached while registering source")
		r.pushRejectionLocked(Rejection{At: r.now().UTC(), Source: source.ID, ReasonCode: reasonCode(ErrMemoryLimit), Message: "memory limit reached while registering source"})
		return ErrMemoryLimit
	}
	r.sources[source.ID] = &sourceState{Source: cloneSource(source), Status: SourceActive}
	r.refreshMemoryLocked()
	return nil
}

func (r *Registry) ResetSource(sourceID string, at time.Time, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	source, ok := r.sources[sourceID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrSourceNotFound, sourceID)
	}
	at = at.UTC()
	if at.After(r.now().UTC()) {
		return ErrFutureSample
	}
	if source.Status == SourceWithdrawn && !at.Before(source.WithdrawnAt) {
		return fmt.Errorf("%w: %s", ErrSourceWithdrawn, sourceID)
	}
	keyStart := at.Truncate(r.levels[0])
	keyEnd := keyStart.Add(r.levels[0])
	if !r.closedThrough.IsZero() && !keyEnd.After(r.closedThrough) {
		return fmt.Errorf("%w: %s", ErrLateSample, keyStart.Format(time.RFC3339Nano))
	}
	w, err := r.openWindowLocked(0, keyStart, Definition{}, true)
	if err != nil {
		r.pushRejectionLocked(Rejection{At: r.now().UTC(), Source: sourceID, Timestamp: at, ReasonCode: reasonCode(err), Message: err.Error()})
		return err
	}
	event := windowEvent{
		Kind:      resetEvent,
		ID:        "reset:" + sourceID + ":" + strconv.FormatInt(at.UnixNano(), 10) + ":" + strconv.Itoa(len(w.Events)),
		Metric:    "*",
		Source:    sourceID,
		Timestamp: at,
		Reason:    reason,
	}
	w.Events = append(w.Events, event)
	r.rebuildWindowLocked(w)
	insertResetCheckpointLocked(r.resetTimes, sourceID, at)
	return nil
}

func (r *Registry) WithdrawSource(sourceID string, at time.Time, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	source, ok := r.sources[sourceID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrSourceNotFound, sourceID)
	}
	if source.Status == SourceWithdrawn {
		return fmt.Errorf("source %q already withdrawn", sourceID)
	}
	at = at.UTC()
	if at.After(r.now().UTC()) {
		return ErrFutureSample
	}
	if at.Before(source.WithdrawnAt) {
		return fmt.Errorf("source %q cannot re-enter after withdrawal; register a replacement source", sourceID)
	}
	source.Status = SourceWithdrawn
	source.WithdrawnAt = at
	source.WithdrawReason = reason
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
		err := ErrFutureSample
		r.recordRejectionLocked(err, "sample timestamp is in the future", sample)
		return err
	}
	if _, ok := r.seen[sample.ID]; ok {
		r.stats.DuplicateSamples++
		err := fmt.Errorf("%w: %s", ErrDuplicateSample, sample.ID)
		r.recordRejectionLocked(err, "duplicate sample", sample)
		return err
	}
	def, ok := r.metrics[sample.Metric]
	if !ok {
		err := fmt.Errorf("%w: %s", ErrMetricNotFound, sample.Metric)
		r.recordRejectionLocked(err, "metric not found", sample)
		return err
	}
	source, ok := r.sources[sample.Source]
	if !ok {
		r.recordRejectionLocked(ErrSourceNotFound, "source not found", sample)
		return fmt.Errorf("%w: %s", ErrSourceNotFound, sample.Source)
	}
	if source.Status == SourceWithdrawn && !ts.Before(source.WithdrawnAt) {
		err := fmt.Errorf("%w: %s", ErrSourceWithdrawn, sample.Source)
		r.recordRejectionLocked(err, "source has withdrawn", sample)
		return err
	}

	start := ts.Truncate(r.levels[0])
	end := start.Add(r.levels[0])
	if !r.closedThrough.IsZero() && !end.After(r.closedThrough) {
		r.stats.LateSamples++
		err := fmt.Errorf("%w: %s ends at %s", ErrLateSample, start.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano))
		r.recordRejectionLocked(err, "sample belongs to a closed window", sample)
		return err
	}
	w, exists := r.windows[windowKey{Level: 0, Start: start}]
	if exists && !w.Open {
		r.stats.LateSamples++
		err := fmt.Errorf("%w: %s", ErrLateSample, start.Format(time.RFC3339Nano))
		r.recordRejectionLocked(err, "sample belongs to a sealed window", sample)
		return err
	}
	if !exists {
		var err error
		w, err = r.openWindowLocked(0, start, def, true)
		if err != nil {
			r.recordRejectionLocked(err, "resource limit rejected sample", sample)
			return err
		}
	}

	w.Events = append(w.Events, windowEvent{
		Kind:      sampleEvent,
		ID:        sample.ID,
		Metric:    sample.Metric,
		Source:    sample.Source,
		Timestamp: ts,
		Value:     sample.Value,
	})
	r.appendCounterCheckpointLocked(sample.Source, sample.Metric, ts, sample.Value)
	r.applyEventToWindowLocked(w, def, w.Events[len(w.Events)-1])
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

	for level := 0; level < len(r.levels); level++ {
		for _, w := range r.windows {
			if w.Level != level || !w.Open || w.End.After(now) {
				continue
			}
			if level > 0 && !r.childWindowsSealedLocked(w, level, now) {
				continue
			}
			w.Open = false
			closed++
			if level == 0 && w.End.After(r.closedThrough) {
				r.closedThrough = w.End
			}
			if level+1 < len(r.levels) {
				if err := r.rollUpLocked(windowKey{Level: level, Start: w.Start}, w, level+1); err != nil {
					r.recordLimitLocked(err.Error())
				}
			}
		}
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
			delete(r.history, start)
			purged++
		}
	}
	r.refreshMemoryLocked()
	return purged
}

func (r *Registry) SnapshotWindow(start time.Time) (WindowResult, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	w, ok := r.windows[windowKey{Level: 0, Start: start.UTC().Truncate(r.levels[0])}]
	if !ok {
		return WindowResult{}, false
	}
	return exportWindow(w), true
}

func (r *Registry) SnapshotWindows() []WindowResult {
	r.mu.RLock()
	defer r.mu.RUnlock()
	all := make([]*windowState, 0, len(r.windows))
	for _, w := range r.windows {
		all = append(all, w)
	}
	for _, revisions := range r.history {
		all = append(all, revisions...)
	}
	results := make([]WindowResult, 0, len(all))
	for _, w := range all {
		results = append(results, exportWindow(w))
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Level != results[j].Level {
			return results[i].Level < results[j].Level
		}
		if !results[i].Start.Equal(results[j].Start) {
			return results[i].Start.Before(results[j].Start)
		}
		return results[i].Revision < results[j].Revision
	})
	return results
}

func (r *Registry) SnapshotWindowRevision(level int, start time.Time, revision int) (WindowResult, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if level < 0 || level >= len(r.levels) {
		return WindowResult{}, false
	}
	key := windowKey{Level: level, Start: start.UTC().Truncate(r.levels[level])}
	if revision <= 0 {
		w, ok := r.windows[key]
		return exportWindow(w), ok
	}
	if current := r.windows[key]; current != nil && current.Revision == revision {
		return exportWindow(current), true
	}
	for _, w := range r.history[key] {
		if w.Revision == revision {
			return exportWindow(w), true
		}
	}
	return WindowResult{}, false
}

func (r *Registry) AmendWindow(level int, start time.Time, samples []Sample, reason string) (WindowResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if level < 0 || level >= len(r.levels) {
		return WindowResult{}, fmt.Errorf("window level %d out of range", level)
	}
	start = start.UTC().Truncate(r.levels[level])
	key := windowKey{Level: level, Start: start}
	original := r.windows[key]
	if original == nil || original.Open {
		return WindowResult{}, ErrWindowOpen
	}
	for _, sample := range samples {
		if err := sample.validate(); err != nil {
			return WindowResult{}, err
		}
		if _, ok := r.metrics[sample.Metric]; !ok {
			return WindowResult{}, fmt.Errorf("%w: %s", ErrMetricNotFound, sample.Metric)
		}
		if source, ok := r.sources[sample.Source]; !ok || (source.Status == SourceWithdrawn && !sample.Timestamp.UTC().Before(source.WithdrawnAt)) {
			if !ok {
				return WindowResult{}, fmt.Errorf("%w: %s", ErrSourceNotFound, sample.Source)
			}
			return WindowResult{}, fmt.Errorf("%w: %s", ErrSourceWithdrawn, sample.Source)
		}
		if _, ok := r.seen[sample.ID]; ok {
			return WindowResult{}, fmt.Errorf("%w: %s", ErrDuplicateSample, sample.ID)
		}
		windowStart := sample.Timestamp.UTC().Truncate(r.levels[level])
		if !windowStart.Equal(start) {
			return WindowResult{}, fmt.Errorf("sample %s does not belong to amended window", sample.ID)
		}
	}
	additional := int64(256)
	for _, sample := range samples {
		additional += int64(128 + len(sample.ID) + len(sample.Metric) + len(sample.Source))
	}
	for ancestorLevel := level + 1; ancestorLevel < len(r.levels); ancestorLevel++ {
		ancestorKey := windowKey{Level: ancestorLevel, Start: start.Truncate(r.levels[ancestorLevel])}
		if r.windows[ancestorKey] != nil {
			additional += 256
		}
	}
	if r.stats.EstimatedMemoryBytes+additional > r.cfg.MaxMemoryBytes {
		r.recordLimitLocked("memory limit reached while revising sealed window")
		for _, sample := range samples {
			r.pushRejectionLocked(Rejection{At: r.now().UTC(), SampleID: sample.ID, Metric: sample.Metric, Source: sample.Source, Timestamp: sample.Timestamp.UTC(), ReasonCode: reasonCode(ErrMemoryLimit), Message: "memory limit reached while revising sealed window"})
		}
		return WindowResult{}, ErrMemoryLimit
	}
	revised := cloneWindow(original)
	revised.Revision = original.Revision + 1
	revised.RevisedAt = r.now().UTC()
	revised.Reason = reason
	revised.Superseded = false
	revised.Open = false
	for _, sample := range samples {
		def := r.metrics[sample.Metric]
		event := windowEvent{Kind: sampleEvent, ID: sample.ID, Metric: sample.Metric, Source: sample.Source, Timestamp: sample.Timestamp.UTC(), Value: sample.Value}
		revised.Events = append(revised.Events, event)
		r.appendCounterCheckpointLocked(sample.Source, sample.Metric, sample.Timestamp.UTC(), sample.Value)
		r.applyEventToWindowLocked(revised, def, event)
		r.seen[sample.ID] = start
	}
	backupHistory := len(r.history[key])
	r.history[key] = append(r.history[key], original)
	original.Superseded = true
	r.windows[key] = revised
	if err := r.reviseAncestorsLocked(key, revised); err != nil {
		r.history[key] = r.history[key][:backupHistory]
		original.Superseded = false
		r.windows[key] = original
		return WindowResult{}, err
	}
	r.stats.WindowRevisions++
	r.refreshMemoryLocked()
	return exportWindow(revised), nil
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

func (r *Registry) recordRejectionLocked(err error, message string, sample Sample) {
	r.pushRejectionLocked(Rejection{
		At:         r.now().UTC(),
		SampleID:   sample.ID,
		Metric:     sample.Metric,
		Source:     sample.Source,
		Timestamp:  sample.Timestamp.UTC(),
		ReasonCode: reasonCode(err),
		Message:    message + ": " + err.Error(),
	})

}

func (r *Registry) pushRejectionLocked(rejection Rejection) {
	r.rejections = append(r.rejections, rejection)
	if r.maxRejections == 0 {
		r.rejections = r.rejections[:0]
		r.stats.DroppedRejections++
	} else if len(r.rejections) > r.maxRejections {
		drop := len(r.rejections) - r.maxRejections
		r.rejections = append(r.rejections[:0], r.rejections[drop:]...)
		r.stats.DroppedRejections += int64(drop)
	}
	r.stats.RejectionRecords = int64(len(r.rejections))
}

func reasonCode(err error) string {
	switch {
	case errors.Is(err, ErrDuplicateSample):
		return "duplicate_sample"
	case errors.Is(err, ErrLateSample):
		return "late_sample"
	case errors.Is(err, ErrFutureSample):
		return "future_sample"
	case errors.Is(err, ErrMetricNotFound):
		return "metric_not_found"
	case errors.Is(err, ErrSourceNotFound):
		return "source_not_found"
	case errors.Is(err, ErrSourceWithdrawn):
		return "source_withdrawn"
	case errors.Is(err, ErrSourceLimit):
		return "source_limit"
	case errors.Is(err, ErrMetricLimit):
		return "metric_limit"
	case errors.Is(err, ErrMemoryLimit):
		return "memory_limit"
	default:
		return "rejected"
	}
}

func (r *Registry) Rejections() []Rejection {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Rejection, len(r.rejections))
	copy(out, r.rejections)
	return out
}

func (r *Registry) refreshMemoryLocked() {
	var estimate int64 = 256
	for name := range r.metrics {
		estimate += int64(96 + len(name) + len(r.metrics[name].Description) + 8*len(r.metrics[name].BucketBounds))
	}
	for id, source := range r.sources {
		estimate += int64(128 + len(id) + len(source.WithdrawReason))
		for k, v := range source.Source.Labels {
			estimate += int64(64 + len(k) + len(v))
		}
	}
	windows := make(map[windowKey]*windowState, len(r.windows)+len(r.history))
	for key, w := range r.windows {
		windows[key] = w
	}
	for key, revisions := range r.history {
		for _, w := range revisions {
			windows[windowKey{Level: key.Level, Start: key.Start.Add(time.Duration(w.Revision))}] = w
		}
	}
	for _, w := range windows {
		estimate += 192
		for _, event := range w.Events {
			estimate += int64(96 + len(event.ID) + len(event.Metric) + len(event.Source))
		}
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
	for key, points := range r.checkpoints {
		estimate += int64(48+len(key)) + int64(24*len(points))
	}
	for _, rejection := range r.rejections {
		estimate += int64(96 + len(rejection.SampleID) + len(rejection.Metric) + len(rejection.Source) + len(rejection.ReasonCode) + len(rejection.Message))
	}
	r.stats.EstimatedMemoryBytes = estimate
}

func newWindow(level int, start time.Time, size time.Duration) *windowState {
	return &windowState{
		Level:          level,
		Revision:       1,
		Start:          start,
		End:            start.Add(size),
		Open:           true,
		Events:         make([]windowEvent, 0),
		RolledChildren: make(map[windowKey]struct{}),
		Counters:       make(map[string]CounterResult),
		Distributions:  make(map[string]DistributionResult),
	}
}

func exportWindow(w *windowState) WindowResult {
	out := WindowResult{
		Level:      w.Level,
		Revision:   w.Revision,
		Start:      w.Start,
		End:        w.End,
		Open:       w.Open,
		Superseded: w.Superseded,
		RevisedAt:  w.RevisedAt,
		Reason:     w.Reason,
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

func normalizeLevels(levels []time.Duration) []time.Duration {
	seen := make(map[time.Duration]struct{}, len(levels))
	out := make([]time.Duration, 0, len(levels))
	for _, level := range levels {
		if level <= 0 {
			continue
		}
		if _, ok := seen[level]; ok {
			continue
		}
		seen[level] = struct{}{}
		out = append(out, level)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	valid := make([]time.Duration, 0, len(out))
	if len(out) > 0 {
		valid = append(valid, out[0])
	}
	for _, level := range out[1:] {
		if level%valid[len(valid)-1] == 0 {
			valid = append(valid, level)
		}
	}
	return out
}

func (r *Registry) openWindowLocked(level int, start time.Time, def Definition, enforceLimit bool) (*windowState, error) {
	key := windowKey{Level: level, Start: start}
	if w := r.windows[key]; w != nil {
		return w, nil
	}
	var additional int64 = 256
	if enforceLimit && level == 0 {
		additional = estimateWindowBytes(def)
	}
	if enforceLimit && r.stats.EstimatedMemoryBytes+additional > r.cfg.MaxMemoryBytes {
		msg := fmt.Sprintf("memory limit reached while opening level-%d window %s", level, start.Format(time.RFC3339Nano))
		r.recordLimitLocked(msg)
		return nil, ErrMemoryLimit
	}
	w := newWindow(level, start, r.levels[level])
	r.windows[key] = w
	return w, nil
}

func (r *Registry) applyEventToWindowLocked(w *windowState, def Definition, event windowEvent) {
	sort.SliceStable(w.Events, func(i, j int) bool {
		if !w.Events[i].Timestamp.Equal(w.Events[j].Timestamp) {
			return w.Events[i].Timestamp.Before(w.Events[j].Timestamp)
		}
		return w.Events[i].Kind < w.Events[j].Kind
	})
	r.rebuildWindowLocked(w)
}

func (r *Registry) rebuildWindowLocked(w *windowState) {
	w.Counters = make(map[string]CounterResult)
	w.Distributions = make(map[string]DistributionResult)
	for _, event := range w.Events {
		switch event.Kind {
		case sampleEvent:
			def := r.metrics[event.Metric]
			key := ResultKey(event.Source, event.Metric)
			switch def.Type {
			case Counter:
				result := w.Counters[key]
				result.Samples++
				previous, hasPrevious := r.previousCounterCheckpointLocked(key, event.Timestamp)
				if hasPrevious {
					resetBetween := r.hasResetBetweenLocked(event.Source, previous.Timestamp, event.Timestamp)
					if resetBetween || event.Value < previous.Value {
						if event.Value < 0 {
							result.NegativeDrops++
						} else {
							result.Resets++
							result.Total += event.Value
						}
					} else {
						result.Total += event.Value - previous.Value
					}
				}
				w.Counters[key] = result
			case Distribution:
				w.Distributions[key] = applyDistribution(w.Distributions[key], def.BucketBounds, event.Value)
			}
		}
	}
}

func (r *Registry) previousCounterCheckpointLocked(key string, at time.Time) (counterCheckpoint, bool) {
	points := r.checkpoints[key]
	idx := sort.Search(len(points), func(i int) bool { return !points[i].Timestamp.Before(at) })
	if idx <= 0 {
		return counterCheckpoint{}, false
	}
	return points[idx-1], true
}

func (r *Registry) hasResetBetweenLocked(sourceID string, after, before time.Time) bool {
	points := r.resetTimes[sourceID]
	idx := sort.Search(len(points), func(i int) bool { return points[i].After(after) })
	return idx < len(points) && !points[idx].After(before)
}

func (r *Registry) appendCounterCheckpointLocked(sourceID, metricName string, at time.Time, value float64) {
	def := r.metrics[metricName]
	if def.Type != Counter {
		return
	}
	key := ResultKey(sourceID, metricName)
	points := r.checkpoints[key]
	insert := sort.Search(len(points), func(i int) bool { return !points[i].Timestamp.Before(at) })
	point := counterCheckpoint{Timestamp: at, Value: value, Reset: r.hasResetAtOrBeforeLocked(sourceID, at)}
	points = append(points, counterCheckpoint{})
	copy(points[insert+1:], points[insert:])
	points[insert] = point
	r.checkpoints[key] = points
}

func (r *Registry) hasResetAtOrBeforeLocked(sourceID string, at time.Time) bool {
	points := r.resetTimes[sourceID]
	idx := sort.Search(len(points), func(i int) bool { return !points[i].Before(at) })
	return idx > 0 || (idx < len(points) && points[idx].Equal(at))
}

func insertResetCheckpointLocked(resetTimes map[string][]time.Time, sourceID string, at time.Time) {
	points := resetTimes[sourceID]
	idx := sort.Search(len(points), func(i int) bool { return !points[i].Before(at) })
	points = append(points, time.Time{})
	copy(points[idx+1:], points[idx:])
	points[idx] = at
	resetTimes[sourceID] = points
}

func (r *Registry) childWindowsSealedLocked(parent *windowState, parentLevel int, now time.Time) bool {
	size := r.levels[parentLevel-1]
	count := int(r.levels[parentLevel] / size)
	for i := 0; i < count; i++ {
		childStart := parent.Start.Add(time.Duration(i) * size)
		child := r.windows[windowKey{Level: parentLevel - 1, Start: childStart}]
		if child == nil {
			if childStart.Add(size).After(now) {
				return false
			}
			continue
		}
		if child.Open {
			return false
		}
	}
	return true
}

func (r *Registry) rollUpLocked(childKey windowKey, child *windowState, parentLevel int) error {
	start := child.Start.Truncate(r.levels[parentLevel])
	parent, err := r.openWindowLocked(parentLevel, start, Definition{}, false)
	if err != nil {
		return err
	}
	if _, ok := parent.RolledChildren[childKey]; !ok {
		mergeWindowResults(parent, child)
		parent.RolledChildren[childKey] = struct{}{}
	}
	return nil
}

func mergeWindowResults(dst, src *windowState) {
	if dst.Counters == nil {
		dst.Counters = make(map[string]CounterResult)
	}
	if dst.Distributions == nil {
		dst.Distributions = make(map[string]DistributionResult)
	}
	for key, value := range src.Counters {
		current := dst.Counters[key]
		current.Samples += value.Samples
		current.Total += value.Total
		current.Resets += value.Resets
		current.NegativeDrops += value.NegativeDrops
		dst.Counters[key] = current
	}
	for key, value := range src.Distributions {
		current := dst.Distributions[key]
		hadCurrent := current.Count > 0
		current.Count += value.Count
		current.Sum += value.Sum
		if value.Count > 0 && !hadCurrent {
			current.Min = value.Min
			current.Max = value.Max
		} else if value.Count > 0 {
			if value.Min < current.Min {
				current.Min = value.Min
			}
			if value.Max > current.Max {
				current.Max = value.Max
			}
		}
		if len(current.Buckets) != len(value.Buckets) {
			current.Buckets = append([]uint64(nil), value.Buckets...)
		} else {
			for i := range value.Buckets {
				current.Buckets[i] += value.Buckets[i]
			}
		}
		dst.Distributions[key] = current
	}
}

func (r *Registry) reviseAncestorsLocked(key windowKey, revised *windowState) error {
	for level := key.Level + 1; level < len(r.levels); level++ {
		parentStart := revised.Start.Truncate(r.levels[level])
		parentKey := windowKey{Level: level, Start: parentStart}
		parent := r.windows[parentKey]
		if parent == nil {
			continue
		}
		newParent := &windowState{
			Level:          level,
			Revision:       parent.Revision + 1,
			Start:          parent.Start,
			End:            parent.End,
			Open:           parent.Open,
			RevisedAt:      r.now().UTC(),
			Reason:         "roll-up revision",
			RolledChildren: make(map[windowKey]struct{}),
			Counters:       make(map[string]CounterResult),
			Distributions:  make(map[string]DistributionResult),
		}
		childSize := r.levels[level-1]
		count := int(r.levels[level] / childSize)
		for i := 0; i < count; i++ {
			childStart := parent.Start.Add(time.Duration(i) * childSize)
			child := r.windows[windowKey{Level: level - 1, Start: childStart}]
			if child != nil {
				mergeWindowResults(newParent, child)
			}
		}
		r.history[parentKey] = append(r.history[parentKey], parent)
		parent.Superseded = true
		r.windows[parentKey] = newParent
		revised = newParent
		r.stats.WindowRevisions++
	}
	return nil
}

func cloneWindow(w *windowState) *windowState {
	clone := &windowState{
		Level: w.Level, Revision: w.Revision, Start: w.Start, End: w.End, Open: w.Open,
		Superseded: w.Superseded, RevisedAt: w.RevisedAt, Reason: w.Reason,
		Events:         append([]windowEvent(nil), w.Events...),
		RolledChildren: make(map[windowKey]struct{}, len(w.RolledChildren)),
		Counters:       make(map[string]CounterResult, len(w.Counters)),
		Distributions:  make(map[string]DistributionResult, len(w.Distributions)),
	}
	for key, value := range w.Counters {
		clone.Counters[key] = value
	}
	for key, value := range w.Distributions {
		value.Buckets = append([]uint64(nil), value.Buckets...)
		clone.Distributions[key] = value
	}
	for childKey := range w.RolledChildren {
		clone.RolledChildren[childKey] = struct{}{}
	}
	return clone
}
