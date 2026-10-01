package metriccore

import (
	"encoding/json"
	"io"
	"time"
)

const checkpointVersion = 2

type checkpoint struct {
	Version       int                     `json:"version"`
	Config        Config                  `json:"config"`
	Metrics       map[string]Definition   `json:"metrics"`
	Sources       []checkpointSource      `json:"sources"`
	Windows       []checkpointWindowSet   `json:"windows"`
	CounterBases  []checkpointCounterBase `json:"counter_bases"`
	SourceResets  []checkpointReset       `json:"source_resets,omitempty"`
	Seen          map[string]time.Time    `json:"seen"`
	ClosedThrough time.Time               `json:"closed_through"`
	Stats         checkpointStats         `json:"stats"`
	Rejections    []Rejection             `json:"rejections,omitempty"`
}

type checkpointSource struct {
	Source         Source       `json:"source"`
	Status         SourceStatus `json:"status"`
	WithdrawnAt    time.Time    `json:"withdrawn_at,omitempty"`
	WithdrawReason string       `json:"withdraw_reason,omitempty"`
}

type checkpointWindowSet struct {
	Level     int                `json:"level"`
	Start     time.Time          `json:"start"`
	Revisions []checkpointWindow `json:"revisions"`
	Current   checkpointWindow   `json:"current"`
}

type checkpointWindow struct {
	Start          time.Time                     `json:"start"`
	End            time.Time                     `json:"end"`
	Open           bool                          `json:"open"`
	Level          int                           `json:"level"`
	Revision       int                           `json:"revision"`
	Superseded     bool                          `json:"superseded,omitempty"`
	RevisedAt      time.Time                     `json:"revised_at,omitempty"`
	Reason         string                        `json:"reason,omitempty"`
	Events         []checkpointEvent             `json:"events,omitempty"`
	RolledChildren []checkpointWindowKey         `json:"rolled_children,omitempty"`
	Counters       map[string]CounterResult      `json:"counters,omitempty"`
	Distributions  map[string]DistributionResult `json:"distributions,omitempty"`
}

type checkpointEvent struct {
	Kind      eventKind `json:"kind"`
	ID        string    `json:"id,omitempty"`
	Metric    string    `json:"metric,omitempty"`
	Source    string    `json:"source,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value,omitempty"`
	Reason    string    `json:"reason,omitempty"`
}

type checkpointWindowKey struct {
	Level int       `json:"level"`
	Start time.Time `json:"start"`
}

type checkpointCounterPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
	Reset     bool      `json:"reset"`
}

type checkpointCounterBase struct {
	Source string                   `json:"source"`
	Metric string                   `json:"metric"`
	Points []checkpointCounterPoint `json:"points"`
}

type checkpointReset struct {
	Source string      `json:"source"`
	Times  []time.Time `json:"times"`
}

type checkpointStats struct {
	DuplicateSamples  int64  `json:"duplicate_samples"`
	LateSamples       int64  `json:"late_samples"`
	LimitErrors       int64  `json:"limit_errors"`
	LastLimitError    string `json:"last_limit_error,omitempty"`
	WindowRevisions   int64  `json:"window_revisions"`
	DroppedRejections int64  `json:"dropped_rejections"`
}

func (r *Registry) Save(writer io.Writer) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return json.NewEncoder(writer).Encode(r.checkpointLocked())
}

func Load(reader io.Reader) (*Registry, error) {
	var state checkpoint
	if err := json.NewDecoder(reader).Decode(&state); err != nil {
		return nil, err
	}
	if state.Version != checkpointVersion {
		return nil, ErrUnsupportedCheckpoint
	}
	r := NewRegistry(state.Config)
	r.metrics = state.Metrics
	if r.metrics == nil {
		r.metrics = make(map[string]Definition)
	}
	r.sources = make(map[string]*sourceState, len(state.Sources))
	for _, saved := range state.Sources {
		status := saved.Status
		if status == "" {
			status = SourceActive
		}
		r.sources[saved.Source.ID] = &sourceState{
			Source: cloneSource(saved.Source), Status: status,
			WithdrawnAt: saved.WithdrawnAt, WithdrawReason: saved.WithdrawReason,
		}
	}
	r.windows = make(map[windowKey]*windowState)
	r.history = make(map[windowKey][]*windowState)
	for _, set := range state.Windows {
		key := windowKey{Level: set.Level, Start: set.Start}
		for _, saved := range set.Revisions {
			r.history[key] = append(r.history[key], loadWindow(saved))
		}
		r.windows[key] = loadWindow(set.Current)
	}
	r.checkpoints = make(map[string][]counterCheckpoint)
	for _, base := range state.CounterBases {
		key := ResultKey(base.Source, base.Metric)
		for _, point := range base.Points {
			r.checkpoints[key] = append(r.checkpoints[key], counterCheckpoint{
				Timestamp: point.Timestamp, Value: point.Value, Reset: point.Reset,
			})
		}
	}
	r.resetTimes = make(map[string][]time.Time)
	for _, reset := range state.SourceResets {
		r.resetTimes[reset.Source] = append([]time.Time(nil), reset.Times...)
	}
	r.seen = state.Seen
	if r.seen == nil {
		r.seen = make(map[string]time.Time)
	}
	r.closedThrough = state.ClosedThrough
	r.stats.DuplicateSamples = state.Stats.DuplicateSamples
	r.stats.LateSamples = state.Stats.LateSamples
	r.stats.LimitErrors = state.Stats.LimitErrors
	r.stats.LastLimitError = state.Stats.LastLimitError
	r.stats.WindowRevisions = state.Stats.WindowRevisions
	r.stats.DroppedRejections = state.Stats.DroppedRejections
	r.rejections = append([]Rejection(nil), state.Rejections...)
	r.stats.RejectionRecords = int64(len(r.rejections))
	r.refreshMemoryLocked()
	return r, nil
}

func loadWindow(saved checkpointWindow) *windowState {
	w := &windowState{
		Start: saved.Start, End: saved.End, Open: saved.Open, Level: saved.Level,
		Revision: saved.Revision, Superseded: saved.Superseded, RevisedAt: saved.RevisedAt,
		Reason:         saved.Reason,
		Events:         make([]windowEvent, 0, len(saved.Events)),
		RolledChildren: make(map[windowKey]struct{}, len(saved.RolledChildren)),
		Counters:       saved.Counters,
		Distributions:  saved.Distributions,
	}
	if w.Revision == 0 {
		w.Revision = 1
	}
	if w.Counters == nil {
		w.Counters = make(map[string]CounterResult)
	}
	if w.Distributions == nil {
		w.Distributions = make(map[string]DistributionResult)
	}
	for _, event := range saved.Events {
		w.Events = append(w.Events, windowEvent{
			Kind: event.Kind, ID: event.ID, Metric: event.Metric, Source: event.Source,
			Timestamp: event.Timestamp, Value: event.Value, Reason: event.Reason,
		})
	}
	for _, child := range saved.RolledChildren {
		w.RolledChildren[windowKey{Level: child.Level, Start: child.Start}] = struct{}{}
	}
	return w
}

func (r *Registry) checkpointLocked() checkpoint {
	setsByKey := make(map[windowKey]checkpointWindowSet)
	keys := make([]windowKey, 0, len(r.windows))
	for key, w := range r.windows {
		setsByKey[key] = checkpointWindowSet{Level: key.Level, Start: key.Start, Current: saveWindow(w)}
		keys = append(keys, key)
	}
	for key, revisions := range r.history {
		set := setsByKey[key]
		set.Level = key.Level
		set.Start = key.Start
		for _, w := range revisions {
			set.Revisions = append(set.Revisions, saveWindow(w))
		}
		setsByKey[key] = set
	}
	windows := make([]checkpointWindowSet, 0, len(keys))
	for _, key := range keys {
		windows = append(windows, setsByKey[key])
	}
	sources := make([]checkpointSource, 0, len(r.sources))
	for _, state := range r.sources {
		sources = append(sources, checkpointSource{
			Source: state.Source, Status: state.Status,
			WithdrawnAt: state.WithdrawnAt, WithdrawReason: state.WithdrawReason,
		})
	}
	bases := make([]checkpointCounterBase, 0, len(r.checkpoints))
	for key, points := range r.checkpoints {
		source, metric, _ := ParseResultKey(key)
		saved := checkpointCounterBase{Source: source, Metric: metric}
		for _, point := range points {
			saved.Points = append(saved.Points, checkpointCounterPoint{
				Timestamp: point.Timestamp, Value: point.Value, Reset: point.Reset,
			})
		}
		bases = append(bases, saved)
	}
	resets := make([]checkpointReset, 0, len(r.resetTimes))
	for source, times := range r.resetTimes {
		resets = append(resets, checkpointReset{Source: source, Times: append([]time.Time(nil), times...)})
	}
	return checkpoint{
		Version: checkpointVersion, Config: r.cfg, Metrics: r.metrics, Sources: sources,
		Windows: windows, CounterBases: bases, SourceResets: resets,
		Seen: r.seen, ClosedThrough: r.closedThrough,
		Stats: checkpointStats{
			DuplicateSamples: r.stats.DuplicateSamples, LateSamples: r.stats.LateSamples,
			LimitErrors: r.stats.LimitErrors, LastLimitError: r.stats.LastLimitError,
			WindowRevisions: r.stats.WindowRevisions, DroppedRejections: r.stats.DroppedRejections,
		},
		Rejections: append([]Rejection(nil), r.rejections...),
	}
}

func saveWindow(w *windowState) checkpointWindow {
	saved := checkpointWindow{
		Start: w.Start, End: w.End, Open: w.Open, Level: w.Level, Revision: w.Revision,
		Superseded: w.Superseded, RevisedAt: w.RevisedAt, Reason: w.Reason,
		Events:         make([]checkpointEvent, 0, len(w.Events)),
		RolledChildren: make([]checkpointWindowKey, 0, len(w.RolledChildren)),
		Counters:       w.Counters,
		Distributions:  w.Distributions,
	}
	for _, event := range w.Events {
		saved.Events = append(saved.Events, checkpointEvent{
			Kind: event.Kind, ID: event.ID, Metric: event.Metric, Source: event.Source,
			Timestamp: event.Timestamp, Value: event.Value, Reason: event.Reason,
		})
	}
	for child := range w.RolledChildren {
		saved.RolledChildren = append(saved.RolledChildren, checkpointWindowKey{Level: child.Level, Start: child.Start})
	}
	return saved
}
