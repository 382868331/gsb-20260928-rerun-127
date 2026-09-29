package metriccore

import (
	"encoding/json"
	"io"
	"time"
)

const checkpointVersion = 1

type checkpoint struct {
	Version       int                     `json:"version"`
	Config        Config                  `json:"config"`
	Metrics       map[string]Definition   `json:"metrics"`
	Sources       map[string]Source       `json:"sources"`
	Windows       []checkpointWindow      `json:"windows"`
	LastValues    map[string]counterState `json:"last_values"`
	Seen          map[string]time.Time    `json:"seen"`
	ClosedThrough time.Time               `json:"closed_through"`
	Stats         checkpointStats         `json:"stats"`
}

type checkpointWindow struct {
	Start         time.Time                     `json:"start"`
	End           time.Time                     `json:"end"`
	Open          bool                          `json:"open"`
	Counters      map[string]CounterResult      `json:"counters,omitempty"`
	Distributions map[string]DistributionResult `json:"distributions,omitempty"`
}

type checkpointStats struct {
	DuplicateSamples int64  `json:"duplicate_samples"`
	LateSamples      int64  `json:"late_samples"`
	LimitErrors      int64  `json:"limit_errors"`
	LastLimitError   string `json:"last_limit_error,omitempty"`
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
	r.sources = state.Sources
	if r.sources == nil {
		r.sources = make(map[string]Source)
	}
	r.windows = make(map[time.Time]*windowState, len(state.Windows))
	for _, saved := range state.Windows {
		w := &windowState{
			Start:         saved.Start,
			End:           saved.End,
			Open:          saved.Open,
			Counters:      saved.Counters,
			Distributions: saved.Distributions,
		}
		if w.Counters == nil {
			w.Counters = make(map[string]CounterResult)
		}
		if w.Distributions == nil {
			w.Distributions = make(map[string]DistributionResult)
		}
		r.windows[saved.Start] = w
	}
	r.lastValue = state.LastValues
	if r.lastValue == nil {
		r.lastValue = make(map[string]counterState)
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
	r.refreshMemoryLocked()
	return r, nil
}

func (r *Registry) checkpointLocked() checkpoint {
	windows := make([]checkpointWindow, 0, len(r.windows))
	for _, w := range r.windows {
		windows = append(windows, checkpointWindow{
			Start:         w.Start,
			End:           w.End,
			Open:          w.Open,
			Counters:      w.Counters,
			Distributions: w.Distributions,
		})
	}
	return checkpoint{
		Version:       checkpointVersion,
		Config:        r.cfg,
		Metrics:       r.metrics,
		Sources:       r.sources,
		Windows:       windows,
		LastValues:    r.lastValue,
		Seen:          r.seen,
		ClosedThrough: r.closedThrough,
		Stats: checkpointStats{
			DuplicateSamples: r.stats.DuplicateSamples,
			LateSamples:      r.stats.LateSamples,
			LimitErrors:      r.stats.LimitErrors,
			LastLimitError:   r.stats.LastLimitError,
		},
	}
}
