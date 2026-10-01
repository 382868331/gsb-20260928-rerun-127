package metriccore

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type MetricType string

const (
	Counter      MetricType = "counter"
	Distribution MetricType = "distribution"
)

type Definition struct {
	Name         string     `json:"name"`
	Type         MetricType `json:"type"`
	Description  string     `json:"description,omitempty"`
	BucketBounds []float64  `json:"bucket_bounds,omitempty"`
}

type Source struct {
	ID     string            `json:"id"`
	Labels map[string]string `json:"labels,omitempty"`
}

type Sample struct {
	ID        string    `json:"id"`
	Metric    string    `json:"metric"`
	Source    string    `json:"source"`
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

type SourceStatus string

const (
	SourceActive    SourceStatus = "active"
	SourceWithdrawn SourceStatus = "withdrawn"
)

type sourceState struct {
	Source         Source       `json:"source"`
	Status         SourceStatus `json:"status"`
	WithdrawnAt    time.Time    `json:"withdrawn_at,omitempty"`
	WithdrawReason string       `json:"withdraw_reason,omitempty"`
}

type Rejection struct {
	At         time.Time `json:"at"`
	SampleID   string    `json:"sample_id,omitempty"`
	Metric     string    `json:"metric,omitempty"`
	Source     string    `json:"source,omitempty"`
	Timestamp  time.Time `json:"sample_timestamp,omitempty"`
	ReasonCode string    `json:"reason_code"`
	Message    string    `json:"message"`
}

type CounterResult struct {
	Samples       int64   `json:"samples"`
	Total         float64 `json:"total"`
	Resets        int64   `json:"resets"`
	NegativeDrops int64   `json:"negative_drops"`
}

type DistributionResult struct {
	Count   int64    `json:"count"`
	Sum     float64  `json:"sum"`
	Min     float64  `json:"min"`
	Max     float64  `json:"max"`
	Buckets []uint64 `json:"buckets"`
}

type WindowResult struct {
	Level         int                           `json:"level"`
	Revision      int                           `json:"revision"`
	Start         time.Time                     `json:"start"`
	End           time.Time                     `json:"end"`
	Open          bool                          `json:"open"`
	Superseded    bool                          `json:"superseded,omitempty"`
	RevisedAt     time.Time                     `json:"revised_at,omitempty"`
	Reason        string                        `json:"reason,omitempty"`
	Counters      map[string]CounterResult      `json:"counters,omitempty"`
	Distributions map[string]DistributionResult `json:"distributions,omitempty"`
}

type Stats struct {
	Metrics              int64  `json:"metrics"`
	Sources              int64  `json:"sources"`
	Windows              int64  `json:"windows"`
	WindowRevisions      int64  `json:"window_revisions"`
	DedupeEntries        int64  `json:"dedupe_entries"`
	EstimatedMemoryBytes int64  `json:"estimated_memory_bytes"`
	DuplicateSamples     int64  `json:"duplicate_samples"`
	LateSamples          int64  `json:"late_samples"`
	LimitErrors          int64  `json:"limit_errors"`
	LastLimitError       string `json:"last_limit_error,omitempty"`
	RejectionRecords     int64  `json:"rejection_records"`
	DroppedRejections    int64  `json:"dropped_rejections"`
}

func (d Definition) validate() error {
	if strings.TrimSpace(d.Name) == "" {
		return errors.New("metric name is required")
	}
	switch d.Type {
	case Counter:
		if len(d.BucketBounds) != 0 {
			return fmt.Errorf("counter metric %q cannot define buckets", d.Name)
		}
	case Distribution:
		if len(d.BucketBounds) == 0 {
			return fmt.Errorf("distribution metric %q requires bucket bounds", d.Name)
		}
		for i, bound := range d.BucketBounds {
			if i > 0 && bound <= d.BucketBounds[i-1] {
				return fmt.Errorf("distribution metric %q buckets must be strictly increasing", d.Name)
			}
		}
	default:
		return fmt.Errorf("unsupported metric type %q", d.Type)
	}
	return nil
}

func (d Definition) normalized() Definition {
	if d.Type != Distribution || len(d.BucketBounds) == 0 {
		return d
	}
	copyBounds := append([]float64(nil), d.BucketBounds...)
	sort.Float64s(copyBounds)
	d.BucketBounds = copyBounds
	return d
}

func (s Sample) validate() error {
	if strings.TrimSpace(s.ID) == "" {
		return errors.New("sample id is required")
	}
	if strings.TrimSpace(s.Metric) == "" {
		return errors.New("metric name is required")
	}
	if strings.TrimSpace(s.Source) == "" {
		return errors.New("source id is required")
	}
	if s.Timestamp.IsZero() {
		return errors.New("sample timestamp is required")
	}
	return nil
}
