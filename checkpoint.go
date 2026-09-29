package metrics

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const checkpointVersion = 1

// checkpointData is the on-disk JSON representation of an Aggregator.
type checkpointData struct {
	Version     int
	WindowNS    int64
	MaxSources  int
	MaxMemory   int
	Definitions []Definition
	Sources     []string
	Windows     []windowData
	Cursors     []cursorData
	Seen        []string
	Stats       Stats
}

type windowData struct {
	Index   int64
	StartNS int64
	Closed  bool
	Metrics map[string]*metricAgg
}

type cursorData struct {
	Source string
	Metric string
	Seen   bool
	Value  float64
}

// SaveCheckpoint writes a self-contained snapshot. No monitoring service or
// external database is involved.
func (a *Aggregator) SaveCheckpoint(w io.Writer) error {
	data := checkpointData{
		Version:     checkpointVersion,
		WindowNS:    int64(a.windowSize),
		MaxSources:  a.maxSources,
		MaxMemory:   a.maxMemory,
		Definitions: make([]Definition, 0, len(a.definitions)),
		Sources:     make([]string, 0, len(a.sources)),
		Windows:     make([]windowData, 0, len(a.windows)),
		Cursors:     make([]cursorData, 0, len(a.cursors)),
		Seen:        make([]string, 0, len(a.seen)),
		Stats:       a.stats,
	}
	for _, d := range a.definitions {
		data.Definitions = append(data.Definitions, d)
	}
	for source := range a.sources {
		data.Sources = append(data.Sources, source)
	}
	for idx, w := range a.windows {
		data.Windows = append(data.Windows, windowData{
			Index:   idx,
			StartNS: w.start.UnixNano(),
			Closed:  w.closed,
			Metrics: w.metrics,
		})
	}
	for key, cur := range a.cursors {
		source, metric, _ := splitCursorKey(key)
		data.Cursors = append(data.Cursors, cursorData{
			Source: source,
			Metric: metric,
			Seen:   cur.seen,
			Value:  cur.value,
		})
	}
	for fingerprint := range a.seen {
		data.Seen = append(data.Seen, fingerprint)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(&data)
}

// LoadCheckpoint restores an Aggregator previously written with
// SaveCheckpoint.
func LoadCheckpoint(r io.Reader) (*Aggregator, error) {
	var data checkpointData
	if err := json.NewDecoder(r).Decode(&data); err != nil {
		return nil, err
	}
	if data.Version != checkpointVersion {
		return nil, errCheckpointVersion(data.Version)
	}
	a, err := New(Config{
		WindowSize:       time.Duration(data.WindowNS),
		MaxSources:       data.MaxSources,
		MaxMemoryEntries: data.MaxMemory,
	}, data.Definitions...)
	if err != nil {
		return nil, err
	}
	for _, source := range data.Sources {
		a.sources[source] = struct{}{}
	}
	for _, wd := range data.Windows {
		aggs := make(map[string]*metricAgg, len(wd.Metrics))
		for name, value := range wd.Metrics {
			aggs[name] = value
		}
		a.windows[wd.Index] = &windowState{
			start:   time.Unix(0, wd.StartNS),
			closed:  wd.Closed,
			metrics: aggs,
		}
	}
	for _, c := range data.Cursors {
		a.cursors[c.Source+"\x00"+c.Metric] = counterCursor{
			seen:  c.Seen,
			value: c.Value,
		}
	}
	for _, fingerprint := range data.Seen {
		a.seen[fingerprint] = struct{}{}
	}
	a.stats = data.Stats
	return a, nil
}

func splitCursorKey(key string) (source, metric string, ok bool) {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			return key[:i], key[i+1:], true
		}
	}
	return "", "", false
}

func errCheckpointVersion(version int) error {
	return fmt.Errorf("metrics: unsupported checkpoint version %d (want %d)", version, checkpointVersion)
}
