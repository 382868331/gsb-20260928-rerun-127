package metriccore

import "errors"

var (
	ErrMetricNotFound        = errors.New("metric not found")
	ErrSourceNotFound        = errors.New("source not found")
	ErrDuplicateSample       = errors.New("duplicate sample")
	ErrLateSample            = errors.New("sample belongs to a closed window")
	ErrFutureSample          = errors.New("sample timestamp is in the future")
	ErrSourceLimit           = errors.New("source limit reached")
	ErrMetricLimit           = errors.New("metric limit reached")
	ErrMemoryLimit           = errors.New("memory limit reached")
	ErrUnsupportedCheckpoint = errors.New("unsupported checkpoint version")
)
