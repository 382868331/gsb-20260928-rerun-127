package metriccore

import "time"

func (r *Registry) SetClock(now func() time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if now == nil {
		r.now = time.Now
		return
	}
	r.now = now
}
