package pool

import (
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Backend struct {
	Address           string
	ActiveConnections int
	Healthy           bool
	Failures          int
	State             BreakerState
	TripTime          time.Time
	ProbeStart        time.Time
}

type BackendPool struct {
	mu            sync.Mutex
	Backends      []Backend
	RoundRobinIdx int
	Algorithm     string
	CbMaxFailures int
	CbCooldown    time.Duration
}
type BreakerState string

const (
	StateClosed   BreakerState = "closed"
	StateOpen     BreakerState = "open"
	StateHalfOpen BreakerState = "half-open"
)

var (
	activeConnsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "lb_active_connections",
		Help: "Current number of active connections per backend",
	}, []string{"backend"})

	backendHealthGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "lb_backend_health",
		Help: "Health status of backends (1 = healthy, 0 = unhealthy)",
	}, []string{"backend"})

	cbStateGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "lb_cb_state",
		Help: "Circuit breaker state (0 = closed, 1 = half open, 2 = open)",
	}, []string{"backend"})
)

type BackendStatus struct {
	Address           string `json:"address"`
	Healthy           bool   `json:"healthy"`
	ActiveConnections int    `json:"active_connections"`
	State             string `json:"circuit_breaker_state"`
	Failures          int    `json:"failures"`
}

type PoolStatus struct {
	TotalBackends int             `json:"total_backends"`
	HealthyCount  int             `json:"total_healthy"`
	Backends      []BackendStatus `json:"backends"`
}

func (p *BackendPool) GetStatusSnapshot() PoolStatus {
	p.mu.Lock()
	defer p.mu.Unlock()

	status := PoolStatus{
		TotalBackends: len(p.Backends),
		Backends:      make([]BackendStatus, 0, len(p.Backends)),
	}

	for _, b := range p.Backends {
		if b.Healthy {
			status.HealthyCount++
		}
		status.Backends = append(status.Backends, BackendStatus{
			Address:           b.Address,
			Healthy:           b.Healthy,
			ActiveConnections: b.ActiveConnections,
			State:             string(b.State),
			Failures:          b.Failures,
		})
	}
	return status
}

func (p *BackendPool) AcquireBackend() *Backend {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	isEligible := func(b *Backend) bool {
		if !b.Healthy {
			return false
		}
		if b.State == StateOpen {
			// if time.Since(b.TripTime) > p.CbCooldown {
			// 	b.State = StateHalfOpen
			// 	cbStateGauge.WithLabelValues(b.Address).Set(1)
			// 	slog.Info("circuit breaker HALF-OPEN ", "backend", b.Address)
			// 	return true
			// }
			// return false
			return now.Sub(b.TripTime) >= p.CbCooldown
		}
		if b.State == StateHalfOpen {
			// A probe is already in flight. Deny all other requests until the probe finishes.
			// return false
			return now.Sub(b.ProbeStart) >= p.CbCooldown // Watchdog: recover if a probe gets lost
		}
		return true
	}
	var backend *Backend
	if p.Algorithm == "round_robin" {
		for i := 0; i < len(p.Backends); i++ {
			idx := (p.RoundRobinIdx + i) % len(p.Backends)
			if isEligible(&p.Backends[idx]) {
				backend = &p.Backends[idx]
				p.RoundRobinIdx = (idx + 1) % len(p.Backends)
				break
			}
		}
	} else {
		for i := range p.Backends {
			if !isEligible(&p.Backends[i]) {
				continue
			}

			if backend == nil || backend.ActiveConnections > p.Backends[i].ActiveConnections {
				backend = &p.Backends[i]
			}
		}
	}

	if backend == nil {
		return nil
	}

	if backend.State != StateClosed {
		backend.State = StateHalfOpen
		backend.ProbeStart = now
		cbStateGauge.WithLabelValues(backend.Address).Set(1)
		slog.Info("circuit breaker HALF-OPEN, probe dispatched", "backend", backend.Address)
	}
	backend.ActiveConnections += 1
	activeConnsGauge.WithLabelValues(backend.Address).Inc()
	return backend
}

func (p *BackendPool) ReleaseBackend(backend *Backend) {
	p.mu.Lock()
	defer p.mu.Unlock()
	backend.ActiveConnections--
	activeConnsGauge.WithLabelValues(backend.Address).Dec()
}

func (p *BackendPool) SetHealth(backend *Backend, health bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	backend.Healthy = health
	if health {
		backendHealthGauge.WithLabelValues(backend.Address).Set(1)
	} else {
		backendHealthGauge.WithLabelValues(backend.Address).Set(0)
	}
}

func (p *BackendPool) RecordSuccess(backend *Backend) {
	p.mu.Lock()
	defer p.mu.Unlock()
	backend.Failures = 0
	backend.State = StateClosed
	cbStateGauge.WithLabelValues(backend.Address).Set(0)
}

func (p *BackendPool) RecordFailure(backend *Backend) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if backend.State == StateOpen {
		return
	}

	backend.Failures++
	if backend.Failures >= p.CbMaxFailures {
		backend.State = StateOpen
		backend.TripTime = time.Now()
		slog.Warn("circuit breaker tripped OPEN", "backend", backend.Address)
		cbStateGauge.WithLabelValues(backend.Address).Set(2)
	}

}
