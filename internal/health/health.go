package health

import (
	"context"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/types"
)

// State is the health state of a node.
type State string

const (
	StateHealthy   State = "healthy"
	StateSuspected State = "suspected"
	StateEjected   State = "ejected"
)

// Status is a snapshot of one node's health.
type Status struct {
	NodeID         string        `json:"node_id"`
	State          State         `json:"state"`
	ConsecutiveBad int           `json:"consecutive_bad"`
	ProbeStreak    int           `json:"probe_streak"`
	Successes      int64         `json:"successes"`
	Failures       int64         `json:"failures"`
	LastCode       types.ErrKind `json:"last_error"`
	EjectedAt      time.Time     `json:"ejected_at"`
}

// Config controls ejection / recovery thresholds and probing.
type Config struct {
	// MaxConsecutiveFail ejects a node after this many consecutive failures.
	MaxConsecutiveFail int
	// ProbeInterval is how long an ejected node waits between recovery probes.
	ProbeInterval time.Duration
	// ProbeSuccessNeeded is consecutive good probes required for recovery.
	ProbeSuccessNeeded int
	// Logger receives state transitions. Nil uses the default logger.
	Logger *log.Logger
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		MaxConsecutiveFail: 3,
		ProbeInterval:      20 * time.Millisecond,
		ProbeSuccessNeeded: 1,
	}
}

type nodeState struct {
	Status
}

// Monitor keeps health state for a set of nodes.
type Monitor struct {
	cfg   Config
	log   *log.Logger
	mu    sync.Mutex
	nodes map[string]*nodeState
	order []string
}

// NewMonitor builds a monitor for the given node ids.
func NewMonitor(ids []string, cfg Config) *Monitor {
	if cfg.MaxConsecutiveFail <= 0 {
		cfg = DefaultConfig()
	}
	if cfg.ProbeInterval <= 0 {
		cfg.ProbeInterval = 20 * time.Millisecond
	}
	if cfg.ProbeSuccessNeeded <= 0 {
		cfg.ProbeSuccessNeeded = 1
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	}
	m := &Monitor{cfg: cfg, log: logger, nodes: map[string]*nodeState{}}
	for _, id := range ids {
		if _, ok := m.nodes[id]; ok {
			continue
		}
		m.nodes[id] = &nodeState{Status: Status{NodeID: id, State: StateHealthy}}
		m.order = append(m.order, id)
	}
	return m
}

func (m *Monitor) snapshotLocked(id string) Status { return m.nodes[id].Status }

// RecordSuccess records one good interaction.
func (m *Monitor) RecordSuccess(id string) {
	m.mu.Lock()
	n, ok := m.nodes[id]
	if !ok {
		m.mu.Unlock()
		return
	}
	old := n.State
	n.Successes++
	n.ConsecutiveBad = 0
	n.LastCode = types.ErrNone
	if old == StateSuspected {
		n.State = StateHealthy
	}
	m.mu.Unlock()
	if old == StateSuspected {
		m.log.Printf("health: node=%s state %s -> %s reason=success_recovered", id, old, StateHealthy)
	}
}

// RecordFailure records one classified failure and returns the resulting state.
func (m *Monitor) RecordFailure(id string, kind types.ErrKind) State {
	m.mu.Lock()
	n, ok := m.nodes[id]
	if !ok {
		m.mu.Unlock()
		return ""
	}
	old := n.State
	n.Failures++
	n.ConsecutiveBad++
	n.LastCode = kind
	result := old
	// Ejected nodes only recover through probes, not traffic.
	if old != StateEjected {
		switch {
		case n.ConsecutiveBad >= m.cfg.MaxConsecutiveFail:
			n.State = StateEjected
			n.EjectedAt = time.Now()
			n.ProbeStreak = 0
			result = StateEjected
		case n.ConsecutiveBad > 0 && old == StateHealthy:
			n.State = StateSuspected
			result = StateSuspected
		}
	}
	m.mu.Unlock()
	if result != old {
		m.log.Printf("health: node=%s state %s -> %s reason=consecutive_failures=%d last_error=%s",
			id, old, result, n.ConsecutiveBad, kind)
	} else {
		m.log.Printf("health: node=%s state=%s failure_recorded consecutive=%d last_error=%s",
			id, old, n.ConsecutiveBad, kind)
	}
	return result
}

// Pick selects the best currently available (non-ejected) node:
// fewer consecutive failures first, then more historical successes.
func (m *Monitor) Pick() (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	candidates := make([]*nodeState, 0, len(m.nodes))
	for _, id := range m.order {
		if n := m.nodes[id]; n.State != StateEjected {
			candidates = append(candidates, n)
		}
	}
	if len(candidates) == 0 {
		return "", false
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].ConsecutiveBad != candidates[j].ConsecutiveBad {
			return candidates[i].ConsecutiveBad < candidates[j].ConsecutiveBad
		}
		if candidates[i].Successes != candidates[j].Successes {
			return candidates[i].Successes > candidates[j].Successes
		}
		return candidates[i].NodeID < candidates[j].NodeID
	})
	return candidates[0].NodeID, true
}

// EjectedIDs returns the ids of currently ejected nodes.
func (m *Monitor) EjectedIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, id := range m.order {
		if m.nodes[id].State == StateEjected {
			out = append(out, id)
		}
	}
	return out
}

// StatusOf returns a copy of one node's status.
func (m *Monitor) StatusOf(id string) (Status, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[id]
	if !ok {
		return Status{}, false
	}
	return n.Status, true
}

// DueProbes returns ejected nodes whose recovery probe is due.
func (m *Monitor) DueProbes(now time.Time) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, id := range m.order {
		n := m.nodes[id]
		if n.State == StateEjected && !n.EjectedAt.IsZero() &&
			now.Sub(n.EjectedAt) >= m.cfg.ProbeInterval {
			out = append(out, id)
		}
	}
	return out
}

// MarkProbeResult records one recovery probe. A successful streak restores the
// node; a failure resets the streak and pushes back the next probe.
func (m *Monitor) MarkProbeResult(id string, ok bool) State {
	m.mu.Lock()
	n, exists := m.nodes[id]
	if !exists {
		m.mu.Unlock()
		return ""
	}
	old := n.State
	if !n.EjectedAt.IsZero() {
		n.EjectedAt = time.Now() // arm the interval regardless of outcome
	}
	if ok {
		n.ProbeStreak++
		if n.ProbeStreak >= m.cfg.ProbeSuccessNeeded {
			n.State = StateHealthy
			n.ConsecutiveBad = 0
			n.ProbeStreak = 0
			n.LastCode = types.ErrNone
		}
	} else {
		n.ProbeStreak = 0
	}
	result := n.State
	m.mu.Unlock()
	m.log.Printf("health: node=%s probe ok=%v streak_needed=%d state %s -> %s",
		id, ok, m.cfg.ProbeSuccessNeeded, old, result)
	return result
}

// Snapshot returns a copy of all node statuses in registration order.
func (m *Monitor) Snapshot() []Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Status, 0, len(m.order))
	for _, id := range m.order {
		out = append(out, m.nodes[id].Status)
	}
	return out
}

// RunProbeLoop periodically probes ejected nodes with fn until ctx is cancelled.
func (m *Monitor) RunProbeLoop(ctx context.Context, fn func(ctx context.Context, id string) error) {
	interval := m.cfg.ProbeInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			for _, id := range m.DueProbes(now) {
				if ctx.Err() != nil {
					return
				}
				err := fn(ctx, id)
				m.MarkProbeResult(id, err == nil)
			}
		}
	}
}
