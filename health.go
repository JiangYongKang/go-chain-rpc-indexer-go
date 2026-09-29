package indexer

import (
	"context"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"
)

// NodeState 是节点在健康管理器中的状态。
type NodeState int

const (
	// StateHealthy 正常服务。
	StateHealthy NodeState = iota
	// StateSuspect 可疑：仍可服务，但排序靠后；达到失败阈值会被摘除。
	StateSuspect
	// StateEjected 已摘除：不参与选节点，只接受定期恢复探测。
	StateEjected
)

func (s NodeState) String() string {
	switch s {
	case StateSuspect:
		return "suspect"
	case StateEjected:
		return "ejected"
	default:
		return "healthy"
	}
}

// NodeStatus 是节点健康状态的快照。
type NodeStatus struct {
	State              NodeState
	ConsecutiveFailure int
	ProbeStreak        int
	Successes          int64
	LatencyEWMA        time.Duration
	LastError          string
	LastChange         time.Time
}

// 健康判定参数。
const (
	ewmaAlpha        = 0.3 // 延迟 EWMA 平滑系数
	recoverThreshold = 2   // 摘除后连续探测成功多少次才恢复
	probeTimeout     = 2 * time.Second
)

type nodeHealth struct {
	client   NodeClient
	status   NodeStatus
	inflight int // 当前通过 Select 选出且尚未 RecordResult 的请求数
}

// HealthManager 负责节点健康度打分、摘除与恢复探测。
type HealthManager struct {
	mu               sync.Mutex
	nodes            map[string]*nodeHealth
	failureThreshold int
	probeInterval    time.Duration
	log              *slog.Logger
}

// NewHealthManager 创建健康管理器。所有节点初始为 healthy。
func NewHealthManager(nodes []NodeClient, failureThreshold int, probeInterval time.Duration, log *slog.Logger) *HealthManager {
	m := &HealthManager{
		nodes:            make(map[string]*nodeHealth, len(nodes)),
		failureThreshold: failureThreshold,
		probeInterval:    probeInterval,
		log:              log,
	}
	now := time.Now()
	for _, c := range nodes {
		m.nodes[c.Name()] = &nodeHealth{
			client: c,
			status: NodeStatus{State: StateHealthy, LastChange: now},
		}
	}
	return m
}

// StartProbeLoop 启动后台恢复探测循环，直到 ctx 取消。
func (m *HealthManager) StartProbeLoop(ctx context.Context) {
	if m.probeInterval <= 0 {
		return
	}
	t := time.NewTicker(m.probeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.ProbeEjected(ctx)
		}
	}
}

// Select 按健康度挑选一个节点，返回节点与判定依据说明。
// 规则：先按状态档位（healthy 优先、suspect 兜底、ejected 不参与），
// 同档位内优先“在途请求更少”的节点以分散并发压力，其次按延迟 EWMA 升序，
// 最后按名字稳定排序。
func (m *HealthManager) Select(_ context.Context) (NodeClient, string, error) {
	m.mu.Lock()
	var healthy, suspect []*nodeHealth
	var total int
	for _, nh := range m.nodes {
		total++
		switch nh.status.State {
		case StateHealthy:
			healthy = append(healthy, nh)
		case StateSuspect:
			suspect = append(suspect, nh)
		}
	}

	// 排序键：在途请求数 -> 延迟 EWMA -> 名字。
	less := func(a, b *nodeHealth) bool {
		if a.inflight != b.inflight {
			return a.inflight < b.inflight
		}
		if a.status.LatencyEWMA != b.status.LatencyEWMA {
			return a.status.LatencyEWMA < b.status.LatencyEWMA
		}
		return a.client.Name() < b.client.Name()
	}
	sort.Slice(healthy, func(i, j int) bool { return less(healthy[i], healthy[j]) })
	sort.Slice(suspect, func(i, j int) bool { return less(suspect[i], suspect[j]) })

	var picked *nodeHealth
	var tier string
	switch {
	case len(healthy) > 0:
		picked, tier = healthy[0], "healthy/least_inflight_lowest_latency"
	case len(suspect) > 0:
		picked, tier = suspect[0], "suspect_fallback"
	default:
		m.mu.Unlock()
		m.log.Warn("select node failed", "reason", "all_nodes_ejected", "total", total)
		return nil, "all_nodes_ejected", ErrNoHealthyNode
	}

	// picked 与候选集在同一把锁内确定并计数，避免竞态下重复压同一节点。
	if picked.status.State == StateEjected {
		m.mu.Unlock()
		return nil, "picked_node_ejected_race", ErrNoHealthyNode
	}
	picked.inflight++
	inflight := picked.inflight
	st := picked.status
	hc, sc := len(healthy), len(suspect)
	m.mu.Unlock()

	reason := "node=" + picked.client.Name() +
		" tier=" + tier +
		" state=" + st.State.String() +
		" inflight=" + strconv.Itoa(inflight) +
		" latency_ewma=" + st.LatencyEWMA.String() +
		" failures=" + strconv.Itoa(st.ConsecutiveFailure) +
		" healthy=" + strconv.Itoa(hc) +
		" suspect=" + strconv.Itoa(sc) +
		" ejected=" + strconv.Itoa(total-hc-sc)
	m.log.Info("node selected", "node", picked.client.Name(), "reason", reason)
	return picked.client, reason, nil
}

// RecordResult 记录一次调用结果并更新健康状态。
func (m *HealthManager) RecordResult(name string, latency time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	nh, ok := m.nodes[name]
	if !ok {
		return
	}
	if nh.inflight > 0 {
		nh.inflight--
	}
	if err != nil {
		nh.status.ConsecutiveFailure++
		nh.status.LastError = err.Error()
		nh.status.LastChange = time.Now()
		kind := KindOf(err).String()
		prev := nh.status.State
		f := nh.status.ConsecutiveFailure
		switch {
		case f >= m.failureThreshold && prev != StateEjected:
			nh.status.State = StateEjected
			nh.status.ProbeStreak = 0
			m.log.Error("node ejected",
				"node", name, "prev_state", prev.String(), "failures", f,
				"threshold", m.failureThreshold, "last_error_kind", kind, "last_error", err.Error())
		case f*2 >= m.failureThreshold && prev == StateHealthy:
			nh.status.State = StateSuspect
			m.log.Warn("node marked suspect",
				"node", name, "failures", f, "threshold", m.failureThreshold,
				"error_kind", kind)
		default:
			m.log.Warn("node call failed",
				"node", name, "state", nh.status.State.String(),
				"failures", f, "error_kind", kind)
		}
		return
	}

	// 成功：更新延迟 EWMA，清除失败计数。
	nh.status.Successes++
	if nh.status.LatencyEWMA == 0 {
		nh.status.LatencyEWMA = latency
	} else {
		nh.status.LatencyEWMA = time.Duration(float64(nh.status.LatencyEWMA)*(1-ewmaAlpha) +
			float64(latency)*ewmaAlpha)
	}
	prev := nh.status.State
	nh.status.ConsecutiveFailure = 0
	nh.status.LastError = ""
	if prev == StateSuspect {
		nh.status.State = StateHealthy
		nh.status.LastChange = time.Now()
		m.log.Info("node recovered from suspect", "node", name,
			"successes", nh.status.Successes, "latency_ewma", nh.status.LatencyEWMA.String())
	}
}

// ProbeEjected 对所有非 healthy 节点（suspect 与 ejected）做一次恢复探测。
//
//	suspect 探测失败：继续累加失败计数，达到阈值即摘除；探测成功：恢复 healthy。
//	ejected 探测失败：维持摘除，连续成功计数清零；
//	ejected 连续 recoverThreshold 次成功：恢复 healthy。
//
// 这样即使存在健康节点、suspect 节点长期不被选中，也不会“永远卡在 suspect”。
func (m *HealthManager) ProbeEjected(ctx context.Context) {
	m.mu.Lock()
	var clients []NodeClient
	for _, nh := range m.nodes {
		if nh.status.State != StateHealthy {
			clients = append(clients, nh.client)
		}
	}
	m.mu.Unlock()

	for _, c := range clients {
		cctx, cancel := context.WithTimeout(ctx, probeTimeout)
		start := time.Now()
		_, err := c.LatestHeight(cctx)
		latency := time.Since(start)
		cancel()

		m.mu.Lock()
		nh := m.nodes[c.Name()]
		if nh == nil || nh.status.State == StateHealthy {
			// 期间已通过正常流量恢复。
			m.mu.Unlock()
			continue
		}
		state := nh.status.State

		if err != nil {
			nh.status.LastError = err.Error()
			if state == StateEjected {
				nh.status.ProbeStreak = 0
				m.log.Warn("ejected node probe failed, stays ejected",
					"node", c.Name(), "probe_latency", latency.String(),
					"error_kind", KindOf(err).String(), "last_error", err.Error())
			} else { // suspect
				nh.status.ConsecutiveFailure++
				if nh.status.ConsecutiveFailure >= m.failureThreshold {
					nh.status.State = StateEjected
					nh.status.ProbeStreak = 0
					m.log.Error("suspect node ejected after failed probe",
						"node", c.Name(), "failures", nh.status.ConsecutiveFailure,
						"threshold", m.failureThreshold,
						"error_kind", KindOf(err).String(), "last_error", err.Error())
				} else {
					m.log.Warn("suspect node probe failed",
						"node", c.Name(), "failures", nh.status.ConsecutiveFailure,
						"threshold", m.failureThreshold, "error_kind", KindOf(err).String())
				}
			}
			m.mu.Unlock()
			continue
		}

		// 探测成功。
		switch state {
		case StateSuspect:
			nh.status.State = StateHealthy
			nh.status.ConsecutiveFailure = 0
			nh.status.LastError = ""
			nh.status.LastChange = time.Now()
			m.log.Info("suspect node restored by probe",
				"node", c.Name(), "final_state", StateHealthy.String())
		case StateEjected:
			nh.status.ProbeStreak++
			m.log.Info("ejected node probe succeeded",
				"node", c.Name(), "streak", nh.status.ProbeStreak,
				"need", recoverThreshold)
			if nh.status.ProbeStreak >= recoverThreshold {
				nh.status.State = StateHealthy
				nh.status.ConsecutiveFailure = 0
				nh.status.ProbeStreak = 0
				nh.status.LastError = ""
				nh.status.LastChange = time.Now()
				nh.status.LatencyEWMA = latency
				m.log.Info("node restored to healthy",
					"node", c.Name(), "final_state", StateHealthy.String())
			}
		}
		m.mu.Unlock()
	}
}

// Snapshot 返回所有节点的状态快照（值拷贝）。
func (m *HealthManager) Snapshot() map[string]NodeStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]NodeStatus, len(m.nodes))
	for name, nh := range m.nodes {
		out[name] = nh.status
	}
	return out
}
