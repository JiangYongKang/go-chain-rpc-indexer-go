package indexer

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

func newHM(nodes []NodeClient, threshold int, probe time.Duration) *HealthManager {
	return NewHealthManager(nodes, threshold, probe,
		slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError})))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestEjectionAndProbeRecovery(t *testing.T) {
	ctx := context.Background()
	chain := newTestChain(t, 1, 10)

	bad := newStubNode("bad", chain)
	good := newStubNode("good", chain)
	hm := newHM([]NodeClient{bad, good}, 3, 10*time.Millisecond)

	timeoutErr := NewRPCError(KindTimeout, "bad", "boom")

	// 连续 3 次失败 -> 摘除。
	for i := 1; i <= 3; i++ {
		hm.RecordResult("bad", time.Millisecond, timeoutErr)
	}
	if st := hm.Snapshot()["bad"]; st.State != StateEjected {
		t.Fatalf("after 3 failures state=%s, want ejected", st.State)
	}
	// 摘除期间 Select 绝不能选到 bad，且不能报错（good 仍在）。
	for i := 0; i < 5; i++ {
		c, _, err := hm.Select(ctx)
		if err != nil || c.Name() != "good" {
			t.Fatalf("select during ejection picked=%v err=%v", c, err)
		}
	}

	// 恢复探测：bad 仍在失败 -> 保持摘除。
	bad.setAlways(timeoutErr)
	hm.ProbeEjected(ctx)
	if st := hm.Snapshot()["bad"]; st.State != StateEjected {
		t.Fatalf("probe while failing should stay ejected, got %s", st.State)
	}

	// 恢复后探测：recoverThreshold(2) 次成功才回到 healthy。
	bad.setAlways(nil)
	hm.ProbeEjected(ctx)
	if st := hm.Snapshot()["bad"]; st.State != StateEjected || st.ProbeStreak != 1 {
		t.Fatalf("after 1 good probe state=%s streak=%d", st.State, st.ProbeStreak)
	}
	hm.ProbeEjected(ctx)
	if st := hm.Snapshot()["bad"]; st.State != StateHealthy {
		t.Fatalf("after 2 good probes state=%s, want healthy", st.State)
	}
}

func TestAllEjectedReturnsError(t *testing.T) {
	ctx := context.Background()
	chain := newTestChain(t, 1, 3)
	a := newStubNode("a", chain)
	b := newStubNode("b", chain)
	hm := newHM([]NodeClient{a, b}, 1, time.Millisecond)
	errKind := KindNodeInternal
	for _, n := range []string{"a", "b"} {
		hm.RecordResult(n, time.Millisecond, NewRPCError(errKind, n, "down"))
	}
	if _, _, err := hm.Select(ctx); err != ErrNoHealthyNode {
		t.Fatalf("select with all ejected err=%v, want ErrNoHealthyNode", err)
	}
	// 任一节点恢复探测成功 2 次即可重新提供服务。
	hm.ProbeEjected(ctx)
	hm.ProbeEjected(ctx)
	c, _, err := hm.Select(ctx)
	if err != nil || c == nil {
		t.Fatalf("after recovery select err=%v c=%v", err, c)
	}
}

func TestSelectionPrefersLowerLatency(t *testing.T) {
	ctx := context.Background()
	chain := newTestChain(t, 1, 3)
	fast := newStubNode("fast", chain)
	slow := newStubNode("slow", chain)
	hm := newHM([]NodeClient{fast, slow}, 5, time.Millisecond)

	hm.RecordResult("slow", 100*time.Millisecond, nil)
	hm.RecordResult("fast", 1*time.Millisecond, nil)
	picked, reason, err := hm.Select(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if picked.Name() != "fast" {
		t.Fatalf("picked=%s, want fast (reason=%s)", picked.Name(), reason)
	}
}
