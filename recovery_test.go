package indexer

import (
	"context"
	"testing"
	"time"
)

// 摘除的节点恢复后，应被探测重新放回 healthy 并重新承担流量。
func TestEjectedNodeRejoinsAfterRecovery(t *testing.T) {
	chain := newTestChain(t, 1, 6)
	n1 := newStubNode("n1", chain)
	n2 := newStubNode("n2", chain).setAlways(NewRPCError(KindNodeInternal, "n2", "down"))

	cfg := testCfg()
	cfg.StopOnCaughtUp = false
	cfg.ProbeInterval = 20 * time.Millisecond
	store := NewMemoryStore(0)
	syn := NewSyncer([]NodeClient{n1, n2}, store, cfg, quietLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = syn.Run(ctx) }()

	waitFor(t, time.Second, func() bool {
		return syn.Health().Snapshot()["n2"].State == StateEjected
	})

	// 节点恢复。
	n2.setAlways(nil)
	waitFor(t, 2*time.Second, func() bool {
		return syn.Health().Snapshot()["n2"].State == StateHealthy
	})
	st := syn.Health().Snapshot()["n2"]
	if st.ConsecutiveFailure != 0 || st.LastError != "" {
		t.Fatalf("recovered node not cleaned: failures=%d lastErr=%q",
			st.ConsecutiveFailure, st.LastError)
	}
}
