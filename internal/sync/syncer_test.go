package sync

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/health"
	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/rpc"
	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/store"
	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/types"
)

type testCluster struct {
	nodes  []*rpc.MockNode
	svrs   []*httptest.Server
	client []NodeClient
}

// safeBuffer is a mutex-protected bytes.Buffer safe for concurrent log writes
// and test-side reads.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *safeBuffer) Contains(sub string) bool {
	return strings.Contains(b.String(), sub)
}

func (b *safeBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

func newCluster(t *testing.T, n int, blocks int64) (*testCluster, func()) {
	t.Helper()
	cl := &testCluster{}
	for i := 0; i < n; i++ {
		id := string(rune('A' + i))
		m := rpc.NewMockNode(id, blocks)
		srv := m.Start()
		cl.nodes = append(cl.nodes, m)
		cl.svrs = append(cl.svrs, srv)
		cl.client = append(cl.client, rpc.NewClient(id, srv.URL, 250*time.Millisecond))
	}
	cleanup := func() {
		for _, m := range cl.nodes {
			m.Close()
		}
	}
	return cl, cleanup
}

func testConfig(buf io.Writer) Config {
	return Config{
		PollInterval:   5 * time.Millisecond,
		RequestTimeout: 250 * time.Millisecond,
		Concurrency:    4,
		BatchHeight:    64,
		Health: health.Config{
			MaxConsecutiveFail: 2,
			ProbeInterval:      15 * time.Millisecond,
			ProbeSuccessNeeded: 1,
			Logger:             log.New(buf, "", 0),
		},
		Logger: log.New(buf, "", 0),
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timed out waiting for: " + msg)
}

func TestSyncBasicHappyPath(t *testing.T) {
	buf := &safeBuffer{}
	cl, cleanup := newCluster(t, 2, 6)
	defer cleanup()
	st := store.New(0)
	s := New(cl.client, st, testConfig(buf))
	n, err := s.SyncOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 6 || st.Tip() != 6 {
		t.Fatalf("applied=%d tip=%d want 6/6", n, st.Tip())
	}
	// A second pass is a no-op: the store has caught up to the nodes' tip.
	buf.Reset()
	n2, err := s.SyncOnce(context.Background())
	if err != nil || n2 != 0 {
		t.Fatalf("second pass applied=%d err=%v want 0", n2, err)
	}
	if strings.Contains(buf.String(), "apply=inserted") {
		t.Fatalf("caught-up pass must not write again:\n%s", buf.String())
	}

	// If the nodes grow by one block, the next pass inserts exactly that block
	// and re-confirming it (force a re-fetch via a fresh pass after rollback is
	// not needed here) stays idempotent at the store level.
	for _, m := range cl.nodes {
		m.ExtendChain()
	}
	n3, err := s.SyncOnce(context.Background())
	if err != nil || n3 != 1 || st.Tip() != 7 {
		t.Fatalf("growth pass applied=%d tip=%d err=%v want 1/7", n3, st.Tip(), err)
	}
}

func TestNodeEjectedThenRecovered(t *testing.T) {
	buf := &safeBuffer{}
	cl, cleanup := newCluster(t, 3, 8)
	defer cleanup()

	// Node A fails everything; B and C are healthy.
	cl.nodes[0].SetBehavior(rpc.Behavior{FailAll: true, FailKind: types.ErrTimeout})

	st := store.New(0)
	s := New(cl.client, st, testConfig(buf))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = s.Run(ctx); close(done) }()

	// Sync still reaches tip 8 despite A being dead.
	waitFor(t, time.Second, func() bool { return st.Tip() == 8 }, "sync to tip 8")

	waitFor(t, time.Second, func() bool {
		st0, ok := s.Health().StatusOf("A")
		return ok && st0.State == health.StateEjected
	}, "node A ejected")

	if !strings.Contains(buf.String(), "state suspected -> ejected") {
		t.Fatalf("ejection transition not logged:\n%s", buf.String())
	}
	// Selected nodes for actual fetches must never be A once ejected.
	if strings.Contains(strings.Split(buf.String(), "ejected")[1], "selected_node=A") {
		t.Fatalf("ejected node A was selected after ejection:\n%s", buf.String())
	}

	// A recovers: probes must bring it back to healthy.
	cl.nodes[0].ResetBehavior()
	waitFor(t, time.Second, func() bool {
		st0, _ := s.Health().StatusOf("A")
		return st0.State == health.StateHealthy
	}, "node A recovered via probe")
	if !strings.Contains(buf.String(), "state ejected -> healthy") {
		t.Fatalf("recovery transition not logged:\n%s", buf.String())
	}

	cancel()
	<-done
}

func TestConcurrentFetchOrderedApplyAndDuplicate(t *testing.T) {
	buf := &safeBuffer{}
	cl, cleanup := newCluster(t, 3, 20)
	defer cleanup()
	st := store.New(0)
	s := New(cl.client, st, testConfig(buf))
	n, err := s.SyncOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 20 {
		t.Fatalf("applied=%d want 20", n)
	}
	// Canonical chain must be contiguous despite concurrent fetch.
	snap := st.Snapshot()
	for i, b := range snap {
		if b.Height != int64(i+1) {
			t.Fatalf("snapshot not ordered at %d: %+v", i, b)
		}
		if b.Hash != rpc.HashOf(b.Height) {
			t.Fatalf("height %d hash=%s want %s", b.Height, b.Hash, rpc.HashOf(b.Height))
		}
	}

	// On the re-fetched height, A rate-limits and B returns an internal error;
	// only C can serve it, forcing the syncer to walk the node list in health
	// order and log each classified failure before succeeding.
	cl.nodes[0].SetBehavior(rpc.Behavior{
		FailHeightSet: true, FailHeight: 20, FailKind: types.ErrRateLimited,
	})
	cl.nodes[1].SetBehavior(rpc.Behavior{
		FailHeightSet: true, FailHeight: 20, FailKind: types.ErrNodeInternal,
	})
	// Roll back the tip so the next pass re-fetches height 20 from the nodes.
	if !st.RollbackTo(19) {
		t.Fatal("rollback of unconfirmed tip failed")
	}
	n, err = s.SyncOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("refetch pass applied=%d err=%v want 1 (only C serves height 20)", n, err)
	}
	if st.Tip() != 20 {
		t.Fatalf("tip=%d want 20", st.Tip())
	}
	if !strings.Contains(buf.String(), "kind=rate_limited") {
		t.Fatalf("rate-limited branch not logged:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "kind=node_internal") {
		t.Fatalf("node-internal branch not logged:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "trying next") {
		t.Fatalf("fallback-to-next-node decision not logged:\n%s", buf.String())
	}

	if _, ok := st.Get(context.Background(), 20); !ok {
		t.Fatal("height 20 missing after refetch")
	}
}

func TestSingleNodeFailureDoesNotStallSync(t *testing.T) {
	buf := &safeBuffer{}
	cl, cleanup := newCluster(t, 3, 10)
	defer cleanup()
	// A returns internal errors, B times out, only C is usable.
	cl.nodes[0].SetBehavior(rpc.Behavior{FailAll: true, FailKind: types.ErrNodeInternal})
	cl.nodes[1].SetBehavior(rpc.Behavior{FailAll: true, FailKind: types.ErrTimeout})
	st := store.New(0)
	s := New(cl.client, st, testConfig(buf))
	n, err := s.SyncOnce(context.Background())
	if err != nil || n != 10 {
		t.Fatalf("applied=%d err=%v want 10 (must be served entirely by C)", n, err)
	}
	stA, _ := s.Health().StatusOf("A")
	stB, _ := s.Health().StatusOf("B")
	stC, _ := s.Health().StatusOf("C")
	if stC.State != health.StateHealthy {
		t.Fatalf("C must stay healthy: %+v", stC)
	}
	if stA.State == health.StateHealthy && stB.State == health.StateHealthy {
		t.Fatalf("A/B should be suspected or ejected: A=%s B=%s", stA.State, stB.State)
	}
	if !strings.Contains(buf.String(), "kind=node_internal") ||
		!strings.Contains(buf.String(), "kind=timeout") {
		t.Fatalf("classified error branches not logged:\n%s", buf.String())
	}
}

func TestReorgRollbackAndRefetch(t *testing.T) {
	buf := &safeBuffer{}
	// Single node, canonical chain 1..6.
	cl, cleanup := newCluster(t, 1, 6)
	defer cleanup()
	st := store.New(0)
	cfg := testConfig(buf)
	s := New(cl.client, st, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = s.Run(ctx); close(done) }()

	waitFor(t, time.Second, func() bool { return st.Tip() == 6 }, "initial sync to 6")

	// Reorg above height 3: node now serves alt blocks for 4..6.
	cl.nodes[0].SetChain(rpc.ForkChain(6, 3))

	// The store must converge onto the alt chain.
	waitFor(t, 2*time.Second, func() bool {
		b, ok := st.Get(context.Background(), 6)
		return ok && b.Hash == rpc.AltHashOf(6)
	}, "reorg: height 6 becomes alt")
	waitFor(t, time.Second, func() bool {
		b, ok := st.Get(context.Background(), 4)
		return ok && b.Hash == rpc.AltHashOf(4)
	}, "reorg: height 4 becomes alt")
	if b, _ := st.Get(context.Background(), 3); b.Hash != rpc.HashOf(3) {
		t.Fatalf("fork point h3 must remain canonical, got %s", b.Hash)
	}
	if !strings.Contains(buf.String(), "divergence=true") {
		t.Fatalf("reorg divergence decision not logged:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "common_ancestor=3") {
		t.Fatalf("common ancestor decision not logged:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "re-fetches from 4") {
		t.Fatalf("reorg refetch decision not logged:\n%s", buf.String())
	}

	cancel()
	<-done
}

func TestConfirmedHeightSurvivesDeepReorg(t *testing.T) {
	buf := &safeBuffer{}
	cl, cleanup := newCluster(t, 1, 6)
	defer cleanup()
	// Two confirmations: with tip 6, heights 1..4 are final.
	st := store.New(2)
	s := New(cl.client, st, testConfig(buf))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = s.Run(ctx); close(done) }()
	waitFor(t, time.Second, func() bool { return st.Tip() == 6 }, "sync to 6")

	// A fork above height 1 would rewrite confirmed h2..h4: must be refused.
	cl.nodes[0].SetChain(rpc.ForkChain(6, 1))
	// Wait for the detector to attempt the deep reorg and record the refusal.
	waitFor(t, time.Second, func() bool {
		return buf.Contains("rejected_finalized")
	}, "deep reorg refusal logged")
	if b, _ := st.Get(context.Background(), 2); b.Hash != rpc.HashOf(2) {
		t.Fatalf("confirmed height 2 must not be overwritten, got %s", b.Hash)
	}
	cancel()
	<-done
	if !buf.Contains("rejected_finalized") {
		t.Fatalf("finalized rejection not logged:\n%s", buf.String())
	}
}
