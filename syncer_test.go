package indexer

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{},
		&slog.HandlerOptions{Level: slog.LevelError + 100})) // 静默全部
}

func testCfg() Config {
	return Config{
		StartHeight:      1,
		Confirmations:    0,
		FetchConcurrency: 3,
		MaxInFlight:      8,
		RequestTimeout:   200 * time.Millisecond,
		FailureThreshold: 2,
		ProbeInterval:    30 * time.Millisecond,
		PollInterval:     20 * time.Millisecond,
		RateLimitBackoff: 5 * time.Millisecond,
		StopOnCaughtUp:   true,
	}
}

// runOneShot 用给定节点跑一轮到追平（或超时）。
func runOneShot(t *testing.T, cfg Config, nodes []NodeClient, store *MemoryStore) {
	t.Helper()
	syn := NewSyncer(nodes, store, cfg, quietLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := syn.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) &&
		!errors.Is(err, context.Canceled) {
		t.Fatalf("syncer run: %v", err)
	}
}

func TestSyncHappyPath(t *testing.T) {
	chain := newTestChain(t, 1, 10)
	nodes := []NodeClient{
		newStubNode("n1", chain),
		newStubNode("n2", chain),
		newStubNode("n3", chain),
	}
	store := NewMemoryStore(0)
	runOneShot(t, testCfg(), nodes, store)

	head, ok := store.Head()
	if !ok || head != 10 {
		t.Fatalf("head=%d,%v, want 10", head, ok)
	}
	for h := uint64(1); h <= 10; h++ {
		if _, err := store.GetBlock(h); err != nil {
			t.Fatalf("missing block %d: %v", h, err)
		}
	}
}

func TestNodeFailureDoesNotSlowOrStopSync(t *testing.T) {
	chain := newTestChain(t, 1, 12)
	bad := newStubNode("bad", chain).setAlways(NewRPCError(KindTimeout, "bad", "always down"))
	ok1 := newStubNode("ok1", chain)
	ok2 := newStubNode("ok2", chain)
	cfg := testCfg()
	cfg.StopOnCaughtUp = false // 常驻模式，让坏节点持续吃到失败并被探测
	store := NewMemoryStore(0)
	syn := NewSyncer([]NodeClient{bad, ok1, ok2}, store, cfg, quietLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = syn.Run(ctx) }()

	// 同步必须照常推进，不被坏节点拖慢。
	waitFor(t, time.Second, func() bool {
		h, ok := store.Head()
		return ok && h == 12
	})
	// 坏节点必须被摘除。
	waitFor(t, time.Second, func() bool {
		return syn.Health().Snapshot()["bad"].State == StateEjected
	})

	for _, name := range []string{"ok1", "ok2"} {
		if st := syn.Health().Snapshot()[name]; st.State == StateEjected {
			t.Fatalf("%s unexpectedly ejected", name)
		}
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestDuplicateAndOutOfOrderFetches(t *testing.T) {
	// 同一批节点重复跑两轮：第二轮所有高度都是“重复抓取”，结果应为 duplicate，
	// 数据不重复、head 不回退。
	chain := newTestChain(t, 1, 8)
	nodes := []NodeClient{newStubNode("n1", chain)}
	cfg := testCfg()
	store := NewMemoryStore(0)
	runOneShot(t, cfg, nodes, store)
	runOneShot(t, cfg, nodes, store)

	if head, _ := store.Head(); head != 8 {
		t.Fatalf("head after duplicate round = %d, want 8", head)
	}
	// 直接验证幂等结果码。
	b, _ := store.GetBlock(5)
	if o, err := store.WriteBlock(b); err != nil || o != WriteDuplicate {
		t.Fatalf("rewrite existing outcome=%v err=%v, want duplicate", o, err)
	}
	if o, _ := store.WriteBlock(mkBlock(5, "different-hash", "h4")); o != WriteHashConflict {
		t.Fatalf("different hash outcome=%v, want hash_conflict", o)
	}
}

func TestErrorKindsAreClassified(t *testing.T) {
	for kind, want := range map[ErrorKind]string{
		KindTimeout: "timeout", KindRateLimited: "rate_limited",
		KindNodeInternal: "node_internal", KindChainBusiness: "chain_business",
	} {
		err := NewRPCError(kind, "n", "x")
		if KindOf(err).String() != want {
			t.Fatalf("kind=%d got=%s want=%s", kind, KindOf(err), want)
		}
		if kind == KindChainBusiness && KindOf(err).Retryable() {
			t.Fatalf("chain business error must not be retryable")
		}
	}
	// context deadline 也应识别为超时。
	if KindOf(context.DeadlineExceeded) != KindTimeout {
		t.Fatal("context.DeadlineExceeded should classify as timeout")
	}
}

func TestNodeLagIsNotTreatedAsReorg(t *testing.T) {
	chain := newTestChain(t, 1, 10)
	ahead := newStubNode("ahead", chain)
	lagging := newStubNode("lagging", chain)
	lagging.tipOffset = 4 // tip 只到 6
	store := NewMemoryStore(0)
	runOneShot(t, testCfg(), []NodeClient{ahead, lagging}, store)
	if head, _ := store.Head(); head != 10 {
		t.Fatalf("head=%d, want 10 (lagging node must not trigger rollback)", head)
	}
}

func TestReorgSwitchesCanonicalChain(t *testing.T) {
	chain := newTestChain(t, 1, 10)
	nodes := []NodeClient{newStubNode("n1", chain), newStubNode("n2", chain)}
	store := NewMemoryStore(0)

	// 初始同步到 10。
	runOneShot(t, testCfg(), nodes, store)
	old8, _ := store.GetBlock(8)

	// 在 7 之后重组：8..10 换成 fork- 分支并延伸到 11。
	chain.reorg(7, 11, "fork")
	runOneShot(t, testCfg(), nodes, store)

	head, _ := store.Head()
	if head != 11 {
		t.Fatalf("head after reorg=%d, want 11", head)
	}
	new8, _ := store.GetBlock(8)
	if new8.Hash == old8.Hash {
		t.Fatal("block 8 hash unchanged after reorg")
	}
	if new8.ParentHash != "h7" {
		t.Fatalf("block8 parent=%q, want h7 (fork point preserved)", new8.ParentHash)
	}
	b7, _ := store.GetBlock(7)
	if b7.Hash != "h7" {
		t.Fatalf("fork point block mutated: %q", b7.Hash)
	}
	b11, err := store.GetBlock(11)
	if err != nil || b11.Hash != "fork-h11" {
		t.Fatalf("new tip: %v %+v", err, b11)
	}
}

func TestReorgBelowConfirmedIsRejected(t *testing.T) {
	chain := newTestChain(t, 1, 12)
	nodes := []NodeClient{newStubNode("n1", chain)}
	cfg := testCfg()
	cfg.Confirmations = 4 // head=12 -> 水位 8
	cfg.MaxRounds = 3
	store := NewMemoryStore(cfg.Confirmations)
	runOneShot(t, cfg, nodes, store)

	safeBefore, _ := store.GetBlock(6)
	// 在高度 5 之上重组，分叉点 5 <= 确认水位 8 -> 必须拒绝，本地链不变。
	chain.reorg(5, 12, "deepfork")
	runOneShot(t, cfg, nodes, store)

	got, err := store.GetBlock(6)
	if err != nil || got.Hash != safeBefore.Hash {
		t.Fatalf("confirmed data overwritten: got=%q err=%v", gotHash(got), err)
	}
	head, _ := store.Head()
	if head != 12 {
		t.Fatalf("head=%d, want 12 (rejected reorg must keep head)", head)
	}
}

func TestRepeatedReorgAtSameHeightStaysConsistent(t *testing.T) {
	// 同一重组信号被多轮重复处理：第一轮切换后，后续轮次不应再触发回退/抖动。
	chain := newTestChain(t, 1, 10)
	nodes := []NodeClient{newStubNode("n1", chain)}
	store := NewMemoryStore(0)
	runOneShot(t, testCfg(), nodes, store)
	chain.reorg(6, 10, "a")
	runOneShot(t, testCfg(), nodes, store)
	hashA, _ := store.GetBlock(7)
	runOneShot(t, testCfg(), nodes, store)
	again, _ := store.GetBlock(7)
	if hashA.Hash != again.Hash {
		t.Fatal("canonical chain changed on repeat round without new reorg")
	}
}

// 一个节点持续限流、其余正常：同步应自动绕开限流节点完成（验证 rate_limited 分类与转移）。
func TestRateLimitedNodeFailsOver(t *testing.T) {
	chain := newTestChain(t, 1, 10)
	limited := newStubNode("limited", chain).
		setAlways(NewRPCError(KindRateLimited, "limited", "429"))
	good := newStubNode("good", chain)
	store := NewMemoryStore(0)
	cfg := testCfg()
	cfg.RateLimitBackoff = time.Millisecond
	runOneShot(t, cfg, []NodeClient{limited, good}, store)
	if h, _ := store.Head(); h != 10 {
		t.Fatalf("head=%d, want 10 despite rate-limited node", h)
	}
}

// 某节点对单个高度返回节点内部错误、其余高度正常：应换节点取到该高度。
func TestNodeInternalErrorOnOneHeightFailsOver(t *testing.T) {
	chain := newTestChain(t, 1, 8)
	flaky := newStubNode("flaky", chain)
	flaky.mu.Lock()
	flaky.always = nil
	flaky.mu.Unlock()
	// 用一个按高度注入错误的包装节点。
	badHeight := &heightErrorNode{stubNode: flaky, height: 4,
		err: NewRPCError(KindNodeInternal, "flaky", "500")}
	good := newStubNode("good", chain)
	store := NewMemoryStore(0)
	runOneShot(t, testCfg(), []NodeClient{badHeight, good}, store)
	if h, _ := store.Head(); h != 8 {
		t.Fatalf("head=%d, want 8", h)
	}
	b4, err := store.GetBlock(4)
	if err != nil || b4.Hash != "h4" {
		t.Fatalf("height 4 not recovered from good node: %v %+v", err, b4)
	}
}

type heightErrorNode struct {
	*stubNode
	height uint64
	err    error
}

func (n *heightErrorNode) GetBlockByHeight(ctx context.Context, h uint64) (*Block, error) {
	if h == n.height {
		return nil, n.err
	}
	return n.stubNode.GetBlockByHeight(ctx, h)
}
