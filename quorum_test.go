package indexer

import (
	"context"
	"sync"
	"testing"
	"time"
)

// 多个节点一起发生同一重组：quorum 必须确认为真重组并完成回退。
func TestMultiNodeSameReorgIsApplied(t *testing.T) {
	chain := newTestChain(t, 1, 12)
	nodes := []NodeClient{
		newStubNode("n1", chain),
		newStubNode("n2", chain),
		newStubNode("n3", chain),
	}
	store := NewMemoryStore(0)
	runOneShot(t, testCfg(), nodes, store)
	if h, _ := store.Head(); h != 12 {
		t.Fatalf("initial head=%d", h)
	}

	// 三个节点共享同一 chain，同时从高度 9 之上重组并延伸到 13。
	chain.reorg(9, 13, "fork")
	cfg := testCfg()
	cfg.MaxRounds = 4
	runOneShot(t, cfg, nodes, store)

	h, _ := store.Head()
	if h != 13 {
		t.Fatalf("head after multi-node reorg=%d, want 13", h)
	}
	b10, _ := store.GetBlock(10)
	if b10.ParentHash != "h9" {
		t.Fatalf("forked block10 parent=%q, want h9", b10.ParentHash)
	}
	b13, err := store.GetBlock(13)
	if err != nil || b13.Hash != "fork-h13" {
		t.Fatalf("new tip block: %v %+v", err, b13)
	}
}

// 单个节点分叉、其余节点维持原链：不能误回退，本地链保持不变。
func TestSingleNodeDivergenceDoesNotReorg(t *testing.T) {
	chain := newTestChain(t, 1, 12)
	n1 := newStubNode("n1", chain)
	n2 := newStubNode("n2", chain)
	n3 := newStubNode("n3", chain)
	store := NewMemoryStore(0)
	runOneShot(t, testCfg(), []NodeClient{n1, n2, n3}, store)

	// 让 n1 单独看到一条私有分叉（只改它引用的链不可行，因为共享 chain，
	// 这里改为固定 n1 的 tip 落后，模拟它停在旧状态）。
	n1.tipOffset = 6
	cfg := testCfg()
	cfg.MaxRounds = 4
	runOneShot(t, cfg, []NodeClient{n1, n2, n3}, store)

	h, _ := store.Head()
	if h != 12 {
		t.Fatalf("head=%d, want 12: single divergent node must not reorg", h)
	}
	b12, _ := store.GetBlock(12)
	if b12.Hash != "h12" {
		t.Fatalf("canonical hash changed: %q", b12.Hash)
	}
}

// 重组进行期间并发读取，不能读到半截状态：读到的每个前缀都必须父子相连。
func TestReadsAreConsistentDuringReorg(t *testing.T) {
	chain := newTestChain(t, 1, 100)
	nodes := []NodeClient{newStubNode("n1", chain), newStubNode("n2", chain)}
	cfg := testCfg()
	cfg.StopOnCaughtUp = false
	cfg.PollInterval = 5 * time.Millisecond
	cfg.ProbeInterval = 50 * time.Millisecond
	store := NewMemoryStore(0)
	syn := NewSyncer(nodes, store, cfg, quietLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = syn.Run(ctx) }()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 读取者：基于单次一致快照校验，看到的必须始终是某一代次的完整链。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			gen, head, blocks := store.Snapshot()
			var prev *Block
			for h := uint64(1); h <= head; h++ {
				b, ok := blocks[h]
				if !ok {
					break
				}
				if prev != nil && b.ParentHash != prev.Hash {
					t.Errorf("generation %d broken prefix at height %d: parent=%q want=%q",
						gen, h, b.ParentHash, prev.Hash)
					return
				}
				bp := b
				prev = &bp
			}
		}
	}()

	// 制造多次重组与延伸。
	waitFor(t, time.Second, func() bool { h, ok := store.Head(); return ok && h == 100 })
	chain.reorg(60, 105, "a")
	time.Sleep(200 * time.Millisecond)
	chain.reorg(40, 110, "b")
	time.Sleep(200 * time.Millisecond)
	chain.extend(5)
	time.Sleep(200 * time.Millisecond)

	close(stop)
	wg.Wait()
}
