package mocknode

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/highcumontoa/go-chain-rpc-indexer-go"
)

func TestLinearChainAndNode(t *testing.T) {
	chain := NewLinearChain(1, 5)
	if chain.Tip() != 5 {
		t.Fatalf("tip=%d want 5", chain.Tip())
	}
	n := New("node", chain)
	ctx := context.Background()
	tip, err := n.LatestHeight(ctx)
	if err != nil || tip != 5 {
		t.Fatalf("latest=%d err=%v", tip, err)
	}
	b, err := n.GetBlockByHeight(ctx, 3)
	if err != nil || b.Hash != "g0-h3" || b.ParentHash != "g0-h2" {
		t.Fatalf("block=%+v err=%v", b, err)
	}
	if _, err := n.GetBlockByHeight(ctx, 99); indexer.KindOf(err) != indexer.KindChainBusiness {
		t.Fatalf("future height kind=%v want chain_business", indexer.KindOf(err))
	}
}

func TestInjectedErrorKinds(t *testing.T) {
	chain := NewLinearChain(1, 3)
	n := New("node", chain)
	rl := indexer.KindRateLimited
	n.FailNext(1, rl)
	_, err := n.LatestHeight(context.Background())
	if indexer.KindOf(err) != indexer.KindRateLimited {
		t.Fatalf("kind=%v", indexer.KindOf(err))
	}
	// 配额用完即恢复。
	if tip, err := n.LatestHeight(context.Background()); err != nil || tip != 3 {
		t.Fatalf("after quota tip=%d err=%v", tip, err)
	}

	ni := indexer.KindNodeInternal
	n.SetBlockError(2, ni)
	if _, err := n.GetBlockByHeight(context.Background(), 2); indexer.KindOf(err) != indexer.KindNodeInternal {
		t.Fatalf("height error kind=%v", indexer.KindOf(err))
	}
}

func TestTimeoutInjection(t *testing.T) {
	chain := NewLinearChain(1, 1)
	n := New("slow", chain).SetLatency(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := n.LatestHeight(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v want deadline exceeded", err)
	}
}

func TestChainExtendAndReorg(t *testing.T) {
	chain := NewLinearChain(1, 6)
	chain.Extend(2)
	if chain.Tip() != 8 {
		t.Fatalf("tip=%d want 8", chain.Tip())
	}
	before, _ := chain.BlockAt(5)
	chain.Reorg(4, 7) // 5..8 换成新分支
	after, _ := chain.BlockAt(5)
	if after.Hash == before.Hash {
		t.Fatal("block 5 should have new hash after reorg")
	}
	fork, _ := chain.BlockAt(4)
	if fork.Hash != "g0-h4" {
		t.Fatalf("fork point changed: %q", fork.Hash)
	}
	b5, _ := chain.BlockAt(5)
	b6, _ := chain.BlockAt(6)
	if b5.ParentHash != fork.Hash || b6.ParentHash != b5.Hash {
		t.Fatal("reorged branch parent links broken")
	}
	if chain.Tip() != 7 {
		t.Fatalf("tip=%d want 7", chain.Tip())
	}
}

func TestLaggingNodeTip(t *testing.T) {
	chain := NewLinearChain(1, 10)
	lag := New("lag", chain).SetTip(6)
	tip, _ := lag.LatestHeight(context.Background())
	if tip != 6 {
		t.Fatalf("tip=%d want 6", tip)
	}
	if _, err := lag.GetBlockByHeight(context.Background(), 7); indexer.KindOf(err) != indexer.KindChainBusiness {
		t.Fatalf("lag node should hide height 7 as chain_business, got %v", err)
	}
}
