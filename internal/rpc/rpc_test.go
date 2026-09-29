package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/types"
)

func TestMockLinearChainAndClient(t *testing.T) {
	m := NewMockNode("n1", 5)
	srv := m.Start()
	defer m.Close()

	c := NewClient("n1", srv.URL, time.Second)
	ctx := context.Background()
	tip, err := c.LatestHeight(ctx)
	if err != nil || tip != 5 {
		t.Fatalf("tip=%d err=%v want 5", tip, err)
	}
	b, err := c.GetBlock(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if b.Hash != HashOf(3) || b.Parent != ParentHashOf(3) || b.Height != 3 {
		t.Fatalf("block mismatch: %+v", b)
	}
}

func TestErrorClassification(t *testing.T) {
	cases := []struct {
		name     string
		kind     types.ErrKind
		behavior Behavior
	}{
		{"rate limited", types.ErrRateLimited, Behavior{FailAll: true, FailKind: types.ErrRateLimited}},
		{"node internal", types.ErrNodeInternal, Behavior{FailAll: true, FailKind: types.ErrNodeInternal}},
		{"chain business", types.ErrChainBusiness, Behavior{FailAll: true, FailKind: types.ErrChainBusiness}},
		{"timeout", types.ErrTimeout, Behavior{FailAll: true, FailKind: types.ErrTimeout}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := NewMockNode("n", 3)
			m.SetBehavior(tc.behavior)
			srv := m.Start()
			defer m.Close()
			c := NewClient("n", srv.URL, time.Second)
			_, err := c.GetBlock(context.Background(), 1)
			ne, ok := types.AsNodeError(err)
			if !ok {
				t.Fatalf("want *NodeError, got %T %v", err, err)
			}
			if ne.Kind != tc.kind {
				t.Fatalf("kind=%s want %s", ne.Kind, tc.kind)
			}
		})
	}
}

func TestTimeoutViaLatency(t *testing.T) {
	m := NewMockNode("slow", 2)
	m.SetBehavior(Behavior{Latency: 50 * time.Millisecond})
	srv := m.Start()
	defer m.Close()
	c := NewClient("slow", srv.URL, 10*time.Millisecond)
	_, err := c.GetBlock(context.Background(), 1)
	ne, ok := types.AsNodeError(err)
	if !ok || ne.Kind != types.ErrTimeout {
		t.Fatalf("want timeout NodeError, got %#v", err)
	}
}

func TestHeightNotAvailableIsChainBusiness(t *testing.T) {
	m := NewMockNode("n", 3)
	srv := m.Start()
	defer m.Close()
	c := NewClient("n", srv.URL, time.Second)
	_, err := c.GetBlock(context.Background(), 99)
	ne, ok := types.AsNodeError(err)
	if !ok || ne.Kind != types.ErrChainBusiness {
		t.Fatalf("want chain_business for future height, got %#v", err)
	}
}

func TestSetChainAndExtend(t *testing.T) {
	m := NewMockNode("n", 3)
	srv := m.Start()
	defer m.Close()
	c := NewClient("n", srv.URL, time.Second)
	ctx := context.Background()
	// Simulate a reorg above height 2: h3,h4 are now alt blocks; h3 attaches
	// to canonical h2 (the fork point).
	m.SetChain(ForkChain(4, 2))
	b, err := c.GetBlock(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	if b.Hash != AltHashOf(4) || b.Parent != AltHashOf(3) {
		t.Fatalf("fork block wrong: %+v", b)
	}
	b3, err := c.GetBlock(ctx, 3)
	if err != nil || b3.Hash != AltHashOf(3) || b3.Parent != HashOf(2) {
		t.Fatalf("height3 should be alt attached to canonical h2: %+v err=%v", b3, err)
	}
	b2, err := c.GetBlock(ctx, 2)
	if err != nil || b2.Hash != HashOf(2) {
		t.Fatalf("height2 should remain canonical: %+v err=%v", b2, err)
	}
	m.ExtendChain()
	if tip, _ := c.LatestHeight(ctx); tip != 5 {
		t.Fatalf("after extend tip=%d want 5", tip)
	}
}
