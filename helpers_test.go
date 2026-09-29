package indexer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// testChain 是包内测试用的内存链（不依赖 mocknode，避免循环引用）。
type testChain struct {
	mu     sync.Mutex
	blocks map[uint64]*Block
	tip    uint64
}

func newTestChain(t *testing.T, start uint64, count int) *testChain {
	t.Helper()
	c := &testChain{blocks: make(map[uint64]*Block), tip: EmptyHeight}
	for i := 0; i < count; i++ {
		h := start + uint64(i)
		c.blocks[h] = &Block{
			Height: h, Hash: fmt.Sprintf("h%d", h),
			ParentHash: parentHashOf(h, start),
			Time:       time.Unix(int64(h), 0),
		}
		c.tip = h
	}
	return c
}

func parentHashOf(h, start uint64) string {
	if h == start || h == 0 {
		return ""
	}
	return fmt.Sprintf("h%d", h-1)
}

func (c *testChain) tipHeight() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tip
}

func (c *testChain) block(h uint64) (*Block, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.blocks[h]
	if !ok {
		return nil, false
	}
	cp := *b
	return &cp, true
}

// reorg 从 fork 之上替换为新哈希前缀的分支，延伸到 newTip。
func (c *testChain) reorg(fork, newTip uint64, tag string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for h := range c.blocks {
		if h > fork {
			delete(c.blocks, h)
		}
	}
	parent := c.blocks[fork].Hash
	for h := fork + 1; h <= newTip; h++ {
		b := &Block{Height: h, Hash: fmt.Sprintf("%s-h%d", tag, h), ParentHash: parent}
		c.blocks[h] = b
		parent = b.Hash
	}
	c.tip = newTip
}

func (c *testChain) extend(n int) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < n; i++ {
		h := c.tip + 1
		parent := ""
		if b, ok := c.blocks[h-1]; ok {
			parent = b.Hash
		}
		c.blocks[h] = &Block{Height: h, Hash: fmt.Sprintf("h%d", h), ParentHash: parent}
		c.tip = h
	}
	return c.tip
}

// stubNode 是可编排错误/延迟/tip 的 NodeClient 桩。
type stubNode struct {
	name       string
	chain      *testChain
	mu         sync.Mutex
	always     error
	latency    time.Duration
	tipOffset  uint64 // 从链 tip 向下偏移多少（模拟落后）
	fixedTip   uint64 // 非 EmptyHeight 时固定 tip
	blockCalls map[uint64]int
}

func newStubNode(name string, chain *testChain) *stubNode {
	return &stubNode{name: name, chain: chain, fixedTip: EmptyHeight, blockCalls: map[uint64]int{}}
}

func (n *stubNode) setAlways(err error) *stubNode {
	n.mu.Lock()
	n.always = err
	n.mu.Unlock()
	return n
}

func (n *stubNode) setLatency(d time.Duration) *stubNode {
	n.mu.Lock()
	n.latency = d
	n.mu.Unlock()
	return n
}

func (n *stubNode) Name() string { return n.name }

func (n *stubNode) effectiveTip() uint64 {
	tip := n.chain.tipHeight()
	n.mu.Lock()
	if n.fixedTip != EmptyHeight {
		tip = n.fixedTip
	} else if n.tipOffset > 0 && tip >= n.tipOffset {
		tip -= n.tipOffset
	}
	n.mu.Unlock()
	return tip
}

func (n *stubNode) sleep(ctx context.Context) error {
	n.mu.Lock()
	d := n.latency
	n.mu.Unlock()
	if d <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func (n *stubNode) LatestHeight(ctx context.Context) (uint64, error) {
	if err := n.sleep(ctx); err != nil {
		return 0, err
	}
	n.mu.Lock()
	err := n.always
	n.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return n.effectiveTip(), nil
}

func (n *stubNode) GetBlockByHeight(ctx context.Context, h uint64) (*Block, error) {
	if err := n.sleep(ctx); err != nil {
		return nil, err
	}
	n.mu.Lock()
	err := n.always
	n.blockCalls[h]++
	n.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if h > n.effectiveTip() {
		return nil, NewRPCError(KindChainBusiness, n.name, fmt.Sprintf("height %d not produced", h))
	}
	b, ok := n.chain.block(h)
	if !ok {
		return nil, NewRPCError(KindChainBusiness, n.name, "missing")
	}
	return b, nil
}
