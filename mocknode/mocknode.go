// Package mocknode 提供进程内的假链节点（NodeClient 实现），
// 用于在不接真实链/外部服务的情况下完整验证同步组件。
//
// 能力：
//   - 基于 Chain 生成线性区块、向前延伸、从某高度发起重组（换哈希分支）；
//   - 多个 Node 可共享同一条 Chain，也可通过 SetTip 模拟落后节点；
//   - 注入固定延迟（超过请求超时即可制造 KindTimeout）；
//   - 注入“一直失败 / 接下来 N 次失败 / 指定高度失败”，覆盖
//     超时、限流、节点内部错误、链上业务错误四类。
package mocknode

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/go-chain-rpc-indexer-go"
)

// Chain 是一条可重组的内存链，可被多个 Node 共享。
type Chain struct {
	mu      sync.RWMutex
	blocks  map[uint64]*indexer.Block
	tip     uint64
	genesis uint64
	gen     int // 重组代数，用于生成分支哈希
}

// NewLinearChain 生成 [start, start+count-1] 的线性链。
func NewLinearChain(start uint64, count int) *Chain {
	c := &Chain{
		blocks:  make(map[uint64]*indexer.Block),
		tip:     Empty,
		genesis: start,
	}
	for i := 0; i < count; i++ {
		c.blocks[start+uint64(i)] = c.mustBuild(start+uint64(i), 0)
		c.tip = start + uint64(i)
	}
	return c
}

// Empty 表示没有高度。
const Empty = ^uint64(0)

// buildLocked 按“代数标签”构造区块，父哈希从已存在的上一高度取。
func (c *Chain) mustBuild(h uint64, gen int) *indexer.Block {
	var parent string
	if h > 0 {
		if prev, ok := c.blocks[h-1]; ok {
			parent = prev.Hash
		}
	}
	return &indexer.Block{
		Height:     h,
		Hash:       fmt.Sprintf("g%d-h%d", gen, h),
		ParentHash: parent,
		Time:       time.Unix(1_700_000_000+int64(h), 0),
		Proposer:   "mock-proposer",
	}
}

// Tip 返回链当前最新高度。
func (c *Chain) Tip() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tip
}

// BlockAt 返回某高度区块的拷贝。
func (c *Chain) BlockAt(h uint64) (*indexer.Block, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	b, ok := c.blocks[h]
	if !ok {
		return nil, false
	}
	cp := *b
	return &cp, true
}

// Extend 沿当前链向后追加 n 个区块。
func (c *Chain) Extend(n int) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < n; i++ {
		h := c.tip + 1
		c.blocks[h] = c.mustBuild(h, c.gen)
		c.tip = h
	}
	return c.tip
}

// Reorg 从 forkHeight（不含）之上整体替换成新哈希分支，并延伸到 newTip。
// forkHeight 处的区块保持不变，作为分叉点。
func (c *Chain) Reorg(forkHeight, newTip uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.blocks[forkHeight]; !ok {
		panic(fmt.Sprintf("mock chain: fork height %d not found", forkHeight))
	}
	if newTip < forkHeight+1 {
		panic("mock chain: newTip must be above forkHeight")
	}
	c.gen++
	gen := c.gen
	for h := range c.blocks {
		if h > forkHeight {
			delete(c.blocks, h)
		}
	}
	for h := forkHeight + 1; h <= newTip; h++ {
		c.blocks[h] = c.mustBuild(h, gen)
	}
	c.tip = newTip
}

// Node 是一个可编排故障的假节点。
type Node struct {
	name  string
	chain *Chain

	mu            sync.Mutex
	latency       time.Duration
	alwaysErr     *indexer.ErrorKind
	failNext      int
	failNextKind  indexer.ErrorKind
	blockErrors   map[uint64]indexer.ErrorKind
	tipOverride   uint64 // Empty 表示跟随链 tip
	tipStickyDown bool   // tip 固定不随链增长（模拟永久落后节点）

	latestCalls int64
	blockCalls  int64
}

var _ indexer.NodeClient = (*Node)(nil)

// New 创建指向某条链的假节点。
func New(name string, chain *Chain) *Node {
	return &Node{name: name, chain: chain, tipOverride: Empty}
}

func (n *Node) Name() string { return n.name }

// SetLatency 设置每次请求的固定人工延迟。
func (n *Node) SetLatency(d time.Duration) *Node {
	n.mu.Lock()
	n.latency = d
	n.mu.Unlock()
	return n
}

// SetAlwaysError 让所有请求持续返回指定错误；传 nil 取消。
func (n *Node) SetAlwaysError(kind *indexer.ErrorKind) *Node {
	n.mu.Lock()
	n.alwaysErr = kind
	n.mu.Unlock()
	return n
}

// FailNext 让接下来 n 次请求（LatestHeight 与 GetBlock 合计）失败。
func (n *Node) FailNext(times int, kind indexer.ErrorKind) *Node {
	n.mu.Lock()
	n.failNext = times
	n.failNextKind = kind
	n.mu.Unlock()
	return n
}

// SetBlockError 让某高度的区块请求返回指定错误。
func (n *Node) SetBlockError(h uint64, kind indexer.ErrorKind) *Node {
	n.mu.Lock()
	if n.blockErrors == nil {
		n.blockErrors = make(map[uint64]indexer.ErrorKind)
	}
	n.blockErrors[h] = kind
	n.mu.Unlock()
	return n
}

// SetTip 让该节点对外暴露固定最新高度（模拟落后）；SetTipAuto 恢复跟随。
func (n *Node) SetTip(h uint64) *Node {
	n.mu.Lock()
	n.tipOverride = h
	n.mu.Unlock()
	return n
}

// SetTipAuto 恢复跟随链最新高度。
func (n *Node) SetTipAuto() *Node {
	n.mu.Lock()
	n.tipOverride = Empty
	n.mu.Unlock()
	return n
}

// Calls 返回 latest/block 调用计数，用于断言流量分布。
func (n *Node) Calls() (latest, block int64) {
	return atomic.LoadInt64(&n.latestCalls), atomic.LoadInt64(&n.blockCalls)
}

func (n *Node) sleep(ctx context.Context) error {
	n.mu.Lock()
	d := n.latency
	n.mu.Unlock()
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// consumeError 返回当前应注入的错误（并消费 failNext 配额）。
func (n *Node) consumeError() *indexer.ErrorKind {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.alwaysErr != nil {
		k := *n.alwaysErr
		return &k
	}
	if n.failNext > 0 {
		n.failNext--
		k := n.failNextKind
		return &k
	}
	return nil
}

func rpcErr(kind indexer.ErrorKind, node, msg string) *indexer.RPCError {
	return indexer.NewRPCError(kind, node, msg)
}

func (n *Node) effectiveTip() uint64 {
	tip := n.chain.Tip()
	n.mu.Lock()
	if n.tipOverride != Empty {
		tip = n.tipOverride
	}
	n.mu.Unlock()
	return tip
}

// LatestHeight 返回节点最新高度。
func (n *Node) LatestHeight(ctx context.Context) (uint64, error) {
	atomic.AddInt64(&n.latestCalls, 1)
	if err := n.sleep(ctx); err != nil {
		return 0, err
	}
	if k := n.consumeError(); k != nil {
		return 0, rpcErr(*k, n.name, "injected latest-height error")
	}
	return n.effectiveTip(), nil
}

// GetBlockByHeight 按高度返回区块拷贝。
func (n *Node) GetBlockByHeight(ctx context.Context, height uint64) (*indexer.Block, error) {
	atomic.AddInt64(&n.blockCalls, 1)
	if err := n.sleep(ctx); err != nil {
		return nil, err
	}
	if k := n.consumeError(); k != nil {
		return nil, rpcErr(*k, n.name, "injected block error")
	}
	n.mu.Lock()
	if kind, ok := n.blockErrors[height]; ok {
		n.mu.Unlock()
		return nil, rpcErr(kind, n.name, fmt.Sprintf("injected error for height %d", height))
	}
	n.mu.Unlock()

	if height > n.effectiveTip() {
		return nil, rpcErr(indexer.KindChainBusiness, n.name,
			fmt.Sprintf("block at height %d not produced yet", height))
	}
	b, ok := n.chain.BlockAt(height)
	if !ok {
		return nil, rpcErr(indexer.KindChainBusiness, n.name,
			fmt.Sprintf("block at height %d pruned or missing", height))
	}
	return b, nil
}
