// Command demo 用进程内假节点完整演示：多节点并发同步、故障摘除与恢复、链重组。
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/highcumontoa/go-chain-rpc-indexer-go"
	"github.com/highcumontoa/go-chain-rpc-indexer-go/mocknode"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// 一条从高度 1 开始、共 8 个区块的链，三个节点共享。
	chain := mocknode.NewLinearChain(1, 8)
	n1 := mocknode.New("node-1", chain)
	n2 := mocknode.New("node-2", chain)
	n3 := mocknode.New("node-3", chain)
	// node-3 加一点延迟，模拟更慢的健康节点。
	n3.SetLatency(20 * time.Millisecond)

	cfg := indexer.DefaultConfig()
	cfg.StopOnCaughtUp = false
	cfg.ProbeInterval = 300 * time.Millisecond
	cfg.PollInterval = 150 * time.Millisecond
	cfg.FailureThreshold = 2
	cfg.Confirmations = 0 // demo 中允许完整回退；“已确认不可覆盖”由单元测试覆盖

	idx, err := indexer.New(cfg, []indexer.NodeClient{n1, n2, n3}, log)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	idx.Start(ctx)

	// 等初始同步完成。
	waitUntil(func() bool { h, ok := idx.Head(ctx); return ok && h >= chain.Tip() }, time.Second)
	log.Info("initial sync done", "head", chain.Tip())

	// node-2 持续超时，应被摘除；随后恢复，应被探测回来。
	timeout := indexer.KindTimeout
	n2.SetAlwaysError(&timeout)
	log.Info("node-2 set to always timeout -> expect ejection")
	time.Sleep(1500 * time.Millisecond)
	n2.SetAlwaysError(nil)
	log.Info("node-2 recovered -> expect probe restore")
	time.Sleep(1500 * time.Millisecond)

	// 链增长 3 块，应继续被同步。
	newTip := chain.Extend(3)
	waitUntil(func() bool { h, _ := idx.Head(ctx); return h >= newTip }, time.Second)
	log.Info("extended sync done", "head", newTip)

	// head=11、confirmations=2，确认水位=9（高度<=9 不可被回退覆盖）。
	// 在高度 9 之后重组：9..11 被新哈希分支替换并延伸到 12；分叉点 9？
	// 注意分叉点本身保留，删除的是其之上的块，因此这里删除的是“分叉点之上”。
	chain.Reorg(8, 12)
	log.Info("chain reorg triggered: blocks above height 8 replaced")

	if !waitUntil(func() bool {
		b, err := idx.GetBlock(ctx, 12)
		return err == nil && b != nil && b.ParentHash == "g1-h11"
	}, 2*time.Second) {
		panic("reorg not observed in time")
	}
	b, _ := idx.GetBlock(ctx, 12)
	log.Info("reorg synced", "height", b.Height, "hash", b.Hash, "parent", b.ParentHash)

	cancel()
	idx.Close()
	log.Info("demo finished")
}

func waitUntil(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}
