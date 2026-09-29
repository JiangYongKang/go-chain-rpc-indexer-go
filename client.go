package indexer

import "context"

// NodeClient 是同步组件对节点的最小依赖。
type NodeClient interface {
	Name() string
	// LatestHeight 返回节点当前最新高度。
	LatestHeight(ctx context.Context) (uint64, error)
	// GetBlockByHeight 按高度拉取区块。
	GetBlockByHeight(ctx context.Context, height uint64) (*Block, error)
}
