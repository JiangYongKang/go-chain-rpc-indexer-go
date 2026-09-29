package indexer

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Config 是同步组件配置。
type Config struct {
	// StartHeight 空库时从哪个高度开始同步。
	StartHeight uint64
	// Confirmations 确认深度：head-confirmations 及以下的高度不可被重组覆盖。
	Confirmations uint64
	// FetchConcurrency 单轮抓区块的并发 worker 数。
	FetchConcurrency int
	// MaxInFlight 单轮最多并发在途的区块窗口大小。
	MaxInFlight int
	// RequestTimeout 单次节点请求超时（超时归类为 KindTimeout）。
	RequestTimeout time.Duration
	// FailureThreshold 连续失败多少次摘除节点。
	FailureThreshold int
	// ProbeInterval 对已摘除节点的恢复探测间隔。
	ProbeInterval time.Duration
	// PollInterval 追链轮询间隔。
	PollInterval time.Duration
	// RateLimitBackoff 遇到限流后的退避时长。
	RateLimitBackoff time.Duration
	// StopOnCaughtUp 追平后停止（测试/一次性任务用）；常驻服务设为 false。
	StopOnCaughtUp bool
	// MaxRounds 大于 0 时，同步满该轮数后停止（测试用）；0 表示不限。
	MaxRounds int
}

// DefaultConfig 返回一组可用的默认配置。
func DefaultConfig() Config {
	return Config{
		StartHeight:      1,
		Confirmations:    6,
		FetchConcurrency: 4,
		MaxInFlight:      16,
		RequestTimeout:   2 * time.Second,
		FailureThreshold: 3,
		ProbeInterval:    2 * time.Second,
		PollInterval:     500 * time.Millisecond,
		RateLimitBackoff: 500 * time.Millisecond,
		StopOnCaughtUp:   false,
	}
}

// Indexer 是对外的门面：后台同步 + 按高度读取。
type Indexer struct {
	cfg    Config
	store  *MemoryStore
	syncer *Syncer
	log    *slog.Logger

	cancel context.CancelFunc
	done   chan struct{}
}

// New 创建索引器。
func New(cfg Config, clients []NodeClient, log *slog.Logger) (*Indexer, error) {
	if len(clients) == 0 {
		return nil, errors.New("indexer: at least one node client is required")
	}
	if log == nil {
		log = slog.Default()
	}
	store := NewMemoryStore(cfg.Confirmations)
	return &Indexer{
		cfg:    cfg,
		store:  store,
		syncer: NewSyncer(clients, store, cfg, log),
		log:    log,
	}, nil
}

// Syncer 暴露同步器（含健康状态快照），便于观测与测试。
func (i *Indexer) Syncer() *Syncer { return i.syncer }

// Start 在后台启动同步循环。
func (i *Indexer) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	i.cancel = cancel
	i.done = make(chan struct{})
	go func() {
		defer close(i.done)
		if err := i.syncer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			i.log.Warn("syncer exited with error", "error", err.Error())
		}
	}()
}

// GetBlock 按高度读取已落库区块；未同步到返回 ErrHeightNotFound。
func (i *Indexer) GetBlock(_ context.Context, height uint64) (*Block, error) {
	return i.store.GetBlock(height)
}

// Head 返回本地连续最高高度；空库 ok=false。
func (i *Indexer) Head(_ context.Context) (uint64, bool) {
	return i.store.Head()
}

// Snapshot 返回当前规范链的单次一致快照（同一读锁内），
// 重组进行中调用也只会看到完整的旧一代或新一代链，不会读到半截状态。
func (i *Indexer) Snapshot() (generation, head uint64, blocks map[uint64]Block) {
	return i.store.Snapshot()
}

// Close 停止后台同步。
func (i *Indexer) Close() {
	if i.cancel != nil {
		i.cancel()
	}
	if i.done != nil {
		<-i.done
	}
}
