package indexer

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrorKind 区分节点返回错误的类别。
type ErrorKind int

const (
	// KindUnknown 未知错误。
	KindUnknown ErrorKind = iota
	// KindTimeout 超时（context deadline exceeded、io 超时等）。
	KindTimeout
	// KindRateLimited 被限流（HTTP 429 一类）。
	KindRateLimited
	// KindNodeInternal 节点内部错误（5xx、节点故障）。
	KindNodeInternal
	// KindChainBusiness 链上业务错误（如请求高度尚未存在/已被裁剪）。
	KindChainBusiness
)

func (k ErrorKind) String() string {
	switch k {
	case KindTimeout:
		return "timeout"
	case KindRateLimited:
		return "rate_limited"
	case KindNodeInternal:
		return "node_internal"
	case KindChainBusiness:
		return "chain_business"
	default:
		return "unknown"
	}
}

// Retryable 报告该类错误是否适合换节点/稍后重试。
func (k ErrorKind) Retryable() bool {
	switch k {
	case KindTimeout, KindRateLimited, KindNodeInternal:
		return true
	default:
		return false
	}
}

// RPCError 是带分类的节点错误。
type RPCError struct {
	Kind       ErrorKind
	Node       string
	Msg        string
	RetryAfter time.Duration
}

func (e *RPCError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("rpc error from node %q: kind=%s: %s", e.Node, e.Kind, e.Msg)
}

// NewRPCError 构造一个分类错误。
func NewRPCError(kind ErrorKind, node, msg string) *RPCError {
	return &RPCError{Kind: kind, Node: node, Msg: msg}
}

// KindOf 从任意错误中提取 ErrorKind；普通 error 归为 KindUnknown。
func KindOf(err error) ErrorKind {
	if err == nil {
		return KindUnknown
	}
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		return rpcErr.Kind
	}
	// context 超时也归为超时类。
	if errors.Is(err, context.DeadlineExceeded) {
		return KindTimeout
	}
	return KindUnknown
}

var (
	// ErrNoHealthyNode 当前没有可挑选的健康节点。
	ErrNoHealthyNode = errors.New("no healthy node available")
	// ErrHeightNotFound 本地没有该高度的区块。
	ErrHeightNotFound = errors.New("block height not found")
	// ErrReorgTooDeep 重组回退到了已确认水位以内，拒绝覆盖。
	ErrReorgTooDeep = errors.New("reorg exceeds confirmation depth, rejected")
	// ErrHashConflict 批量提交中某高度与本地哈希不一致（需走重组判定）。
	ErrHashConflict = errors.New("block hash conflict at existing height")
)
