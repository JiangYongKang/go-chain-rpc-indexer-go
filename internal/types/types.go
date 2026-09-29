// Package types holds the shared domain types for the local block indexer.
package types

import (
	"errors"
	"fmt"
)

// Block is the minimal chain representation used by the indexer.
type Block struct {
	Height   int64
	Hash     string
	Parent   string
	Producer string
}

// Clone returns a value copy of the block.
func (b Block) Clone() Block { return b }

func (b Block) String() string {
	return fmt.Sprintf("Block(height=%d hash=%s parent=%s producer=%s)", b.Height, b.Hash, b.Parent, b.Producer)
}

// ErrKind classifies a failure reported by a node.
type ErrKind string

const (
	// ErrNone means no error.
	ErrNone ErrKind = ""
	// ErrTimeout is a request timeout / deadline exceeded.
	ErrTimeout ErrKind = "timeout"
	// ErrRateLimited is a 429-style throttling response.
	ErrRateLimited ErrKind = "rate_limited"
	// ErrNodeInternal is a 5xx / transport failure of the node itself.
	ErrNodeInternal ErrKind = "node_internal"
	// ErrChainBusiness is a chain-level rejection, e.g. height not yet available.
	ErrChainBusiness ErrKind = "chain_business"
)

// NodeError is a classified error returned by a node client.
type NodeError struct {
	Kind    ErrKind
	NodeID  string
	Message string
}

func (e *NodeError) Error() string {
	return fmt.Sprintf("node %s: %s: %s", e.NodeID, e.Kind, e.Message)
}

// AsNodeError extracts a *NodeError from err.
func AsNodeError(err error) (*NodeError, bool) {
	var ne *NodeError
	if errors.As(err, &ne) {
		return ne, true
	}
	return nil, false
}

// IsTimeout reports whether err is a timeout-class node error.
func IsTimeout(err error) bool {
	if ne, ok := AsNodeError(err); ok {
		return ne.Kind == ErrTimeout
	}
	return false
}
