package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/types"
)

// ChainBlock is one block in a mock chain.
type ChainBlock = types.Block

// Behavior controls how a mock node responds to requests.
type Behavior struct {
	// FailHeight, if >= 0, makes exactly this height return FailKind.
	FailHeight int64
	// FailHeightSet tells whether FailHeight is set.
	FailHeightSet bool
	// FailKind is the error kind returned for the failing request.
	FailKind types.ErrKind
	// FailAll makes every request fail with FailKind.
	FailAll bool
	// Latency is waited before responding (context permitting).
	Latency time.Duration
}

// MockNode is an in-process controllable fake chain node.
type MockNode struct {
	id       string
	mu       sync.RWMutex
	chain    []types.Block // indexed by height-1
	behavior Behavior
	server   *httptest.Server
}

// NewMockNode creates a mock node serving a deterministic linear chain of the
// given length: block n has hash "hash<n>" and parent "hash<n-1>".
func NewMockNode(id string, blocks int64) *MockNode {
	m := &MockNode{id: id, behavior: Behavior{FailHeight: -1}}
	m.chain = LinearChain(blocks)
	return m
}

// LinearChain builds a canonical linear chain: every node serving it agrees on
// every hash, which is what honest in-sync nodes look like.
func LinearChain(blocks int64) []types.Block {
	chain := make([]types.Block, 0, blocks)
	for h := int64(1); h <= blocks; h++ {
		chain = append(chain, types.Block{
			Height:   h,
			Hash:     HashOf(h),
			Parent:   ParentHashOf(h),
			Producer: fmt.Sprintf("validator-%d", h%3),
		})
	}
	return chain
}

// ForkChain builds a chain that agrees with LinearChain up to and including
// forkHeight, then diverges. The first block above the fork (forkHeight+1)
// keeps the canonical parent hash (the fork point); every higher block links
// via the "-alt" parent hashes.
func ForkChain(tip, forkHeight int64) []types.Block {
	chain := make([]types.Block, 0, tip)
	for h := int64(1); h <= tip; h++ {
		if h <= forkHeight {
			chain = append(chain, types.Block{
				Height: h, Hash: HashOf(h), Parent: ParentHashOf(h),
				Producer: fmt.Sprintf("validator-%d", h%3),
			})
		} else {
			parent := AltParentHashOf(h)
			if h == forkHeight+1 {
				parent = HashOf(forkHeight) // attach the fork to the canonical tip
			}
			chain = append(chain, types.Block{
				Height: h, Hash: AltHashOf(h), Parent: parent,
				Producer: fmt.Sprintf("validator-alt-%d", h%3),
			})
		}
	}
	return chain
}

// HashOf returns the canonical block hash for a height.
func HashOf(height int64) string { return fmt.Sprintf("hash%d", height) }

// ParentHashOf returns the canonical parent hash for a height.
func ParentHashOf(height int64) string { return fmt.Sprintf("hash%d", height-1) }

// AltHashOf returns the alternative (forked) block hash for a height.
func AltHashOf(height int64) string { return fmt.Sprintf("hash%d-alt", height) }

// AltParentHashOf returns the parent hash inside the alternative fork.
func AltParentHashOf(height int64) string {
	if height-1 == 0 {
		return "hash0"
	}
	if h := height - 1; h == 0 {
		return "hash0"
	}
	return fmt.Sprintf("hash%d-alt", height-1)
}

// ID returns the node identifier.
func (m *MockNode) ID() string { return m.id }

// Start serves the mock node over httptest and returns the running server.
func (m *MockNode) Start() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/block", m.handleBlock)
	mux.HandleFunc("/tip", m.handleTip)
	m.server = httptest.NewServer(mux)
	return m.server
}

// Close stops the HTTP server.
func (m *MockNode) Close() {
	if m.server != nil {
		m.server.Close()
	}
}

// SetChain replaces the served chain (used to simulate reorgs / catch-up).
func (m *MockNode) SetChain(blocks []ChainBlock) {
	m.mu.Lock()
	m.chain = append([]types.Block(nil), blocks...)
	m.mu.Unlock()
}

// ExtendChain appends one canonical block (new tip).
func (m *MockNode) ExtendChain() {
	m.mu.Lock()
	h := int64(len(m.chain)) + 1
	m.chain = append(m.chain, types.Block{
		Height: h, Hash: HashOf(h), Parent: ParentHashOf(h),
		Producer: fmt.Sprintf("validator-%d", h%3),
	})
	m.mu.Unlock()
}

// ChainTip returns the highest served height (0 for empty chain).
func (m *MockNode) ChainTip(_ context.Context) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(len(m.chain))
}

// SetBehavior replaces the response behavior.
func (m *MockNode) SetBehavior(b Behavior) {
	m.mu.Lock()
	if !b.FailHeightSet {
		b.FailHeight = -1
	}
	m.behavior = b
	m.mu.Unlock()
}

// ResetBehavior restores normal responses.
func (m *MockNode) ResetBehavior() {
	m.SetBehavior(Behavior{FailHeight: -1})
}

type errorBody struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

func statusForKind(kind types.ErrKind) int {
	switch kind {
	case types.ErrTimeout:
		return http.StatusGatewayTimeout // 504
	case types.ErrRateLimited:
		return http.StatusTooManyRequests // 429
	case types.ErrChainBusiness:
		return http.StatusNotFound // 404, e.g. height not produced yet
	default:
		return http.StatusInternalServerError // 500
	}
}

func (m *MockNode) sleep(ctx context.Context) error {
	m.mu.RLock()
	d := m.behavior.Latency
	m.mu.RUnlock()
	if d <= 0 {
		return nil
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// shouldFail decides which error to emit for a request about height (-1 = tip).
func (m *MockNode) shouldFail(height int64) (types.ErrKind, bool) {
	b := m.behavior
	if b.FailAll {
		return b.FailKind, true
	}
	if b.FailHeightSet && b.FailHeight == height {
		return b.FailKind, true
	}
	return types.ErrNone, false
}

func writeErr(w http.ResponseWriter, kind types.ErrKind, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusForKind(kind))
	_ = json.NewEncoder(w).Encode(errorBody{Kind: string(kind), Message: msg})
}

func (m *MockNode) handleBlock(w http.ResponseWriter, r *http.Request) {
	if err := m.sleep(r.Context()); err != nil {
		return
	}
	heightStr := r.URL.Query().Get("height")
	var h int64
	if _, err := fmt.Sscanf(heightStr, "%d", &h); err != nil || h < 1 {
		writeErr(w, types.ErrChainBusiness, "bad height")
		return
	}
	if kind, fail := m.shouldFail(h); fail {
		writeErr(w, kind, fmt.Sprintf("forced failure for height %d", h))
		return
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if h > int64(len(m.chain)) {
		writeErr(w, types.ErrChainBusiness, fmt.Sprintf("height %d not available, tip %d", h, len(m.chain)))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(m.chain[h-1])
}

func (m *MockNode) handleTip(w http.ResponseWriter, r *http.Request) {
	if err := m.sleep(r.Context()); err != nil {
		return
	}
	// tip requests are not affected by FailHeight; FailAll still applies.
	if kind, fail := m.shouldFail(-1); fail {
		writeErr(w, kind, "forced failure for tip")
		return
	}
	m.mu.RLock()
	tip := int64(len(m.chain))
	m.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int64{"height": tip})
}
