package store

import (
	"context"
	"sort"
	"sync"

	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/types"
)

// PutOutcome distinguishes the possible results of offering one block.
type PutOutcome string

const (
	// OutcomeInserted: block appended to the canonical chain (possibly via a drain).
	OutcomeInserted PutOutcome = "inserted"
	// OutcomeDuplicate: same height+hash already known (chain or buffer).
	OutcomeDuplicate PutOutcome = "duplicate"
	// OutcomeConflict: same height known with a different hash; a reorg was handled.
	OutcomeConflict PutOutcome = "conflict"
	// OutcomeOutOfOrder: height beyond the next expected height; buffered for later.
	OutcomeOutOfOrder PutOutcome = "out_of_order"
	// OutcomeRejectedFinalized: would overwrite a confirmed height; refused.
	OutcomeRejectedFinalized PutOutcome = "rejected_finalized"
)

// PutResult is the outcome plus details.
type PutResult struct {
	Outcome      PutOutcome
	Height       int64
	ExpectedNext int64
	// RollbackFrom/RollbackTo are the local heights removed during a reorg.
	// Both are 0 when no rollback happened.
	RollbackFrom int64
	RollbackTo   int64
}

// Store is the local, in-memory, concurrency-safe block store.
//
// "blocks" holds the canonical contiguous chain [1..tip]; "pending" buffers
// blocks that arrived ahead of the chain or wait for a fork to be filled below.
// A single RWMutex guarantees that readers never observe a half-applied reorg.
type Store struct {
	mu            sync.RWMutex
	confirmations int64
	blocks        map[int64]types.Block
	pending       map[int64]types.Block
	tip           int64
}

// New creates an empty store. confirmations is the number of blocks that must
// sit on top of a height for it to become immune to reorgs (0 disables it).
func New(confirmations int64) *Store {
	return &Store{
		confirmations: confirmations,
		blocks:        map[int64]types.Block{},
		pending:       map[int64]types.Block{},
		tip:           -1,
	}
}

func (s *Store) confirmedTipLocked() int64 {
	// With zero confirmations nothing is ever final; otherwise a height is
	// confirmed once at least `confirmations` blocks sit on top of it.
	if s.confirmations <= 0 || s.tip < s.confirmations {
		return -1
	}
	return s.tip - s.confirmations
}

func (s *Store) parentMatchesLocked(b types.Block) bool {
	if b.Height <= 1 {
		_, exists := s.blocks[0]
		return !exists
	}
	parent, ok := s.blocks[b.Height-1]
	return ok && parent.Hash == b.Parent
}

// nextExpectedLocked is the height that extends the canonical chain.
func (s *Store) nextExpectedLocked() int64 {
	if s.tip < 0 {
		return 1
	}
	return s.tip + 1
}

// drainLocked moves buffered blocks onto the chain while they are contiguous
// and parent-hash linked.
func (s *Store) drainLocked() {
	for {
		nb, ok := s.pending[s.tip+1]
		if !ok {
			return
		}
		if !s.parentMatchesLocked(nb) {
			return
		}
		s.blocks[nb.Height] = nb
		delete(s.pending, nb.Height)
		s.tip = nb.Height
	}
}

// applyReorgLocked handles a block that disagrees with the canonical chain:
// a different hash at an existing height, or a new extension whose parent hash
// does not match the old tip. It removes every local height from the first
// divergence point up to the old tip and buffers the incoming block, which
// stays invisible until its new ancestry is filled below it. A reorg whose
// divergence provably reaches a confirmed height is refused (nothing changes).
func (s *Store) applyReorgLocked(b types.Block) PutResult {
	h := b.Height
	oldTip := s.tip

	// Determine the first local height to remove.
	//
	// Case A — conflicting slot (h <= oldTip with a different stored hash):
	// the slot h itself must go; then walk downward following the incoming
	// block's ancestry while stored blocks disagree, to also unwind deeper
	// blocks belonging to the abandoned branch.
	//
	// Case B — wrong-parent extension (h > oldTip): start the walk at oldTip.
	from := h
	if ex, exists := s.blocks[h]; exists {
		if ex.Hash == b.Hash {
			// Defensive: should already have been handled as duplicate.
			return PutResult{Outcome: OutcomeDuplicate, Height: h, ExpectedNext: s.nextExpectedLocked()}
		}
		// Case A: h is the first conflicting slot.
		// Walk downward: does the stored block at h-1 match b.Parent? If yes it
		// is the fork point and we stop. If no, include h-1 and continue.
		claimedParent := b.Parent
		for cur := h - 1; cur >= 1; cur-- {
			anc, ok := s.blocks[cur]
			if !ok {
				break
			}
			if anc.Hash == claimedParent {
				break // cur is the common ancestor; from stays cur+1
			}
			if cur <= s.confirmedTipLocked() {
				return PutResult{Outcome: OutcomeRejectedFinalized, Height: h, ExpectedNext: s.nextExpectedLocked()}
			}
			from = cur
			claimedParent = anc.Parent
		}
	} else {
		// Case B: extension beyond tip with mismatching parent.
		claimedParent := b.Parent
		for cur := oldTip; cur >= 1; cur-- {
			anc, ok := s.blocks[cur]
			if !ok {
				from = cur + 1
				break
			}
			if anc.Hash == claimedParent {
				from = cur + 1
				break
			}
			if cur <= s.confirmedTipLocked() {
				return PutResult{Outcome: OutcomeRejectedFinalized, Height: h, ExpectedNext: s.nextExpectedLocked()}
			}
			from = cur
			claimedParent = anc.Parent
		}
	}
	if from < 1 {
		from = 1
	}

	for k := oldTip; k >= from; k-- {
		delete(s.blocks, k)
	}
	for k := range s.pending {
		if k >= from {
			delete(s.pending, k)
		}
	}
	s.tip = from - 1
	// Buffer the incoming block but do NOT drain: a conflicting block must be
	// re-submitted explicitly on the next pass (the syncer re-fetches the
	// rolled-back range), so no read can observe a freshly rewritten slot within
	// the same operation. It is simply held invisibly in the pending buffer.
	s.pending[h] = b
	return PutResult{
		Outcome:      OutcomeConflict,
		Height:       h,
		ExpectedNext: s.nextExpectedLocked(),
		RollbackFrom: from,
		RollbackTo:   oldTip,
	}
}

// Put offers one block for storage. It is idempotent: offering the same
// height+hash again yields OutcomeDuplicate rather than a second write, and a
// different hash at a known height triggers explicit reorg handling instead of
// a silent overwrite or drop.
func (s *Store) Put(_ context.Context, b types.Block) (PutResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1) Confirmed heights are immutable.
	if b.Height <= s.confirmedTipLocked() {
		if ex, ok := s.blocks[b.Height]; ok && ex.Hash == b.Hash {
			return PutResult{Outcome: OutcomeDuplicate, Height: b.Height, ExpectedNext: s.nextExpectedLocked()}, nil
		}
		return PutResult{Outcome: OutcomeRejectedFinalized, Height: b.Height, ExpectedNext: s.nextExpectedLocked()}, nil
	}

	// 2) Height already in the canonical chain.
	if b.Height <= s.tip {
		ex := s.blocks[b.Height]
		if ex.Hash == b.Hash {
			return PutResult{Outcome: OutcomeDuplicate, Height: b.Height, ExpectedNext: s.nextExpectedLocked()}, nil
		}
		return s.applyReorgLocked(b), nil
	}

	// 3) Already buffered (out-of-order duplicate or competing fork block).
	if pb, ok := s.pending[b.Height]; ok {
		if pb.Hash == b.Hash {
			// Re-submission of a buffered block: if it now extends the tip and
			// links correctly, commit it (a re-fetched block after a rollback);
			// otherwise it is a harmless duplicate of something still waiting.
			if b.Height == s.nextExpectedLocked() && s.parentMatchesLocked(b) {
				s.blocks[b.Height] = b
				delete(s.pending, b.Height)
				s.tip = b.Height
				s.drainLocked()
				return PutResult{Outcome: OutcomeInserted, Height: b.Height, ExpectedNext: s.nextExpectedLocked()}, nil
			}
			return PutResult{Outcome: OutcomeDuplicate, Height: b.Height, ExpectedNext: s.nextExpectedLocked()}, nil
		}
		s.pending[b.Height] = b
		return PutResult{Outcome: OutcomeConflict, Height: b.Height, ExpectedNext: s.nextExpectedLocked()}, nil
	}

	// 4) The immediate next height: append when linked, otherwise it signals a
	// reorg that starts at/below the current tip.
	next := s.nextExpectedLocked()
	if b.Height == next {
		if s.parentMatchesLocked(b) {
			s.blocks[b.Height] = b
			s.tip = b.Height
			s.drainLocked()
			return PutResult{Outcome: OutcomeInserted, Height: b.Height, ExpectedNext: s.nextExpectedLocked()}, nil
		}
		return s.applyReorgLocked(b), nil
	}

	// 5) Gap ahead of the tip: buffer for later, nothing is dropped.
	s.pending[b.Height] = b
	return PutResult{Outcome: OutcomeOutOfOrder, Height: b.Height, ExpectedNext: s.nextExpectedLocked()}, nil
}

// RollbackTo rewinds the canonical chain to the given height (inclusive target
// kept). It never removes confirmed heights and returns false when asked to.
// Buffered entries above the new tip are discarded. Used by reorg detection.
func (s *Store) RollbackTo(target int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if target < s.confirmedTipLocked() {
		return false
	}
	for k := s.tip; k > target; k-- {
		delete(s.blocks, k)
	}
	for k := range s.pending {
		if k > target {
			delete(s.pending, k)
		}
	}
	s.tip = target
	return true
}

// Get returns the canonical block at height. Buffered (not yet linked) blocks
// are deliberately not exposed: readers only ever see connected chain state.
func (s *Store) Get(_ context.Context, height int64) (types.Block, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.blocks[height]
	return b, ok
}

// Tip returns the highest contiguous canonical height (-1 when empty).
func (s *Store) Tip() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tip
}

// ConfirmedTip returns the highest reorg-immune height (-1 when none).
func (s *Store) ConfirmedTip() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.confirmedTipLocked()
}

// Pending returns a copy of buffered blocks (mainly for tests/observability).
func (s *Store) Pending() map[int64]types.Block {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[int64]types.Block, len(s.pending))
	for k, v := range s.pending {
		out[k] = v
	}
	return out
}

// Snapshot returns canonical blocks ordered by height.
func (s *Store) Snapshot() []types.Block {
	s.mu.RLock()
	defer s.mu.RUnlock()
	heights := make([]int64, 0, len(s.blocks))
	for h := range s.blocks {
		heights = append(heights, h)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	out := make([]types.Block, 0, len(heights))
	for _, h := range heights {
		out = append(out, s.blocks[h])
	}
	return out
}
