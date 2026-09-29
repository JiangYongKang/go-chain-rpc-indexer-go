package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/types"
)

func canonical(h int64) types.Block {
	return types.Block{Height: h, Hash: fmt.Sprintf("h%d", h), Parent: fmt.Sprintf("h%d", h-1), Producer: "v"}
}

// forkBlock returns a block of the fork that diverges ABOVE forkHeight:
// forkHeight+1 keeps the canonical parent hash (the fork point), every higher
// block links via "-alt" parent hashes.
func forkBlock(h, forkHeight int64) types.Block {
	hash := fmt.Sprintf("h%d-alt", h)
	parent := fmt.Sprintf("h%d-alt", h-1)
	if h == forkHeight+1 {
		parent = fmt.Sprintf("h%d", h-1)
	}
	return types.Block{Height: h, Hash: hash, Parent: parent, Producer: "va"}
}

func TestPutLinearInsertThenDuplicate(t *testing.T) {
	s := New(0)
	ctx := context.Background()
	for h := int64(1); h <= 3; h++ {
		res, err := s.Put(ctx, canonical(h))
		if err != nil || res.Outcome != OutcomeInserted {
			t.Fatalf("h=%d want inserted, got %v err=%v", h, res.Outcome, err)
		}
	}
	if s.Tip() != 3 {
		t.Fatalf("tip=%d want 3", s.Tip())
	}
	// Re-offer every height: must be duplicate, never a second write.
	for h := int64(1); h <= 3; h++ {
		res, _ := s.Put(ctx, canonical(h))
		if res.Outcome != OutcomeDuplicate {
			t.Fatalf("h=%d want duplicate, got %v", h, res.Outcome)
		}
	}
	if len(s.Snapshot()) != 3 {
		t.Fatalf("snapshot len=%d want 3 (no double writes)", len(s.Snapshot()))
	}
}

func TestPutOutOfOrderBufferedThenDrained(t *testing.T) {
	s := New(0)
	ctx := context.Background()

	// Height 3 arrives first: buffered, not dropped.
	res, _ := s.Put(ctx, canonical(3))
	if res.Outcome != OutcomeOutOfOrder {
		t.Fatalf("want out_of_order, got %v", res.Outcome)
	}
	if s.Tip() != -1 {
		t.Fatalf("tip must stay -1 while gap exists, got %d", s.Tip())
	}
	if _, ok := s.Get(ctx, 3); ok {
		t.Fatal("buffered block must not be readable as canonical")
	}

	// Duplicate of the buffered block.
	res, _ = s.Put(ctx, canonical(3))
	if res.Outcome != OutcomeDuplicate {
		t.Fatalf("buffered duplicate want duplicate, got %v", res.Outcome)
	}

	if _, err := s.Put(ctx, canonical(1)); err != nil {
		t.Fatal(err)
	}
	res, _ = s.Put(ctx, canonical(2))
	if res.Outcome != OutcomeInserted {
		t.Fatalf("height 2 should insert and drain 3, got %v", res.Outcome)
	}
	if s.Tip() != 3 {
		t.Fatalf("after drain tip=%d want 3", s.Tip())
	}
	b3, ok := s.Get(ctx, 3)
	if !ok || b3.Hash != canonical(3).Hash {
		t.Fatalf("drained block wrong: %+v ok=%v", b3, ok)
	}
	if len(s.Pending()) != 0 {
		t.Fatalf("pending should be empty after drain, got %v", s.Pending())
	}
}

func TestReorgAtTipRollbackAndRefill(t *testing.T) {
	s := New(0)
	ctx := context.Background()
	// canonical chain 1..4
	for h := int64(1); h <= 4; h++ {
		if _, err := s.Put(ctx, canonical(h)); err != nil {
			t.Fatal(err)
		}
	}
	// alt4 forks above height 3: same parent as canonical4, different hash.
	res, _ := s.Put(ctx, forkBlock(4, 3))
	if res.Outcome != OutcomeConflict {
		t.Fatalf("want conflict, got %v", res.Outcome)
	}
	if res.RollbackFrom != 4 || res.RollbackTo != 4 {
		t.Fatalf("rollback range=%d..%d want 4..4", res.RollbackFrom, res.RollbackTo)
	}
	if s.Tip() != 3 {
		t.Fatalf("tip after rollback=%d want 3", s.Tip())
	}
	if _, ok := s.Get(ctx, 4); ok {
		t.Fatal("height 4 must not be visible after rollback")
	}
	// Re-offer alt4: now it is the immediate child and links cleanly.
	res, _ = s.Put(ctx, forkBlock(4, 3))
	if res.Outcome != OutcomeInserted {
		t.Fatalf("alt4 refill want inserted, got %v", res.Outcome)
	}
	if s.Tip() != 4 {
		t.Fatalf("tip=%d want 4 after refill", s.Tip())
	}
	if b, _ := s.Get(ctx, 4); b.Hash != forkBlock(4, 3).Hash {
		t.Fatalf("height4 hash=%s want alt", b.Hash)
	}
	if b, _ := s.Get(ctx, 3); b.Hash != canonical(3).Hash {
		t.Fatalf("height3 hash=%s must remain canonical", b.Hash)
	}
}

func TestReorgDeepFork(t *testing.T) {
	s := New(0)
	ctx := context.Background()
	for h := int64(1); h <= 6; h++ {
		if _, err := s.Put(ctx, canonical(h)); err != nil {
			t.Fatal(err)
		}
	}
	// alt6 forks above height 3; its ancestry disagrees at 5 and 4.
	res, _ := s.Put(ctx, forkBlock(6, 3))
	if res.Outcome != OutcomeConflict || res.RollbackFrom != 5 || res.RollbackTo != 6 {
		t.Fatalf("want conflict 5..6, got %+v", res)
	}
	if s.Tip() != 4 {
		t.Fatalf("tip=%d want 4", s.Tip())
	}
	// alt5's parent (alt4) still disagrees with canonical4 -> another rollback.
	res, _ = s.Put(ctx, forkBlock(5, 3))
	if res.Outcome != OutcomeConflict || res.RollbackFrom != 4 || res.RollbackTo != 4 {
		t.Fatalf("want conflict 4..4, got %+v", res)
	}
	if s.Tip() != 3 {
		t.Fatalf("tip=%d want 3 (at fork point)", s.Tip())
	}
	// Refill from the fork point. alt5 stayed buffered during rollback, so
	// committing alt4 drains up through alt5 automatically; alt6 (above the
	// rolled-back fork edge) was discarded and must be re-fetched.
	r, _ := s.Put(ctx, forkBlock(4, 3))
	if r.Outcome != OutcomeInserted {
		t.Fatalf("alt4 want inserted (drains buffered alt5), got %v", r.Outcome)
	}
	if s.Tip() != 5 {
		t.Fatalf("tip=%d want 5 after alt4 drains alt5", s.Tip())
	}
	r, _ = s.Put(ctx, forkBlock(5, 3))
	if r.Outcome != OutcomeDuplicate {
		t.Fatalf("buffered alt5 now linked -> duplicate on resubmit, got %v", r.Outcome)
	}
	r, _ = s.Put(ctx, forkBlock(6, 3))
	if r.Outcome != OutcomeInserted {
		t.Fatalf("re-fetched alt6 want inserted, got %v", r.Outcome)
	}
	if s.Tip() != 6 {
		t.Fatalf("tip=%d want 6 after refill", s.Tip())
	}
	// Re-offering any of them is now idempotent.
	for h := int64(4); h <= 6; h++ {
		if r, _ := s.Put(ctx, forkBlock(h, 3)); r.Outcome != OutcomeDuplicate {
			t.Fatalf("alt%d re-offer want duplicate, got %v", h, r.Outcome)
		}
	}
	if b, _ := s.Get(ctx, 3); b.Hash != canonical(3).Hash {
		t.Fatal("fork point must remain canonical")
	}
	if b, _ := s.Get(ctx, 4); b.Hash != forkBlock(4, 3).Hash {
		t.Fatalf("height4 hash=%s want alt", b.Hash)
	}
	if b, _ := s.Get(ctx, 6); b.Hash != forkBlock(6, 3).Hash {
		t.Fatalf("height6 hash=%s want alt", b.Hash)
	}
}

func TestConfirmedHeightImmuneToReorg(t *testing.T) {
	const conf int64 = 2
	s := New(conf)
	ctx := context.Background()
	for h := int64(1); h <= 5; h++ {
		if _, err := s.Put(ctx, canonical(h)); err != nil {
			t.Fatal(err)
		}
	}
	if s.ConfirmedTip() != 3 {
		t.Fatalf("confirmed tip=%d want 3", s.ConfirmedTip())
	}
	// A block that would rewrite height 2 (confirmed) must be refused outright.
	bad := types.Block{Height: 2, Hash: "h2-evil", Parent: "h1", Producer: "x"}
	res, _ := s.Put(ctx, bad)
	if res.Outcome != OutcomeRejectedFinalized {
		t.Fatalf("want rejected_finalized, got %v", res.Outcome)
	}
	if b, _ := s.Get(ctx, 2); b.Hash != canonical(2).Hash {
		t.Fatalf("confirmed height was overwritten: %s", b.Hash)
	}
	if s.Tip() != 5 {
		t.Fatalf("tip=%d must stay 5 after refusal", s.Tip())
	}

	// Unconfirmed reorg (height 4) still works.
	r, _ := s.Put(ctx, forkBlock(4, 3))
	if r.Outcome != OutcomeConflict {
		t.Fatalf("want conflict at unconfirmed height, got %v", r.Outcome)
	}
}

func TestConfirmedDuplicateStillIdempotent(t *testing.T) {
	s := New(2)
	ctx := context.Background()
	for h := int64(1); h <= 5; h++ {
		if _, err := s.Put(ctx, canonical(h)); err != nil {
			t.Fatal(err)
		}
	}
	// Re-offer a confirmed block with the same hash: duplicate, not refusal.
	res, _ := s.Put(ctx, canonical(1))
	if res.Outcome != OutcomeDuplicate {
		t.Fatalf("want duplicate for confirmed same-hash, got %v", res.Outcome)
	}
}
