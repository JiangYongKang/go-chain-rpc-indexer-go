package indexer

import (
	"errors"
	"testing"
)

func mkBlock(h uint64, hash, parent string) *Block {
	return &Block{Height: h, Hash: hash, ParentHash: parent}
}

func TestWriteBlockIdempotentOutcomes(t *testing.T) {
	s := NewMemoryStore(0)

	b := mkBlock(1, "h1", "h0")
	if o, err := s.WriteBlock(b); err != nil || o != WriteInserted {
		t.Fatalf("first write: outcome=%v err=%v, want inserted", o, err)
	}
	// 同高度同哈希：重复抓取，幂等跳过。
	if o, err := s.WriteBlock(mkBlock(1, "h1", "h0")); err != nil || o != WriteDuplicate {
		t.Fatalf("repeat same block: outcome=%v err=%v, want duplicate", o, err)
	}
	// 同高度不同哈希：主键冲突，不能覆盖也不能静默丢弃。
	if o, err := s.WriteBlock(mkBlock(1, "h1-prime", "h0")); err != nil || o != WriteHashConflict {
		t.Fatalf("conflicting block: outcome=%v err=%v, want hash_conflict", o, err)
	}
	got, err := s.GetBlock(1)
	if err != nil || got.Hash != "h1" {
		t.Fatalf("conflicting write must not overwrite, got hash=%q err=%v", gotHash(got), err)
	}
}

func TestOutOfOrderAdvancesHead(t *testing.T) {
	s := NewMemoryStore(0)
	// 先写高后写低（乱序到达），head 必须在 1 号块补齐后连续推进。
	if o, _ := s.WriteBlock(mkBlock(3, "h3", "h2")); o != WriteInserted {
		t.Fatalf("write 3 outcome=%v", o)
	}
	if head, ok := s.Head(); !ok || head != 3 {
		t.Fatalf("after only block 3, head=%d,%v", head, ok)
	}
	if o, _ := s.WriteBlock(mkBlock(2, "h2", "h1")); o != WriteInserted {
		t.Fatalf("write 2 outcome=%v", o)
	}
	// 仍缺 1，head 不应跨越缺口。
	if head, _ := s.Head(); head != 3 {
		t.Fatalf("head before block1 = %d", head)
	}
	if o, _ := s.WriteBlock(mkBlock(1, "h1", "h0")); o != WriteInserted {
		t.Fatalf("write 1 outcome=%v", o)
	}
	if head, ok := s.Head(); !ok || head != 3 {
		t.Fatalf("head after fill = %d,%v", head, ok)
	}
	if _, err := s.GetBlock(99); !errors.Is(err, ErrHeightNotFound) {
		t.Fatalf("missing height error = %v, want ErrHeightNotFound", err)
	}
}

func TestConfirmedWatermarkAndProtection(t *testing.T) {
	// confirmations=3：head 至少 4 时水位=head-3。
	s := NewMemoryStore(3)
	for h := uint64(1); h <= 6; h++ {
		if _, err := s.WriteBlock(mkBlock(h, "h"+itoaTest(h), "h"+itoaTest(h-1))); err != nil {
			t.Fatal(err)
		}
	}
	wm, ok := s.ConfirmedWatermark()
	if !ok || wm != 3 { // head=6 -> 6-3=3
		t.Fatalf("watermark=%d,%v, want 3", wm, ok)
	}
	// 已确认高度 3 试图写不同哈希 -> 拒绝。
	o, err := s.WriteBlock(mkBlock(3, "h3-fork", "h2"))
	if o != WriteBelowConfirmed || !errors.Is(err, ErrReorgTooDeep) {
		t.Fatalf("confirmed rewrite: outcome=%v err=%v, want below_confirmed+ErrReorgTooDeep", o, err)
	}
}

func TestReplaceChainAtomicAndParentCheck(t *testing.T) {
	s := NewMemoryStore(0)
	for h := uint64(1); h <= 5; h++ {
		_, _ = s.WriteBlock(mkBlock(h, "g0-h"+itoaTest(h), "g0-h"+itoaTest(h-1)))
	}
	// 父哈希链不合法必须整体失败，且本地数据不动。
	bad := []*Block{mkBlock(3, "x3", "WRONG_PARENT")}
	if _, err := s.ReplaceChain(2, bad); err == nil {
		t.Fatal("expected parent-hash validation error")
	}
	if h, _ := s.Head(); h != 5 {
		t.Fatalf("store mutated after failed replace, head=%d", h)
	}
	if got, _ := s.GetBlock(3); got.Hash != "g0-h3" {
		t.Fatalf("block 3 mutated to %q", got.Hash)
	}

	// 合法替换：分叉点 2，删除 3..5，写入新分支 3..4（更短分叉）。
	good := []*Block{
		mkBlock(3, "g1-h3", "g0-h2"),
		mkBlock(4, "g1-h4", "g1-h3"),
	}
	res, err := s.ReplaceChain(2, good)
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	if res.ForkPoint != 2 || res.NewHead != 4 {
		t.Fatalf("result fork=%d head=%d", res.ForkPoint, res.NewHead)
	}
	if len(res.RemovedHeights) != 3 { // 3,4,5
		t.Fatalf("removed=%v", res.RemovedHeights)
	}
	if b2, _ := s.GetBlock(2); b2.Hash != "g0-h2" {
		t.Fatal("fork point block must remain untouched")
	}
	if b3, _ := s.GetBlock(3); b3.Hash != "g1-h3" {
		t.Fatalf("block3 hash=%q, want g1-h3", b3.Hash)
	}
	if _, err := s.GetBlock(5); !errors.Is(err, ErrHeightNotFound) {
		t.Fatalf("old block5 should be gone, err=%v", err)
	}
}

func TestReplaceChainRejectsConfirmedForkPoint(t *testing.T) {
	// confirmations=2，head=6 -> 水位 4；分叉点 4 处于确认区，必须拒绝。
	s := NewMemoryStore(2)
	for h := uint64(1); h <= 6; h++ {
		_, _ = s.WriteBlock(mkBlock(h, "g0-h"+itoaTest(h), "g0-h"+itoaTest(h-1)))
	}
	branch := []*Block{mkBlock(5, "g1-h5", "g0-h4")}
	_, err := s.ReplaceChain(4, branch)
	if !errors.Is(err, ErrReorgTooDeep) {
		t.Fatalf("err=%v, want ErrReorgTooDeep", err)
	}
}

func itoaTest(u uint64) string {
	if u == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	return string(b[i:])
}

func gotHash(b *Block) string {
	if b == nil {
		return "<nil>"
	}
	return b.Hash
}
