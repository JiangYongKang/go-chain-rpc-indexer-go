package indexer

import (
	"fmt"
	"sync"
)

// EmptyHeight 是空库时 Head/水位的哨兵值（uint64 最大值）。
const EmptyHeight = ^uint64(0)

// MemoryStore 是带确认水位的本地内存存储。
// 所有读写都在同一把 RWMutex 下完成：重组替换在单次写锁内
// “删除旧链 + 写入新链 + 重算 head”，读取者不会看到半截状态。
type MemoryStore struct {
	mu            sync.RWMutex
	blocks        map[uint64]*Block // 当前规范链，height -> block
	confirmations uint64
	head          uint64 // 连续最高高度；空库为 EmptyHeight
	base          uint64 // 当前存储的最低高度
	hasAny        bool
	generation    uint64 // 链代次：任何提交/重组都会递增，用于一致性快照
}

// NewMemoryStore 创建存储；confirmations 为不可回退的确认深度。
func NewMemoryStore(confirmations uint64) *MemoryStore {
	return &MemoryStore{
		blocks:        make(map[uint64]*Block),
		confirmations: confirmations,
		head:          EmptyHeight,
		base:          EmptyHeight,
	}
}

// Head 返回本地连续落库的最高高度；空库 ok=false。
func (s *MemoryStore) Head() (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.head, s.hasAny
}

// ConfirmedWatermark 返回已确认（不可被回退覆盖）的最高高度；没有已确认高度时 ok=false。
// 语义：高度 h 上面至少压了 confirmations 个块（即 h+confirmations <= head）才算确认，
// 因此水位为 head-confirmations，并要求 head > confirmations。
func (s *MemoryStore) ConfirmedWatermark() (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// confirmations==0 表示关闭确认保护：0 确认即未确认，任何高度都可回退。
	if s.confirmations == 0 {
		return EmptyHeight, false
	}
	if !s.hasAny || s.head <= s.confirmations {
		return EmptyHeight, false
	}
	return s.head - s.confirmations, true
}

// isConfirmedLocked 调用方持锁时判断某高度是否已确认。
func (s *MemoryStore) isConfirmedLocked(h uint64) bool {
	if s.confirmations == 0 {
		return false
	}
	if !s.hasAny || s.head <= s.confirmations {
		return false
	}
	return h <= s.head-s.confirmations
}

// GetBlock 按高度读取。与重组提交互斥（RLock 与写 Lock 互斥）。
func (s *MemoryStore) GetBlock(height uint64) (*Block, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.blocks[height]
	if !ok {
		return nil, fmt.Errorf("%w: height=%d", ErrHeightNotFound, height)
	}
	cp := *b
	return &cp, nil
}

// Snapshot 在同一把读锁内返回当前规范链的一致快照：
// generation 标识链代次，blocks 为连续区块的拷贝。读取者用它做全链校验时，
// 看到的必然是某一代次的完整状态，不会跨重组读到“半截链”。
func (s *MemoryStore) Snapshot() (generation, head uint64, blocks map[uint64]Block) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[uint64]Block, len(s.blocks))
	for h, b := range s.blocks {
		cp[h] = *b
	}
	return s.generation, s.head, cp
}

// Has 报告某高度是否已存在区块（不返回指针，避免外部绕过锁修改）。
func (s *MemoryStore) Has(height uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.blocks[height]
	return ok
}

// WriteBlock 幂等写入单个区块，返回可区分的处理结果：
//   - 高度不存在            -> WriteInserted
//   - 同高度同哈希          -> WriteDuplicate（重复抓取，幂等跳过）
//   - 同高度不同哈希        -> WriteHashConflict（主键冲突，不覆盖，交给重组判定）
//   - 高度已被确认却要写新值 -> WriteBelowConfirmed + ErrReorgTooDeep（拒绝）
func (s *MemoryStore) WriteBlock(b *Block) (WriteOutcome, error) {
	if b == nil {
		return WriteUnknown, fmt.Errorf("nil block")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.blocks[b.Height]; ok {
		if existing.Hash == b.Hash {
			return WriteDuplicate, nil
		}
		if s.isConfirmedLocked(b.Height) {
			return WriteBelowConfirmed, fmt.Errorf("%w: height=%d confirmed", ErrReorgTooDeep, b.Height)
		}
		return WriteHashConflict, nil
	}
	if s.isConfirmedLocked(b.Height) {
		return WriteBelowConfirmed, fmt.Errorf("%w: cannot insert at confirmed height=%d", ErrReorgTooDeep, b.Height)
	}

	cp := *b
	s.blocks[b.Height] = &cp
	if !s.hasAny {
		s.hasAny = true
		s.base = b.Height
		s.head = b.Height
	} else if b.Height < s.base {
		s.base = b.Height
	}
	s.generation++
	s.advanceHeadLocked()
	return WriteInserted, nil
}

// advanceHeadLocked 从当前 head 向后扫描连续高度。
func (s *MemoryStore) advanceHeadLocked() {
	if !s.hasAny {
		return
	}
	h := s.head
	if h == EmptyHeight {
		h = s.base - 1 // 从最低高度开始
	}
	for {
		if _, ok := s.blocks[h+1]; ok {
			h++
			continue
		}
		break
	}
	s.head = h
}

// CommitBatch 在单次写锁内原子提交一段“按高度升序、父哈希相连”的新区块。
// 整批要么一起可见、要么完全不变，因此读取者绝不会看到提交进行中的半截前缀。
//
// 已存在的高度：同哈希记为 WriteDuplicate；不同哈希记为 WriteHashConflict 且
// 整批中止（不写入任何一个），由上层走重组判定。
func (s *MemoryStore) CommitBatch(blocks []*Block) ([]WriteOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	outcomes := make([]WriteOutcome, len(blocks))
	// 第一阶段：纯校验 + 预演，任何冲突/已确认冲突都整体中止。
	wantParent := ""
	if len(blocks) > 0 {
		if prev, ok := s.blocks[blocks[0].Height-1]; ok {
			wantParent = prev.Hash
		}
	}
	for i, b := range blocks {
		if b == nil {
			return nil, fmt.Errorf("commit batch: nil block at index %d", i)
		}
		if i > 0 && blocks[i-1].Height+1 != b.Height {
			return nil, fmt.Errorf("commit batch: non-contiguous heights at index %d", i)
		}
		if wantParent != "" && b.ParentHash != wantParent {
			return nil, fmt.Errorf("commit batch: parent hash mismatch at height=%d want=%s got=%s",
				b.Height, wantParent, b.ParentHash)
		}
		wantParent = b.Hash

		if existing, ok := s.blocks[b.Height]; ok {
			switch {
			case existing.Hash == b.Hash:
				outcomes[i] = WriteDuplicate
			case s.isConfirmedLocked(b.Height):
				return nil, fmt.Errorf("%w: batch conflict at confirmed height=%d",
					ErrReorgTooDeep, b.Height)
			default:
				return nil, fmt.Errorf("%w: batch conflict at height=%d",
					ErrHashConflict, b.Height)
			}
		} else {
			if s.isConfirmedLocked(b.Height) {
				return nil, fmt.Errorf("%w: cannot insert at confirmed height=%d",
					ErrReorgTooDeep, b.Height)
			}
			outcomes[i] = WriteInserted
		}
	}

	// 第二阶段：全部通过后一次性写入。
	for _, b := range blocks {
		if _, exists := s.blocks[b.Height]; !exists {
			cp := *b
			s.blocks[b.Height] = &cp
			if !s.hasAny {
				s.hasAny = true
				s.base = b.Height
				s.head = b.Height
			} else if b.Height < s.base {
				s.base = b.Height
			}
		}
	}
	s.generation++
	s.advanceHeadLocked()
	return outcomes, nil
}

// ReplaceChainResult 描述一次原子重组替换的结果。
type ReplaceChainResult struct {
	ForkPoint       uint64
	RemovedHeights  []uint64
	Outcomes        []WriteOutcome // 与传入 branch 一一对应
	NewHead         uint64
	ConfirmedHeight uint64
	HasConfirmed    bool
}

// ReplaceChain 在单次写锁内完成：校验分叉点与父哈希链 -> 删除分叉点之上的旧区块
// -> 写入新分支 -> 重算 head。读取者在整个过程中要么看到旧链要么看到新链。
//
// branch 必须按高度升序、对应高度 forkPoint+1 .. forkPoint+len(branch)。
func (s *MemoryStore) ReplaceChain(forkPoint uint64, branch []*Block) (*ReplaceChainResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	forkBlock, ok := s.blocks[forkPoint]
	if !ok {
		return nil, fmt.Errorf("replace chain: fork point block not found at height=%d", forkPoint)
	}
	if s.isConfirmedLocked(forkPoint) {
		return nil, fmt.Errorf("%w: fork point=%d at/below confirmed watermark", ErrReorgTooDeep, forkPoint)
	}
	if len(branch) == 0 {
		return nil, fmt.Errorf("replace chain: empty branch")
	}

	// 校验新分支高度连续且父哈希相连。
	wantParent := forkBlock.Hash
	for i, b := range branch {
		if b == nil {
			return nil, fmt.Errorf("replace chain: nil block at index %d", i)
		}
		wantHeight := forkPoint + 1 + uint64(i)
		if b.Height != wantHeight {
			return nil, fmt.Errorf("replace chain: branch[%d] height=%d want=%d", i, b.Height, wantHeight)
		}
		if b.ParentHash != wantParent {
			return nil, fmt.Errorf("replace chain: parent hash mismatch at height=%d: got=%s want=%s",
				b.Height, b.ParentHash, wantParent)
		}
		wantParent = b.Hash
	}

	res := &ReplaceChainResult{ForkPoint: forkPoint, NewHead: forkPoint}

	// 删除分叉点之上的旧区块。
	for h := range s.blocks {
		if h > forkPoint {
			res.RemovedHeights = append(res.RemovedHeights, h)
			delete(s.blocks, h)
		}
	}

	// 写入新分支（再次防御已确认高度）。
	for _, b := range branch {
		if s.isConfirmedLocked(b.Height) {
			// 理论不可达：删除后 head 已回退。保持防御。
			res.Outcomes = append(res.Outcomes, WriteBelowConfirmed)
			continue
		}
		cp := *b
		s.blocks[b.Height] = &cp
		res.Outcomes = append(res.Outcomes, WriteInserted)
		res.NewHead = b.Height
	}

	s.head = forkPoint
	s.generation++
	s.advanceHeadLocked()
	res.NewHead = s.head
	if wm, ok := func() (uint64, bool) {
		if s.confirmations == 0 || !s.hasAny || s.head <= s.confirmations {
			return EmptyHeight, false
		}
		return s.head - s.confirmations, true
	}(); ok {
		res.ConfirmedHeight = wm
		res.HasConfirmed = true
	}
	return res, nil
}
