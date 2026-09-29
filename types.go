package indexer

import "time"

// Block 表示链上的一个区块。
type Block struct {
	Height     uint64
	Hash       string
	ParentHash string
	Time       time.Time
	Proposer   string
}

// WriteOutcome 表示单个区块落库的可区分处理结果。
type WriteOutcome int

const (
	// WriteUnknown 未处理。
	WriteUnknown WriteOutcome = iota
	// WriteInserted 新写入。
	WriteInserted
	// WriteDuplicate 同高度同哈希，重复抓取，幂等跳过。
	WriteDuplicate
	// WriteHashConflict 同高度不同哈希（主键冲突），需走重组判定。
	WriteHashConflict
	// WriteBelowConfirmed 回退数据试图覆盖已确认高度，拒绝。
	WriteBelowConfirmed
)

func (o WriteOutcome) String() string {
	switch o {
	case WriteInserted:
		return "inserted"
	case WriteDuplicate:
		return "duplicate"
	case WriteHashConflict:
		return "hash_conflict"
	case WriteBelowConfirmed:
		return "below_confirmed"
	default:
		return "unknown"
	}
}
