package sync

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/health"
	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/store"
	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/types"
)

// NodeClient is the minimal node interface the syncer needs.
type NodeClient interface {
	ID() string
	GetBlock(ctx context.Context, height int64) (types.Block, error)
	LatestHeight(ctx context.Context) (int64, error)
}

// Config controls the sync loop.
type Config struct {
	// PollInterval is the idle wait between catch-up passes.
	PollInterval time.Duration
	// RequestTimeout bounds a single RPC.
	RequestTimeout time.Duration
	// Concurrency bounds in-flight block fetches.
	Concurrency int
	// BatchHeight bounds how far ahead of the local tip one pass may fetch.
	BatchHeight int64
	// ReorgLookback bounds how many recent heights are hash-verified each pass
	// when the syncer is caught up (0 uses the default).
	ReorgLookback int64
	// Health configures ejection/recovery behavior.
	Health health.Config
	// Logger receives decisions and outcomes. Nil uses the default logger.
	Logger *log.Logger
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		PollInterval:   10 * time.Millisecond,
		RequestTimeout: 2 * time.Second,
		Concurrency:    4,
		BatchHeight:    32,
		ReorgLookback:  32,
		Health:         health.DefaultConfig(),
	}
}

// Syncer orchestrates fetching, health tracking and ordered storage.
type Syncer struct {
	clients []NodeClient
	byID    map[string]NodeClient
	store   *store.Store
	health  *health.Monitor
	cfg     Config
	log     *log.Logger
}

// New builds a syncer.
func New(clients []NodeClient, st *store.Store, cfg Config) *Syncer {
	if cfg.PollInterval <= 0 || cfg.RequestTimeout <= 0 || cfg.Concurrency <= 0 || cfg.BatchHeight <= 0 {
		d := DefaultConfig()
		if cfg.PollInterval <= 0 {
			cfg.PollInterval = d.PollInterval
		}
		if cfg.RequestTimeout <= 0 {
			cfg.RequestTimeout = d.RequestTimeout
		}
		if cfg.Concurrency <= 0 {
			cfg.Concurrency = d.Concurrency
		}
		if cfg.BatchHeight <= 0 {
			cfg.BatchHeight = d.BatchHeight
		}
		if cfg.ReorgLookback <= 0 {
			cfg.ReorgLookback = d.ReorgLookback
		}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	}
	ids := make([]string, len(clients))
	byID := make(map[string]NodeClient, len(clients))
	for i, c := range clients {
		ids[i] = c.ID()
		byID[c.ID()] = c
	}
	return &Syncer{
		clients: clients,
		byID:    byID,
		store:   st,
		health:  health.NewMonitor(ids, cfg.Health),
		cfg:     cfg,
		log:     logger,
	}
}

// Health exposes the monitor (for tests / status endpoints).
func (s *Syncer) Health() *health.Monitor { return s.health }

// targetHeight asks healthy nodes for their tip and returns the maximum.
func (s *Syncer) targetHeight(ctx context.Context) (int64, error) {
	var maxTip int64 = -1
	var lastErr error
	anyOK := false
	for _, c := range s.clients {
		tip, err := c.LatestHeight(ctx)
		if err != nil {
			kind := types.ErrNodeInternal
			if ne, ok := types.AsNodeError(err); ok {
				kind = ne.Kind
			}
			s.health.RecordFailure(c.ID(), kind)
			s.log.Printf("sync: tip_query node=%s result=error kind=%s err=%q",
				c.ID(), kind, err.Error())
			lastErr = err
			continue
		}
		anyOK = true
		s.health.RecordSuccess(c.ID())
		if tip > maxTip {
			maxTip = tip
		}
	}
	if !anyOK {
		return -1, fmt.Errorf("no node could report a tip: %w", lastErr)
	}
	return maxTip, nil
}

type fetchResult struct {
	height int64
	block  types.Block
	nodeID string
	err    error
}

// fetchFromAny tries nodes in health order until one serves height. Within one
// attempt every currently-available node is tried at most once; if none serves
// it, the error is returned and the next pass retries.
func (s *Syncer) fetchFromAny(ctx context.Context, height int64) fetchResult {
	tried := map[string]bool{}
	for {
		id, ok := s.health.Pick()
		if !ok {
			return fetchResult{height: height, err: errors.New("all nodes ejected")}
		}
		if tried[id] {
			// Every currently-available node has been tried once this round.
			return fetchResult{height: height, err: errors.New("no healthy node served height " + itoa(height))}
		}
		tried[id] = true
		client := s.byID[id]
		rctx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
		b, err := client.GetBlock(rctx, height)
		cancel()
		if err == nil {
			s.health.RecordSuccess(id)
			s.log.Printf("sync: height=%d selected_node=%s decision=healthiest_available result=fetched hash=%s",
				height, id, b.Hash)
			return fetchResult{height: height, block: b, nodeID: id}
		}
		kind := types.ErrNodeInternal
		if ne, ok := types.AsNodeError(err); ok {
			kind = ne.Kind
		}
		s.health.RecordFailure(id, kind)
		s.log.Printf("sync: height=%d tried_node=%s result=fetch_error kind=%s err=%q -> trying next",
			height, id, kind, err.Error())
	}
}

func itoa(h int64) string {
	return strconv.FormatInt(h, 10)
}

// fetchBatch concurrently fetches the inclusive range [from,to].
func (s *Syncer) fetchBatch(ctx context.Context, from, to int64) []fetchResult {
	n := int(to-from) + 1
	results := make([]fetchResult, n)
	sem := make(chan struct{}, s.cfg.Concurrency)
	var wg sync.WaitGroup
	for i := range results {
		h := from + int64(i)
		wg.Add(1)
		sem <- struct{}{}
		go func(idx, height int64) {
			defer wg.Done()
			defer func() { <-sem }()
			results[idx] = s.fetchFromAny(ctx, height)
		}(int64(i), h)
	}
	wg.Wait()
	return results
}

// detectReorg re-fetches unconfirmed blocks from the tip downward until it
// finds a height whose stored and remote hash agree. On the first mismatch it
// rewinds the store to the divergence point's parent and returns true, so the
// next catch-up pass re-fetches everything above. Confirmed heights are never
// rewound.
func (s *Syncer) detectReorg(ctx context.Context) bool {
	tip := s.store.Tip()
	if tip < 1 {
		return false
	}
	confirmed := s.store.ConfirmedTip()

	// Confirmed heights are never scanned for rewrites. The scan is also bounded
	// by ReorgLookback so that, on a long chain, verification is cheap; the floor
	// is the higher of the confirmed boundary and the lookback window edge.
	floor := confirmed + 1
	if lb := s.cfg.ReorgLookback; lb > 0 {
		if edge := tip - lb + 1; edge > floor {
			floor = edge
		}
	}

	// Find the common ancestor scanning from the tip downward down to floor.
	commonAncestor := int64(-1)
	var topBlock types.Block
	topNode := ""
	for h := tip; h >= floor; h-- {
		r := s.fetchFromAny(ctx, h)
		if r.err != nil {
			s.log.Printf("sync: reorg_check height=%d unavailable err=%q (skip this pass)", h, r.err)
			return false
		}
		local, ok := s.store.Get(ctx, h)
		if !ok {
			return false
		}
		if local.Hash == r.block.Hash {
			commonAncestor = h
			break
		}
		if topBlock.Height == 0 {
			topBlock = r.block
			topNode = r.nodeID
		}
		s.log.Printf("sync: reorg_check height=%d node=%s local_hash=%s remote_hash=%s divergence=true",
			h, r.nodeID, local.Hash, r.block.Hash)
	}

	// No agreement inside the scan window.
	if topBlock.Height == 0 {
		// Every scanned height agreed: no divergence near the tip.
		return false
	}

	// The lowest scanned height disagreed. Decide whether that is a confirmed
	// boundary (refuse) or simply the lookback window edge (rewind to floor-1
	// and re-fetch; resolution continues on subsequent passes).
	if floor == confirmed+1 {
		// The scan reached the confirmed boundary height itself: compare it to
		// distinguish a safe fork (boundary agrees) from a too-deep fork.
		if confirmed >= 1 {
			r := s.fetchFromAny(ctx, confirmed)
			if r.err != nil {
				s.log.Printf("sync: reorg_check confirmed height=%d unavailable err=%q (skip)", confirmed, r.err)
				return false
			}
			if local, ok := s.store.Get(ctx, confirmed); ok && local.Hash == r.block.Hash {
				commonAncestor = confirmed
				s.log.Printf("sync: reorg_check confirmed height=%d node=%s hash_agrees -> fork starts just above finalized data",
					confirmed, r.nodeID)
			}
		}
		if commonAncestor < 0 {
			h := confirmed + 1
			r := s.fetchFromAny(ctx, h)
			s.log.Printf("sync: reorg_check no_common_ancestor above confirmed_tip=%d lowest_unconfirmed=%d -> rejected_finalized",
				confirmed, h)
			if r.err == nil {
				res, _ := s.store.Put(ctx, r.block)
				s.log.Printf("sync: height=%d node=%s apply=%s confirmed_tip=%d",
					h, r.nodeID, res.Outcome, confirmed)
			}
			return false
		}
	} else {
		// Disagreement only down to the lookback edge: treat floor-1 as the
		// rollback target; deeper resolution happens on following passes.
		commonAncestor = floor - 1
		s.log.Printf("sync: reorg_check divergence extends to lookback_floor=%d -> rewinding to %d (deeper resolution on next passes)",
			floor, commonAncestor)
	}

	// Rewind to the common ancestor and let normal catch-up refill above it.
	if !s.store.RollbackTo(commonAncestor) {
		s.log.Printf("sync: reorg_check cannot rewind to common_ancestor=%d", commonAncestor)
		return false
	}
	s.log.Printf("sync: reorg_check common_ancestor=%d node_at_tip=%s rewound; next pass re-fetches from %d",
		commonAncestor, topNode, commonAncestor+1)
	return true
}

// SyncOnce performs one catch-up pass: ask for the chain tip, fetch everything
// missing up to it concurrently, then apply results strictly in height order.
// When caught up, it verifies the unconfirmed tail against a node so that a
// reorg at an already-synced height is detected and repaired.
func (s *Syncer) SyncOnce(ctx context.Context) (int, error) {
	target, err := s.targetHeight(ctx)
	if err != nil {
		return 0, err
	}
	from := s.store.Tip() + 1
	if from < 1 {
		from = 1
	}
	if from > target {
		// Caught up: verify the tail for reorgs before idling.
		if s.detectReorg(ctx) {
			// Returning nil lets the caller immediately run another pass to
			// re-fetch the rewound range.
			return 0, nil
		}
		return 0, nil
	}
	to := target
	if to-from+1 > s.cfg.BatchHeight {
		to = from + s.cfg.BatchHeight - 1
	}

	results := s.fetchBatch(ctx, from, to)

	applied := 0
	// Apply in strict height order. A failure / gap stops this pass; the next
	// pass retries from whatever height is still missing.
	for i, r := range results {
		h := from + int64(i)
		if r.err != nil {
			s.log.Printf("sync: height=%d apply=skipped reason=all_nodes_failed err=%q", h, r.err)
			return applied, r.err
		}
		// Defensive: results must be indexed by height.
		if r.height != h {
			s.log.Printf("sync: height=%d apply=skipped reason=index_mismatch got=%d", h, r.height)
			return applied, fmt.Errorf("internal: result index mismatch at %d", h)
		}
		res, perr := s.store.Put(ctx, r.block)
		if perr != nil {
			return applied, perr
		}
		switch res.Outcome {
		case store.OutcomeInserted:
			applied++
			s.log.Printf("sync: height=%d node=%s apply=inserted hash=%s tip=%d",
				h, r.nodeID, r.block.Hash, s.store.Tip())
		case store.OutcomeDuplicate:
			s.log.Printf("sync: height=%d node=%s apply=duplicate hash=%s (no second write)",
				h, r.nodeID, r.block.Hash)
		case store.OutcomeOutOfOrder:
			s.log.Printf("sync: height=%d node=%s apply=out_of_order buffered expected=%d",
				h, r.nodeID, res.ExpectedNext)
		case store.OutcomeConflict:
			s.log.Printf("sync: height=%d node=%s apply=conflict reorg rollback=%d..%d new_hash=%s -> refetch_from=%d",
				h, r.nodeID, res.RollbackFrom, res.RollbackTo, r.block.Hash, res.RollbackFrom)
			return applied, nil
		case store.OutcomeRejectedFinalized:
			s.log.Printf("sync: height=%d node=%s apply=rejected_finalized confirmed_tip=%d",
				h, r.nodeID, s.store.ConfirmedTip())
		}
	}
	return applied, nil
}

// probeFn is the recovery probe handed to the health monitor.
func (s *Syncer) probeFn(ctx context.Context, id string) error {
	c := s.byID[id]
	rctx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()
	if _, err := c.LatestHeight(rctx); err != nil {
		return err
	}
	return nil
}

// Run executes catch-up passes and recovery probes until ctx is cancelled.
func (s *Syncer) Run(ctx context.Context) error {
	probeCtx, cancelProbes := context.WithCancel(ctx)
	defer cancelProbes()
	go s.health.RunProbeLoop(probeCtx, s.probeFn)

	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if _, err := s.SyncOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			s.log.Printf("sync: pass_error err=%q (will retry)", err.Error())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
