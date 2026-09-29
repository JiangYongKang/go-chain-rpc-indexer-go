package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Syncer 负责并发抓取、顺序提交与重组处理。
//
// 抓取：多个 worker 各自通过 HealthManager 选节点（按健康度/延迟挑选），
// 单请求超时控制，错误按四类区分，可重试错误换节点重试，业务错误不重试。
// 提交：唯一的提交者按高度顺序写 MemoryStore，因此抓取可乱序、提交必顺序。
type Syncer struct {
	cfg     Config
	store   *MemoryStore
	health  *HealthManager
	clients []NodeClient
	log     *slog.Logger
	rounds  int
}

// NewSyncer 创建同步器。
func NewSyncer(clients []NodeClient, store *MemoryStore, cfg Config, log *slog.Logger) *Syncer {
	if cfg.FetchConcurrency < 1 {
		cfg.FetchConcurrency = 1
	}
	if cfg.MaxInFlight < 1 {
		cfg.MaxInFlight = 1
	}
	return &Syncer{
		cfg:     cfg,
		store:   store,
		health:  NewHealthManager(clients, cfg.FailureThreshold, cfg.ProbeInterval, log),
		clients: clients,
		log:     log,
	}
}

// Health 暴露健康管理器，便于观察节点状态。
func (s *Syncer) Health() *HealthManager { return s.health }

// Run 运行同步循环直到 ctx 取消；StopOnCaughtUp 时追平即返回；
// MaxRounds>0 时跑满指定轮数即返回（测试用）。
func (s *Syncer) Run(ctx context.Context) error {
	go s.health.StartProbeLoop(ctx)
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	runRound := func() bool {
		s.rounds++
		caughtUp, err := s.syncRound(ctx)
		if err != nil && ctx.Err() == nil {
			s.log.Warn("sync round failed", "error", err.Error(), "error_kind", KindOf(err).String())
		}
		return caughtUp
	}

	stop := func(caughtUp bool) bool {
		if s.cfg.MaxRounds > 0 && s.rounds >= s.cfg.MaxRounds {
			s.log.Info("max rounds reached, stopping", "rounds", s.rounds)
			return true
		}
		return caughtUp && s.cfg.StopOnCaughtUp
	}

	if stop(runRound()) {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if stop(runRound()) {
				return nil
			}
		}
	}
}

// syncRound 执行一轮：确定目标高度 -> 并发抓窗口 -> 顺序提交。
// caughtUp 为 true 表示本地已追平所选节点的最新高度。
func (s *Syncer) syncRound(ctx context.Context) (caughtUp bool, err error) {
	head, hasHead := s.store.Head()
	expected := s.cfg.StartHeight
	if hasHead {
		expected = head + 1
	}

	target, tip, err := s.resolveTarget(ctx)
	if err != nil {
		return false, err
	}

	// 所选节点落后于本地：若其 tip 是本地链的祖先，说明只是节点滞后，不算回退。
	if hasHead && target < expected {
		if local, gErr := s.store.GetBlock(target); gErr == nil && local.Hash == tip.Hash {
			s.log.Info("selected node lags behind local chain, treated as caught up",
				"node_tip", target, "local_head", head, "tip_hash", tip.Hash)
			return true, nil
		}
		s.log.Warn("height rollback detected with diverged hash, verifying quorum",
			"node_tip", target, "local_head", head,
			"node_tip_hash", tip.Hash)
		if !s.conflictConfirmedByQuorum(ctx, target, tip) {
			s.log.Warn("rollback hash not corroborated, skipping this round",
				"node_tip", target)
			return false, nil
		}
		s.log.Warn("quorum confirmed height rollback, start reorg",
			"node_tip", target, "node_tip_hash", tip.Hash)
		if rErr := s.handleReorg(ctx, tip); rErr != nil {
			return false, rErr
		}
		return false, nil
	}

	if expected > target {
		return true, nil
	}

	window := int(target - expected + 1)
	if window > s.cfg.MaxInFlight {
		window = s.cfg.MaxInFlight
	}
	end := expected + uint64(window) - 1
	s.log.Info("fetch window", "from", expected, "to", end,
		"target", target, "concurrency", s.cfg.FetchConcurrency)

	blocks, fErr := s.fetchRange(ctx, expected, end)
	if fErr != nil {
		return false, fErr
	}

	// 顺序提交：抓取允许乱序到达，这里按高度攒成“连续段”，并以单次原子批次落库，
	// 读取者在批次提交期间不会看到半截前缀。
	var batch []*Block
	for h := expected; h <= end; h++ {
		b, ok := blocks[h]
		if !ok {
			// 该高度是链上业务错误（尚未产生等），本轮到此为止。
			s.log.Info("height unavailable on nodes, pause round", "height", h)
			break
		}
		batch = append(batch, b)
	}

	// 提交前先检查批次首个块与本地链的衔接（区分重组 / 陈旧来源）。
	if len(batch) > 0 {
		first := batch[0]
		if prev, gErr := s.store.GetBlock(first.Height - 1); gErr == nil &&
			first.ParentHash != prev.Hash {
			s.log.Warn("parent hash mismatch detected",
				"height", first.Height, "local_parent", prev.Hash,
				"incoming_parent", first.ParentHash, "incoming_hash", first.Hash)
			if !s.conflictConfirmedByQuorum(ctx, first.Height, first) {
				s.log.Warn("parent mismatch not corroborated by another node, skipped round",
					"height", first.Height, "suspect_hash", first.Hash)
				return false, nil
			}
			s.log.Warn("parent hash mismatch confirmed by quorum, start reorg",
				"height", first.Height, "incoming_hash", first.Hash)
			if rErr := s.handleReorg(ctx, first); rErr != nil {
				return false, rErr
			}
			return false, nil
		}
	}

	if len(batch) > 0 {
		outcomes, cErr := s.store.CommitBatch(batch)
		if cErr != nil {
			switch {
			case errors.Is(cErr, ErrHashConflict):
				// 找到第一个冲突高度做重组判定（批次已整体回滚，本地未变）。
				var conflict *Block
				for _, b := range batch {
					if local, gErr := s.store.GetBlock(b.Height); gErr == nil &&
						local.Hash != b.Hash {
						conflict = b
						break
					}
				}
				if conflict == nil {
					return false, cErr
				}
				s.log.Warn("batch hash conflict, verifying with other nodes",
					"height", conflict.Height, "incoming_hash", conflict.Hash)
				if !s.conflictConfirmedByQuorum(ctx, conflict.Height, conflict) {
					s.log.Warn("hash conflict not corroborated, skipped round",
						"height", conflict.Height, "suspect_hash", conflict.Hash)
					return false, nil
				}
				s.log.Warn("hash conflict confirmed by quorum, start reorg",
					"height", conflict.Height, "incoming_hash", conflict.Hash)
				if rErr := s.handleReorg(ctx, conflict); rErr != nil {
					return false, rErr
				}
				return false, nil
			case errors.Is(cErr, ErrReorgTooDeep):
				s.log.Error("batch rejected below confirmed watermark", "error", cErr.Error())
				return false, cErr
			default:
				return false, cErr
			}
		}
		inserted, duplicates := 0, 0
		for _, o := range outcomes {
			switch o {
			case WriteInserted:
				inserted++
			case WriteDuplicate:
				duplicates++
			}
		}
		newHead, _ := s.store.Head()
		s.log.Info("batch committed atomically",
			"from", batch[0].Height, "to", batch[len(batch)-1].Height,
			"inserted", inserted, "duplicates", duplicates, "head", newHead)
	}

	head, _ = s.store.Head()
	caughtUp = head >= target
	s.log.Info("round finished", "head", head, "target", target, "caught_up", caughtUp)
	return caughtUp, nil
}

// resolveTarget 对所有可用节点做“最新高度”多数派表决，避免跟随单个落后节点。
//
// 收集每个节点自报的 (height)，取出现次数最多的最高高度：
//   - 单节点部署：信任唯一来源；
//   - 多节点：该高度至少被 2 个不同节点自报才采用；
//     票数相同时取更高高度（更倾向跟随多数健康节点的链头）。
//
// 返回选出的目标高度及从任一投票节点取到的该高度 tip 区块。
func (s *Syncer) resolveTarget(ctx context.Context) (uint64, *Block, error) {
	type vote struct {
		client NodeClient
		height uint64
	}
	var votes []vote
	probed := map[string]bool{}
	maxAttempts := len(s.clients) * 2

	for attempts := 0; attempts < maxAttempts && len(probed) < len(s.clients); attempts++ {
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}
		client, reason, sErr := s.health.Select(ctx)
		if sErr != nil {
			break
		}
		if probed[client.Name()] {
			time.Sleep(time.Millisecond)
			continue
		}
		probed[client.Name()] = true
		reqCtx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
		start := time.Now()
		height, err := client.LatestHeight(reqCtx)
		latency := time.Since(start)
		cancel()
		s.health.RecordResult(client.Name(), latency, err)
		if err != nil {
			s.log.Warn("latest height probe failed during target vote",
				"node", client.Name(), "error_kind", KindOf(err).String(),
				"error", err.Error())
			continue
		}
		votes = append(votes, vote{client: client, height: height})
		s.log.Info("target vote", "node", client.Name(), "height", height,
			"select_reason", reason)
	}
	if len(votes) == 0 {
		return 0, nil, ErrNoHealthyNode
	}

	// 统计每个高度票数。
	count := map[uint64]int{}
	var byNode = map[uint64]NodeClient{}
	for _, v := range votes {
		count[v.height]++
		byNode[v.height] = v.client
	}
	need := 2
	if len(s.clients) <= 1 {
		need = 1
	}
	var best uint64
	bestVotes := 0
	for h, c := range count {
		if c >= need && (c > bestVotes || (c == bestVotes && h > best)) {
			best, bestVotes = h, c
		}
	}
	if bestVotes == 0 {
		// 没有高度拿到足够票数（例如多节点高度全不一致）：
		// 退化为取得票最高且最高的高度，至少保证能继续。
		for _, v := range votes {
			if v.height > best {
				best = v.height
			}
		}
		s.log.Warn("no height reached quorum, falling back to max reported",
			"fallback_height", best, "votes", len(votes))
	}

	tip, err := s.fetchHeightFrom(ctx, byNode[best], best)
	if err != nil {
		// 该投票节点恰好拿不到自己 tip，换任意可用节点重试。
		tip, _, err = s.fetchHeight(ctx, best)
		if err != nil {
			return 0, nil, err
		}
	}
	s.log.Info("target resolved by quorum",
		"target", best, "votes", bestVotes, "need", need,
		"responded", len(votes), "tip_hash", tip.Hash)
	return best, tip, nil
}

// backoff 在限流/重试时做短暂等待；ctx 取消则立即返回。
func (s *Syncer) backoff(ctx context.Context, kind ErrorKind) {
	d := s.cfg.PollInterval / 4
	if kind == KindRateLimited && s.cfg.RateLimitBackoff > 0 {
		d = s.cfg.RateLimitBackoff
	}
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// fetchRange 并发抓取 [from,to] 的区块。
// 可重试错误在单高度内换节点重试；链上业务错误作为该高度不可用标记（map 中缺项）。
func (s *Syncer) fetchRange(ctx context.Context, from, to uint64) (map[uint64]*Block, error) {
	total := int(to-from) + 1
	tasks := make(chan uint64, total)
	results := make(chan fetchResult, total)
	for h := from; h <= to; h++ {
		tasks <- h
	}
	close(tasks)

	workers := s.cfg.FetchConcurrency
	if workers > total {
		workers = total
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for h := range tasks {
				b, node, err := s.fetchHeight(ctx, h)
				select {
				case results <- fetchResult{height: h, block: b, err: err, node: node}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	wg.Wait()
	close(results)

	blocks := make(map[uint64]*Block, total)
	for r := range results {
		if r.err != nil {
			kind := KindOf(r.err)
			if kind == KindChainBusiness {
				s.log.Warn("chain business error for height, marked unavailable",
					"height", r.height, "node", r.node, "error", r.err.Error())
				continue
			}
			return nil, fmt.Errorf("fetch height %d failed (kind=%s): %w", r.height, kind, r.err)
		}
		blocks[r.height] = r.block
	}
	s.reconcileWindow(ctx, from, blocks)
	return blocks, nil
}

// reconcileWindow 校验窗口内区块的父子链一致性，并对可疑块换节点重取。
//
// 背景：并发抓取时不同 worker 可能命中不同健康度的节点，少数落后节点会返回
// 陈旧高度的数据，表现为“父哈希对不上”。这里不直接当重组，而是先向其他节点
// 复核：若多数节点给出另一条连续链，则保留；复核后仍断裂的高度从结果中剔除，
// 由下一轮在多数派 target 下重新抓取。
func (s *Syncer) reconcileWindow(ctx context.Context, from uint64, blocks map[uint64]*Block) {
	// 期望的首个父哈希：本地已存在 from-1 时以本地为准。
	wantParent := ""
	if prev, err := s.store.GetBlock(from - 1); err == nil {
		wantParent = prev.Hash
	}
	for h := from; ; h++ {
		b, ok := blocks[h]
		if !ok {
			return
		}
		if wantParent != "" && b.ParentHash != wantParent {
			// 可疑：向其他节点复核该高度，区分“陈旧来源”与“真实重组”。
			s.log.Warn("window chain broken, verifying height against other nodes",
				"height", h, "want_parent", wantParent,
				"got_parent", b.ParentHash, "suspect_hash", b.Hash)
			switch rep, consensus, vErr := s.verifyHeight(ctx, h, b.Hash, wantParent); {
			case vErr != nil:
				// 无法形成任何结论（其他节点都在报错）：剔除该高度，下一轮再试。
				s.log.Warn("height unverifiable, dropping from this window",
					"height", h, "error_kind", KindOf(vErr).String())
				delete(blocks, h)
				return
			case rep != nil:
				// 找到另一个节点给出“能正确接链”的块：原来源陈旧，替换。
				s.log.Info("stale source replaced with chaining block from another node",
					"height", h, "new_hash", rep.Hash)
				blocks[h] = rep
				b = rep
			case consensus:
				// 其他节点与可疑块哈希一致（或单节点无法交叉验证）：
				// 这是真实重组信号，保留区块，交给提交阶段做多数派确认与回退。
				s.log.Info("conflicting block is node-wide consensus, keep for reorg handling",
					"height", h, "hash", b.Hash)
				return
			default:
				s.log.Warn("no consensus for height, dropping from this window",
					"height", h)
				delete(blocks, h)
				return
			}
		}
		wantParent = b.Hash
	}
}

// verifyHeight 对窗口内可疑高度做交叉复核。
//
// 返回：
//   - rep != nil：某个不同节点给出了哈希不同、且父哈希==wantParent 的可接链区块（陈旧来源）；
//   - consensus=true：没有可接链区块，但其它节点（含单节点情形）都认同 suspectHash（真实重组）；
//   - err != nil：所有其它节点都取不到数据，无法判定。
func (s *Syncer) verifyHeight(ctx context.Context, h uint64, suspectHash, wantParent string,
) (rep *Block, consensus bool, err error) {
	probed := map[string]bool{}
	agree := 0
	maxAttempts := len(s.clients) + 2
	reachable := 0
	for attempts := 0; attempts < maxAttempts; attempts++ {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		if len(probed) >= len(s.clients) {
			break
		}
		client, _, sErr := s.health.Select(ctx)
		if sErr != nil {
			break
		}
		if probed[client.Name()] {
			time.Sleep(time.Millisecond)
			continue
		}
		probed[client.Name()] = true
		other, fErr := s.fetchHeightFrom(ctx, client, h)
		if fErr != nil {
			s.log.Warn("verify probe failed, ignoring source",
				"height", h, "node", client.Name(),
				"error_kind", KindOf(fErr).String())
			continue
		}
		reachable++
		if other.ParentHash == wantParent && other.Hash != suspectHash {
			return other, false, nil // 找到能正确接链的另一来源
		}
		if other.Hash == suspectHash {
			agree++
		}
	}
	if reachable == 0 {
		return nil, false, ErrNoHealthyNode
	}
	// 单节点或全网一致认同可疑哈希 => 真实重组。
	need := 2
	if len(s.clients) <= 1 {
		need = 1
	}
	return nil, agree >= need, nil
}

type fetchResult struct {
	height uint64
	block  *Block
	err    error
	node   string
}

// fetchHeight 通过健康管理器反复选节点抓取单个高度，直到成功、遇到业务错误或 ctx 取消。
func (s *Syncer) fetchHeight(ctx context.Context, h uint64) (*Block, string, error) {
	attempt := 0
	for {
		attempt++
		client, _, err := s.health.Select(ctx)
		if err != nil {
			return nil, "", err
		}
		b, err := s.fetchHeightFrom(ctx, client, h)
		if err == nil {
			return b, client.Name(), nil
		}
		kind := KindOf(err)
		if !kind.Retryable() {
			// 业务错误（高度不存在等）：原样返回，由上层标记不可用。
			return nil, client.Name(), err
		}
		s.log.Warn("fetch block failed, retrying on another node",
			"height", h, "node", client.Name(), "attempt", attempt,
			"error_kind", kind.String(), "error", err.Error())
		s.backoff(ctx, kind)
		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}
	}
}

// fetchHeightFrom 对单个节点发起一次带超时的区块请求并上报健康结果。
func (s *Syncer) fetchHeightFrom(ctx context.Context, client NodeClient, h uint64) (*Block, error) {
	reqCtx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()
	start := time.Now()
	b, err := client.GetBlockByHeight(reqCtx, h)
	s.health.RecordResult(client.Name(), time.Since(start), err)
	if err != nil {
		return nil, err
	}
	cp := *b
	return &cp, nil
}

// handleReorg 处理重组：以冲突区块为起点向下找分叉点，确认分叉点高于
// 已确认水位后，原子替换本地链。已确认高度绝不被覆盖。
func (s *Syncer) handleReorg(ctx context.Context, conflict *Block) error {
	if wm, ok := s.store.ConfirmedWatermark(); ok && conflict.Height <= wm {
		return fmt.Errorf("%w: conflict height=%d <= confirmed=%d",
			ErrReorgTooDeep, conflict.Height, wm)
	}

	s.log.Info("locating fork point", "conflict_height", conflict.Height,
		"incoming_hash", conflict.Hash)

	// newChain 收集“冲突块及其祖先”，高度降序。
	newChain := []*Block{conflict}
	cur := conflict
	var forkPoint uint64
	found := false
	for cur.Height > 0 {
		h := cur.Height - 1
		parent, err := s.fetchMust(ctx, h)
		if err != nil {
			return fmt.Errorf("reorg: fetch ancestor at %d: %w", h, err)
		}
		if local, gErr := s.store.GetBlock(h); gErr == nil {
			if wm, ok := s.store.ConfirmedWatermark(); ok && h <= wm {
				return fmt.Errorf("%w: diverged height=%d <= confirmed=%d",
					ErrReorgTooDeep, h, wm)
			}
			if local.Hash == parent.Hash {
				forkPoint = h
				found = true
				s.log.Info("fork point found", "fork_point", h, "hash", parent.Hash)
				break
			}
			s.log.Info("height diverged, walk back further",
				"height", h, "local_hash", local.Hash, "incoming_hash", parent.Hash)
		} else {
			s.log.Info("height absent locally, walk back further", "height", h)
		}
		newChain = append(newChain, parent)
		cur = parent
	}
	if !found {
		// 走到创世高度仍不一致：根部分叉，拒绝（防止覆盖已确认的低高度）。
		return fmt.Errorf("%w: no common ancestor above genesis", ErrReorgTooDeep)
	}

	// 反转成升序分支：forkPoint+1 .. conflict.Height。
	branch := make([]*Block, 0, len(newChain))
	for i := len(newChain) - 1; i >= 0; i-- {
		branch = append(branch, newChain[i])
	}

	// 尝试沿新链向前延伸到节点 tip（最多延伸一个窗口，避免无限追）。
	tip, _, err := s.fetchHeight(ctx, conflict.Height+1)
	extended := 0
	for err == nil && tip != nil && uint64(extended) < uint64(s.cfg.MaxInFlight) {
		if tip.ParentHash != branch[len(branch)-1].Hash {
			break
		}
		branch = append(branch, tip)
		extended++
		next, _, nErr := s.fetchHeight(ctx, tip.Height+1)
		if nErr != nil {
			break
		}
		tip = next
	}

	res, rErr := s.store.ReplaceChain(forkPoint, branch)
	if rErr != nil {
		s.log.Error("reorg replace rejected", "fork_point", forkPoint,
			"branch_len", len(branch), "error", rErr.Error())
		return rErr
	}
	s.log.Info("reorg applied",
		"fork_point", res.ForkPoint,
		"removed_heights", removedRange(res.RemovedHeights),
		"new_branch_len", len(branch),
		"new_head", res.NewHead,
		"confirmed_height", confirmedLabel(res),
		"final_state", "canonical_switched")
	return nil
}

// conflictConfirmedByQuorum 判断某个“与本地冲突”的区块是否得到足够节点背书，
// 以区分“真实链重组”和“单个节点落后/分叉/作恶”。
//
// 规则：
//   - 只有 1 个节点时无法交叉验证，信任该唯一来源（假节点环境与最简部署）；
//   - 多于 1 个节点时，需要除首发节点外再有 >=1 个不同节点返回相同区块哈希，
//     即至少 2 个独立来源背书才确认为真重组。
//
// 探测只走健康管理器选节点，失败节点会被跳过，不会拖垮同步。
func (s *Syncer) conflictConfirmedByQuorum(ctx context.Context, h uint64, suspect *Block) bool {
	if len(s.clients) <= 1 {
		s.log.Info("quorum check: single node, trusting sole source",
			"height", h, "hash", suspect.Hash)
		return true
	}

	votes := 1 // 首发节点本身算一票
	probed := map[string]bool{}
	maxAttempts := len(s.clients) + 2

	for attempts := 0; attempts < maxAttempts; attempts++ {
		if ctx.Err() != nil {
			return false
		}
		// 所有其他节点都已取过证。
		if len(probed) >= len(s.clients) {
			break
		}
		client, _, err := s.health.Select(ctx)
		if err != nil {
			break
		}
		if probed[client.Name()] {
			time.Sleep(time.Millisecond)
			continue
		}
		probed[client.Name()] = true
		other, err := s.fetchHeightFrom(ctx, client, h)
		if err != nil {
			s.log.Warn("quorum probe failed, ignoring this source",
				"height", h, "node", client.Name(),
				"error_kind", KindOf(err).String())
			continue
		}
		matches := other.Hash == suspect.Hash
		if matches {
			votes++
		}
		s.log.Info("quorum vote", "height", h, "node", client.Name(),
			"hash", other.Hash, "suspect_hash", suspect.Hash,
			"matches", matches, "votes", votes, "need", 2)
		if votes >= 2 {
			s.log.Info("quorum confirmed reorg", "height", h, "votes", votes)
			return true
		}
	}
	s.log.Warn("quorum not reached, conflict attributed to single source",
		"height", h, "votes", votes, "probed", len(probed))
	return false
}

// fetchMust 抓取区块，业务错误同样视为失败（重组路径上祖先必须存在）。
func (s *Syncer) fetchMust(ctx context.Context, h uint64) (*Block, error) {
	b, _, err := s.fetchHeight(ctx, h)
	return b, err
}

func removedRange(heights []uint64) [2]uint64 {
	if len(heights) == 0 {
		return [2]uint64{}
	}
	minH, maxH := heights[0], heights[0]
	for _, h := range heights {
		if h < minH {
			minH = h
		}
		if h > maxH {
			maxH = h
		}
	}
	return [2]uint64{minH, maxH}
}

func confirmedLabel(r *ReplaceChainResult) any {
	if !r.HasConfirmed {
		return "none"
	}
	return r.ConfirmedHeight
}
