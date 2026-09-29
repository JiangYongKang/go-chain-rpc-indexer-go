# go-chain-rpc-indexer-go

本地区块同步与查询组件：从**多个本地模拟节点**并发拉取区块、顺序落本地存储，
对外提供按高度读取接口。全程不依赖真实链或外部服务，用进程内假节点即可完整验证
（多节点并发、节点摘除与恢复、重复/乱序写入、链重组）。

## 快速开始

```bash
# 跑全部测试（含竞态检测）
go test ./... -race -count=1

# 端到端演示：3 个假节点 -> 初始同步 -> 节点超时摘除 -> 探测恢复 -> 链增长 -> 链重组
go run ./cmd/demo
```

## 包结构

| 文件 | 职责 |
| --- | --- |
| `client.go` | `NodeClient` 接口：节点的最小依赖（`LatestHeight` / `GetBlockByHeight`） |
| `errors.go` | 四类节点错误分类 `RPCError` / `ErrorKind`，以及哨兵错误 |
| `health.go` | `HealthManager`：健康度打分、选节点、摘除、定期探测恢复 |
| `store.go` | `MemoryStore`：幂等/顺序/原子批次写入、确认水位、原子重组、一致快照 |
| `syncer.go` | `Syncer`：目标高度多数派表决、并发抓取、顺序提交、重组定位与回退 |
| `indexer.go` | `Indexer` 门面 + `Config`；后台启动、按高度读取、一致快照、关闭 |
| `mocknode/` | 进程内假链 `Chain` 与可编排故障的假节点 `Node`（延迟/四类错误/落后/重组） |
| `cmd/demo/` | 可运行的完整场景演示 |

## 同步流程

```
                 ┌──────────────────────────── HealthManager ───────────────────────────┐
                 │ healthy ──(连续失败>=阈值)──> suspect ──(探测继续失败)──> ejected      │
   节点池 ──────▶│   ▲                          │  排序靠后/仍可服务          │ 不参与选择  │
                 │   └──── 探测连续成功 2 次 ────┴──── 定期探测恢复 <─────────┘           │
                 └───────────────▲───────────────────────────────▲───────────────────────┘
                                 │ Select(按状态/在途/延迟)        │ RecordResult / Probe
   ┌──────────────┐   1.目标高度表决(多数派)   ┌──────────────┐    │
   │   Syncer     │ ────────────────────────▶ │ LatestHeight │ ───┘
   │ (单轮循环)    │                           └──────────────┘
   │              │   2.窗口 [head+1, +N] 并发抓取（FetchConcurrency 个 worker）
   │              │      每个请求带 RequestTimeout；失败按四类区分：
   │              │        timeout/rate_limited/node_internal -> 换节点重试
   │              │        chain_business(高度未产生/已裁剪)    -> 该高度标记不可用
   │              │      抓取乱序到达没关系，结果按高度收进 map。
   │              │   3.窗口一致性复核 reconcileWindow：
   │              │      相邻块父哈希相连；可疑块向其它节点复核，
   │              │      能正确接链 => 判定陈旧来源并替换；全网一致 => 保留走重组。
   │              │   4.顺序 + 原子批次提交 CommitBatch（单写锁，整体可见）。
   │              │      结果可区分：inserted / duplicate / hash_conflict / below_confirmed。
   └──────┬───────┘
          │ 父哈希不符 / 同高度哈希冲突（需 quorum 二次确认）
          ▼
   重组处理 handleReorg：向下找分叉点 -> 校验高于确认水位 -> 原子 ReplaceChain
          │
          ▼
   ┌──────────────┐  RLock/GetBlock/Snapshot   ┌────────────┐
   │ MemoryStore  │ ◀────────────────────────  │  读取方     │  永远看不到半截链
   └──────────────┘                            └────────────┘
```

轮询：常驻模式按 `PollInterval` 循环；`StopOnCaughtUp=true` 追平即停；
`MaxRounds>0` 跑满 N 轮停（测试用）。

### 幂等与顺序（可区分结果，不重复写也不静默丢弃）

`MemoryStore.WriteBlock` 与 `CommitBatch` 对每个高度给出明确结果：

- `inserted`：高度不存在，正常写入；
- `duplicate`：同高度**同哈希**重复抓取，幂等跳过，不算错误；
- `hash_conflict`：同高度**不同哈希**主键冲突，**不覆盖、不丢弃**，转入重组判定；
- `below_confirmed`：新数据试图写入/回退已确认高度，返回 `ErrReorgTooDeep` 拒绝。

顺序保证：抓取阶段允许乱序，但提交阶段按高度升序把一个连续窗口
`CommitBatch` 原子写入（同一把写锁，先整批校验再整批落库），因此：

- 乱序到达不会造成 head 跨越缺口（`advanceHeadLocked` 只推进连续高度）；
- 读取者在批次进行中只会看到“提交前”或“提交后”的完整状态。

## 健康判定与摘除/恢复

选节点排序键（高优先级在前）：

1. **状态档位**：`healthy` 优先；`healthy` 全不可用时 `suspect` 兜底；`ejected` 不参与；
2. **在途请求数**：同档位优先把请求分给当前更空闲的节点，避免并发全压一台；
3. **延迟 EWMA**（平滑系数 0.3）：再按历史延迟从低到高；
4. **名字**：稳定兜底排序，保证可预测。

状态迁移：

- 连续失败计数达到 `FailureThreshold` -> `ejected`，日志 `node ejected`/
  `suspect node ejected after failed probe`（含节点、失败数、阈值、错误类别）；
- 失败达到阈值的一半 -> 先置 `suspect`（仍可服务、排序靠后）；
- 摘除/可疑节点由 `StartProbeLoop` 每 `ProbeInterval` 主动探测：
  - `ejected`：连续 **2 次**探测成功才恢复 `healthy`，中途任一失败计数清零；
  - `suspect`：探测成功立即恢复，继续失败则计数并在达阈值时摘除
    （解决“有健康节点时 suspect 长期不被选中、永远摘不掉”的问题）。
- 摘除期间同步不阻塞：选择器直接跳过坏节点，单节点故障不会拖慢整体；
  全部被摘时返回 `ErrNoHealthyNode`，等待探测恢复。

选节点日志 `node selected` 会打印判定依据：`tier / state / inflight /
latency_ewma / failures / healthy / suspect / ejected`。

## 错误分类

`RPCError.Kind` 四类，`KindOf(err)` 可从任意 error 提取：

| Kind | 含义 | 是否重试 |
| --- | --- | --- |
| `timeout` | 请求超时（含 `context.DeadlineExceeded`） | 是，换节点 |
| `rate_limited` | 被限流（429 一类），按 `RateLimitBackoff` 退避 | 是，换节点 |
| `node_internal` | 节点内部错误/故障（5xx 一类） | 是，换节点 |
| `chain_business` | 链上业务错误（高度尚未产生、已裁剪等） | 否，标记该高度不可用 |

## 链重组（reorg）规则

触发信号（顺序提交阶段）：

- 高度回退：所选节点 tip 低于本地 head，且该高度哈希与本地不同；
- 父哈希不符：新窗口首块的 `ParentHash != 本地(head).Hash`；
- 同高度哈希冲突：`CommitBatch` 返回 `ErrHashConflict`。

判定为“真重组”而非“单节点落后/分叉/作恶”：

- `resolveTarget` 对**目标高度做多节点多数派表决**（高度至少被 2 个节点自报才采用，
  单节点部署信任唯一来源），从源头不跟随落后节点；
- 冲突哈希再经 `conflictConfirmedByQuorum` 交叉验证：需另一不同节点返回
  **相同冲突哈希**才确认；单节点则信任该唯一来源。
- `reconcileWindow` 在窗口内部对断链块向其它节点复核：
  能取到“可正确接链”的块 => 视为陈旧来源替换；全网一致 => 视为真重组。

回退与保护：

1. 从冲突块沿 `ParentHash` 向下逐高度抓取，与本地比较直到找到哈希相同的**分叉点**；
2. 分叉点（及任何分歧高度）若 `<= ConfirmedWatermark`，直接 `ErrReorgTooDeep`
   拒绝，**已确认高度绝不被回退数据覆盖**；
3. 否则 `ReplaceChain` 在**单次写锁**内：校验新分支高度连续且父哈希相连
   -> 删除分叉点之上的旧块 -> 写入新分支 -> 重算 head，并尝试沿新链向前延伸到 tip；
4. 日志打印 `fork point found` 与 `reorg applied`（分叉点、删除高度区间、
   新分支长度、新 head、确认水位、最终状态）。

确认水位语义：高度 `h` 上面至少压了 `Confirmations` 个块
（`h + Confirmations <= head`，水位 = `head - Confirmations`）才算确认。
`Confirmations = 0` 表示关闭确认保护（0 确认=未确认，任何高度都允许回退）。

读取一致性：`GetBlock`/`Snapshot` 与重组/批次提交共用同一把 `RWMutex`；
`Snapshot` 在一次读锁内返回带 `generation`（链代次）的完整拷贝，重组进行中读取
只会看到完整旧代或完整新代，不会看到半截状态。

## 配置项（`Config`）

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `StartHeight` | `1` | 空库起始同步高度 |
| `Confirmations` | `6` | 确认深度；`head-Confirmations` 及以下不可回退，`0` 关闭保护 |
| `FetchConcurrency` | `4` | 单轮抓区块的并发 worker 数 |
| `MaxInFlight` | `16` | 单轮最大在途区块窗口大小，也限制重组时向前延伸长度 |
| `RequestTimeout` | `2s` | 单次节点请求超时，超时归类为 `timeout` |
| `FailureThreshold` | `3` | 连续失败多少次摘除节点（半数先置 suspect） |
| `ProbeInterval` | `2s` | 对 suspect/ejected 节点的恢复探测间隔 |
| `PollInterval` | `500ms` | 追链轮询间隔 |
| `RateLimitBackoff` | `500ms` | 遇限流后的退避时长 |
| `StopOnCaughtUp` | `false` | 追平即停（一次性任务/测试）；常驻服务保持 false |
| `MaxRounds` | `0` | 大于 0 时跑满 N 轮停止（主要用于测试） |

## 对外接口

```go
idx, _ := indexer.New(cfg, []indexer.NodeClient{n1, n2, n3}, logger)
idx.Start(ctx)
defer idx.Close()

b, err   := idx.GetBlock(ctx, 100)     // 按高度读取，未同步到返回 ErrHeightNotFound
head, ok := idx.Head(ctx)              // 连续最高高度
gen, head, snap := idx.Snapshot()      // 单次一致快照（带链代次）
idx.Syncer().Health().Snapshot()       // 观察各节点健康状态
```

## 用假节点编排场景

```go
chain := mocknode.NewLinearChain(1, 10)
n1 := mocknode.New("n1", chain)
n2 := mocknode.New("n2", chain)

n2.SetLatency(5 * time.Second)                       // 制造超时
k := indexer.KindRateLimited
n2.SetAlwaysError(&k)                                // 持续故障 -> 摘除
n2.SetAlwaysError(nil)                               // 恢复 -> 探测后回归
n2.FailNext(2, indexer.KindNodeInternal)             // 接下来 2 次失败
n2.SetBlockError(7, indexer.KindChainBusiness)       // 指定高度业务错误
n2.SetTip(6)                                         // 模拟落后节点
chain.Extend(3)                                      // 链增长
chain.Reorg(forkHeight, newTip)                      // 触发链重组（换哈希分支）
```

## 测试覆盖

- 节点摘除与恢复：`TestEjectionAndProbeRecovery`、`TestAllEjectedReturnsError`、
  `TestNodeFailureDoesNotSlowOrStopSync`、`TestEjectedNodeRejoinsAfterRecovery`；
- 重复/乱序写入：`TestWriteBlockIdempotentOutcomes`、`TestOutOfOrderAdvancesHead`、
  `TestDuplicateAndOutOfOrderFetches`、批次原子性见 `TestReplaceChainAtomicAndParentCheck`；
- 链重组：`TestReorgSwitchesCanonicalChain`、`TestReorgBelowConfirmedIsRejected`、
  `TestRepeatedReorgAtSameHeightStaysConsistent`、`TestMultiNodeSameReorgIsApplied`、
  `TestSingleNodeDivergenceDoesNotReorg`、`TestNodeLagIsNotTreatedAsReorg`；
- 读取一致性：`TestReadsAreConsistentDuringReorg`（多次重组并发读取，校验链代次完整）；
- 错误分类：`TestErrorKindsAreClassified`；假节点包：`mocknode/mocknode_test.go`。
