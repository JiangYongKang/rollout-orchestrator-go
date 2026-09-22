# 渐进式发布编排器（Go，本地可运行）

一套**不依赖任何外部服务/网络/凭据**即可运行与验证的渐进式发布编排能力。
用本地 JSON 文件做持久化，用带副作用记录的模拟器充当部署目标，
使「创建 → 放量 → 观测收敛 → 健康门控 → 暂停/恢复/回滚 → 进程重启」
在异常、边界与并发条件下结论一致且可解释。

## 快速开始

```bash
go build ./...
go test -race ./...          # 全部测试（以异常/边界链路为主），无需网络

go run ./cmd/orchestrator -data ./.rollout-data demo   # 端到端脚本演示
```

常用操作（全局 `-data` 必须在子命令之前）：

```bash
O="go run ./cmd/orchestrator -data ./.rollout-data"
$O create -id p1 -target svc-a -strategy canary -desired v2 -stable v1 -replicas 100
$O advance p1                                  # Pending -> Promoting（幂等副作用）
$O report  -id p1 -batch b1 -ver v2 -pct 5     # 观测收敛 -> Gated
$O report  -id p1 -batch b2 -ver v2 -pct 5     # 门控确认 -> 下一批 Promoting
$O pause p1 ; $O resume p1
$O rollback p1 "manual reason"
$O get p1                                      # 状态 + 每步判定 + 完整事件链
$O list
# 故障/延迟注入仅用于本地验证：
#   见 orchestrator.Service.InjectFailN / InjectLagN（CLI demo 已演示）
```

每次命令的**输入、状态迁移与判定依据**都写入事件链（`get` 可见），
运行期同时输出结构化日志（`[CREATE]/[ADVANCE]/[REPORT]/[ROLLBACK]/[RECOVER]…`）。

## 两种发布策略（共享同一套状态语义）

| 策略 | 放量单位 | 批次（默认） | 收敛语义 |
|---|---|---|---|
| `canary` | 新版本**流量百分比** | 5% → 25% → 50% → 100% | 实际百分比落在 `desired ± TolerancePct`（默认 ±2pct）即收敛，**吸收观测抖动** |
| `wave` | 新版本**副本数**（⌈比例·总数⌉，严格递增去重） | 10% → 25% → 50% → 100% | 副本数必须**精确等于**批次期望，多一个少一个都不收敛 |

两种策略对「版本必须等于期望版本」的要求一致：版本不对（延迟/缺失/回退）一律不收敛。
小集群下 wave 的批次用向上取整并去重，保证严格递增且最终等于总副本数（如 N=3 → [1,2,3]）。

## 共享状态模型

```
Pending ──advance(副作用生效)──▶ Promoting ──收敛且健康──▶ Gated
   ▲  ▲ resume                     │  ▲                     │
   │  └──────── pause ─────────────┴──┴────── pause ───────┤
   │                                                      │
   │   Gated ──门控确认 + 下一步副作用生效──▶ Promoting     │
   │                                                      │
   └── 任一步骤最后一批健康通过 ─▶ Succeeded (终态)
Promoting/Gated/Pending/Paused ──显式或自动──▶ RollingBack ─▶ RolledBack (终态)
超过有界失败上限 ─▶ Failed (终态)
```

- **Promoting**：副作用（放量）已发起，等待观测证明实际状态收敛。副作用成功本身**绝不**触发越级。
- **Gated**：当前批已收敛并健康，停在门控点；需要一批**新的**观测确认后才放下一批
  （同一批观测不能既证明收敛又证明门控，杜绝「一批观测冲两关」）。
- **Paused**：记录 `ResumeFrom` 位点，恢复时严格回到暂停前状态。
- 终态（Succeeded/RolledBack/Failed）拒绝一切后续命令。

## 期望状态与模拟实际状态的收敛

- 观测通过 `Report(batch_id, actual, metrics, observed_at)` 上报；模拟器支持
  **延迟（lag，副作用暂时不生效）、硬失败、观测抖动与短暂缺失**（见 `Service.InjectLagN/InjectFailN`）。
- **批次幂等**：同一 `batch_id` 重复上报返回 `ErrDuplicateCommand`，
  不重复放量、不跳过阶段、不重复扣减预算、不改变结论。
- 观测年龄超过 `StaleThreshold` 返回 `ErrObservationStale`，不参与收敛或门控。
- 实际状态仍是旧版本/短暂缺失：保持 Promoting；抖动在容差内：canary 收敛，wave 不收敛。

## 健康门控、失败预算与自动回滚

- `CriticalMetrics`（默认 `error_rate ≤ 0.05`、`latency_p99_ms ≤ 500`）**缺失或越界**都判不健康，
  逐条给出可解释原因；不健康时计划停留，不允许继续推进。
- `FailureBudget` 为计划级不健康新批次计数上限：预算耗尽（`budget_left < 0`）时
  **自动回滚到 `StableVer`**，事件链保留触发批次、越界指标与阈值。
- 同一不健康批次重复上报**不会**二次扣减预算（批次去重）。

## 副作用幂等、有界重试与资源上限

- 每个副作用（放量/回滚/baseline）都带 **token**。模拟器在磁盘上记录每个 token 的首次结论
  （生效/失败/延迟），**同 token 永远返回首次结论**，进程重启后重放也不重复执行。
- 编排器发起副作用前先做**目标快照对账**：若实际状态已是期望，则直接认领，
  覆盖「副作用已生效但计划状态尚未落盘」的崩溃窗口，杜绝重复放量/重复回滚。
- **只有硬失败**消耗 `MaxAttempts`；延迟（lag）不是失败，不消耗重试预算。
  达到上限进入 `Failed` 终态，错误为 `ErrRetryExhausted`，目标留在稳定版本。
- 数据损坏 / schema 不支持属于**不可重试**错误，立即上抛，不被误计为放量失败。
- `MaxPlans` 限制活动计划数量，超出返回 `ErrCapacityExceeded`；终态计划释放名额。
- 服务**不持有后台 goroutine**；锁 map 中空闲键会回收，长运行不泄漏 goroutine/文件句柄/内存。

## 持久化与断点恢复

- `plans/*.json` 与 `targets/*.json` 均以「临时文件 + rename + fsync 目录」**原子写**，
  崩溃后不存在半文件；每次状态变更整体落盘（含事件链），读改写在锁内完成。
- 记录带 `schema_version`：当前为 `1`。**旧/未来版本、截断损坏文件一律明确拒绝**
  （`ErrUnsupportedVersion` / `ErrCorruptData`），`List/Recover/Get` 都不会跳过坏数据
  或按零值默认值静默继续。
- 启动时 `Service.Recover()`（CLI 每次启动自动执行）逐计划对账：
  - RollingBack：重放在途回滚 token / 快照对账 → RolledBack，绝不二次回滚；
  - Promoting/Gated 中存在「在途副作用 token」：幂等重放，已生效则认领；
  - 其余状态原样恢复，不重新执行任何已完成副作用。

## 并发一致性

- 固定锁序 **target 锁 → plan 锁**：同一目标的并发命令（创建/推进/上报/回滚）串行化，
  不同目标/不同计划完全并行。同一 target 同时只允许一个非终态计划（`ErrTargetBusy`）。
- 竞态测试（`go test -race`）覆盖：30 路并发 Advance 只有一次有效放量、
  20 路并发创建只有一个成功、25 路相同批次上报只有一份被接受且预算不漂移。

## 错误目录（均可用 `errors.Is` 区分）

| 哨兵错误 | 触发场景 |
|---|---|
| `ErrPlanNotFound` / `ErrPlanAlreadyExists` | 计划不存在 / ID 重复创建 |
| `ErrInvalidTransition` | 违反共享状态机的迁移 |
| `ErrDuplicateCommand` | 重复批次上报 / 回滚已在途 |
| `ErrPlanFinished` | 对终态计划执行任何操作 |
| `ErrAlreadyPaused` / `ErrNotPaused` / `ErrPlanPaused` | 暂停态相关非法操作 |
| `ErrTargetBusy` | 同一目标已有活动计划 |
| `ErrObservationStale` / `ErrObservationMissing` | 观测过期 / 门控缺观测 |
| `ErrMetricMissing` / `ErrUnhealthy` | 关键指标缺失 / 越界 |
| `ErrBudgetExhausted` | 失败预算耗尽（随后自动回滚） |
| `ErrSideEffectFailed` / `ErrRetryExhausted` | 模拟硬失败 / 重试达上限 |
| `ErrUnsupportedVersion` / `ErrCorruptData` | schema 版本不支持 / 磁盘数据损坏 |
| `ErrCapacityExceeded` / `ErrInvalidArgument` | 活动计划超上限 / 入参非法 |

所有失败路径都返回非空错误并在事件链记录 `rejected` 事件；CLI 打印 `REJECTED: <原因>`，
不会静默成功或吞掉异常。

## 代码结构

```
internal/domain        状态机、模型、配置校验、哨兵错误（无外部依赖）
internal/strategy      CanaryStrategy（百分比+容差）/ WaveStrategy（精确副本）
internal/actuator      持久化、token 幂等、可注入失败/延迟的模拟目标
internal/store         plan 原子持久化、schema 校验、List/Get/Create/Save
internal/fsx           原子 JSON 写（tmp+rename+fsync）
internal/orchestrator  编排服务：命令校验/收敛/门控/预算/回滚/恢复/分片锁/上限
cmd/orchestrator       本地 CLI（create/advance/pause/resume/rollback/report/get/list/recover/demo）
```

## 测试（以异常与边界链路为主）

- `TestInvalidTransitionsAndDuplicates`：非法迁移、重复指令、终态操作
- `TestHealthGateBudgetAndAutoRollback`：缺指标、重复不漂移、预算耗尽自动回滚只回滚一次
- `TestStaleObservationRejected`：上报过期、Gated 观测过期禁止推进
- `TestLagAndActualMissingNoSkip`：延迟、版本缺失、抖动边界、不跳步不重复放量
- `TestBoundedRetrySucceeds` / `TestRetryExhaustedFailsTerminal` / `TestRetryReplayAcrossRestart`
- `TestRestartDuringDelayedRollback` / `TestRestartStateConsistency`：中断重启一致性
- `TestConcurrent*`：同计划/同目标/同批次并发竞争（`-race`）
- `TestOldVersionRejected` / `TestCorruptFileRejected` / `TestOldTargetVersionRejected` / `TestCapacityLimit`
- `TestCanaryToleranceSemantics` / `TestWaveExactReplicaSemantics` / `TestApplyIdempotentAcrossRestart`

测试日志打印每次输入、状态迁移与判定依据（使用 `t.Logf`，`go test -v` 可见）。
