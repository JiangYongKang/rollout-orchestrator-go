# rollout-orchestrator-go

本地可运行、可验证的渐进式发布编排器。以本地 JSON 文件持久化 + 模拟目标环境驱动发布计划，
保证创建、推进、暂停、回滚与进程重启后的结论一致且可解释。无外部服务、凭据或网络依赖。

## 发布策略语义

三种策略共享同一套状态机、同一套阶段位点与恢复规则
（应用期望状态 → 等待多维门控收敛 → 进入下一阶段）：

| 策略 | 阶段序列 | 语义 |
| --- | --- | --- |
| `canary` | 10% → 50% → 100% | 逐批扩大新版本副本数与流量，每批都需收敛且通过多维门控 |
| `bluegreen` | deploy-green(0% 流量, 全量副本) → switch-traffic(100%) | 先全量部署 green 但不接流量，观测通过后一次性切换 |
| `batch` | 由 `-batches` / `BatchSizes` 配置 | **按固定副本数分批**：每批新增固定数量新版本副本 |

### batch（固定副本分批）

- 批次数 = `BatchSizes` 数组长度；每个元素是**该批新增**的新版本副本数。
- 每阶段的期望承载 = 前面各批累计副本数；期望流量占比 = 累计副本 / 总副本数。
- 例：`-replicas 10 -batches 2,3,5` 产生三批：累计 `2(20%) → 5(50%) → 10(100%)`。
- 各批总和**允许小于**承载上限（即分批放量不要求最后全量），但**不得超过**。
- 与 canary/bluegreen 完全共用状态、恢复、暂停/继续/回滚与多维门控；
  支持查询、暂停、继续、回滚，进程重启后从一致位点继续。

非法配置一律返回 `ErrInvalidArgument`，**不会**凑成默认批次继续跑：

- 批次列表为空（未配置 `-batches`）；
- 任一批次副本数 ≤ 0；
- 各批累计副本数超过总副本数（目标承载上限）。

## 多维阶段门控（判定看哪些维度）

一个阶段必须**同时**满足以下维度才算真正完成，缺一不可（结论与逐维度期望值/观测值
都记录在 `Plan.Verdicts` 供审计）：

1. **生效版本**：观测的 `EffectiveVersion` == 该阶段期望版本（计划的 `ToVersion`）；
2. **流量占比**：观测的 `TrafficPercent` ≥ 该阶段期望占比；
3. **实际承载**：观测的 `Replicas` == 该阶段期望副本数（精确收敛，不多不少）；
4. 健康维度：观测未过期、关键指标齐全、样本数达标、错误率不超阈值。

判定结论代码：`ok` / `stale-observation` / `version-mismatch` / `traffic-not-reached` /
`replicas-not-converged` / `missing-metric` / `insufficient-samples` / `error-rate-exceeded`。

**严格防误判：**

- 阶段期望状态应用时记录观测水位线 `GateSeq`；**只有该阶段应用之后产生的新观测**
  （按单调递增、不受环形裁剪影响的观测序号 `Seq` 比较）才能让该阶段通过。
- 因此**前面阶段的观测不能拿来当后面阶段的通过依据，一次观测也不可能同时让两个阶段
  通过**；进入新阶段后必须等到新上报。
- 实际状态延迟、抖动或短暂未上报时，只判“尚未收敛”并继续等待，**不提前放行、不跳过
  阶段、不改写结论**。
- 只有“观测已在当前阶段产生、且内容表明新版本本身不健康”（指标缺失 / 样本不足 /
  错误率超标）才计入失败预算；连续达到 `FailureBudget` 自动回滚。收敛慢不算发布失败。

## 状态模型

```
Pending(隐含) → Running ⇄ Paused
Running → RollingBack → RolledBack   (手动或健康门控自动触发)
Running/RollingBack → Failed         (副作用重试耗尽)
```

阶段位点（三种策略共用，用于无重复放量与断点恢复）：

- `CurrentStep`：最后一个**已通过门控**的阶段下标（-1 表示尚无阶段通过）；
- `PendingStep`：已应用期望、正在等待门控的阶段下标（-1 无在途阶段）；
- `PendingApplied`：在途阶段的期望状态是否**已确认成功下发并落盘**
  （false=已登记位点但下发未确认，true=下发已确认）；
- `GateSeq`：在途阶段认可的最早观测序号（新观测水位线）。

推进一个阶段严格按三段同步落盘，崩溃只可能停在其中两个窗口，重启都能自愈：

1. **登记**：`PendingStep=n, PendingApplied=false, GateSeq=NextSeq` 同步落盘；
2. **下发**：对目标 `Apply(version, replicas, percent)`（幂等，有界重试）；
3. **确认**：下发成功后 `PendingApplied=true` 再次同步落盘，随后才进入门控等待。

只有没有在途阶段时才会进入下一阶段，所以重复调用、超时重试、进程重启都不会
重复放量或跳过阶段。

- 终态：`Succeeded` / `RolledBack` / `Failed`。对终态计划的任何操作返回 `ErrPlanFinished`。
- 非法迁移（如 Paused 时 Advance、重复 Pause）返回 `ErrInvalidTransition`。
- 每个变更命令携带 `cmdID`，命令在门控通过那一刻入账；重复命令返回
  `ErrDuplicateCommand`，不产生重复副作用。超时/重启后用同一 `cmdID` 继续等待在途
  阶段是允许的（此时副作用早已应用，不会重复）。

## 同一目标互斥规则

- **同一目标同一时刻只允许有一个进行中（非终态）的发布**；在它成功 / 回滚 / 失败前，
  对该目标再创建发布一律返回 `ErrTargetBusy`（CLI 退出码 7），与参数错误
  （`ErrInvalidArgument`）、非法状态（`ErrInvalidTransition`）等原因明确区分。
- 同一目标上的**创建 / 推进 / 暂停 / 回滚按目标互斥锁串行**（加锁顺序：目标锁 →
  计划锁），不会重复放量、半更新或状态跳级；**不同目标之间并行、互不影响**。
- 进行中计划集合随存储持久化，**进程重启后同一目标的互斥约束照样生效**。
- 计划到达终态后释放目标，之后允许在该目标上新建发布。

## 收敛、上报与有界状态

- `Advance` 应用期望副本数/流量后，等待一条满足全部维度的**新观测**。
- 实际状态由 `SimApplier` 模拟：承载逐步收敛（延迟）、流量占比即时生效，
  可注入抖动、指标缺失与短暂目标缺失。
- 观测按 `ReportID` 幂等去重：重复上报返回 `ErrDuplicateReport`，计数不变。
- 观测、已见上报 ID、已应用命令 ID 均按 `MaxObservations` 环形裁剪；判定审计封顶
  `verdictCap=1000`。**长期运行批次数不会增多、状态记录不会无限膨胀。**
- 落盘采用去抖合并：高频上报只更新内存并由单个刷盘协程周期性原子写，状态迁移 /
  位点变更 / 终态 / 关闭仍同步落盘，恢复语义不变。

## 持久化、断点恢复与旧数据兼容

- 单文件 JSON 存储（当前模式版本 **v2**），写盘采用临时文件 + rename 原子替换。
- JSON 损坏返回 `ErrStoreCorrupt`。
- **兼容范围（不静默按缺省值解释）：**
  - **v2**：当前格式，直接读取；
  - **v1**（旧：观测无版本/流量维度、位点只有单个 `currentStep`）：**可读取并迁移**，
    位点按下述确定性规则映射，随后继续推进的门控结论与新格式一致：
    - 进行中：该阶段视为“在途”，重新等待多维门控，且必须使用迁移后产生的**新观测**；
    - 已暂停：视为停在上一已通过位点，恢复后从下一阶段开始；
    - 给旧观测回填稳定序号，`NextSeq` 不回退；旧观测缺版本/流量（零值），在多维门控
      下天然不可能通过（空版本 ≠ 期望版本、0 流量达不到正期望），因此不会被当成功解释。
  - **其它版本：明确拒绝**并返回 `ErrStoreVersion`，可与损坏/参数错误区分。
- 副作用（`Apply`）幂等：崩溃发生在“已应用未落盘确认”窗口时，重启后重放同一期望状态安全；
  进行中阶段重启后重新等待门控、不重复放量。
- **推进中断后的重启恢复（本仓库重点保证）**：进程在一个阶段“刚推进、这批期望还没
  真正下发出去”时被杀掉，磁盘上只会留下 `PendingStep=n, PendingApplied=false`。
  重启（`NewEngine`）时按下发确认位决定动作，表现与没中断过一致：
  - **断在下发之前**（登记后、Apply 前崩溃）：重启自动**补发同一批期望**
    （相同 version/replicas/percent），随后照常等待门控并一路推进到成功；
  - **断在下发过程中**（Apply 已返回或副作用可能已落地、确认位未落盘）：重启补发
    **完全相同**的期望；`Apply` 幂等，目标以该期望为准收敛，不叠加放量、不改乱
    实际状态或位点记录，也不会因此多走一个阶段（`PendingStep` 始终指向同一阶段）；
  - **断在下发确认之后**（等待门控期间崩溃）：重启**只重新等待门控，不再下发**；
  - **不重启、仅超时/取消重入**：未确认则补发同期望，已确认则只等待；同一 `cmdID`
    在门控通过、命令入账前允许继续等待同一阶段，门控通过后再用即为 `ErrDuplicateCommand`；
  - **与暂停穿插**：`Paused` 计划挂着未确认在途时重启，**不会在暂停状态下自动放量**，
    而是丢弃该在途位点（保留 `CurrentStep`），Resume 后对**同一阶段**重新应用并等待；
  - **与回滚穿插**：`RollingBack` 是崩溃可能留下的中间态，重启幂等重放回滚并落定
    `RolledBack`（回滚副作用重试耗尽则 `Failed`），回滚断在前/后窗口结论一致。
  恢复期间的补发/回滚若重试耗尽：计划落 `Failed` 并同步落盘，`NewEngine` 返回
  包装了 `ErrRetryExhausted` 的错误（CLI 退出码 5），可与参数不合法
  （`ErrInvalidArgument`）、非法状态（`ErrInvalidTransition`）、目标被占
  （`ErrTargetBusy`）、存储版本/损坏等原因用 `errors.Is` 明确区分。
- `RollingBack` 是崩溃可能留下的中间态，重启时幂等重放回滚并落定 `RolledBack`/`Failed`。
- 命令幂等表（`AppliedCmds`）随计划持久化，重启后重复命令仍被拒绝。

## 错误分类（CLI 退出码）

| 退出码 | 含义 |
| --- | --- |
| 2 | 参数错误（含 batch 非法配置） / 计划不存在 |
| 3 | 非法迁移 / 终态操作 / 重复命令 / 重复上报 |
| 4 | 健康门控拒绝 / 观测过期 / 版本或流量不符 / 承载未收敛 |
| 5 | 重试耗尽 / 资源超限 |
| 6 | 存储版本不兼容 / 数据损坏 |
| 7 | **同一目标已有进行中发布（`ErrTargetBusy`）** |

## 本地验证

```bash
# 运行全部测试（异常与边界链路为主，含竞态检测，可多轮）
go test -race -count=2 ./...
go test -v ./...          # 日志打印每条用例的输入、状态变化与逐维度判定依据

go build -o /tmp/rolloutctl ./cmd/rolloutctl

# 固定副本分批：非法配置必须被拒
/tmp/rolloutctl -store /tmp/c.json create -id b1 -target svcX -strategy batch \
  -to v2 -replicas 10 -batches 4,4,4 -cmd x        # exit=2，累计 12 > 10

# 合法分批 + 模拟推进三批
/tmp/rolloutctl -store /tmp/c.json create -id b1 -target svcX -strategy batch \
  -to v2 -replicas 10 -batches 2,3,5 -cmd c1
/tmp/rolloutctl -store /tmp/c.json create -id b2 -target svcX -strategy canary -cmd c2  # exit=7 目标忙
for i in 0 1 2; do /tmp/rolloutctl -store /tmp/c.json advance -id b1 -cmd a$i -simulate; done
/tmp/rolloutctl -store /tmp/c.json get -id b1     # 查看 verdicts 逐维度判定依据

# 多维门控：版本不对不放行，全对才放行
/tmp/rolloutctl -store /tmp/m.json create -id m1 -target svcM -strategy canary -cmd c1
/tmp/rolloutctl -store /tmp/m.json report -id m1 -report-id r1 -version v9 -traffic 10 -replicas 1
/tmp/rolloutctl -store /tmp/m.json advance -id m1 -cmd a1 -timeout 400ms   # exit=4 版本不符
/tmp/rolloutctl -store /tmp/m.json report -id m1 -report-id r2 -version v2 -traffic 10 -replicas 1
/tmp/rolloutctl -store /tmp/m.json advance -id m1 -cmd a2                  # 通过

# 一键演示固定副本分批完整流程
/tmp/rolloutctl -store /tmp/d.json demo -strategy batch -replicas 10 -batches 2,3,5
```

### 推进中断恢复的本地验证

```bash
# 只跑中断恢复专项：-v 会打印每个用例的计划输入（各阶段期望签名）、崩溃窗口、
# 重启后位点、逐维度门控判定（期望/实测版本、流量、副本）与最终下发序列。
go test -run TestRecovery -v ./...

# 典型输出（断在下发之前 -> 重启自动补发 -> 一路成功）：
#   === crash window: BEFORE applying step 0 desired state ===
#   recovered site: state=Running currentStep=-1 pendingStep=0 pendingApplied=true
#   [drive] cmd=a2 -> state=Running currentStep=1 ... code=ok got(v=v2,t=50,r=5)
#   [drive] cmd=a3 -> state=Succeeded currentStep=2 ... code=ok got(v=v2,t=100,r=10)
#   actual application sequence: [...r=2|t=20 ...r=5|t=50 ...r=10|t=100]   # 无重复、无跳阶段

# 全量回归（含竞态），可多轮重复：
go test -race -count=2 ./...
```

判定方法：看三条断言性日志——`pendingApplied=true`（重启已自动补发）、
每阶段 `code=ok` 且 `currentStep` 严格 +1（不跳阶段）、`actual application sequence`
压缩连续同签名后与阶段序列一一对应（同签名至多相邻重复一次，表示 at-least-once + 幂等）。

### 新增/增强的自动化测试

中断恢复专项（`recovery_test.go`，均为确定性崩溃注入 + 真实 JSON 落盘/重载，
无网络、无真实凭据）：

| 测试 | 覆盖点 |
| --- | --- |
| `TestRecoveryCrashBeforeApply` | 断在下发**之前**：重启自动补发同一批期望并一路成功；实际下发序列每阶段一次、无重复无跳级 |
| `TestRecoveryCrashDuringApply` | 断在下发**过程中**（副作用可能已落地）：重启补发同期望、目标精确收敛到该批、最终状态不被改乱 |
| `TestRecoveryCrashAfterConfirm` | 断在下发**确认之后**的等待期：重启只等待门控、绝不二次下发；同 cmdID 继续等待、不重复放量 |
| `TestRecoveryTimeoutReentryIdempotent` | 不重启仅超时重入：未确认补发/已确认只等待；cmdID 入账前后语义不同，下一批不被误下发 |
| `TestRecoveryCrashInterleavedPause` | 与暂停/恢复穿插：暂停态重启不自动放量、丢弃未确认在途，Resume 后从同一阶段重新应用并成功 |
| `TestRecoveryCrashDuringRollback` | 与回滚穿插（断在回滚前/后两窗口）：重启幂等重放回滚落定 RolledBack，终态操作返回 ErrPlanFinished、目标释放 |
| `TestRecoveryErrorCausesDistinguishable` | 参数非法 / 非法状态 / 目标占用 / 补发重试耗尽（Failed + ErrRetryExhausted）分别可判定 |

原有用例（异常与边界链路为主）：

| 测试 | 覆盖点 |
| --- | --- |
| `TestBatchHappyPath` / `TestBatchPartialCapacity` | 分批正常链路、累计副本/流量推导、允许小于上限 |
| `TestBatchInvalidConfig` | 空批次、非正副本数、累计超上限、零承载 → `ErrInvalidArgument` 且不落库 |
| `TestBatchPauseResumeRollback` | 分批方式的暂停/继续/回滚 |
| `TestBatchRestartRecovery` | 分批推进中途重启，位点一致、不重复放量、命令幂等跨重启 |
| `TestBatchBoundedGrowth` | 长期高频上报下批次不增多、观测/命令/审计有界 |
| `TestGateDimensionsIndependent` | 版本/流量/承载三维度逐一不匹配均阻塞，全对才通过 |
| `TestGateOneObservationCannotPassTwoSteps` | 旧观测不能跨阶段、一次观测不能连过两阶段 |
| `TestGateJitterDoesNotPass` | 承载抖动时不提前放行 |
| `TestTargetBusyRejectsSecondPlan` / `TestTargetBusyConcurrentCreate` | 同目标互斥、拒绝原因可区分、并发恰好一个成功 |
| `TestSameTargetOpsSerialNoDoubleRollout` | 同目标并发推进串行、不重复放量/不跳级 |
| `TestDifferentTargetsParallel` | 不同目标并行互不影响 |
| `TestTargetBusySurvivesRestart` | 互斥约束进程重启后仍生效 |
| `TestLoadV1RunningMigrated` / `TestLoadV1PausedMigrated` / `TestUnsupportedVersionRejected` | v1 可读迁移、需新观测、其它版本明确拒绝 |

原有用例（非法迁移、重复命令/上报、观测过期、指标缺失、健康门控自动回滚、重试耗尽、
断点恢复、存储版本拒绝、并发竞争、资源上限、蓝绿语义）全部保留并在新门控下持续通过。
所有测试均为本地内存 + 临时文件 + 模拟环境，不依赖外部服务、真实凭据或网络。
