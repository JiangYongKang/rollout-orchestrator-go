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
- `GateSeq`：在途阶段认可的最早观测序号（新观测水位线）。

只有没有在途阶段时才会真正应用下一批期望，所以重复调用、超时重试、进程重启都不会
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
- 副作用（`Apply`）幂等：崩溃发生在“已应用未落盘”窗口时，重启后重放同一期望状态安全；
  进行中阶段重启后重新等待门控、不重复放量。
- `RollingBack` 是崩溃可能留下的中间态，重启时幂等重放回滚并落定 `RolledBack`/`Failed`。
- 命令幂等表（`AppliedCmds`）随计划持久化，重启后重复命令仍被拒绝。

### 推进中断后的重启恢复（补发语义）

`Advance` 先把在途位点（`PendingStep`/`GateSeq`）同步落盘，再下发该批期望状态。
进程可能死在位点落盘之后的任意时点，重启后无法区分下发是否完成：

| 中断时点 | 目标环境状态 | 重启后继续推进的行为 |
| --- | --- | --- |
| 断在下发之前 | 从未收到这批期望 | 自动补发同一批期望，再等门控 |
| 断在下发过程中 | 已收到期望，但引擎不知道结果 | 幂等补发同一批期望（同参数），再等门控 |
| 断在下发之后 | 期望已生效，门控未通过 | 幂等补发同一批期望（同参数），再等门控 |

恢复规则与保证：

- **自动补齐**：重启后首次继续推进在途阶段时，引擎按持久化的位点重放该阶段期望
  （`appliedStep` 是纯内存标记，重启即空，因此重启后必然补发一次）；随后正常等待
  多维门控，一路推进到成功，表现与没中断过一致。
- **幂等不重复放量**：补发携带与首次完全相同的期望参数（版本/副本/流量），`Apply`
  幂等，因此目标实际状态与记录不被改乱；位点（`CurrentStep`/`PendingStep`/`GateSeq`）
  不由补发改动，**不会因此多走或跳过一个阶段**。同一进程内已下发成功的
  （ctx 超时重入）不会重复 `Apply`。
- **与暂停/恢复、回滚穿插**：暂停中的在途阶段重启后仍是 `Paused`，`Resume` 后推进
  时才补发；重启前后都可对同一在途计划回滚，回滚落定 `RolledBack` 后目标释放。
- **串行与互斥不变**：同一目标的推进/暂停/恢复/回滚仍按目标锁串行，同一目标始终
  只允许一个进行中的发布（`ErrTargetBusy`），重启前后一致。
- **结果可区分**：恢复推进的成功结果与参数不合法（`ErrInvalidArgument`）、非法状态
  （`ErrInvalidTransition`）、目标被占用（`ErrTargetBusy`）、计划不存在
  （`ErrNotFound`）等拒绝原因各自独立，可用 `errors.Is` 判定。
- 补发本身重试耗尽（目标环境持续不可用）按副作用失败处理：落 `Failed` 终态，
  与新阶段下发失败的语义一致。

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

# 推进中断后重启恢复（每次 CLI 调用都是一个独立进程，天然模拟崩溃重启）：
# 第一次推进超时退出 = 在途位点已落盘、进程中断；新进程用同一 cmdID 继续推进，
# 自动幂等补发该批期望并通过门控，随后一路到 Succeeded。
/tmp/rolloutctl -store /tmp/r.json create -id c1 -target svcC -strategy canary -to v2 -replicas 10 -cmd c1
/tmp/rolloutctl -store /tmp/r.json advance -id c1 -cmd a0 -timeout 300ms   # exit=4 超时中断，pendingStep=0 已落盘
/tmp/rolloutctl -store /tmp/r.json advance -id c1 -cmd a0 -simulate        # 重启续推：自动补发，阶段 0 通过
/tmp/rolloutctl -store /tmp/r.json advance -id c1 -cmd a1 -simulate
/tmp/rolloutctl -store /tmp/r.json advance -id c1 -cmd a2 -simulate        # Succeeded

# 中断恢复专项单测（日志含输入、状态迁移、逐维度判定依据与每次期望下发参数流水）
go test -v -run 'TestCrash|TestTimeout' ./internal/rollout/
```

### 新增/增强的自动化测试

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
| `TestCrashBeforeApplyRecovery` | 断在下发之前：重启后自动补发该批期望并一路成功，补发参数与首次完全一致 |
| `TestCrashDuringApplyRecovery` | 断在下发过程中（环境已收到期望、引擎不知结果）：幂等补发、实际承载不被改乱 |
| `TestCrashAfterApplyBeforeGate` | 断在下发之后（门控通过前）：重启重入门控等待，补发幂等、不跳阶段 |
| `TestTimeoutReentryNoReapply` | 同进程 ctx 超时重入在途阶段：不重复 Apply、不重复放量 |
| `TestCrashRecoveryWithPauseResume` | 在途阶段暂停后崩溃：重启仍 Paused、推进被拒（ErrInvalidTransition），Resume 后补发推进到成功 |
| `TestCrashRecoveryWithRollback` | 在途阶段崩溃后回滚：落定 RolledBack、目标释放、终态操作可区分 |
| `TestCrashRecoveryErrorDistinction` | 恢复推进结果与参数非法/状态非法/目标占用等原因可区分 |

上述中断恢复用例通过 `crashApplier` 在指定期望下发调用点模拟进程被杀
（可选择该次是否已把期望落到模拟环境），随后用同一存储文件 + 同一模拟环境
重开引擎验证恢复语义；全部本地运行，不依赖外部服务、真实凭据或网络。
运行 `go test -v -run 'TestCrash|TestTimeout' ./internal/rollout/` 可在日志中看到
每条用例的输入、状态迁移、逐维度判定依据与每次期望下发的参数流水。

原有用例（非法迁移、重复命令/上报、观测过期、指标缺失、健康门控自动回滚、重试耗尽、
断点恢复、存储版本拒绝、并发竞争、资源上限、蓝绿语义）全部保留并在新门控下持续通过。
所有测试均为本地内存 + 临时文件 + 模拟环境，不依赖外部服务、真实凭据或网络。
