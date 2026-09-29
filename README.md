# rollout-orchestrator-go

本地可运行、可验证的渐进式发布编排器。以本地 JSON 文件持久化 + 模拟目标环境驱动发布计划，
保证创建、推进、暂停、回滚与进程重启后的结论一致且可解释。无外部服务、凭据或网络依赖。

## 发布策略语义

三种策略共享同一套状态机与阶段语义（应用期望状态 → 等待收敛 → 多维门控 → 进入下一阶段），
也共用同一套恢复、暂停/继续/回滚与并发规则：

| 策略 | 阶段序列 | 语义 |
| --- | --- | --- |
| `canary` | 10% → 50% → 100% | 按比例逐批扩大新版本副本数与流量，每批都需收敛且通过多维门控 |
| `bluegreen` | deploy-green(0% 流量, 全量副本) → switch-traffic(100%) | 先全量部署 green 但不接流量，门控通过后一次性切换 |
| `batch` | 由 `-batches n1,n2,...` 显式给出 | 按**固定副本数**分批：每批“新增” ni 个新版本副本，批次数与每批数量完全由配置决定 |

### batch（固定副本数分批）

- 配置形如 `2,3,5`（10 副本上限下即 2 → 5 → 10 三个批次），每个数字是该批**新增**副本数，
  阶段期望为累计副本数与对应流量占比（`累计/总量`，四舍五入，最后一批必为 100%）。
- 允许批次总量**小于**承载上限（只放一部分），但任何非法配置都直接返回
  `ErrInvalidArgument`，绝不静默回退默认值：
  - 批次列表为空；
  - 任一批副本数 ≤ 0；
  - 各批副本数之和超过 `TotalReplicas`（目标承载上限）。
- batch 与 canary/bluegreen 一样支持查询、暂停、继续、回滚；进程重启后从“最后一个
  已通过门控的阶段”继续（见下文），不会重放已确认批次，也不会多出批次。

## 状态模型

```
Pending(隐含) → Running ⇄ Paused
Running → RollingBack → RolledBack   (手动或健康门控自动触发)
Running → Succeeded                  (最后一个阶段收敛且健康)
Running/RollingBack → Failed         (副作用重试耗尽)
```

- 终态：`Succeeded` / `RolledBack` / `Failed`。对终态计划的任何操作返回 `ErrPlanFinished`。
- 非法迁移（如 Paused 时 Advance、重复 Pause）返回 `ErrInvalidTransition`。
- 每个变更命令携带 `cmdID`，重复命令返回 `ErrDuplicateCommand`，不会产生重复副作用。

## 收敛与上报幂等

- `Advance` 应用期望副本数与期望流量后，等待门控通过才确认阶段完成。
- 实际状态由 `SimApplier` 模拟：副本与流量逐步收敛（延迟）、可注入抖动、指标缺失与短暂目标缺失。
- 观测按 `ReportID` 幂等去重：重复上报返回 `ErrDuplicateReport`，观测计数不变，
  不会造成重复放量、跳过阶段或结论漂移。

## 多维阶段门控（版本 / 流量 / 承载）

一个阶段算真正完成，必须在**同一次、且属于当前阶段**的观测上同时确认三个维度，
外加健康维度全部满足；任一不满足都只能继续等待或按失败预算处理，绝不提前放行：

1. **生效版本**：当前实际生效版本（`Observation.ActiveVersion`）等于该阶段期望版本
   （`Step.Version`）——版本对不上 → `version-mismatch`；
2. **流量占比**：新版本实际流量（`Observation.TrafficPercent`）等于该阶段期望
   （`Step.Percent`）——对不上 → `traffic-not-reached`；
3. **实际承载**：新版本实际副本数（`Observation.Replicas`）等于该阶段期望
   （`Step.Replicas`）——未收敛 → `capacity-not-converged`；
4. **健康**：观测时效、关键指标、样本数、错误率（原有四规则）。

隔离与不可跨阶段规则：

- 每次进入（或崩溃/超时后重新进入）一个阶段都会分配单调递增的判定代次 `GateSeq`，
  上报只有在“该阶段进行中”被接收时才盖上该代次戳；
- 门控只采纳 `GateSeq` 与当前阶段相同的最新观测：前面阶段的观测不能拿来通过后面阶段，
  一次观测也不可能同时让两个阶段通过；
- 版本/流量/承载维度暂未对上或尚无合格观测（延迟、抖动、短暂未上报）一律“继续等”，
  不消耗失败预算、不写错误结论；只有观测本身不健康（过期/缺指标/样本不足/错误率超标）
  才计入失败预算，结论只追加不改写。
- 崩溃若发生在“已应用、未确认”窗口，重启后该阶段会以**新代次重新等待确认**，
  门控不可能因重启被跳过。

## 健康门控与自动回滚

判定规则（任一不满足即不健康，不得推进，全部记录到 `Plan.Verdicts` 供审计）：

1. 无观测或观测超过 `ObservationMaxAge` → `stale-observation`
2. 关键指标缺失（`HasMetrics=false`）→ `missing-metric`
3. 样本数低于 `MinSamples` → `insufficient-samples`
4. 错误率超过 `MaxErrorRate` → `error-rate-exceeded`

连续不健康次数达到 `FailureBudget` 后自动回滚到上一个稳定版本
（`FromVersion`，新版本副本归零），计划进入 `RolledBack`，判定依据保留在 `Verdicts` 中。

## 持久化与断点恢复

- 单文件 JSON 存储，写盘采用临时文件 + rename 原子替换。
- 文件含 `version` 模式版本，当前为 **v2**：
  - v2 在 v1 基础上增加 `Step.Version`、观测的版本/流量维度与 `GateSeq`、
    `Plan.Gate`（门控进度）、`Plan.BatchReplicas`（分批配置）；v2 数据还会做
    内部一致性校验，结构非法返回 `ErrStoreCorrupt`。
  - **v1 兼容范围**：结构兼容的 v1 计划（id、步骤、下标合法）在加载时被**显式迁移**
    后继续使用——补全阶段期望版本；进行中的计划从“当前阶段”以新代次重新完成多维
    门控（旧观测缺版本/流量维度，会被丢弃并要求重新上报，绝不按零值/缺省值放行）；
    命令幂等表保留。首次成功落盘后文件自动升级为 v2。
  - 结构不兼容的 v1（空 id、空步骤、下标越界等）返回 `ErrStoreVersion` 明确拒绝；
    其它未知版本同样返回 `ErrStoreVersion`；JSON 损坏返回 `ErrStoreCorrupt`。
    即旧数据“要么能正常读出来继续用，要么给出可区分的拒绝原因”，绝不静默按缺省值解释。
- 副作用（`Apply`）幂等：崩溃发生在“已应用未落盘”窗口时，重启后重放同一期望状态是安全的。
- `RollingBack` 是崩溃可能留下的中间态，重启时幂等重放回滚并落定 `RolledBack`/`Failed`。
- 命令幂等表（`AppliedCmds`）随计划持久化，重启后重复命令仍被拒绝。

## 并发与资源上限

- **同一目标互斥**：同一目标（`Target`）同时只允许有一个进行中的发布。
  在它结束（`Succeeded`/`RolledBack`/`Failed`）之前，对同一目标的再次 `Create`
  一律返回 `ErrTargetBusy`（CLI 退出码 7），与参数错误（`ErrInvalidArgument`）、
  非法状态（`ErrInvalidTransition`）等明确区分；计划进入 `Paused` 期间仍占用目标。
- 同一目标上的并发创建、推进、暂停、恢复、回滚由该目标的互斥锁串行处理，
  不会出现重复放量、半更新或状态跳级；**不同目标之间完全并行、互不影响**。
- 互斥状态由持久化的计划状态决定（非终态即占用），**进程重启后约束照样生效**。
- 每个计划另有一把锁兼容按 planID 的旧入口；上报只走引擎锁，不被推进流程阻塞。
- 无后台常驻协程：`Advance` 通过通知通道等待上报，`Close` 仅落盘，不泄漏协程/句柄。
- `Limits`：`MaxPlans`（计划数上限）、`MaxRetries`（副作用重试上限，超限进入 `Failed`）、
  `MaxObservations`（观测/上报 ID 环形上限，防内存膨胀）；判定审计封顶 1000 条。
  batch 的批次数在创建时固定，长时间运行不会批次越跑越多。

## 错误分类（CLI 退出码）

| 退出码 | 含义 |
| --- | --- |
| 2 | 参数错误 / 计划不存在 |
| 3 | 非法迁移 / 终态操作 / 重复命令 / 重复上报 |
| 4 | 健康门控拒绝 / 观测过期 / 指标缺失 / 未收敛 |
| 5 | 重试耗尽 / 资源超限 |
| 6 | 存储版本不兼容 / 数据损坏 |
| 7 | 目标互斥：同一目标已有进行中的发布（`ErrTargetBusy`） |

## 本地验证

```bash
# 运行全部测试（异常与边界链路为主，含 -race）
go test -race ./...
go test -race -v ./...   # -v 可看到每个用例打印的输入、状态变化与判定依据

# CLI 全链路（canary）
go build -o /tmp/rolloutctl ./cmd/rolloutctl
cd /tmp && rm -f st.json
./rolloutctl -store st.json create  -id p1 -target svc-a -strategy canary -cmd c1
./rolloutctl -store st.json advance -id p1 -cmd a1 -simulate   # 模拟环境驱动收敛
./rolloutctl -store st.json pause   -id p1 -cmd p1
./rolloutctl -store st.json advance -id p1 -cmd a2             # 退出码 3：Paused 不可推进
./rolloutctl -store st.json resume  -id p1 -cmd r1
./rolloutctl -store st.json advance -id p1 -cmd a2 -simulate
./rolloutctl -store st.json advance -id p1 -cmd a3 -simulate   # -> Succeeded
./rolloutctl -store st.json get     -id p1                     # 查看 gate 与 verdicts 判定依据
./rolloutctl -store st.json demo                               # 一键演示完整金丝雀流程

# batch 固定副本数分批
rm -f bt.json
./rolloutctl -store bt.json create -id b1 -target svc-b -strategy batch -replicas 10 -batches 2,3,5 -cmd bc1
./rolloutctl -store bt.json advance -id b1 -cmd ba1 -simulate  # 每批三维门控确认
./rolloutctl -store bt.json advance -id b1 -cmd ba2 -simulate
./rolloutctl -store bt.json advance -id b1 -cmd ba3 -simulate  # -> Succeeded
./rolloutctl -store bt.json demo-batch                         # 一键演示 batch 流程
./rolloutctl -store bt.json create -id bad -target svc-x -strategy batch -replicas 10 -batches 4,4,4 -cmd x
#                                                                              ↑ 退出码 2：批次总量超过承载

# 同一目标互斥（第二个 create 退出码 7）
rm -f m.json
./rolloutctl -store m.json create -id m1 -target svc-m -strategy batch -replicas 10 -batches 5,5 -cmd mc1
./rolloutctl -store m.json create -id m2 -target svc-m -strategy canary -cmd mc2   # 退出码 7
```

新增自动化测试覆盖（`internal/rollout/feature_test.go`，全部本地运行、无外部服务/凭据/网络）：

- batch 正常链路（每批三维度确认）、参数边界（空批次/非正副本数/超上限）、
  部分承载、暂停继续回滚、断点重启续推；
- 多维门控：单次观测不能跨阶段通过、七个维度缺失/错误子用例均不放行；
- 同目标并发创建只有一个成功（其余 `ErrTargetBusy`）、同目标跨计划串行、
  不同目标并行、重启后互斥仍生效；
- 长时间运行观测/判定/批次有界；
- v1 计划迁移续推与不兼容 v1 的明确拒绝。

原有异常与边界用例（非法迁移、重复命令/上报、观测过期、指标缺失、自动回滚、
重试耗尽、断点恢复、存储版本/损坏、并发竞争、资源上限、蓝绿语义）全部保留且不回归。
单测日志（`go test -v`）打印输入、状态迁移与判定依据。
