# rollout-orchestrator-go

本地可运行、可验证的渐进式发布编排器。以本地 JSON 文件持久化 + 模拟目标环境驱动发布计划，
保证创建、推进、暂停、回滚与进程重启后的结论一致且可解释。无外部服务、凭据或网络依赖。

## 发布策略语义

两种策略共享同一套状态机与阶段语义（应用期望状态 → 等待收敛 → 健康门控 → 进入下一阶段）：

| 策略 | 阶段序列 | 语义 |
| --- | --- | --- |
| `canary` | 10% → 50% → 100% | 逐批扩大新版本副本数与流量，每批都需收敛且通过健康门控 |
| `bluegreen` | deploy-green(0% 流量, 全量副本) → switch-traffic(100%) | 先全量部署 green 但不接流量，观测通过后一次性切换 |

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

- `Advance` 应用期望副本数后，等待最新观测显示实际副本数等于期望值且健康判定通过。
- 实际状态由 `SimApplier` 模拟：逐步收敛（延迟）、可注入抖动、指标缺失与短暂目标缺失。
- 观测按 `ReportID` 幂等去重：重复上报返回 `ErrDuplicateReport`，观测计数不变，
  不会造成重复放量、跳过阶段或结论漂移。

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
- 文件含 `version` 模式版本：版本不匹配返回 `ErrStoreVersion`，JSON 损坏返回
  `ErrStoreCorrupt`，绝不按默认值静默解释旧数据。
- 副作用（`Apply`）幂等：崩溃发生在“已应用未落盘”窗口时，重启后重放同一期望状态是安全的。
- `RollingBack` 是崩溃可能留下的中间态，重启时幂等重放回滚并落定 `RolledBack`/`Failed`。
- 命令幂等表（`AppliedCmds`）随计划持久化，重启后重复命令仍被拒绝。

## 并发与资源上限

- 每个计划一把互斥锁串行化变更操作；上报只走引擎锁，不被推进流程阻塞。
- 无后台常驻协程：`Advance` 通过通知通道等待上报，`Close` 仅落盘，不泄漏协程/句柄。
- `Limits`：`MaxPlans`（计划数上限）、`MaxRetries`（副作用重试上限，超限进入 `Failed`）、
  `MaxObservations`（观测/上报 ID 环形上限，防内存膨胀）；判定审计封顶 1000 条。

## 错误分类（CLI 退出码）

| 退出码 | 含义 |
| --- | --- |
| 2 | 参数错误 / 计划不存在 |
| 3 | 非法迁移 / 终态操作 / 重复命令 / 重复上报 |
| 4 | 健康门控拒绝 / 观测过期 / 指标缺失 / 未收敛 |
| 5 | 重试耗尽 / 资源超限 |
| 6 | 存储版本不兼容 / 数据损坏 |

## 本地验证

```bash
# 运行全部测试（异常与边界链路为主，含 -race）
go test -race ./...

# CLI 全链路
go build -o /tmp/rolloutctl ./cmd/rolloutctl
cd /tmp && rm -f st.json
./rolloutctl -store st.json create  -id p1 -target svc-a -strategy canary -cmd c1
./rolloutctl -store st.json advance -id p1 -cmd a1 -simulate   # 模拟环境驱动收敛
./rolloutctl -store st.json pause   -id p1 -cmd p1
./rolloutctl -store st.json advance -id p1 -cmd a2             # 退出码 3：Paused 不可推进
./rolloutctl -store st.json resume  -id p1 -cmd r1
./rolloutctl -store st.json advance -id p1 -cmd a2 -simulate
./rolloutctl -store st.json advance -id p1 -cmd a3 -simulate   # -> Succeeded
./rolloutctl -store st.json get     -id p1                     # 查看判定依据 verdicts
./rolloutctl -store st.json demo                               # 一键演示完整金丝雀流程
```

测试覆盖：非法状态迁移、重复命令/上报、观测过期、指标缺失、健康门控自动回滚、
重试耗尽、断点重启恢复、存储版本拒绝、并发竞争（`-race`）、资源上限。
单测日志（`go test -v`）打印输入、状态迁移与判定依据。
