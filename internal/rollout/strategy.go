package rollout

import "fmt"

// canaryPercents 金丝雀默认放量批次（新版本副本占总量百分比）。
var canaryPercents = []int{10, 50, 100}

// BuildSteps 根据策略生成阶段序列。
//
// 三种策略共享同一套状态语义（每阶段：应用期望状态 -> 等待多维门控 -> 进入下一阶段），
// 但批次含义不同：
//   - canary：逐批扩大新版本副本数与流量，Percent 与 Replicas 同步增长；
//   - bluegreen：先以 0% 流量全量部署 green（Replicas=总量, Percent=0），
//     观测通过后再一次性切换 100% 流量；
//   - batch：每批新增固定数量的新版本副本，批次数量=len(batchSizes)，
//     Replicas 为累计值，Percent 按累计副本占总量的比例推导。
//
// batchSizes 仅 StrategyBatch 使用；非法配置一律返回 ErrInvalidArgument，
// 绝不回落到默认批次：
//   - 批次列表为空；
//   - 任一批次副本数 <= 0；
//   - 各批副本数之和超过 totalReplicas（目标承载上限）。
func BuildSteps(kind StrategyKind, totalReplicas int, batchSizes []int) ([]Step, error) {
	if totalReplicas <= 0 {
		return nil, fmt.Errorf("%w: totalReplicas must be positive", ErrInvalidArgument)
	}
	switch kind {
	case StrategyCanary:
		steps := make([]Step, 0, len(canaryPercents))
		for i, pct := range canaryPercents {
			replicas := totalReplicas * pct / 100
			if pct == 100 {
				replicas = totalReplicas
			}
			if replicas == 0 {
				replicas = 1
			}
			steps = append(steps, Step{
				Index:    i,
				Name:     fmt.Sprintf("canary-%d%%", pct),
				Percent:  pct,
				Replicas: replicas,
			})
		}
		return steps, nil
	case StrategyBlueGreen:
		return []Step{
			{Index: 0, Name: "deploy-green", Percent: 0, Replicas: totalReplicas},
			{Index: 1, Name: "switch-traffic", Percent: 100, Replicas: totalReplicas},
		}, nil
	case StrategyBatch:
		return buildBatchSteps(totalReplicas, batchSizes)
	default:
		return nil, fmt.Errorf("%w: unknown strategy %q", ErrInvalidArgument, kind)
	}
}

// buildBatchSteps 构造固定副本分批序列，并对配置做严格校验。
func buildBatchSteps(totalReplicas int, batchSizes []int) ([]Step, error) {
	if len(batchSizes) == 0 {
		return nil, fmt.Errorf("%w: batch strategy requires a non-empty batch list", ErrInvalidArgument)
	}
	steps := make([]Step, 0, len(batchSizes))
	cumulative := 0
	for i, n := range batchSizes {
		if n <= 0 {
			return nil, fmt.Errorf("%w: batch %d size must be positive, got %d", ErrInvalidArgument, i, n)
		}
		cumulative += n
		if cumulative > totalReplicas {
			return nil, fmt.Errorf("%w: cumulative batch replicas %d exceed target capacity %d", ErrInvalidArgument, cumulative, totalReplicas)
		}
		steps = append(steps, Step{
			Index:    i,
			Name:     fmt.Sprintf("batch-%d(+%d)", i+1, n),
			Percent:  cumulative * 100 / totalReplicas,
			Replicas: cumulative,
		})
	}
	return steps, nil
}
