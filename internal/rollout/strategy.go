package rollout

import "fmt"

// canaryPercents 金丝雀默认放量批次（新版本副本占总量百分比）。
var canaryPercents = []int{10, 50, 100}

// BuildSteps 根据策略生成阶段序列。
//
// 两种策略共享同一套状态语义（每阶段：应用期望状态 -> 等待收敛 -> 健康门控 -> 进入下一阶段），
// 但语义不同：
//   - canary：逐批扩大新版本副本数与流量，Percent 与 Replicas 同步增长；
//   - bluegreen：先以 0% 流量全量部署 green（Replicas=总量, Percent=0），
//     观测通过后再一次性切换 100% 流量。
func BuildSteps(kind StrategyKind, totalReplicas int) ([]Step, error) {
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
	default:
		return nil, fmt.Errorf("%w: unknown strategy %q", ErrInvalidArgument, kind)
	}
}
