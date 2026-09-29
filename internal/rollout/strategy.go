package rollout

import "fmt"

// canaryPercents 金丝雀默认放量批次（新版本副本占总量百分比）。
var canaryPercents = []int{10, 50, 100}

// StepSpec 阶段构建输入。
type StepSpec struct {
	Kind          StrategyKind
	ToVersion     string // 每个新版本阶段的期望生效版本
	TotalReplicas int    // 目标承载上限（总副本数）
	// BatchReplicas 仅 batch 策略使用：每个元素是该批“新增”的新版本副本数。
	BatchReplicas []int
}

// BuildSteps 根据策略生成阶段序列。
//
// 三种策略共享同一套状态语义（每阶段：应用期望状态 -> 等待收敛 -> 多维门控 -> 进入下一阶段），
// 但语义不同：
//   - canary：逐批扩大新版本副本数与流量，Percent 与 Replicas 同步增长；
//   - bluegreen：先以 0% 流量全量部署 green（Replicas=总量, Percent=0），
//     观测通过后再一次性切换 100% 流量；
//   - batch：按 BatchReplicas 配置，每批增加固定数量的新版本副本，
//     批次数与每批副本数完全由调用方给出，非法配置直接报错。
func BuildSteps(spec StepSpec) ([]Step, error) {
	if spec.ToVersion == "" {
		return nil, fmt.Errorf("%w: toVersion required", ErrInvalidArgument)
	}
	if spec.TotalReplicas <= 0 {
		return nil, fmt.Errorf("%w: totalReplicas must be positive", ErrInvalidArgument)
	}
	switch spec.Kind {
	case StrategyCanary:
		steps := make([]Step, 0, len(canaryPercents))
		for i, pct := range canaryPercents {
			replicas := spec.TotalReplicas * pct / 100
			if pct == 100 {
				replicas = spec.TotalReplicas
			}
			if replicas == 0 {
				replicas = 1
			}
			steps = append(steps, Step{
				Index:    i,
				Name:     fmt.Sprintf("canary-%d%%", pct),
				Percent:  pct,
				Replicas: replicas,
				Version:  spec.ToVersion,
			})
		}
		return steps, nil
	case StrategyBlueGreen:
		return []Step{
			{Index: 0, Name: "deploy-green", Percent: 0, Replicas: spec.TotalReplicas, Version: spec.ToVersion},
			{Index: 1, Name: "switch-traffic", Percent: 100, Replicas: spec.TotalReplicas, Version: spec.ToVersion},
		}, nil
	case StrategyBatch:
		return buildBatchSteps(spec)
	default:
		return nil, fmt.Errorf("%w: unknown strategy %q", ErrInvalidArgument, spec.Kind)
	}
}

// buildBatchSteps 构建固定副本数分批阶段，配置不合法时一律报错，绝不回退默认值：
//   - 批次列表为空          -> ErrInvalidArgument
//   - 任一批副本数 <= 0     -> ErrInvalidArgument
//   - 各批副本数之和超过承载上限 -> ErrInvalidArgument
func buildBatchSteps(spec StepSpec) ([]Step, error) {
	batches := spec.BatchReplicas
	if len(batches) == 0 {
		return nil, fmt.Errorf("%w: batch strategy requires non-empty batchReplicas", ErrInvalidArgument)
	}
	steps := make([]Step, 0, len(batches))
	cum := 0
	for i, n := range batches {
		if n <= 0 {
			return nil, fmt.Errorf("%w: batch %d replicas must be positive, got %d", ErrInvalidArgument, i, n)
		}
		cum += n
		if cum > spec.TotalReplicas {
			return nil, fmt.Errorf("%w: batch total %d exceeds target capacity %d", ErrInvalidArgument, cum, spec.TotalReplicas)
		}
		pct := (cum*100 + spec.TotalReplicas/2) / spec.TotalReplicas // 四舍五入
		if cum == spec.TotalReplicas {
			pct = 100
		}
		steps = append(steps, Step{
			Index:    i,
			Name:     fmt.Sprintf("batch-%d(+%d)", i+1, n),
			Percent:  pct,
			Replicas: cum,
			Version:  spec.ToVersion,
		})
	}
	return steps, nil
}
