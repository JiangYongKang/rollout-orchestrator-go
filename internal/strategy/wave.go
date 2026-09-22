package strategy

import (
	"fmt"
	"math"
	"time"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

// WaveStrategy 按"新版本副本批次"放量：10% -> 25% -> 50% -> 100%
// （副本数向上取整、去重，保证严格递增且最终为 TotalReplicas）。
// 语义：副本数必须与期望批次精确相等，不存在百分比抖动容差；
// 多一个、少一个副本都不算收敛，因此与 CanaryStrategy 语义不同。
type WaveStrategy struct{}

func (s *WaveStrategy) Kind() domain.StrategyKind { return domain.StrategyWave }

func (s *WaveStrategy) BuildSteps(desiredVer string, totalReplicas int) ([]domain.Step, error) {
	if desiredVer == "" {
		return nil, fmt.Errorf("%w: desired version required", domain.ErrInvalidArgument)
	}
	if totalReplicas <= 0 {
		return nil, fmt.Errorf("%w: total_replicas must be > 0", domain.ErrInvalidArgument)
	}
	fracs := []float64{0.1, 0.25, 0.5, 1.0}
	steps := make([]domain.Step, 0, len(fracs))
	last := 0
	idx := 0
	for _, f := range fracs {
		n := int(math.Ceil(float64(totalReplicas) * f))
		if n <= last {
			continue // 小集群下去重，保证严格递增
		}
		steps = append(steps, domain.Step{Index: idx, Desired: n})
		last = n
		idx++
	}
	return steps, nil
}

func (s *WaveStrategy) IsConverged(plan *domain.Plan, obs *domain.Observation, now time.Time) (bool, string) {
	cur := plan.CurrentStep()
	if cur == nil {
		return false, "no current step"
	}
	if obs == nil {
		return false, "observation missing"
	}
	if age := now.Sub(obs.ObservedAt); age > plan.Config.StaleThreshold {
		return false, fmt.Sprintf("observation stale: age=%s threshold=%s", age, plan.Config.StaleThreshold)
	}
	if obs.Actual.Version != plan.DesiredVer {
		return false, fmt.Sprintf("actual version=%q want=%q (not promoted yet)", obs.Actual.Version, plan.DesiredVer)
	}
	if obs.Actual.Replicas != cur.Desired {
		return false, fmt.Sprintf("actual replicas=%d, wave requires exactly %d",
			obs.Actual.Replicas, cur.Desired)
	}
	return true, fmt.Sprintf("actual replicas=%d matches wave batch", cur.Desired)
}
