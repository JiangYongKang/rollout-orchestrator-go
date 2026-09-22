package strategy

import (
	"fmt"
	"time"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

// CanaryStrategy 按"新版本流量百分比"放量：5% -> 25% -> 50% -> 100%。
// 语义：实际百分比落在 [desired-tol, desired+tol] 内即视为收敛，
// 因此容忍观测抖动；版本不对则绝不收敛。
type CanaryStrategy struct{}

func (s *CanaryStrategy) Kind() domain.StrategyKind { return domain.StrategyCanary }

func (s *CanaryStrategy) BuildSteps(desiredVer string, totalReplicas int) ([]domain.Step, error) {
	if desiredVer == "" {
		return nil, fmt.Errorf("%w: desired version required", domain.ErrInvalidArgument)
	}
	if totalReplicas <= 0 {
		return nil, fmt.Errorf("%w: total_replicas must be > 0", domain.ErrInvalidArgument)
	}
	pcts := []int{5, 25, 50, 100}
	steps := make([]domain.Step, 0, len(pcts))
	for i, p := range pcts {
		steps = append(steps, domain.Step{Index: i, Desired: p})
	}
	return steps, nil
}

func (s *CanaryStrategy) IsConverged(plan *domain.Plan, obs *domain.Observation, now time.Time) (bool, string) {
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
	low := cur.Desired - plan.Config.TolerancePct
	high := cur.Desired + plan.Config.TolerancePct
	if obs.Actual.Percent < low || obs.Actual.Percent > high {
		return false, fmt.Sprintf("actual percent=%d outside [%d,%d] desired=%d",
			obs.Actual.Percent, low, high, cur.Desired)
	}
	return true, fmt.Sprintf("actual percent=%d within tolerance of desired=%d", obs.Actual.Percent, cur.Desired)
}
