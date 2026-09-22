package strategy

import (
	"fmt"
	"time"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

// Strategy 抽象两种语义不同的放量策略，共享同一套 Plan 状态语义。
type Strategy interface {
	Kind() domain.StrategyKind
	BuildSteps(desiredVer string, totalReplicas int) ([]domain.Step, error)
	// IsConverged 判断实际状态是否已收敛到当前步骤的期望状态。
	// 延迟/抖动由实现按各自语义吸收；ok=false 时 reason 必须可解释。
	IsConverged(plan *domain.Plan, obs *domain.Observation, now time.Time) (ok bool, reason string)
}

// New 按类型返回策略实现。
func New(kind domain.StrategyKind) (Strategy, error) {
	switch kind {
	case domain.StrategyCanary:
		return &CanaryStrategy{}, nil
	case domain.StrategyWave:
		return &WaveStrategy{}, nil
	default:
		return nil, fmt.Errorf("%w: unknown strategy %q", domain.ErrInvalidArgument, kind)
	}
}
