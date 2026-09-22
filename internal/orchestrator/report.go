package orchestrator

import (
	"context"
	"fmt"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/strategy"
)

// Report 接收外部对目标的一次观测上报。
//
// 同一 BatchID 重复上报：以 ErrDuplicateCommand 拒绝，绝不造成重复放量、
// 跳过阶段、重复扣减预算或结论漂移。
// 观测过期（早于 StaleThreshold）或关键指标缺失时，不允许继续推进。
// 收敛 + 健康通过后自动进入下一阶段（Gated/Promoting/终态），
// 不健康则按失败预算自动回滚。
func (s *Service) Report(ctx context.Context, id string, obs domain.Observation) (*domain.Plan, error) {
	return s.runForTarget(ctx, id, func(p *domain.Plan) error {
		if p.State.Terminal() {
			return fmt.Errorf("%w: state=%s", domain.ErrPlanFinished, p.State)
		}
		if p.State == domain.StatePaused {
			return fmt.Errorf("%w: plan paused; resume or rollback", domain.ErrPlanPaused)
		}
		if p.State == domain.StatePending {
			return fmt.Errorf("%w: no step promoted yet; advance first", domain.ErrInvalidTransition)
		}
		if p.State == domain.StateRollingBack {
			return fmt.Errorf("%w: rolling back; observation rejected", domain.ErrInvalidTransition)
		}
		if obs.BatchID == "" {
			return fmt.Errorf("%w: observation batch_id required for dedup", domain.ErrInvalidArgument)
		}
		if obs.BatchID == p.LastBatchID {
			return fmt.Errorf("%w: batch=%s already processed in state=%s",
				domain.ErrDuplicateCommand, obs.BatchID, p.State)
		}

		now := s.now()
		s.logf("[REPORT] plan=%s state=%s batch=%s observed_at=%s actual=%+v metrics=%d",
			id, p.State, obs.BatchID, obs.ObservedAt.Format("15:04:05.000"),
			obs.Actual, len(obs.Metrics))

		if age := now.Sub(obs.ObservedAt); age > p.Config.StaleThreshold {
			return fmt.Errorf("%w: age=%s threshold=%s",
				domain.ErrObservationStale, age, p.Config.StaleThreshold)
		}

		cur := p.CurrentStep()
		strat, err := strategy.New(p.Strategy)
		if err != nil {
			return err
		}
		converged, reason := strat.IsConverged(p, &obs, now)
		// Gated 已有门控依据时，新的"不收敛/版本回退"观测不得污染门控依据：
		// 旧的新鲜收敛观测仍然有效（操作员可凭它 Advance；或用 rollback 处理回退）。
		// Promoting 阶段则始终记录最新观测用于对账。
		if p.State != domain.StateGated || converged {
			p.LastObs = &obs
			p.LastBatchID = obs.BatchID
		}
		p.AddEvent(domain.EventObserved, "observation accepted", map[string]interface{}{
			"batch":                obs.BatchID,
			"actual":               fmt.Sprintf("%+v", obs.Actual),
			"converged":            converged,
			"reason":               reason,
			"gate_basis_preserved": p.State == domain.StateGated && !converged,
		}, now)

		if !converged {
			s.logf("[REPORT] plan=%s not converged: %s", id, reason)
			return nil // 保持 Promoting：延迟/抖动由后续观测确认，不跳步
		}
		cur.Converged = true
		p.AddEvent(domain.EventConverged, reason, map[string]interface{}{"step": cur.Index}, now)
		s.logf("[REPORT] plan=%s converged step=%d: %s", id, cur.Index, reason)

		healthy, reasons := checkHealth(p, &obs)
		cur.Healthy = healthy
		p.AddEvent(domain.EventHealthChecked, healthVerdict(healthy, reasons),
			map[string]interface{}{"step": cur.Index, "batch": obs.BatchID}, now)

		if !healthy {
			// 新批次的不健康观测才消耗预算；收敛判定已保证不是重复批次。
			p.BudgetBatchID = obs.BatchID
			p.BudgetLeft--
			p.AddEvent(domain.EventBudgetConsumed, fmt.Sprintf("unhealthy: %v", reasons),
				map[string]interface{}{"budget_left": p.BudgetLeft, "batch": obs.BatchID}, now)
			s.logf("[REPORT] plan=%s unhealthy=%v budget_left=%d", id, reasons, p.BudgetLeft)
			if p.BudgetLeft < 0 {
				return s.executeRollback(ctx, p,
					fmt.Sprintf("failure budget exhausted by batch=%s reasons=%v", obs.BatchID, reasons))
			}
			return fmt.Errorf("%w: %v", domain.ErrUnhealthy, reasons)
		}

		// 收敛且健康：自动推进，不依赖操作员再次命令。
		if cur.Index == len(p.Steps)-1 {
			if err := s.transition(p, domain.StateSucceeded, "final step converged & healthy", nil); err != nil {
				return err
			}
			p.AddEvent(domain.EventSucceeded, "rollout succeeded at desired version",
				map[string]interface{}{"version": p.DesiredVer}, s.now())
			s.logf("[REPORT] plan=%s -> Succeeded", id)
			return nil
		}

		if p.State == domain.StatePromoting {
			if err := s.transition(p, domain.StateGated, "converged & healthy; gate reached", nil); err != nil {
				return err
			}
			s.logf("[REPORT] plan=%s -> Gated step=%d", id, cur.Index)
			return nil
		}

		// Gated 状态下收到"再次确认收敛且健康"的新批次：先放量下一步（生效才移指针）。
		moved, _, applyReason, aerr := s.advanceToNext(ctx, p)
		if aerr != nil {
			return s.failTerminal(ctx, p, aerr)
		}
		if !moved {
			// 副作用延迟：保持 Gated，由下一次 Advance 用幂等 token 对账。
			s.logf("[REPORT] plan=%s next step delayed, stay Gated: %s", id, applyReason)
			return nil
		}
		if err := s.transition(p, domain.StatePromoting, "auto promoted; "+applyReason, nil); err != nil {
			return err
		}
		s.logf("[REPORT] plan=%s Gated -> Promoting step=%d", id, p.StepIdx)
		return nil
	})
}

func healthVerdict(healthy bool, reasons []string) string {
	if healthy {
		return "health gate passed"
	}
	return fmt.Sprintf("health gate rejected: %v", reasons)
}
