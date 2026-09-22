package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

// Advance 是操作员驱动的"推进/重试/对账"命令，语义随当前状态而定：
//   - Pending   : 对第 0 步发起副作用，进入 Promoting；
//   - Promoting : 重放或重试当前步副作用（幂等，受 MaxAttempts 上限约束）；
//   - Gated     : 依据最近一次观测做健康门控，通过则进入下一步（或成功），
//     不健康则消耗失败预算，预算耗尽自动回滚；
//   - Paused    : 被拒绝（ErrPlanPaused）；
//   - 终态      : 被拒绝（ErrPlanFinished）。
func (s *Service) Advance(ctx context.Context, id string) (*domain.Plan, error) {
	return s.runForTarget(ctx, id, func(p *domain.Plan) error {
		s.logf("[ADVANCE] plan=%s input-state=%s step=%d", id, p.State, p.StepIdx)
		switch p.State {
		case domain.StatePending:
			cur := p.CurrentStep()
			applied, reason, err := s.ensureStepApplied(ctx, p, cur)
			if err != nil {
				return s.failTerminal(ctx, p, err)
			}
			if !applied {
				s.logf("[ADVANCE] plan=%s pending side-effect not applied: %s", id, reason)
				return nil // 仍为 Pending，等待再次 Advance 重试
			}
			if err := s.transition(p, domain.StatePromoting, reason, nil); err != nil {
				return err
			}
			s.logf("[ADVANCE] plan=%s -> Promoting (%s)", id, reason)
			return nil

		case domain.StatePromoting:
			cur := p.CurrentStep()
			applied, reason, err := s.ensureStepApplied(ctx, p, cur)
			if err != nil {
				return s.failTerminal(ctx, p, err)
			}
			if !applied {
				s.logf("[ADVANCE] plan=%s promoting side-effect not applied: %s", id, reason)
				return nil
			}
			s.logf("[ADVANCE] plan=%s side effect in place, awaiting observation: %s", id, reason)
			return nil // 保持 Promoting：必须由观测确认收敛，不能凭副作用成功越级

		case domain.StateGated:
			cur := p.CurrentStep()
			if cur.AppliedToken == "" {
				// 上一步已过门控，但下一步副作用仍延迟/失败：先幂等重试（有界）。
				applied, reason, aerr := s.ensureStepApplied(ctx, p, cur)
				if aerr != nil {
					return s.failTerminal(ctx, p, aerr)
				}
				if !applied {
					s.logf("[ADVANCE] plan=%s gated pending next step delayed: %s", id, reason)
					return nil
				}
				if err := s.transition(p, domain.StatePromoting, "pending next step now applied; "+reason, nil); err != nil {
					return err
				}
				s.logf("[ADVANCE] plan=%s gated -> Promoting (pending step applied)", id)
				return nil
			}
			if p.LastObs == nil {
				return fmt.Errorf("%w: gated step requires an observation", domain.ErrObservationMissing)
			}
			obs := p.LastObs
			if age := s.now().Sub(obs.ObservedAt); age > p.Config.StaleThreshold {
				return fmt.Errorf("%w: age=%s threshold=%s", domain.ErrObservationStale, age, p.Config.StaleThreshold)
			}
			healthy, reasons := checkHealth(p, obs)
			cur.Healthy = healthy
			detail := map[string]interface{}{"step": cur.Index, "batch": obs.BatchID, "reasons": reasons}
			if healthy {
				p.AddEvent(domain.EventHealthChecked, "health gate passed", detail, s.now())
				if cur.Index == len(p.Steps)-1 {
					if err := s.transition(p, domain.StateSucceeded, "final step healthy", nil); err != nil {
						return err
					}
					p.AddEvent(domain.EventSucceeded, "rollout succeeded at desired version",
						map[string]interface{}{"version": p.DesiredVer}, s.now())
					s.logf("[ADVANCE] plan=%s -> Succeeded", id)
					return nil
				}
				moved, _, reason, aerr := s.advanceToNext(ctx, p)
				if aerr != nil {
					return s.failTerminal(ctx, p, aerr)
				}
				if !moved {
					// 下一步副作用延迟：保持 Gated，等待再次 Advance（不越级到 Promoting）。
					s.logf("[ADVANCE] plan=%s next step delayed, stay Gated: %s", id, reason)
					return nil
				}
				if err := s.transition(p, domain.StatePromoting, "gate passed; "+reason, nil); err != nil {
					return err
				}
				s.logf("[ADVANCE] plan=%s gate passed -> step %d Promoting", id, p.StepIdx)
				return nil
			}

			// 不健康：同一观测批次只消耗一次预算（重复上报不得造成结论漂移）。
			if obs.BatchID != "" && obs.BatchID == p.BudgetBatchID {
				p.AddEvent(domain.EventHealthChecked,
					"unhealthy observation already consumed budget; no drift", detail, s.now())
				return fmt.Errorf("%w: %v", domain.ErrUnhealthy, reasons)
			}
			p.BudgetBatchID = obs.BatchID
			p.BudgetLeft--
			p.AddEvent(domain.EventBudgetConsumed, fmt.Sprintf("unhealthy: %v", reasons),
				map[string]interface{}{"budget_left": p.BudgetLeft, "batch": obs.BatchID}, s.now())
			s.logf("[ADVANCE] plan=%s unhealthy reasons=%v budget_left=%d", id, reasons, p.BudgetLeft)
			if p.BudgetLeft < 0 {
				p.AddEvent(domain.EventRollingBack, "failure budget exhausted -> auto rollback",
					map[string]interface{}{"reasons": reasons}, s.now())
				return s.executeRollback(ctx, p, "failure budget exhausted")
			}
			return fmt.Errorf("%w: %v", domain.ErrUnhealthy, reasons)

		case domain.StatePaused:
			return fmt.Errorf("%w: state=%s", domain.ErrPlanPaused, p.State)

		case domain.StateRollingBack:
			// 崩溃后可通过 Advance 对账回滚；重复操作不会重复副作用（token 幂等）。
			return s.executeRollback(ctx, p, "reconcile in-flight rollback")

		default:
			return fmt.Errorf("%w: state=%s", domain.ErrPlanFinished, p.State)
		}
	})
}

// Pause 仅在 Pending/Promoting/Gated 下允许；暂停时记录恢复位点。
func (s *Service) Pause(ctx context.Context, id string) (*domain.Plan, error) {
	return s.runForTarget(ctx, id, func(p *domain.Plan) error {
		switch p.State {
		case domain.StatePending, domain.StatePromoting, domain.StateGated:
			p.ResumeFrom = p.State
			if err := s.transition(p, domain.StatePaused, "operator pause", nil); err != nil {
				return err
			}
			p.AddEvent(domain.EventPaused, "paused by operator",
				map[string]interface{}{"resume_from": string(p.ResumeFrom)}, s.now())
			s.logf("[PAUSE] plan=%s %s -> Paused (resume_from=%s)", id, p.ResumeFrom, p.ResumeFrom)
			return nil
		case domain.StatePaused:
			return fmt.Errorf("%w", domain.ErrAlreadyPaused)
		case domain.StateRollingBack:
			return fmt.Errorf("%w: cannot pause while rolling back", domain.ErrInvalidTransition)
		default:
			return fmt.Errorf("%w: state=%s", domain.ErrPlanFinished, p.State)
		}
	})
}

// Resume 从暂停位点恢复；非暂停计划被拒绝。
func (s *Service) Resume(ctx context.Context, id string) (*domain.Plan, error) {
	return s.runForTarget(ctx, id, func(p *domain.Plan) error {
		if p.State != domain.StatePaused {
			if p.State.Terminal() {
				return fmt.Errorf("%w: state=%s", domain.ErrPlanFinished, p.State)
			}
			return fmt.Errorf("%w: state=%s", domain.ErrNotPaused, p.State)
		}
		to := p.ResumeFrom
		if to == "" {
			to = domain.StatePending
		}
		if !domain.CanTransition(domain.StatePaused, to) {
			to = domain.StatePending
		}
		if err := s.transition(p, to, "operator resume", map[string]interface{}{
			"resume_to": string(to),
		}); err != nil {
			return err
		}
		p.ResumeFrom = ""
		p.AddEvent(domain.EventResumed, "resumed by operator", nil, s.now())
		s.logf("[RESUME] plan=%s Paused -> %s", id, to)
		return nil
	})
}

// Rollback 发起显式回滚到上一个稳定版本。
// 已回滚/成功计划拒绝；重复回滚以 ErrDuplicateCommand 区分；
// RollingBack 中的再次调用做幂等对账，不重复执行副作用。
func (s *Service) Rollback(ctx context.Context, id, reason string) (*domain.Plan, error) {
	return s.runForTarget(ctx, id, func(p *domain.Plan) error {
		switch p.State {
		case domain.StateSucceeded, domain.StateRolledBack, domain.StateFailed:
			return fmt.Errorf("%w: state=%s", domain.ErrPlanFinished, p.State)
		case domain.StatePaused, domain.StateGated, domain.StatePromoting, domain.StatePending:
			return s.executeRollback(ctx, p, reason)
		case domain.StateRollingBack:
			// 回滚已在途（可能因延迟未完成）。重复指令被明确拒绝；
			// 对账/重试走 Advance 或进程重启时的 Recover（token 幂等，不重复回滚）。
			return fmt.Errorf("%w: rollback already in flight (attempts=%d)",
				domain.ErrDuplicateCommand, p.RollbackAttempts)
		default:
			return fmt.Errorf("%w: unknown state=%s", domain.ErrInvalidTransition, p.State)
		}
	})
}

// executeRollback 驱动回滚副作用：
//   - 先做快照对账，目标已在稳定版本时直接完结，绝不重复回滚；
//   - 意图 token 先持久化（由调用链 withPlan 保证），重放使用同一 token；
//   - 旧 token 已知失败/延迟时换新 token 继续，次数受 MaxAttempts 约束；
//   - 超过上限进入 Failed 终态。
func (s *Service) executeRollback(ctx context.Context, p *domain.Plan, reason string) error {
	if p.State != domain.StateRollingBack {
		if err := s.transition(p, domain.StateRollingBack, "rollback requested: "+reason, nil); err != nil {
			return err
		}
		p.AddEvent(domain.EventRollingBack, reason,
			map[string]interface{}{"to_version": p.StableVer}, s.now())
		s.logf("[ROLLBACK] plan=%s -> RollingBack reason=%q target=%s stable=%s",
			p.ID, reason, p.TargetID, p.StableVer)
	}

	want := domain.ActualState{Version: p.StableVer, Percent: 0, Replicas: 0}
	snap, snapErr := s.act.Snapshot(ctx, p.TargetID)
	if snapErr != nil && !errors.Is(snapErr, domain.ErrObservationMissing) {
		return snapErr // 数据损坏/schema 不支持：不可重试，立即上抛
	}
	if snapErr == nil && snap.Version == p.StableVer && snap.Percent == 0 && snap.Replicas == 0 {
		if err := s.transition(p, domain.StateRolledBack,
			"reconciled: target already at stable version "+p.StableVer, nil); err != nil {
			return err
		}
		p.RollbackToken = ""
		p.AddEvent(domain.EventRolledBack, "rollback complete by snapshot reconciliation",
			map[string]interface{}{"stable_ver": p.StableVer}, s.now())
		s.logf("[ROLLBACK] plan=%s -> RolledBack (reconciled by snapshot)", p.ID)
		return nil
	}

	for {
		isReplay := p.RollbackToken != ""
		if !isReplay {
			p.RollbackToken = rollbackToken(p.ID, p.RollbackAttempts+1)
		}
		token := p.RollbackToken
		res, duplicate, didApply, applyErr := s.act.Apply(ctx, p.TargetID, token, want)
		_ = res

		// 不可重试错误（数据损坏/schema 不支持/取消）直接上抛。
		if applyErr != nil && !errors.Is(applyErr, domain.ErrSideEffectFailed) {
			return applyErr
		}

		if applyErr != nil {
			if isReplay {
				p.RollbackToken = ""
				continue
			}
			p.RollbackAttempts++ // 仅硬失败计入回滚的有界重试
			if p.RollbackAttempts >= p.Config.MaxAttempts {
				return s.failTerminal(ctx, p, fmt.Errorf("%w: rollback failures=%d: %v",
					domain.ErrRetryExhausted, p.RollbackAttempts, applyErr))
			}
			p.RollbackToken = ""
			return applyErr
		}
		if !didApply {
			if duplicate {
				p.RollbackToken = ""
				continue
			}
			p.RollbackToken = ""
			return nil // 延迟：等待下一次驱动用新 token 重试
		}
		if err := s.transition(p, domain.StateRolledBack, "restored stable version "+p.StableVer,
			map[string]interface{}{"token": token}); err != nil {
			return err
		}
		p.RollbackToken = ""
		p.AddEvent(domain.EventRolledBack, "rollback complete; stable version restored",
			map[string]interface{}{"stable_ver": p.StableVer}, s.now())
		s.logf("[ROLLBACK] plan=%s -> RolledBack stable=%s token=%s", p.ID, p.StableVer, token)
		return nil
	}
}

// failTerminal 把计划移入 Failed 终态并保留原始判定依据。
func (s *Service) failTerminal(ctx context.Context, p *domain.Plan, cause error) error {
	if p.State.Terminal() {
		return cause
	}
	p.AddEvent(domain.EventFailed, "terminated by bound: "+cause.Error(), nil, s.now())
	if err := s.transition(p, domain.StateFailed, cause.Error(), nil); err != nil {
		return err
	}
	s.logf("[FAIL] plan=%s -> Failed cause=%v", p.ID, cause)
	return cause
}
