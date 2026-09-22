package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

// desiredActual 构造当前步骤期望的"目标实际状态"。
// canary 语义以 Percent 表达；wave 语义以 Replicas 表达。
func desiredActual(plan *domain.Plan, step *domain.Step) domain.ActualState {
	switch plan.Strategy {
	case domain.StrategyWave:
		return domain.ActualState{
			Version:  plan.DesiredVer,
			Replicas: step.Desired,
			Percent:  int(math.Round(float64(step.Desired) * 100.0 / float64(plan.TotalReplicas))),
		}
	default:
		replicas := int(math.Round(float64(step.Desired) * float64(plan.TotalReplicas) / 100.0))
		return domain.ActualState{
			Version:  plan.DesiredVer,
			Percent:  step.Desired,
			Replicas: replicas,
		}
	}
}

func stepToken(planID string, idx int) string {
	return fmt.Sprintf("step:%s:%d:%d", planID, idx, time.Now().UnixNano())
}

func rollbackToken(planID string, attempt int) string {
	return fmt.Sprintf("rollback:%s:%d:%d", planID, attempt, time.Now().UnixNano())
}

// transition 断言状态迁移合法并落一条可解释事件；非法迁移直接拒绝。
func (s *Service) transition(p *domain.Plan, to domain.PlanState, reason string, detail map[string]interface{}) error {
	from := p.State
	if !domain.CanTransition(from, to) {
		return fmt.Errorf("%w: %s -> %s (%s)", domain.ErrInvalidTransition, from, to, reason)
	}
	p.State = to
	p.AddEvent(domain.EventKind("state_"+string(to)), reason, detail, s.now())
	return nil
}

// actualEqualsWant 比较 actuator 当前快照是否已等于期望状态
// （两种策略各有权威字段：wave 看副本数，canary 看百分比）。
func actualEqualsWant(plan *domain.Plan, got, want domain.ActualState) bool {
	if got.Version != want.Version {
		return false
	}
	if plan.Strategy == domain.StrategyWave {
		return got.Replicas == want.Replicas
	}
	return got.Percent == want.Percent
}

// ensureStepApplied 以"快照对账 -> 重放在先 -> 新尝试在后"的顺序驱动当前步副作用。
//   - 崩溃窗口内若副作用已生效但计划未及存盘，快照对账直接认领，不重复执行；
//   - 崩溃重放使用同一 token，actuator 保证幂等；
//   - 重放发现旧 token 失败/延迟时，丢弃旧 token 发起新尝试；
//   - 硬失败受 MaxAttempts 上界约束，超出返回 ErrRetryExhausted。
//
// 返回 applied=false 表示副作用尚未生效（失败或延迟）。
func (s *Service) ensureStepApplied(ctx context.Context, p *domain.Plan, cur *domain.Step) (applied bool, reason string, err error) {
	want := desiredActual(p, cur)

	if cur.AppliedToken != "" {
		return true, "step already applied: " + cur.AppliedToken, nil
	}

	// 崩溃窗口对账：目标实际状态已经是期望值时，绝不再次执行副作用。
	if snap, snapErr := s.act.Snapshot(ctx, p.TargetID); snapErr == nil {
		if actualEqualsWant(p, snap, want) {
			cur.AppliedToken = fmt.Sprintf("reconciled:step:%s:%d", p.ID, cur.Index)
			cur.AttemptToken = ""
			cur.LastAppliedAt = s.now()
			return true, fmt.Sprintf("reconciled pre-crash side effect by snapshot=%v", snap), nil
		}
	} else if !errors.Is(snapErr, domain.ErrObservationMissing) {
		// 磁盘损坏/schema 不支持属于不可重试错误，立即上抛，不得消耗失败计数。
		return false, "target state unreadable", snapErr
	}

	for {
		isReplay := cur.AttemptToken != ""
		token := cur.AttemptToken
		if !isReplay {
			// 延迟不是失败，不消耗失败计数；token 仅用于幂等。
			token = stepToken(p.ID, cur.Index)
			cur.AttemptToken = token
			cur.LastAppliedAt = s.now()
		}

		res, duplicate, didApply, applyErr := s.act.Apply(ctx, p.TargetID, token, want)

		// 不可重试的基础设施错误（schema 不支持/数据损坏/上下文取消）：
		// 直接上抛，绝不静默吞掉或误算为放量失败。
		if applyErr != nil && !errors.Is(applyErr, domain.ErrSideEffectFailed) {
			return false, "side effect infra error", applyErr
		}

		if applyErr != nil {
			if isReplay {
				// 旧 token 已确定失败（计数已在首次发生）：丢弃并立即换新 token 重试。
				cur.AttemptToken = ""
				continue
			}
			cur.Attempts++ // 仅硬失败计入有界重试
			p.AddEvent(domain.EventApplyFailed, applyErr.Error(), map[string]interface{}{
				"step": cur.Index, "token": token, "failures": cur.Attempts,
			}, s.now())
			cur.AttemptToken = ""
			if cur.Attempts >= p.Config.MaxAttempts {
				return false, fmt.Sprintf("failures=%d >= max=%d: %v", cur.Attempts, p.Config.MaxAttempts, applyErr),
					fmt.Errorf("%w: step=%d last_error=%v", domain.ErrRetryExhausted, cur.Index, applyErr)
			}
			return false, applyErr.Error(), nil
		}

		if didApply {
			cur.AppliedToken = token
			cur.AttemptToken = ""
			cur.LastAppliedAt = s.now()
			_ = res
			verb := "applied"
			if duplicate {
				verb = "replay-applied"
			}
			return true, fmt.Sprintf("%s token=%s desired=%v", verb, token, want), nil
		}

		// 延迟：重放的旧 token 当时未生效则丢弃换新；新 token 延迟则交还调用方稍后重试。
		if isReplay {
			cur.AttemptToken = ""
			continue
		}
		cur.AttemptToken = ""
		return false, "side effect delayed (lag): actual stays at stable version", nil
	}
}

// advanceToNext 先对"下一步"执行幂等副作用，只有确认生效后才前移 StepIdx。
// 副作用的尝试 token 会先写回 p.Steps 并由调用链持久化，因此即使延迟或
// 在崩溃窗口内被杀，重启后也能用同一 token 对账，不重复放量。
// 返回 moved=false 表示副作用尚未生效（reason 可解释），调用方保持当前状态。
func (s *Service) advanceToNext(ctx context.Context, p *domain.Plan) (moved bool, to domain.PlanState, reason string, err error) {
	cur := p.CurrentStep()
	if cur == nil || cur.Index == len(p.Steps)-1 {
		return false, "", "no next step", nil
	}
	nextIdx := cur.Index + 1
	next := &p.Steps[nextIdx]
	applied, r, aerr := s.ensureStepApplied(ctx, p, next)
	if aerr != nil {
		return false, "", r, aerr
	}
	if !applied {
		return false, "", r, nil // StepIdx 不动；AttemptToken 已挂在 next 上（若为重放/延迟）
	}
	p.StepIdx = nextIdx
	return true, domain.StatePromoting, r, nil
}

// replayPendingNext 对账"指针已到下一步但副作用尚未确认生效"的场景：
// 发生在 Gated 中下一步副作用延迟、进程随后被杀的窗口。
func (s *Service) replayPendingNext(ctx context.Context, p *domain.Plan) (applied bool, reason string, err error) {
	cur := p.CurrentStep()
	if cur == nil || cur.AppliedToken != "" || cur.AttemptToken == "" {
		return true, "no pending next step", nil
	}
	return s.ensureStepApplied(ctx, p, cur)
}

// checkHealth 执行健康门控：关键指标缺失或越界都判定为不健康，
// 并逐条返回可解释依据。
func checkHealth(p *domain.Plan, obs *domain.Observation) (healthy bool, reasons []string) {
	for _, name := range p.Config.CriticalMetrics {
		m, ok := obs.Metrics[name]
		if !ok || !m.Exists {
			reasons = append(reasons, fmt.Sprintf("critical metric %q missing", name))
			continue
		}
		bounds := p.Config.MetricBounds[name]
		if m.Value < bounds[0] || m.Value > bounds[1] {
			reasons = append(reasons, fmt.Sprintf("metric %q=%.4f outside bounds [%.4f,%.4f]",
				name, m.Value, bounds[0], bounds[1]))
		}
	}
	return len(reasons) == 0, reasons
}
