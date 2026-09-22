package orchestrator

import (
	"context"
	"fmt"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

// Recover 在进程启动时从磁盘对账所有计划，保证从一致位点继续：
//   - RollingBack 且存在在途 token：幂等重放回滚副作用，绝不重复回滚；
//     若当时副作用延迟，则保留在 RollingBack，等待 Advance 用新 token 重试；
//   - Promoting 且存在在途 token：幂等重放当前步副作用；
//   - 其他状态：原样恢复，绝不越级、绝不重新执行已完成的副作用。
//
// 旧 schema / 损坏数据在 store.List 阶段即被明确拒绝（ErrUnsupportedVersion）。
func (s *Service) Recover(ctx context.Context) error {
	all, err := s.store.List(ctx)
	if err != nil {
		return err
	}
	for _, p0 := range all {
		p := p0
		s.targetLk.withLock(p.TargetID, func() {
			s.planLk.withLock(p.ID, func() {
				latest, gerr := s.store.Get(ctx, p.ID)
				if gerr != nil {
					err = gerr
					return
				}
				switch latest.State {
				case domain.StateRollingBack:
					latest.AddEvent(domain.EventKind("recover"),
						"restart reconciliation of in-flight rollback", nil, s.now())
					s.logf("[RECOVER] plan=%s state=RollingBack attempts=%d token_present=%v",
						latest.ID, latest.RollbackAttempts, latest.RollbackToken != "")
					// executeRollback 会重放现有 token；延迟时保留状态等待后续驱动。
					if rerr := s.executeRollback(ctx, latest, "reconcile after restart"); rerr != nil {
						err = rerr
						return
					}
				case domain.StatePromoting:
					cur := latest.CurrentStep()
					if cur != nil && cur.AppliedToken == "" && cur.AttemptToken != "" {
						latest.AddEvent(domain.EventKind("recover"),
							"replaying in-flight step side effect",
							map[string]interface{}{"step": cur.Index, "token": cur.AttemptToken}, s.now())
						s.logf("[RECOVER] plan=%s state=Promoting replay step=%d token=%s",
							latest.ID, cur.Index, cur.AttemptToken)
						applied, reason, aerr := s.ensureStepApplied(ctx, latest, cur)
						if aerr != nil {
							err = s.failTerminal(ctx, latest, aerr)
							return
						}
						s.logf("[RECOVER] plan=%s replay result applied=%v reason=%s",
							latest.ID, applied, reason)
					} else {
						latest.AddEvent(domain.EventKind("recover"),
							"promoting step awaiting fresh observation", nil, s.now())
					}
				case domain.StateGated:
					cur := latest.CurrentStep()
					if cur != nil && cur.AppliedToken == "" && cur.AttemptToken != "" {
						// 崩溃窗口：上一门控已通过、下一步副作用延迟。对账成功即进入 Promoting。
						latest.AddEvent(domain.EventKind("recover"),
							"replaying pending next-step side effect after gate",
							map[string]interface{}{"step": cur.Index, "token": cur.AttemptToken}, s.now())
						applied, reason, aerr := s.replayPendingNext(ctx, latest)
						if aerr != nil {
							err = s.failTerminal(ctx, latest, aerr)
							return
						}
						if applied {
							if terr := s.transition(latest, domain.StatePromoting,
								"pending next step applied after recover: "+reason, nil); terr != nil {
								err = terr
								return
							}
						}
						s.logf("[RECOVER] plan=%s gated pending step applied=%v reason=%s",
							latest.ID, applied, reason)
					} else {
						latest.AddEvent(domain.EventKind("recover"),
							"gated step awaiting operator advance / fresh observation", nil, s.now())
					}
				default:
					latest.AddEvent(domain.EventKind("recover"),
						fmt.Sprintf("restart recovered state=%s without re-executing side effects", latest.State),
						nil, s.now())
				}
				if serr := s.store.Save(ctx, latest); serr != nil {
					err = serr
					return
				}
				p = latest
			})
		})
		if err != nil {
			return err
		}
	}
	s.logf("[RECOVER] scanned %d plans, consistency restored", len(all))
	return nil
}
