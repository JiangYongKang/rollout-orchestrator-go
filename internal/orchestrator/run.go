package orchestrator

import (
	"context"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

// runForTarget 以固定锁序（target -> plan）执行变更：
// target 锁串行化同一目标上的所有副作用相关操作，
// plan 锁串行化同一计划上的状态迁移。
// 锁序全局一致，不会死锁。
func (s *Service) runForTarget(ctx context.Context, id string, fn func(p *domain.Plan) error) (*domain.Plan, error) {
	// 先读一次以找到 target（读操作不需要锁，文件存储本身原子）。
	p, err := s.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	target := p.TargetID

	var out *domain.Plan
	var outErr error
	s.targetLk.withLock(target, func() {
		s.planLk.withLock(id, func() {
			latest, gerr := s.store.Get(ctx, id)
			if gerr != nil {
				outErr = gerr
				return
			}
			if ferr := fn(latest); ferr != nil {
				// 即使 fn 失败也保存：事件与副作用尝试计数必须可解释、可恢复。
				latest.AddEvent(domain.EventRejected, ferr.Error(),
					map[string]interface{}{"state": string(latest.State)}, s.now())
				if serr := s.store.Save(ctx, latest); serr != nil {
					outErr = serr
					return
				}
				out = latest
				outErr = ferr
				return
			}
			if serr := s.store.Save(ctx, latest); serr != nil {
				outErr = serr
				return
			}
			out = latest
		})
	})
	return out, outErr
}
