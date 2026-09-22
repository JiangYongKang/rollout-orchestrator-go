package orchestrator

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/actuator"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/store"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/strategy"
)

// defaultMaxPlans 限制同时存在的 plan 数（资源上限）。
const defaultMaxPlans = 100

type CreateParams struct {
	ID            string
	TargetID      string
	Strategy      domain.StrategyKind
	DesiredVer    string
	StableVer     string
	TotalReplicas int
	Config        domain.Config
}

// Service 是发布编排的唯一操作入口。
type Service struct {
	dataDir  string
	store    *store.Store
	act      *actuator.Actuator
	now      func() time.Time
	logger   *log.Logger
	maxPlans int
	targetLk *keyedLocks
	planLk   *keyedLocks
}

func NewService(dataDir string, st *store.Store, act *actuator.Actuator, logger *log.Logger) *Service {
	if logger == nil {
		logger = log.Default()
	}
	return &Service{
		dataDir:  dataDir,
		store:    st,
		act:      act,
		now:      time.Now,
		logger:   logger,
		maxPlans: defaultMaxPlans,
		targetLk: newKeyedLocks(),
		planLk:   newKeyedLocks(),
	}
}

// WithClock 注入测试时钟（用于确定性地验证观测过期）。
func (s *Service) WithClock(now func() time.Time) *Service {
	s.now = now
	return s
}

// WithMaxPlans 配置 plan 数量资源上限。
func (s *Service) WithMaxPlans(n int) *Service {
	s.maxPlans = n
	return s
}

func (s *Service) logf(format string, args ...interface{}) {
	s.logger.Printf(format, args...)
}

// Create 校验输入并以 Pending 态落盘新计划。
// 同一 target 同时只允许一个非终态计划（ErrTargetBusy）。
func (s *Service) Create(ctx context.Context, p CreateParams) (*domain.Plan, error) {
	if p.ID == "" || p.TargetID == "" {
		return nil, fmt.Errorf("%w: id and target_id required", domain.ErrInvalidArgument)
	}
	if p.DesiredVer == "" || p.StableVer == "" || p.DesiredVer == p.StableVer {
		return nil, fmt.Errorf("%w: desired/stable version required and must differ", domain.ErrInvalidArgument)
	}
	if err := p.Config.Validate(); err != nil {
		return nil, err
	}
	strat, err := strategy.New(p.Strategy)
	if err != nil {
		return nil, err
	}
	steps, err := strat.BuildSteps(p.DesiredVer, p.TotalReplicas)
	if err != nil {
		return nil, err
	}

	var out *domain.Plan
	var outErr error
	s.targetLk.withLock(p.TargetID, func() {
		all, err := s.store.List(ctx)
		if err != nil {
			outErr = err
			return
		}
		active := 0
		for _, ex := range all {
			if !ex.State.Terminal() {
				active++
			}
			if ex.TargetID == p.TargetID && !ex.State.Terminal() {
				outErr = fmt.Errorf("%w: target=%s active plan=%s state=%s",
					domain.ErrTargetBusy, p.TargetID, ex.ID, ex.State)
				return
			}
		}
		if s.maxPlans > 0 && active >= s.maxPlans {
			outErr = fmt.Errorf("%w: active plans=%d limit=%d",
				domain.ErrCapacityExceeded, active, s.maxPlans)
			return
		}

		now := s.now()
		plan := &domain.Plan{
			ID:            p.ID,
			TargetID:      p.TargetID,
			Strategy:      strat.Kind(),
			DesiredVer:    p.DesiredVer,
			StableVer:     p.StableVer,
			TotalReplicas: p.TotalReplicas,
			Steps:         steps,
			StepIdx:       0,
			State:         domain.StatePending,
			BudgetLeft:    p.Config.FailureBudget,
			Config:        p.Config,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		plan.AddEvent(domain.EventCreated, "plan created", map[string]interface{}{
			"strategy":   string(plan.Strategy),
			"steps":      len(steps),
			"stable_ver": plan.StableVer,
			"desired":    plan.DesiredVer,
		}, now)

		// 建立目标的基线实际状态（稳定版本、零放量），便于重启后对账。
		baseline := domain.ActualState{Version: p.StableVer, Percent: 0, Replicas: 0, UpdatedAt: now}
		if _, _, _, aerr := s.act.Apply(ctx, p.TargetID, "baseline:"+p.ID, baseline); aerr != nil {
			outErr = aerr
			return
		}
		if err := s.store.Create(ctx, plan); err != nil {
			outErr = err
			return
		}
		s.logf("[CREATE] plan=%s target=%s strategy=%s steps=%d desired=%s stable=%s -> state=%s",
			plan.ID, plan.TargetID, plan.Strategy, len(plan.Steps), plan.DesiredVer, plan.StableVer, plan.State)
		out = plan
	})
	return out, outErr
}

func (s *Service) Get(ctx context.Context, id string) (*domain.Plan, error) {
	p, err := s.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Service) List(ctx context.Context) ([]*domain.Plan, error) {
	return s.store.List(ctx)
}

// withPlan 串行化同一 plan 上的所有变更，并在变更后原子落盘。
func (s *Service) withPlan(ctx context.Context, id string, fn func(p *domain.Plan) error) (*domain.Plan, error) {
	var out *domain.Plan
	var outErr error
	s.planLk.withLock(id, func() {
		p, err := s.store.Get(ctx, id)
		if err != nil {
			outErr = err
			return
		}
		if err := fn(p); err != nil {
			outErr = err
			out = p // 即使失败也返回当前 plan，便于调用方观察状态
			return
		}
		if err := s.store.Save(ctx, p); err != nil {
			outErr = err
			return
		}
		out = p
	})
	return out, outErr
}

// Close 释放底层资源。服务本身不持有后台协程，
// 因而长时间运行不会泄漏 goroutine；此处仅关闭文件存储与模拟器。
func (s *Service) Close() error {
	if err := s.store.Close(); err != nil {
		return err
	}
	return s.act.Close()
}

// InjectFailN 让 target 接下来 n 个新 token 的副作用硬失败（测试/本地演示用）。
func (s *Service) InjectFailN(targetID string, n int) { s.act.SetFailN(targetID, n) }

// InjectLagN 让 target 接下来 n 个新 token 的副作用延迟不生效（测试/本地演示用）。
func (s *Service) InjectLagN(targetID string, n int) { s.act.SetLagN(targetID, n) }

// TargetSnapshot 读取目标当前模拟实际状态（测试/本地验证用）。
func (s *Service) TargetSnapshot(ctx context.Context, targetID string) (domain.ActualState, error) {
	return s.act.Snapshot(ctx, targetID)
}
