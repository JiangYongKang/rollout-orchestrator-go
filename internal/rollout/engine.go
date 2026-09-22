package rollout

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// verdictCap 每个计划保留的判定审计条数上限（防内存膨胀）。
const verdictCap = 1000

// Engine 发布编排器，对外操作入口。
//
// 并发模型：engine.mu 保护 plans/notify 与落盘；每个计划一把 pmu 串行化
// 针对该计划的变更类操作（Advance/Pause/Resume/Rollback），Report 只走
// engine.mu，因此上报永远不会被推进流程阻塞。无后台协程，Close 不泄漏。
type Engine struct {
	mu      sync.Mutex
	store   Store
	applier Applier
	limits  Limits
	plans   map[string]*Plan
	pmu     map[string]*sync.Mutex
	notify  map[string]chan struct{}
	closed  bool
}

// NewEngine 创建编排器并从 store 恢复。
// 恢复规则：RollingBack 是崩溃可能留下的中间态，重启后幂等重放回滚到
// RolledBack；其余状态原样恢复，不重复已完成的副作用（Apply 幂等）。
func NewEngine(store Store, applier Applier, limits Limits) (*Engine, error) {
	if store == nil || applier == nil {
		return nil, fmt.Errorf("%w: store and applier are required", ErrInvalidArgument)
	}
	if limits.MaxPlans <= 0 {
		limits.MaxPlans = 100
	}
	if limits.MaxRetries <= 0 {
		limits.MaxRetries = 3
	}
	if limits.MaxObservations <= 0 {
		limits.MaxObservations = 100
	}
	plans, err := store.Load()
	if err != nil {
		return nil, err
	}
	e := &Engine{
		store:   store,
		applier: applier,
		limits:  limits,
		plans:   map[string]*Plan{},
		pmu:     map[string]*sync.Mutex{},
		notify:  map[string]chan struct{}{},
	}
	for _, p := range plans {
		if p.State == StateRollingBack {
			// 崩溃恢复：回滚副作用幂等，重放后落定终态。
			if err := e.applyWithRetry(p.Target, p.FromVersion, 0); err != nil {
				p.State = StateFailed
			} else {
				p.State = StateRolledBack
			}
			p.UpdatedAt = time.Now()
		}
		e.plans[p.ID] = p
		e.pmu[p.ID] = &sync.Mutex{}
		e.notify[p.ID] = make(chan struct{}, 1)
	}
	if err := e.saveLocked(); err != nil {
		return nil, err
	}
	return e, nil
}

// Close 落盘并标记关闭。无后台协程需要停止。
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	return e.saveLocked()
}

func (e *Engine) saveLocked() error {
	plans := make([]*Plan, 0, len(e.plans))
	for _, p := range e.plans {
		plans = append(plans, p)
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].ID < plans[j].ID })
	return e.store.SaveAll(plans)
}

// getPlan 取计划与配套锁，不存在时报 ErrNotFound。
func (e *Engine) getPlan(id string) (*Plan, *sync.Mutex, chan struct{}, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.plans[id]
	if p == nil {
		return nil, nil, nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return p, e.pmu[id], e.notify[id], nil
}

// checkCmd 命令幂等：重复命令直接拒绝，可区分。
func checkCmd(p *Plan, cmdID string) error {
	if cmdID == "" {
		return fmt.Errorf("%w: cmdID required", ErrInvalidArgument)
	}
	for _, c := range p.AppliedCmds {
		if c == cmdID {
			return fmt.Errorf("%w: %s", ErrDuplicateCommand, cmdID)
		}
	}
	return nil
}

func (e *Engine) applyWithRetry(target, version string, replicas int) error {
	var err error
	for attempt := 0; attempt <= e.limits.MaxRetries; attempt++ {
		if err = e.applier.Apply(target, version, replicas); err == nil {
			return nil
		}
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
	return fmt.Errorf("%w: %v", ErrRetryExhausted, err)
}

// Create 创建发布计划（状态 Running，尚未应用任何期望状态）。
func (e *Engine) Create(cmdID string, p Plan) (*Plan, error) {
	if p.ID == "" || p.Target == "" || p.ToVersion == "" {
		return nil, fmt.Errorf("%w: id/target/toVersion required", ErrInvalidArgument)
	}
	steps, err := BuildSteps(p.Strategy, p.TotalReplicas)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, fmt.Errorf("%w: engine closed", ErrInvalidArgument)
	}
	if _, ok := e.plans[p.ID]; ok {
		return nil, fmt.Errorf("%w: plan %s exists", ErrDuplicateCommand, p.ID)
	}
	if len(e.plans) >= e.limits.MaxPlans {
		return nil, fmt.Errorf("%w: max plans %d", ErrLimitExceeded, e.limits.MaxPlans)
	}
	p.Steps = steps
	p.CurrentStep = -1
	p.State = StateRunning
	p.AppliedCmds = []string{cmdID}
	p.UpdatedAt = time.Now()
	e.plans[p.ID] = &p
	e.pmu[p.ID] = &sync.Mutex{}
	e.notify[p.ID] = make(chan struct{}, 1)
	if err := e.saveLocked(); err != nil {
		delete(e.plans, p.ID)
		delete(e.pmu, p.ID)
		delete(e.notify, p.ID)
		return nil, err
	}
	cp := p
	return &cp, nil
}

// Get 查询计划快照。
func (e *Engine) Get(id string) (*Plan, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.plans[id]
	if p == nil {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	cp := *p
	return &cp, nil
}

// Advance 推进到下一阶段：应用期望状态（有界重试）-> 等待收敛 + 健康门控。
// 健康判定连续失败达到预算时自动回滚到上一个稳定版本。
func (e *Engine) Advance(ctx context.Context, cmdID, id string) (*Plan, error) {
	p, pmu, notify, err := e.getPlan(id)
	if err != nil {
		return nil, err
	}
	pmu.Lock()
	defer pmu.Unlock()

	e.mu.Lock()
	if p.State.Terminal() {
		e.mu.Unlock()
		return nil, fmt.Errorf("%w: state=%s", ErrPlanFinished, p.State)
	}
	if p.State != StateRunning {
		e.mu.Unlock()
		return nil, fmt.Errorf("%w: %s -> Advance", ErrInvalidTransition, p.State)
	}
	if err := checkCmd(p, cmdID); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	next := p.CurrentStep + 1
	if next >= len(p.Steps) {
		e.mu.Unlock()
		return nil, fmt.Errorf("%w: no more steps", ErrInvalidTransition)
	}
	step := p.Steps[next]
	e.mu.Unlock()

	// 应用期望状态（幂等，崩溃后重放安全）。
	if err := e.applyWithRetry(p.Target, p.ToVersion, step.Replicas); err != nil {
		e.mu.Lock()
		p.State = StateFailed
		p.UpdatedAt = time.Now()
		e.saveLocked()
		e.mu.Unlock()
		return nil, err
	}

	e.mu.Lock()
	p.CurrentStep = next
	p.UpdatedAt = time.Now()
	if err := e.saveLocked(); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	e.mu.Unlock()

	// 等待收敛 + 健康门控。
	for {
		e.mu.Lock()
		v := Evaluate(p, time.Now())
		converged := len(p.Observations) > 0 &&
			p.Observations[len(p.Observations)-1].Replicas == step.Replicas
		switch {
		case converged && v.Healthy:
			p.Verdicts = appendVerdict(p.Verdicts, v)
			p.AppliedCmds = append(p.AppliedCmds, cmdID)
			p.FailStreak = 0
			if next == len(p.Steps)-1 {
				p.State = StateSucceeded
			}
			p.UpdatedAt = time.Now()
			err := e.saveLocked()
			cp := *p
			e.mu.Unlock()
			if err != nil {
				return nil, err
			}
			return &cp, nil
		case !v.Healthy:
			p.FailStreak++
			p.Verdicts = appendVerdict(p.Verdicts, v)
			if p.FailStreak >= p.Health.FailureBudget {
				// 失败预算耗尽：自动回滚到上一个稳定版本。
				p.State = StateRollingBack
				p.UpdatedAt = time.Now()
				e.saveLocked()
				e.mu.Unlock()
				rerr := e.applyWithRetry(p.Target, p.FromVersion, 0)
				e.mu.Lock()
				if rerr != nil {
					p.State = StateFailed
				} else {
					p.State = StateRolledBack
				}
				p.UpdatedAt = time.Now()
				e.saveLocked()
				e.mu.Unlock()
				return nil, fmt.Errorf("%w: %s (auto-rollback -> %s)", ErrHealthGate, v.Reason, p.State)
			}
			p.UpdatedAt = time.Now()
			e.saveLocked()
			e.mu.Unlock()
		default:
			e.mu.Unlock()
		}

		select {
		case <-notify:
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: step %d (%s): %v", ErrNotConverged, next, step.Name, ctx.Err())
		}
	}
}

func appendVerdict(vs []Verdict, v Verdict) []Verdict {
	vs = append(vs, v)
	if len(vs) > verdictCap {
		vs = vs[len(vs)-verdictCap:]
	}
	return vs
}

// Pause 暂停计划（仅 Running 可暂停）。
func (e *Engine) Pause(cmdID, id string) (*Plan, error) {
	return e.transition(cmdID, id, StateRunning, StatePaused, "Pause")
}

// Resume 恢复暂停的计划。
func (e *Engine) Resume(cmdID, id string) (*Plan, error) {
	return e.transition(cmdID, id, StatePaused, StateRunning, "Resume")
}

func (e *Engine) transition(cmdID, id string, from, to State, op string) (*Plan, error) {
	p, pmu, _, err := e.getPlan(id)
	if err != nil {
		return nil, err
	}
	pmu.Lock()
	defer pmu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	if p.State.Terminal() {
		return nil, fmt.Errorf("%w: state=%s", ErrPlanFinished, p.State)
	}
	if p.State != from {
		return nil, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, p.State, op)
	}
	if err := checkCmd(p, cmdID); err != nil {
		return nil, err
	}
	p.State = to
	p.AppliedCmds = append(p.AppliedCmds, cmdID)
	p.UpdatedAt = time.Now()
	if err := e.saveLocked(); err != nil {
		return nil, err
	}
	cp := *p
	return &cp, nil
}

// Rollback 手动回滚到上一个稳定版本（Running/Paused 可触发）。
func (e *Engine) Rollback(cmdID, id string) (*Plan, error) {
	p, pmu, _, err := e.getPlan(id)
	if err != nil {
		return nil, err
	}
	pmu.Lock()
	defer pmu.Unlock()

	e.mu.Lock()
	if p.State.Terminal() {
		e.mu.Unlock()
		return nil, fmt.Errorf("%w: state=%s", ErrPlanFinished, p.State)
	}
	if p.State != StateRunning && p.State != StatePaused {
		e.mu.Unlock()
		return nil, fmt.Errorf("%w: %s -> Rollback", ErrInvalidTransition, p.State)
	}
	if err := checkCmd(p, cmdID); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	p.State = StateRollingBack
	p.UpdatedAt = time.Now()
	e.saveLocked()
	e.mu.Unlock()

	rerr := e.applyWithRetry(p.Target, p.FromVersion, 0)

	e.mu.Lock()
	defer e.mu.Unlock()
	if rerr != nil {
		p.State = StateFailed
		p.UpdatedAt = time.Now()
		e.saveLocked()
		return nil, rerr
	}
	p.State = StateRolledBack
	p.AppliedCmds = append(p.AppliedCmds, cmdID)
	p.UpdatedAt = time.Now()
	if err := e.saveLocked(); err != nil {
		return nil, err
	}
	cp := *p
	return &cp, nil
}

// Report 上报实际状态观测（按 ReportID 幂等去重，重复上报拒绝）。
func (e *Engine) Report(id string, obs Observation) error {
	if obs.ReportID == "" {
		return fmt.Errorf("%w: reportID required", ErrInvalidArgument)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.plans[id]
	if p == nil {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if p.State.Terminal() {
		return fmt.Errorf("%w: state=%s", ErrPlanFinished, p.State)
	}
	for _, r := range p.SeenReports {
		if r == obs.ReportID {
			return fmt.Errorf("%w: %s", ErrDuplicateReport, obs.ReportID)
		}
	}
	p.Observations = append(p.Observations, obs)
	if len(p.Observations) > e.limits.MaxObservations {
		p.Observations = p.Observations[len(p.Observations)-e.limits.MaxObservations:]
	}
	p.SeenReports = append(p.SeenReports, obs.ReportID)
	if len(p.SeenReports) > e.limits.MaxObservations {
		p.SeenReports = p.SeenReports[len(p.SeenReports)-e.limits.MaxObservations:]
	}
	p.UpdatedAt = time.Now()
	if err := e.saveLocked(); err != nil {
		return err
	}
	// 非阻塞唤醒等待中的 Advance。
	select {
	case e.notify[id] <- struct{}{}:
	default:
	}
	return nil
}
