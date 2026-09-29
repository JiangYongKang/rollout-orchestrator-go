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
// 并发模型（锁序固定为 tmu -> pmu -> e.mu，禁止反向获取，避免死锁）：
//   - 同一目标的创建/推进/暂停/恢复/回滚由该目标的 tmu 串行化，
//     保证同一目标同时只有一个进行中的发布，也不会出现重复放量/半更新/状态跳级；
//   - 不同目标持不同 tmu，可完全并行；
//   - 每个计划另有一把 pmu（tmu 已覆盖当前活跃计划，pmu 兼容按 planID 的旧入口）；
//   - Report 只走 e.mu，因此上报永远不会被推进流程阻塞。
//
// 无后台协程，Close 不泄漏。目标互斥状态随计划持久化（State 非终态即占用目标），
// 进程重启后恢复，约束依旧生效。
type Engine struct {
	mu      sync.Mutex
	store   Store
	applier Applier
	limits  Limits
	plans   map[string]*Plan
	pmu     map[string]*sync.Mutex
	tmu     map[string]*sync.Mutex // 按目标串行化变更操作
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
		tmu:     map[string]*sync.Mutex{},
		notify:  map[string]chan struct{}{},
	}
	for _, p := range plans {
		if p.State == StateRollingBack {
			// 崩溃恢复：回滚副作用幂等，重放后落定终态。
			if err := e.applyWithRetry(p.Target, p.FromVersion, 0, 0); err != nil {
				p.State = StateFailed
			} else {
				p.State = StateRolledBack
			}
			p.UpdatedAt = time.Now()
		}
		e.plans[p.ID] = p
		e.pmu[p.ID] = &sync.Mutex{}
		e.targetLock(p.Target) // 恢复时建立/复用目标锁；活跃目标将拒绝新发布
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
// 调用方必须按 tmu -> pmu -> e.mu 的顺序持锁；本函数只短暂持有 e.mu 取指针。
func (e *Engine) getPlan(id string) (*Plan, *sync.Mutex, *sync.Mutex, chan struct{}, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.plans[id]
	if p == nil {
		return nil, nil, nil, nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return p, e.pmu[id], e.tmu[p.Target], e.notify[id], nil
}

// targetLock 返回（必要时创建）目标锁。调用方需持有 e.mu。
func (e *Engine) targetLock(target string) *sync.Mutex {
	l := e.tmu[target]
	if l == nil {
		l = &sync.Mutex{}
		e.tmu[target] = l
	}
	return l
}

// targetHasActiveLocked 判断目标上是否存在进行中（非终态）的发布。
func (e *Engine) targetHasActiveLocked(target, excludeID string) (string, bool) {
	for _, p := range e.plans {
		if p.Target == target && p.ID != excludeID && !p.State.Terminal() {
			return p.ID, true
		}
	}
	return "", false
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

func (e *Engine) applyWithRetry(target, version string, replicas, trafficPercent int) error {
	var err error
	for attempt := 0; attempt <= e.limits.MaxRetries; attempt++ {
		if err = e.applier.Apply(target, version, replicas, trafficPercent); err == nil {
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
	steps, err := BuildSteps(StepSpec{
		Kind:          p.Strategy,
		ToVersion:     p.ToVersion,
		TotalReplicas: p.TotalReplicas,
		BatchReplicas: p.BatchReplicas,
	})
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
	// 同一目标同时只允许一个进行中的发布；拒绝原因与参数错误、非法状态区分。
	if activeID, busy := e.targetHasActiveLocked(p.Target, ""); busy {
		return nil, fmt.Errorf("%w: target %s is occupied by plan %s", ErrTargetBusy, p.Target, activeID)
	}
	p.Steps = steps
	p.CurrentStep = -1
	p.State = StateRunning
	p.AppliedCmds = []string{cmdID}
	p.Gate = StepInfo{CompletedStep: -1, GateStep: -1, GateSeq: 0}
	p.UpdatedAt = time.Now()
	e.plans[p.ID] = &p
	e.pmu[p.ID] = &sync.Mutex{}
	e.targetLock(p.Target)
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

// Advance 推进到下一阶段：应用期望状态（有界重试）-> 等待多维门控确认。
//
// 阶段完成的唯一条件是 EvaluateGate 通过（版本/流量/承载三维度 + 健康同时满足），
// 且判定依据必须是属于当前阶段代次（GateSeq）的观测。崩溃/超时后重新调用本接口
// 会重入同一个未确认阶段并启用新代次，门控不可能被跳过。
// 健康判定连续失败达到预算时自动回滚到上一个稳定版本。
func (e *Engine) Advance(ctx context.Context, cmdID, id string) (*Plan, error) {
	p, pmu, tmu, notify, err := e.getPlan(id)
	if err != nil {
		return nil, err
	}
	tmu.Lock()
	defer tmu.Unlock()
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
	// 本次要判定的阶段：优先重入门控未确认的阶段（含崩溃/超时重入），
	// 否则进入上一个已确认阶段的下一个阶段——绝不跳过未确认阶段。
	stepIdx := p.Gate.CompletedStep + 1
	if p.Gate.GateStep >= 0 {
		stepIdx = p.Gate.GateStep
	}
	if stepIdx >= len(p.Steps) {
		e.mu.Unlock()
		return nil, fmt.Errorf("%w: no more steps", ErrInvalidTransition)
	}
	step := p.Steps[stepIdx]
	e.mu.Unlock()

	// 应用期望状态（幂等，崩溃后重放安全）。
	if err := e.applyWithRetry(p.Target, p.ToVersion, step.Replicas, step.Percent); err != nil {
		e.mu.Lock()
		p.State = StateFailed
		p.UpdatedAt = time.Now()
		e.saveLocked()
		e.mu.Unlock()
		return nil, err
	}

	e.mu.Lock()
	p.CurrentStep = stepIdx
	entering := p.Gate.GateStep != stepIdx
	p.Gate.GateStep = stepIdx
	if entering {
		// 新代次：此前所有观测都不能判定本阶段，杜绝跨阶段/跨重入通过。
		p.Gate.GateSeq++
	}
	p.UpdatedAt = time.Now()
	if err := e.saveLocked(); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	e.mu.Unlock()

	// 等待属于本代次的观测让多维门控通过。
	for {
		e.mu.Lock()
		gr := EvaluateGate(p, time.Now())
		switch {
		case gr.Healthy:
			p.Verdicts = appendVerdict(p.Verdicts, gr.Verdict)
			p.AppliedCmds = append(p.AppliedCmds, cmdID)
			p.FailStreak = 0
			p.Gate.CompletedStep = stepIdx
			p.Gate.GateStep = -1
			if stepIdx == len(p.Steps)-1 {
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
		case isHealthFailure(gr.Code):
			// 观测本身不健康（过期/缺指标/样本不足/错误率超标）：计入失败预算，
			// 结论只追加不改写；预算耗尽自动回滚。
			p.FailStreak++
			p.Verdicts = appendVerdict(p.Verdicts, gr.Verdict)
			if p.FailStreak >= p.Health.FailureBudget {
				// 失败预算耗尽：自动回滚到上一个稳定版本。
				p.State = StateRollingBack
				p.Gate.GateStep = -1
				p.UpdatedAt = time.Now()
				e.saveLocked()
				e.mu.Unlock()
				rerr := e.applyWithRetry(p.Target, p.FromVersion, 0, 0)
				e.mu.Lock()
				if rerr != nil {
					p.State = StateFailed
				} else {
					p.State = StateRolledBack
				}
				p.UpdatedAt = time.Now()
				e.saveLocked()
				e.mu.Unlock()
				return nil, fmt.Errorf("%w: %s (auto-rollback -> %s)", ErrHealthGate, gr.Reason, p.State)
			}
			p.UpdatedAt = time.Now()
			e.saveLocked()
			e.mu.Unlock()
		default:
			// no-eligible-observation 或版本/流量/承载维度尚未对上：
			// 属延迟/抖动/短暂未上报，只能继续等；不提前放行、不跳过、不改结论。
			e.mu.Unlock()
		}

		select {
		case <-notify:
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: step %d (%s): %v", ErrNotConverged, stepIdx, step.Name, ctx.Err())
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
	p, pmu, tmu, _, err := e.getPlan(id)
	if err != nil {
		return nil, err
	}
	tmu.Lock()
	defer tmu.Unlock()
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
	p, pmu, tmu, _, err := e.getPlan(id)
	if err != nil {
		return nil, err
	}
	tmu.Lock()
	defer tmu.Unlock()
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
	p.Gate.GateStep = -1
	p.UpdatedAt = time.Now()
	e.saveLocked()
	e.mu.Unlock()

	rerr := e.applyWithRetry(p.Target, p.FromVersion, 0, 0)

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
	if obs.Replicas < 0 || (obs.HasTraffic && (obs.TrafficPercent < 0 || obs.TrafficPercent > 100)) {
		return fmt.Errorf("%w: replicas/traffic out of range", ErrInvalidArgument)
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
	// 盖上当前阶段判定代次：该观测只能参与当前（或崩溃重入后重新打标）阶段的判定，
	// 不能在以后被拿去通过后续阶段。
	if p.Gate.GateStep >= 0 {
		obs.GateSeq = p.Gate.GateSeq
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
