package rollout

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	// verdictCap 每个计划保留的判定审计条数上限（防内存膨胀）。
	verdictCap = 1000
	// flushInterval 去抖刷盘间隔：高频上报期间合并落盘，避免每条上报同步全量写。
	flushInterval = 50 * time.Millisecond
	// reevalInterval 等待门控期间的兜底重估间隔：防止通知通道里的旧信号被消费后
	// 纯等下一条上报（上报本身有间隔），保证延迟受控。仅在 Advance 调用栈内使用，
	// 不产生额外后台协程。
	reevalInterval = 20 * time.Millisecond
)

// Engine 发布编排器，对外操作入口。
//
// 并发模型：
//   - engine.mu 保护 plans/notify 与内存状态；Report 只走 engine.mu，不做同步磁盘写。
//   - 每个目标一把 tmu：同一目标上的创建/推进/暂停/回滚严格串行，不同目标并行。
//   - 每个计划一把 pmu：与 tmu 一起把同一计划的变更串行化（加锁顺序 tmu -> pmu）。
//   - 进行中（非终态）计划对目标具有排他性：同目标新计划创建返回 ErrTargetBusy，
//     该集合随存储持久化，进程重启后约束照样生效。
//   - 单个去抖刷盘协程合并落盘；状态迁移/终态/关闭强制同步刷盘，崩溃恢复语义不变。
type Engine struct {
	mu      sync.Mutex
	store   Store
	applier Applier
	limits  Limits
	plans   map[string]*Plan
	pmu     map[string]*sync.Mutex
	tmu     map[string]*sync.Mutex
	notify  map[string]chan struct{}
	closed  bool
	dirty   bool          // 自上次刷盘后是否有未落盘变更
	stopFl  chan struct{} // 关闭刷盘协程
	flDone  chan struct{} // 刷盘协程退出
}

// NewEngine 创建编排器并从 store 恢复。
// 恢复规则：
//   - RollingBack 是崩溃可能留下的中间态，重启后幂等重放回滚到 RolledBack；
//   - Running 且存在“在途但下发未确认”（PendingStep>=0 && !PendingApplied）的阶段时：
//     崩溃可能发生在该批期望下发之前/之中，重启即幂等补发同一期望，随后照常等待
//     门控，表现与未中断一致；补发成功置 PendingApplied=true，重试耗尽则 Failed；
//   - Paused 计划挂着未确认在途时，丢弃该在途位点（保留 CurrentStep），Resume 后
//     从同一阶段重新应用（Apply 幂等），绝不在暂停状态下擅自放量；
//   - 其余状态（含旧版本数据迁移后的位点）原样恢复，已确认下发的阶段只等待不重放。
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
		stopFl:  make(chan struct{}),
		flDone:  make(chan struct{}),
	}
	var recoveryErr error
	for _, p := range plans {
		switch {
		case p.State == StateRollingBack:
			// 崩溃恢复：回滚副作用幂等，重放后落定终态。
			if err := e.applyWithRetry(p.Target, p.FromVersion, 0, 0); err != nil {
				p.State = StateFailed
				recoveryErr = fmt.Errorf("%w: recovery rollback plan %s: %v", ErrRetryExhausted, p.ID, err)
			} else {
				p.State = StateRolledBack
			}
			p.PendingStep = -1
			p.PendingApplied = false
			p.UpdatedAt = time.Now()
		case p.State == StateRunning && p.PendingStep >= 0 && !p.PendingApplied:
			// 关键恢复：在途阶段的期望状态下发未确认（崩溃在下发前/下发中），
			// 重启后幂等补发同一批期望，使其与未中断时一样继续等待门控直至成功。
			// Apply 幂等：若崩溃时其实已下发成功，补发同一期望也不重复放量。
			step := p.Steps[p.PendingStep]
			if err := e.applyWithRetry(p.Target, p.expectedVersion(step), step.Replicas, step.Percent); err != nil {
				p.State = StateFailed
				p.UpdatedAt = time.Now()
				recoveryErr = fmt.Errorf("%w: recovery replay plan %s step %d: %v", ErrRetryExhausted, p.ID, p.PendingStep, err)
			} else {
				p.PendingApplied = true
				p.UpdatedAt = time.Now()
			}
		case p.State == StatePaused && p.PendingStep >= 0 && !p.PendingApplied:
			// 暂停中的计划不应在重启时被自动放量：丢弃未确认在途位点，Resume 后
			// Advance 会对同一阶段重新走“登记 -> 下发 -> 等待”（Apply 幂等）。
			p.PendingStep = -1
			p.PendingApplied = false
			p.UpdatedAt = time.Now()
		}
		e.plans[p.ID] = p
		e.pmu[p.ID] = &sync.Mutex{}
		tl := e.tmu[p.Target]
		if tl == nil {
			tl = &sync.Mutex{}
			e.tmu[p.Target] = tl
		}
		e.notify[p.ID] = make(chan struct{}, 1)
	}
	// 即使恢复补发失败也要先把 Failed 位点落盘，再把错误返回给调用方（可与
	// 参数非法、状态非法、目标占用等原因区分），避免静默“看似恢复成功”。
	if err := e.store.SaveAll(e.snapshotLocked()); err != nil {
		return nil, err
	}
	if recoveryErr != nil {
		return nil, recoveryErr
	}
	go e.flusher()
	return e, nil
}

// flusher 周期性合并落盘。刷盘内容是持锁取出的快照，实际磁盘写在锁外进行，
// 因此高频上报不会被磁盘 IO 阻塞。
func (e *Engine) flusher() {
	defer close(e.flDone)
	t := time.NewTicker(flushInterval)
	defer t.Stop()
	for {
		select {
		case <-e.stopFl:
			return
		case <-t.C:
			e.flush(false)
		}
	}
}

// snapshotLocked 取出按 ID 排序的计划快照（浅拷贝 Plan 值，避免写盘期间被并发改）。
// 调用方持 e.mu。
func (e *Engine) snapshotLocked() []*Plan {
	plans := make([]*Plan, 0, len(e.plans))
	for _, p := range e.plans {
		cp := *p
		plans = append(plans, &cp)
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].ID < plans[j].ID })
	return plans
}

// flush 落盘一次。sync=true 时无论是否有脏标记都强制写（状态迁移/关闭路径）。
func (e *Engine) flush(syncWrite bool) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	if !syncWrite && !e.dirty {
		e.mu.Unlock()
		return nil
	}
	snap := e.snapshotLocked()
	e.dirty = false
	e.mu.Unlock()
	return e.store.SaveAll(snap) // 原子写（tmp+rename），与刷盘协程/同步写互斥由 store 保证
}

// markDirtyLocked 标记内存状态已变更，由去抖协程尽快合并落盘。调用方持 e.mu。
func (e *Engine) markDirtyLocked() { e.dirty = true }

// persistLocked 立即同步落盘（用于状态迁移、位点变更、终态等恢复关键点）。
// 调用方持 e.mu。
func (e *Engine) persistLocked() error {
	snap := e.snapshotLocked()
	e.dirty = false
	e.mu.Unlock()
	err := e.store.SaveAll(snap)
	e.mu.Lock()
	return err
}

// Close 同步落盘并停止刷盘协程。
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	snap := e.snapshotLocked()
	e.dirty = false
	e.mu.Unlock()

	close(e.stopFl)
	<-e.flDone
	return e.store.SaveAll(snap)
}

// targetBusyLocked 判断目标是否已有进行中（非终态）的发布。调用方持 e.mu。
func (e *Engine) targetBusyLocked(target string) (string, bool) {
	for _, p := range e.plans {
		if p.Target == target && !p.State.Terminal() {
			return p.ID, true
		}
	}
	return "", false
}

// lockPlan 按 tmu -> pmu 顺序锁定计划及其目标，返回解锁函数。
// 同一目标的所有变更操作由此串行；不同目标互不阻塞。
func (e *Engine) lockPlan(id string) (*Plan, chan struct{}, func(), error) {
	e.mu.Lock()
	p := e.plans[id]
	if p == nil {
		e.mu.Unlock()
		return nil, nil, nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	tl := e.tmu[p.Target]
	pl := e.pmu[id]
	ch := e.notify[id]
	e.mu.Unlock()

	tl.Lock()
	pl.Lock()
	unlock := func() {
		pl.Unlock()
		tl.Unlock()
	}
	return p, ch, unlock, nil
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

// appendCmd 记录已应用命令并按观测上限环形裁剪，防长期运行膨胀。
func appendCmd(p *Plan, cmdID string, limit int) {
	p.AppliedCmds = append(p.AppliedCmds, cmdID)
	if limit > 0 && len(p.AppliedCmds) > limit {
		p.AppliedCmds = p.AppliedCmds[len(p.AppliedCmds)-limit:]
	}
}

func (e *Engine) applyWithRetry(target, version string, replicas, percent int) error {
	var err error
	for attempt := 0; attempt <= e.limits.MaxRetries; attempt++ {
		if err = e.applier.Apply(target, version, replicas, percent); err == nil {
			return nil
		}
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
	return fmt.Errorf("%w: %v", ErrRetryExhausted, err)
}

// Create 创建发布计划（状态 Running，尚未应用任何期望状态）。
// 同一目标只允许存在一个进行中的发布，冲突时返回 ErrTargetBusy（与参数错误、
// 非法状态等原因可区分）。
func (e *Engine) Create(cmdID string, p Plan) (*Plan, error) {
	if cmdID == "" {
		return nil, fmt.Errorf("%w: cmdID required", ErrInvalidArgument)
	}
	if p.ID == "" || p.Target == "" || p.ToVersion == "" {
		return nil, fmt.Errorf("%w: id/target/toVersion required", ErrInvalidArgument)
	}
	steps, err := BuildSteps(p.Strategy, p.TotalReplicas, p.BatchSizes)
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
	if busyID, busy := e.targetBusyLocked(p.Target); busy {
		return nil, fmt.Errorf("%w: target %s is locked by active plan %s", ErrTargetBusy, p.Target, busyID)
	}
	if len(e.plans) >= e.limits.MaxPlans {
		return nil, fmt.Errorf("%w: max plans %d", ErrLimitExceeded, e.limits.MaxPlans)
	}
	p.Steps = steps
	p.CurrentStep = -1 // 尚无阶段通过门控
	p.PendingStep = -1 // 无在途阶段
	p.PendingApplied = false
	p.GateSeq = 0
	p.State = StateRunning
	p.AppliedCmds = []string{cmdID}
	p.UpdatedAt = time.Now()
	e.plans[p.ID] = &p
	e.pmu[p.ID] = &sync.Mutex{}
	tl := e.tmu[p.Target]
	if tl == nil {
		tl = &sync.Mutex{}
		e.tmu[p.Target] = tl
	}
	e.notify[p.ID] = make(chan struct{}, 1)
	if err := e.persistLocked(); err != nil {
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

// Advance 推进到下一阶段：应用期望状态（有界重试）-> 等待多维门控收敛。
//
// 位点语义（canary/bluegreen/batch 三种策略共用同一套状态与恢复规则）：
//   - CurrentStep：最后一个“已通过门控”的阶段下标；
//   - PendingStep：已应用期望、正在等待门控的阶段下标（-1 无在途阶段）；
//   - GateSeq：在途阶段认可的最早观测序号（应用期望之后产生的新观测水位线）。
//
// 只有 PendingStep 无在途时才会真正应用下一批期望并放量，因此重复调用、
// ctx 超时重试、进程重启都不会重复放量或跳过阶段。
func (e *Engine) Advance(ctx context.Context, cmdID, id string) (*Plan, error) {
	p, notify, unlock, err := e.lockPlan(id)
	if err != nil {
		return nil, err
	}
	defer unlock()

	e.mu.Lock()
	if p.State.Terminal() {
		e.mu.Unlock()
		return nil, fmt.Errorf("%w: state=%s", ErrPlanFinished, p.State)
	}
	if p.State != StateRunning {
		e.mu.Unlock()
		return nil, fmt.Errorf("%w: %s -> Advance", ErrInvalidTransition, p.State)
	}

	// 确定本次要进入/继续的阶段。
	var next int
	switch {
	case p.PendingStep >= 0:
		next = p.PendingStep // 在途阶段（上次超时/崩溃）：只重新等待，绝不重新放量
	default:
		next = p.CurrentStep + 1
		if next >= len(p.Steps) {
			e.mu.Unlock()
			return nil, fmt.Errorf("%w: no more steps", ErrInvalidTransition)
		}
	}
	step := p.Steps[next]
	inFlight := p.PendingStep == next
	applied := inFlight && p.PendingApplied

	if !applied {
		// 需要实际下发的两种情况：
		//  1. 新阶段（无在途）：登记位点后下发；
		//  2. 在途但下发未确认（ctx 超时/崩溃/重启后重入，或 NewEngine 恢复时
		//     尚未补发成功）：幂等补发同一期望，绝不因为“在途”就跳过下发而卡死。
		// 两种情况复用同一 cmdID 去重规则。
		if err := checkCmd(p, cmdID); err != nil {
			e.mu.Unlock()
			return nil, err
		}
		if !inFlight {
			// 先登记在途位点与观测水位线并同步落盘：只有此后产生的观测才能让该阶段
			// 通过，前面阶段遗留观测不能复用，一次观测也不可能连过两个阶段；
			// 即使下一步崩溃，恢复后也知道有在途阶段而不会跳过阶段。
			// 此时 PendingApplied=false：崩溃发生在下发前/下发中时，重启会补发。
			p.PendingStep = next
			p.PendingApplied = false
			p.GateSeq = p.NextSeq
			p.UpdatedAt = time.Now()
			if err := e.persistLocked(); err != nil {
				e.mu.Unlock()
				return nil, err
			}
		}
		e.mu.Unlock()

		// 应用期望状态。Apply 幂等：重放/补发同一期望安全，不会对同一批重复放量；
		// 若崩溃时下发其实已生效，重复下发的也是完全相同的期望，目标实际状态不变。
		if err := e.applyWithRetry(p.Target, p.expectedVersion(step), step.Replicas, step.Percent); err != nil {
			e.mu.Lock()
			p.State = StateFailed
			p.PendingStep = -1
			p.PendingApplied = false
			p.UpdatedAt = time.Now()
			_ = e.persistLocked()
			e.mu.Unlock()
			return nil, err
		}
		// 下发成功后同步落盘“已确认”标记：此后重入/重启只等待门控、不再补发，
		// 保证不会重复放量、也不会多走阶段（位点仍指向同一个 next）。
		e.mu.Lock()
		p.PendingApplied = true
		p.UpdatedAt = time.Now()
		if err := e.persistLocked(); err != nil {
			// 落盘失败：保守落 Failed 并清在途（避免下次按未确认无限补发掩盖存储故障）。
			p.State = StateFailed
			p.PendingStep = -1
			p.PendingApplied = false
			p.UpdatedAt = time.Now()
			_ = e.persistLocked()
			e.mu.Unlock()
			return nil, err
		}
		e.mu.Unlock()
	} else {
		// 已确认下发的在途阶段（上次 ctx 超时/崩溃后重入）：副作用已应用，
		// 绝不重复 Apply；同一 cmdID 仍按重复命令拒绝，新 cmdID 用于继续等待。
		if err := checkCmd(p, cmdID); err != nil {
			e.mu.Unlock()
			return nil, err
		}
		e.mu.Unlock()
	}

	// 等待多维门控：版本、流量、副本承载三者同时收敛，且健康维度通过。
	for {
		e.mu.Lock()
		v := EvaluateStep(p, step, p.GateSeq, time.Now())
		switch {
		case v.Healthy:
			p.Verdicts = appendVerdict(p.Verdicts, v)
			appendCmd(p, cmdID, e.limits.MaxObservations)
			p.FailStreak = 0
			p.CurrentStep = next
			p.PendingStep = -1
			p.PendingApplied = false
			if next == len(p.Steps)-1 {
				p.State = StateSucceeded
			}
			p.UpdatedAt = time.Now()
			err := e.persistLocked()
			cp := *p
			e.mu.Unlock()
			if err != nil {
				return nil, err
			}
			return &cp, nil
		case consumesFailureBudget(v.Code):
			// 真实健康失败（指标缺失/样本不足/错误率超标）才计入失败预算。
			p.FailStreak++
			p.Verdicts = appendVerdict(p.Verdicts, v)
			if p.FailStreak >= p.Health.FailureBudget {
				// 失败预算耗尽：自动回滚到上一个稳定版本（同步落盘中间态与终态）。
				p.State = StateRollingBack
				p.UpdatedAt = time.Now()
				_ = e.persistLocked()
				e.mu.Unlock()
				rerr := e.applyWithRetry(p.Target, p.FromVersion, 0, 0)
				e.mu.Lock()
				if rerr != nil {
					p.State = StateFailed
				} else {
					p.State = StateRolledBack
				}
				p.PendingStep = -1
				p.PendingApplied = false
				p.UpdatedAt = time.Now()
				_ = e.persistLocked()
				e.mu.Unlock()
				return nil, fmt.Errorf("%w: %s (auto-rollback -> %s)", ErrHealthGate, v.Reason, p.State)
			}
			// 失败计数变化属恢复相关信息，立即落盘。
			p.UpdatedAt = time.Now()
			_ = e.persistLocked()
			e.mu.Unlock()
		default:
			// 维度尚未收敛（观测缺失/过期、版本/流量/承载未达期望）：
			// 实际状态延迟、抖动或短暂未上报时只能继续等待，不烧失败预算、
			// 不提前放行、不跳过阶段。依据入内存审计（封顶 verdictCap），
			// 等待位点已在进入在途时落盘，故这里不每次重估都落盘（防 IO 放大）。
			p.Verdicts = appendVerdict(p.Verdicts, v)
			p.UpdatedAt = time.Now()
			e.mu.Unlock()
		}

		select {
		case <-notify:
		case <-time.After(reevalInterval):
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
	p, _, unlock, err := e.lockPlan(id)
	if err != nil {
		return nil, err
	}
	defer unlock()
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
	appendCmd(p, cmdID, e.limits.MaxObservations)
	p.UpdatedAt = time.Now()
	if err := e.persistLocked(); err != nil {
		return nil, err
	}
	cp := *p
	return &cp, nil
}

// Rollback 手动回滚到上一个稳定版本（Running/Paused 可触发）。
func (e *Engine) Rollback(cmdID, id string) (*Plan, error) {
	p, _, unlock, err := e.lockPlan(id)
	if err != nil {
		return nil, err
	}
	defer unlock()

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
	_ = e.persistLocked()
	e.mu.Unlock()

	rerr := e.applyWithRetry(p.Target, p.FromVersion, 0, 0)

	e.mu.Lock()
	defer e.mu.Unlock()
	if rerr != nil {
		p.State = StateFailed
		p.UpdatedAt = time.Now()
		_ = e.persistLocked()
		return nil, rerr
	}
	p.State = StateRolledBack
	p.PendingStep = -1
	p.PendingApplied = false
	appendCmd(p, cmdID, e.limits.MaxObservations)
	p.UpdatedAt = time.Now()
	if err := e.persistLocked(); err != nil {
		return nil, err
	}
	cp := *p
	return &cp, nil
}

// Report 上报实际状态观测（按 ReportID 幂等去重，重复上报拒绝）。
// 只更新内存并由去抖协程合并落盘，因此高频上报不被磁盘 IO 阻塞。
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
	obs.Seq = p.NextSeq
	p.NextSeq++
	p.Observations = append(p.Observations, obs)
	if len(p.Observations) > e.limits.MaxObservations {
		p.Observations = p.Observations[len(p.Observations)-e.limits.MaxObservations:]
	}
	p.SeenReports = append(p.SeenReports, obs.ReportID)
	if len(p.SeenReports) > e.limits.MaxObservations {
		p.SeenReports = p.SeenReports[len(p.SeenReports)-e.limits.MaxObservations:]
	}
	p.UpdatedAt = time.Now()
	e.markDirtyLocked()
	// 非阻塞唤醒等待中的 Advance。
	select {
	case e.notify[id] <- struct{}{}:
	default:
	}
	return nil
}
