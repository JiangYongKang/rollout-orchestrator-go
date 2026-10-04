package rollout

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ============================================================
// 推进中断（进程被杀/崩溃）后重启的恢复语义测试。
//
// 崩溃窗口：Advance 先把在途位点（PendingStep/GateSeq）落盘，再下发期望状态。
// 进程可能死在“位点已落盘”之后的任意时点：
//   - 断在下发之前：期望状态从未到达目标环境；
//   - 断在下发过程中：目标环境已收到期望，但引擎不知道（结果未返回）；
//   - 断在下发之后：期望已生效，门控尚未通过。
// 重启后继续推进必须自动幂等补发这批期望并走到成功，表现与没中断过一致。
// ============================================================

// applyCall 记录一次期望下发的完整参数，用于断言补发幂等（同参数、不重复放量）。
type applyCall struct {
	target   string
	version  string
	replicas int
	percent  int
}

// crashApplier 包装 SimApplier：在第 crashAt 次（从 0 计）Apply 调用时模拟进程崩溃——
// 该次调用按 deliverOnCrash 决定是否先把期望真正下发到模拟环境，随后永久阻塞
// （等价于进程被杀：引擎不再有任何状态变更与落盘，测试直接弃用该引擎）。
// 测试收尾时关闭 release 解除阻塞，让残留的 Advance 协程随 ctx 取消退出。
type crashApplier struct {
	inner          *SimApplier
	mu             sync.Mutex
	calls          []applyCall
	crashAt        int  // -1 表示不注入崩溃，只记录调用
	deliverOnCrash bool // 崩溃这次是否先把期望下发到模拟环境
	entered        chan struct{}
	release        chan struct{}
}

func newCrashApplier(inner *SimApplier, crashAt int, deliverOnCrash bool) *crashApplier {
	return &crashApplier{
		inner:          inner,
		crashAt:        crashAt,
		deliverOnCrash: deliverOnCrash,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}
}

func (c *crashApplier) Apply(target, version string, replicas, percent int) error {
	c.mu.Lock()
	idx := len(c.calls)
	c.calls = append(c.calls, applyCall{target, version, replicas, percent})
	c.mu.Unlock()
	if idx == c.crashAt {
		if c.deliverOnCrash {
			if err := c.inner.Apply(target, version, replicas, percent); err != nil {
				return err
			}
		}
		close(c.entered)
		<-c.release // 进程“死亡”：本次调用永不返回
		return nil
	}
	return c.inner.Apply(target, version, replicas, percent)
}

func (c *crashApplier) applyCalls() []applyCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]applyCall, len(c.calls))
	copy(out, c.calls)
	return out
}

// logApplyCalls 打印期望下发流水（判定补发幂等性的直接依据）。
func logApplyCalls(t *testing.T, calls []applyCall) {
	t.Helper()
	for i, c := range calls {
		t.Logf("apply#%d target=%s version=%s replicas=%d percent=%d", i, c.target, c.version, c.replicas, c.percent)
	}
}

// firstN 取前 n 次下发记录；不足时直接判失败（说明补发/放量次数不符预期）。
func firstN(t *testing.T, calls []applyCall, n int) []applyCall {
	t.Helper()
	if len(calls) < n {
		t.Fatalf("expected at least %d apply calls, got %d", n, len(calls))
	}
	return calls[:n]
}

// assertSameExpectation 断言一组 Apply 调用携带完全相同的期望状态
// （补发必须幂等：同一批期望重复下发，不得变成另一批放量）。
func assertSameExpectation(t *testing.T, calls []applyCall, why string) {
	t.Helper()
	if len(calls) == 0 {
		t.Fatalf("%s: no apply calls recorded", why)
	}
	first := calls[0]
	for i, c := range calls[1:] {
		if c != first {
			t.Fatalf("%s: apply#%d %+v differs from apply#0 %+v (re-issue must be idempotent)", why, i+1, c, first)
		}
	}
	t.Logf("%s: %d 次下发参数完全一致（幂等补发，未重复放量）", why, len(calls))
}

// simDesired 读取模拟环境当前的期望状态（同包测试可直接观测）。
func simDesired(sim *SimApplier, target string) (version string, replicas, percent int, ok bool) {
	sim.mu.Lock()
	defer sim.mu.Unlock()
	tg := sim.targets[target]
	if tg == nil {
		return "", 0, 0, false
	}
	return tg.version, tg.desired, tg.desiredPct, true
}

// simActual 读取模拟环境当前实际承载的新版本副本数。
func simActual(sim *SimApplier, target string) int {
	sim.mu.Lock()
	defer sim.mu.Unlock()
	if tg := sim.targets[target]; tg != nil {
		return tg.actual
	}
	return 0
}

// reopenEngine 模拟进程重启：同一 store 文件 + 同一模拟环境，全新引擎实例。
// 不 Close 旧引擎（崩溃没有优雅退出）；旧引擎在崩溃点之后不会再有任何落盘。
func reopenEngine(t *testing.T, dir string, applier Applier) *Engine {
	t.Helper()
	e, err := NewEngine(NewFileStore(filepath.Join(dir, "store.json")), applier, testLimits())
	if err != nil {
		t.Fatalf("reopen NewEngine: %v", err)
	}
	return e
}

// advanceToStep 推进一次并断言恰好落在 wantStep（不跳阶段），打印状态迁移与判定依据。
func advanceToStep(t *testing.T, e *Engine, cmdID, planID string, wantStep int) *Plan {
	t.Helper()
	before := mustGet(t, e, planID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	after, err := e.Advance(ctx, cmdID, planID)
	cancel()
	logTransition(t, fmt.Sprintf("Advance(cmd=%s)", cmdID), before, after, err)
	if err != nil {
		t.Fatalf("Advance(%s): %v", cmdID, err)
	}
	if after.CurrentStep != wantStep {
		t.Fatalf("阶段跳跃：want CurrentStep=%d, got %d", wantStep, after.CurrentStep)
	}
	return after
}

// crashMidAdvance 让 e1 在推进第一批时“进程崩溃”：Advance 协程阻塞在崩溃点，
// 返回的 cancel 用于测试收尾时放行残留协程。调用方保证之后不再使用 e1。
func crashMidAdvance(t *testing.T, e1 *Engine, cap *crashApplier, planID string) (cleanup func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := e1.Advance(ctx, "adv-0", planID); done <- err }()
	<-cap.entered // 崩溃点：在途位点已落盘，本次 Apply 永不返回
	t.Logf("=== 模拟进程崩溃：Advance 已将在途位点落盘，进程死在 Apply 内（deliver=%v）===", cap.deliverOnCrash)
	return func() {
		cancel()
		close(cap.release)
		<-done
	}
}

// TestCrashBeforeApplyRecovery 断在下发之前：位点已落盘，期望从未到达目标环境。
// 重启后继续推进必须自动补发这批期望并一路走到成功。
func TestCrashBeforeApplyRecovery(t *testing.T) {
	dir := t.TempDir()
	sim := NewSimApplier()
	sim.StepReplicas = 1
	cap := newCrashApplier(sim, 0, false) // 第 0 次 Apply：不下发，直接“死”
	e1 := reopenEngine(t, dir, cap)

	p, err := e1.Create("c1", newPlan("p1", StrategyCanary))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Logf("输入: strategy=canary steps=%+v", p.Steps)
	cleanup := crashMidAdvance(t, e1, cap, p.ID)
	defer cleanup()

	// 崩溃时点核验：模拟环境没有收到任何期望状态。
	if v, r, pct, ok := simDesired(sim, p.Target); ok {
		t.Fatalf("断在下发之前：目标环境不应有期望状态，got version=%q replicas=%d percent=%d", v, r, pct)
	}
	t.Logf("崩溃时点：目标环境无任何期望状态（下发未发生）")

	// === 重启 ===
	e2 := reopenEngine(t, dir, cap)
	defer e2.Close()
	got := mustGet(t, e2, p.ID)
	t.Logf("重启后恢复: state=%s currentStep=%d pendingStep=%d gateSeq=%d", got.State, got.CurrentStep, got.PendingStep, got.GateSeq)
	if got.State != StateRunning || got.PendingStep != 0 || got.CurrentStep != -1 {
		t.Fatalf("恢复位点不符: state=%s current=%d pending=%d", got.State, got.CurrentStep, got.PendingStep)
	}

	// 重启后继续推进：应自动补发第 0 批期望并通过门控。
	stop := startReporter(t, e2, sim, p.Target, p.ID)
	defer stop()
	advanceToStep(t, e2, "adv-0", p.ID, 0)
	if v, r, pct, ok := simDesired(sim, p.Target); !ok || v != "v2" || r != p.Steps[0].Replicas || pct != p.Steps[0].Percent {
		t.Fatalf("补发后目标环境期望状态不符: version=%q replicas=%d percent=%d ok=%v", v, r, pct, ok)
	}
	t.Logf("补发生效: 目标环境期望 version=v2 replicas=%d percent=%d", p.Steps[0].Replicas, p.Steps[0].Percent)

	// 幂等核验：崩溃那次（未下发）+ 重启补发，参数必须完全一致。
	calls := cap.applyCalls()
	logApplyCalls(t, calls)
	assertSameExpectation(t, firstN(t, calls, 2), "阶段0补发")

	// 继续推进剩余阶段，一路到成功、不跳阶段。
	for i := 1; i < len(p.Steps); i++ {
		advanceToStep(t, e2, fmt.Sprintf("adv-%d", i), p.ID, i)
	}
	final := mustGet(t, e2, p.ID)
	if final.State != StateSucceeded {
		t.Fatalf("want Succeeded, got %s", final.State)
	}
	t.Logf("结果: 中断恢复后一路推进到 %s（与未中断表现一致）", final.State)
}

// TestCrashDuringApplyRecovery 断在下发过程中：期望已到达目标环境，但结果未返回引擎。
// 重启后补发必须幂等：同一批期望再下一次，目标实际状态不被改乱、不重复放量、不跳阶段。
func TestCrashDuringApplyRecovery(t *testing.T) {
	dir := t.TempDir()
	sim := NewSimApplier()
	sim.StepReplicas = 1
	cap := newCrashApplier(sim, 0, true) // 第 0 次 Apply：先下发成功，再“死”
	e1 := reopenEngine(t, dir, cap)

	p, err := e1.Create("c1", newPlan("p1", StrategyCanary))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Logf("输入: strategy=canary steps=%+v", p.Steps)
	cleanup := crashMidAdvance(t, e1, cap, p.ID)
	defer cleanup()

	// 崩溃时点核验：期望已生效，且实际承载已开始收敛。
	if v, r, pct, ok := simDesired(sim, p.Target); !ok || v != "v2" || r != p.Steps[0].Replicas || pct != p.Steps[0].Percent {
		t.Fatalf("断在下发过程中：目标环境应已收到期望，got version=%q replicas=%d percent=%d ok=%v", v, r, pct, ok)
	}
	if _, err := sim.Observe(p.Target); err != nil { // 收敛一步：actual=1（阶段0期望副本数）
		t.Fatal(err)
	}
	beforeActual := simActual(sim, p.Target)
	t.Logf("崩溃时点：期望已生效（replicas=%d percent=%d），实际承载已收敛到 %d", p.Steps[0].Replicas, p.Steps[0].Percent, beforeActual)

	// === 重启 ===
	e2 := reopenEngine(t, dir, cap)
	defer e2.Close()
	got := mustGet(t, e2, p.ID)
	t.Logf("重启后恢复: state=%s currentStep=%d pendingStep=%d", got.State, got.CurrentStep, got.PendingStep)
	if got.State != StateRunning || got.PendingStep != 0 {
		t.Fatalf("恢复位点不符: state=%s pending=%d", got.State, got.PendingStep)
	}

	stop := startReporter(t, e2, sim, p.Target, p.ID)
	defer stop()
	advanceToStep(t, e2, "adv-0", p.ID, 0)

	// 幂等核验：崩溃那次（已生效）+ 重启补发，参数完全一致；
	// 且补发没有重置/改乱目标实际承载。
	calls := cap.applyCalls()
	logApplyCalls(t, calls)
	assertSameExpectation(t, firstN(t, calls, 2), "阶段0补发")
	if got := simActual(sim, p.Target); got < beforeActual {
		t.Fatalf("补发把目标实际状态改乱：actual %d -> %d", beforeActual, got)
	}
	t.Logf("补发后实际承载 actual=%d（未被重置，从 %d 继续）", simActual(sim, p.Target), beforeActual)

	for i := 1; i < len(p.Steps); i++ {
		advanceToStep(t, e2, fmt.Sprintf("adv-%d", i), p.ID, i)
	}
	if final := mustGet(t, e2, p.ID); final.State != StateSucceeded {
		t.Fatalf("want Succeeded, got %s", final.State)
	}
	t.Logf("结果: 下发过程中断恢复后推进到 Succeeded")
}

// TestCrashAfterApplyBeforeGate 断在下发之后：期望已生效、门控尚未通过时进程崩溃。
// 重启后重入在途阶段：幂等补发同一批期望，等门控通过后继续，不跳阶段。
func TestCrashAfterApplyBeforeGate(t *testing.T) {
	dir := t.TempDir()
	sim := NewSimApplier()
	sim.StepReplicas = 1
	cap := newCrashApplier(sim, -1, false) // 不注入崩溃，只记录下发流水
	e1 := reopenEngine(t, dir, cap)

	p, err := e1.Create("c1", newPlan("p1", StrategyCanary))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// 无上报源：下发后等门控，ctx 超时 -> 在途位点已落盘、期望已生效。
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, err = e1.Advance(ctx, "adv-0", p.ID)
	cancel()
	if !errors.Is(err, ErrNotConverged) {
		t.Fatalf("want ErrNotConverged, got %v", err)
	}
	t.Logf("崩溃前: 期望已下发（apply 1 次），门控未通过，Advance 超时返回 %v", err)
	// 不 Close，直接弃用 e1 == 进程崩溃（位点与状态此前已同步落盘）。

	// === 重启 ===
	e2 := reopenEngine(t, dir, cap)
	defer e2.Close()
	got := mustGet(t, e2, p.ID)
	t.Logf("重启后恢复: state=%s currentStep=%d pendingStep=%d", got.State, got.CurrentStep, got.PendingStep)
	if got.State != StateRunning || got.PendingStep != 0 || got.CurrentStep != -1 {
		t.Fatalf("恢复位点不符: state=%s current=%d pending=%d", got.State, got.CurrentStep, got.PendingStep)
	}

	stop := startReporter(t, e2, sim, p.Target, p.ID)
	defer stop()
	advanceToStep(t, e2, "adv-0", p.ID, 0)

	// 幂等核验：重启补发与崩溃前那次参数完全一致，总共 2 次、同一批期望。
	calls := cap.applyCalls()
	logApplyCalls(t, calls)
	assertSameExpectation(t, firstN(t, calls, 2), "阶段0补发")

	for i := 1; i < len(p.Steps); i++ {
		advanceToStep(t, e2, fmt.Sprintf("adv-%d", i), p.ID, i)
	}
	if final := mustGet(t, e2, p.ID); final.State != StateSucceeded {
		t.Fatalf("want Succeeded, got %s", final.State)
	}
	t.Logf("结果: 下发后中断恢复，推进到 Succeeded")
}

// TestTimeoutReentryNoReapply 同一进程内 ctx 超时重入在途阶段：
// 期望上次已下发成功，重入只继续等门控，不得重复 Apply（不重复放量）。
func TestTimeoutReentryNoReapply(t *testing.T) {
	dir := t.TempDir()
	sim := NewSimApplier()
	sim.StepReplicas = 1
	cap := newCrashApplier(sim, -1, false)
	e := reopenEngine(t, dir, cap)
	defer e.Close()

	p, err := e.Create("c1", newPlan("p1", StrategyCanary))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// 第一次推进：下发后无观测，ctx 超时。
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, err = e.Advance(ctx, "adv-0", p.ID)
	cancel()
	if !errors.Is(err, ErrNotConverged) {
		t.Fatalf("want ErrNotConverged, got %v", err)
	}
	n := len(cap.applyCalls())
	t.Logf("首次推进超时: apply 调用 %d 次，pendingStep=%d", n, mustGet(t, e, p.ID).PendingStep)

	// 同进程重入（同一 cmdID 允许继续等待）：这次有上报源，应直接等门控通过。
	stop := startReporter(t, e, sim, p.Target, p.ID)
	defer stop()
	advanceToStep(t, e, "adv-0", p.ID, 0)

	calls := cap.applyCalls()
	logApplyCalls(t, calls)
	if len(calls) != n {
		t.Fatalf("同进程超时重入重复放量：apply %d -> %d 次", n, len(calls))
	}
	t.Logf("结果: 同进程重入未重复 Apply（仍 %d 次），阶段 0 通过", len(calls))
}

// TestCrashRecoveryWithPauseResume 中断与暂停/恢复穿插：
// 在途阶段暂停后崩溃，重启后仍是 Paused（推进被拒、原因可区分），
// Resume 后继续推进自动补发期望并走到成功。
func TestCrashRecoveryWithPauseResume(t *testing.T) {
	dir := t.TempDir()
	sim := NewSimApplier()
	sim.StepReplicas = 1
	cap := newCrashApplier(sim, -1, false)
	e1 := reopenEngine(t, dir, cap)

	p, err := e1.Create("c1", newPlan("p1", StrategyCanary))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// 推进第一批：下发成功、门控未过（无上报），ctx 超时使在途位点落盘。
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, err = e1.Advance(ctx, "adv-0", p.ID)
	cancel()
	if !errors.Is(err, ErrNotConverged) {
		t.Fatalf("want ErrNotConverged, got %v", err)
	}
	if _, err := e1.Pause("pause-1", p.ID); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	t.Logf("崩溃前: 在途阶段 pendingStep=0 且已暂停")
	// 弃用 e1 == 崩溃。

	// === 重启 ===
	e2 := reopenEngine(t, dir, cap)
	defer e2.Close()
	got := mustGet(t, e2, p.ID)
	t.Logf("重启后恢复: state=%s currentStep=%d pendingStep=%d", got.State, got.CurrentStep, got.PendingStep)
	if got.State != StatePaused || got.PendingStep != 0 {
		t.Fatalf("恢复位点不符: state=%s pending=%d", got.State, got.PendingStep)
	}

	// 暂停中推进：非法迁移，与恢复语义可区分。
	if _, err := e2.Advance(context.Background(), "adv-x", p.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("advance while paused: want ErrInvalidTransition, got %v", err)
	}
	t.Logf("暂停中推进被拒: ErrInvalidTransition（可区分）")

	// 恢复后推进：自动补发在途批次，继续到成功。
	if _, err := e2.Resume("resume-1", p.ID); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	stop := startReporter(t, e2, sim, p.Target, p.ID)
	defer stop()
	for i := 0; i < len(p.Steps); i++ {
		advanceToStep(t, e2, fmt.Sprintf("adv-%d", i), p.ID, i)
	}
	calls := cap.applyCalls()
	logApplyCalls(t, calls)
	assertSameExpectation(t, firstN(t, calls, 2), "阶段0补发")
	if final := mustGet(t, e2, p.ID); final.State != StateSucceeded {
		t.Fatalf("want Succeeded, got %s", final.State)
	}
	t.Logf("结果: 暂停+中断恢复后推进到 Succeeded")
}

// TestCrashRecoveryWithRollback 中断后选择回滚：
// 在途阶段崩溃，重启后回滚落定 RolledBack（回滚幂等），目标随之释放；
// 终态操作被拒且原因可区分，同目标可新建发布。
func TestCrashRecoveryWithRollback(t *testing.T) {
	dir := t.TempDir()
	sim := NewSimApplier()
	sim.StepReplicas = 1
	cap := newCrashApplier(sim, -1, false)
	e1 := reopenEngine(t, dir, cap)

	p, err := e1.Create("c1", newPlan("p1", StrategyCanary))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, err = e1.Advance(ctx, "adv-0", p.ID)
	cancel()
	if !errors.Is(err, ErrNotConverged) {
		t.Fatalf("want ErrNotConverged, got %v", err)
	}
	t.Logf("崩溃前: 在途阶段 pendingStep=0，期望已下发")
	// 弃用 e1 == 崩溃。

	// === 重启 ===
	e2 := reopenEngine(t, dir, cap)
	defer e2.Close()
	got := mustGet(t, e2, p.ID)
	if got.State != StateRunning || got.PendingStep != 0 {
		t.Fatalf("恢复位点不符: state=%s pending=%d", got.State, got.PendingStep)
	}

	before := mustGet(t, e2, p.ID)
	rb, err := e2.Rollback("rb-1", p.ID)
	logTransition(t, "Rollback", before, rb, err)
	if err != nil {
		t.Fatalf("Rollback after restart: %v", err)
	}
	if rb.State != StateRolledBack {
		t.Fatalf("want RolledBack, got %s", rb.State)
	}
	// 目标环境已切回稳定版本（0 个新版本副本）。
	if v, r, _, ok := simDesired(sim, p.Target); !ok || r != 0 {
		t.Fatalf("回滚后目标期望应为 0 副本: version=%q replicas=%d ok=%v", v, r, ok)
	}
	t.Logf("回滚落定: 目标期望副本=0（回到稳定版本 %s）", p.FromVersion)

	// 终态操作：ErrPlanFinished，可区分。
	if _, err := e2.Advance(context.Background(), "adv-9", p.ID); !errors.Is(err, ErrPlanFinished) {
		t.Fatalf("advance on rolled-back plan: want ErrPlanFinished, got %v", err)
	}
	// 目标已释放：同目标可新建发布（互斥约束随终态解除）。
	p2 := newPlan("p2", StrategyCanary)
	p2.Target = p.Target
	if _, err := e2.Create("c2", p2); err != nil {
		t.Fatalf("create on released target: %v", err)
	}
	t.Logf("结果: 回滚后目标释放，新发布创建成功")
}

// TestCrashRecoveryErrorDistinction 中断恢复后的结果与各类拒绝原因可区分：
// 目标被占用（ErrTargetBusy）、参数不合法（ErrInvalidArgument）、
// 非法状态（ErrInvalidTransition）各自独立，且都不影响恢复推进。
func TestCrashRecoveryErrorDistinction(t *testing.T) {
	dir := t.TempDir()
	sim := NewSimApplier()
	sim.StepReplicas = 1
	cap := newCrashApplier(sim, -1, false)
	e1 := reopenEngine(t, dir, cap)

	p, err := e1.Create("c1", newPlan("p1", StrategyCanary))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, _ = e1.Advance(ctx, "adv-0", p.ID) // 在途位点落盘后超时
	cancel()
	// 弃用 e1 == 崩溃。

	e2 := reopenEngine(t, dir, cap)
	defer e2.Close()

	// 目标已被进行中发布占用：ErrTargetBusy。
	busy := newPlan("p2", StrategyCanary)
	busy.Target = p.Target
	if _, err := e2.Create("c2", busy); !errors.Is(err, ErrTargetBusy) {
		t.Fatalf("create on busy target: want ErrTargetBusy, got %v", err)
	}
	t.Logf("同目标新建被拒: ErrTargetBusy（与恢复推进可区分）")

	// 参数不合法：ErrInvalidArgument。
	bad := newPlan("p3", "random")
	if _, err := e2.Create("c3", bad); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("create with bad strategy: want ErrInvalidArgument, got %v", err)
	}
	if _, err := e2.Advance(context.Background(), "", p.ID); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("advance with empty cmdID: want ErrInvalidArgument, got %v", err)
	}
	t.Logf("非法参数被拒: ErrInvalidArgument（可区分）")

	// 非法状态：对不存在的计划推进 -> ErrNotFound；重复命令 -> ErrDuplicateCommand。
	if _, err := e2.Advance(context.Background(), "x", "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("advance missing plan: want ErrNotFound, got %v", err)
	}
	t.Logf("计划不存在: ErrNotFound（可区分）")

	// 恢复推进不受上述拒绝影响：补发后一路到成功。
	stop := startReporter(t, e2, sim, p.Target, p.ID)
	defer stop()
	for i := 0; i < len(p.Steps); i++ {
		advanceToStep(t, e2, fmt.Sprintf("adv-%d", i), p.ID, i)
	}
	if final := mustGet(t, e2, p.ID); final.State != StateSucceeded {
		t.Fatalf("want Succeeded, got %s", final.State)
	}
	// 原计划到终态后目标释放，此前被拒的 p2 现在可以创建。
	if _, err := e2.Create("c2b", busy); err != nil {
		t.Fatalf("create after predecessor finished: %v", err)
	}
	t.Logf("结果: 各类拒绝原因可区分，恢复推进到 Succeeded，终态后目标释放")
}
