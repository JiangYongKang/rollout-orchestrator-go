package rollout

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testLimits() Limits {
	return Limits{MaxPlans: 10, MaxRetries: 2, MaxObservations: 50}
}

func testHealth() HealthPolicy {
	return HealthPolicy{
		MaxErrorRate:      0.05,
		FailureBudget:     3,
		ObservationMaxAge: time.Minute,
		MinSamples:        10,
	}
}

func newPlan(id string, kind StrategyKind) Plan {
	return Plan{
		ID: id, Target: "svc-" + id, Strategy: kind,
		FromVersion: "v1", ToVersion: "v2",
		TotalReplicas: 10, Health: testHealth(),
	}
}

func newEngine(t *testing.T, dir string) (*Engine, *SimApplier) {
	t.Helper()
	sim := NewSimApplier()
	sim.StepReplicas = 4 // 10 副本需 3 次观测收敛，覆盖“延迟收敛”路径
	e, err := NewEngine(NewFileStore(filepath.Join(dir, "store.json")), sim, testLimits())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return e, sim
}

// reporterInstance 区分同一计划在“重启”前后的上报器，避免 ReportID 撞已落盘的旧 ID。
var reporterInstance int64

// startReporter 模拟外部监控周期性上报实际状态。
func startReporter(t *testing.T, e *Engine, sim *SimApplier, target, planID string) (stop func()) {
	t.Helper()
	inst := atomic.AddInt64(&reporterInstance, 1)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
			}
			i++
			obs, err := sim.Observe(target)
			if err != nil {
				continue // 模拟短暂缺失
			}
			obs.ReportID = fmt.Sprintf("%s-i%d-r%d", planID, inst, i)
			if err := e.Report(planID, obs); err != nil &&
				!errors.Is(err, ErrPlanFinished) && !errors.Is(err, ErrNotFound) {
				t.Logf("report %s -> %v", obs.ReportID, err)
			}
		}
	}()
	return func() { close(done); wg.Wait() }
}

func logTransition(t *testing.T, op string, before, after *Plan, err error) {
	t.Helper()
	bs, as := "-", "-"
	if before != nil {
		bs = fmt.Sprintf("%s(step=%d)", before.State, before.CurrentStep)
	}
	if after != nil {
		as = fmt.Sprintf("%s(step=%d)", after.State, after.CurrentStep)
	}
	t.Logf("op=%s state %s -> %s err=%v", op, bs, as, err)
	if after != nil {
		for _, v := range after.Verdicts {
			t.Logf("  verdict healthy=%v code=%s reason=%q", v.Healthy, v.Code, v.Reason)
		}
	}
}

func mustGet(t *testing.T, e *Engine, id string) *Plan {
	t.Helper()
	p, err := e.Get(id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	return p
}

// 正常链路：金丝雀三阶段推进至成功，验证收敛等待与判定依据。
func TestCanaryHappyPath(t *testing.T) {
	dir := t.TempDir()
	e, sim := newEngine(t, dir)
	defer e.Close()

	p, err := e.Create("cmd-create", newPlan("p1", StrategyCanary))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Logf("created: steps=%+v", p.Steps)
	stop := startReporter(t, e, sim, p.Target, p.ID)
	defer stop()

	for i := 0; i < len(p.Steps); i++ {
		before := mustGet(t, e, p.ID)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		after, err := e.Advance(ctx, fmt.Sprintf("cmd-adv-%d", i), p.ID)
		cancel()
		logTransition(t, "Advance", before, after, err)
		if err != nil {
			t.Fatalf("Advance step %d: %v", i, err)
		}
	}
	final := mustGet(t, e, p.ID)
	if final.State != StateSucceeded {
		t.Fatalf("want Succeeded, got %s", final.State)
	}
	if len(final.Verdicts) == 0 {
		t.Fatal("expected verdict audit trail")
	}
}

// 非法状态迁移：暂停中推进、重复暂停、终态后任何操作。
func TestIllegalTransitions(t *testing.T) {
	dir := t.TempDir()
	e, _ := newEngine(t, dir)
	defer e.Close()
	p, _ := e.Create("c1", newPlan("p1", StrategyCanary))

	ctx0, cancel0 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err0 := e.Advance(ctx0, "c2", p.ID)
	cancel0()
	if !errors.Is(err0, ErrNotConverged) {
		// 无观测时推进应阻塞直至 ctx 超时 -> NotConverged
		t.Fatalf("advance without observations: want ErrNotConverged, got %v", err0)
	}
	t.Logf("advance without observations err=%v", err0)
	if _, err := e.Pause("c3", p.ID); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	cases := []struct {
		name string
		fn   func() error
		want error
	}{
		{"advance while paused", func() error { _, err := e.Advance(context.Background(), "c4", p.ID); return err }, ErrInvalidTransition},
		{"pause while paused", func() error { _, err := e.Pause("c5", p.ID); return err }, ErrInvalidTransition},
	}
	for _, c := range cases {
		err := c.fn()
		t.Logf("input=%s err=%v", c.name, err)
		if !errors.Is(err, c.want) {
			t.Fatalf("%s: want %v, got %v", c.name, c.want, err)
		}
	}
	// 暂停后恢复是合法迁移。
	if _, err := e.Resume("c6", p.ID); err != nil {
		t.Fatalf("Resume from paused: %v", err)
	}
	// 恢复后再次恢复 -> 非法迁移。
	if _, err := e.Resume("c6b", p.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("resume while running: want ErrInvalidTransition, got %v", err)
	}
	// 再次暂停以便后续回滚。
	if _, err := e.Pause("c6c", p.ID); err != nil {
		t.Fatalf("Pause again: %v", err)
	}
	// 回滚到终态后，所有操作必须被拒绝且可区分。
	if _, err := e.Rollback("c7", p.ID); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	term := mustGet(t, e, p.ID)
	if term.State != StateRolledBack {
		t.Fatalf("want RolledBack, got %s", term.State)
	}
	terminalOps := []struct {
		name string
		fn   func() error
	}{
		{"advance", func() error { _, err := e.Advance(context.Background(), "c8", p.ID); return err }},
		{"pause", func() error { _, err := e.Pause("c9", p.ID); return err }},
		{"resume", func() error { _, err := e.Resume("c10", p.ID); return err }},
		{"rollback", func() error { _, err := e.Rollback("c11", p.ID); return err }},
		{"report", func() error {
			return e.Report(p.ID, Observation{ReportID: "x", ReportedAt: time.Now(), HasMetrics: true, Samples: 100})
		}},
	}
	for _, op := range terminalOps {
		err := op.fn()
		t.Logf("terminal op=%s err=%v", op.name, err)
		if !errors.Is(err, ErrPlanFinished) {
			t.Fatalf("%s on terminal plan: want ErrPlanFinished, got %v", op.name, err)
		}
	}
}

// 重复命令与重复上报必须被幂等拒绝。
func TestDuplicateCommandAndReport(t *testing.T) {
	dir := t.TempDir()
	e, sim := newEngine(t, dir)
	defer e.Close()
	p, _ := e.Create("c1", newPlan("p1", StrategyCanary))
	stop := startReporter(t, e, sim, p.Target, p.ID)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := e.Advance(ctx, "adv-1", p.ID); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	// 同一 cmdID 重复推进 -> 拒绝，且不得重复放量。
	if _, err := e.Advance(context.Background(), "adv-1", p.ID); !errors.Is(err, ErrDuplicateCommand) {
		t.Fatalf("want ErrDuplicateCommand, got %v", err)
	}
	got := mustGet(t, e, p.ID)
	if got.CurrentStep != 0 {
		t.Fatalf("duplicate advance caused extra rollout: step=%d", got.CurrentStep)
	}
	t.Logf("duplicate command rejected, step stays %d", got.CurrentStep)

	// 同一 ReportID 重复上报 -> 拒绝，观测数不增加。
	obs := Observation{ReportID: "dup-1", ReportedAt: time.Now(), Replicas: 1, HasMetrics: true, Samples: 100}
	if err := e.Report(p.ID, obs); err != nil {
		t.Fatalf("Report: %v", err)
	}
	n := len(mustGet(t, e, p.ID).Observations)
	if err := e.Report(p.ID, obs); !errors.Is(err, ErrDuplicateReport) {
		t.Fatalf("want ErrDuplicateReport, got %v", err)
	}
	if got := len(mustGet(t, e, p.ID).Observations); got != n {
		t.Fatalf("duplicate report changed observations: %d -> %d", n, got)
	}
	t.Logf("duplicate report rejected, observations=%d unchanged", n)
}

// 观测过期：不得推进，保持 Running。
func TestStaleObservationBlocksAdvance(t *testing.T) {
	dir := t.TempDir()
	sim := NewSimApplier()
	sim.StepReplicas = 0
	e, err := NewEngine(NewFileStore(filepath.Join(dir, "s.json")), sim, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	pl := newPlan("p1", StrategyCanary)
	pl.Health.ObservationMaxAge = 30 * time.Millisecond
	if _, err := e.Create("c1", pl); err != nil {
		t.Fatal(err)
	}
	// 上报一次后静默，等待观测过期。
	if err := e.Report(pl.ID, Observation{ReportID: "r1", ReportedAt: time.Now(), Replicas: 1, HasMetrics: true, Samples: 100}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err = e.Advance(ctx, "adv-1", pl.ID)
	t.Logf("advance with stale observation err=%v", err)
	if !errors.Is(err, ErrNotConverged) {
		t.Fatalf("want ErrNotConverged, got %v", err)
	}
	if got := mustGet(t, e, pl.ID); got.State != StateRunning {
		t.Fatalf("stale observation must not advance: state=%s step=%d", got.State, got.CurrentStep)
	}
}

// 健康门控：错误率超预算 -> 自动回滚到上一稳定版本，保留判定依据。
func TestHealthGateAutoRollback(t *testing.T) {
	dir := t.TempDir()
	e, sim := newEngine(t, dir)
	defer e.Close()
	p, _ := e.Create("c1", newPlan("p1", StrategyCanary))
	sim.ErrorRate = 0.5 // 远超阈值 0.05
	stop := startReporter(t, e, sim, p.Target, p.ID)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := e.Advance(ctx, "adv-1", p.ID)
	t.Logf("advance with unhealthy metrics err=%v", err)
	if !errors.Is(err, ErrHealthGate) {
		t.Fatalf("want ErrHealthGate, got %v", err)
	}
	got := mustGet(t, e, p.ID)
	logTransition(t, "auto-rollback", nil, got, err)
	if got.State != StateRolledBack {
		t.Fatalf("want RolledBack, got %s", got.State)
	}
	if len(got.Verdicts) == 0 {
		t.Fatal("expected recorded verdicts explaining the rollback")
	}
	for _, v := range got.Verdicts {
		if v.Healthy {
			t.Fatalf("unexpected healthy verdict: %+v", v)
		}
	}
	// 模拟环境应已切回旧版本（0 个新版本副本）。
	obs, oerr := sim.Observe(p.Target)
	if oerr != nil {
		t.Fatal(oerr)
	}
	if obs.Replicas != 0 {
		t.Fatalf("rollback did not restore stable version: replicas=%d", obs.Replicas)
	}
}

// 关键指标缺失：计入失败预算，不得推进。
func TestMissingMetricBlocks(t *testing.T) {
	dir := t.TempDir()
	e, sim := newEngine(t, dir)
	defer e.Close()
	p, _ := e.Create("c1", newPlan("p1", StrategyCanary))
	sim.DropMetrics = true
	stop := startReporter(t, e, sim, p.Target, p.ID)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := e.Advance(ctx, "adv-1", p.ID)
	t.Logf("advance with missing metrics err=%v", err)
	if !errors.Is(err, ErrHealthGate) {
		t.Fatalf("want ErrHealthGate, got %v", err)
	}
	got := mustGet(t, e, p.ID)
	found := false
	for _, v := range got.Verdicts {
		t.Logf("verdict code=%s reason=%q", v.Code, v.Reason)
		if v.Code == VerdictMissingMetric {
			found = true
		}
	}
	if !found {
		t.Fatal("expected missing-metric verdict")
	}
}

// 应用副作用重试有界：超过上限进入 Failed 终态。
func TestRetryExhausted(t *testing.T) {
	dir := t.TempDir()
	e, sim := newEngine(t, dir)
	defer e.Close()
	sim.Failures = 10 // 超过 MaxRetries=2
	p, _ := e.Create("c1", newPlan("p1", StrategyCanary))
	_, err := e.Advance(context.Background(), "adv-1", p.ID)
	t.Logf("advance with persistent apply failure err=%v", err)
	if !errors.Is(err, ErrRetryExhausted) {
		t.Fatalf("want ErrRetryExhausted, got %v", err)
	}
	if got := mustGet(t, e, p.ID); got.State != StateFailed {
		t.Fatalf("want Failed, got %s", got.State)
	}
}

// 瞬时失败在重试预算内应成功（不进入 Failed）。
func TestRetryWithinBudget(t *testing.T) {
	dir := t.TempDir()
	e, sim := newEngine(t, dir)
	defer e.Close()
	sim.Failures = 2 // 恰好等于 MaxRetries
	p, _ := e.Create("c1", newPlan("p1", StrategyCanary))
	stop := startReporter(t, e, sim, p.Target, p.ID)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := e.Advance(ctx, "adv-1", p.ID); err != nil {
		t.Fatalf("Advance with transient failures: %v", err)
	}
	if got := mustGet(t, e, p.ID); got.CurrentStep != 0 {
		t.Fatalf("want step 0, got %d", got.CurrentStep)
	}
}

// 断点恢复：推进到一半后“重启”，从一致位点继续，命令幂等性跨重启保持。
func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	e1, sim := newEngine(t, dir)
	p, _ := e1.Create("c1", newPlan("p1", StrategyCanary))
	stop1 := startReporter(t, e1, sim, p.Target, p.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if _, err := e1.Advance(ctx, "adv-0", p.ID); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	cancel()
	stop1()
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("=== simulated crash & restart ===")

	// 重启：同一 store，同一 sim（模拟环境状态仍在）。
	e2, err := NewEngine(NewFileStore(filepath.Join(dir, "store.json")), sim, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	got := mustGet(t, e2, p.ID)
	t.Logf("recovered state=%s step=%d", got.State, got.CurrentStep)
	if got.State != StateRunning || got.CurrentStep != 0 {
		t.Fatalf("inconsistent recovery: state=%s step=%d", got.State, got.CurrentStep)
	}
	// 重启前已应用的命令不得重复执行。
	if _, err := e2.Advance(context.Background(), "adv-0", p.ID); !errors.Is(err, ErrDuplicateCommand) {
		t.Fatalf("want ErrDuplicateCommand after restart, got %v", err)
	}
	// 从断点继续推进至成功。
	stop2 := startReporter(t, e2, sim, p.Target, p.ID)
	defer stop2()
	for i := 1; i < len(got.Steps); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := e2.Advance(ctx, fmt.Sprintf("adv-%d", i), p.ID)
		cancel()
		if err != nil {
			t.Fatalf("Advance step %d after restart: %v", i, err)
		}
	}
	if got := mustGet(t, e2, p.ID); got.State != StateSucceeded {
		t.Fatalf("want Succeeded, got %s", got.State)
	}
}

// 旧版本/损坏数据必须被明确拒绝，不得按默认值静默解释。
func TestStoreVersionAndCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")
	if err := os.WriteFile(path, []byte(`{"version":99,"plans":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := NewEngine(NewFileStore(path), NewSimApplier(), testLimits())
	t.Logf("load version=99 err=%v", err)
	if !errors.Is(err, ErrStoreVersion) {
		t.Fatalf("want ErrStoreVersion, got %v", err)
	}

	if err := os.WriteFile(path, []byte(`{not json`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = NewEngine(NewFileStore(path), NewSimApplier(), testLimits())
	t.Logf("load corrupt err=%v", err)
	if !errors.Is(err, ErrStoreCorrupt) {
		t.Fatalf("want ErrStoreCorrupt, got %v", err)
	}
}

// 并发：同一计划的并发推进/暂停/上报与多计划并行，状态必须一致。
func TestConcurrency(t *testing.T) {
	dir := t.TempDir()
	e, sim := newEngine(t, dir)
	defer e.Close()
	p, _ := e.Create("c1", newPlan("p1", StrategyCanary))
	stop := startReporter(t, e, sim, p.Target, p.ID)
	defer stop()

	// 5 个并发 Advance：pmu 串行化，每步只应用一次，多余请求得到可区分错误。
	var wg sync.WaitGroup
	errs := make([]error, 5)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, errs[i] = e.Advance(ctx, fmt.Sprintf("conc-adv-%d", i), p.ID)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		t.Logf("concurrent advance %d err=%v", i, err)
	}
	got := mustGet(t, e, p.ID)
	t.Logf("after concurrent advances: state=%s step=%d", got.State, got.CurrentStep)
	if got.CurrentStep < 0 || got.CurrentStep >= len(got.Steps) {
		t.Fatalf("step out of range: %d", got.CurrentStep)
	}

	// 并发上报同一 ReportID：只允许成功一次。
	var ok, dup int32
	var mu sync.Mutex
	var wg2 sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			err := e.Report(p.ID, Observation{ReportID: "race-1", ReportedAt: time.Now(), Replicas: 1, HasMetrics: true, Samples: 100})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				ok++
			} else if errors.Is(err, ErrDuplicateReport) || errors.Is(err, ErrPlanFinished) {
				dup++
			}
		}()
	}
	wg2.Wait()
	t.Logf("concurrent same-report: ok=%d dup=%d", ok, dup)
	if ok+dup == 20 && ok > 1 {
		t.Fatalf("duplicate report applied %d times", ok)
	}

	// 多计划并行推进互不干扰。
	var wg3 sync.WaitGroup
	for i := 0; i < 3; i++ {
		pl, err := e.Create(fmt.Sprintf("mc-%d", i), newPlan(fmt.Sprintf("m%d", i), StrategyBlueGreen))
		if err != nil {
			t.Fatal(err)
		}
		st := startReporter(t, e, sim, pl.Target, pl.ID)
		defer st()
		wg3.Add(1)
		go func(pl *Plan) {
			defer wg3.Done()
			for s := 0; s < len(pl.Steps); s++ {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := e.Advance(ctx, fmt.Sprintf("%s-adv-%d", pl.ID, s), pl.ID)
				cancel()
				if err != nil {
					t.Errorf("plan %s step %d: %v", pl.ID, s, err)
					return
				}
			}
		}(pl)
	}
	wg3.Wait()
	for i := 0; i < 3; i++ {
		if got := mustGet(t, e, fmt.Sprintf("m%d", i)); got.State != StateSucceeded {
			t.Fatalf("plan m%d: want Succeeded, got %s", i, got.State)
		}
	}
}

// 蓝绿策略：先 0% 流量全量部署，再一次切换。
func TestBlueGreenSemantics(t *testing.T) {
	dir := t.TempDir()
	e, sim := newEngine(t, dir)
	defer e.Close()
	p, _ := e.Create("c1", newPlan("p1", StrategyBlueGreen))
	if len(p.Steps) != 2 || p.Steps[0].Percent != 0 || p.Steps[0].Replicas != 10 ||
		p.Steps[1].Percent != 100 {
		t.Fatalf("unexpected bluegreen steps: %+v", p.Steps)
	}
	stop := startReporter(t, e, sim, p.Target, p.ID)
	defer stop()
	for i := range p.Steps {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		after, err := e.Advance(ctx, fmt.Sprintf("adv-%d", i), p.ID)
		cancel()
		logTransition(t, "Advance(bluegreen)", nil, after, err)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	if got := mustGet(t, e, p.ID); got.State != StateSucceeded {
		t.Fatalf("want Succeeded, got %s", got.State)
	}
}

// 资源上限：计划数超限被拒绝。
func TestPlanLimit(t *testing.T) {
	dir := t.TempDir()
	lim := testLimits()
	lim.MaxPlans = 1
	e, err := NewEngine(NewFileStore(filepath.Join(dir, "s.json")), NewSimApplier(), lim)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.Create("c1", newPlan("p1", StrategyCanary)); err != nil {
		t.Fatal(err)
	}
	_, err = e.Create("c2", newPlan("p2", StrategyCanary))
	t.Logf("create beyond limit err=%v", err)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("want ErrLimitExceeded, got %v", err)
	}
}

// 无效参数：未知策略、非法副本数。
func TestInvalidArguments(t *testing.T) {
	dir := t.TempDir()
	e, _ := newEngine(t, dir)
	defer e.Close()
	if _, err := e.Create("c1", newPlan("p1", "random")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument, got %v", err)
	}
	bad := newPlan("p2", StrategyCanary)
	bad.TotalReplicas = 0
	if _, err := e.Create("c2", bad); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument, got %v", err)
	}
	if _, err := e.Get("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
