package rollout

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestBatchHappyPath 固定副本分批正常链路：每批新增固定副本，逐步推进至成功。
func TestBatchHappyPath(t *testing.T) {
	dir := t.TempDir()
	sim := NewSimApplier()
	sim.StepReplicas = 1 // 承载逐副本收敛，制造中间态
	e, err := NewEngine(NewFileStore(dir+"/s.json"), sim, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	pl := newPlan("b1", StrategyBatch)
	pl.BatchSizes = []int{2, 3, 5} // 累计 2 -> 5 -> 10（恰等于承载上限）
	p, err := e.Create("c-create", pl)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Logf("input batches=%v total=%d -> steps:", p.BatchSizes, p.TotalReplicas)
	for _, s := range p.Steps {
		t.Logf("  step %d %s: replicas=%d percent=%d%%", s.Index, s.Name, s.Replicas, s.Percent)
	}
	if len(p.Steps) != 3 {
		t.Fatalf("want 3 steps, got %d", len(p.Steps))
	}
	wantRep := []int{2, 5, 10}
	wantPct := []int{20, 50, 100}
	for i := range p.Steps {
		if p.Steps[i].Replicas != wantRep[i] || p.Steps[i].Percent != wantPct[i] {
			t.Fatalf("step %d: got replicas=%d percent=%d, want %d/%d",
				i, p.Steps[i].Replicas, p.Steps[i].Percent, wantRep[i], wantPct[i])
		}
	}

	stop := startReporter(t, e, sim, p.Target, p.ID)
	defer stop()
	for i := range p.Steps {
		before := mustGet(t, e, p.ID)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		after, err := e.Advance(ctx, fmt.Sprintf("c-adv-%d", i), p.ID)
		cancel()
		logTransition(t, "Advance(batch)", before, after, err)
		if err != nil {
			t.Fatalf("Advance step %d: %v", i, err)
		}
		if after.CurrentStep != i || after.PendingStep != -1 {
			t.Fatalf("step %d: currentStep=%d pending=%d", i, after.CurrentStep, after.PendingStep)
		}
	}
	final := mustGet(t, e, p.ID)
	if final.State != StateSucceeded {
		t.Fatalf("want Succeeded, got %s", final.State)
	}
	// 每个阶段都必须留下一条 ok 判定，且逐维度依据齐备。
	oks := 0
	for _, v := range final.Verdicts {
		if v.Code == VerdictOK {
			oks++
			t.Logf("ok verdict step=%d want/got version=%q/%q traffic=%d/%d replicas=%d/%d report=%s",
				v.Step, v.WantVersion, v.GotVersion, v.WantPercent, v.GotPercent, v.WantReplicas, v.GotReplicas, v.ReportID)
		}
	}
	if oks != 3 {
		t.Fatalf("want 3 ok verdicts (one per step), got %d", oks)
	}
}

// TestBatchInvalidConfig 分批配置非法必须明确报错，绝不回落默认值继续跑。
func TestBatchInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	e, _ := newEngine(t, dir)
	defer e.Close()

	cases := []struct {
		name  string
		total int
		sizes []int
	}{
		{"empty batch list", 10, nil},
		{"empty batch list (zero len)", 10, []int{}},
		{"zero replicas in a batch", 10, []int{2, 0, 3}},
		{"negative replicas", 10, []int{2, -1}},
		{"sum exceeds capacity", 10, []int{4, 4, 4}},     // 12 > 10
		{"single batch exceeds capacity", 10, []int{11}}, // 11 > 10
		{"zero total replicas", 0, []int{1}},
	}
	for _, c := range cases {
		_, err := BuildSteps(StrategyBatch, c.total, c.sizes)
		t.Logf("input=%s total=%d sizes=%v -> err=%v", c.name, c.total, c.sizes, err)
		if !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%s: want ErrInvalidArgument, got %v", c.name, err)
		}
		// 经引擎创建同样必须被拒绝，且计划不应落库。
		pl := newPlan("x-"+c.name, StrategyBatch)
		pl.TotalReplicas = c.total
		pl.BatchSizes = c.sizes
		if _, cerr := e.Create("cmd-x", pl); !errors.Is(cerr, ErrInvalidArgument) {
			t.Fatalf("%s via Create: want ErrInvalidArgument, got %v", c.name, cerr)
		}
		if _, gerr := e.Get(pl.ID); !errors.Is(gerr, ErrNotFound) {
			t.Fatalf("%s: rejected plan must not be stored, Get err=%v", c.name, gerr)
		}
	}
}

// TestBatchPartialCapacity 各批总和允许小于承载上限（分批放量不要求最终全量）。
func TestBatchPartialCapacity(t *testing.T) {
	steps, err := BuildSteps(StrategyBatch, 10, []int{2, 3}) // 累计 5 < 10
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 || steps[1].Replicas != 5 || steps[1].Percent != 50 {
		t.Fatalf("unexpected partial steps: %+v", steps)
	}
	t.Logf("partial capacity accepted: %+v", steps)
}

// TestBatchPauseResumeRollback 分批方式同样支持暂停、继续与回滚。
func TestBatchPauseResumeRollback(t *testing.T) {
	dir := t.TempDir()
	e, sim := newEngine(t, dir)
	defer e.Close()
	pl := newPlan("bp1", StrategyBatch)
	pl.BatchSizes = []int{2, 3, 5}
	p, _ := e.Create("c1", pl)
	stop := startReporter(t, e, sim, p.Target, p.ID)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if _, err := e.Advance(ctx, "a1", p.ID); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	cancel()
	if _, err := e.Pause("p1", p.ID); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	// 暂停期间推进被拒绝。
	if _, err := e.Advance(context.Background(), "a2", p.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("advance while paused: want ErrInvalidTransition, got %v", err)
	}
	if _, err := e.Resume("r1", p.ID); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	// 继续推进第二批。
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	if _, err := e.Advance(ctx2, "a2", p.ID); err != nil {
		t.Fatalf("Advance step2: %v", err)
	}
	cancel2()
	got := mustGet(t, e, p.ID)
	t.Logf("after resume+advance: state=%s current=%d pending=%d", got.State, got.CurrentStep, got.PendingStep)

	// 回滚到稳定版本。
	if _, err := e.Rollback("rb1", p.ID); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	fin := mustGet(t, e, p.ID)
	if fin.State != StateRolledBack {
		t.Fatalf("want RolledBack, got %s", fin.State)
	}
	// 回滚到稳定版本（模拟承载逐步归零后，观测应显示旧版本、0 新副本）。
	var obs Observation
	for i := 0; i < 10; i++ {
		obs, _ = sim.Observe(p.Target)
		if obs.Replicas == 0 {
			break
		}
	}
	if obs.Replicas != 0 || obs.EffectiveVersion != "v1" {
		t.Fatalf("rollback restore: replicas=%d version=%q", obs.Replicas, obs.EffectiveVersion)
	}
	t.Logf("batch rolled back: replicas=0 version=v1")
}

// TestBatchRestartRecovery 分批推进到一半重启：位点一致继续推进，不重复放量。
func TestBatchRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	e1, sim := newEngine(t, dir)
	pl := newPlan("br1", StrategyBatch)
	pl.BatchSizes = []int{2, 3, 5}
	p, _ := e1.Create("c1", pl)
	stop1 := startReporter(t, e1, sim, p.Target, p.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if _, err := e1.Advance(ctx, "a1", p.ID); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	cancel()
	stop1()
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("=== simulated crash & restart (batch) ===")

	e2, err := NewEngine(NewFileStore(dir+"/store.json"), sim, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	got := mustGet(t, e2, p.ID)
	t.Logf("recovered: state=%s current=%d pending=%d gateSeq=%d nextSeq=%d",
		got.State, got.CurrentStep, got.PendingStep, got.GateSeq, got.NextSeq)
	if got.State != StateRunning || got.CurrentStep != 0 {
		t.Fatalf("recovery site inconsistent: %+v", got)
	}
	// 已应用命令跨重启仍拒绝。
	if _, err := e2.Advance(context.Background(), "a1", p.ID); !errors.Is(err, ErrDuplicateCommand) {
		t.Fatalf("want ErrDuplicateCommand after restart, got %v", err)
	}
	stop2 := startReporter(t, e2, sim, p.Target, p.ID)
	defer stop2()
	for i := 1; i < len(got.Steps); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		after, err := e2.Advance(ctx, fmt.Sprintf("a%d", i+1), p.ID)
		cancel()
		if err != nil {
			t.Fatalf("Advance step %d after restart: %v", i, err)
		}
		t.Logf("post-restart step %d: state=%s current=%d", i, after.State, after.CurrentStep)
	}
	if fin := mustGet(t, e2, p.ID); fin.State != StateSucceeded {
		t.Fatalf("want Succeeded, got %s", fin.State)
	}
}

// TestBatchBoundedGrowth 长期运行下批次不会越跑越多、状态记录不无限膨胀。
func TestBatchBoundedGrowth(t *testing.T) {
	dir := t.TempDir()
	lim := testLimits()
	lim.MaxObservations = 20
	sim := NewSimApplier()
	sim.StepReplicas = 0 // 立即收敛，快速跑大量观测
	e, err := NewEngine(NewFileStore(dir+"/s.json"), sim, lim)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	pl := newPlan("bb1", StrategyBatch)
	pl.BatchSizes = []int{10}
	p, _ := e.Create("c1", pl)
	stop := startReporter(t, e, sim, p.Target, p.ID)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if _, err := e.Advance(ctx, "a1", p.ID); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	cancel()
	time.Sleep(150 * time.Millisecond) // 终态后上报器继续尝试，均被拒
	got := mustGet(t, e, p.ID)
	t.Logf("after heavy reports: observations=%d (cap %d), seen=%d, cmds=%d, verdicts<=cap, steps=%d",
		len(got.Observations), lim.MaxObservations, len(got.SeenReports), len(got.AppliedCmds), len(got.Steps))
	if len(got.Observations) > lim.MaxObservations {
		t.Fatalf("observations unbounded: %d", len(got.Observations))
	}
	if len(got.SeenReports) > lim.MaxObservations {
		t.Fatalf("seen reports unbounded: %d", len(got.SeenReports))
	}
	if len(got.Steps) != 1 {
		t.Fatalf("batch count drifted: %d", len(got.Steps))
	}
	if len(got.Verdicts) > verdictCap {
		t.Fatalf("verdicts unbounded: %d", len(got.Verdicts))
	}
}
