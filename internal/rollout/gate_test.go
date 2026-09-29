package rollout

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// gateEngine 构造不自动上报的引擎（手动喂观测，精确控制每个维度）。
// HealthPolicy 挂在计划上（newGatePlan 会把失败预算调大），避免等待循环反复
// 判定消耗预算干扰维度验证。
func gateEngine(t *testing.T, dir string) (*Engine, *SimApplier) {
	t.Helper()
	sim := NewSimApplier()
	sim.StepReplicas = 0
	e, err := NewEngine(NewFileStore(dir+"/s.json"), sim, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	return e, sim
}

// gatePlan 构造失败预算很大的金丝雀计划，避免中间态判定烧预算。
func gatePlan(id string, kind StrategyKind) Plan {
	pl := newPlan(id, kind)
	pl.Health.FailureBudget = 1000
	return pl
}

// startStep 触发某阶段的应用并建立在途位点（GateSeq 水位线）。无观测时 Advance
// 会等到短超时返回 ErrNotConverged，此时位点已落盘；之后喂的观测才属于该阶段。
func startStep(t *testing.T, e *Engine, cmdID, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	_, err := e.Advance(ctx, cmdID, id)
	cancel()
	if !errors.Is(err, ErrNotConverged) {
		t.Fatalf("startStep: want ErrNotConverged before feeding obs, got %v", err)
	}
}

// obs 构造一条各维度齐全的观测（版本 v2 / traffic / replicas 由参数指定）。
func obs(seqHint string, traffic, replicas int) Observation {
	return Observation{
		ReportID:         seqHint,
		ReportedAt:       time.Now(),
		EffectiveVersion: "v2",
		TrafficPercent:   traffic,
		Replicas:         replicas,
		ErrorRate:        0,
		Samples:          100,
		HasMetrics:       true,
	}
}

// TestGateDimensionsIndependent 版本/流量/承载三维度分别对上才算通过，
// 任一维度不匹配都只能继续等，不能提前放行。
func TestGateDimensionsIndependent(t *testing.T) {
	dir := t.TempDir()
	e, _ := gateEngine(t, dir)
	defer e.Close()
	pl := gatePlan("g1", StrategyCanary) // step0: percent=10 replicas=1
	p, _ := e.Create("c1", pl)

	// 先应用 step0 期望并建立观测水位线，再逐条喂观测。
	startStep(t, e, "adv-init", p.ID)

	type probe struct {
		name    string
		version string
		traffic int
		repl    int
		want    string
	}
	probes := []probe{
		{"wrong version", "v1", 10, 1, VerdictVersionMismatch},
		{"traffic short", "v2", 9, 1, VerdictTrafficNotReached},
		{"replicas under", "v2", 10, 0, VerdictNotConverged},
		{"replicas over", "v2", 10, 2, VerdictNotConverged},
		{"traffic over but replicas wrong still blocked", "v2", 50, 2, VerdictNotConverged},
	}
	for i, pr := range probes {
		o := obs(fmt.Sprintf("bad-%d", i), pr.traffic, pr.repl)
		o.EffectiveVersion = pr.version
		if err := e.Report(p.ID, o); err != nil {
			t.Fatal(err)
		}
		// 重入在途阶段触发判定；维度不匹配会一直等到短超时返回 ErrNotConverged。
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
		_, aerr := e.Advance(ctx, fmt.Sprintf("adv-bad-%d", i), p.ID)
		cancel()
		got := mustGet(t, e, p.ID)
		var last Verdict
		for _, v := range got.Verdicts {
			if v.Step == 0 && v.ReportID == fmt.Sprintf("bad-%d", i) {
				last = v
			}
		}
		t.Logf("input=%-45s version=%q traffic=%2d repl=%d -> code=%s (%s); state=%s pending=%d err=%v",
			pr.name, pr.version, pr.traffic, pr.repl, last.Code, last.Reason, got.State, got.PendingStep, aerr)
		if last.Code != pr.want {
			t.Fatalf("%s: want verdict %s, got %s", pr.name, pr.want, last.Code)
		}
		if got.PendingStep != 0 || got.CurrentStep != -1 {
			t.Fatalf("%s: must remain on pending step 0, got current=%d pending=%d", pr.name, got.CurrentStep, got.PendingStep)
		}
		if !errors.Is(aerr, ErrNotConverged) {
			t.Fatalf("%s: want ErrNotConverged, got %v", pr.name, aerr)
		}
	}

	// 三个维度同时对上：阶段通过，进入 step1（重入在途阶段，新 cmdID）。
	if err := e.Report(p.ID, obs("good-0", 10, 1)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	after, err := e.Advance(ctx, "adv-good", p.ID)
	cancel()
	if err != nil {
		t.Fatalf("Advance good: %v", err)
	}
	if after.CurrentStep != 0 || after.PendingStep != -1 {
		t.Fatalf("want step 0 passed, current=%d pending=%d", after.CurrentStep, after.PendingStep)
	}
	t.Logf("all dimensions matched -> step 0 passed, state=%s current=%d", after.State, after.CurrentStep)
}

// TestGateOneObservationCannotPassTwoSteps 一次观测绝不能同时让两个阶段通过：
// 第二阶段应用后，即便已有观测“看起来”满足第二阶段期望，也必须等到新上报。
func TestGateOneObservationCannotPassTwoSteps(t *testing.T) {
	dir := t.TempDir()
	e, _ := gateEngine(t, dir)
	defer e.Close()
	// batch [1, 1] 总量 10：step0 累计 1 副本 / 10%；step1 累计 2 副本 / 20%。
	pl := gatePlan("g2", StrategyBatch)
	pl.TotalReplicas = 10
	pl.BatchSizes = []int{1, 1}
	p, _ := e.Create("c1", pl)

	startStep(t, e, "adv0-init", p.ID)

	// 喂一条“甚至超过 step1 期望”的观测（2 副本 / 20%）。它对 step0 而言承载
	// 不相等，不能让 step0 通过；更不可能提前满足 step1。
	if err := e.Report(p.ID, obs("r-over", 20, 2)); err != nil {
		t.Fatal(err)
	}
	ctx0, cancel0 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err0 := e.Advance(ctx0, "adv0-over", p.ID)
	cancel0()
	if !errors.Is(err0, ErrNotConverged) {
		t.Fatalf("over-shooting observation must not pass step0: %v", err0)
	}
	got := mustGet(t, e, p.ID)
	if got.PendingStep != 0 || got.CurrentStep != -1 {
		t.Fatalf("over-shooting observation must not pass step0: current=%d pending=%d", got.CurrentStep, got.PendingStep)
	}
	var last Verdict
	for _, v := range got.Verdicts {
		if v.ReportID == "r-over" {
			last = v
		}
	}
	t.Logf("over-shooting observation blocked step0: code=%s %s", last.Code, last.Reason)
	if last.Code != VerdictNotConverged {
		t.Fatalf("want replicas-not-converged, got %s", last.Code)
	}

	// step0 精确对上，通过。
	if err := e.Report(p.ID, obs("r0", 10, 1)); err != nil {
		t.Fatal(err)
	}
	ctx1, cancel1 := context.WithTimeout(context.Background(), 3*time.Second)
	a0, err := e.Advance(ctx1, "adv0", p.ID)
	cancel1()
	if err != nil {
		t.Fatalf("Advance step0: %v", err)
	}
	t.Logf("step0 passed: current=%d gateSeq=%d nextSeq=%d", a0.CurrentStep, a0.GateSeq, a0.NextSeq)

	// 进入 step1 后，不喂新观测：已有观测即使是 2 副本/20%，也因序号 < gateSeq
	// 必须判 stale，不能通过 step1。
	startStep(t, e, "adv1-init", p.ID)
	got = mustGet(t, e, p.ID)
	last = got.Verdicts[len(got.Verdicts)-1]
	t.Logf("step1 with no fresh observation -> code=%s reason=%q pending=%d", last.Code, last.Reason, got.PendingStep)
	if last.Code != VerdictStaleObservation {
		t.Fatalf("single observation must not carry across stages: %s", last.Code)
	}
	if got.PendingStep != 1 || got.CurrentStep != 0 {
		t.Fatalf("must stay at pending step1, got current=%d pending=%d", got.CurrentStep, got.PendingStep)
	}

	// 一条全新观测满足 step1：通过。
	if err := e.Report(p.ID, obs("r1", 20, 2)); err != nil {
		t.Fatal(err)
	}
	ctx3, cancel3 := context.WithTimeout(context.Background(), 3*time.Second)
	a1, err := e.Advance(ctx3, "adv1", p.ID)
	cancel3()
	if err != nil {
		t.Fatalf("Advance step1: %v", err)
	}
	if a1.CurrentStep != 1 {
		t.Fatalf("want step1 passed, got %d", a1.CurrentStep)
	}
	t.Logf("fresh observation passed step1: current=%d state=%s", a1.CurrentStep, a1.State)
}

// TestGateJitterDoesNotPass 抖动导致承载短暂偏离时不能判通过。
func TestGateJitterDoesNotPass(t *testing.T) {
	dir := t.TempDir()
	e, _ := gateEngine(t, dir)
	defer e.Close()
	pl := gatePlan("g3", StrategyCanary)
	p, _ := e.Create("c1", pl)
	startStep(t, e, "a-init", p.ID)
	// 版本/流量对上，但承载抖动到 2（期望 1）：必须阻塞。
	if err := e.Report(p.ID, obs("j1", 10, 2)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, aerr := e.Advance(ctx, "a-jitter", p.ID)
	cancel()
	if !errors.Is(aerr, ErrNotConverged) {
		t.Fatalf("jitter advance: want ErrNotConverged, got %v", aerr)
	}
	got := mustGet(t, e, p.ID)
	var jv Verdict
	for _, v := range got.Verdicts {
		if v.ReportID == "j1" {
			jv = v
		}
	}
	if jv.Code != VerdictNotConverged {
		t.Fatalf("jitter must block: %+v", jv)
	}
	t.Logf("jitter blocked: %s", jv.Reason)
}
