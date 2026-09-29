package rollout

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// 本文件覆盖新增能力：
//  1. batch（固定副本数分批）策略的正常链路与参数边界；
//  2. 多维门控（版本/流量/承载）单次观测不能跨阶段通过；
//  3. 同一目标并发互斥、不同目标并行；
//  4. 长时间运行状态有界；
//  5. v1 旧数据迁移与不兼容数据拒绝。

func newBatchPlan(id string, total int, batches []int) Plan {
	return Plan{
		ID: id, Target: "svc-" + id, Strategy: StrategyBatch,
		FromVersion: "v1", ToVersion: "v2",
		TotalReplicas: total, BatchReplicas: batches, Health: testHealth(),
	}
}

// 正常链路：按固定副本数分批推进，每阶段三维度同时对上才算完成，直至成功。
func TestBatchHappyPath(t *testing.T) {
	dir := t.TempDir()
	e, sim := newEngine(t, dir)
	defer e.Close()

	p, err := e.Create("c1", newBatchPlan("b1", 10, []int{2, 3, 5}))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Logf("input: total=10 batches=[2,3,5]")
	for _, st := range p.Steps {
		t.Logf("  step %d %s: want version=%s replicas=%d traffic=%d%%",
			st.Index, st.Name, st.Version, st.Replicas, st.Percent)
	}
	if len(p.Steps) != 3 || p.Steps[0].Replicas != 2 || p.Steps[1].Replicas != 5 || p.Steps[2].Replicas != 10 {
		t.Fatalf("unexpected batch steps: %+v", p.Steps)
	}
	if p.Steps[2].Percent != 100 {
		t.Fatalf("final batch percent want 100, got %d", p.Steps[2].Percent)
	}

	stop := startReporter(t, e, sim, p.Target, p.ID)
	defer stop()
	for i := range p.Steps {
		before := mustGet(t, e, p.ID)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		after, err := e.Advance(ctx, fmt.Sprintf("adv-%d", i), p.ID)
		cancel()
		if err != nil {
			t.Fatalf("Advance step %d: %v", i, err)
		}
		t.Logf("step %d done: completed=%d gateStep=%d state=%s",
			i, after.Gate.CompletedStep, after.Gate.GateStep, after.State)
		if after.Gate.CompletedStep != i {
			t.Fatalf("step %d: completedStep=%d", i, after.Gate.CompletedStep)
		}
		last := after.Verdicts[len(after.Verdicts)-1]
		t.Logf("  gate basis: code=%s reason=%q", last.Code, last.Reason)
		if last.Code != VerdictOK {
			t.Fatalf("step %d final verdict %s", i, last.Code)
		}
		_ = before
	}
	final := mustGet(t, e, p.ID)
	if final.State != StateSucceeded {
		t.Fatalf("want Succeeded, got %s", final.State)
	}
}

// 参数边界：空批次、非正副本数、总量超过承载上限都必须报 ErrInvalidArgument。
func TestBatchValidation(t *testing.T) {
	dir := t.TempDir()
	e, _ := newEngine(t, dir)
	defer e.Close()

	cases := []struct {
		name    string
		total   int
		batches []int
	}{
		{"empty batches", 10, nil},
		{"zero replicas in batch", 10, []int{2, 0, 3}},
		{"negative replicas in batch", 10, []int{2, -1}},
		{"batch total exceeds capacity", 10, []int{4, 4, 4}}, // 12 > 10
		{"single batch exceeds capacity", 3, []int{4}},
		{"zero total", 0, []int{1}},
	}
	for _, c := range cases {
		_, err := e.Create("cmd-"+c.name, newBatchPlan("p-"+c.name, c.total, c.batches))
		t.Logf("input=%s total=%d batches=%v -> err=%v", c.name, c.total, c.batches, err)
		if !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%s: want ErrInvalidArgument, got %v", c.name, err)
		}
	}
}

// 合法的部分发布（批次总量小于承载上限）应当被接受。
func TestBatchPartialCapacityAccepted(t *testing.T) {
	dir := t.TempDir()
	e, _ := newEngine(t, dir)
	defer e.Close()
	p, err := e.Create("c1", newBatchPlan("b1", 10, []int{2, 3})) // 累计 5 < 10
	if err != nil {
		t.Fatalf("partial-capacity batch should be accepted: %v", err)
	}
	if last := p.Steps[len(p.Steps)-1]; last.Replicas != 5 {
		t.Fatalf("want last replicas=5, got %d", last.Replicas)
	}
	t.Logf("partial batch accepted: steps=%+v", p.Steps)
}

// batch 策略支持暂停/继续/回滚，且回滚后目标被释放，可对同一目标重新发布。
func TestBatchPauseResumeRollback(t *testing.T) {
	dir := t.TempDir()
	e, sim := newEngine(t, dir)
	defer e.Close()
	p, _ := e.Create("c1", newBatchPlan("b1", 10, []int{2, 2, 6}))
	stop := startReporter(t, e, sim, p.Target, p.ID)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if _, err := e.Advance(ctx, "adv-0", p.ID); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	cancel()
	if _, err := e.Pause("p1", p.ID); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	t.Log("batch plan paused after first batch")
	// 暂停期间同目标新发布仍被拒绝（互斥包含 Paused）。
	sameTarget := newBatchPlan("b2", 10, []int{1})
	sameTarget.Target = p.Target
	if _, err := e.Create("c2", sameTarget); !errors.Is(err, ErrTargetBusy) {
		t.Fatalf("paused plan must still hold target: got %v", err)
	}
	if _, err := e.Resume("r1", p.ID); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if _, err := e.Rollback("rb1", p.ID); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if got := mustGet(t, e, p.ID); got.State != StateRolledBack {
		t.Fatalf("want RolledBack, got %s", got.State)
	}
	t.Logf("rolled back, target %s released", p.Target)
	// 终态后同目标可再建新发布。
	p2plan := newBatchPlan("b1b", 10, []int{1, 9})
	p2plan.Target = p.Target
	p2, err := e.Create("c3", p2plan)
	if err != nil {
		t.Fatalf("create after rollback on same target: %v", err)
	}
	if p2.Target != p.Target {
		t.Fatalf("target mismatch: %s vs %s", p2.Target, p.Target)
	}
}

// 多维门控：单次观测不能让两个阶段同时通过；阶段必须各等各的新代次观测。
func TestGateNoCrossStepPass(t *testing.T) {
	dir := t.TempDir()
	sim := NewSimApplier()
	sim.StepReplicas = 0 // 立即收敛，消除“爬坡”干扰
	e, err := NewEngine(NewFileStore(filepath.Join(dir, "s.json")), sim, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	p, err := e.Create("c1", newBatchPlan("g1", 10, []int{2, 3, 5}))
	if err != nil {
		t.Fatal(err)
	}
	steps := p.Steps

	// 阶段 0：上报一条完全匹配阶段 0 的观测，应通过阶段 0。
	advanceAndConfirm(t, e, p.ID, "adv-0", func(seq int64) Observation {
		return Observation{
			ReportID: fmt.Sprintf("r-s0"), ReportedAt: time.Now(),
			Replicas: steps[0].Replicas, ActiveVersion: steps[0].Version, HasVersion: true,
			TrafficPercent: steps[0].Percent, HasTraffic: true,
			HasMetrics: true, Samples: 100,
		}
	})
	got := mustGet(t, e, p.ID)
	if got.Gate.CompletedStep != 0 || got.Gate.GateStep != -1 {
		t.Fatalf("after step0: completed=%d gateStep=%d", got.Gate.CompletedStep, got.Gate.GateStep)
	}
	t.Logf("step0 confirmed on its own observation; gateSeq now %d", got.Gate.GateSeq)

	// 进入阶段 1 后，旧观测（阶段 0 代次）必须被忽略，哪怕数字碰巧也对不上。
	// 先直接给一条“什么都对但属于旧代次”的观测：不可能，因为 GateSeq 由引擎盖戳；
	// 这里改为验证：阶段 1 等待期间，不产生任何新观测时绝不通过（旧观测被排除）。
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_, err := e.Advance(ctx, "adv-1", p.ID)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	waiting := mustGet(t, e, p.ID)
	t.Logf("step1 waiting with stale step0 observation only: gateStep=%d gateSeq=%d state=%s",
		waiting.Gate.GateStep, waiting.Gate.GateSeq, waiting.State)
	if waiting.Gate.GateStep != 1 || waiting.State != StateRunning {
		t.Fatalf("unexpected state while waiting step1: %+v", waiting.Gate)
	}
	if err := <-done; !errors.Is(err, ErrNotConverged) {
		t.Fatalf("stale observation must not pass step1: got %v", err)
	}

	// 新代次观测匹配阶段 1 -> 阶段 1 通过，阶段 2 仍未开始。
	advanceAndConfirm(t, e, p.ID, "adv-1b", func(seq int64) Observation {
		return Observation{
			ReportID: "r-s1", ReportedAt: time.Now(),
			Replicas: steps[1].Replicas, ActiveVersion: steps[1].Version, HasVersion: true,
			TrafficPercent: steps[1].Percent, HasTraffic: true,
			HasMetrics: true, Samples: 100,
		}
	})
	got = mustGet(t, e, p.ID)
	if got.Gate.CompletedStep != 1 {
		t.Fatalf("after step1: completed=%d", got.Gate.CompletedStep)
	}
	t.Log("step1 confirmed only on a fresh-epoch observation; no cross-step pass")
}

// 多维门控：版本/流量/承载任一维度对不上都不得放行；三者同时对上才通过。
func TestGateDimensionsMustAllMatch(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(o *Observation, s Step)
		want   string
	}{
		{"wrong version", func(o *Observation, s Step) { o.ActiveVersion = "v9" }, VerdictVersionMismatch},
		{"version not reported", func(o *Observation, s Step) { o.HasVersion = false }, VerdictVersionMismatch},
		{"traffic below", func(o *Observation, s Step) { o.TrafficPercent = s.Percent - 1 }, VerdictTrafficNotReached},
		{"traffic above", func(o *Observation, s Step) { o.TrafficPercent = s.Percent + 1 }, VerdictTrafficNotReached},
		{"traffic not reported", func(o *Observation, s Step) { o.HasTraffic = false }, VerdictTrafficNotReached},
		{"replicas short", func(o *Observation, s Step) { o.Replicas = s.Replicas - 1 }, VerdictCapacityNotConverged},
		{"replicas over", func(o *Observation, s Step) { o.Replicas = s.Replicas + 1 }, VerdictCapacityNotConverged},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			sim := NewSimApplier()
			sim.StepReplicas = 0
			e, err := NewEngine(NewFileStore(filepath.Join(dir, "s.json")), sim, testLimits())
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			p, _ := e.Create("c1", newBatchPlan("d1", 10, []int{4, 6}))
			s := p.Steps[0]

			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
				defer cancel()
				_, _ = e.Advance(ctx, "adv-0", p.ID)
			}()
			// 等进入阶段后，上报一条“恰好缺一个维度”的观测，再直接评估门控。
			time.Sleep(30 * time.Millisecond)
			cur := mustGet(t, e, p.ID)
			o := Observation{
				ReportID: "bad-1", ReportedAt: time.Now(),
				Replicas: s.Replicas, ActiveVersion: s.Version, HasVersion: true,
				TrafficPercent: s.Percent, HasTraffic: true,
				HasMetrics: true, Samples: 100,
			}
			c.mutate(&o, s)
			if err := e.Report(p.ID, o); err != nil {
				t.Fatal(err)
			}
			cur = mustGet(t, e, p.ID)
			gr := EvaluateGate(cur, time.Now())
			t.Logf("input=%s -> gate healthy=%v code=%s reason=%q", c.name, gr.Healthy, gr.Code, gr.Reason)
			if gr.Healthy || gr.Code != c.want {
				t.Fatalf("want unhealthy %s, got healthy=%v code=%s", c.want, gr.Healthy, gr.Code)
			}
			if g := mustGet(t, e, p.ID); g.Gate.CompletedStep != -1 {
				t.Fatalf("dimension mismatch must not complete step: %d", g.Gate.CompletedStep)
			}
		})
	}
}

// advanceAndConfirm 启动一次 Advance，等待进入阶段后用 build 构造并上报匹配观测直到通过。
func advanceAndConfirm(t *testing.T, e *Engine, id, cmdID string, build func(gateSeq int64) Observation) {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := e.Advance(ctx, cmdID, id)
		result <- err
	}()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		cur := mustGet(t, e, id)
		if cur.Gate.GateStep >= 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cur := mustGet(t, e, id)
	if cur.Gate.GateStep < 0 {
		t.Fatalf("advance %s did not enter a gate step", cmdID)
	}
	o := build(cur.Gate.GateSeq)
	if o.ReportID == "" {
		o.ReportID = fmt.Sprintf("%s-%d", cmdID, time.Now().UnixNano())
	}
	if err := e.Report(id, o); err != nil {
		t.Fatalf("Report: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("Advance %s: %v", cmdID, err)
	}
}

// 同一目标并发创建：只允许一个进行中发布，其余得到可区分的 ErrTargetBusy。
func TestTargetMutexConcurrentCreate(t *testing.T) {
	dir := t.TempDir()
	e, _ := newEngine(t, dir)
	defer e.Close()

	const target = "shared-svc"
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ok, busy, other int
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pl := newBatchPlan(fmt.Sprintf("p%02d", i), 10, []int{1, 9})
			pl.Target = target
			_, err := e.Create(fmt.Sprintf("create-%d", i), pl)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case errors.Is(err, ErrTargetBusy):
				busy++
			default:
				other++
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	t.Logf("concurrent create on same target: ok=%d busy=%d other=%d", ok, busy, other)
	if ok != 1 || busy != 19 || other != 0 {
		t.Fatalf("want exactly 1 accepted and 19 ErrTargetBusy, got ok=%d busy=%d other=%d", ok, busy, other)
	}
}

// 不同目标可以并行推进，互不影响、各自成功。
func TestDifferentTargetsParallel(t *testing.T) {
	dir := t.TempDir()
	e, sim := newEngine(t, dir)
	defer e.Close()

	const n = 4
	var wg sync.WaitGroup
	stops := []func(){}
	for i := 0; i < n; i++ {
		p, err := e.Create(fmt.Sprintf("c-%d", i), newBatchPlan(fmt.Sprintf("t%d", i), 10, []int{2, 3, 5}))
		if err != nil {
			t.Fatal(err)
		}
		st := startReporter(t, e, sim, p.Target, p.ID)
		stops = append(stops, st)
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for s := 0; s < 3; s++ {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := e.Advance(ctx, fmt.Sprintf("%s-adv-%d", id, s), id)
				cancel()
				if err != nil {
					t.Errorf("plan %s step %d: %v", id, s, err)
					return
				}
			}
		}(p.ID)
	}
	wg.Wait()
	for _, st := range stops {
		st()
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("t%d", i)
		got := mustGet(t, e, id)
		t.Logf("parallel target %s: state=%s completed=%d", id, got.State, got.Gate.CompletedStep)
		if got.State != StateSucceeded {
			t.Fatalf("target %s: want Succeeded, got %s", id, got.State)
		}
	}
}

// 同一目标串行：在前一个发布结束前，针对该目标的推进/暂停/回滚与新创建互斥。
func TestSameTargetSerialAcrossPlans(t *testing.T) {
	dir := t.TempDir()
	e, sim := newEngine(t, dir)
	defer e.Close()

	first := newBatchPlan("a1", 10, []int{2, 8})
	first.Target = "serial-svc"
	p, err := e.Create("c1", first)
	if err != nil {
		t.Fatal(err)
	}
	stop := startReporter(t, e, sim, p.Target, p.ID)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if _, err := e.Advance(ctx, "adv-0", p.ID); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	cancel()

	// 同目标第二个计划被拒绝，且原因与参数错误区分。
	second := newBatchPlan("a2", 10, []int{5, 5})
	second.Target = "serial-svc"
	_, err = e.Create("c2", second)
	t.Logf("create on busy target err=%v", err)
	if !errors.Is(err, ErrTargetBusy) {
		t.Fatalf("want ErrTargetBusy, got %v", err)
	}

	// 第一个计划正常走完后，目标释放。
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	if _, err := e.Advance(ctx, "adv-1", p.ID); err != nil {
		t.Fatalf("Advance final: %v", err)
	}
	cancel()
	if _, err := e.Create("c3", second); err != nil {
		t.Fatalf("target should be free after success: %v", err)
	}
}

// 重启后互斥依旧生效：进行中的计划占用目标，拒绝新发布。
func TestTargetMutexAfterRestart(t *testing.T) {
	dir := t.TempDir()
	e1, sim := newEngine(t, dir)
	pl := newBatchPlan("r1", 10, []int{2, 8})
	pl.Target = "restart-svc"
	p, err := e1.Create("c1", pl)
	if err != nil {
		t.Fatal(err)
	}
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
	t.Log("=== simulated crash & restart (target mutex) ===")

	e2, err := NewEngine(NewFileStore(filepath.Join(dir, "store.json")), sim, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	other := newBatchPlan("r2", 10, []int{1, 9})
	other.Target = "restart-svc"
	_, err = e2.Create("c2", other)
	t.Logf("after restart, create on active target err=%v", err)
	if !errors.Is(err, ErrTargetBusy) {
		t.Fatalf("want ErrTargetBusy after restart, got %v", err)
	}
	// 已回滚的旧目标释放：先回滚再建新计划。
	if _, err := e2.Rollback("rb1", p.ID); err != nil {
		t.Fatalf("Rollback after restart: %v", err)
	}
	other.ID = "r3"
	if _, err := e2.Create("c3", other); err != nil {
		t.Fatalf("target should be released after rollback: %v", err)
	}
}

// 长时间运行状态有界：批次/观测/判定不会越跑越多、不会无限膨胀。
func TestStateStaysBounded(t *testing.T) {
	dir := t.TempDir()
	sim := NewSimApplier()
	sim.StepReplicas = 0
	lim := testLimits()
	lim.MaxObservations = 8
	e, err := NewEngine(NewFileStore(filepath.Join(dir, "s.json")), sim, lim)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	p, _ := e.Create("c1", newBatchPlan("b1", 10, []int{2, 3, 5}))

	// 进入阶段 0 后灌大量观测：环形缓冲必须封顶。
	entered := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = e.Advance(ctx, "adv-0", p.ID)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if g := mustGet(t, e, p.ID); g.Gate.GateStep == 0 {
			close(entered)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	<-entered
	for i := 0; i < 100; i++ {
		if err := e.Report(p.ID, Observation{
			ReportID: fmt.Sprintf("flood-%d", i), ReportedAt: time.Now(),
			Replicas: 1, HasMetrics: true, Samples: 100,
			// 故意给不匹配维度，制造“继续等”而不通过。
			ActiveVersion: "v2", HasVersion: true, TrafficPercent: 0, HasTraffic: true,
		}); err != nil {
			t.Fatalf("Report %d: %v", i, err)
		}
	}
	got := mustGet(t, e, p.ID)
	t.Logf("after 100 reports with cap=%d: observations=%d seenReports=%d steps=%d",
		lim.MaxObservations, len(got.Observations), len(got.SeenReports), len(got.Steps))
	if len(got.Observations) > lim.MaxObservations || len(got.SeenReports) > lim.MaxObservations {
		t.Fatalf("state grew unbounded: obs=%d seen=%d cap=%d",
			len(got.Observations), len(got.SeenReports), lim.MaxObservations)
	}
	if len(got.Steps) != 3 {
		t.Fatalf("batch count must be fixed at creation: got %d", len(got.Steps))
	}
	if len(got.Verdicts) > verdictCap {
		t.Fatalf("verdicts unbounded: %d", len(got.Verdicts))
	}
}

// v1 旧数据：结构可兼容的计划可迁移继续推进（旧观测被清、当前阶段重新门控）；
// 结构性不兼容必须以可区分原因拒绝，不按缺省值解释。
func TestV1MigrationAndRejection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")

	// 构造一个 v1 的进行中计划（无 Step.Version / Plan.Gate / 新观测字段）。
	v1JSON := `{
	  "version": 1,
	  "plans": [{
	    "id":"old1","target":"svc-old","strategy":"canary",
	    "fromVersion":"v1","toVersion":"v2","totalReplicas":10,
	    "steps":[{"index":0,"name":"canary-10%","percent":10,"replicas":1},
	             {"index":1,"name":"canary-50%","percent":50,"replicas":5},
	             {"index":2,"name":"canary-100%","percent":100,"replicas":10}],
	    "currentStep":1,"state":"Running",
	    "health":{"maxErrorRate":0.05,"failureBudget":3,"observationMaxAge":60000000000,"minSamples":10},
	    "failStreak":0,
	    "observations":[{"reportID":"ancient","reportedAt":"2020-01-01T00:00:00Z","replicas":5,"errorRate":0,"samples":100,"hasMetrics":true}],
	    "seenReports":["ancient"],"appliedCmds":["create","adv-0","adv-1"],"verdicts":[]
	  }]
	}`
	if err := os.WriteFile(path, []byte(v1JSON), 0o644); err != nil {
		t.Fatal(err)
	}
	sim := NewSimApplier()
	sim.StepReplicas = 0
	e, err := NewEngine(NewFileStore(path), sim, testLimits())
	if err != nil {
		t.Fatalf("compatible v1 should migrate, got %v", err)
	}
	defer e.Close()
	got := mustGet(t, e, "old1")
	t.Logf("migrated: state=%s current=%d gate=%+v observations=%d",
		got.State, got.CurrentStep, got.Gate, len(got.Observations))
	for i, st := range got.Steps {
		if st.Version != "v2" {
			t.Fatalf("step %d version not backfilled: %q", i, st.Version)
		}
	}
	if len(got.Observations) != 0 {
		t.Fatalf("v1 observations must be dropped, got %d", len(got.Observations))
	}
	if got.Gate.CompletedStep != 0 || got.Gate.GateStep != -1 {
		t.Fatalf("in-flight step must re-enter gate: %+v", got.Gate)
	}
	// 迁移后再推进：必须从当前阶段重新按三维门控确认（旧命令仍幂等拒绝）。
	if _, err := e.Advance(context.Background(), "adv-1", "old1"); !errors.Is(err, ErrDuplicateCommand) {
		t.Fatalf("old cmd idempotency must survive migration: got %v", err)
	}
	stop := startReporter(t, e, sim, got.Target, got.ID)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if _, err := e.Advance(ctx, "adv-1-retry", got.ID); err != nil {
		t.Fatalf("re-gate current step after migration: %v", err)
	}
	cancel()
	t.Log("v1 plan re-gated current step on fresh observations and continues")

	// 写回后应为 v2。
	e.mu.Lock()
	data, _ := os.ReadFile(path)
	e.mu.Unlock()
	if !strings.Contains(string(data), `"version": 2`) {
		t.Fatalf("store not upgraded to v2 on save")
	}

	// 结构性不兼容的 v1（空步骤）必须明确拒绝。
	badPath := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(badPath, []byte(`{"version":1,"plans":[{"id":"x","steps":null}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = NewEngine(NewFileStore(badPath), NewSimApplier(), testLimits())
	t.Logf("incompatible v1 load err=%v", err)
	if !errors.Is(err, ErrStoreVersion) {
		t.Fatalf("want ErrStoreVersion for incompatible v1, got %v", err)
	}
}

// batch 断点恢复：推进到中途“重启”，从已确认位点继续，批次既不重放也不丢失。
func TestBatchRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	e1, sim := newEngine(t, dir)
	p, err := e1.Create("c1", newBatchPlan("br1", 10, []int{2, 3, 5}))
	if err != nil {
		t.Fatal(err)
	}
	stop1 := startReporter(t, e1, sim, p.Target, p.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if _, err := e1.Advance(ctx, "adv-0", p.ID); err != nil {
		t.Fatalf("Advance 0: %v", err)
	}
	cancel()
	stop1()
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("=== batch plan crash & restart ===")

	e2, err := NewEngine(NewFileStore(filepath.Join(dir, "store.json")), sim, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	got := mustGet(t, e2, p.ID)
	t.Logf("recovered: state=%s currentStep=%d gate=%+v", got.State, got.CurrentStep, got.Gate)
	if got.State != StateRunning || got.CurrentStep != 0 || got.Gate.CompletedStep != 0 || got.Gate.GateStep != -1 {
		t.Fatalf("unexpected recovery state: %+v", got)
	}
	if len(got.Steps) != 3 {
		t.Fatalf("batch steps lost on restart: %d", len(got.Steps))
	}
	// 已确认的批次不会因重启再放一次量；新批次从 step1 开始。
	stop2 := startReporter(t, e2, sim, p.Target, p.ID)
	defer stop2()
	for i := 1; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		after, err := e2.Advance(ctx, fmt.Sprintf("adv-%d", i), p.ID)
		cancel()
		if err != nil {
			t.Fatalf("Advance %d after restart: %v", i, err)
		}
		t.Logf("after restart step %d: completed=%d state=%s", i, after.Gate.CompletedStep, after.State)
	}
	if got := mustGet(t, e2, p.ID); got.State != StateSucceeded || got.Gate.CompletedStep != 2 {
		t.Fatalf("want Succeeded with completed=2, got state=%s completed=%d",
			got.State, got.Gate.CompletedStep)
	}
}
