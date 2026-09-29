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

// TestTargetBusyRejectsSecondPlan 同一目标在发布结束前只允许一个进行中计划，
// 拒绝原因必须是 ErrTargetBusy，与参数错误/非法状态区分；结束后允许新建。
func TestTargetBusyRejectsSecondPlan(t *testing.T) {
	dir := t.TempDir()
	e, _ := newEngine(t, dir)
	defer e.Close()

	p1 := newPlan("t1", StrategyCanary)
	p1.Target = "shared-target"
	if _, err := e.Create("c1", p1); err != nil {
		t.Fatalf("Create p1: %v", err)
	}

	// 同目标、不同计划 ID：被拒。
	p2 := newPlan("t2", StrategyCanary)
	p2.Target = "shared-target"
	_, err := e.Create("c2", p2)
	t.Logf("create second plan on same target -> %v", err)
	if !errors.Is(err, ErrTargetBusy) {
		t.Fatalf("want ErrTargetBusy, got %v", err)
	}
	// 被拒计划不得落库。
	if _, gerr := e.Get("t2"); !errors.Is(gerr, ErrNotFound) {
		t.Fatalf("rejected plan must not be stored: %v", gerr)
	}

	// 参数错误必须与目标忙区分。
	bad := newPlan("t3", StrategyCanary)
	bad.Target = "shared-target"
	bad.TotalReplicas = 0
	if _, err := e.Create("c3", bad); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument distinct from busy, got %v", err)
	}
	t.Log("invalid argument still reported as ErrInvalidArgument while target busy")

	// 不同目标可同时存在。
	other := newPlan("t4", StrategyCanary)
	other.Target = "other-target"
	if _, err := e.Create("c4", other); err != nil {
		t.Fatalf("create on different target: %v", err)
	}

	// 回滚结束 p1 后，同目标可再建新计划（终态释放目标）。
	if _, err := e.Rollback("rb1", "t1"); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	p5 := newPlan("t5", StrategyBatch)
	p5.Target = "shared-target"
	p5.BatchSizes = []int{2}
	if _, err := e.Create("c5", p5); err != nil {
		t.Fatalf("create after terminal should succeed: %v", err)
	}
	t.Log("target released after terminal state, new plan accepted")
}

// TestTargetBusyConcurrentCreate 同一目标并发创建：恰好一个成功，其余 ErrTargetBusy。
func TestTargetBusyConcurrentCreate(t *testing.T) {
	dir := t.TempDir()
	e, _ := newEngine(t, dir)
	defer e.Close()

	const n = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ok, busy, other int
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pl := newPlan(fmt.Sprintf("cc-%d", i), StrategyCanary)
			pl.Target = "race-target"
			_, err := e.Create(fmt.Sprintf("cmd-%d", i), pl)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case errors.Is(err, ErrTargetBusy):
				busy++
			default:
				other++
				t.Errorf("unexpected create err: %v", err)
			}
		}(i)
	}
	wg.Wait()
	t.Logf("concurrent same-target creates: ok=%d busy=%d other=%d", ok, busy, other)
	if ok != 1 || busy != n-1 || other != 0 {
		t.Fatalf("want exactly 1 ok and %d busy, got ok=%d busy=%d other=%d", n-1, ok, busy, other)
	}
}

// TestSameTargetOpsSerialNoDoubleRollout 同一目标上的并发推进严格串行，
// 不出现重复放量、半更新或状态跳级。
func TestSameTargetOpsSerialNoDoubleRollout(t *testing.T) {
	dir := t.TempDir()
	// 不挂自动上报器：阶段应用后无法通过门控，Advance 会在短 ctx 后停在在途态，
	// 正好暴露“并发推进是否重复放量/跳阶段”。
	sim := NewSimApplier()
	sim.StepReplicas = 0
	e, err := NewEngine(NewFileStore(filepath.Join(dir, "s.json")), sim, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	pl := newPlan("ser1", StrategyBatch)
	pl.BatchSizes = []int{2} // 单阶段
	p, _ := e.Create("c1", pl)

	// 5 个并发 Advance：tmu 串行，只有第一个真正应用期望（放量一次）进入在途，
	// 其余看到 Running+在途后同样在等待循环里阻塞，随后因 ctx 超时返回。
	var wg sync.WaitGroup
	errs := make([]error, 5)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
			defer cancel()
			_, errs[i] = e.Advance(ctx, fmt.Sprintf("ser-adv-%d", i), p.ID)
		}(i)
	}
	wg.Wait()
	var timedout int
	for i, err := range errs {
		t.Logf("serial advance %d: %v", i, err)
		if errors.Is(err, ErrNotConverged) {
			timedout++
		}
	}
	got := mustGet(t, e, p.ID)
	t.Logf("after concurrent advances: state=%s current=%d pending=%d gateSeq=%d (timedout=%d)",
		got.State, got.CurrentStep, got.PendingStep, got.GateSeq, timedout)
	// 关键不变量：只放量一次 -> 在途阶段固定为 0，没有进入下一阶段/成功。
	if got.State != StateRunning || got.PendingStep != 0 || got.CurrentStep != -1 {
		t.Fatalf("double rollout / state jump: state=%s current=%d pending=%d",
			got.State, got.CurrentStep, got.PendingStep)
	}
	// 模拟环境期望副本只被设置为单批 2（无重复/加倍）。
	so, _ := sim.Observe(p.Target)
	if so.Replicas != 2 {
		t.Fatalf("desired replicas applied more than once? got %d", so.Replicas)
	}

	// 喂一条满足门控的观测，重入推进使单阶段成功；之后并发推进全部终态拒绝。
	if err := e.Report(p.ID, obs("ser-ok", 20, 2)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if _, err := e.Advance(ctx, "ser-finish", p.ID); err != nil {
		t.Fatalf("finish single step: %v", err)
	}
	cancel()
	for i := 0; i < 3; i++ {
		if _, err := e.Advance(context.Background(), fmt.Sprintf("ser-term-%d", i), p.ID); !errors.Is(err, ErrPlanFinished) {
			t.Fatalf("terminal advance: want ErrPlanFinished, got %v", err)
		}
	}
	fin := mustGet(t, e, p.ID)
	if fin.State != StateSucceeded || fin.PendingStep != -1 {
		t.Fatalf("want clean Succeeded, got state=%s pending=%d", fin.State, fin.PendingStep)
	}
}

// TestDifferentTargetsParallel 不同目标并行互不影响，各自推进到成功。
func TestDifferentTargetsParallel(t *testing.T) {
	dir := t.TempDir()
	e, sim := newEngine(t, dir)
	defer e.Close()

	const n = 4
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		pl := newPlan(fmt.Sprintf("par-%d", i), StrategyBatch)
		pl.Target = fmt.Sprintf("par-target-%d", i)
		pl.BatchSizes = []int{2, 8}
		created, err := e.Create(fmt.Sprintf("par-create-%d", i), pl)
		if err != nil {
			t.Fatal(err)
		}
		st := startReporter(t, e, sim, created.Target, created.ID)
		defer st()
		wg.Add(1)
		go func(p *Plan) {
			defer wg.Done()
			for s := 0; s < len(p.Steps); s++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := e.Advance(ctx, fmt.Sprintf("%s-a%d", p.ID, s), p.ID)
				cancel()
				if err != nil {
					t.Errorf("plan %s step %d: %v", p.ID, s, err)
					return
				}
			}
		}(created)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		got := mustGet(t, e, fmt.Sprintf("par-%d", i))
		t.Logf("parallel target %d: state=%s current=%d", i, got.State, got.CurrentStep)
		if got.State != StateSucceeded {
			t.Fatalf("par-%d: want Succeeded, got %s", i, got.State)
		}
	}
}

// TestTargetBusySurvivesRestart 进程重启后，同目标互斥约束照样生效。
func TestTargetBusySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	e1, _ := newEngine(t, dir)
	pl := newPlan("live1", StrategyCanary)
	pl.Target = "persist-target"
	if _, err := e1.Create("c1", pl); err != nil {
		t.Fatal(err)
	}
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := NewEngine(NewFileStore(filepath.Join(dir, "store.json")), NewSimApplier(), testLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	rival := newPlan("live2", StrategyCanary)
	rival.Target = "persist-target"
	_, err = e2.Create("c2", rival)
	t.Logf("create rival after restart on same target -> %v", err)
	if !errors.Is(err, ErrTargetBusy) {
		t.Fatalf("want ErrTargetBusy after restart, got %v", err)
	}
}
