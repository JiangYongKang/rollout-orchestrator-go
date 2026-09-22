package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

func metricsWith(er, p99 float64, erOK, p99OK bool) map[string]domain.Metric {
	return map[string]domain.Metric{
		"error_rate":     {Name: "error_rate", Value: er, Exists: erOK},
		"latency_p99_ms": {Name: "latency_p99_ms", Value: p99, Exists: p99OK},
	}
}

func obsFull(clk *testClock, batch, ver string, pct, replicas int, m map[string]domain.Metric) domain.Observation {
	o := obs(clk, batch, ver, pct, replicas)
	o.Metrics = m
	return o
}

// 关键指标缺失 / 越界不得推进；预算耗尽自动回滚；重复上报不漂移。
func TestHealthGateBudgetAndAutoRollback(t *testing.T) {
	svc, act, clk := newFixture(t)
	ctx := context.Background()
	cfg := domain.DefaultConfig()
	cfg.FailureBudget = 1 // 第一次不健康被拒绝，第二次（新批次）触发自动回滚
	p, err := svc.Create(ctx, CreateParams{ID: "h1", TargetID: "th", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 100, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if p, err = svc.Advance(ctx, "h1"); err != nil {
		t.Fatal(err)
	}

	// 1) 关键指标缺失：error_rate 不存在。
	missing := metricsWith(0, 120, false, true)
	if p, err = svc.Report(ctx, "h1", obsFull(clk, "h-missing", "v2", 5, 5, missing)); !errors.Is(err, domain.ErrUnhealthy) {
		t.Fatalf("missing metric: want ErrUnhealthy got %v (state=%s)", err, p.State)
	}
	t.Logf("input missing metric -> state=%s budget_left=%d reasons=%q",
		p.State, p.BudgetLeft, lastReason(p))
	if p.State != domain.StatePromoting {
		t.Fatalf("must stay Promoting on missing metric, got %s", p.State)
	}

	// 2) 同一批次重复上报：ErrDuplicateCommand，预算不二次扣减。
	if _, err = svc.Report(ctx, "h1", obsFull(clk, "h-missing", "v2", 5, 5, missing)); !errors.Is(err, domain.ErrDuplicateCommand) {
		t.Fatalf("dup unhealthy batch: want ErrDuplicateCommand got %v", err)
	}
	p, _ = svc.Get(ctx, "h1")
	if p.BudgetLeft != 0 {
		t.Fatalf("duplicate batch consumed budget twice: left=%d", p.BudgetLeft)
	}
	t.Logf("duplicate unhealthy batch rejected; budget_left=%d unchanged", p.BudgetLeft)

	// 3) 新批次指标越界（error_rate=0.2 > 0.05）：预算耗尽 -> 自动回滚。
	bad := metricsWith(0.2, 120, true, true)
	p, err = svc.Report(ctx, "h1", obsFull(clk, "h-bad", "v2", 5, 5, bad))
	if err != nil {
		t.Fatalf("auto rollback may return nil, got %v", err)
	}
	if p.State != domain.StateRolledBack {
		t.Fatalf("want RolledBack got %s", p.State)
	}
	t.Logf("transition Promoting -> RollingBack -> RolledBack; budget_left=%d", p.BudgetLeft)

	// 4) 实际目标必须回到稳定版本，且回滚 token 只有一个。
	snap, err := act.Snapshot(ctx, "th")
	if err != nil {
		t.Fatal(err)
	}
	if snap.Version != "v1" || snap.Percent != 0 || snap.Replicas != 0 {
		t.Fatalf("actual not restored: %+v", snap)
	}
	tokens, _ := act.EffectiveTokens("th")
	rollbackTokens := 0
	for _, tk := range tokens {
		if len(tk) >= 8 && tk[:8] == "rollback" {
			rollbackTokens++
		}
	}
	if rollbackTokens != 1 {
		t.Fatalf("rollback side effects=%d tokens=%v, want exactly 1", rollbackTokens, tokens)
	}
	t.Logf("target restored to %+v; rollback tokens=%v", snap, tokens)

	// 5) 重复回滚指令 / 在途重复：终态后拒绝。
	if _, err = svc.Rollback(ctx, "h1", "again"); !errors.Is(err, domain.ErrPlanFinished) {
		t.Fatalf("rollback after terminal: want ErrPlanFinished got %v", err)
	}
}

// 观测过期不得用于收敛或门控。
func TestStaleObservationRejected(t *testing.T) {
	svc, _, clk := newFixture(t)
	ctx := context.Background()
	_, _ = svc.Create(ctx, CreateParams{ID: "s1", TargetID: "ts", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 100, Config: domain.DefaultConfig()})
	if _, err := svc.Advance(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	o := obs(clk, "old", "v2", 5, 5)
	o.ObservedAt = clk.t.Add(-6 * time.Second) // StaleThreshold=5s
	p, err := svc.Report(ctx, "s1", o)
	if !errors.Is(err, domain.ErrObservationStale) {
		t.Fatalf("stale report: want ErrObservationStale got %v", err)
	}
	t.Logf("stale observation age=6s -> rejected state=%s last_batch=%q", p.State, p.LastBatchID)
	if p.LastBatchID == "old" {
		t.Fatal("stale batch must not be accepted into LastBatchID")
	}

	// Gated 阶段：时钟前进使已接受观测过期，Advance 必须拒绝继续推进。
	clk.t = clk.t.Add(1 * time.Second)
	if p, err = svc.Report(ctx, "s1", obs(clk, "fresh-1", "v2", 5, 5)); err != nil {
		t.Fatalf("fresh converge: %v", err)
	}
	if p.State != domain.StateGated {
		t.Fatalf("want Gated got %s", p.State)
	}
	clk.t = clk.t.Add(6 * time.Second)
	if _, err := svc.Advance(ctx, "s1"); !errors.Is(err, domain.ErrObservationStale) {
		t.Fatalf("advance with stale gated obs: want ErrObservationStale got %v", err)
	}
	t.Log("gated advance with expired observation rejected")
}

// 实际状态短暂缺失/版本回退：不得收敛，状态保持 Promoting，不跳步、不重复放量。
func TestLagAndActualMissingNoSkip(t *testing.T) {
	svc, act, clk := newFixture(t)
	ctx := context.Background()
	_, _ = svc.Create(ctx, CreateParams{ID: "l1", TargetID: "tl", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 100, Config: domain.DefaultConfig()})

	// 先让第 0 步副作用延迟一次。
	act.SetLagN("tl", 1)
	p, err := svc.Advance(ctx, "l1")
	if err != nil {
		t.Fatal(err)
	}
	if p.State != domain.StatePending {
		t.Fatalf("lagged first apply must keep Pending, got %s", p.State)
	}
	snap, _ := act.Snapshot(ctx, "tl")
	if snap.Version != "v1" {
		t.Fatalf("lagged apply must not change actual: %+v", snap)
	}
	t.Logf("lag #1 kept plan Pending, actual=%+v", snap)

	// 再次 Advance：新 token 生效 -> Promoting。
	p, err = svc.Advance(ctx, "l1")
	if err != nil || p.State != domain.StatePromoting {
		t.Fatalf("retry after lag: %v state=%s", err, p.State)
	}
	tokens, _ := act.AppliedTokens("tl")
	t.Logf("lag retry produced Promoting; tokens=%v", tokens)

	// 观测仍是旧版本/缺失新版本：不收敛。
	p, err = svc.Report(ctx, "l1", obs(clk, "lag-a", "v1", 0, 0))
	if err != nil {
		t.Fatalf("lagging observation should be accepted, got %v", err)
	}
	if p.State != domain.StatePromoting {
		t.Fatalf("lagging actual must not advance state, got %s", p.State)
	}
	t.Log("actual still v1 -> not converged, stayed Promoting")

	// 新批次到正确版本但百分比偏离（模拟抖动超出容差）：仍不收敛。
	p, err = svc.Report(ctx, "l1", obs(clk, "lag-b", "v2", 9, 9))
	if err != nil {
		t.Fatalf("jitter report: %v", err)
	}
	if p.State != domain.StatePromoting {
		t.Fatalf("jitter outside tolerance must not converge, got %s", p.State)
	}
	// 抖动落在容差内：5±2 => 6 收敛。
	p, err = svc.Report(ctx, "l1", obs(clk, "lag-c", "v2", 6, 6))
	if err != nil {
		t.Fatalf("within-tolerance report: %v", err)
	}
	if p.State != domain.StateGated {
		t.Fatalf("jitter inside tolerance must converge -> Gated, got %s", p.State)
	}
	t.Logf("actual 5->6 within tolerance ±2 -> Gated; steps advanced=%d", p.StepIdx)

	// 该步放量副作用只执行了一次（lag token 未生效 + 成功 token 生效）。
	appliedStep := 0
	for _, tk := range tokens {
		if len(tk) > 4 && tk[:4] == "step" {
			appliedStep++
		}
	}
	if appliedStep != 2 {
		t.Fatalf("expected 2 step tokens (1 lagged + 1 applied), got %d: %v", appliedStep, tokens)
	}
}

func lastReason(p *domain.Plan) string {
	if len(p.Events) == 0 {
		return ""
	}
	return p.Events[len(p.Events)-1].Reason
}
