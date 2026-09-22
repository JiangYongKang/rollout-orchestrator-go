package orchestrator

import (
	"context"
	"testing"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

// Gated 后：一批新版本回退（不收敛）的新观测不得污染门控依据，
// 原新鲜收敛观测仍可用于门控推进；回退本身被记录但不跳步。
func TestGatedBasisNotPollutedByRegression(t *testing.T) {
	svc, _, clk := newFixture(t)
	ctx := context.Background()
	_, _ = svc.Create(ctx, CreateParams{ID: "g1", TargetID: "tg", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 100, Config: domain.DefaultConfig()})
	if _, err := svc.Advance(ctx, "g1"); err != nil {
		t.Fatal(err)
	}
	// 收敛 -> Gated。
	p, err := svc.Report(ctx, "g1", obs(clk, "g-a", "v2", 5, 5))
	if err != nil || p.State != domain.StateGated {
		t.Fatalf("converge: err=%v state=%s", err, p.State)
	}
	goodBatch := p.LastBatchID

	// 新版本回退到 v1（实际短暂缺失/回滚），新批次被接受但不收敛。
	p, err = svc.Report(ctx, "g1", obs(clk, "g-regress", "v1", 0, 0))
	if err != nil {
		t.Fatalf("regression observation: %v", err)
	}
	if p.State != domain.StateGated {
		t.Fatalf("regression must not move state, got %s", p.State)
	}
	if p.LastBatchID != goodBatch || p.LastObs.Actual.Version != "v2" {
		t.Fatalf("gate basis polluted: last_batch=%s obs=%+v", p.LastBatchID, p.LastObs.Actual)
	}
	t.Logf("regression batch accepted as event but gate basis preserved: batch=%s actual=%s",
		p.LastBatchID, p.LastObs.Actual.Version)

	// 原门控依据仍然有效：Advance 通过健康门控进入下一步。
	p, err = svc.Advance(ctx, "g1")
	if err != nil {
		t.Fatalf("advance on preserved gate basis: %v", err)
	}
	if p.State != domain.StatePromoting || p.StepIdx != 1 {
		t.Fatalf("want step-1 Promoting got state=%s idx=%d", p.State, p.StepIdx)
	}
	t.Log("gate advanced using preserved fresh observation; no state skip/regression")
}

// 预算恰好为 0 时不回滚，-1 才回滚（边界）。
func TestBudgetBoundary(t *testing.T) {
	svc, _, clk := newFixture(t)
	ctx := context.Background()
	cfg := domain.DefaultConfig()
	cfg.FailureBudget = 0 // 第一次不健康即耗尽（left -> -1）
	_, _ = svc.Create(ctx, CreateParams{ID: "b0", TargetID: "tb0", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 100, Config: cfg})
	if _, err := svc.Advance(ctx, "b0"); err != nil {
		t.Fatal(err)
	}
	missing := healthyMetrics()
	delete(missing, "error_rate")
	o := obs(clk, "b0-1", "v2", 5, 5)
	o.Metrics = missing
	p, err := svc.Report(ctx, "b0", o)
	if err != nil {
		t.Fatalf("budget=0 unhealthy should auto-rollback with nil error, got %v", err)
	}
	if p.State != domain.StateRolledBack {
		t.Fatalf("budget crossing -1 must rollback, got %s", p.State)
	}
	t.Logf("failure_budget=0 + first unhealthy -> RolledBack (boundary correct)")
}

var _ = domain.StateGated
