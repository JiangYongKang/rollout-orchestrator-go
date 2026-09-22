package orchestrator

import (
	"context"
	"testing"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

func TestWaveExactReplicaSemantics(t *testing.T) {
	svc, _, clk := newFixture(t)
	ctx := context.Background()
	// 10 副本：10%->ceil=1, 25%->ceil=3, 50%->5, 100%->10（严格递增）。
	p, err := svc.Create(ctx, CreateParams{ID: "w1", TargetID: "tw", Strategy: domain.StrategyWave,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 10, Config: domain.DefaultConfig()})
	if err != nil {
		t.Fatal(err)
	}
	wantDesired := []int{1, 3, 5, 10}
	for i, d := range wantDesired {
		if p.Steps[i].Desired != d {
			t.Fatalf("step %d desired=%d want %d", i, p.Steps[i].Desired, d)
		}
	}
	t.Logf("wave steps desired replicas=%v", wantDesired)

	if p, err = svc.Advance(ctx, "w1"); err != nil {
		t.Fatal(err)
	}

	// 副本数差一个都不收敛（wave 无百分比容差，与 canary 语义不同）。
	o := domain.Observation{
		BatchID: "w-o1", ObservedAt: clk.t,
		Actual:  domain.ActualState{Version: "v2", Replicas: 2, Percent: 20, UpdatedAt: clk.t},
		Metrics: healthyMetrics(),
	}
	if p, err = svc.Report(ctx, "w1", o); err != nil {
		t.Fatal(err)
	}
	if p.State != domain.StatePromoting {
		t.Fatalf("2 replicas must not converge wave-0 (want 1): got %s", p.State)
	}
	t.Log("actual replicas=2 != desired batch=1 -> not converged (exact semantics)")

	// 精确等于 1 才收敛。
	o2 := domain.Observation{
		BatchID: "w-o2", ObservedAt: clk.t,
		Actual:  domain.ActualState{Version: "v2", Replicas: 1, Percent: 10, UpdatedAt: clk.t},
		Metrics: healthyMetrics(),
	}
	if p, err = svc.Report(ctx, "w1", o2); err != nil {
		t.Fatal(err)
	}
	if p.State != domain.StateGated {
		t.Fatalf("exact 1 replica must converge -> Gated, got %s", p.State)
	}
	// 门控确认后进入第二批，期望副本为 3。
	o3 := o2
	o3.BatchID = "w-o3"
	if p, err = svc.Report(ctx, "w1", o3); err != nil || p.State != domain.StatePromoting {
		t.Fatalf("gate: err=%v state=%s", err, p.State)
	}
	cur := p.CurrentStep()
	if cur.Desired != 3 || cur.Index != 1 {
		t.Fatalf("advanced to wrong wave: idx=%d desired=%d", cur.Index, cur.Desired)
	}
	t.Logf("gate passed -> wave batch idx=%d desired replicas=%d", cur.Index, cur.Desired)
}
