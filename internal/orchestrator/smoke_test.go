package orchestrator

import (
	"context"
	"errors"
	"io"
	"log"
	"path/filepath"
	"testing"
	"time"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/actuator"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/store"
)

type testClock struct{ t time.Time }

func newFixture(t *testing.T) (*Service, *actuator.Actuator, *testClock) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	act := actuator.New(dir)
	logger := log.New(io.Discard, "", 0)
	clk := &testClock{t: time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)}
	svc := NewService(filepath.Join(dir, "data"), st, act, logger).WithClock(func() time.Time { return clk.t })
	t.Cleanup(func() { _ = svc.Close(); _ = act.Close() })
	return svc, act, clk
}

func healthyMetrics() map[string]domain.Metric {
	return map[string]domain.Metric{
		"error_rate":     {Name: "error_rate", Value: 0.01, Exists: true},
		"latency_p99_ms": {Name: "latency_p99_ms", Value: 120, Exists: true},
	}
}

func obs(clk *testClock, batch string, ver string, pct, replicas int) domain.Observation {
	return domain.Observation{
		BatchID:    batch,
		ObservedAt: clk.t,
		Actual:     domain.ActualState{Version: ver, Percent: pct, Replicas: replicas, UpdatedAt: clk.t},
		Metrics:    healthyMetrics(),
	}
}

func mustStep(t *testing.T, p *domain.Plan, want domain.PlanState) {
	t.Helper()
	if p.State != want {
		t.Fatalf("state=%s want=%s events=%d", p.State, want, len(p.Events))
	}
}

func TestSmokeCanaryHappyPath(t *testing.T) {
	svc, act, clk := newFixture(t)
	ctx := context.Background()
	if err := svc.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	p, err := svc.Create(ctx, CreateParams{
		ID: "p1", TargetID: "tg", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 100, Config: domain.DefaultConfig(),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	mustStep(t, p, domain.StatePending)

	// 同一 target 不能有第二个活动计划。
	if _, err := svc.Create(ctx, CreateParams{
		ID: "p2", TargetID: "tg", Strategy: domain.StrategyCanary,
		DesiredVer: "v3", StableVer: "v1", TotalReplicas: 100, Config: domain.DefaultConfig(),
	}); !errors.Is(err, domain.ErrTargetBusy) {
		t.Fatalf("want ErrTargetBusy, got %v", err)
	}

	desired := []int{5, 25, 50, 100}
	for i, pct := range desired {
		p, err = svc.Advance(ctx, "p1")
		if err != nil {
			t.Fatalf("advance %d: %v", i, err)
		}
		if p.State != domain.StatePromoting {
			t.Fatalf("step %d state=%s want Promoting", i, p.State)
		}
		// 上报收敛观测（5% 档允许抖动，这里给精确值）。
		p, err = svc.Report(ctx, "p1", obs(clk, batch(i, "a"), "v2", pct, pct))
		if err != nil {
			t.Fatalf("report converge %d: %v", i, err)
		}
		if i == len(desired)-1 {
			mustStep(t, p, domain.StateSucceeded)
			break
		}
		mustStep(t, p, domain.StateGated)
		// 门控通过：同批观测不能重复用，必须来一批新的确认观测。
		if _, err = svc.Report(ctx, "p1", obs(clk, batch(i, "a"), "v2", pct, pct)); !errors.Is(err, domain.ErrDuplicateCommand) {
			t.Fatalf("dup batch: want ErrDuplicateCommand got %v", err)
		}
		p, err = svc.Report(ctx, "p1", obs(clk, batch(i, "b"), "v2", pct, pct))
		if err != nil {
			t.Fatalf("gate report %d: %v", i, err)
		}
		mustStep(t, p, domain.StatePromoting)
	}

	// 成功后所有命令必须被拒绝。
	if _, err := svc.Advance(ctx, "p1"); !errors.Is(err, domain.ErrPlanFinished) {
		t.Fatalf("advance finished: %v", err)
	}
	if _, err := svc.Pause(ctx, "p1"); !errors.Is(err, domain.ErrPlanFinished) {
		t.Fatalf("pause finished: %v", err)
	}
	// 副作用 token：baseline + 4 个步骤，没有重复。
	tokens, err := act.EffectiveTokens("tg")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("applied tokens: %v", tokens)
	if len(tokens) != 5 {
		t.Fatalf("tokens=%v want 5 unique", tokens)
	}
	_ = clk
}

func batch(i int, suffix string) string {
	return "b-" + string(rune('0'+i)) + "-" + suffix
}
