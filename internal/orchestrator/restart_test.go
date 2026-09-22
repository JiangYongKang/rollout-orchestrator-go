package orchestrator

import (
	"context"
	"errors"
	"log"
	"path/filepath"
	"testing"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/actuator"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/store"
)

func spawn(t *testing.T, dir string) (*Service, *actuator.Actuator) {
	t.Helper()
	st, err := store.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	act := actuator.New(dir)
	logger := log.New(&testLogWriter{t: t, prefix: ""}, "", 0)
	svc := NewService(filepath.Join(dir, "data"), st, act, logger)
	return svc, act
}

// 回滚副作用被延迟，进程在 RollingBack 中被杀：
// 重启 Recover 必须幂等推进到 RolledBack，且目标只回滚一次。
func TestRestartDuringDelayedRollback(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	{
		svc, act := spawn(t, dir)
		if _, err := svc.Create(ctx, CreateParams{ID: "cr1", TargetID: "tcr", Strategy: domain.StrategyCanary,
			DesiredVer: "v2", StableVer: "v1", TotalReplicas: 100, Config: domain.DefaultConfig()}); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.Advance(ctx, "cr1"); err != nil {
			t.Fatal(err)
		}
		// 让回滚 token 第一次延迟（不生效）。
		act.SetLagN("tcr", 1)
		p, err := svc.Rollback(ctx, "cr1", "manual rollback test")
		if err != nil || p.State != domain.StateRollingBack {
			t.Fatalf("lagged rollback: err=%v state=%s", err, p.State)
		}
		snap, _ := act.Snapshot(ctx, "tcr")
		if snap.Version != "v2" {
			t.Fatalf("lagged rollback must not restore yet, got %+v", snap)
		}
		t.Logf("proc1: rolling back delayed, actual=%+v state=%s", snap, p.State)

		// 重复回滚指令必须被明确拒绝。
		if _, err := svc.Rollback(ctx, "cr1", "dup"); !errors.Is(err, domain.ErrDuplicateCommand) {
			t.Fatalf("dup rollback: want ErrDuplicateCommand got %v", err)
		}
		_ = svc.Close()
	}

	tokensBefore := tokensFor(t, dir, "tcr")
	{
		svc, _ := spawn(t, dir)
		if err := svc.Recover(ctx); err != nil {
			t.Fatalf("recover: %v", err)
		}
		p, err := svc.Get(ctx, "cr1")
		if err != nil {
			t.Fatal(err)
		}
		if p.State != domain.StateRolledBack {
			t.Fatalf("after recover want RolledBack got %s", p.State)
		}
		if p.RollbackAttempts != 0 {
			t.Fatalf("rollback failures=%d want 0 (lag does not consume retry budget)", p.RollbackAttempts)
		}
		st := mustSnapshot(t, svc, "tcr")
		if st.Version != "v1" || st.Percent != 0 || st.Replicas != 0 {
			t.Fatalf("restored state wrong: %+v", st)
		}
		t.Logf("proc2 recover -> RolledBack, actual=%+v attempts=%d", st, p.RollbackAttempts)
		_ = svc.Close()
	}
	tokensAfter := tokensFor(t, dir, "tcr")
	rb := 0
	for _, tk := range tokensAfter {
		if len(tk) >= 8 && tk[:8] == "rollback" {
			rb++
		}
	}
	if rb != 1 {
		t.Fatalf("rollback side effects=%d before=%v after=%v, want exactly 1",
			rb, tokensBefore, tokensAfter)
	}
	t.Logf("exactly one rollback token across crash: %v", tokensAfter)
}

// 计划在各阶段被杀，重启后状态与事件结论一致，已完成副作用不重复执行。
func TestRestartStateConsistency(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	clk := &testClock{}
	clk.t = fixedNow()

	{
		svc, _ := spawn(t, dir)
		if _, err := svc.Create(ctx, CreateParams{ID: "cr2", TargetID: "tcr2", Strategy: domain.StrategyCanary,
			DesiredVer: "v2", StableVer: "v1", TotalReplicas: 100, Config: domain.DefaultConfig()}); err != nil {
			t.Fatal(err)
		}
		// 推进到第 0 步 Promoting 后被杀。
		if _, err := svc.Advance(ctx, "cr2"); err != nil {
			t.Fatal(err)
		}
		p, _ := svc.Get(ctx, "cr2")
		if p.State != domain.StatePromoting || p.Steps[0].AppliedToken == "" {
			t.Fatalf("pre-crash setup: state=%s applied=%q", p.State, p.Steps[0].AppliedToken)
		}
		before := tokensFor(t, dir, "tcr2")
		_ = svc.Close()

		// 重启：Recover 不得重复执行已生效副作用。
		svc2, _ := spawn(t, dir)
		if err := svc2.Recover(ctx); err != nil {
			t.Fatalf("recover: %v", err)
		}
		after := tokensFor(t, dir, "tcr2")
		if len(after) != len(before) {
			t.Fatalf("recover re-executed side effects: before=%v after=%v", before, after)
		}
		p2, _ := svc2.Get(ctx, "cr2")
		if p2.State != domain.StatePromoting || p2.StepIdx != 0 {
			t.Fatalf("recovered plan changed unexpectedly: state=%s idx=%d", p2.State, p2.StepIdx)
		}
		// 仍可从一致位点继续：上报收敛观测后正常进入 Gated。
		o := domain.Observation{
			BatchID: "post-restart-1", ObservedAt: fixedNow(),
			Actual:  domain.ActualState{Version: "v2", Percent: 5, Replicas: 5, UpdatedAt: fixedNow()},
			Metrics: healthyMetricsAt(fixedNow()),
		}
		if _, err := svc2.Report(ctx, "cr2", o); err != nil {
			t.Fatalf("post-recover report: %v", err)
		}
		p2, _ = svc2.Get(ctx, "cr2")
		if p2.State != domain.StateGated {
			t.Fatalf("want Gated after post-restart convergence, got %s", p2.State)
		}
		t.Logf("proc2 recovered Promoting without duplicate effects tokens=%v -> Gated", after)
		_ = svc2.Close()
	}
}
