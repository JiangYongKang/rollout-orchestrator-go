package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

// 非法状态迁移、重复指令、对已结束计划的操作：全部必须被可区分地拒绝。
func TestInvalidTransitionsAndDuplicates(t *testing.T) {
	svc, _, clk := newFixture(t)
	ctx := context.Background()

	// 非法参数：不能用零值配置推进。
	_, err := svc.Create(ctx, CreateParams{ID: "bad", TargetID: "t", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 10, Config: domain.Config{}})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("zero config: want ErrInvalidArgument got %v", err)
	}
	_, err = svc.Create(ctx, CreateParams{ID: "bad2", TargetID: "t", Strategy: domain.StrategyCanary,
		DesiredVer: "v1", StableVer: "v1", TotalReplicas: 10, Config: domain.DefaultConfig()})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("same version: want ErrInvalidArgument got %v", err)
	}
	if _, err := svc.Get(ctx, "nope"); !errors.Is(err, domain.ErrPlanNotFound) {
		t.Fatalf("missing: want ErrPlanNotFound got %v", err)
	}

	p, err := svc.Create(ctx, CreateParams{ID: "p", TargetID: "t", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 100, Config: domain.DefaultConfig()})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("created plan=%s state=%s", p.ID, p.State)

	// 重复创建同一 plan ID。
	if _, err := svc.Create(ctx, CreateParams{ID: "p", TargetID: "t2", Strategy: domain.StrategyCanary,
		DesiredVer: "v3", StableVer: "v1", TotalReplicas: 100, Config: domain.DefaultConfig()}); !errors.Is(err, domain.ErrPlanAlreadyExists) {
		t.Fatalf("dup create: want ErrPlanAlreadyExists got %v", err)
	}

	// Pending 下：Resume 非法（未暂停）、上报非法（还没放量）、暂停后再暂停非法。
	if _, err := svc.Resume(ctx, "p"); !errors.Is(err, domain.ErrNotPaused) {
		t.Fatalf("resume pending: want ErrNotPaused got %v", err)
	}
	if _, err := svc.Report(ctx, "p", obs(clk, "x1", "v2", 5, 5)); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("report pending: want ErrInvalidTransition got %v", err)
	}
	if p, err = svc.Pause(ctx, "p"); err != nil || p.State != domain.StatePaused {
		t.Fatalf("pause: %v state=%s", err, p.State)
	}
	t.Logf("transition Pending -> Paused resume_from=%s", p.ResumeFrom)
	if _, err := svc.Pause(ctx, "p"); !errors.Is(err, domain.ErrAlreadyPaused) {
		t.Fatalf("pause again: want ErrAlreadyPaused got %v", err)
	}
	// 暂停期间不允许 Advance / Report。
	if _, err := svc.Advance(ctx, "p"); !errors.Is(err, domain.ErrPlanPaused) {
		t.Fatalf("advance paused: want ErrPlanPaused got %v", err)
	}
	if _, err := svc.Report(ctx, "p", obs(clk, "x2", "v2", 5, 5)); !errors.Is(err, domain.ErrPlanPaused) {
		t.Fatalf("report paused: want ErrPlanPaused got %v", err)
	}
	if p, err = svc.Resume(ctx, "p"); err != nil || p.State != domain.StatePending {
		t.Fatalf("resume: %v state=%s", err, p.State)
	}
	t.Logf("transition Paused -> %s", p.State)

	// 进入 Promoting；无观测时重复 Advance 是合法的对账重试，但状态不越级。
	p, err = svc.Advance(ctx, "p")
	if err != nil || p.State != domain.StatePromoting {
		t.Fatalf("advance to promoting: %v state=%s", err, p.State)
	}
	p2, err := svc.Advance(ctx, "p")
	if err != nil || p2.State != domain.StatePromoting {
		t.Fatalf("idempotent re-advance: %v state=%s", err, p2.State)
	}
	if p2.Steps[0].AppliedToken != p.Steps[0].AppliedToken {
		t.Fatalf("re-advance must not create a second side effect")
	}
	t.Logf("re-advance kept token=%s (no duplicate promotion)", p2.Steps[0].AppliedToken)

	// Promoting 阶段上报缺失 BatchID 被拒绝。
	bad := obs(clk, "", "v2", 5, 5)
	if _, err := svc.Report(ctx, "p", bad); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("empty batch: want ErrInvalidArgument got %v", err)
	}

	// 推进到成功，再验证终态操作全部被拒绝。
	finishCanary(t, svc, "p", clk)
	got, err := svc.Get(ctx, "p")
	if err != nil {
		t.Fatal(err)
	}
	mustStep(t, got, domain.StateSucceeded)
	for _, op := range []struct {
		name string
		fn   func() (*domain.Plan, error)
	}{
		{"advance", func() (*domain.Plan, error) { return svc.Advance(ctx, "p") }},
		{"pause", func() (*domain.Plan, error) { return svc.Pause(ctx, "p") }},
		{"resume", func() (*domain.Plan, error) { return svc.Resume(ctx, "p") }},
		{"report", func() (*domain.Plan, error) { return svc.Report(ctx, "p", obs(clk, "late", "v2", 100, 100)) }},
	} {
		if _, err := op.fn(); !errors.Is(err, domain.ErrPlanFinished) {
			t.Fatalf("%s on finished: want ErrPlanFinished got %v", op.name, err)
		}
	}
	t.Log("all operations on finished plan rejected with ErrPlanFinished")

	// 回滚成功后的计划也必须拒绝所有后续操作。
	rp, err := svc.Create(ctx, CreateParams{ID: "pr", TargetID: "tr", Strategy: domain.StrategyWave,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 10, Config: domain.DefaultConfig()})
	if err != nil {
		t.Fatal(err)
	}
	if rp, err = svc.Rollback(ctx, "pr", "manual before start"); err != nil || rp.State != domain.StateRolledBack {
		t.Fatalf("rollback pending: %v state=%s", err, rp.State)
	}
	t.Logf("plan pr -> RolledBack reason=%q", "manual before start")
	if _, err := svc.Rollback(ctx, "pr", "again"); !errors.Is(err, domain.ErrPlanFinished) {
		t.Fatalf("rollback rolled-back: want ErrPlanFinished got %v", err)
	}
	if _, err := svc.Advance(ctx, "pr"); !errors.Is(err, domain.ErrPlanFinished) {
		t.Fatalf("advance rolled-back: want ErrPlanFinished got %v", err)
	}
	_ = fmt.Sprint
}
