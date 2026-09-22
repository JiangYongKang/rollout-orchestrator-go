package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

// 同一 plan 上的并发 Advance 必须串行一致：
// 不得出现半更新、重复放量、状态越级。
func TestConcurrentAdvanceSamePlan(t *testing.T) {
	svc, act, _ := newFixture(t)
	ctx := context.Background()
	_, _ = svc.Create(ctx, CreateParams{ID: "c1", TargetID: "tc", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 100, Config: domain.DefaultConfig()})

	var wg sync.WaitGroup
	var ok, failed int64
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := svc.Advance(ctx, "c1")
			if err != nil {
				atomic.AddInt64(&failed, 1)
				return
			}
			if p.State == domain.StatePromoting {
				atomic.AddInt64(&ok, 1)
			}
		}()
	}
	wg.Wait()

	p, _ := svc.Get(ctx, "c1")
	if p.State != domain.StatePromoting {
		t.Fatalf("state=%s want Promoting", p.State)
	}
	if p.StepIdx != 0 {
		t.Fatalf("step index jumped: %d", p.StepIdx)
	}
	if p.Steps[0].Attempts != 0 {
		t.Fatalf("unexpected failure attempts: %d", p.Steps[0].Attempts)
	}
	// 30 次并发推进，真正生效的第 0 步副作用必须恰好一个。
	tokens, _ := act.EffectiveTokens("tc")
	stepApplied := 0
	for _, tk := range tokens {
		if len(tk) > 4 && tk[:4] == "step" {
			stepApplied++
		}
	}
	if stepApplied != 1 {
		t.Fatalf("effective step side effects=%d tokens=%v, want exactly 1", stepApplied, tokens)
	}
	t.Logf("30 concurrent advances: state=%s, effective tokens=%v (exactly one promotion)",
		p.State, tokens)
}

// 同一 target 并发创建多个计划：恰好一个成功，其余全部 ErrTargetBusy。
func TestConcurrentCreateSameTarget(t *testing.T) {
	svc, _, _ := newFixture(t)
	ctx := context.Background()

	const n = 20
	var wg sync.WaitGroup
	var success, busy, other int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("cp-%02d", i)
			_, err := svc.Create(ctx, CreateParams{
				ID: id, TargetID: "shared", Strategy: domain.StrategyCanary,
				DesiredVer: "v" + id, StableVer: "v0", TotalReplicas: 100, Config: domain.DefaultConfig(),
			})
			switch {
			case err == nil:
				atomic.AddInt64(&success, 1)
			case isTargetBusy(err):
				atomic.AddInt64(&busy, 1)
			default:
				atomic.AddInt64(&other, 1)
				t.Errorf("unexpected create error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if success != 1 || busy != n-1 || other != 0 {
		t.Fatalf("success=%d busy=%d other=%d", success, busy, other)
	}
	t.Logf("%d concurrent creates on one target: 1 success, %d ErrTargetBusy, no half-state",
		n, busy)
}

// 并发观测上报：只有一个批次胜出并驱动迁移，其余按重复批次拒绝。
func TestConcurrentReportDedup(t *testing.T) {
	svc, _, clk := newFixture(t)
	ctx := context.Background()
	_, _ = svc.Create(ctx, CreateParams{ID: "c2", TargetID: "tc2", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 100, Config: domain.DefaultConfig()})
	if _, err := svc.Advance(ctx, "c2"); err != nil {
		t.Fatal(err)
	}

	const n = 25
	var wg sync.WaitGroup
	var accepted, dup int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.Report(ctx, "c2", obs(clk, "same-batch", "v2", 5, 5))
			if err != nil {
				if isDup(err) {
					atomic.AddInt64(&dup, 1)
					return
				}
				t.Errorf("unexpected report error: %v", err)
				return
			}
			atomic.AddInt64(&accepted, 1)
		}()
	}
	wg.Wait()
	if accepted != 1 || dup != n-1 {
		t.Fatalf("accepted=%d dup=%d", accepted, dup)
	}
	p, _ := svc.Get(ctx, "c2")
	if p.State != domain.StateGated {
		t.Fatalf("state=%s want Gated (exactly one report advanced)", p.State)
	}
	if p.BudgetLeft != domain.DefaultConfig().FailureBudget {
		t.Fatalf("budget drifted: %d", p.BudgetLeft)
	}
	t.Logf("%d concurrent identical reports: 1 accepted -> Gated, %d rejected duplicates; budget unchanged=%d",
		n, dup, p.BudgetLeft)
}

func isTargetBusy(err error) bool {
	for err != nil {
		if err == domain.ErrTargetBusy {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func isDup(err error) bool {
	for err != nil {
		if err == domain.ErrDuplicateCommand {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
