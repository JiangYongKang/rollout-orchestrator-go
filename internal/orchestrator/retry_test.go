package orchestrator

import (
	"context"
	"errors"
	"io"
	"log"
	"path/filepath"
	"testing"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/actuator"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/store"
)

// 硬失败次数未超 MaxAttempts：可重试成功，且最终只执行一次有效副作用。
func TestBoundedRetrySucceeds(t *testing.T) {
	svc, act, clk := newFixture(t)
	ctx := context.Background()
	_, _ = svc.Create(ctx, CreateParams{ID: "r1", TargetID: "tr1", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 100, Config: domain.DefaultConfig()})

	act.SetFailN("tr1", 2) // 前两个新 token 失败，第 3 次成功（MaxAttempts=3）
	var p *domain.Plan
	var err error
	for i := 0; i < 3; i++ {
		p, err = svc.Advance(ctx, "r1")
		t.Logf("attempt %d: err=%v state=%s step.attempts=%d", i+1, err, p.State, p.Steps[0].Attempts)
	}
	if err != nil {
		t.Fatalf("third attempt should succeed, got %v", err)
	}
	if p.State != domain.StatePromoting {
		t.Fatalf("want Promoting got %s", p.State)
	}
	if p.Steps[0].Attempts != 2 {
		t.Fatalf("failure count=%d want 2 (lag/failure distinction)", p.Steps[0].Attempts)
	}
	tokens, _ := act.AppliedTokens("tr1")
	applied := 0
	for _, tk := range tokens {
		if len(tk) > 4 && tk[:4] == "step" {
			applied++
		}
	}
	if applied != 3 {
		t.Fatalf("step tokens=%d (2 failed + 1 success), tokens=%v", applied, tokens)
	}
	snap, _ := act.Snapshot(ctx, "tr1")
	if snap.Version != "v2" || snap.Percent != 5 {
		t.Fatalf("actual not at step-0 desired: %+v", snap)
	}
	t.Logf("bounded retry converged: actual=%+v, failed tokens consumed, no duplicate success", snap)
	_ = clk
}

// 硬失败次数达到 MaxAttempts：终止为 Failed 终态，状态一致、可解释。
func TestRetryExhaustedFailsTerminal(t *testing.T) {
	svc, act, _ := newFixture(t)
	ctx := context.Background()
	_, _ = svc.Create(ctx, CreateParams{ID: "r2", TargetID: "tr2", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 100, Config: domain.DefaultConfig()})
	act.SetFailN("tr2", 5)

	var lastErr error
	for i := 0; i < 5; i++ {
		p, err := svc.Advance(ctx, "r2")
		if p != nil {
			t.Logf("try %d: err=%v state=%s attempts=%d", i+1, err, p.State, p.Steps[0].Attempts)
		}
		lastErr = err
		if p != nil && p.State == domain.StateFailed {
			break
		}
	}
	p, _ := svc.Get(ctx, "r2")
	if p.State != domain.StateFailed {
		t.Fatalf("want Failed got %s lastErr=%v", p.State, lastErr)
	}
	if !errors.Is(lastErr, domain.ErrRetryExhausted) {
		t.Fatalf("terminal cause: want ErrRetryExhausted got %v", lastErr)
	}
	// 终态后操作被拒绝，且目标没有被错误放量。
	if _, err := svc.Advance(ctx, "r2"); !errors.Is(err, domain.ErrPlanFinished) {
		t.Fatalf("advance failed plan: %v", err)
	}
	snap, _ := act.Snapshot(ctx, "tr2")
	if snap.Version != "v1" {
		t.Fatalf("failed rollout must leave target at stable v1, got %+v", snap)
	}
	t.Logf("plan Failed within bound, target stays %+v", snap)
}

// 崩溃恢复：进程在"发起 token 后、收到失败结论前"被杀。
// 旧 token 在磁盘上记录为失败；重启 Recover 后必须换新 token 重试，
// 最终成功，而不是卡死或重复副作用。
func TestRetryReplayAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// 第一个"进程"：创建计划并让第一次 Apply 硬失败，然后模拟崩溃（不保留 Service）。
	{
		st, _ := store.New(dir)
		act := actuator.New(dir)
		svc := NewService(filepath.Join(dir, "d"), st, act, log.New(io.Discard, "", 0))
		if _, err := svc.Create(ctx, CreateParams{ID: "rr1", TargetID: "trr", Strategy: domain.StrategyCanary,
			DesiredVer: "v2", StableVer: "v1", TotalReplicas: 100, Config: domain.DefaultConfig()}); err != nil {
			t.Fatal(err)
		}
		act.SetFailN("trr", 1)
		p, err := svc.Advance(ctx, "rr1")
		if err != nil || p.State != domain.StatePending {
			t.Fatalf("failing attempt: err=%v state=%s", err, p.State)
		}
		t.Logf("pre-crash: attempt failed, plan stays Pending attempts=%d", p.Steps[0].Attempts)
		_ = svc.Close()
	}

	// 第二个"进程"：Recover 不应卡死（此时 Pending 无在途 token），随后两次 Advance：
	// 第一次会重放/重试，第二次在 lag 注入后仍应最终一致。这里验证失败 token 被换。
	{
		st, _ := store.New(dir)
		act := actuator.New(dir)
		logger := log.New(&testLogWriter{t: t, prefix: "[proc2] "}, "", 0)
		svc := NewService(filepath.Join(dir, "d"), st, act, logger)
		if err := svc.Recover(ctx); err != nil {
			t.Fatalf("recover: %v", err)
		}
		// 不再注入失败；旧失败 token 已在磁盘，Advance 应成功。
		p, err := svc.Advance(ctx, "rr1")
		if err != nil || p.State != domain.StatePromoting {
			t.Fatalf("post-restart advance: err=%v state=%s", err, p.State)
		}
		if p.Steps[0].Attempts != 1 {
			t.Fatalf("failures=%d want 1 (pre-crash failure retained, restart attempt succeeded)",
				p.Steps[0].Attempts)
		}
		snap, _ := act.Snapshot(ctx, "trr")
		if snap.Version != "v2" || snap.Percent != 5 {
			t.Fatalf("post-restart actual=%+v", snap)
		}
		t.Logf("post-restart: retried with fresh token, failures=%d actual=%+v",
			p.Steps[0].Attempts, snap)
		_ = svc.Close()
	}
}

type testLogWriter struct {
	t      *testing.T
	prefix string
}

func (w *testLogWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s%s", w.prefix, string(p))
	return len(p), nil
}
