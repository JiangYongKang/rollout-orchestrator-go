package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/orchestrator"
)

// cmdDemo 不依赖外部系统，脚本化演示：
//  1. canary 计划经历延迟 -> 抖动 -> 正常放量（重复上报不重复放量）；
//  2. 第二个 canary 计划在健康门控上耗尽失败预算并自动回滚。
func cmdDemo(ctx context.Context, dataDir string, svc *orchestrator.Service) error {
	fmt.Println("=== Demo 1: 延迟、抖动与重复上报下的幂等放量 ===")
	const target1 = "demo-svc"
	cfg := domain.DefaultConfig()
	cfg.StaleThreshold = time.Minute // demo 中不用真实时钟卡过期
	p, err := svc.Create(ctx, orchestrator.CreateParams{
		ID: "demo-canary", TargetID: target1, Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 100, Config: cfg,
	})
	if err != nil {
		return err
	}
	fmt.Printf("created: state=%s\n", p.State)

	// 第一次副作用被基础设施延迟。
	svc.InjectLagN(target1, 1)
	if p, err = svc.Advance(ctx, "demo-canary"); err != nil {
		return err
	}
	fmt.Printf("advance with lag: state=%s (side effect delayed, no promotion)\n", p.State)
	if p, err = svc.Advance(ctx, "demo-canary"); err != nil {
		return err
	}
	fmt.Printf("retry advance: state=%s (promoted exactly once)\n", p.State)

	// 观测：先短暂缺失（仍为 v1），再带 ±2 抖动收敛。
	mk := func(batch, ver string, pct int) domain.Observation {
		return domain.Observation{
			BatchID: batch, ObservedAt: time.Now(),
			Actual:  domain.ActualState{Version: ver, Percent: pct, Replicas: pct},
			Metrics: demoMetrics(0.01, 120),
		}
	}
	if p, err = svc.Report(ctx, "demo-canary", mk("d1", "v1", 0)); err != nil {
		return err
	}
	fmt.Printf("report missing-actual (v1): state=%s (not converged, no skip)\n", p.State)
	if p, err = svc.Report(ctx, "demo-canary", mk("d2", "v2", 7)); err != nil { // 5+2 边界抖动
		return err
	}
	fmt.Printf("report jitter pct=7 (want 5±2): state=%s\n", p.State)
	// 同批次重复上报：拒绝，不产生任何效果。
	if _, err = svc.Report(ctx, "demo-canary", mk("d2", "v2", 7)); err != nil {
		fmt.Printf("duplicate batch d2 rejected: %v\n", err)
	}
	// 新批次确认门控，进入下一批；演示跑完第 2 批即暂停脚本，避免输出过长。
	if p, err = svc.Report(ctx, "demo-canary", mk("d3", "v2", 5)); err != nil {
		return err
	}
	fmt.Printf("gate-confirm batch d3: state=%s step=%d/%d\n", p.State, p.StepIdx+1, len(p.Steps))

	fmt.Println()
	fmt.Println("=== Demo 2: 关键指标异常耗尽失败预算 -> 自动回滚 ===")
	cfg2 := domain.DefaultConfig()
	cfg2.FailureBudget = 1
	cfg2.StaleThreshold = time.Minute
	const target2 = "demo-unhealthy"
	if _, err := svc.Create(ctx, orchestrator.CreateParams{
		ID: "demo-unhealthy", TargetID: target2, Strategy: domain.StrategyCanary,
		DesiredVer: "v9", StableVer: "v1", TotalReplicas: 100, Config: cfg2,
	}); err != nil {
		return err
	}
	if _, err := svc.Advance(ctx, "demo-unhealthy"); err != nil {
		return err
	}
	// 第一次不健康：预算 1 -> 0，计划停留 Promoting（ErrUnhealthy 是预期拒绝）。
	bad := domain.Observation{
		BatchID: "u1", ObservedAt: time.Now(),
		Actual:  domain.ActualState{Version: "v9", Percent: 5, Replicas: 5},
		Metrics: demoMetrics(0.4, 800), // error_rate 0.4 远超 0.05 上限
	}
	p, err = svc.Report(ctx, "demo-unhealthy", bad)
	if !errors.Is(err, domain.ErrUnhealthy) {
		return fmt.Errorf("expected ErrUnhealthy, got %v", err)
	}
	fmt.Printf("unhealthy report u1 REJECTED (%v)\n  -> state=%s budget_left=%d (rollout held)\n",
		err, p.State, p.BudgetLeft)
	// 同批次重复：预算不得二次扣减。
	if _, err = svc.Report(ctx, "demo-unhealthy", bad); !errors.Is(err, domain.ErrDuplicateCommand) {
		return fmt.Errorf("expected ErrDuplicateCommand, got %v", err)
	}
	fmt.Printf("duplicate unhealthy batch u1 REJECTED; budget_left=%d unchanged\n", p.BudgetLeft)
	// 新批次仍不健康：预算耗尽，自动回滚到 v1（自动回滚本身返回 nil）。
	bad.BatchID = "u2"
	p, err = svc.Report(ctx, "demo-unhealthy", bad)
	if err != nil {
		return err
	}
	fmt.Printf("unhealthy report u2: state=%s (auto rollback), restored stable=v1\n", p.State)

	snap, _ := svc.TargetSnapshot(ctx, target2)
	fmt.Printf("actual target snapshot: %+v\n", snap)
	fmt.Println()
	fmt.Println("use 'get <plan-id>' to inspect the full event trail and decision reasons.")
	return nil
}

func demoMetrics(er, p99 float64) map[string]domain.Metric {
	return map[string]domain.Metric{
		"error_rate":     {Name: "error_rate", Value: er, Exists: true},
		"latency_p99_ms": {Name: "latency_p99_ms", Value: p99, Exists: true},
	}
}
