// 本地可运行的渐进式发布编排器 CLI。
//
// 子命令：
//
//	create   创建发布计划
//	advance  推进 / 重试 / 对账
//	pause    暂停
//	resume   恢复
//	rollback 回滚到稳定版本
//	report   上报一次模拟观测
//	get      查询单个计划（状态、事件、判定依据）
//	list     列出全部计划
//	demo     端到端脚本演示（含延迟、抖动、失败预算与自动回滚）
//
// 所有数据落在 -data 指定的本地目录（默认 ./.rollout-data），
// 不访问网络、不需要任何外部服务或凭据。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/actuator"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/orchestrator"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/store"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return fmt.Errorf("missing subcommand")
	}
	// 全局 flag 必须出现在子命令之前：orchestrator -data DIR <subcommand> [flags...]
	gfs := flag.NewFlagSet("global", flag.ContinueOnError)
	dataDir := gfs.String("data", ".rollout-data", "local data directory")
	gfs.Usage = usage
	if err := gfs.Parse(args); err != nil {
		return err
	}
	rest := gfs.Args()
	if len(rest) == 0 {
		usage()
		return fmt.Errorf("missing subcommand")
	}
	cmd := rest[0]
	rest = rest[1:]
	_ = cmd

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return err
	}
	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmsgprefix)

	ctx := context.Background()
	switch cmd {
	case "create":
		return withSvc(*dataDir, logger, func(svc *orchestrator.Service) error {
			return cmdCreate(ctx, svc, rest)
		})
	case "advance":
		return withSvc(*dataDir, logger, func(svc *orchestrator.Service) error {
			return cmdSimple(ctx, svc, rest, svc.Advance)
		})
	case "pause":
		return withSvc(*dataDir, logger, func(svc *orchestrator.Service) error {
			return cmdSimple(ctx, svc, rest, svc.Pause)
		})
	case "resume":
		return withSvc(*dataDir, logger, func(svc *orchestrator.Service) error {
			return cmdSimple(ctx, svc, rest, svc.Resume)
		})
	case "rollback":
		return withSvc(*dataDir, logger, func(svc *orchestrator.Service) error {
			if len(rest) < 1 {
				return fmt.Errorf("usage: rollback <plan-id> [reason]")
			}
			reason := "manual rollback"
			if len(rest) > 1 {
				reason = rest[1]
			}
			p, err := svc.Rollback(ctx, rest[0], reason)
			printResult(p, err)
			return err
		})
	case "report":
		return withSvc(*dataDir, logger, func(svc *orchestrator.Service) error {
			return cmdReport(ctx, svc, rest)
		})
	case "get":
		return withSvc(*dataDir, logger, func(svc *orchestrator.Service) error {
			return cmdGet(ctx, svc, rest)
		})
	case "list":
		return withSvc(*dataDir, logger, func(svc *orchestrator.Service) error {
			ps, err := svc.List(ctx)
			if err != nil {
				return err
			}
			for _, p := range ps {
				fmt.Printf("%-24s target=%-12s strategy=%-7s state=%-11s step=%d/%d budget_left=%d\n",
					p.ID, p.TargetID, p.Strategy, p.State, p.StepIdx+1, len(p.Steps), p.BudgetLeft)
			}
			if len(ps) == 0 {
				fmt.Println("(no plans)")
			}
			return nil
		})
	case "recover":
		return withSvc(*dataDir, logger, func(svc *orchestrator.Service) error {
			return svc.Recover(ctx)
		})
	case "demo":
		return withSvc(*dataDir, logger, func(svc *orchestrator.Service) error {
			return cmdDemo(ctx, *dataDir, svc)
		})
	default:
		usage()
		return fmt.Errorf("unknown subcommand %q", cmd)
	}
}

func withSvc(dataDir string, logger *log.Logger, fn func(*orchestrator.Service) error) error {
	st, err := store.New(dataDir)
	if err != nil {
		return err
	}
	act := actuator.New(dataDir)
	svc := orchestrator.NewService(filepath.Clean(dataDir), st, act, logger)
	if err := svc.Recover(context.Background()); err != nil {
		_ = svc.Close()
		return fmt.Errorf("startup recovery failed: %w", err)
	}
	defer svc.Close()
	return fn(svc)
}

type simpleCmd func(context.Context, string) (*domain.Plan, error)

func cmdSimple(ctx context.Context, svc *orchestrator.Service, args []string, fn simpleCmd) error {
	if len(args) < 1 {
		return fmt.Errorf("missing plan-id")
	}
	p, err := fn(ctx, args[0])
	printResult(p, err)
	return err
}

func cmdCreate(ctx context.Context, svc *orchestrator.Service, args []string) error {
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	id := fs.String("id", "", "plan id (required)")
	target := fs.String("target", "", "target id (required)")
	strat := fs.String("strategy", "canary", "canary | wave")
	desired := fs.String("desired", "", "desired version (required)")
	stable := fs.String("stable", "", "stable version (required)")
	replicas := fs.Int("replicas", 100, "total replicas")
	budget := fs.Int("budget", 2, "failure budget (unhealthy observations before auto rollback)")
	maxAttempts := fs.Int("max-attempts", 3, "max hard-failure retries per step")
	staleMs := fs.Int("stale-ms", 5000, "observation staleness threshold (ms)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg := domain.DefaultConfig()
	cfg.FailureBudget = *budget
	cfg.MaxAttempts = *maxAttempts
	cfg.StaleThreshold = time.Duration(*staleMs) * time.Millisecond
	p, err := svc.Create(ctx, orchestrator.CreateParams{
		ID: *id, TargetID: *target,
		Strategy:      domain.StrategyKind(*strat),
		DesiredVer:    *desired,
		StableVer:     *stable,
		TotalReplicas: *replicas,
		Config:        cfg,
	})
	printResult(p, err)
	return err
}

func cmdReport(ctx context.Context, svc *orchestrator.Service, args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	id := fs.String("id", "", "plan id")
	batch := fs.String("batch", "", "observation batch id (dedup key)")
	ver := fs.String("ver", "v2", "observed actual version")
	pct := fs.Int("pct", 0, "observed percent (canary)")
	rep := fs.Int("replicas", 0, "observed replicas (wave)")
	er := fs.Float64("error-rate", 0.01, "error_rate metric (use -error-rate-missing to omit)")
	p99 := fs.Float64("p99", 120, "latency_p99_ms metric")
	erMissing := fs.Bool("error-rate-missing", false, "simulate missing error_rate metric")
	ageMs := fs.Int("age-ms", 0, "observation age in ms (simulate stale)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" || *batch == "" {
		return fmt.Errorf("report requires -id and -batch")
	}
	now := time.Now()
	metrics := map[string]domain.Metric{
		"latency_p99_ms": {Name: "latency_p99_ms", Value: *p99, Exists: true},
	}
	if !*erMissing {
		metrics["error_rate"] = domain.Metric{Name: "error_rate", Value: *er, Exists: true}
	}
	o := domain.Observation{
		BatchID:    *batch,
		ObservedAt: now.Add(-time.Duration(*ageMs) * time.Millisecond),
		Actual:     domain.ActualState{Version: *ver, Percent: *pct, Replicas: *rep, UpdatedAt: now},
		Metrics:    metrics,
	}
	p, err := svc.Report(ctx, *id, o)
	printResult(p, err)
	return err
}

func cmdGet(ctx context.Context, svc *orchestrator.Service, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("missing plan-id")
	}
	p, err := svc.Get(ctx, args[0])
	if err != nil {
		return err
	}
	fmt.Printf("plan=%s target=%s strategy=%s state=%s desired=%s stable=%s step=%d/%d budget_left=%d\n",
		p.ID, p.TargetID, p.Strategy, p.State, p.DesiredVer, p.StableVer,
		p.StepIdx+1, len(p.Steps), p.BudgetLeft)
	for _, st := range p.Steps {
		fmt.Printf("  step %d desired=%-4d attempts=%d applied=%q converged=%v healthy=%v\n",
			st.Index, st.Desired, st.Attempts, st.AppliedToken, st.Converged, st.Healthy)
	}
	fmt.Println("events (newest last):")
	for _, ev := range p.Events {
		d, _ := json.Marshal(ev.Detail)
		fmt.Printf("  #%-3d %-22s %s %s\n", ev.Seq, ev.Kind, ev.Reason, string(d))
	}
	return nil
}

func printResult(p *domain.Plan, err error) {
	if err != nil {
		fmt.Printf("REJECTED: %v\n", err)
		if p != nil {
			fmt.Printf("  plan=%s state=%s step=%d budget_left=%d\n",
				p.ID, p.State, p.StepIdx, p.BudgetLeft)
		}
		return
	}
	fmt.Printf("OK plan=%s state=%s step=%d/%d budget_left=%d\n",
		p.ID, p.State, p.StepIdx+1, len(p.Steps), p.BudgetLeft)
}

func usage() {
	fmt.Fprintln(os.Stderr, `commands:
  create   -id ID -target T -strategy canary|wave -desired V -stable V [-replicas N] [-budget N] [-max-attempts N] [-stale-ms MS]
  advance  ID
  pause    ID
  resume   ID
  rollback ID [reason]
  report   -id ID -batch B [-ver V] [-pct N] [-replicas N] [-error-rate F] [-p99 F] [-error-rate-missing] [-age-ms MS]
  get      ID
  list
  recover
  demo
global flag: -data DIR (default ./.rollout-data), must appear before subcommand fields`)
}

var _ = strconv.Itoa
