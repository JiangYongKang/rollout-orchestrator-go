// rolloutctl 是本地发布编排命令行入口。
//
// 用法：
//
//	rolloutctl -store <path> create   -id p1 -target svc -strategy canary -from v1 -to v2 -replicas 10
//	rolloutctl -store <path> get      -id p1
//	rolloutctl -store <path> advance  -id p1 -cmd c1 [-simulate] [-timeout 10s]
//	rolloutctl -store <path> pause    -id p1 -cmd c2
//	rolloutctl -store <path> resume   -id p1 -cmd c3
//	rolloutctl -store <path> rollback -id p1 -cmd c4
//	rolloutctl -store <path> report   -id p1 -report-id r1 -replicas 5 -error-rate 0.01
//	rolloutctl -store <path> demo
//
// 说明：单进程内通过 -simulate 由内置模拟环境持续上报实际状态；
// 跨进程场景请用 report 子命令上报观测（按 ReportID 幂等）。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/rollout"
)

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	var code int
	switch {
	case errors.Is(err, rollout.ErrNotFound),
		errors.Is(err, rollout.ErrInvalidArgument):
		code = 2
	case errors.Is(err, rollout.ErrInvalidTransition),
		errors.Is(err, rollout.ErrPlanFinished),
		errors.Is(err, rollout.ErrDuplicateCommand),
		errors.Is(err, rollout.ErrDuplicateReport):
		code = 3
	case errors.Is(err, rollout.ErrHealthGate),
		errors.Is(err, rollout.ErrStaleObservation),
		errors.Is(err, rollout.ErrMissingMetric),
		errors.Is(err, rollout.ErrNotConverged):
		code = 4
	case errors.Is(err, rollout.ErrRetryExhausted),
		errors.Is(err, rollout.ErrLimitExceeded):
		code = 5
	case errors.Is(err, rollout.ErrStoreVersion),
		errors.Is(err, rollout.ErrStoreCorrupt):
		code = 6
	default:
		code = 1
	}
	os.Exit(code)
}

func printPlan(p *rollout.Plan) {
	out, _ := json.MarshalIndent(p, "", "  ")
	fmt.Println(string(out))
}

func main() {
	storePath := flag.String("store", "rollout-store.json", "持久化文件路径")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "missing subcommand: create|get|advance|pause|resume|rollback|report|demo")
		os.Exit(2)
	}
	cmd, rest := args[0], args[1:]
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	id := fs.String("id", "", "计划 ID")
	cmdID := fs.String("cmd", "", "命令 ID（幂等）")
	target := fs.String("target", "", "目标名称")
	strategy := fs.String("strategy", "canary", "策略: canary|bluegreen")
	from := fs.String("from", "v1", "当前稳定版本")
	to := fs.String("to", "v2", "目标版本")
	replicas := fs.Int("replicas", 10, "总副本数")
	reportID := fs.String("report-id", "", "上报 ID（幂等）")
	errRate := fs.Float64("error-rate", 0, "观测错误率")
	samples := fs.Int("samples", 100, "样本数")
	simulate := fs.Bool("simulate", false, "用内置模拟环境驱动收敛上报")
	timeout := fs.Duration("timeout", 10*time.Second, "推进等待超时")
	if err := fs.Parse(rest); err != nil {
		fatal(err)
	}

	sim := rollout.NewSimApplier()
	sim.StepReplicas = 2
	eng, err := rollout.NewEngine(rollout.NewFileStore(*storePath), sim, rollout.Limits{})
	if err != nil {
		fatal(err)
	}
	defer eng.Close()

	switch cmd {
	case "create":
		p, err := eng.Create(*cmdID, rollout.Plan{
			ID: *id, Target: *target, Strategy: rollout.StrategyKind(*strategy),
			FromVersion: *from, ToVersion: *to, TotalReplicas: *replicas,
			Health: rollout.HealthPolicy{
				MaxErrorRate: 0.05, FailureBudget: 3,
				ObservationMaxAge: time.Minute, MinSamples: 10,
			},
		})
		if err != nil {
			fatal(err)
		}
		printPlan(p)
	case "get":
		p, err := eng.Get(*id)
		if err != nil {
			fatal(err)
		}
		printPlan(p)
	case "advance":
		if *simulate {
			stop := startSimReporter(eng, sim, *id)
			defer stop()
		}
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		p, err := eng.Advance(ctx, *cmdID, *id)
		if err != nil {
			fatal(err)
		}
		printPlan(p)
	case "pause":
		p, err := eng.Pause(*cmdID, *id)
		if err != nil {
			fatal(err)
		}
		printPlan(p)
	case "resume":
		p, err := eng.Resume(*cmdID, *id)
		if err != nil {
			fatal(err)
		}
		printPlan(p)
	case "rollback":
		p, err := eng.Rollback(*cmdID, *id)
		if err != nil {
			fatal(err)
		}
		printPlan(p)
	case "report":
		p, err := eng.Get(*id)
		if err != nil {
			fatal(err)
		}
		err = eng.Report(*id, rollout.Observation{
			ReportID: *reportID, ReportedAt: time.Now(),
			Replicas: *replicas, ErrorRate: *errRate,
			Samples: *samples, HasMetrics: true,
		})
		if err != nil {
			fatal(err)
		}
		fmt.Printf("report accepted for plan %s (state=%s step=%d)\n", p.ID, p.State, p.CurrentStep)
	case "demo":
		if err := demo(eng, sim); err != nil {
			fatal(err)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand:", cmd)
		os.Exit(2)
	}
}

// startSimReporter 在进程内用模拟环境周期性上报实际状态。
func startSimReporter(eng *rollout.Engine, sim *rollout.SimApplier, planID string) (stop func()) {
	done := make(chan struct{})
	go func() {
		i := 0
		for {
			select {
			case <-done:
				return
			case <-time.After(20 * time.Millisecond):
			}
			p, err := eng.Get(planID)
			if err != nil {
				return
			}
			i++
			obs, err := sim.Observe(p.Target)
			if err != nil {
				continue
			}
			obs.ReportID = fmt.Sprintf("%s-auto-%d", planID, i)
			_ = eng.Report(planID, obs) // 终态后拒绝属预期
		}
	}()
	return func() { close(done) }
}

// demo 完整演示：创建金丝雀计划并逐步推进到成功。
func demo(eng *rollout.Engine, sim *rollout.SimApplier) error {
	p, err := eng.Create("demo-create", rollout.Plan{
		ID: "demo", Target: "svc-demo", Strategy: rollout.StrategyCanary,
		FromVersion: "v1", ToVersion: "v2", TotalReplicas: 10,
		Health: rollout.HealthPolicy{
			MaxErrorRate: 0.05, FailureBudget: 3,
			ObservationMaxAge: time.Minute, MinSamples: 10,
		},
	})
	if err != nil {
		return err
	}
	fmt.Printf("created plan demo: %d steps\n", len(p.Steps))
	stop := startSimReporter(eng, sim, p.ID)
	defer stop()
	for i := range p.Steps {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		after, err := eng.Advance(ctx, fmt.Sprintf("demo-adv-%d", i), p.ID)
		cancel()
		if err != nil {
			return err
		}
		fmt.Printf("step %d (%s) done: state=%s\n", i, p.Steps[i].Name, after.State)
	}
	final, err := eng.Get(p.ID)
	if err != nil {
		return err
	}
	fmt.Printf("final state=%s, verdicts=%d\n", final.State, len(final.Verdicts))
	return nil
}
