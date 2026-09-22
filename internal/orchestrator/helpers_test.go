package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

// finishCanary 用新批次观测把一个 canary 计划驱动到 Succeeded。
func finishCanary(t *testing.T, svc *Service, id string, clk *testClock) {
	t.Helper()
	ctx := context.Background()
	p, err := svc.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	for i := p.StepIdx; i < len(p.Steps); i++ {
		if p.State == domain.StatePending {
			p, err = svc.Advance(ctx, id)
			if err != nil {
				t.Fatalf("advance %d: %v", i, err)
			}
		}
		pct := p.Steps[i].Desired
		p, err = svc.Report(ctx, id, obs(clk, "f-"+itoa(i)+"-a", "v2", pct, pct))
		if err != nil {
			t.Fatalf("converge report %d: %v", i, err)
		}
		if i == len(p.Steps)-1 {
			break
		}
		if p.State != domain.StateGated {
			t.Fatalf("step %d want Gated got %s", i, p.State)
		}
		p, err = svc.Report(ctx, id, obs(clk, "f-"+itoa(i)+"-b", "v2", pct, pct))
		if err != nil {
			t.Fatalf("gate report %d: %v", i, err)
		}
		if p.State != domain.StatePromoting {
			t.Fatalf("step %d want Promoting got %s", i, p.State)
		}
	}
	if p.State != domain.StateSucceeded {
		t.Fatalf("finish: state=%s", p.State)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func healthyMetricsAt(_ time.Time) map[string]domain.Metric {
	return map[string]domain.Metric{
		"error_rate":     {Name: "error_rate", Value: 0.01, Exists: true},
		"latency_p99_ms": {Name: "latency_p99_ms", Value: 120, Exists: true},
	}
}
