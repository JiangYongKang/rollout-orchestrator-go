package strategy

import (
	"testing"
	"time"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

func TestCanaryToleranceSemantics(t *testing.T) {
	s := &CanaryStrategy{}
	steps, err := s.BuildSteps("v2", 100)
	if err != nil {
		t.Fatal(err)
	}
	want := []int{5, 25, 50, 100}
	for i, w := range want {
		if steps[i].Desired != w {
			t.Fatalf("step %d = %d want %d", i, steps[i].Desired, w)
		}
	}
	cfg := domain.DefaultConfig()
	now := time.Now()
	mkPlan := func() *domain.Plan {
		return &domain.Plan{
			DesiredVer: "v2", Steps: steps, StepIdx: 0, Config: cfg,
		}
	}
	mkObs := func(ver string, pct int, age time.Duration) *domain.Observation {
		return &domain.Observation{
			BatchID: "b", ObservedAt: now.Add(-age),
			Actual: domain.ActualState{Version: ver, Percent: pct},
		}
	}

	cases := []struct {
		name     string
		ver      string
		pct      int
		age      time.Duration
		converge bool
	}{
		{"exact", "v2", 5, 0, true},
		{"jitter low edge", "v2", 3, 0, true},  // 5-2
		{"jitter high edge", "v2", 7, 0, true}, // 5+2
		{"jitter too low", "v2", 2, 0, false},
		{"jitter too high", "v2", 8, 0, false},
		{"wrong version", "v1", 5, 0, false},
		{"stale", "v2", 5, 6 * time.Second, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, reason := s.IsConverged(mkPlan(), mkObs(c.ver, c.pct, c.age), now)
			t.Logf("input ver=%s pct=%d age=%s -> converged=%v reason=%q",
				c.ver, c.pct, c.age, ok, reason)
			if ok != c.converge {
				t.Fatalf("converged=%v want %v", ok, c.converge)
			}
		})
	}
}

func TestWaveSmallClusterRounding(t *testing.T) {
	s := &WaveStrategy{}
	steps, err := s.BuildSteps("v2", 3)
	if err != nil {
		t.Fatal(err)
	}
	// ceil: 0.3->1, 0.75->1(dup dropped), 1.5->2, 3->3 => [1,2,3]
	got := make([]int, len(steps))
	for i := range steps {
		got[i] = steps[i].Desired
	}
	want := []int{1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("steps=%v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("steps=%v want %v", got, want)
		}
	}
	t.Logf("small cluster (n=3) wave batches=%v strictly increasing", got)
}
