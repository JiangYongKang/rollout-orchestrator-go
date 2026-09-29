package rollout

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// v1 JSON（旧模式：观测无 seq/version/trafficPercent，位点只有 currentStep）。
func writeV1Store(t *testing.T, path, state string, currentStep int) {
	t.Helper()
	body := `{
  "version": 1,
  "plans": [
    {
      "id": "old1",
      "target": "svc-old",
      "strategy": "canary",
      "fromVersion": "v1",
      "toVersion": "v2",
      "totalReplicas": 10,
      "steps": [
        {"index":0,"name":"canary-10%","percent":10,"replicas":1},
        {"index":1,"name":"canary-50%","percent":50,"replicas":5},
        {"index":2,"name":"canary-100%","percent":100,"replicas":10}
      ],
      "currentStep": ` + itoa(currentStep) + `,
      "state": "` + state + `",
      "health": {"maxErrorRate":0.05,"failureBudget":3,"observationMaxAge":60000000000,"minSamples":10},
      "failStreak": 0,
      "observations": [
        {"reportID":"legacy-r1","reportedAt":"2026-01-01T00:00:00Z","replicas":1,"errorRate":0,"samples":100,"hasMetrics":true}
      ],
      "seenReports": ["legacy-r1"],
      "appliedCmds": ["legacy-adv-0"],
      "verdicts": [],
      "updatedAt": "2026-01-01T00:00:00Z"
    }
  ]
}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

// TestLoadV1RunningMigrated 旧 v1 进行中计划可读出并保守迁移：在途阶段重新等待
// 新观测，绝不拿旧观测当通过依据。
func TestLoadV1RunningMigrated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")
	writeV1Store(t, path, "Running", 0)

	sim := NewSimApplier()
	sim.StepReplicas = 0
	e, err := NewEngine(NewFileStore(path), sim, testLimits())
	if err != nil {
		t.Fatalf("v1 should be loadable: %v", err)
	}
	defer e.Close()

	p := mustGet(t, e, "old1")
	t.Logf("migrated: state=%s current=%d pending=%d gateSeq=%d nextSeq=%d obsSeq=%d",
		p.State, p.CurrentStep, p.PendingStep, p.GateSeq, p.NextSeq, p.Observations[0].Seq)
	if p.State != StateRunning || p.CurrentStep != -1 || p.PendingStep != 0 {
		t.Fatalf("v1 running migration site wrong: %+v", p)
	}
	if p.GateSeq != 1 || p.NextSeq != 1 {
		t.Fatalf("v1 must require fresh observations: gateSeq=%d nextSeq=%d", p.GateSeq, p.NextSeq)
	}
	if p.Observations[0].Seq != 0 {
		t.Fatalf("legacy observation seq backfill wrong: %d", p.Observations[0].Seq)
	}

	// 旧命令已入账，重放仍被拒绝；在途阶段必须用迁移后新观测才能通过。
	if _, err := e.Advance(nil, "legacy-adv-0", p.ID); err == nil || !errors.Is(err, ErrDuplicateCommand) {
		t.Fatalf("legacy command must stay rejected after migration, got %v", err)
	}
	// 即便旧观测内容“看起来”满足，也不能通过（无新观测 -> 超时）。
	ctx, cancel := newCtx100ms()
	defer cancel()
	if _, err := e.Advance(ctx, "new-adv-0", p.ID); !errors.Is(err, ErrNotConverged) {
		t.Fatalf("legacy observation must not pass migrated stage, got %v", err)
	}

	// 喂一条迁移后的新观测（版本/流量/承载全对），在途阶段通过并可继续。
	_ = sim.Apply(p.Target, "v2", 1, 10)
	if err := e.Report(p.ID, Observation{
		ReportID: "fresh-1", ReportedAt: time.Now(),
		EffectiveVersion: "v2", TrafficPercent: 10, Replicas: 1,
		ErrorRate: 0, Samples: 100, HasMetrics: true,
	}); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := newCtx3s()
	defer cancel2()
	after, err := e.Advance(ctx2, "new-adv-0b", p.ID)
	if err != nil {
		t.Fatalf("migrated stage should pass with fresh obs: %v", err)
	}
	t.Logf("migrated stage passed: current=%d pending=%d state=%s", after.CurrentStep, after.PendingStep, after.State)
	if after.CurrentStep != 0 || after.PendingStep != -1 {
		t.Fatalf("want step0 passed, current=%d pending=%d", after.CurrentStep, after.PendingStep)
	}
}

// TestLoadV1PausedMigrated 旧 v1 暂停计划迁移为停在上一已通过位点。
func TestLoadV1PausedMigrated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")
	writeV1Store(t, path, "Paused", 1)
	e, err := NewEngine(NewFileStore(path), NewSimApplier(), testLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	p := mustGet(t, e, "old1")
	t.Logf("paused migrated: current=%d pending=%d state=%s", p.CurrentStep, p.PendingStep, p.State)
	if p.State != StatePaused || p.CurrentStep != 0 || p.PendingStep != -1 {
		t.Fatalf("v1 paused migration wrong: %+v", p)
	}
}

// TestUnsupportedVersionRejected 非 1/2 的版本必须明确拒绝，给出可区分原因。
func TestUnsupportedVersionRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")
	if err := os.WriteFile(path, []byte(`{"version":0,"plans":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := NewEngine(NewFileStore(path), NewSimApplier(), testLimits())
	t.Logf("load version=0 -> %v", err)
	if !errors.Is(err, ErrStoreVersion) {
		t.Fatalf("want ErrStoreVersion, got %v", err)
	}
}
