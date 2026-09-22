package orchestrator_test

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/actuator"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
	orch "github.com/highcumontoa/rollout-orchestrator-go/internal/orchestrator"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/store"
)

// 旧 schema 版本的数据必须被明确拒绝，绝不按默认值静默解释。
func TestOldVersionRejected(t *testing.T) {
	dir := t.TempDir()
	st, err := store.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	act := actuator.New(dir)
	svc := orch.NewService(filepath.Join(dir, "data"), st, act, log.New(io.Discard, "", 0))
	ctx := context.Background()
	_, err = svc.Create(ctx, orch.CreateParams{ID: "old1", TargetID: "to", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 10, Config: domain.DefaultConfig()})
	if err != nil {
		t.Fatal(err)
	}
	_ = svc.Close()

	// 篡改磁盘文件为"未来版本"。
	planFile := filepath.Join(dir, "plans", "old1.json")
	raw, err := os.ReadFile(planFile)
	if err != nil {
		t.Fatal(err)
	}
	tampered := bytesReplace(raw, []byte(`"schema_version":1`), []byte(`"schema_version":99`))
	tampered = bytesReplace(tampered, []byte(`"schema_version": 1`), []byte(`"schema_version": 99`))
	if err := os.WriteFile(planFile, tampered, 0o644); err != nil {
		t.Fatal(err)
	}

	st2, _ := store.New(dir)
	act2 := actuator.New(dir)
	svc2 := orch.NewService(filepath.Join(dir, "data"), st2, act2, log.New(io.Discard, "", 0))
	err = svc2.Recover(ctx)
	if !errors.Is(err, domain.ErrUnsupportedVersion) {
		t.Fatalf("recover old schema: want ErrUnsupportedVersion got %v", err)
	}
	t.Logf("schema=99 rejected on recover: %v", err)
	if _, err := svc2.Get(ctx, "old1"); !errors.Is(err, domain.ErrUnsupportedVersion) {
		t.Fatalf("get old schema: want ErrUnsupportedVersion got %v", err)
	}
	// List 也必须拒绝（不能跳过坏数据后静默继续）。
	if _, err := svc2.List(ctx); !errors.Is(err, domain.ErrUnsupportedVersion) {
		t.Fatalf("list with old schema: want ErrUnsupportedVersion got %v", err)
	}
}

// 损坏（截断）文件必须报 ErrCorruptData。
func TestCorruptFileRejected(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.New(dir)
	act := actuator.New(dir)
	svc := orch.NewService(filepath.Join(dir, "data"), st, act, log.New(io.Discard, "", 0))
	ctx := context.Background()
	_, _ = svc.Create(ctx, orch.CreateParams{ID: "corrupt1", TargetID: "tc", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 10, Config: domain.DefaultConfig()})
	_ = svc.Close()

	planFile := filepath.Join(dir, "plans", "corrupt1.json")
	if err := os.WriteFile(planFile, []byte(`{"schema_version": 1, "id": "corrupt1",`), 0o644); err != nil {
		t.Fatal(err)
	}
	st2, _ := store.New(dir)
	act2 := actuator.New(dir)
	svc2 := orch.NewService(filepath.Join(dir, "data"), st2, act2, log.New(io.Discard, "", 0))
	if _, err := svc2.Get(ctx, "corrupt1"); !errors.Is(err, domain.ErrCorruptData) {
		t.Fatalf("truncated json: want ErrCorruptData got %v", err)
	}
	t.Logf("truncated plan file rejected as corrupt (no default-value interpretation)")
}

// 目标侧旧 schema 同样被拒绝。
func TestOldTargetVersionRejected(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.New(dir)
	act := actuator.New(dir)
	svc := orch.NewService(filepath.Join(dir, "data"), st, act, log.New(io.Discard, "", 0))
	ctx := context.Background()
	_, err := svc.Create(ctx, orch.CreateParams{ID: "ot", TargetID: "tt", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 10, Config: domain.DefaultConfig()})
	if err != nil {
		t.Fatal(err)
	}
	targetFile := filepath.Join(dir, "targets", "tt.json")
	raw, _ := os.ReadFile(targetFile)
	tampered := regexp.MustCompile(`"schema_version":\s*1`).
		ReplaceAll(raw, []byte(`"schema_version": 42`))
	if string(tampered) == string(raw) {
		t.Fatal("test setup failed to tamper target schema")
	}
	_ = os.WriteFile(targetFile, tampered, 0o644)
	if _, err := svc.Advance(ctx, "ot"); !errors.Is(err, domain.ErrUnsupportedVersion) {
		t.Fatalf("old target schema: want ErrUnsupportedVersion got %v", err)
	}
	t.Logf("target schema=42 rejected: %v", err)
}

// 资源上限：活动 plan 数达到 MaxPlans 后创建被拒绝；终态计划释放名额。
func TestCapacityLimit(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.New(dir)
	act := actuator.New(dir)
	svc := orch.NewService(filepath.Join(dir, "data"), st, act, log.New(io.Discard, "", 0)).WithMaxPlans(2)
	ctx := context.Background()
	mk := func(id, target string) error {
		_, err := svc.Create(ctx, orch.CreateParams{ID: id, TargetID: target, Strategy: domain.StrategyCanary,
			DesiredVer: "v2", StableVer: "v1", TotalReplicas: 10, Config: domain.DefaultConfig()})
		return err
	}
	if err := mk("cap1", "tg1"); err != nil {
		t.Fatal(err)
	}
	if err := mk("cap2", "tg2"); err != nil {
		t.Fatal(err)
	}
	if err := mk("cap3", "tg3"); !errors.Is(err, domain.ErrCapacityExceeded) {
		t.Fatalf("over capacity: want ErrCapacityExceeded got %v", err)
	}
	t.Log("third active plan rejected by ErrCapacityExceeded")

	// 让其中一个进入终态，名额释放后可以再建。
	if _, err := svc.Rollback(ctx, "cap1", "free slot"); err != nil {
		t.Fatal(err)
	}
	if err := mk("cap3", "tg3"); err != nil {
		t.Fatalf("after terminal frees slot: %v", err)
	}
	t.Log("terminal plan released capacity; new plan created")
}

func bytesReplace(s, old, repl []byte) []byte {
	return []byte(strings.ReplaceAll(string(s), string(old), string(repl)))
}
