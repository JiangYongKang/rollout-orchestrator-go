package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

func samplePlan(id string) *domain.Plan {
	return &domain.Plan{
		ID: id, TargetID: "t", Strategy: domain.StrategyCanary,
		DesiredVer: "v2", StableVer: "v1", TotalReplicas: 10,
		Steps:     []domain.Step{{Index: 0, Desired: 5}},
		State:     domain.StatePending,
		Config:    domain.DefaultConfig(),
		CreatedAt: time.Now(),
	}
}

func TestCreateGetSaveListAndReject(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	p := samplePlan("a")
	if err := st.Create(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := st.Create(ctx, p); !errors.Is(err, domain.ErrPlanAlreadyExists) {
		t.Fatalf("dup create: %v", err)
	}
	if _, err := st.Get(ctx, "missing"); !errors.Is(err, domain.ErrPlanNotFound) {
		t.Fatalf("missing: %v", err)
	}
	p.State = domain.StatePromoting
	p.Steps[0].AppliedToken = "tok"
	if err := st.Save(ctx, p); err != nil {
		t.Fatal(err)
	}
	got, err := st.Get(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.StatePromoting || got.Steps[0].AppliedToken != "tok" {
		t.Fatalf("save/get roundtrip lost data: %+v", got)
	}
	all, err := st.List(ctx)
	if err != nil || len(all) != 1 {
		t.Fatalf("list: %v %d", err, len(all))
	}
}

func TestStoreRejectsUnknownSchema(t *testing.T) {
	dir := t.TempDir()
	st, _ := New(dir)
	p := samplePlan("future")
	p.SchemaVersion = 7
	err := st.Save(context.Background(), p)
	if !errors.Is(err, domain.ErrUnsupportedVersion) {
		t.Fatalf("save future schema: %v", err)
	}
}
