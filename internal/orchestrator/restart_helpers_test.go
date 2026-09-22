package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

func fixedNow() time.Time { return time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC) }

func tokensFor(t *testing.T, dir, target string) []string {
	t.Helper()
	_, act := spawn(t, dir)
	tokens, err := act.EffectiveTokens(target)
	if err != nil {
		t.Fatal(err)
	}
	_ = act.Close()
	return tokens
}

func mustSnapshot(t *testing.T, svc *Service, target string) domain.ActualState {
	t.Helper()
	st, err := svc.act.Snapshot(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	return st
}
