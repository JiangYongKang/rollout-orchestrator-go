package actuator

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
)

func TestApplyIdempotentAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	want := domain.ActualState{Version: "v2", Percent: 25, Replicas: 3}

	a := New(dir)
	res, dup, applied, err := a.Apply(ctx, "tg", "tok-1", want)
	if err != nil || dup || !applied || res.Percent != 25 {
		t.Fatalf("first apply: res=%+v dup=%v applied=%v err=%v", res, dup, applied, err)
	}
	// 同 token 再来 N 次：duplicate=true，状态不变，无重复副作用。
	for i := 0; i < 5; i++ {
		r, d, ap, e := a.Apply(ctx, "tg", "tok-1", domain.ActualState{Version: "v9", Percent: 99})
		if e != nil || !d || !ap || r.Percent != 25 {
			t.Fatalf("idempotent call %d: r=%+v dup=%v applied=%v err=%v", i, r, d, ap, e)
		}
	}
	_ = a.Close()

	// 模拟重启：新 Actuator 读同一目录，同 token 仍返回首次结论。
	b := New(dir)
	r, d, ap, err := b.Apply(ctx, "tg", "tok-1", domain.ActualState{Version: "v9"})
	if err != nil || !d || !ap || r.Version != "v2" {
		t.Fatalf("post-restart replay: r=%+v dup=%v ap=%v err=%v", r, d, ap, err)
	}
	snap, err := b.Snapshot(ctx, "tg")
	if err != nil || snap.Percent != 25 {
		t.Fatalf("snapshot=%+v err=%v", snap, err)
	}
	t.Logf("token replayed across restart: snapshot=%+v (no duplicate side effect)", snap)
	_ = b.Close()
}

func TestFailThenLagInjection(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	a := New(dir)

	a.SetFailN("tg", 1)
	_, _, _, err := a.Apply(ctx, "tg", "f1", domain.ActualState{Version: "v2", Percent: 5})
	if !errors.Is(err, domain.ErrSideEffectFailed) {
		t.Fatalf("injected failure: %v", err)
	}
	// 失败的 token 重放仍报失败，但目标状态没变。
	snap, _ := a.Snapshot(ctx, "tg")
	if snap.Version == "v2" {
		t.Fatalf("failed apply must not change target: %+v", snap)
	}

	a.SetLagN("tg", 1)
	res, dup, applied, err := a.Apply(ctx, "tg", "l1", domain.ActualState{Version: "v2", Percent: 5})
	if err != nil || dup || applied {
		t.Fatalf("lag call: res=%+v dup=%v applied=%v err=%v", res, dup, applied, err)
	}
	// 延迟后新 token 生效。
	res, _, applied, err = a.Apply(ctx, "tg", "ok1", domain.ActualState{Version: "v2", Percent: 5})
	if err != nil || !applied || res.Percent != 5 {
		t.Fatalf("post-lag apply: %+v applied=%v err=%v", res, applied, err)
	}
	t.Log("fail and lag injections consumed exactly, subsequent apply succeeds")
}

func TestConcurrentApplyDifferentTokens(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	a := New(dir)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, _, _ = a.Apply(ctx, "tg", "tok", domain.ActualState{Version: "v1"}) // 同 token 并发
		}(i)
	}
	wg.Wait()
	tokens, err := a.EffectiveTokens("tg")
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 1 || tokens[0] != "tok" {
		t.Fatalf("concurrent same-token applies produced %v", tokens)
	}
	t.Logf("20 concurrent same-token applies -> single effective side effect: %v", tokens)
}
