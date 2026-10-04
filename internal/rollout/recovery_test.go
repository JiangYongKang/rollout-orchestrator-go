package rollout

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// crashApplier 在 SimApplier 外包一层“崩溃注入 + 下发计数”，用于确定性地复现
// 进程在期望下发前 / 下发中 / 下发后（确认位未落盘）被杀掉的场景。
//
// 每次 Apply 用 (target,version,replicas,percent) 作为签名：同一签名重复下发
// 在语义上是幂等补发；inner 计数用于断言“没有对同一批重复放量、没有多走阶段”。
type crashApplier struct {
	inner       *SimApplier
	mu          sync.Mutex
	attempts    []string // 每次进入 Apply 的签名（含崩溃的尝试）
	applied     []string // 实际透传给 inner 的签名
	panicBefore map[string]bool
	panicAfter  map[string]bool
	appliedCh   chan string // 可选：每次成功透传后非阻塞通知（测试等待确认点用）
}

func newCrashApplier(sim *SimApplier) *crashApplier {
	return &crashApplier{
		inner:       sim,
		panicBefore: map[string]bool{},
		panicAfter:  map[string]bool{},
	}
}

func applySig(target, version string, replicas, percent int) string {
	return fmt.Sprintf("%s|%s|r=%d|t=%d", target, version, replicas, percent)
}

func (c *crashApplier) Apply(target, version string, replicas, percent int) error {
	sig := applySig(target, version, replicas, percent)
	c.mu.Lock()
	c.attempts = append(c.attempts, sig)
	if c.panicBefore[sig] {
		delete(c.panicBefore, sig)
		c.mu.Unlock()
		panic("simulated crash BEFORE desired state is applied: " + sig)
	}
	c.mu.Unlock()

	err := c.inner.Apply(target, version, replicas, percent) // 真实副作用（模拟）

	c.mu.Lock()
	if err == nil {
		c.applied = append(c.applied, sig)
		if c.appliedCh != nil {
			select {
			case c.appliedCh <- sig:
			default:
			}
		}
	}
	if c.panicAfter[sig] {
		delete(c.panicAfter, sig)
		c.mu.Unlock()
		panic("simulated crash AFTER desired state applied, BEFORE confirm persisted: " + sig)
	}
	c.mu.Unlock()
	return err
}

func (c *crashApplier) countAttempts(sig string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, s := range c.attempts {
		if s == sig {
			n++
		}
	}
	return n
}

func (c *crashApplier) snapshotApplied() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.applied))
	copy(out, c.applied)
	return out
}

// mustRunAdvanceWithPanic 调用一次预期会“崩溃”（panic）的操作并 recover，
// 模拟进程在该点被杀；defer 解锁照常执行，随后用 Close 模拟崩溃前最后落盘状态。
func mustRunAdvanceWithPanic(t *testing.T, e *Engine, cmdID, id string) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected crash panic, got none")
		} else {
			t.Logf("[crash] engine panics during Advance: %v", r)
		}
	}()
	_, _ = e.Advance(context.Background(), cmdID, id)
}

func mustRunRollbackWithPanic(t *testing.T, e *Engine, cmdID, id string) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected crash panic, got none")
		} else {
			t.Logf("[crash] engine panics during Rollback: %v", r)
		}
	}()
	_, _ = e.Rollback(cmdID, id)
}

// driveAdvance 后台推进一个阶段，主线程周期性确定性上报直到门控通过，
// 打印输入、状态变化与逐维度判定依据。
func driveAdvance(t *testing.T, e *Engine, sim *SimApplier, id, cmdID string) *Plan {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type res struct {
		p *Plan
		e error
	}
	ch := make(chan res, 1)
	go func() {
		p, err := e.Advance(ctx, cmdID, id)
		ch <- res{p, err}
	}()
	i := 0
	for {
		select {
		case r := <-ch:
			if r.e != nil {
				t.Fatalf("[drive] Advance cmd=%s failed: %v", cmdID, r.e)
			}
			t.Logf("[drive] cmd=%s -> state=%s currentStep=%d pendingStep=%d applied=%v lastVerdict=%s",
				cmdID, r.p.State, r.p.CurrentStep, r.p.PendingStep, r.p.PendingApplied, lastVerdict(r.p))
			return r.p
		case <-time.After(2 * time.Millisecond):
			obs, err := sim.Observe(e.plans[id].Target)
			if err != nil {
				continue
			}
			obs.ReportID = fmt.Sprintf("%s-%s-r%d", id, cmdID, i)
			i++
			_ = e.Report(id, obs)
		}
	}
}

func lastVerdict(p *Plan) string {
	if len(p.Verdicts) == 0 {
		return "-"
	}
	v := p.Verdicts[len(p.Verdicts)-1]
	return fmt.Sprintf("code=%s healthy=%v got(v=%s,t=%d,r=%d) reason=%q",
		v.Code, v.Healthy, v.GotVersion, v.GotPercent, v.GotReplicas, v.Reason)
}

func newRecoveryEngine(t *testing.T, dir string, sim Applier) *Engine {
	t.Helper()
	e, err := NewEngine(NewFileStore(filepath.Join(dir, "store.json")), sim, testLimits())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return e
}

func newImmediateSim() *SimApplier {
	sim := NewSimApplier()
	sim.StepReplicas = 0 // 下发后一次 Observe 即精确收敛，保证用例确定性
	return sim
}

// planSignatures 返回该计划各阶段期望下发的签名序列（按阶段顺序）。
func planSignatures(p *Plan) []string {
	sigs := make([]string, 0, len(p.Steps))
	for _, s := range p.Steps {
		sigs = append(sigs, applySig(p.Target, p.expectedVersion(s), s.Replicas, s.Percent))
	}
	return sigs
}

// assertAppliedSequence 断言实际下发序列恰好等于期望签名（每阶段一次、顺序正确，
// 无重复放量、无跳阶段）。允许同签名相邻重复（补发窗口），但必须连续且数量受限。
func assertAppliedSequence(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) < len(want) {
		t.Fatalf("applied sequence shorter than steps: got=%v want=%v", got, want)
	}
	gi := 0
	for wi, w := range want {
		// 同一签名可能因补发出现连续重复（at-least-once + 幂等），压缩后比较。
		run := 0
		for gi < len(got) && got[gi] == w {
			gi++
			run++
		}
		if run == 0 {
			t.Fatalf("applied sequence mismatch at step %d: want signature %q, got tail=%v (full=%v)",
				wi, w, got[gi:], got)
		}
		if run > 2 {
			t.Fatalf("signature %q applied %d times, non-idempotent double rollout: %v", w, run, got)
		}
	}
	if gi != len(got) {
		t.Fatalf("extra applications beyond all steps: %v (steps=%v)", got[gi:], want)
	}
}

// TestRecoveryCrashBeforeApply 阶段刚推进、期望状态还没真正下发时进程崩溃，
// 重启后必须自动补发同一批期望并继续推进，最终 Succeeded；全程不重复放量、不跳阶段。
func TestRecoveryCrashBeforeApply(t *testing.T) {
	dir := t.TempDir()
	sim := newImmediateSim()
	crash := newCrashApplier(sim)

	e1 := newRecoveryEngine(t, dir, crash)
	pl := newPlan("rcb-before", StrategyBatch)
	pl.BatchSizes = []int{2, 3, 5}
	p, err := e1.Create("c1", pl)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sigs := planSignatures(p)
	t.Logf("plan=%s target=%s steps signatures=%v (toVersion=%s)", p.ID, p.Target, sigs, p.ToVersion)

	// 注入：第 1 批（2 副本）的下发“进入前”崩溃 -> 模拟断在下发之前。
	crash.panicBefore[sigs[0]] = true
	t.Log("=== crash window: BEFORE applying step 0 desired state ===")
	mustRunAdvanceWithPanic(t, e1, "a1", p.ID)
	// Close 模拟进程死亡：内存未确认标记随进程丢失，磁盘只保留 pendingStep 位点。
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	t.Log("=== restart: engine must replay the unconfirmed batch automatically ===")
	e2 := newRecoveryEngine(t, dir, crash)
	defer e2.Close()
	rec := mustGet(t, e2, p.ID)
	t.Logf("recovered site: state=%s currentStep=%d pendingStep=%d pendingApplied=%v gateSeq=%d",
		rec.State, rec.CurrentStep, rec.PendingStep, rec.PendingApplied, rec.GateSeq)
	if rec.State != StateRunning || rec.PendingStep != 0 || !rec.PendingApplied {
		t.Fatalf("recovery should replay+confirm step0, got state=%s pending=%d applied=%v",
			rec.State, rec.PendingStep, rec.PendingApplied)
	}
	if n := crash.countAttempts(sigs[0]); n != 2 {
		t.Fatalf("step0 desired must be issued twice total (crash attempt + replay), got %d: %v",
			n, crash.attempts)
	}

	// 重启后继续：第 1 批已补发，等待门控通过；随后推进其余两批，最终成功。
	driveAdvance(t, e2, sim, p.ID, "a1-resume")
	driveAdvance(t, e2, sim, p.ID, "a2")
	fin := driveAdvance(t, e2, sim, p.ID, "a3")
	if fin.State != StateSucceeded || fin.CurrentStep != len(sigs)-1 {
		t.Fatalf("want Succeeded at last step, got state=%s current=%d", fin.State, fin.CurrentStep)
	}
	got := crash.snapshotApplied()
	t.Logf("actual application sequence: %v", got)
	assertAppliedSequence(t, got, sigs)
}

// TestRecoveryCrashDuringApply 崩溃发生在下发过程中（目标可能已部分收到期望）。
// 重启补发同一期望，目标以该期望为准收敛；补发幂等，不会叠加放量或多走阶段。
func TestRecoveryCrashDuringApply(t *testing.T) {
	dir := t.TempDir()
	sim := newImmediateSim()
	crash := newCrashApplier(sim)

	e1 := newRecoveryEngine(t, dir, crash)
	pl := newPlan("rcb-during", StrategyCanary)
	p, err := e1.Create("c1", pl)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sigs := planSignatures(p)
	t.Logf("plan=%s canary signatures=%v", p.ID, sigs)

	// 模拟“下发过程中”：副作用已经写入目标（可能只是部分生效），但进程在返回确认前崩溃。
	crash.panicAfter[sigs[0]] = true
	t.Log("=== crash window: DURING applying step 0 (side effect may have landed) ===")
	mustRunAdvanceWithPanic(t, e1, "a1", p.ID)
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}
	if n := crash.countAttempts(sigs[0]); n != 1 {
		t.Fatalf("before restart step0 attempts want 1, got %d", n)
	}

	t.Log("=== restart: replay same desired; it must converge to that exact batch ===")
	e2 := newRecoveryEngine(t, dir, crash)
	defer e2.Close()
	rec := mustGet(t, e2, p.ID)
	if rec.State != StateRunning || !rec.PendingApplied || rec.PendingStep != 0 {
		t.Fatalf("want replayed+confirmed step0, got state=%s pending=%d applied=%v",
			rec.State, rec.PendingStep, rec.PendingApplied)
	}
	// 同一期望共下发两次（崩溃时一次 + 重启补发一次）：内容完全相同，幂等不重复放量。
	if n := crash.countAttempts(sigs[0]); n != 2 {
		t.Fatalf("step0 desired attempts after replay want 2, got %d: %v", n, crash.attempts)
	}

	driveAdvance(t, e2, sim, p.ID, "a1-wait")
	driveAdvance(t, e2, sim, p.ID, "a2")
	fin := driveAdvance(t, e2, sim, p.ID, "a3")
	if fin.State != StateSucceeded {
		t.Fatalf("want Succeeded, got %s", fin.State)
	}
	got := crash.snapshotApplied()
	t.Logf("actual application sequence: %v", got)
	assertAppliedSequence(t, got, sigs)

	// 目标最终实际状态必须精确等于最后一批期望（100% / 全副本），不能被补发改乱。
	obs, oerr := sim.Observe(p.Target)
	if oerr != nil {
		t.Fatal(oerr)
	}
	last := p.Steps[len(p.Steps)-1]
	t.Logf("final observed: version=%s traffic=%d replicas=%d; want v=%s t=%d r=%d",
		obs.EffectiveVersion, obs.TrafficPercent, obs.Replicas, p.ToVersion, last.Percent, last.Replicas)
	if obs.EffectiveVersion != p.ToVersion || obs.TrafficPercent != last.Percent || obs.Replicas != last.Replicas {
		t.Fatalf("final actual state differs from desired: %+v", obs)
	}
}

// TestRecoveryCrashAfterConfirm 期望已下发并确认（PendingApplied=true）后，在
// 等待门控期间崩溃。重启后只重新等待门控，绝不重复下发；重复命令仍被拒绝。
func TestRecoveryCrashAfterConfirm(t *testing.T) {
	dir := t.TempDir()
	sim := newImmediateSim()
	crash := newCrashApplier(sim)

	e1 := newRecoveryEngine(t, dir, crash)
	p, err := e1.Create("c1", newPlan("rcb-after", StrategyCanary))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sigs := planSignatures(p)

	// 阶段 0：下发确认后门控立即通过；随后进入阶段 1，让其停在“已确认、等待门控”。
	driveAdvance(t, e1, sim, p.ID, "a1")
	ctx, cancel := context.WithCancel(context.Background())
	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		_, _ = e1.Advance(ctx, "a2", p.ID)
	}()
	// 确定性等待：进入等待循环前会同步落盘 PendingApplied=true，轮询到即说明
	// “下发 + 确认”都已完成（无上报，协程此后只会阻塞在等待上）。
	deadline := time.Now().Add(2 * time.Second)
	for {
		g := mustGet(t, e1, p.ID)
		if g.PendingStep == 1 && g.PendingApplied {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("step1 never reached confirmed in-flight: %+v", g)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if n := crash.countAttempts(sigs[1]); n != 1 {
		t.Fatalf("step1 should be applied exactly once before crash, got %d: %v", n, crash.attempts)
	}
	cancel() // 模拟进程被杀：等待中的 Advance 随 ctx 取消退出
	<-done1  // 必须等协程释放目标锁后再 Close，避免与同步落盘相互阻塞
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("=== crash AFTER step1 desired confirmed, while waiting gate ===")

	e2 := newRecoveryEngine(t, dir, crash)
	defer e2.Close()
	rec := mustGet(t, e2, p.ID)
	t.Logf("recovered: state=%s current=%d pending=%d applied=%v",
		rec.State, rec.CurrentStep, rec.PendingStep, rec.PendingApplied)
	if rec.PendingStep != 1 || !rec.PendingApplied {
		t.Fatalf("want step1 in-flight & applied, got pending=%d applied=%v", rec.PendingStep, rec.PendingApplied)
	}
	// 重启本身不能补发（NewEngine 只补发未确认的在途）。
	if n := crash.countAttempts(sigs[1]); n != 1 {
		t.Fatalf("confirmed in-flight must not be replayed on restart, attempts=%d", n)
	}
	// 同一 cmdID 继续等待：命令在门控通过那一刻才入账，故允许重入等待，
	// 但短超时内无新上报只能返回 ErrNotConverged，且绝不再下发一次。
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	_, err2 := e2.Advance(ctx2, "a2", p.ID)
	cancel2()
	if !errors.Is(err2, ErrNotConverged) {
		t.Fatalf("re-wait with same cmd want ErrNotConverged, got %v", err2)
	}
	if n := crash.countAttempts(sigs[1]); n != 1 {
		t.Fatalf("re-wait must not re-apply, step1 attempts=%d", n)
	}
	// 上报补齐后用同一 cmdID 继续等待，门控通过；随后推进最后一批。
	driveAdvance(t, e2, sim, p.ID, "a2")
	if n := crash.countAttempts(sigs[1]); n != 1 {
		t.Fatalf("step1 must still be applied exactly once, got %d", n)
	}
	fin := driveAdvance(t, e2, sim, p.ID, "a3")
	if fin.State != StateSucceeded {
		t.Fatalf("want Succeeded, got %s", fin.State)
	}
	assertAppliedSequence(t, crash.snapshotApplied(), sigs)
}

// TestRecoveryTimeoutReentryIdempotent 不重启、仅 ctx 超时重入：同一批期望绝不下发两次，
// 同一 cmdID 被拒绝，新 cmdID 继续等待。
func TestRecoveryTimeoutReentryIdempotent(t *testing.T) {
	dir := t.TempDir()
	sim := NewSimApplier() // 默认 StepReplicas=1，10 副本需多次观测，构造超时窗口
	crash := newCrashApplier(sim)
	e := newRecoveryEngine(t, dir, crash)
	defer e.Close()

	p, err := e1Create(t, e, "rcb-timeout")
	if err != nil {
		t.Fatal(err)
	}
	sigs := planSignatures(p)

	// Advance 在协程中运行：下发 + 确认落盘后，因没有新上报而停在等待门控。
	ctx, cancel := context.WithCancel(context.Background())
	done1 := make(chan struct{})
	var err1 error
	go func() {
		defer close(done1)
		_, err1 = e.Advance(ctx, "a1", p.ID)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		g := mustGet(t, e, p.ID)
		if g.PendingStep == 0 && g.PendingApplied {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("batch never reached confirmed in-flight: %+v", g)
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel() // 模拟调用方超时放弃：等待中的 Advance 随 ctx 取消退出
	<-done1
	if !errors.Is(err1, ErrNotConverged) {
		t.Fatalf("want ErrNotConverged on timeout, got %v", err1)
	}
	t.Logf("advance timed out with batch pending: attempts=%v", crash.attempts)
	if n := crash.countAttempts(sigs[0]); n != 1 {
		t.Fatalf("batch applied once before timeout, got %d", n)
	}
	// 同一 cmdID 在门控通过前允许继续等待（命令尚未入账），但不能触发二次下发：
	// 短超时无新上报 -> ErrNotConverged。
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	_, err2 := e.Advance(ctx2, "a1", p.ID)
	cancel2()
	if !errors.Is(err2, ErrNotConverged) {
		t.Fatalf("same cmdID re-wait want ErrNotConverged, got %v", err2)
	}
	if n := crash.countAttempts(sigs[0]); n != 1 {
		t.Fatalf("re-wait must not trigger re-apply, attempts=%d", n)
	}
	// 上报补齐后仍用同一 cmdID 继续等待：门控通过，不重复下发。
	got := driveAdvance(t, e, sim, p.ID, "a1")
	if n := crash.countAttempts(sigs[0]); n != 1 {
		t.Fatalf("batch still applied once after reentry, got %d", n)
	}
	if got.CurrentStep != 0 {
		t.Fatalf("want step0 passed, current=%d", got.CurrentStep)
	}
	// 门控通过后同一 cmdID 已入账：再次推进（进入下一阶段前的命令检查）必须拒绝，
	// 且不能对下一批产生任何下发。
	before := crash.countAttempts(sigs[1])
	ctx3, cancel3 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err3 := e.Advance(ctx3, "a1", p.ID)
	cancel3()
	if !errors.Is(err3, ErrDuplicateCommand) {
		t.Fatalf("recorded cmd on next step want ErrDuplicateCommand, got %v", err3)
	}
	if n := crash.countAttempts(sigs[1]); n != before {
		t.Fatalf("duplicate cmd must not apply next batch, attempts %d -> %d", before, n)
	}
}

// e1Create 小辅助：建一个 canary 计划。
func e1Create(t *testing.T, e *Engine, id string) (*Plan, error) {
	t.Helper()
	return e.Create("c1", newPlan(id, StrategyCanary))
}

// TestRecoveryCrashInterleavedPause 下发未确认的在途阶段与“暂停”穿插时崩溃：
// 重启后保持 Paused 且不得自动放量；Resume 后对同一阶段重新应用（幂等）并成功，
// 不跳过该阶段、也不重复放量。
//
// 构造方式：直接落盘一个 Running 且 PendingStep=0/PendingApplied=false 的计划，
// 再将其改为 Paused 后用新引擎加载，模拟“暂停状态下进程带着未确认在途重启”。
func TestRecoveryCrashInterleavedPause(t *testing.T) {
	dir := t.TempDir()
	sim := newImmediateSim()
	crash := newCrashApplier(sim)

	// 1) 正常建计划并让阶段 0 完成，随后推进阶段 1 时在“下发前”崩溃，制造未确认在途。
	e1 := newRecoveryEngine(t, dir, crash)
	p, err := e1.Create("c1", newPlan("rcb-pause", StrategyCanary))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sigs := planSignatures(p)
	driveAdvance(t, e1, sim, p.ID, "a1")
	crash.panicBefore[sigs[1]] = true
	t.Log("=== crash BEFORE step1 apply (unconfirmed in-flight) ===")
	mustRunAdvanceWithPanic(t, e1, "a2", p.ID)
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}
	if n := crash.countAttempts(sigs[1]); n != 1 {
		t.Fatalf("step1 attempted once (crashed before apply), got %d", n)
	}

	// 2) 模拟“崩溃前其实已收到暂停请求”：把磁盘状态改为 Paused，其余位点保持未确认在途。
	raw, err := readStorePlans(filepath.Join(dir, "store.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, rp := range raw {
		if rp.ID == p.ID {
			rp.State = StatePaused
		}
	}
	if err := writeStorePlans(filepath.Join(dir, "store.json"), raw); err != nil {
		t.Fatal(err)
	}

	// 3) 重启：Paused + 未确认在途 -> 丢弃在途位点，保持暂停，绝不在重启时自动放量。
	e2 := newRecoveryEngine(t, dir, crash)
	defer e2.Close()
	rec := mustGet(t, e2, p.ID)
	t.Logf("recovered while paused: state=%s current=%d pending=%d applied=%v attempts(step1)=%d",
		rec.State, rec.CurrentStep, rec.PendingStep, rec.PendingApplied, crash.countAttempts(sigs[1]))
	if rec.State != StatePaused || rec.PendingStep != -1 || rec.CurrentStep != 0 {
		t.Fatalf("want Paused at currentStep=0 no in-flight, got state=%s current=%d pending=%d",
			rec.State, rec.CurrentStep, rec.PendingStep)
	}
	if n := crash.countAttempts(sigs[1]); n != 1 {
		t.Fatalf("restart while paused must not auto-apply, step1 attempts=%d", n)
	}

	// 4) 暂停状态下推进仍是非法迁移；恢复后从同一阶段（step1）重新应用，不跳阶段。
	if _, err := e2.Advance(context.Background(), "a2", p.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Advance while paused want ErrInvalidTransition, got %v", err)
	}
	if _, err := e2.Resume("r1", p.ID); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	driveAdvance(t, e2, sim, p.ID, "a2")
	if n := crash.countAttempts(sigs[1]); n != 2 {
		// 崩溃前尝试 1 次（未生效）+ Resume 后真正下发 1 次。
		t.Fatalf("step1 applied once after resume (2 attempts total), got %d: %v", n, crash.attempts)
	}
	fin := driveAdvance(t, e2, sim, p.ID, "a3")
	if fin.State != StateSucceeded || fin.CurrentStep != len(sigs)-1 {
		t.Fatalf("want Succeeded at last step, got %s current=%d", fin.State, fin.CurrentStep)
	}
	assertAppliedSequence(t, crash.snapshotApplied(), sigs)
}

// readStorePlans / writeStorePlans 直接读写磁盘 JSON，供测试构造“崩溃后暂停”等
// 交错位点（同包可访问 storeFile 与模式版本常量）。
func readStorePlans(path string) ([]*Plan, error) {
	s := NewFileStore(path)
	return s.Load()
}

func writeStorePlans(path string, plans []*Plan) error {
	return NewFileStore(path).SaveAll(plans)
}

// TestRecoveryCrashDuringRollback 回滚与崩溃穿插：断在回滚下发之前、之后（终态未落盘）
// 两个窗口，重启都必须幂等重放回滚并落定 RolledBack，不残留 Running/Failed；
// 终态后释放目标（可在同目标新建发布），对终态计划的操作返回 ErrPlanFinished。
func TestRecoveryCrashDuringRollback(t *testing.T) {
	for _, window := range []string{"before", "after"} {
		t.Run(window, func(t *testing.T) {
			dir := t.TempDir()
			sim := newImmediateSim()
			crash := newCrashApplier(sim)

			e1 := newRecoveryEngine(t, dir, crash)
			pl := newPlan("rcb-rb-"+window, StrategyBatch)
			pl.BatchSizes = []int{4, 6}
			p, err := e1.Create("c1", pl)
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			sigs := planSignatures(p)
			// 第一批下发前崩溃制造未确认在途，随后“人工回滚”：
			// 回滚请求要先把在途批次补发的可能性取消，直接回滚到 fromVersion。
			crash.panicBefore[sigs[0]] = true
			mustRunAdvanceWithPanic(t, e1, "a1", p.ID)
			if err := e1.Close(); err != nil {
				t.Fatal(err)
			}

			// 用第二个引擎做人工回滚（它会先补发未确认批次——为聚焦回滚窗口，
			// 这里改为直接在磁盘 Running 位点上发回滚：先重启补发确认，再回滚）。
			e2 := newRecoveryEngine(t, dir, crash)
			rec := mustGet(t, e2, p.ID)
			if !rec.PendingApplied {
				t.Fatalf("restart should have replayed unconfirmed batch")
			}
			rbSig := applySig(p.Target, p.FromVersion, 0, 0)
			if window == "before" {
				crash.panicBefore[rbSig] = true
			} else {
				crash.panicAfter[rbSig] = true
			}
			t.Logf("=== crash %s rollback side effect (target=%s) ===", window, p.Target)
			mustRunRollbackWithPanic(t, e2, "rb1", p.ID)
			if err := e2.Close(); err != nil {
				t.Fatal(err)
			}

			// 重启：RollingBack 中间态被幂等重放，落定 RolledBack。
			e3 := newRecoveryEngine(t, dir, crash)
			defer e3.Close()
			fin := mustGet(t, e3, p.ID)
			t.Logf("recovered after rollback crash: state=%s pending=%d rollbackAttempts=%d",
				fin.State, fin.PendingStep, crash.countAttempts(rbSig))
			if fin.State != StateRolledBack || fin.PendingStep != -1 {
				t.Fatalf("want RolledBack with no in-flight, got state=%s pending=%d", fin.State, fin.PendingStep)
			}
			if n := crash.countAttempts(rbSig); n != 2 {
				t.Fatalf("idempotent rollback replay: want 2 attempts (crash + restart), got %d", n)
			}
			// 对终态计划的各种操作给出可区分的 ErrPlanFinished。
			if _, err := e3.Advance(context.Background(), "x", p.ID); !errors.Is(err, ErrPlanFinished) {
				t.Fatalf("Advance on rolled-back want ErrPlanFinished, got %v", err)
			}
			if _, err := e3.Pause("x", p.ID); !errors.Is(err, ErrPlanFinished) {
				t.Fatalf("Pause on rolled-back want ErrPlanFinished, got %v", err)
			}
			// 目标已释放：同目标可以新建发布（与 ErrTargetBusy 区分开）。
			p2 := newPlan("rcb-rb2-"+window, StrategyCanary)
			p2.Target = p.Target
			if _, err := e3.Create("c2", p2); err != nil {
				t.Fatalf("target should be free after rollback, got %v", err)
			}
			t.Logf("target %s released: new plan %s created", p.Target, p2.ID)
		})
	}
}

// failAlwaysApplier 对任何 Apply 都失败，用于模拟补发/回滚重试耗尽。
type failAlwaysApplier struct{ calls int }

func (f *failAlwaysApplier) Apply(string, string, int, int) error {
	f.calls++
	return fmt.Errorf("target control plane unavailable")
}

// TestRecoveryErrorCausesDistinguishable 中断恢复相关的失败原因必须可区分：
// 参数非法 ErrInvalidArgument、非法状态 ErrInvalidTransition、目标被占 ErrTargetBusy、
// 重启补发重试耗尽 -> 计划 Failed 且 NewEngine 返回错误供调用方判断。
func TestRecoveryErrorCausesDistinguishable(t *testing.T) {
	dir := t.TempDir()
	sim := newImmediateSim()
	e := newRecoveryEngine(t, dir, sim)
	defer e.Close()

	// 参数非法：缺 toVersion / 非法 batch 配置。
	if _, err := e.Create("c", Plan{ID: "x", Target: "t", Strategy: StrategyCanary}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing toVersion want ErrInvalidArgument, got %v", err)
	}
	bad := newPlan("badbatch", StrategyBatch)
	bad.BatchSizes = []int{4, 4, 4} // 累计 12 > 10
	if _, err := e.Create("c", bad); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("oversized batches want ErrInvalidArgument, got %v", err)
	}
	t.Log("invalid argument cases -> ErrInvalidArgument (exit 2 semantics)")

	ok1, err := e.Create("c1", newPlan("dist-1", StrategyCanary))
	if err != nil {
		t.Fatal(err)
	}
	// 目标被占：同目标第二个进行中发布 -> ErrTargetBusy（与参数错误区分）。
	busy := newPlan("dist-2", StrategyCanary)
	busy.Target = ok1.Target
	if _, err := e.Create("c2", busy); !errors.Is(err, ErrTargetBusy) {
		t.Fatalf("busy target want ErrTargetBusy, got %v", err)
	}
	t.Logf("target %s busy -> ErrTargetBusy (exit 7 semantics)", ok1.Target)

	// 非法状态：Running 计划不能 Resume / 重复 Pause 前置状态不符等。
	if _, err := e.Resume("r", ok1.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Resume on Running want ErrInvalidTransition, got %v", err)
	}
	if _, err := e.Pause("p1", ok1.ID); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if _, err := e.Advance(context.Background(), "a", ok1.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Advance on Paused want ErrInvalidTransition, got %v", err)
	}
	t.Log("illegal state cases -> ErrInvalidTransition (exit 3 semantics)")

	// 补发重试耗尽：构造未确认在途计划落盘，再用恒失败 applier 重启。
	dir2 := t.TempDir()
	e2 := newRecoveryEngine(t, dir2, newImmediateSim())
	pp, err := e2.Create("c1", func() Plan {
		q := newPlan("dist-fail", StrategyBatch)
		q.BatchSizes = []int{4, 6}
		return q
	}())
	if err != nil {
		t.Fatal(err)
	}
	// 直接写盘一个未确认在途的 Running 计划（等价于断在下发之前）。
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
	plans, err := readStorePlans(filepath.Join(dir2, "store.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, rp := range plans {
		if rp.ID == pp.ID {
			rp.PendingStep = 0
			rp.PendingApplied = false
			rp.GateSeq = rp.NextSeq
		}
	}
	if err := writeStorePlans(filepath.Join(dir2, "store.json"), plans); err != nil {
		t.Fatal(err)
	}
	failApp := &failAlwaysApplier{}
	if _, err := NewEngine(NewFileStore(filepath.Join(dir2, "store.json")), failApp, testLimits()); err == nil {
		t.Fatalf("restart with unrecoverable apply failure must return error")
	} else {
		t.Logf("replay exhausted -> %v", err)
	}
	// 落盘状态应为 Failed，且该目标已释放（不再 ErrTargetBusy）。
	e3 := newRecoveryEngine(t, dir2, sim)
	defer e3.Close()
	fin := mustGet(t, e3, pp.ID)
	if fin.State != StateFailed {
		t.Fatalf("want Failed after replay exhausted, got %s", fin.State)
	}
	t.Logf("plan marked %s after replay retry exhaustion; apply calls=%d", fin.State, failApp.calls)
	if failApp.calls == 0 {
		t.Fatalf("expected replay attempts against failing applier")
	}
}
