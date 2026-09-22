package actuator

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/fsx"
)

// targetRecord 是单个模拟目标的磁盘状态：
//   - current 为当前"实际"状态（允许落后于期望状态，模拟延迟）；
//   - applied 记录已接受的副作用 token，使 Apply 严格幂等，
//     进程重启后用同一 token 重试不会重复执行。
type targetRecord struct {
	SchemaVersion int                  `json:"schema_version"`
	Current       domain.ActualState   `json:"current"`
	Applied       map[string]appliedOp `json:"applied"`
	FailN         int                  `json:"fail_n"`
	LagN          int                  `json:"lag_n"`
}

type appliedOp struct {
	Result  domain.ActualState `json:"result"`
	Applied bool               `json:"applied"`
	Failed  bool               `json:"failed"`
	At      time.Time          `json:"at"`
}

// Actuator 是模拟的部署目标集合。所有方法并发安全。
type Actuator struct {
	dir string
	mu  sync.Mutex
}

func New(dataDir string) *Actuator {
	return &Actuator{dir: filepath.Join(dataDir, "targets")}
}

func (a *Actuator) path(targetID string) string {
	return filepath.Join(a.dir, targetID+".json")
}

func (a *Actuator) load(targetID string) (*targetRecord, bool, error) {
	var rec targetRecord
	exists, err := fsx.ReadJSON(a.path(targetID), &rec)
	if err != nil {
		return nil, false, fmt.Errorf("%w: target %s state unreadable: %v", domain.ErrCorruptData, targetID, err)
	}
	if !exists {
		rec = targetRecord{Applied: map[string]appliedOp{}}
		return &rec, false, nil
	}
	if rec.SchemaVersion != domain.SchemaVersion {
		return nil, true, fmt.Errorf("%w: target %s schema=%d want=%d",
			domain.ErrUnsupportedVersion, targetID, rec.SchemaVersion, domain.SchemaVersion)
	}
	if rec.Applied == nil {
		rec.Applied = map[string]appliedOp{}
	}
	return &rec, true, nil
}

func (a *Actuator) store(targetID string, rec *targetRecord) error {
	rec.SchemaVersion = domain.SchemaVersion
	rec.Current.UpdatedAt = time.Now()
	if err := fsx.WriteJSON(a.path(targetID), rec); err != nil {
		return fmt.Errorf("persist target %s: %w", targetID, err)
	}
	return nil
}

// Apply 对目标执行一次幂等副作用。
//
//	duplicate=true：该 token 已执行过，原样返回首次结论（不重复放量/回滚）；
//	               applied=false 表示该 token 首次执行时处于延迟（lag）中。
//	duplicate=false, applied=false, err=nil：本次副作用被延迟（lag），未生效。
//	applied=true, err=nil：副作用已生效。
//	err!=nil：注入的硬失败，已消耗一次尝试；err 为 domain.ErrSideEffectFailed。
func (a *Actuator) Apply(ctx context.Context, targetID, token string, want domain.ActualState) (result domain.ActualState, duplicate, applied bool, err error) {
	if err := ctx.Err(); err != nil {
		return domain.ActualState{}, false, false, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	rec, _, err := a.load(targetID)
	if err != nil {
		return domain.ActualState{}, false, false, err
	}

	// 幂等：相同 token 永远返回首次的结论，绝不重复执行副作用。
	if op, seen := rec.Applied[token]; seen {
		if op.Failed {
			return rec.Current, true, false, fmt.Errorf("%w: token %s already failed once",
				domain.ErrSideEffectFailed, token)
		}
		return op.Result, true, op.Applied, nil
	}

	now := time.Now()
	want.UpdatedAt = now

	if rec.FailN > 0 {
		rec.FailN--
		rec.Applied[token] = appliedOp{Failed: true, At: now}
		if perr := a.store(targetID, rec); perr != nil {
			return domain.ActualState{}, false, false, perr
		}
		return rec.Current, false, false, fmt.Errorf("%w: simulated infrastructure failure for %s",
			domain.ErrSideEffectFailed, token)
	}
	if rec.LagN > 0 {
		// 延迟：副作用记录为"已见但未生效"，实际状态保持不变（模拟延迟/短暂缺失）。
		rec.LagN--
		rec.Applied[token] = appliedOp{Result: rec.Current, Applied: false, At: now}
		if perr := a.store(targetID, rec); perr != nil {
			return domain.ActualState{}, false, false, perr
		}
		return rec.Current, false, false, nil
	}

	rec.Current = want
	rec.Applied[token] = appliedOp{Result: want, Applied: true, At: now}
	if err := a.store(targetID, rec); err != nil {
		return domain.ActualState{}, false, false, err
	}
	return want, false, true, nil
}

// Snapshot 读取目标当前实际状态；目标不存在返回错误（不得按默认值静默解释）。
func (a *Actuator) Snapshot(ctx context.Context, targetID string) (domain.ActualState, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	rec, exists, err := a.load(targetID)
	if err != nil {
		return domain.ActualState{}, err
	}
	if !exists {
		return domain.ActualState{}, fmt.Errorf("%w: target %s has no state", domain.ErrObservationMissing, targetID)
	}
	return rec.Current, nil
}

// SetFailN 让接下来 n 个"新 token"的 Apply 硬失败（测试有界重试）。
func (a *Actuator) SetFailN(targetID string, n int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	rec, _, _ := a.load(targetID)
	rec.FailN = n
	_ = a.store(targetID, rec)
}

// SetLagN 让接下来 n 个"新 token"的 Apply 延迟不生效（测试收敛延迟）。
func (a *Actuator) SetLagN(targetID string, n int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	rec, _, _ := a.load(targetID)
	rec.LagN = n
	_ = a.store(targetID, rec)
}

// AppliedTokens 返回已执行 token 的排序列表（测试/可解释性用）。
func (a *Actuator) AppliedTokens(targetID string) ([]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	rec, _, err := a.load(targetID)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rec.Applied))
	for t := range rec.Applied {
		out = append(out, t)
	}
	sort.Strings(out)
	return out, nil
}

func (a *Actuator) Close() error { return nil }

// EffectiveTokens 仅返回真正改变过目标状态的副作用 token（失败与延迟的不算）。
// 用于验证"绝不重复放量/重复回滚"：同一意图无论重试多少次，生效 token 只有一个。
func (a *Actuator) EffectiveTokens(targetID string) ([]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	rec, _, err := a.load(targetID)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rec.Applied))
	for t, op := range rec.Applied {
		if op.Applied {
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out, nil
}
