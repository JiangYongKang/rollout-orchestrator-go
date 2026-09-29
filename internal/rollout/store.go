package rollout

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

const (
	// storeSchemaVersion 当前持久化模式版本。
	storeSchemaVersion = 2
	// storeSchemaV1 旧版本（观测无版本/流量维度、位点为 CurrentStep 单游标）。
	storeSchemaV1 = 1
)

// storeFile 磁盘上的数据信封。
type storeFile struct {
	Version int     `json:"version"`
	Plans   []*Plan `json:"plans"`
}

// Store 计划持久化接口。
type Store interface {
	Load() ([]*Plan, error)
	SaveAll(plans []*Plan) error
}

// FileStore 基于单个 JSON 文件的持久化实现（原子写、带模式版本）。
type FileStore struct {
	mu   sync.Mutex
	path string
}

// NewFileStore 创建文件存储。
func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

// Load 读取全部计划。文件不存在时返回空；版本不兼容或数据损坏时报错。
//
// 兼容范围（不静默按缺省值解释）：
//   - v2：当前格式，直接读取；
//   - v1：可读取并迁移，位点语义按下述确定性规则映射到 v2 双游标，
//     迁移后计划继续推进的门控结论与新格式一致；
//   - 其它版本：明确拒绝并返回 ErrStoreVersion。
func (s *FileStore) Load() ([]*Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read store: %w", err)
	}
	var f storeFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStoreCorrupt, err)
	}
	switch f.Version {
	case storeSchemaVersion:
	case storeSchemaV1:
		for _, p := range f.Plans {
			migratePlanV1toV2(p)
		}
	default:
		return nil, fmt.Errorf("%w: got %d, want %d (supported: %d,%d)", ErrStoreVersion, f.Version, storeSchemaVersion, storeSchemaV1, storeSchemaVersion)
	}
	for _, p := range f.Plans {
		if p == nil || p.ID == "" {
			return nil, fmt.Errorf("%w: plan with empty id", ErrStoreCorrupt)
		}
		if len(p.Steps) == 0 {
			return nil, fmt.Errorf("%w: plan %s has no steps", ErrStoreCorrupt, p.ID)
		}
		if !p.State.Terminal() && (p.CurrentStep < -1 || p.CurrentStep >= len(p.Steps)) {
			return nil, fmt.Errorf("%w: plan %s currentStep %d out of range", ErrStoreCorrupt, p.ID, p.CurrentStep)
		}
	}
	return f.Plans, nil
}

// migratePlanV1toV2 把 v1 计划位点映射为 v2 双游标。
//
// v1 的 CurrentStep 是“当前所处阶段”，无法区分该阶段是“已通过”还是“已应用待门控”。
// 为绝不提前放行、绝不跳过阶段，迁移采取保守策略：
//   - 进行中（Running）：视为该阶段在途（PendingStep=CurrentStep），重新等待
//     多维门控且需要迁移后产生的新观测（GateSeq=已有观测数）；副作用幂等不重放；
//   - 已暂停（Paused）：视为已停在已通过位点（CurrentStep-1，无在途阶段），
//     Resume 后从下一阶段开始；
//   - 终态 / 回滚中间态：无在途阶段，由引擎恢复逻辑落定。
func migratePlanV1toV2(p *Plan) {
	// v1 观测没有 Seq 字段（零值）。统一按落盘顺序回填稳定序号，并把 NextSeq
	// 置为已见条数，保证后续新观测序号不回退、环形裁剪不影响 GateSeq 比较。
	if p.NextSeq == 0 {
		for i := range p.Observations {
			p.Observations[i].Seq = i
		}
		p.NextSeq = len(p.Observations)
	}
	switch p.State {
	case StateRunning:
		if p.CurrentStep >= 0 {
			p.PendingStep = p.CurrentStep
			// 在途阶段只接受迁移后新产生的观测（水位线 = 已见条数），
			// 旧观测在多维门控下也不可能通过（见函数末尾说明）。
			p.GateSeq = p.NextSeq
			p.CurrentStep--
		} else {
			p.PendingStep = -1
		}
	case StatePaused:
		p.PendingStep = -1
		if p.CurrentStep >= 0 {
			p.CurrentStep--
		}
	default:
		p.PendingStep = -1
	}
	// 旧观测缺版本/流量维度（Go 零值）。它们在多维门控下天然不可能通过：
	// 空版本 != 期望版本，0 流量达不到正期望占比，因此不会被静默当成功解释。
}

// SaveAll 原子写全部计划（先写临时文件再 rename）。
func (s *FileStore) SaveAll(plans []*Plan) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	f := storeFile{Version: storeSchemaVersion, Plans: plans}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal store: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("mkdir store dir: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write tmp store: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename store: %w", err)
	}
	return nil
}
