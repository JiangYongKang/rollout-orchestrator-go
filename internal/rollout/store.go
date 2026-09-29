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

// storeSchemaVersion 当前持久化模式版本。
// 旧版本数据必须被明确拒绝或显式迁移，不得按默认值静默解释。
//
// v2 在 v1 基础上增加了：Step.Version（阶段期望版本）、Observation 的版本/流量
// 维度与 GateSeq、Plan.Gate（阶段门控进度）、Plan.BatchReplicas（分批策略配置）。
const storeSchemaVersion = 2

// v1SchemaVersion 上一版模式：含按比例/蓝绿策略，但判定只看一个数字。
const v1SchemaVersion = 1

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
		for _, p := range f.Plans {
			if err := validateV2(p); err != nil {
				return nil, err
			}
		}
		return f.Plans, nil
	case v1SchemaVersion:
		plans := make([]*Plan, 0, len(f.Plans))
		for _, p := range f.Plans {
			mp, err := migrateV1(p)
			if err != nil {
				return nil, err
			}
			plans = append(plans, mp)
		}
		return plans, nil
	default:
		return nil, fmt.Errorf("%w: got %d, want %d", ErrStoreVersion, f.Version, storeSchemaVersion)
	}
}

// validateV2 校验 v2 计划内部一致性；违反时明确拒绝（ErrStoreCorrupt），
// 绝不按缺省值解释后继续推进。
func validateV2(p *Plan) error {
	if p == nil || p.ID == "" {
		return fmt.Errorf("%w: plan with empty id", ErrStoreCorrupt)
	}
	if len(p.Steps) == 0 {
		return fmt.Errorf("%w: plan %s has no steps", ErrStoreCorrupt, p.ID)
	}
	if p.CurrentStep < -1 || p.CurrentStep >= len(p.Steps) {
		return fmt.Errorf("%w: plan %s currentStep %d out of range", ErrStoreCorrupt, p.ID, p.CurrentStep)
	}
	for i, st := range p.Steps {
		if st.Index != i {
			return fmt.Errorf("%w: plan %s step %d bad index %d", ErrStoreCorrupt, p.ID, i, st.Index)
		}
		if st.Version == "" {
			return fmt.Errorf("%w: plan %s step %d missing expected version", ErrStoreCorrupt, p.ID, i)
		}
		if st.Percent < 0 || st.Percent > 100 || st.Replicas < 0 || st.Replicas > p.TotalReplicas {
			return fmt.Errorf("%w: plan %s step %d bad desired state percent=%d replicas=%d",
				ErrStoreCorrupt, p.ID, i, st.Percent, st.Replicas)
		}
	}
	if p.Gate.CompletedStep < -1 || p.Gate.CompletedStep >= len(p.Steps) {
		return fmt.Errorf("%w: plan %s completedStep %d out of range", ErrStoreCorrupt, p.ID, p.Gate.CompletedStep)
	}
	if p.Gate.GateStep < -1 || p.Gate.GateStep >= len(p.Steps) {
		return fmt.Errorf("%w: plan %s gateStep %d out of range", ErrStoreCorrupt, p.ID, p.Gate.GateStep)
	}
	return nil
}

// migrateV1 把 v1 计划显式迁移到 v2：
//   - 结构性不兼容（空 id / 空步骤 / 越界下标）-> ErrStoreVersion，明确拒绝；
//   - 可迁移：补全每个阶段的期望版本；清掉无法用于多维判定的旧观测（其
//     版本/流量维度本来就不存在，按缺省值解释会误放行）；进行中的计划
//     从“当前阶段”重新进入门控（CompletedStep=CurrentStep-1, GateStep=-1,
//     GateSeq+1），重启后凭新观测重新完成三维判定，不会跳过阶段。
func migrateV1(p *Plan) (*Plan, error) {
	if p == nil || p.ID == "" {
		return nil, fmt.Errorf("%w: v1 plan with empty id cannot migrate", ErrStoreVersion)
	}
	if len(p.Steps) == 0 {
		return nil, fmt.Errorf("%w: v1 plan %s has no steps; refusing to migrate", ErrStoreVersion, p.ID)
	}
	if p.CurrentStep < -1 || p.CurrentStep >= len(p.Steps) {
		return nil, fmt.Errorf("%w: v1 plan %s currentStep %d out of range", ErrStoreVersion, p.ID, p.CurrentStep)
	}
	for i := range p.Steps {
		p.Steps[i].Version = p.ToVersion
	}
	// 旧观测没有版本/流量维度，无法参与 v2 门控；丢弃并要求重新上报，
	// 避免按零值/缺省值静默解释造成提前放行。
	p.Observations = nil
	p.SeenReports = nil

	if p.State.Terminal() {
		p.Gate = StepInfo{CompletedStep: len(p.Steps) - 1, GateStep: -1, GateSeq: int64(len(p.Steps))}
	} else {
		completed := p.CurrentStep - 1 // 已应用但未经多维门控确认的阶段不算完成
		if completed < -1 {
			completed = -1
		}
		p.Gate = StepInfo{
			CompletedStep: completed,
			GateStep:      -1,
			GateSeq:       int64(p.CurrentStep + 1),
		}
	}
	return p, nil
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
