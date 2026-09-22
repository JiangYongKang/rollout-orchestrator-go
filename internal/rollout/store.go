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
// 旧版本数据必须被明确拒绝，不得按默认值静默解释。
const storeSchemaVersion = 1

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
	if f.Version != storeSchemaVersion {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrStoreVersion, f.Version, storeSchemaVersion)
	}
	for _, p := range f.Plans {
		if p == nil || p.ID == "" {
			return nil, fmt.Errorf("%w: plan with empty id", ErrStoreCorrupt)
		}
	}
	return f.Plans, nil
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
