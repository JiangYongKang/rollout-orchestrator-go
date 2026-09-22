package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/highcumontoa/rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/rollout-orchestrator-go/internal/fsx"
)

// Store 以 JSON 文件持久化 Plan。
// 每个 plan 一个文件，写入为"临时文件 + rename + 目录 sync"的原子写，
// 进程在任意时刻被杀都不会读到半更新。
type Store struct {
	dir string
}

func New(dataDir string) (*Store, error) {
	s := &Store{dir: filepath.Join(dataDir, "plans")}
	if err := mkdirAll(s.dir); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) path(id string) string {
	return filepath.Join(s.dir, id+".json")
}

// Create 要求目标文件尚不存在，防止同一个 plan ID 被覆盖创建。
func (s *Store) Create(ctx context.Context, p *domain.Plan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil || p.ID == "" {
		return fmt.Errorf("%w: plan id required", domain.ErrInvalidArgument)
	}
	exists, err := fsx.ReadJSON(s.path(p.ID), &domain.Plan{})
	if err != nil {
		return fmt.Errorf("%w: plan %s unreadable: %v", domain.ErrCorruptData, p.ID, err)
	}
	if exists {
		return fmt.Errorf("%w: %s", domain.ErrPlanAlreadyExists, p.ID)
	}
	p.SchemaVersion = domain.SchemaVersion
	if err := validate(p); err != nil {
		return err
	}
	return fsx.WriteJSON(s.path(p.ID), p)
}

func (s *Store) Save(ctx context.Context, p *domain.Plan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil || p.ID == "" {
		return fmt.Errorf("%w: plan id required", domain.ErrInvalidArgument)
	}
	if err := validate(p); err != nil {
		return err
	}
	p.SchemaVersion = domain.SchemaVersion
	return fsx.WriteJSON(s.path(p.ID), p)
}

func (s *Store) Get(ctx context.Context, id string) (*domain.Plan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var p domain.Plan
	exists, err := fsx.ReadJSON(s.path(id), &p)
	if err != nil {
		return nil, fmt.Errorf("%w: plan %s data: %v", domain.ErrCorruptData, id, err)
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", domain.ErrPlanNotFound, id)
	}
	if err := validate(&p); err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Store) List(ctx context.Context) ([]*domain.Plan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := readDirNames(s.dir)
	if err != nil {
		return nil, err
	}
	out := make([]*domain.Plan, 0, len(entries))
	for _, name := range entries {
		if !strings.HasSuffix(name, ".json") || strings.HasPrefix(name, ".tmp-") {
			continue
		}
		id := strings.TrimSuffix(name, ".json")
		p, err := s.Get(ctx, id)
		if err != nil {
			return nil, err // 旧版本/损坏数据必须被明确拒绝，而不是跳过
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// validate 拒绝旧版本 schema 与按默认值无法安全解释的残缺记录。
func validate(p *domain.Plan) error {
	if p.SchemaVersion != domain.SchemaVersion {
		return fmt.Errorf("%w: plan %s schema=%d supported=%d",
			domain.ErrUnsupportedVersion, p.ID, p.SchemaVersion, domain.SchemaVersion)
	}
	if p.TargetID == "" || p.DesiredVer == "" || len(p.Steps) == 0 {
		return fmt.Errorf("%w: plan %s missing required fields", domain.ErrCorruptData, p.ID)
	}
	if p.State == "" {
		return fmt.Errorf("%w: plan %s has empty state", domain.ErrCorruptData, p.ID)
	}
	if p.StepIdx < 0 || p.StepIdx > len(p.Steps) {
		return fmt.Errorf("%w: plan %s step index out of range", domain.ErrCorruptData, p.ID)
	}
	return nil
}

func (s *Store) Close() error { return nil }
