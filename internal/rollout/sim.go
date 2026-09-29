package rollout

import (
	"fmt"
	"sync"
	"time"
)

// Applier 将期望状态应用到（模拟的）目标环境。
type Applier interface {
	// Apply 设置目标的期望状态：新版本副本数与新版本流量占比（0-100），
	// 可能瞬时失败（调用方需有界重试）。实现需幂等。
	Apply(target string, version string, replicas int, percent int) error
}

// simTarget 模拟环境的单目标内部状态。
type simTarget struct {
	version     string // 最近一次下发的版本
	fromVersion string // 0 副本（回滚）前的稳定版本
	desired     int
	desiredPct  int
	actual      int
}

// SimApplier 本地模拟实现：实际承载向期望状态渐进收敛，流量占比随控制面即时生效，
// 支持注入失败、抖动、指标缺失与短暂观测缺失。
type SimApplier struct {
	mu       sync.Mutex
	targets  map[string]*simTarget
	observed int

	// 以下为可注入行为（测试/演示用）：
	Failures     int        // 前 N 次 Apply 注入瞬时失败
	StepReplicas int        // 每次 Observe 实际副本向期望收敛的步长；<=0 时立即收敛
	Jitter       func() int // 可选：对实际副本数施加抖动
	ErrorRate    float64    // Observe 上报的错误率
	DropMetrics  bool       // Observe 上报时丢弃关键指标
	MissEvery    int        // 每第 N 次 Observe 返回“目标短暂缺失”；<=0 关闭
}

// NewSimApplier 创建模拟 Applier。
func NewSimApplier() *SimApplier {
	return &SimApplier{targets: map[string]*simTarget{}, StepReplicas: 1}
}

// Apply 实现 Applier：记录期望状态，实际承载随后经 Observe 逐步收敛，
// 流量占比即时更新（模拟流量权重控制面）。
func (s *SimApplier) Apply(target string, version string, replicas int, percent int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Failures > 0 {
		s.Failures--
		return fmt.Errorf("simulated transient apply failure (%d left)", s.Failures)
	}
	t := s.targets[target]
	if t == nil {
		t = &simTarget{}
		s.targets[target] = t
	}
	// 幂等：相同期望状态重复 Apply 不产生额外副作用。
	if replicas == 0 {
		// 回滚到稳定版本：记录稳定版本以便观测期仍能上报有效版本。
		t.fromVersion = version
	} else {
		t.version = version
	}
	t.desired = replicas
	t.desiredPct = percent
	return nil
}

// Observe 生成一次实际状态观测：实际承载向期望值收敛一步，流量占比即时反映期望，
// 可叠加抖动/缺失。
func (s *SimApplier) Observe(target string) (Observation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observed++
	if s.MissEvery > 0 && s.observed%s.MissEvery == 0 {
		return Observation{}, fmt.Errorf("%w: target %q temporarily missing", ErrNotFound, target)
	}
	t := s.targets[target]
	if t == nil {
		return Observation{}, fmt.Errorf("%w: target %q", ErrNotFound, target)
	}
	step := s.StepReplicas
	if step <= 0 {
		t.actual = t.desired
	} else if t.actual < t.desired {
		t.actual += step
		if t.actual > t.desired {
			t.actual = t.desired
		}
	} else if t.actual > t.desired {
		t.actual -= step
		if t.actual < t.desired {
			t.actual = t.desired
		}
	}
	actual := t.actual
	if s.Jitter != nil {
		actual += s.Jitter()
		if actual < 0 {
			actual = 0
		}
	}
	// 生效版本：承载尚未收敛时（含回滚过程）按实际承载判断当前对外版本。
	effVersion := t.version
	if actual == 0 {
		effVersion = t.fromVersion
	}
	return Observation{
		ReportedAt:       time.Now(),
		EffectiveVersion: effVersion,
		TrafficPercent:   t.desiredPct,
		Replicas:         actual,
		ErrorRate:        s.ErrorRate,
		Samples:          100,
		HasMetrics:       !s.DropMetrics,
	}, nil
}
