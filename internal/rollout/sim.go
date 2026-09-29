package rollout

import (
	"fmt"
	"sync"
	"time"
)

// Applier 将期望状态应用到（模拟的）目标环境。
type Applier interface {
	// Apply 设置目标上指定版本的期望副本数与期望流量占比，可能瞬时失败（调用方需有界重试）。
	Apply(target string, version string, replicas int, trafficPercent int) error
}

// simTarget 模拟环境的单目标内部状态。
type simTarget struct {
	version        string
	desired        int
	actual         int
	desiredTraffic int
	actualTraffic  int
}

// SimApplier 本地模拟实现：实际状态向期望状态渐进收敛，
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

// Apply 实现 Applier：记录期望副本数与期望流量，实际状态随后经 Observe 逐步收敛。
func (s *SimApplier) Apply(target string, version string, replicas int, trafficPercent int) error {
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
	t.version = version
	t.desired = replicas
	t.desiredTraffic = trafficPercent
	return nil
}

// converge 辅助：actual 向 desired 按步长收敛。
func converge(actual, desired, step int) int {
	if step <= 0 {
		return desired
	}
	if actual < desired {
		actual += step
		if actual > desired {
			actual = desired
		}
	} else if actual > desired {
		actual -= step
		if actual < desired {
			actual = desired
		}
	}
	return actual
}

// Observe 生成一次实际状态观测：副本与流量分别向期望值收敛一步，可叠加抖动/缺失。
// 返回观测三维判定输入：生效版本（新版本副本>0 即视为新版本已生效）、流量占比、实际副本数。
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
	t.actual = converge(t.actual, t.desired, s.StepReplicas)
	// 流量按相对差距的约一半收敛（至少 1 个百分点），与副本收敛耗时相当。
	diff := t.desiredTraffic - t.actualTraffic
	if diff != 0 {
		d := diff / 2
		if d == 0 {
			if diff > 0 {
				d = 1
			} else {
				d = -1
			}
		}
		t.actualTraffic += d
		if (diff > 0 && t.actualTraffic > t.desiredTraffic) || (diff < 0 && t.actualTraffic < t.desiredTraffic) {
			t.actualTraffic = t.desiredTraffic
		}
	}
	actual := t.actual
	if s.Jitter != nil {
		actual += s.Jitter()
		if actual < 0 {
			actual = 0
		}
	}
	// 新版本副本为 0 时（如回滚到 FromVersion），生效版本上报旧版本。
	active := t.version
	if t.actual == 0 {
		active = ""
	}
	return Observation{
		ReportedAt:     time.Now(),
		Replicas:       actual,
		ActiveVersion:  active,
		HasVersion:     true,
		TrafficPercent: t.actualTraffic,
		HasTraffic:     true,
		ErrorRate:      s.ErrorRate,
		Samples:        100,
		HasMetrics:     !s.DropMetrics,
	}, nil
}
