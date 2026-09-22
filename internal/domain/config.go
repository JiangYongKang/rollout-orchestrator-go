package domain

import (
	"fmt"
	"time"
)

// SchemaVersion 是磁盘数据的当前格式版本。
const SchemaVersion = 1

// DefaultConfig 返回一套可本地演示的健康门控参数。
func DefaultConfig() Config {
	return Config{
		StaleThreshold:  5 * time.Second,
		TolerancePct:    2, // canary 允许 ±2 个百分点的观测抖动
		FailureBudget:   2, // 健康失败计数上限，超过即自动回滚
		MaxAttempts:     3, // 单步副作用（Apply）的有界重试次数
		CriticalMetrics: []string{"error_rate", "latency_p99_ms"},
		MetricBounds: map[string][2]float64{
			"error_rate":     {0, 0.05},
			"latency_p99_ms": {0, 500},
		},
	}
}

// Validate 检查配置本身是否可执行，避免按零值静默解释。
func (c Config) Validate() error {
	if c.StaleThreshold <= 0 {
		return fmt.Errorf("%w: stale_threshold must be > 0", ErrInvalidArgument)
	}
	if c.TolerancePct < 0 || c.TolerancePct > 50 {
		return fmt.Errorf("%w: tolerance_pct out of range", ErrInvalidArgument)
	}
	if c.FailureBudget < 0 {
		return fmt.Errorf("%w: failure_budget must be >= 0", ErrInvalidArgument)
	}
	if c.MaxAttempts < 1 {
		return fmt.Errorf("%w: max_attempts must be >= 1", ErrInvalidArgument)
	}
	for _, m := range c.CriticalMetrics {
		b, ok := c.MetricBounds[m]
		if !ok {
			return fmt.Errorf("%w: critical metric %q has no bounds", ErrInvalidArgument, m)
		}
		if b[0] > b[1] {
			return fmt.Errorf("%w: metric %q bounds invalid", ErrInvalidArgument, m)
		}
	}
	return nil
}
