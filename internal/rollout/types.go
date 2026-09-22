// Package rollout 提供本地渐进式发布编排能力。
package rollout

import "time"

// State 发布计划状态。
type State string

const (
	StatePending     State = "Pending"
	StateRunning     State = "Running"
	StatePaused      State = "Paused"
	StateRollingBack State = "RollingBack"
	StateSucceeded   State = "Succeeded"
	StateRolledBack  State = "RolledBack"
	StateFailed      State = "Failed"
)

// Terminal 判断状态是否为终态。
func (s State) Terminal() bool {
	switch s {
	case StateSucceeded, StateRolledBack, StateFailed:
		return true
	}
	return false
}

// StrategyKind 发布策略类型。
type StrategyKind string

const (
	// StrategyCanary 金丝雀：按批次百分比逐步放量，每批需通过健康门控。
	StrategyCanary StrategyKind = "canary"
	// StrategyBlueGreen 蓝绿：先全量部署新版本到 green，观测通过后一次性切换流量。
	StrategyBlueGreen StrategyKind = "bluegreen"
)

// Step 一个发布阶段。
type Step struct {
	Index    int    `json:"index"`
	Name     string `json:"name"`
	Percent  int    `json:"percent"` // 新版本目标流量/副本百分比
	Replicas int    `json:"replicas"`
}

// HealthPolicy 健康门控策略。
type HealthPolicy struct {
	MaxErrorRate      float64       `json:"maxErrorRate"`      // 允许的最大错误率
	FailureBudget     int           `json:"failureBudget"`     // 允许的连续不健康判定次数
	ObservationMaxAge time.Duration `json:"observationMaxAge"` // 观测最大有效期
	MinSamples        int           `json:"minSamples"`        // 判定所需最小样本数
}

// Limits 资源与重试上限。
type Limits struct {
	MaxPlans        int `json:"maxPlans"`
	MaxRetries      int `json:"maxRetries"`
	MaxObservations int `json:"maxObservations"` // 每个计划保留的观测上限（环形）
}

// Observation 一次实际状态上报。
type Observation struct {
	ReportID   string    `json:"reportID"`
	ReportedAt time.Time `json:"reportedAt"`
	Replicas   int       `json:"replicas"`   // 实际运行的新版本副本数
	ErrorRate  float64   `json:"errorRate"`  // 观测到的错误率
	Samples    int       `json:"samples"`    // 样本数
	HasMetrics bool      `json:"hasMetrics"` // 关键指标是否齐全
}

// 判定结论代码，便于调用方区分失败原因。
const (
	VerdictOK                  = "ok"
	VerdictStaleObservation    = "stale-observation"
	VerdictMissingMetric       = "missing-metric"
	VerdictInsufficientSamples = "insufficient-samples"
	VerdictErrorRateExceeded   = "error-rate-exceeded"
)

// Verdict 一次判定结论及依据。
type Verdict struct {
	At      time.Time `json:"at"`
	Healthy bool      `json:"healthy"`
	Code    string    `json:"code"`
	Reason  string    `json:"reason"`
}

// Plan 发布计划（持久化实体）。
type Plan struct {
	ID            string        `json:"id"`
	Target        string        `json:"target"`
	Strategy      StrategyKind  `json:"strategy"`
	FromVersion   string        `json:"fromVersion"`
	ToVersion     string        `json:"toVersion"`
	TotalReplicas int           `json:"totalReplicas"`
	Steps         []Step        `json:"steps"`
	CurrentStep   int           `json:"currentStep"` // 当前所处阶段下标，-1 表示未开始
	State         State         `json:"state"`
	Health        HealthPolicy  `json:"health"`
	FailStreak    int           `json:"failStreak"`
	Observations  []Observation `json:"observations"`
	SeenReports   []string      `json:"seenReports"` // 已处理的上报 ID（幂等）
	AppliedCmds   []string      `json:"appliedCmds"` // 已应用的命令 ID（幂等）
	Verdicts      []Verdict     `json:"verdicts"`    // 判定依据审计
	UpdatedAt     time.Time     `json:"updatedAt"`
}
