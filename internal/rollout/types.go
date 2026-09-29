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
	// StrategyBatch 固定副本数分批：每批新增固定数量的新版本副本，
	// 批次数与每批副本数由 Plan.BatchReplicas 显式配置。
	StrategyBatch StrategyKind = "batch"
)

// Step 一个发布阶段。
type Step struct {
	Index    int    `json:"index"`
	Name     string `json:"name"`
	Percent  int    `json:"percent"` // 该阶段期望的新版本流量占比（0-100）
	Replicas int    `json:"replicas"`
	// Version 该阶段生效版本（新版本步为 ToVersion；回滚后的旧版本步为 FromVersion）。
	Version string `json:"version"`
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
	// ActiveVersion 当前实际生效版本；HasVersion=false 表示该维度暂未上报。
	ActiveVersion string `json:"activeVersion"`
	HasVersion    bool   `json:"hasVersion"`
	// TrafficPercent 当前实际新版本流量占比（0-100）；HasTraffic=false 表示该维度暂未上报。
	TrafficPercent int  `json:"trafficPercent"`
	HasTraffic     bool `json:"hasTraffic"`
	// GateSeq 该上报可参与判定的阶段序号（由引擎在进入阶段时写入，旧观测无法判定后续阶段）。
	GateSeq int64 `json:"gateSeq"`
}

// 判定结论代码，便于调用方区分失败原因。
const (
	VerdictOK                  = "ok"
	VerdictStaleObservation    = "stale-observation"
	VerdictMissingMetric       = "missing-metric"
	VerdictInsufficientSamples = "insufficient-samples"
	VerdictErrorRateExceeded   = "error-rate-exceeded"
	// 以下为多维阶段门控（版本/流量/承载三维度）的结论代码。
	VerdictNoEligibleObservation = "no-eligible-observation" // 尚无属于当前阶段的观测
	VerdictVersionMismatch       = "version-mismatch"        // 生效版本不是该阶段期望版本
	VerdictTrafficNotReached     = "traffic-not-reached"     // 流量占比未达该阶段期望
	VerdictCapacityNotConverged  = "capacity-not-converged"  // 实际承载未收敛到该阶段期望
)

// GateResult 多维阶段门控结论：版本/流量/承载必须同时对上才算通过。
type GateResult struct {
	Verdict
	StepIndex int    `json:"stepIndex"`
	ReportID  string `json:"reportID"` // 判定所依据的那次上报
}

// StepInfo 记录当前待判定阶段与其判定代次：只有 GateSeq 匹配的观测才能判定本阶段，
// 保证前面阶段的观测不能拿来通过后面阶段，且一次观测不可能同时通过两个阶段。
type StepInfo struct {
	// CompletedStep 已通过门控的最后一个阶段下标；-1 表示尚无阶段通过。
	CompletedStep int `json:"completedStep"`
	// GateStep 正在等待判定的阶段下标；-1 表示当前没有进行中的阶段。
	GateStep int `json:"gateStep"`
	// GateSeq 单调递增的判定代次，每次（重新）进入一个阶段时 +1。
	GateSeq int64 `json:"gateSeq"`
}

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
	// BatchReplicas batch 策略配置：每个元素是该批“新增”的新版本副本数。
	// 非 batch 策略为空；创建时严格校验，绝不回退默认值。
	BatchReplicas []int `json:"batchReplicas,omitempty"`
	// Gate 阶段门控进度与判定代次（多维判定、防跨阶段通过、防重启跳过阶段）。
	Gate StepInfo `json:"gate"`
	UpdatedAt     time.Time     `json:"updatedAt"`
}
