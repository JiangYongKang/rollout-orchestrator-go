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
	// StrategyBatch 固定副本分批：每批增加固定数量的新版本副本，批次数与每批副本数可配置。
	StrategyBatch StrategyKind = "batch"
)

// Step 一个发布阶段。
type Step struct {
	Index    int    `json:"index"`
	Name     string `json:"name"`
	Version  string `json:"version,omitempty"` // 该阶段期望生效版本；空值表示用计划的 ToVersion
	Percent  int    `json:"percent"`           // 该阶段期望的新版本流量占比（百分比）
	Replicas int    `json:"replicas"`          // 该阶段期望的新版本副本数（累计）
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
	Seq              int       `json:"seq"` // 计划内单调递增的观测序号（环形裁剪不影响序号比较）
	ReportID         string    `json:"reportID"`
	ReportedAt       time.Time `json:"reportedAt"`
	EffectiveVersion string    `json:"effectiveVersion"` // 当前实际生效版本（多维门控：必须等于阶段期望版本）
	TrafficPercent   int       `json:"trafficPercent"`   // 当前实际新版本流量占比（百分比）
	Replicas         int       `json:"replicas"`         // 实际运行的新版本副本数
	ErrorRate        float64   `json:"errorRate"`        // 观测到的错误率
	Samples          int       `json:"samples"`          // 样本数
	HasMetrics       bool      `json:"hasMetrics"`       // 关键指标是否齐全
}

// 判定结论代码，便于调用方区分失败原因。
const (
	VerdictOK                  = "ok"
	VerdictStaleObservation    = "stale-observation"
	VerdictMissingMetric       = "missing-metric"
	VerdictInsufficientSamples = "insufficient-samples"
	VerdictErrorRateExceeded   = "error-rate-exceeded"
	VerdictVersionMismatch     = "version-mismatch"       // 实际生效版本 != 阶段期望版本
	VerdictTrafficNotReached   = "traffic-not-reached"    // 实际流量占比未达阶段期望
	VerdictNotConverged        = "replicas-not-converged" // 实际副本数未收敛到阶段期望
)

// Verdict 一次判定结论及依据（含每个维度的期望值与观测值，便于审计）。
type Verdict struct {
	At      time.Time `json:"at"`
	Step    int       `json:"step"` // 判定针对的阶段下标
	Healthy bool      `json:"healthy"`
	Code    string    `json:"code"`
	Reason  string    `json:"reason"`
	// 各维度判定依据：
	ReportID     string `json:"reportID"`
	WantVersion  string `json:"wantVersion"`
	GotVersion   string `json:"gotVersion"`
	WantPercent  int    `json:"wantPercent"`
	GotPercent   int    `json:"gotPercent"`
	WantReplicas int    `json:"wantReplicas"`
	GotReplicas  int    `json:"gotReplicas"`
}

// Plan 发布计划（持久化实体）。
type Plan struct {
	ID            string       `json:"id"`
	Target        string       `json:"target"`
	Strategy      StrategyKind `json:"strategy"`
	FromVersion   string       `json:"fromVersion"`
	ToVersion     string       `json:"toVersion"`
	TotalReplicas int          `json:"totalReplicas"`
	BatchSizes    []int        `json:"batchSizes,omitempty"` // StrategyBatch：每批新增的新版本副本数
	Steps         []Step       `json:"steps"`
	CurrentStep   int          `json:"currentStep"` // 最后一个已通过阶段下标，-1 表示尚无阶段通过
	PendingStep   int          `json:"pendingStep"` // 已应用期望、等待门控的阶段下标；-1 表示无在途阶段
	// PendingApplied 标记在途阶段的期望状态是否已确认成功下发（Apply 成功并已落盘）。
	// false 表示“已登记在途位点、但下发未确认”：崩溃可能发生在下发之前/之中，
	// 重启（或同进程超时重入）后必须幂等补发同一期望；true 表示下发已确认，
	// 重入只等待门控、绝不重复放量。Apply 本身幂等，补发同一期望不产生额外副作用。
	PendingApplied bool          `json:"pendingApplied"`
	GateSeq        int           `json:"gateSeq"` // 当前在途阶段要求的最早观测序号（防旧观测跨阶段通过）
	NextSeq        int           `json:"nextSeq"` // 下一条观测的序号（单调递增，环形裁剪不影响 GateSeq）
	State          State         `json:"state"`
	Health         HealthPolicy  `json:"health"`
	FailStreak     int           `json:"failStreak"`
	Observations   []Observation `json:"observations"`
	SeenReports    []string      `json:"seenReports"` // 已处理的上报 ID（幂等）
	AppliedCmds    []string      `json:"appliedCmds"` // 已应用的命令 ID（幂等）
	Verdicts       []Verdict     `json:"verdicts"`    // 判定依据审计
	UpdatedAt      time.Time     `json:"updatedAt"`
}

// expectedVersion 返回阶段期望生效版本（空版本回落到计划目标版本）。
func (p *Plan) expectedVersion(s Step) string {
	if s.Version != "" {
		return s.Version
	}
	return p.ToVersion
}
