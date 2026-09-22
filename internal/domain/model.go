package domain

import "time"

type StrategyKind string

const (
	StrategyCanary StrategyKind = "canary"
	StrategyWave   StrategyKind = "wave"
)

// Step 是一次放量：把目标推进到 Desired（百分比 或 副本数）。
type Step struct {
	Index         int       `json:"index"`
	Desired       int       `json:"desired"`
	AttemptToken  string    `json:"attempt_token,omitempty"` // 在途/最近一次副作用 token（崩溃重放用）
	AppliedToken  string    `json:"applied_token,omitempty"` // 已确认生效的副作用 token
	Converged     bool      `json:"converged"`
	Healthy       bool      `json:"healthy"`
	Attempts      int       `json:"attempts"`
	LastAppliedAt time.Time `json:"last_applied_at,omitempty"`
}

// ActualState 是模拟目标的观测实际状态。
type ActualState struct {
	Version   string    `json:"version"`
	Percent   int       `json:"percent"`
	Replicas  int       `json:"replicas"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Metric struct {
	Name   string  `json:"name"`
	Value  float64 `json:"value"`
	Exists bool    `json:"exists"`
}

// Observation 是外部上报的一次观测（同一 BatchID 去重）。
type Observation struct {
	BatchID    string            `json:"batch_id"`
	ObservedAt time.Time         `json:"observed_at"`
	Actual     ActualState       `json:"actual"`
	Metrics    map[string]Metric `json:"metrics"`
}

type EventKind string

const (
	EventCreated        EventKind = "created"
	EventAdvanced       EventKind = "advanced"
	EventPaused         EventKind = "paused"
	EventResumed        EventKind = "resumed"
	EventObserved       EventKind = "observed"
	EventConverged      EventKind = "converged"
	EventHealthChecked  EventKind = "health_checked"
	EventBudgetConsumed EventKind = "budget_consumed"
	EventApplyFailed    EventKind = "apply_failed"
	EventRollingBack    EventKind = "rolling_back"
	EventRolledBack     EventKind = "rolled_back"
	EventSucceeded      EventKind = "succeeded"
	EventFailed         EventKind = "failed"
	EventRejected       EventKind = "rejected"
)

type Event struct {
	Seq    int                    `json:"seq"`
	At     time.Time              `json:"at"`
	Kind   EventKind              `json:"kind"`
	Reason string                 `json:"reason"`
	Detail map[string]interface{} `json:"detail,omitempty"`
}

// Config 是每个计划可配置的门控/上限参数。
type Config struct {
	StaleThreshold  time.Duration         `json:"stale_threshold"`
	TolerancePct    int                   `json:"tolerance_pct"`
	FailureBudget   int                   `json:"failure_budget"`
	MaxAttempts     int                   `json:"max_attempts"`
	CriticalMetrics []string              `json:"critical_metrics"`
	MetricBounds    map[string][2]float64 `json:"metric_bounds"`
}

// Plan 是持久化的聚合根。
type Plan struct {
	SchemaVersion    int          `json:"schema_version"`
	ID               string       `json:"id"`
	TargetID         string       `json:"target_id"`
	Strategy         StrategyKind `json:"strategy"`
	DesiredVer       string       `json:"desired_version"`
	StableVer        string       `json:"stable_version"`
	TotalReplicas    int          `json:"total_replicas"`
	Steps            []Step       `json:"steps"`
	StepIdx          int          `json:"step_idx"`
	State            PlanState    `json:"state"`
	ResumeFrom       PlanState    `json:"resume_from,omitempty"`
	BudgetLeft       int          `json:"budget_left"`
	BudgetBatchID    string       `json:"budget_batch_id,omitempty"` // 已消耗过预算的观测批次，防重复计数
	LastBatchID      string       `json:"last_batch_id,omitempty"`
	RollbackToken    string       `json:"rollback_token,omitempty"`
	RollbackAttempts int          `json:"rollback_attempts"`
	Config           Config       `json:"config"`
	LastObs          *Observation `json:"last_obs,omitempty"`
	CreatedAt        time.Time    `json:"created_at"`
	UpdatedAt        time.Time    `json:"updated_at"`
	Events           []Event      `json:"events"`
}

func (p *Plan) CurrentStep() *Step {
	if p.StepIdx < 0 || p.StepIdx >= len(p.Steps) {
		return nil
	}
	return &p.Steps[p.StepIdx]
}

func (p *Plan) AddEvent(kind EventKind, reason string, detail map[string]interface{}, now time.Time) Event {
	ev := Event{Seq: len(p.Events) + 1, At: now, Kind: kind, Reason: reason, Detail: detail}
	p.Events = append(p.Events, ev)
	p.UpdatedAt = now
	return ev
}
