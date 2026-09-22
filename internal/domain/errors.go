package domain

import "errors"

// 可区分的哨兵错误：调用方可用 errors.Is 判定失败原因。
var (
	ErrPlanNotFound       = errors.New("plan not found")
	ErrPlanAlreadyExists  = errors.New("plan already exists")
	ErrInvalidTransition  = errors.New("invalid state transition")
	ErrDuplicateCommand   = errors.New("duplicate command rejected")
	ErrPlanFinished       = errors.New("plan already finished")
	ErrNotPaused          = errors.New("plan is not paused")
	ErrAlreadyPaused      = errors.New("plan is already paused")
	ErrPlanPaused         = errors.New("plan is paused; resume before advancing")
	ErrTargetBusy         = errors.New("target has an active plan")
	ErrNotConverged       = errors.New("actual state not converged yet")
	ErrObservationStale   = errors.New("observation is stale")
	ErrObservationMissing = errors.New("observation is missing")
	ErrMetricMissing      = errors.New("critical metric missing")
	ErrUnhealthy          = errors.New("health gate rejected")
	ErrBudgetExhausted    = errors.New("failure budget exhausted")
	ErrRetryExhausted     = errors.New("retry limit exhausted")
	ErrSideEffectFailed   = errors.New("side effect apply failed")
	ErrUnsupportedVersion = errors.New("unsupported persisted schema version")
	ErrCorruptData        = errors.New("persisted data is corrupt")
	ErrCapacityExceeded   = errors.New("resource capacity exceeded")
	ErrInvalidArgument    = errors.New("invalid argument")
)
