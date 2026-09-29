package rollout

import "errors"

// 可区分的错误原因，调用方可用 errors.Is 判定。
var (
	ErrNotFound          = errors.New("plan not found")
	ErrInvalidTransition = errors.New("invalid state transition")
	ErrPlanFinished      = errors.New("plan already in terminal state")
	ErrDuplicateCommand  = errors.New("duplicate command")
	ErrDuplicateReport   = errors.New("duplicate observation report")
	ErrStaleObservation  = errors.New("observation stale or missing")
	ErrMissingMetric     = errors.New("required metric missing")
	ErrHealthGate        = errors.New("health gate rejected")
	ErrNotConverged      = errors.New("actual state not converged to desired")
	ErrStoreVersion      = errors.New("unsupported store schema version")
	ErrStoreCorrupt      = errors.New("store data corrupt")
	ErrLimitExceeded     = errors.New("resource limit exceeded")
	ErrRetryExhausted    = errors.New("retry budget exhausted")
	ErrInvalidArgument   = errors.New("invalid argument")
	ErrTargetBusy        = errors.New("target already has an active rollout")
)
