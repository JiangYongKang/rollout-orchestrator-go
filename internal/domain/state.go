package domain

// PlanState 是所有发布策略共享的状态语义。
//
//	Pending -> Promoting -> Gated -> Promoting -> ... -> Succeeded
//	    \          |            \-> Paused
//	     \-> Paused \-> RollingBack -> RolledBack (terminal)
//	                 \-> Failed (terminal, 超过重试/资源上限)
type PlanState string

const (
	StatePending     PlanState = "Pending"
	StatePromoting   PlanState = "Promoting"
	StateGated       PlanState = "Gated"
	StatePaused      PlanState = "Paused"
	StateRollingBack PlanState = "RollingBack"
	StateSucceeded   PlanState = "Succeeded"
	StateRolledBack  PlanState = "RolledBack"
	StateFailed      PlanState = "Failed"
)

func (s PlanState) Terminal() bool {
	return s == StateSucceeded || s == StateRolledBack || s == StateFailed
}

// CanTransition 描述共享状态机允许的迁移。
func CanTransition(from, to PlanState) bool {
	_, ok := transitions[from][to]
	return ok
}

var transitions = map[PlanState]map[PlanState]struct{}{
	StatePending: {
		StatePromoting:   {},
		StatePaused:      {},
		StateFailed:      {},
		StateRollingBack: {},
	},
	StatePromoting: {
		StateGated:       {},
		StateSucceeded:   {},
		StatePaused:      {},
		StateRollingBack: {},
		StateFailed:      {},
	},
	StateGated: {
		StatePromoting:   {},
		StatePaused:      {},
		StateRollingBack: {},
		StateFailed:      {},
	},
	StatePaused: {
		StatePending:     {},
		StatePromoting:   {},
		StateGated:       {},
		StateRollingBack: {},
		StateFailed:      {},
	},
	StateRollingBack: {
		StateRolledBack: {},
		StateFailed:     {},
	},
	StateSucceeded:  {},
	StateRolledBack: {},
	StateFailed:     {},
}
