package domain

import "testing"

func TestStateMachineTransitions(t *testing.T) {
	allowed := []struct{ from, to PlanState }{
		{StatePending, StatePromoting}, {StatePending, StatePaused},
		{StatePending, StateRollingBack}, {StatePending, StateFailed},
		{StatePromoting, StateGated}, {StatePromoting, StateSucceeded},
		{StatePromoting, StatePaused}, {StatePromoting, StateRollingBack},
		{StateGated, StatePromoting}, {StateGated, StatePaused},
		{StateGated, StateRollingBack}, {StatePaused, StatePending},
		{StatePaused, StatePromoting}, {StatePaused, StateGated},
		{StateRollingBack, StateRolledBack}, {StateRollingBack, StateFailed},
	}
	for _, c := range allowed {
		if !CanTransition(c.from, c.to) {
			t.Errorf("allowed transition rejected: %s -> %s", c.from, c.to)
		}
	}
	forbidden := []struct{ from, to PlanState }{
		{StateSucceeded, StatePromoting}, {StateRolledBack, StateRollingBack},
		{StateFailed, StateGated}, {StatePending, StateGated},
		{StatePending, StateSucceeded}, {StateGated, StateSucceeded},
		{StatePromoting, StatePending}, {StateRollingBack, StatePaused},
	}
	for _, c := range forbidden {
		if CanTransition(c.from, c.to) {
			t.Errorf("illegal transition allowed: %s -> %s", c.from, c.to)
		}
	}
	if !StateSucceeded.Terminal() || !StateRolledBack.Terminal() || !StateFailed.Terminal() {
		t.Fatal("terminal flags wrong")
	}
	if StateGated.Terminal() {
		t.Fatal("Gated must not be terminal")
	}
}
