package rollout

import (
	"fmt"
	"time"
)

// Evaluate 基于最新一次观测做健康判定，返回结论与可解释原因。
// 规则（任一不满足即不健康，且不得继续推进）：
//  1. 无观测或最新观测已过期 -> stale-observation
//  2. 关键指标缺失           -> missing-metric
//  3. 样本数不足             -> insufficient-samples
//  4. 错误率超过阈值         -> error-rate-exceeded
func Evaluate(p *Plan, now time.Time) Verdict {
	v := Verdict{At: now}
	if len(p.Observations) == 0 {
		v.Code = VerdictStaleObservation
		v.Reason = "no observation reported yet"
		return v
	}
	obs := p.Observations[len(p.Observations)-1]
	age := now.Sub(obs.ReportedAt)
	if age > p.Health.ObservationMaxAge {
		v.Code = VerdictStaleObservation
		v.Reason = fmt.Sprintf("latest observation %s old exceeds max age %s", age.Round(time.Millisecond), p.Health.ObservationMaxAge)
		return v
	}
	if !obs.HasMetrics {
		v.Code = VerdictMissingMetric
		v.Reason = "required metrics missing in latest observation"
		return v
	}
	if obs.Samples < p.Health.MinSamples {
		v.Code = VerdictInsufficientSamples
		v.Reason = fmt.Sprintf("samples %d below minimum %d", obs.Samples, p.Health.MinSamples)
		return v
	}
	if obs.ErrorRate > p.Health.MaxErrorRate {
		v.Code = VerdictErrorRateExceeded
		v.Reason = fmt.Sprintf("error rate %.4f exceeds max %.4f (samples=%d)", obs.ErrorRate, p.Health.MaxErrorRate, obs.Samples)
		return v
	}
	v.Healthy = true
	v.Code = VerdictOK
	v.Reason = fmt.Sprintf("error rate %.4f within max %.4f, samples=%d, age=%s", obs.ErrorRate, p.Health.MaxErrorRate, obs.Samples, age.Round(time.Millisecond))
	return v
}

// EvaluateGate 多维阶段门控：一个阶段算完成，必须同时确认
//  1. 当前生效版本 == 该阶段期望版本（Step.Version）
//  2. 新版本流量占比 == 该阶段期望（Step.Percent）
//  3. 实际承载（新版本副本数）== 该阶段期望（Step.Replicas）
//  并且健康维度（时效/指标/样本/错误率）全部通过。
//
// 隔离规则：只有 Observation.GateSeq 与当前阶段代次（Plan.Gate.GateSeq）相同的
// 观测才有资格判定本阶段——前面阶段留下的观测不能拿来通过后面阶段；每次进入
// （或崩溃后重新进入）一个阶段都会拿到新的代次，因此一次观测不可能同时通过
// 两个阶段。
//
// 返回 Healthy=false 且 Code 为健康/维度类失败码时，表示当前观测确实不达标，
// 由调用方按失败预算处理；Code=no-eligible-observation 表示“还在等”，
// 不消耗失败预算，也绝不放行。
func EvaluateGate(p *Plan, now time.Time) GateResult {
	res := GateResult{Verdict: Verdict{At: now}, StepIndex: -1}
	g := p.Gate
	if g.GateStep < 0 || g.GateStep >= len(p.Steps) {
		res.Code = VerdictNoEligibleObservation
		res.Reason = "no step currently awaiting gate"
		return res
	}
	res.StepIndex = g.GateStep
	obs := latestForGate(p.Observations, g.GateSeq)
	if obs == nil {
		res.Code = VerdictNoEligibleObservation
		res.Reason = fmt.Sprintf("no observation tagged for step %d gateSeq %d yet; keep waiting", g.GateStep, g.GateSeq)
		return res
	}
	res.ReportID = obs.ReportID
	step := p.Steps[g.GateStep]

	// 健康维度（不通过即为不健康，计入失败预算）。
	v := evaluateObservation(p, obs, now)
	if !v.Healthy {
		res.Verdict = v
		return res
	}

	// 版本维度。
	if !obs.HasVersion {
		res.Code = VerdictVersionMismatch
		res.Reason = fmt.Sprintf("step %d: active version not reported yet, want %q", g.GateStep, step.Version)
		return res
	}
	if obs.ActiveVersion != step.Version {
		res.Code = VerdictVersionMismatch
		res.Reason = fmt.Sprintf("step %d: active version %q != expected %q", g.GateStep, obs.ActiveVersion, step.Version)
		return res
	}

	// 流量维度。
	if !obs.HasTraffic {
		res.Code = VerdictTrafficNotReached
		res.Reason = fmt.Sprintf("step %d: traffic percent not reported yet, want %d%%", g.GateStep, step.Percent)
		return res
	}
	if obs.TrafficPercent != step.Percent {
		res.Code = VerdictTrafficNotReached
		res.Reason = fmt.Sprintf("step %d: traffic %d%% != expected %d%%", g.GateStep, obs.TrafficPercent, step.Percent)
		return res
	}

	// 承载维度。
	if obs.Replicas != step.Replicas {
		res.Code = VerdictCapacityNotConverged
		res.Reason = fmt.Sprintf("step %d: replicas %d != expected %d", g.GateStep, obs.Replicas, step.Replicas)
		return res
	}

	res.Healthy = true
	res.Code = VerdictOK
	res.Reason = fmt.Sprintf("step %d confirmed on report %s: version=%s, traffic=%d%%, replicas=%d",
		g.GateStep, obs.ReportID, step.Version, step.Percent, step.Replicas)
	return res
}

// isHealthFailure 判断结论是否属于“观测本身不健康”（消耗失败预算）。
// 版本/流量/承载维度未对上以及尚无合格观测属于等待范畴，不消耗预算。
func isHealthFailure(code string) bool {
	switch code {
	case VerdictStaleObservation, VerdictMissingMetric, VerdictInsufficientSamples, VerdictErrorRateExceeded:
		return true
	}
	return false
}

// latestForGate 倒序查找属于指定判定代次的最新观测；环形缓冲下旧代次观测被跳过。
func latestForGate(obs []Observation, gateSeq int64) *Observation {
	for i := len(obs) - 1; i >= 0; i-- {
		if obs[i].GateSeq == gateSeq {
			return &obs[i]
		}
	}
	return nil
}

// evaluateObservation 对单条观测做健康维度判定，规则同 Evaluate。
func evaluateObservation(p *Plan, obs *Observation, now time.Time) Verdict {
	v := Verdict{At: now}
	age := now.Sub(obs.ReportedAt)
	if age > p.Health.ObservationMaxAge {
		v.Code = VerdictStaleObservation
		v.Reason = fmt.Sprintf("latest observation %s old exceeds max age %s", age.Round(time.Millisecond), p.Health.ObservationMaxAge)
		return v
	}
	if !obs.HasMetrics {
		v.Code = VerdictMissingMetric
		v.Reason = "required metrics missing in latest observation"
		return v
	}
	if obs.Samples < p.Health.MinSamples {
		v.Code = VerdictInsufficientSamples
		v.Reason = fmt.Sprintf("samples %d below minimum %d", obs.Samples, p.Health.MinSamples)
		return v
	}
	if obs.ErrorRate > p.Health.MaxErrorRate {
		v.Code = VerdictErrorRateExceeded
		v.Reason = fmt.Sprintf("error rate %.4f exceeds max %.4f (samples=%d)", obs.ErrorRate, p.Health.MaxErrorRate, obs.Samples)
		return v
	}
	v.Healthy = true
	v.Code = VerdictOK
	v.Reason = fmt.Sprintf("error rate %.4f within max %.4f, samples=%d, age=%s", obs.ErrorRate, p.Health.MaxErrorRate, obs.Samples, age.Round(time.Millisecond))
	return v
}
