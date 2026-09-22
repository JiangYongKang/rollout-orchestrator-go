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
