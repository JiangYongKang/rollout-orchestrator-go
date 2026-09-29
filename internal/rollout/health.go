package rollout

import (
	"fmt"
	"time"
)

// EvaluateStep 针对指定阶段做多维门控判定，返回结论与逐维度依据。
//
// 一个阶段只有同时满足以下三个收敛维度才算通过（缺一不可）：
//  1. 生效版本：观测到的 EffectiveVersion == 该阶段期望版本；
//  2. 流量占比：观测到的 TrafficPercent >= 该阶段期望占比；
//  3. 实际承载：观测到的 Replicas == 该阶段期望副本数（已精确收敛，不多不少）。
//
// 此外仍保留原健康维度（观测时效、指标齐全、样本数、错误率）。
//
// 防跨阶段误判的关键约束：只接受 Seq >= gateSeq 的观测。gateSeq 是阶段期望状态
// 应用之后才产生的“新观测”序号水位线（Seq 单调递增且不受环形裁剪影响），因此：
//   - 前面阶段遗留的观测不能作为后面阶段的通过依据；
//   - 一次观测不可能同时让两个阶段通过（新阶段应用后必须等到新上报）。
//
// 实际状态延迟、抖动或短暂未上报时返回非 OK 结论，调用方只能继续等待或按失败
// 预算处理；这里不会提前放行、跳过阶段或改写结论。
func EvaluateStep(p *Plan, step Step, gateSeq int, now time.Time) Verdict {
	v := Verdict{
		At:           now,
		Step:         step.Index,
		WantVersion:  p.expectedVersion(step),
		WantPercent:  step.Percent,
		WantReplicas: step.Replicas,
	}
	// 环形裁剪可能丢掉旧观测；在剩余切片中定位“当前阶段产生的最新观测”。
	// 只接受 Seq >= gateSeq 的观测（阶段期望应用之后才产生），但以其中最新一条
	// 为准：连续上报时后到的观测反映更接近当前的实际状态。
	idx := -1
	for i := range p.Observations {
		if p.Observations[i].Seq >= gateSeq {
			idx = i
		}
	}
	if idx < 0 {
		v.Code = VerdictStaleObservation
		v.Reason = fmt.Sprintf("step %d: no observation since desired state applied (need seq >= %d, next seq %d)", step.Index, gateSeq, p.NextSeq)
		return v
	}
	obs := p.Observations[idx]
	// 该观测一定是阶段期望应用之后产生的（旧阶段观测序号更小），绝不回看更早阶段。
	v.ReportID = obs.ReportID
	v.GotVersion = obs.EffectiveVersion
	v.GotPercent = obs.TrafficPercent
	v.GotReplicas = obs.Replicas

	age := now.Sub(obs.ReportedAt)
	if age > p.Health.ObservationMaxAge {
		v.Code = VerdictStaleObservation
		v.Reason = fmt.Sprintf("step %d: observation %s old exceeds max age %s", step.Index, age.Round(time.Millisecond), p.Health.ObservationMaxAge)
		return v
	}
	if obs.EffectiveVersion != v.WantVersion {
		v.Code = VerdictVersionMismatch
		v.Reason = fmt.Sprintf("step %d: effective version %q != want %q", step.Index, obs.EffectiveVersion, v.WantVersion)
		return v
	}
	if obs.TrafficPercent < step.Percent {
		v.Code = VerdictTrafficNotReached
		v.Reason = fmt.Sprintf("step %d: traffic %d%% < want %d%%", step.Index, obs.TrafficPercent, step.Percent)
		return v
	}
	if obs.Replicas != step.Replicas {
		v.Code = VerdictNotConverged
		v.Reason = fmt.Sprintf("step %d: replicas %d != want %d (not converged)", step.Index, obs.Replicas, step.Replicas)
		return v
	}
	if !obs.HasMetrics {
		v.Code = VerdictMissingMetric
		v.Reason = fmt.Sprintf("step %d: required metrics missing in observation %s", step.Index, obs.ReportID)
		return v
	}
	if obs.Samples < p.Health.MinSamples {
		v.Code = VerdictInsufficientSamples
		v.Reason = fmt.Sprintf("step %d: samples %d below minimum %d", step.Index, obs.Samples, p.Health.MinSamples)
		return v
	}
	if obs.ErrorRate > p.Health.MaxErrorRate {
		v.Code = VerdictErrorRateExceeded
		v.Reason = fmt.Sprintf("step %d: error rate %.4f exceeds max %.4f (samples=%d)", step.Index, obs.ErrorRate, p.Health.MaxErrorRate, obs.Samples)
		return v
	}
	v.Healthy = true
	v.Code = VerdictOK
	v.Reason = fmt.Sprintf("step %d: version=%q traffic=%d%% replicas=%d error-rate=%.4f samples=%d age=%s",
		step.Index, v.WantVersion, obs.TrafficPercent, obs.Replicas, obs.ErrorRate, obs.Samples, age.Round(time.Millisecond))
	return v
}

// consumesFailureBudget 判断判定结论是否计入连续失败预算。
//
// 只有“观测已经在当前阶段产生、且内容表明新版本本身不健康”才烧预算：
// 指标缺失、样本不足、错误率超标。
// 观测缺失/过期、版本/流量/承载尚未收敛属于实际状态的延迟与抖动，只能继续等待，
// 不烧预算，避免收敛慢被误判成发布失败。
func consumesFailureBudget(code string) bool {
	switch code {
	case VerdictMissingMetric, VerdictInsufficientSamples, VerdictErrorRateExceeded:
		return true
	}
	return false
}
