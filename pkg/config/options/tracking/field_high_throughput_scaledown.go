// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracking

import (
	"fmt"

	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

// HighThroughputScaledown is a *mitigation* experiment, not a feature flag.
//
// The high-throughput scale-down flag values (--kube-client-qps, --kube-client-burst,
// --max-scale-down-parallelism, --max-concurrent-nodes-tainting) are raised in the
// control-plane manifest, so by default every cluster on a new GKE version gets them.
// The experiment is enabled by default; disabling it for a cluster is the kill switch
// that clamps those flags back down to the Baseline* values that ran on production
// before the increase, without a manifest rollback.
//
// Because the experiment can only ever *lower* a value, the fields below follow the
// same shape:
//   - a flag value already at or below its baseline is passed through unchanged (the
//     experiment is irrelevant to it, so it is not even evaluated, and a Giraffe flip
//     cannot trigger a spurious restart on such a cluster);
//   - an explicit --override_<flag> wins over the experiment, so the oncaller can still
//     set a value per cluster (go/gke-flag-overrides);
//   - otherwise, a value above the baseline is kept only while the experiment is on.
const (
	// BaselineKubeClientQPS is the --kube-client-qps value that ran on production before
	// the high-throughput scale-down increase; the mitigation clamps larger values back down to it.
	BaselineKubeClientQPS = float32(100)
	// BaselineKubeClientBurst is the --kube-client-burst value that ran on production
	// before the high-throughput scale-down increase; the mitigation clamps larger values back down to it.
	BaselineKubeClientBurst = 100
	// BaselineMaxScaleDownParallelism is the --max-scale-down-parallelism value that ran
	// on production before the high-throughput scale-down increase; the mitigation clamps larger values back down to it.
	BaselineMaxScaleDownParallelism = 100
	// BaselineMaxConcurrentNodesTainting is the --max-concurrent-nodes-tainting value that
	// ran on production before the high-throughput scale-down increase; the mitigation clamps larger values back down to it.
	BaselineMaxConcurrentNodesTainting = 5

	kubeClientQPSFlag              = "kube-client-qps"
	kubeClientBurstFlag            = "kube-client-burst"
	maxScaleDownParallelismFlag    = "max-scale-down-parallelism"
	maxConcurrentNodesTaintingFlag = "max-concurrent-nodes-tainting"
)

var kubeClientOptsField = trackedField{
	name: "KubeClientOpts",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.KubeClientOpts == optsB.KubeClientOpts
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("{KubeClientQPS:%v KubeClientBurst:%v}", opts.KubeClientOpts.KubeClientQPS, opts.KubeClientOpts.KubeClientBurst)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		kubeClientOpts := optsFromFlags.KubeClientOpts
		// Only values above the baseline that weren't explicitly overridden are candidates for clamping.
		clampQPS := kubeClientOpts.KubeClientQPS > BaselineKubeClientQPS && !optsFromFlags.FlagOverrideResult.HasFlagOverride(kubeClientQPSFlag)
		clampBurst := kubeClientOpts.KubeClientBurst > BaselineKubeClientBurst && !optsFromFlags.FlagOverrideResult.HasFlagOverride(kubeClientBurstFlag)

		// Evaluate the experiment only if there is something to clamp.
		if (clampQPS || clampBurst) && !isHighThroughputScaledownEnabled(experimentsManager) {
			if clampQPS {
				// In NewOptionsTracker, optsToModify starts equal to optsFromFlags and is kept across
				// RecomputeOptions() iterations, so guarding on != Baseline* logs once on startup (or
				// when the experiment transitions from enabled to disabled) without repeating every loop.
				if optsToModify.KubeClientOpts.KubeClientQPS != BaselineKubeClientQPS {
					klog.V(4).Infof("HighThroughputScaledown disabled by experiment, clamping --%s from %v to %v", kubeClientQPSFlag, kubeClientOpts.KubeClientQPS, BaselineKubeClientQPS)
				}
				kubeClientOpts.KubeClientQPS = BaselineKubeClientQPS
			}
			if clampBurst {
				if optsToModify.KubeClientOpts.KubeClientBurst != BaselineKubeClientBurst {
					klog.V(4).Infof("HighThroughputScaledown disabled by experiment, clamping --%s from %v to %v", kubeClientBurstFlag, kubeClientOpts.KubeClientBurst, BaselineKubeClientBurst)
				}
				kubeClientOpts.KubeClientBurst = BaselineKubeClientBurst
			}
		}

		optsToModify.KubeClientOpts = kubeClientOpts
		return nil
	},
	caRestartNeededOnValueChange: true,
}

var maxScaleDownParallelismField = trackedField{
	name: "MaxScaleDownParallelism",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.MaxScaleDownParallelism == optsB.MaxScaleDownParallelism
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.MaxScaleDownParallelism)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		// Pass the flag value through if it is already at or below the baseline (nothing to clamp, so the
		// experiment is not evaluated), if it was explicitly overridden, or if the mitigation is not active.
		if optsFromFlags.MaxScaleDownParallelism <= BaselineMaxScaleDownParallelism || optsFromFlags.FlagOverrideResult.HasFlagOverride(maxScaleDownParallelismFlag) || isHighThroughputScaledownEnabled(experimentsManager) {
			optsToModify.MaxScaleDownParallelism = optsFromFlags.MaxScaleDownParallelism
			return nil
		}
		if optsToModify.MaxScaleDownParallelism != BaselineMaxScaleDownParallelism {
			klog.V(4).Infof("HighThroughputScaledown disabled by experiment, clamping --%s from %v to %v", maxScaleDownParallelismFlag, optsFromFlags.MaxScaleDownParallelism, BaselineMaxScaleDownParallelism)
		}
		optsToModify.MaxScaleDownParallelism = BaselineMaxScaleDownParallelism
		return nil
	},
	caRestartNeededOnValueChange: true,
}

var maxConcurrentNodesTaintingField = trackedField{
	name: "MaxConcurrentNodesTainting",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.MaxConcurrentNodesTainting == optsB.MaxConcurrentNodesTainting
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.MaxConcurrentNodesTainting)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		// Pass the flag value through if it is already at or below the baseline (nothing to clamp, so the
		// experiment is not evaluated), if it was explicitly overridden, or if the mitigation is not active.
		if optsFromFlags.MaxConcurrentNodesTainting <= BaselineMaxConcurrentNodesTainting || optsFromFlags.FlagOverrideResult.HasFlagOverride(maxConcurrentNodesTaintingFlag) || isHighThroughputScaledownEnabled(experimentsManager) {
			optsToModify.MaxConcurrentNodesTainting = optsFromFlags.MaxConcurrentNodesTainting
			return nil
		}
		if optsToModify.MaxConcurrentNodesTainting != BaselineMaxConcurrentNodesTainting {
			klog.V(4).Infof("HighThroughputScaledown disabled by experiment, clamping --%s from %v to %v", maxConcurrentNodesTaintingFlag, optsFromFlags.MaxConcurrentNodesTainting, BaselineMaxConcurrentNodesTainting)
		}
		optsToModify.MaxConcurrentNodesTainting = BaselineMaxConcurrentNodesTainting
		return nil
	},
	caRestartNeededOnValueChange: true,
}

// isHighThroughputScaledownEnabled reports whether the mitigation is *inactive*, i.e. the raised
// flag values may be kept. Both Giraffe flags fail safe to true, so a Giraffe outage keeps the
// manifest values rather than silently clamping every cluster.
func isHighThroughputScaledownEnabled(experimentsManager experiments.Manager) bool {
	enabled := experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.HighThroughputScaledownEnabledFlag, true)
	currentVersionSupported := experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.HighThroughputScaledownMinCAVersionFlag, true)
	return enabled && currentVersionSupported
}
