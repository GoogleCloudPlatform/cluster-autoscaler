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

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	quota "k8s.io/apiserver/pkg/quota/v1"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

const defaultCoarseStepMilliCpus = 2000

var ekvmsIncrementStepField = trackedField{
	name: "EkvmsIncrementStep",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return quota.Equals(optsA.EkvmsIncrementStep, optsB.EkvmsIncrementStep)
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		if len(opts.EkvmsIncrementStep) == 0 {
			return ""
		}
		return fmt.Sprintf("cpu=%s,memory=%s", opts.EkvmsIncrementStep.Cpu().String(), opts.EkvmsIncrementStep.Memory().String())
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		// Default to CLI flag value (2 CPU unless overridden by CLI flag).
		optsToModify.EkvmsIncrementStep = optsFromFlags.EkvmsIncrementStep

		// If the CLI flag was explicitly set to a non-default CPU step (anything other than 2000m),
		// respect the explicit CLI override.
		if cpu, ok := optsFromFlags.EkvmsIncrementStep[apiv1.ResourceCPU]; ok && cpu.MilliValue() != defaultCoarseStepMilliCpus {
			return nil
		}

		// EkFineGrainedResizeEnabledFlag is ON by default; used for emergency mitigation.
		enabled := experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.EkFineGrainedResizeEnabledFlag, true)
		// EkFineGrainedResizeMinCAVersionFlag is not set by default (failsafe = false).
		currentVersionSupported := experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.EkFineGrainedResizeMinCAVersionFlag, false)
		// EkFineGrainedResizeIncrementStepFlag specifies the target CPU step quantity (e.g. "1", "500m", "10m").
		stepCpuStr := experimentsManager.EvaluateStringFlagOrFailsafe(experiments.EkFineGrainedResizeIncrementStepFlag, "")

		if !enabled || !currentVersionSupported || stepCpuStr == "" {
			return nil
		}

		cpuQuantity, err := resource.ParseQuantity(stepCpuStr)
		if err != nil || cpuQuantity.MilliValue() <= 0 {
			klog.Warningf("Experiment %q provided invalid CPU step quantity %q (err=%v), falling back to CLI flag default", experiments.EkFineGrainedResizeIncrementStepFlag, stepCpuStr, err)
			return nil
		}

		memQuantity, ok := optsFromFlags.EkvmsIncrementStep[apiv1.ResourceMemory]
		if !ok {
			memQuantity = resource.MustParse("1Mi")
		}

		optsToModify.EkvmsIncrementStep = apiv1.ResourceList{
			apiv1.ResourceCPU:    cpuQuantity,
			apiv1.ResourceMemory: memQuantity,
		}
		return nil
	},
	caRestartNeededOnValueChange: true,
}
