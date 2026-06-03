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
)

var capacityBuffersControllerEnabledField = trackedField{
	name: "capacityBuffersControllerEnabledField",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.CapacitybufferControllerEnabled == optsB.CapacitybufferControllerEnabled
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.CapacitybufferControllerEnabled)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		optsToModify.CapacitybufferControllerEnabled = isCapacityBufferEnabled(optsFromFlags.CapacitybufferControllerEnabled, experimentsManager)
		return nil
	},
}

var capacityBuffersPodInjectionEnabledField = trackedField{
	name: "capacityBuffersPodInjectionEnabledField",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.CapacitybufferPodInjectionEnabled == optsB.CapacitybufferPodInjectionEnabled
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.CapacitybufferPodInjectionEnabled)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		optsToModify.CapacitybufferPodInjectionEnabled = isCapacityBufferEnabled(optsFromFlags.CapacitybufferPodInjectionEnabled, experimentsManager)
		return nil
	},
}

func isCapacityBufferEnabled(capacityBufferFlag bool, em experiments.Manager) bool {
	isEnabledForCluster := em.DirectLaunchBoolFlag(experiments.CapacityBuffersEnabled)
	if !isEnabledForCluster {
		return false
	}

	isPrivatePreview := em.EvaluateMinimumVersionFlagOrFailsafe(experiments.CapacityBuffersPrivatePreviewMinCAVersion, false)
	isPublicPreview := capacityBufferFlag && em.EvaluateMinimumVersionFlagOrFailsafe(experiments.CapacityBuffersMinCAVersion, true)

	return isPrivatePreview || isPublicPreview
}
