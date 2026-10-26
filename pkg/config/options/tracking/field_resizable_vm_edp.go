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

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

var resizableVmEdpEnabledField = trackedField{
	name: "ResizableVmEdpEnabled",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.ResizableVmEdpEnabled == optsB.ResizableVmEdpEnabled
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.ResizableVmEdpEnabled)
	},
	setValueFromClusterProto: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, cluster gkeclient.Cluster, optsToModify *internalopts.AutoscalingOptions) error {
		optsToModify.ResizableVmEdpEnabled = isResizableVmEdpEnabled(cluster.ClusterVersion, experimentsManager)
		return nil
	},
	caRestartNeededOnValueChange: false,
}

func isResizableVmEdpEnabled(clusterVersion string, experimentsManager experiments.Manager) bool {
	var minGkeVersion = experimentsManager.EvaluateStringFlagOrFailsafe(experiments.EnableResizableVmEdpMinGKEVersionFlag, "999.999.999-gke.0")

	v, err := version.FromString(clusterVersion)
	if err != nil {
		klog.Errorf("Failed to parse cluster version %q: %v", clusterVersion, err)
		return false
	}
	vMin, err := version.FromString(minGkeVersion)
	if err != nil {
		klog.Errorf("Failed to parse min GKE version %q: %v", minGkeVersion, err)
		return false
	}

	return !v.LessThan(vMin)
}
