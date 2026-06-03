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

package processors

import (
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/klog/v2"
)

const readinessAfterClusterProvisioningTimeout = 15 * time.Second

// GkeEmptyClusterProcessor is a GKE extension of EmptyClusterProcessor to cater for GKE specific logic
type GkeEmptyClusterProcessor struct {
	provisioningTime time.Time
}

// CleanUp cleans up the Processor
func (e *GkeEmptyClusterProcessor) CleanUp() {
}

// ShouldAbort give the decision on whether CA can act on the cluster
func (e *GkeEmptyClusterProcessor) ShouldAbort(context *context.AutoscalingContext,
	allNodes []*apiv1.Node,
	readyNodes []*apiv1.Node,
	currentTime time.Time) (bool, errors.AutoscalerError) {

	gkeCloudProvider, ok := context.CloudProvider.(ProcessorsCloudProvider)
	if !ok {
		klog.Errorf("Unable to fetch ProcessorsCloudProvider. Aborting the loop")
		return true, nil
	}

	if !context.AutoscalingOptions.ScaleUpFromZero {
		if len(allNodes) == 0 {
			klog.Errorf("Scale from Zero is disabled and cluster is empty. Aborting the loop.")
			return true, nil
		}
	}
	status, err := gkeCloudProvider.ClusterStarted()
	if err != nil {
		klog.Warningf("Aborting the loop. Unable to fetch cluster started state: %v", err)
		return true, nil
	}
	if !status {
		e.provisioningTime = currentTime
		klog.Warning("Cluster is still provisioning. Aborting the loop.")
		return true, nil
	}
	if len(allNodes) != len(readyNodes) && currentTime.Before(e.provisioningTime.Add(readinessAfterClusterProvisioningTimeout)) {
		klog.Warning("Cluster just started, but there are still unready nodes. Aborting the loop.")
		return true, nil
	}

	return false, nil
}

// NewGkeEmptyClusterProcessor return a new Processor instance
func NewGkeEmptyClusterProcessor() *GkeEmptyClusterProcessor {
	return &GkeEmptyClusterProcessor{}
}
