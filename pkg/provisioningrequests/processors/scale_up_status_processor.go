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
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	klog "k8s.io/klog/v2"
)

// ProvisioningRequestScaleUpStatusProcessor filters out injected provreq pods from scaleup status.
type ProvisioningRequestScaleUpStatusProcessor struct{}

// NewProvisioningRequestScaleUpStatusProcessor return an instance of ProvisioningRequestScaleUpStatusProcessor
func NewProvisioningRequestScaleUpStatusProcessor() *ProvisioningRequestScaleUpStatusProcessor {
	return &ProvisioningRequestScaleUpStatusProcessor{}
}

// Process updates scaleupStatus to remove all injected provreq pods from PodsRemainUnschedulable
func (a *ProvisioningRequestScaleUpStatusProcessor) Process(_ *ca_context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) {
	scaleUpStatus.PodsRemainUnschedulable = filterProvReqPods(scaleUpStatus.PodsRemainUnschedulable)
}

// CleanUp is called at CA termination
func (a *ProvisioningRequestScaleUpStatusProcessor) CleanUp() {}

func filterProvReqPods(infos []status.NoScaleUpInfo) []status.NoScaleUpInfo {
	filtered := make([]status.NoScaleUpInfo, 0)
	for _, info := range infos {
		if _, isInjected := pods.InjectedPodProvReqRef(info.Pod); !isInjected {
			filtered = append(filtered, info)
		}
	}
	klog.V(2).Infof("ProvisioningRequestScaleUpStatusProcessor filtered out %d NoScaleUpInfos", len(infos)-len(filtered))
	return filtered
}
