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

package reconciler

import (
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

const (
	obtainabilityZoneSelectorUnsupportedReason  = "ObtainabilityStrategyAndZoneIncompatible"
	obtainabilityZoneSelectorUnsupportedMessage = "OBTAINABILITY capacity search strategy and zonal node selector are incompatible and cannot be used together, please use only one of the two"
)

type obtainabilityZoneSelectorReconciler struct {
	prClient provreqClient
}

func NewObtainabilityZoneSelectorReconciler(prClient provreqClient) reconcilingProcessor {
	return &obtainabilityZoneSelectorReconciler{
		prClient: prClient,
	}
}

func (r *obtainabilityZoneSelectorReconciler) reconcileRequests(in *reconcilingInput) (map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, error) {
	targetStates := allNonTerminalStates

	propertyFn := func(pr *provreqwrapper.ProvisioningRequest) bool {
		// Check if ObtainabilityStrategy is used
		if !queuedwrapper.ToQueuedProvisioningRequest(*pr).ObtainabilityStrategy() {
			return false
		}

		// Check if any PodSet has a topology.kubernetes.io/zone node selector
		podSets, err := pr.PodSets()
		if err != nil {
			// If we can't get PodSets, we can't evaluate the property, so we return false
			// to not fail it for this specific reason.
			return false
		}

		for _, ps := range podSets {
			if ps.PodTemplate.Spec.NodeSelector != nil {
				if _, ok := ps.PodTemplate.Spec.NodeSelector[apiv1.LabelTopologyZone]; ok {
					return true
				}
			}
		}
		return false
	}

	for _, state := range targetStates {
		failProvReqsWithProperty(r.prClient, in.prs[state], propertyFn, obtainabilityZoneSelectorUnsupportedReason, obtainabilityZoneSelectorUnsupportedMessage, in.now)
		removeReconciled("obtainabilityZoneSelectorReconciler", "obtainability_zone_selector_guard", in.prs, state, propertyFn)
	}

	return in.prs, nil
}

func (r *obtainabilityZoneSelectorReconciler) queuedNodesImmunityStartInvalidate() {}

func (r *obtainabilityZoneSelectorReconciler) nodeHasScaleDownImmunity(*apiv1.Node, *QueuedProvisioningMigSpec, time.Time) bool {
	return false
}
