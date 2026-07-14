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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	klog "k8s.io/klog/v2"
)

const (
	multiplePodSetsUnsupportedReason        = "MultiplePodSetsUnsupported"
	multiplePodSetsUnsupportedMessage       = "Provisioning Request doesn't support multiple PodSets, please define only 1."
	obtainabilityStrategyUnsupportedReason  = "ObtainabilityStrategyUnsupported"
	obtainabilityStrategyUnsupportedMessage = "Provisioning Request doesn't support OBTAINABILITY capacity search strategy, please remove the parameter."
)

// featureGuardReconciler enables failing Provisioning Requests with particular properties as a fail-safe mechanism.
type featureGuardReconciler struct {
	prClient           provreqClient
	experimentsManager experiments.Manager
	featureGuards      []featureGuard
}

type featureGuard struct {
	name string
	// Experiments are enabled by default
	minCAVersionExp, enabledExp string
	// actionableStates is a list of ProvReq states the feature guard filter should apply to
	actionableStates []provreqstate.ProvisioningRequestState
	// propertyFn defines the property ProvReqs need to have to have the guard-filtering applied
	propertyFn              func(pr *provreqwrapper.ProvisioningRequest) bool
	failReason, failMessage string
}

func NewFeatureGuardReconciler(prClient provreqClient, experimentsManager experiments.Manager) reconcilingProcessor {
	return &featureGuardReconciler{
		prClient:           prClient,
		experimentsManager: experimentsManager,
		featureGuards: []featureGuard{
			{
				name:            "fail_MultiplePodSets_ProvReqs",
				minCAVersionExp: experiments.ProvisioningRequestMultiplePodSetsMinCAVersionFlag,
				enabledExp:      experiments.ProvisioningRequestMultiplePodSetsEnabledFlag,
				// This will prevent attempting scale ups for multiple PodSet ProvReqs.
				// In progress scale ups (i.e. already queueing Resize Requests) will stay unaffected.
				actionableStates: []provreqstate.ProvisioningRequestState{
					provreqstate.UninitializedState,
					provreqstate.PendingState,
				},
				propertyFn: func(pr *provreqwrapper.ProvisioningRequest) bool {
					return len(pr.Spec.PodSets) > 1
				},
				failReason:  multiplePodSetsUnsupportedReason,
				failMessage: multiplePodSetsUnsupportedMessage,
			},
			{
				name:             "fail_ObtainabilityStrategy_ProvReqs",
				minCAVersionExp:  experiments.ProvisioningRequestObtainabilityStrategyMinCAVersionFlag,
				enabledExp:       experiments.ProvisioningRequestObtainabilityStrategyEnabledFlag,
				actionableStates: allNonTerminalStates,
				propertyFn: func(pr *provreqwrapper.ProvisioningRequest) bool {
					return queuedwrapper.ToQueuedProvisioningRequest(*pr).ObtainabilityStrategy()
				},
				failReason:  obtainabilityStrategyUnsupportedReason,
				failMessage: obtainabilityStrategyUnsupportedMessage,
			},
		},
	}
}

func (r *featureGuardReconciler) featureEnabled(enabledFlag string, minCAVersionFlag string) bool {
	enabled := r.experimentsManager.EvaluateBoolFlagOrFailsafe(enabledFlag, true)
	currentVersionSupported := r.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(minCAVersionFlag, true)
	return enabled && currentVersionSupported
}

func (r *featureGuardReconciler) reconcileRequests(in *reconcilingInput) (map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, error) {
	if r.featureGuards == nil {
		klog.Warningf("featureGuardReconciler has nil featureGuards, possible CA misconfiguration")
		return in.prs, nil
	}

	for _, featureGuard := range r.featureGuards {
		if r.featureEnabled(featureGuard.enabledExp, featureGuard.minCAVersionExp) {
			continue
		}

		for _, state := range featureGuard.actionableStates {
			failProvReqsWithProperty(r.prClient, in.prs[state], featureGuard.propertyFn, featureGuard.failReason, featureGuard.failMessage, in.now)
			removeReconciled("featureGuardReconciler", featureGuard.name, in.prs, state, featureGuard.propertyFn)
		}
	}
	return in.prs, nil
}

func (r *featureGuardReconciler) queuedNodesImmunityStartInvalidate() {}

func (r *featureGuardReconciler) nodeHasScaleDownImmunity(*apiv1.Node, *QueuedProvisioningMigSpec, time.Time) bool {
	return false
}
