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

package estimator

import (
	gce_api "google.golang.org/api/compute/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/klog/v2"
)

type reservationsThreshold struct {
	reservationsPuller       *gceclient.ReservationsPuller
	localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider
	cloudProvider            CloudProvider
	optionsTracker           *optstracking.OptionsTracker
}

type CloudProvider interface {
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

// NodeLimit returns maximum number of new nodes that can be added to the cluster.
// Possible return values are:
//   - 0 when there are no reservations available or node group has no capacity left
//     Return value of 0 means that this threshold sets no limit for maximum number
//     of nodes.
//   - Any positive number representing maximum possible number of new nodes to be
//     added.
//   - -1 when a node group has any-then-fail affinity and no reservations are
//     available, which explicitly rejects the node group from further scaling.
//
// By design, this threshold caps the limit to `availableReservations` instead
// of the node group's total `availableCapacity` when reservations are present.
// This prevents the Cluster Autoscaler from greedily overconsuming on-demand
// capacity in a single scale-up loop, for example, when a node group with a
// small reservation is scaled-up before a node group with a large reservation.
//
// Examples of behavior based on affinity:
//   - ANY (the silent default): If a node group requires 7 nodes but only
//     has 5 reservations, the limit is capped at 5. In the first loop, 5 nodes
//     scale up using reservations. In the next loop, available reservations
//     will be 0. The limiter will return 0 (no limit), allowing the remaining
//     2 nodes to scale up using on-demand capacity.
//   - SPECIFIC: The behavior is the exact same as ANY, but reservations are
//     filtered by their specific names, additionally to the node shape.
//   - NONE: The node group ignores reservations, resulting in 0 matching
//     reservations. The limiter returns 0 (no limit), allowing scale-up to
//     proceed entirely via on-demand capacity.
//   - ANY_THEN_FAIL: Similar to ANY, but if available reservations reach 0,
//     the limiter returns -1, strictly blocking any fallback to on-demand
//     capacity.
func (t *reservationsThreshold) NodeLimit(nodeGroup cloudprovider.NodeGroup, context estimator.EstimationContext) estimator.NodeLimitResult {
	allReservations := t.reservationsPuller.GetReservations()
	nodeLimit := t.unusedReservationsCount(nodeGroup, allReservations)
	// Define threshold using reservations just for the current node group if similar node groups data is not available
	if context == nil {
		return estimator.NodeLimitResult{Limit: t.nodeLimitForAnyReservationThenFailAffinityNodeGroup(nodeGroup, nodeLimit)}
	}

	for _, sng := range context.SimilarNodeGroups() {
		nodeLimit += t.unusedReservationsCount(sng, allReservations)
	}

	return estimator.NodeLimitResult{Limit: t.nodeLimitForAnyReservationThenFailAffinityNodeGroup(nodeGroup, nodeLimit)}
}

// Returns minimum of available reservations and max node limit for the node group
func (t *reservationsThreshold) unusedReservationsCount(nodeGroup cloudprovider.NodeGroup, allReservations []*gce_api.Reservation) int {
	availableReservations := reservations.MatchingUnusedReservations(t.cloudProvider, nodeGroup, allReservations, t.localSSDDiskSizeProvider)
	if availableReservations <= 0 {
		return 0
	}

	nodeGroupTargetSize, err := nodeGroup.TargetSize()
	if err != nil {
		return 0
	}
	availableCapacity := nodeGroup.MaxSize() - nodeGroupTargetSize
	if availableCapacity <= 0 {
		return 0
	}

	if availableReservations > availableCapacity {
		return availableCapacity
	}
	return availableReservations
}

// DurationLimit always returns 0 for this threshold, meaning that no limit is set.
func (t *reservationsThreshold) DurationLimit(cloudprovider.NodeGroup, estimator.EstimationContext) estimator.DurationLimitResult {
	return estimator.DurationLimitResult{Duration: 0}
}

// NewReservationsThreshold returns a Threshold that can be used to limit binpacking
// by available reservations for current and all similar node groups.
func NewReservationsThreshold(puller *gceclient.ReservationsPuller, localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider, cloudProvider CloudProvider, optionsTracker *optstracking.OptionsTracker) estimator.Threshold {
	return &reservationsThreshold{
		reservationsPuller:       puller,
		localSSDDiskSizeProvider: localSSDDiskSizeProvider,
		cloudProvider:            cloudProvider,
		optionsTracker:           optionsTracker,
	}
}

func (t *reservationsThreshold) nodeLimitForAnyReservationThenFailAffinityNodeGroup(nodeGroup cloudprovider.NodeGroup, nodeLimit int) int {
	mig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		klog.Errorf("Nodegroup - %v cannot be converted to MIG", nodeGroup.Debug())
		return nodeLimit
	}

	anyThenFailThresholdEnabled := true
	if t.optionsTracker != nil && t.optionsTracker.ExperimentsManager() != nil {
		anyThenFailThresholdEnabled = t.optionsTracker.ExperimentsManager().EvaluateBoolFlagOrFailsafe(experiments.AnyThenFailReservationAffinityThresholdEnabledFlag, true)
	}

	if nodeLimit == 0 && mig.Spec().Labels[gkelabels.ReservationAffinityLabel] == reservations.AnyThenFail && anyThenFailThresholdEnabled {
		klog.V(4).Infof("Rejecting mig %s with affinity any-then-fail, no reservations available", mig.Id())
		return -1
	}
	return nodeLimit
}
