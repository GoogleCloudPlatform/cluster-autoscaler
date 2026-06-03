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
	"time"

	gce_api "google.golang.org/api/compute/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
)

type reservationsThreshold struct {
	reservationsPuller       *gceclient.ReservationsPuller
	localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider
	cloudProvider            CloudProvider
}

type CloudProvider interface {
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

// NodeLimit returns maximum number of new nodes that can be added to the cluster.
// Possible return values are
//   - 0 when there are no reservations available or node group has no capacity left
//     Return value of 0 means that this threshold sets no limit for maximum number of nodes
//   - Any positive number representing maximum possible number of new nodes to be added
func (t *reservationsThreshold) NodeLimit(nodeGroup cloudprovider.NodeGroup, context estimator.EstimationContext) int {
	allReservations := t.reservationsPuller.GetReservations()
	nodeLimit := t.unusedReservationsCount(nodeGroup, allReservations)
	// Define threshold using reservations just for the current node group if similar node groups data is not available
	if context == nil {
		return nodeLimit
	}

	for _, sng := range context.SimilarNodeGroups() {
		nodeLimit += t.unusedReservationsCount(sng, allReservations)
	}

	return nodeLimit
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
func (t *reservationsThreshold) DurationLimit(cloudprovider.NodeGroup, estimator.EstimationContext) time.Duration {
	return 0
}

// NewReservationsThreshold returns a Threshold that can be used to limit binpacking
// by available reservations for current and all similar node groups.
func NewReservationsThreshold(puller *gceclient.ReservationsPuller, localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider, cloudProvider CloudProvider) estimator.Threshold {
	return &reservationsThreshold{
		reservationsPuller:       puller,
		localSSDDiskSizeProvider: localSSDDiskSizeProvider,
		cloudProvider:            cloudProvider,
	}
}
