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

package backoff

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	base_backoff "k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/klog/v2"
)

const (
	FutureReservationNotStartedYetError = "futureReservationNotStartedYet"
)

// FutureReservationsProvider provides non-blocking API for GCE future reservations,
// to be used in GKE cluster logic
type FutureReservationsProvider interface {
	// GetLocalFutureReservations returns a slice of future reservations in this cluster's GCP project
	GetLocalFutureReservations() []*gceclient.GceFutureReservation
}

// FutureReservationsBackoffConfig is a configuration for base_backoff.Backoff
// implementation which uses GCE Future Reservations in its logic
type FutureReservationsBackoffConfig struct {
	Enabled   bool
	Provider  FutureReservationsProvider
	ProjectID string
}

type frBackoffKey struct {
	nodeGroupId     string
	reservationName string
}
type frBackoffValue struct {
	backoffUntil      time.Time
	frErrorInfo       cloudprovider.InstanceErrorInfo
	originalErrorInfo cloudprovider.InstanceErrorInfo
}

// frBackoff implements base_backoff.Backoff that uses GCE Future
// Reservations in its logic, according to b/359770844
type frBackoff struct {
	provider  FutureReservationsProvider
	projectID string

	backoffsMutex sync.Mutex
	backoffs      map[frBackoffKey]frBackoffValue
}

// NewFutureReservationsBackoff creates a new frBackoff instance
func NewFutureReservationsBackoff(config *FutureReservationsBackoffConfig) base_backoff.Backoff {
	return &frBackoff{
		provider:  config.Provider,
		projectID: config.ProjectID,
		backoffs:  make(map[frBackoffKey]frBackoffValue),
	}
}

// Backoff implements base_backoff.Backoff.
func (b *frBackoff) Backoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) time.Time {
	key, ok := b.createKey(nodeGroup, nodeInfo)
	if !ok {
		return currentTime
	}

	fr, ok := b.matchFutureReservation(key.reservationName, currentTime)
	if !ok {
		return currentTime
	}

	// back off until future reservation start time minus 30 minutes as VMs should be available then
	// calculation logic according to b/359770844
	backoffUntil := fr.StartTime.Add(-30 * time.Minute)
	if backoffUntil.Before(currentTime) {
		return currentTime
	}

	b.backoffsMutex.Lock()
	defer b.backoffsMutex.Unlock()

	b.backoffs[key] = frBackoffValue{
		backoffUntil:      backoffUntil,
		originalErrorInfo: errorInfo,
		frErrorInfo: cloudprovider.InstanceErrorInfo{
			ErrorClass:   cloudprovider.OtherErrorClass,
			ErrorCode:    FutureReservationNotStartedYetError,
			ErrorMessage: fmt.Sprintf("GCE future reservation [%s] has not started yet, start time: %v", fr.Name, fr.StartTime),
		},
	}

	klog.Infof("Applying backoff to node group %s for future reservation %s.", nodeGroup.Id(), fr.Name)
	return backoffUntil
}

func (b *frBackoff) createKey(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo) (frBackoffKey, bool) {
	if nodeInfo == nil {
		klog.Warningf("NodeInfo not provided for nodeGroup %v", nodeGroup.Id())
		return frBackoffKey{}, false
	}
	nodeLabels := nodeInfo.Node().GetLabels()
	reservationName := nodeLabels[labels.ReservationNameLabel]
	if reservationName == "" ||
		nodeLabels[labels.ReservationProjectLabel] != b.projectID { // only same project reservations are supported
		return frBackoffKey{}, false
	}

	return frBackoffKey{nodeGroupId: nodeGroup.Id(), reservationName: reservationName}, true
}

// matchFutureReservation returns matching and valid future reservation, nil if no matching future reservation is found
func (b *frBackoff) matchFutureReservation(reservationName string, currentTime time.Time) (value *gceclient.GceFutureReservation, ok bool) {
	frs := validate(b.provider.GetLocalFutureReservations(), currentTime)
	for _, i := range frs {
		if reservationName == i.Name {
			return i, true
		}
	}
	return nil, false
}

var (
	// future reservation procurement statuses which are valid to be used in future reservation backoff logic
	procurementStatusesValidForBackoff = sets.New(
		gceclient.ProcurementStatusApproved,
		gceclient.ProcurementStatusCommitted,
		gceclient.ProcurementStatusFailedPartiallyFulfilled,
		gceclient.ProcurementStatusFulfilled,
		gceclient.ProcurementStatusPendingAmendmentApproval,
		gceclient.ProcurementStatusPendingApproval,
		gceclient.ProcurementStatusProcuring,
		gceclient.ProcurementStatusProvisioning,
	)
)

// validate verifies Future Reservations returned from GCE if they have all necessary data
// for future reservations backoff logic
func validate(frs []*gceclient.GceFutureReservation, currentTime time.Time) []*gceclient.GceFutureReservation {
	valid := make([]*gceclient.GceFutureReservation, 0, len(frs))
	for _, fr := range frs {
		if fr.Name == "" {
			klog.Warningf("Future Reservation id=%d ignored due to empty name", fr.Id)
			continue
		}

		// check if future reservation is in usable status
		if fr.PlanningStatus != gceclient.PlanningStatusSubmitted ||
			!procurementStatusesValidForBackoff.Has(fr.ProcurementStatus) {
			continue
		}

		if fr.StartTime.Before(currentTime) {
			continue
		}

		valid = append(valid, fr)
	}
	return valid
}

// BackoffStatus implements base_backoff.BackoffStatus.
func (b *frBackoff) BackoffStatus(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, currentTime time.Time) base_backoff.Status {
	key, ok := b.createKey(nodeGroup, nodeInfo)
	if !ok {
		return base_backoff.Status{IsBackedOff: false}
	}

	b.backoffsMutex.Lock()
	defer b.backoffsMutex.Unlock()
	entry, was := b.backoffs[key]
	if !was || currentTime.After(entry.backoffUntil) {
		return base_backoff.Status{IsBackedOff: false}
	}

	return base_backoff.Status{
		IsBackedOff: true,
		ErrorInfo:   entry.frErrorInfo,
	}
}

// RemoveBackoff implements base_backoff.RemoveBackoff.
func (b *frBackoff) RemoveBackoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo) {
	key, ok := b.createKey(nodeGroup, nodeInfo)
	if !ok {
		return
	}
	b.backoffsMutex.Lock()
	defer b.backoffsMutex.Unlock()
	delete(b.backoffs, key)
}

// RemoveStaleBackoffData implements base_backoff.RemoveStaleBackoffData.
func (b *frBackoff) RemoveStaleBackoffData(currentTime time.Time) {
	b.backoffsMutex.Lock()
	defer b.backoffsMutex.Unlock()

	for key, entry := range b.backoffs {
		if currentTime.After(entry.backoffUntil) {
			delete(b.backoffs, key)
		}
	}
}
