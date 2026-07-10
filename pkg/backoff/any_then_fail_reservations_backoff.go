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
	"sync"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	base_backoff "k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	klog "k8s.io/klog/v2"
)

// NodeShapeKey represents a comparable version of a node shape used as a map key.
type NodeShapeKey struct {
	MachineType       string
	MinCpuPlatform    string
	Zone              string
	AcceleratorType   string
	AcceleratorCount  int64
	LocalSSDSCSICount int
	LocalSSDNVMECount int
}

// anyThenFailReservationsBackoff is the backoff for handling any_then_fail
// reservation affinity.
//
// Key for the backoff entry is NodeShapeKey.
type anyThenFailReservationsBackoff struct {
	mu              sync.RWMutex
	initialDuration time.Duration
	maxDuration     time.Duration
	resetTime       time.Duration
	backoffs        map[NodeShapeKey]*exponentialBackoff
}

// NewAnyThenFailReservationsBackoff initialises anyThenFailReservationsBackoff.
func NewAnyThenFailReservationsBackoff(initialBackoffDuration, maxBackoffDuration, resetTime time.Duration) base_backoff.Backoff {
	return &anyThenFailReservationsBackoff{
		initialDuration: initialBackoffDuration,
		maxDuration:     maxBackoffDuration,
		resetTime:       resetTime,
		backoffs:        make(map[NodeShapeKey]*exponentialBackoff),
	}
}

// Backoff execution for the given node group. Returns time till execution is backed off.
func (b *anyThenFailReservationsBackoff) Backoff(nodeGroup cloudprovider.NodeGroup, _ *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) time.Time {
	gkeMig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		return currentTime
	}
	if affinity, ok := gkeMig.Spec().Labels[gkelabels.ReservationAffinityLabel]; !ok || affinity != reservations.AnyThenFail {
		return currentTime
	}
	if !isAffinityAnyThenFailError(errorInfo) {
		return currentTime
	}

	key := buildNodeShapeKey(gkeMig)

	b.mu.Lock()
	defer b.mu.Unlock()

	backoffEntry, ok := b.backoffs[key]

	if !ok {
		backoffEntry = NewExponentialBackoff(b.initialDuration, b.maxDuration, b.resetTime)
		b.backoffs[key] = backoffEntry
	}

	backoffEntry.Backoff(errorInfo, currentTime)
	klog.Infof("Applying backoff for affinity any_then_fail for shape %+v until %v", key, backoffEntry.BackoffUntil())
	return backoffEntry.BackoffUntil()
}

// BackoffStatus returns whether the execution is backed off for the given node group and error info when the node group is backed off.
func (b *anyThenFailReservationsBackoff) BackoffStatus(nodeGroup cloudprovider.NodeGroup, _ *framework.NodeInfo, currentTime time.Time) base_backoff.Status {
	mig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		return base_backoff.Status{IsBackedOff: false}
	}
	if affinity, ok := mig.Spec().Labels[gkelabels.ReservationAffinityLabel]; !ok || affinity != reservations.AnyThenFail {
		return base_backoff.Status{IsBackedOff: false}
	}

	key := buildNodeShapeKey(mig)
	b.mu.RLock()
	defer b.mu.RUnlock()

	backoffEntry, ok := b.backoffs[key]
	if !ok {
		return base_backoff.Status{IsBackedOff: false}
	}
	if currentTime.After(backoffEntry.BackoffUntil()) {
		return base_backoff.Status{IsBackedOff: false}
	}
	return base_backoff.Status{IsBackedOff: true, ErrorInfo: backoffEntry.ErrorInfo()}
}

// RemoveBackoff removes backoff data for the given node group.
func (b *anyThenFailReservationsBackoff) RemoveBackoff(nodeGroup cloudprovider.NodeGroup, _ *framework.NodeInfo) {
	gkeMig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		return
	}
	key := buildNodeShapeKey(gkeMig)

	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.backoffs, key)
}

// RemoveStaleBackoffData removes stale backoff data.
func (b *anyThenFailReservationsBackoff) RemoveStaleBackoffData(currentTime time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for backoffKey, backoffEntry := range b.backoffs {
		// Remove backoff only on case its reset time has come
		if currentTime.After(backoffEntry.ResetTime()) {
			delete(b.backoffs, backoffKey)
		}
	}
}

func isAffinityAnyThenFailError(errorInfo cloudprovider.InstanceErrorInfo) bool {
	return errorInfo.ErrorCode == gce.ErrorAutomaticReservationsNotAvailable || errorInfo.ErrorCode == gce.ErrorAutomaticReservationsNoCapacity
}

func buildNodeShapeKey(mig *gke.GkeMig) NodeShapeKey {
	key := NodeShapeKey{
		MachineType:       mig.Spec().MachineType,
		MinCpuPlatform:    mig.Spec().MinCpuPlatform,
		Zone:              mig.GceRef().Zone,
		LocalSSDSCSICount: mig.GetSCSILLocalSSDCount(),
		LocalSSDNVMECount: mig.GetNVMELocalSSDCount(),
	}
	if len(mig.Spec().Accelerators) > 0 {
		// Assuming one type of accelerator per node pool, which is standard for GKE.
		key.AcceleratorType = mig.Spec().Accelerators[0].AcceleratorType
		key.AcceleratorCount = mig.Spec().Accelerators[0].AcceleratorCount
	}
	return key
}
