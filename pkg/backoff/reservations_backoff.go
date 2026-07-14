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

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	base_backoff "k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	klog "k8s.io/klog/v2"
)

const (
	// invalidReservationBackoffResetTime is the time after first backoff when the backoff duration is reset,
	// applies only to invalid reservation related errors
	invalidReservationBackoffResetTime = time.Hour

	// reservationOutOfResourcesBackoffResetTime is the time after first backoff when the backoff duration is reset,
	// applies only to out of resources related errors
	reservationOutOfResourcesBackoffResetTime = time.Minute * 10
)

var invalidReservationErrs = map[string]bool{
	gce.ErrorInvalidReservation:      true,
	gce.ErrorReservationNotFound:     true,
	gce.ErrorReservationNotReady:     true,
	gce.ErrorReservationIncompatible: true,
}

// TODO(b/429117018): add here new error from b/422125156 when fully rolled out
var outOfResourcesReservationErrs = map[string]bool{
	gce.ErrorReservationCapacityExceeded: true,
}

type reservationsBackoff struct {
	mu                  sync.RWMutex
	initialDuration     time.Duration
	maxDuration         time.Duration
	reservationBackoffs map[reservationKey]reservationBackoffEntry

	provider backoffCloudProvider
}

// reservationKey is used as a key for caching reservation validity.
// It combines the reservation's unique reference with the specific machine
// configuration being requested against it.
//
// We use this composite key because a reservation's validity depends not just on
// the reservation itself (which specifies a total amount of resources), but
// also on the machine type and topology (multi-host vs. single-host) being
// requested.
//
// TODO(b/294973485): Distinguish between actual reservation related errors
// (e.g. doesn't exist) and when reservation is okay but the machine type
// doesn't match the reservation
type reservationKey struct {
	id            gceclient.ReservationRef
	machineFamily string
	isMultiHost   bool
}

type reservationBackoffEntry struct {
	backoffUntil    time.Time
	backoffDuration time.Duration
	reset           time.Time
	errorInfo       cloudprovider.InstanceErrorInfo
}

// NewReservationsBackoff initialises a backoff mechanism for reservations.
func NewReservationsBackoff(initialBackoffDuration, maxBackoffDuration time.Duration, provider backoffCloudProvider) base_backoff.Backoff {
	return &reservationsBackoff{
		initialDuration:     initialBackoffDuration,
		maxDuration:         maxBackoffDuration,
		reservationBackoffs: make(map[reservationKey]reservationBackoffEntry),
		provider:            provider,
	}
}

// Backoff triggers a backoff for the reservation defined by the given nodeInfo.
// It returns the time until which the operation should be backed off.
func (b *reservationsBackoff) Backoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.isInvalidReservationError(errorInfo) && !b.isReservationOutOfResources(nodeGroup, nodeInfo, errorInfo) {
		return currentTime
	}
	if nodeInfo == nil {
		klog.Warningf("Not backing off nodeGroup %v due to invalid reservation: nodeInfo is nil", nodeGroup.Id())
		return currentTime
	}
	nodeLabels := nodeInfo.Node().GetLabels()
	key, ok := b.createReservationKey(nodeLabels)
	if !ok {
		klog.Warningf("Not backing off nodeGroup %q due to invalid reservation: could not create backoff key from labels", nodeGroup.Id())
		return currentTime
	}
	backoffEntry := b.calculateBackoff(nodeGroup, nodeInfo, b.reservationBackoffs[key], errorInfo, currentTime)
	b.reservationBackoffs[key] = backoffEntry
	klog.Infof("Applying backoff to node group %s for reservation %+v", nodeGroup.Id(), key)
	return backoffEntry.backoffUntil
}

func (b *reservationsBackoff) calculateBackoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, backoffEntry reservationBackoffEntry, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) reservationBackoffEntry {
	if b.isInvalidReservationError(errorInfo) {
		return b.backoffExponential(backoffEntry, errorInfo, currentTime, invalidReservationBackoffResetTime)
	}
	if b.isReservationOutOfResources(nodeGroup, nodeInfo, errorInfo) {
		return b.backoffExponential(backoffEntry, errorInfo, currentTime, reservationOutOfResourcesBackoffResetTime)
	}
	return backoffEntry
}

func (b *reservationsBackoff) isInvalidReservationError(errorInfo cloudprovider.InstanceErrorInfo) bool {
	return invalidReservationErrs[errorInfo.ErrorCode]
}

// isReservationOutOfResources checks the error class of failed scale up and determines
// if the reservation-based backoff should be applied to the node group. It returns false
// for a stockout, which for reservation can happen only if the error is of "Quota Exceeded"
// type.
func (b *reservationsBackoff) isReservationOutOfResources(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo) bool {
	gkeMig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		return false
	}
	if !gkeMig.UsesReservation() {
		return false
	}
	if res, ok := outOfResourcesReservationErrs[errorInfo.ErrorCode]; ok {
		return res
	}
	return errorInfo.ErrorClass == cloudprovider.OutOfResourcesErrorClass && !IsStockout(nodeGroup, nodeInfo, errorInfo)
}

func (b *reservationsBackoff) backoffExponential(backoffEntry reservationBackoffEntry, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time, resetTime time.Duration) reservationBackoffEntry {
	if backoffEntry.backoffDuration == 0 {
		backoffEntry.backoffDuration = b.initialDuration
		backoffEntry.reset = currentTime.Add(resetTime)
	} else {
		backoffEntry.backoffDuration = 2 * backoffEntry.backoffDuration
		if backoffEntry.backoffDuration > b.maxDuration {
			backoffEntry.backoffDuration = b.maxDuration
		}
	}
	backoffEntry.backoffUntil = currentTime.Add(backoffEntry.backoffDuration)
	backoffEntry.errorInfo = errorInfo
	return backoffEntry
}

// BackoffStatus returns status of backoff for the given reservationKey.
func (b *reservationsBackoff) BackoffStatus(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, currentTime time.Time) base_backoff.Status {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if nodeInfo == nil {
		if nodeGroup == nil {
			return base_backoff.Status{IsBackedOff: false}
		}
		klog.Warningf("Not checking reservation backoff for nodeGroup %v: nodeInfo is nil", nodeGroup.Id())
		return base_backoff.Status{IsBackedOff: false}
	}
	nodeLabels := nodeInfo.Node().GetLabels()
	key, ok := b.createReservationKey(nodeLabels)
	if !ok {
		return base_backoff.Status{IsBackedOff: false}
	}
	backoffEntry := b.reservationBackoffs[key]
	if currentTime.After(backoffEntry.backoffUntil) {
		return base_backoff.Status{IsBackedOff: false}
	}
	return base_backoff.Status{IsBackedOff: true, ErrorInfo: backoffEntry.errorInfo}
}

// RemoveBackoff removes the backoff for the reservation requested by the node of nodeInfo.
func (b *reservationsBackoff) RemoveBackoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if nodeInfo == nil {
		klog.Warningf("Not removing reservation backoff for nodeGroup %v: nodeInfo is nil", nodeGroup.Id())
		return
	}
	nodeLabels := nodeInfo.Node().GetLabels()
	key, ok := b.createReservationKey(nodeLabels)
	if !ok {
		klog.Warningf("Not removing reservation backoff for nodeGroup %v: could not create backoff key from labels", nodeGroup.Id())
		return
	}
	delete(b.reservationBackoffs, key)
}

// RemoveStaleBackoffData removes stale backoff data.
func (b *reservationsBackoff) RemoveStaleBackoffData(currentTime time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for reservationKey, backoffEntry := range b.reservationBackoffs {
		if currentTime.After(backoffEntry.reset) {
			delete(b.reservationBackoffs, reservationKey)
		}
	}
}

// createReservationKey constructs a key from node labels. It returns the key
// and a boolean indicating if the essential reservation labels were present.
func (b *reservationsBackoff) createReservationKey(nodeLabels map[string]string) (reservationKey, bool) {
	reservationName := nodeLabels[labels.ReservationNameLabel]
	if reservationName == "" {
		return reservationKey{}, false
	}

	key := reservationKey{
		id: gceclient.ReservationRef{
			Name:         reservationName,
			Project:      nodeLabels[labels.ReservationProjectLabel],
			Zone:         nodeLabels[apiv1.LabelTopologyZone],
			BlockName:    nodeLabels[labels.ReservationBlocksLabel],
			SubBlockName: nodeLabels[labels.ReservationSubBlocksLabel],
		},
		machineFamily: nodeLabels[labels.MachineFamilyLabel],
		isMultiHost:   false,
	}

	tpuType := nodeLabels[labels.TPULabel]
	if tpuType == "" {
		return key, true
	}

	machineType := nodeLabels[apiv1.LabelInstanceTypeStable]
	tpuCount, err := b.provider.MachineConfigProvider().GetTpuCountForMachineType(machineType)
	if err != nil {
		klog.Warningf("Couldn't get TPU count of machine type %q: %v", nodeLabels[apiv1.LabelInstanceTypeStable], err)
		return key, true
	}

	tpuTopology := nodeLabels[labels.TPUTopologyLabel]
	isMultiHost, err := b.provider.MachineConfigProvider().IsMultiHostTpuPodslice(tpuType, tpuTopology, tpuCount)
	if err != nil {
		klog.Warningf("Couldn't get whether MultiHost or SingleHost of configuration: (TpuType=%v, TpuTopology=%v, TpuCount=%v): %v", tpuType, nodeLabels[labels.TPUTopologyLabel], tpuCount, err)
		return key, true
	}
	key.isMultiHost = isMultiHost
	return key, true
}
