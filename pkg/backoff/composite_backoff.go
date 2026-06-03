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
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	base_backoff "k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"

	klog "k8s.io/klog/v2"
)

// CompositeBackoff is a backoff combining multiple individual backoffs.
type CompositeBackoff interface {
	base_backoff.Backoff
	// BackoffInAllZones enters a backoff in all provided zones.
	BackoffInAllZones(
		nodeGroup cloudprovider.NodeGroup,
		zones []string,
		nodeInfo *framework.NodeInfo,
		errorInfo cloudprovider.InstanceErrorInfo,
		currentTime time.Time,
	)
	// RemoveBackoff removes backoff data for the given node group for all the backoff objects wrapped
	// by this composite backoff.
	RemoveBackoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo)
	// RemoveBackoff removes backoff data for the given node group for all the backoff objects wrapped
	// by this composite backoff.
	RemoveStaleBackoffData(currentTime time.Time)
	// GetBackoffs returns all individual backoffs that make up the composite backoff.
	GetBackoffs() []base_backoff.Backoff
}

type unsynchronizedCompositeBackoff struct {
	backoffs  []base_backoff.Backoff
	observers []BackoffObserver
}

// NewCompositeBackoff creates instance of composite backoff.
func NewCompositeBackoff(backoffs []base_backoff.Backoff, observers []BackoffObserver) CompositeBackoff {
	if len(backoffs) == 0 {
		klog.Fatalf("At least one backoff must be passed to NewCompositeBackoff()")
	}
	return &unsynchronizedCompositeBackoff{
		backoffs:  append([]base_backoff.Backoff{}, backoffs...),
		observers: observers,
	}
}

func (b *unsynchronizedCompositeBackoff) Backoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) time.Time {
	var maxBackoffTime time.Time
	for _, backoff := range b.backoffs {
		backoffTime := backoff.Backoff(nodeGroup, nodeInfo, errorInfo, currentTime)
		if maxBackoffTime.Before(backoffTime) {
			maxBackoffTime = backoffTime
		}
	}
	if !maxBackoffTime.IsZero() {
		for _, obs := range b.observers {
			obs.OnBackoff(nodeGroup, errorInfo, maxBackoffTime)
		}
	}
	return maxBackoffTime
}

func (b *unsynchronizedCompositeBackoff) BackoffInAllZones(
	nodeGroup cloudprovider.NodeGroup,
	zones []string,
	nodeInfo *framework.NodeInfo,
	errorInfo cloudprovider.InstanceErrorInfo,
	currentTime time.Time,
) {
	mig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		klog.Errorf("Expected GkeMig; got %+v", nodeGroup)
		b.Backoff(nodeGroup, nodeInfo, errorInfo, currentTime)
		return
	}
	for _, z := range zones {
		b.Backoff(mig.ShallowCopyInZone(z), nodeInfo, errorInfo, currentTime)
	}
}

func (b *unsynchronizedCompositeBackoff) BackoffStatus(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, currentTime time.Time) base_backoff.Status {
	for _, backoff := range b.backoffs {
		if status := backoff.BackoffStatus(nodeGroup, nodeInfo, currentTime); status.IsBackedOff {
			return status
		}
	}
	return base_backoff.Status{IsBackedOff: false}
}

func (b *unsynchronizedCompositeBackoff) RemoveBackoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo) {
	for _, backoff := range b.backoffs {
		backoff.RemoveBackoff(nodeGroup, nodeInfo)
	}
}

func (b *unsynchronizedCompositeBackoff) RemoveStaleBackoffData(currentTime time.Time) {
	for _, backoff := range b.backoffs {
		backoff.RemoveStaleBackoffData(currentTime)
	}
	for _, obs := range b.observers {
		obs.RemoveExpiredBackoffs(currentTime)
	}
}

func (b *unsynchronizedCompositeBackoff) GetBackoffs() []base_backoff.Backoff {
	return b.backoffs
}

type synchronizedCompositeBackoff struct {
	mutex   sync.Mutex
	backoff CompositeBackoff
}

// NewSynchronizedCompositeBackoff creates instance of composite backoff that is synchronized and can be used concurrenlty.
func NewSynchronizedCompositeBackoff(backoffs []base_backoff.Backoff, observers []BackoffObserver) CompositeBackoff {
	unsynchronizedBackoff := NewCompositeBackoff(backoffs, observers)
	return &synchronizedCompositeBackoff{
		backoff: unsynchronizedBackoff,
	}
}

func (b *synchronizedCompositeBackoff) Backoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) time.Time {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.backoff.Backoff(nodeGroup, nodeInfo, errorInfo, currentTime)
}

func (b *synchronizedCompositeBackoff) BackoffInAllZones(ng cloudprovider.NodeGroup, zones []string, i *framework.NodeInfo, e cloudprovider.InstanceErrorInfo, t time.Time) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.backoff.BackoffInAllZones(ng, zones, i, e, t)
}

func (b *synchronizedCompositeBackoff) BackoffStatus(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, currentTime time.Time) base_backoff.Status {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.backoff.BackoffStatus(nodeGroup, nodeInfo, currentTime)
}

func (b *synchronizedCompositeBackoff) RemoveBackoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.backoff.RemoveBackoff(nodeGroup, nodeInfo)
}

func (b *synchronizedCompositeBackoff) RemoveStaleBackoffData(currentTime time.Time) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.backoff.RemoveStaleBackoffData(currentTime)
}

func (b *synchronizedCompositeBackoff) GetBackoffs() []base_backoff.Backoff {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.backoff.GetBackoffs()
}
