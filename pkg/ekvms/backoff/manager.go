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

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	gke_backoff "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	ek_errors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/nodetracker"
	resizable_vm_utils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/utils"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	ekvms_customthresholds "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/backoff/customthresholds"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

const (
	nodeBackoffExpiry = 5 * time.Minute
	// clusterBackoffThreshold specifies how many nodes need to be in node-level backoff to trigger cluster-level backoff.
	clusterBackoffThreshold = 3
	metricInterval          = 30 * time.Second
	// backoffResetTime is the time after first backoff when the backoff duration is reset
	backoffResetTime = 20 * time.Hour
)

type Manager interface {
	Backoff(node *v1.Node, resizeError ek_errors.ResizeError)
	IsBackedOff(machineFamily, nodeName string) bool
	DeleteNode(machineFamily, nodeName string)
	Run(stopCh <-chan struct{})
}

type cloudProvider interface {
	NodeGroupForNode(node *v1.Node) (cloudprovider.NodeGroup, error)
	ResizingEnabled(machineFamily string) bool
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

type backoffMetrics interface {
	UpdateResizeBackoffStatus(string, bool)
}

type exponentialBackoff interface {
	Backoff(errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) time.Time
	RemoveStaleBackoffData(currentTime time.Time)
	BackoffUntil() time.Time
}

type nodeTrackers map[string]map[string]nodetracker.Interface

func (t nodeTrackers) setTracker(machineFamily, errorType string, tracker nodetracker.Interface) {
	if t[machineFamily] == nil {
		t[machineFamily] = make(map[string]nodetracker.Interface)
	}
	t[machineFamily][errorType] = tracker
}

func (t nodeTrackers) getTracker(machineFamily, errorType string) (nodetracker.Interface, bool) {
	if trackerPerMachineFamily, ok := t[machineFamily]; ok {
		if tracker, ok := trackerPerMachineFamily[errorType]; ok {
			return tracker, true
		}
	}
	return nil, false
}

type manager struct {
	cloudProvider            cloudProvider
	customThresholdsProvider ekvms_customthresholds.CustomThresholdsProvider
	clock                    clock.PassiveClock
	metrics                  backoffMetrics

	mu                sync.RWMutex
	nodeBackoffs      nodeTrackers
	clusterBackoffs   map[string]exponentialBackoff
	nodeBasedBackoffs map[string]time.Time // stores machine family backoffs based on nodes backoff along with the earliest expiration time among those nodes.
}

func NewManager(cloudProvider cloudProvider, customThresholdsProvider ekvms_customthresholds.CustomThresholdsProvider, clock clock.PassiveClock) *manager {
	return &manager{
		cloudProvider:            cloudProvider,
		customThresholdsProvider: customThresholdsProvider,
		clock:                    clock,
		metrics:                  internalmetrics.Metrics,
		nodeBackoffs:             make(nodeTrackers),
		clusterBackoffs:          make(map[string]exponentialBackoff),
		nodeBasedBackoffs:        make(map[string]time.Time),
	}
}

func (m *manager) Run(stopCh <-chan struct{}) {
	wait.Until(m.updateBackoffMetrics, metricInterval, stopCh)
}

func (m *manager) Backoff(node *v1.Node, resizeError ek_errors.ResizeError) {
	if resizeError.Backoff == ek_errors.NoBackoff {
		return
	}

	family, err := resizable_vm_utils.GetMachineFamilyName(node)
	if err != nil {
		klog.Errorf("Cannot get machine family for node %q, skipping: %v", node.Name, err)
		return
	}

	switch resizeError.Backoff {
	case ek_errors.NodeLevel:
		m.backoffNodeLevel(family, node.Name, resizeError.ErrType)
	case ek_errors.ClusterLevel:
		m.backoffClusterLevel(family, resizeError)
	}
}

func (m *manager) backoffNodeLevel(machineFamily, nodeName string, errType ek_errors.ResizeErrorType) {
	m.mu.Lock()
	defer m.mu.Unlock()

	errTypeStr := string(errType)
	expirationTime := m.clock.Now().Add(nodeBackoffExpiry)
	tracker, ok := m.nodeBackoffs.getTracker(machineFamily, errTypeStr)
	if !ok {
		tracker = nodetracker.New(m.clock)
		m.nodeBackoffs.setTracker(machineFamily, errTypeStr, tracker)
	}
	tracker.AddNode(nodeName, expirationTime)
	klog.Warningf("%s %q node-level backoff (errType: %q) expires at %v", machineFamily, nodeName, errTypeStr, expirationTime)
}

func (m *manager) backoffClusterLevel(machineFamily string, resizeError ek_errors.ResizeError) {
	m.mu.Lock()
	defer m.mu.Unlock()

	currentTime := m.clock.Now()
	if _, ok := m.clusterBackoffs[machineFamily]; !ok {
		m.clusterBackoffs[machineFamily] = gke_backoff.NewExponentialBackoff(gke_backoff.ResizableFamilyInitialBackOffDuration, gke_backoff.MaxNodeGroupBackoffDuration, backoffResetTime)
	}
	backoff := m.clusterBackoffs[machineFamily]
	backoff.RemoveStaleBackoffData(currentTime)
	until := backoff.Backoff(cloudprovider.InstanceErrorInfo{
		ErrorClass:   cloudprovider.OtherErrorClass,
		ErrorCode:    string(resizeError.ErrType),
		ErrorMessage: resizeError.Error(),
	}, currentTime)
	klog.Warningf("%s cluster-level backoff expires at %v", machineFamily, until)
}

func (m *manager) IsBackedOff(machineFamily, nodeName string) bool {
	if m.isClusterBackedOff(machineFamily) {
		return true
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, tracker := range m.nodeBackoffs[machineFamily] {
		if tracker.IsTracked(nodeName) {
			return true
		}
	}
	return false
}

func (m *manager) isClusterBackoffStillActive(machineFamily string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if earliest node backoff expiration is still valid.
	if earliestNodeBackoffExpiration, ok := m.nodeBasedBackoffs[machineFamily]; ok {
		if earliestNodeBackoffExpiration.After(m.clock.Now()) {
			return true
		}
	}
	// Check if explicit cluster backoff expiration is still valid.
	if backoff, ok := m.clusterBackoffs[machineFamily]; ok {
		if backoff.BackoffUntil().After(m.clock.Now()) {
			return true
		}
	}
	return false
}

func (m *manager) isClusterBackedOff(machineFamily string) bool {
	if m.isClusterBackoffStillActive(machineFamily) {
		return true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var totalErrors int
	var earliestExpirationTime time.Time
	for errType, tracker := range m.nodeBackoffs[machineFamily] {
		nodeCount, expirationTime := tracker.Count()
		if nodeCount == 0 {
			continue
		}
		if m.customThresholdsProvider.IsErrorThresholdsFeatureEnabled() {
			// If an error type is not found in customErrorThresholds, it means that it does not trigger cluster backoff.
			if threshold, ok := m.customThresholdsProvider.GetThreshold(errType); ok && nodeCount >= threshold {
				klog.Warningf("%s cluster-level backoff is valid at least until %v", machineFamily, expirationTime)
				m.nodeBasedBackoffs[machineFamily] = expirationTime
				return true
			}
		} else {
			// If feature is disabled -> rollback to per node total error calculations
			totalErrors += nodeCount
			if earliestExpirationTime.IsZero() || expirationTime.Before(earliestExpirationTime) {
				// Determine the earliest expiration time (at least) until which a lot of nodes will be in the backoff and consequently the cluster will be.
				earliestExpirationTime = expirationTime
			}
			if totalErrors >= clusterBackoffThreshold {
				klog.Warningf("%s cluster-level backoff is valid at least until %v", machineFamily, earliestExpirationTime)
				m.nodeBasedBackoffs[machineFamily] = earliestExpirationTime
				return true
			}
		}
	}

	return false
}

func (m *manager) DeleteNode(machineFamily, nodeName string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	trackersPerMachineFamily, found := m.nodeBackoffs[machineFamily]
	if !found {
		return
	}
	for _, tracker := range trackersPerMachineFamily {
		tracker.DeleteNode(nodeName)
	}
}

func (m *manager) updateBackoffMetrics() {
	for _, family := range m.cloudProvider.MachineConfigProvider().AllResizableMachineFamilies() {
		m.metrics.UpdateResizeBackoffStatus(family.Name(), m.isClusterBackedOff(family.Name()))
	}
}
