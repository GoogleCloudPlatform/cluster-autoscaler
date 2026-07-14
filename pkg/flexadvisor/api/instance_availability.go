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

package api

import (
	"fmt"
	"sync"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
)

// InstanceAvailability holds the state of available capacity for a given instance configuration.
// It acts as a thread-safe cache that reconciles the latest guidance from the
// provider with local provisioning decisions made since the last refresh.
// TODO(b/514649544): refactor InstanceAvailability's thread-safety
type InstanceAvailability struct {
	mutex                           sync.Mutex
	zonalInstanceCount              map[string]int
	zonalProvisionsSinceLastRefresh map[string]int
	zonalGcePreferenceScore         map[string]float64
	guidanceId                      string
	instanceConfigKey               string
	flexibilityScopeKey             string
	decisionChan                    chan ProvisioningDecisionNotification
	provider                        instanceavailability.Provider
}

// String formats InstanceAvailability omitting non-thread-safe fields
func (ia *InstanceAvailability) String() string {
	return fmt.Sprintf("InstanceAvailability{flexibilityScopeKey: %s, instanceConfigKey: %s, guidanceId: %s, zonalInstanceCount: <non-thread-safe format>, zonalProvisionsSinceLastRefresh: <non-thread-safe format>, zonalGcePreferenceScore: <non-thread-safe format>}",
		ia.flexibilityScopeKey, ia.instanceConfigKey, ia.guidanceId)
}

// threadSafeFullString thread safe version of String() that includes all the fields
func (ia *InstanceAvailability) threadSafeFullString() string {
	ia.mutex.Lock()
	defer ia.mutex.Unlock()
	return fmt.Sprintf("InstanceAvailability{flexibilityScopeKey: %s, instanceConfigKey: %s, guidanceId: %s, zonalInstanceCount: %v, zonalProvisionsSinceLastRefresh: %v, zonalGcePreferenceScore: %v}",
		ia.flexibilityScopeKey, ia.instanceConfigKey, ia.guidanceId, ia.zonalInstanceCount, ia.zonalProvisionsSinceLastRefresh, ia.zonalGcePreferenceScore)
}

// NewInstanceAvailability creates a new InstanceAvailability.
func NewInstanceAvailability(flexibilityScopeKey, instanceConfigKey, guidanceId string, zonalInstanceCount map[string]int, zonalGcePreferenceScore map[string]float64) *InstanceAvailability {
	return &InstanceAvailability{
		flexibilityScopeKey:             flexibilityScopeKey,
		instanceConfigKey:               instanceConfigKey,
		guidanceId:                      guidanceId,
		zonalInstanceCount:              zonalInstanceCount,
		zonalProvisionsSinceLastRefresh: make(map[string]int),
		zonalGcePreferenceScore:         zonalGcePreferenceScore,
	}
}

// MarkUsed in-place updates current scopes entry with information about VMs actually provisioned.
func (i *InstanceAvailability) MarkUsed(zonalInstancesToProvision map[string]int, decisionId, guidanceId string) {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	for zone, count := range zonalInstancesToProvision {
		if _, found := i.zonalInstanceCount[zone]; !found {
			continue
		}
		i.zonalProvisionsSinceLastRefresh[zone] += count
	}
	i.sendProvisioningDecision(decisionId, guidanceId, zonalInstancesToProvision)
}

func (i *InstanceAvailability) sendProvisioningDecision(decisionId, guidanceId string, zonalInstancesToProvision map[string]int) {
	decision := NewProvisioningDecisionNotification(i.flexibilityScopeKey, i.instanceConfigKey, guidanceId, decisionId, zonalInstancesToProvision)

	select {
	case i.decisionChan <- decision:
	default:
		metrics.Metrics.IncrementFlexAdvisorFeedbackDecisionCount(metrics.FADecisionFeedbackDropped)
		klog.Warningf("FlexAdvisor[async-worker]: Provisioning decision channel is full, dropping notification flexibilityScopeKey=%v, instanceConfigKey=%v", i.flexibilityScopeKey, i.instanceConfigKey)
	}
}

// ReconcileAndUpdate updates the new availability data (i) by reconciling it
// with the previous state.
//
// This method solves a race condition where provisioning decisions can be made
// while a remote API call to fetch new availability data is in-flight. It
// calculates these "in-flight" provisions by comparing the state of the
// oldConfigData against the snapshot taken before the API call, and then
// subtracts that delta from the newly fetched data to get a correct,
// up-to-date count.
func (i *InstanceAvailability) ReconcileAndUpdate(oldConfigData *InstanceAvailability, provisionsBeforeApiCall map[string]int) {
	// mutex of `i` is not locked because only a single go routine is expected to work on this object.
	// `i` is the newly fetched object, and it is not exposed via flexibility scope
	oldConfigData.mutex.Lock()
	defer oldConfigData.mutex.Unlock()

	// This method performs an atomic update of the scopes value. Its logic is based
	// on the fact that `zonalProvisionsSinceLastRefresh` is a *delta* counter for
	// the current refresh cycle, not a cumulative total.
	//
	// The process is:
	// 1. Read the most up-to-date provision count (`provisionsAfterApiCall`). This
	//    represents all `MarkUsed` calls made during the current cycle.
	// 2. Calculate the number of provisions that happened *during* the API call
	//    (provisionsAfterApiCall - provisionsBeforeApiCall).
	// 3. Adjust the new `zonalInstanceCount` from the API by subtracting these
	//    in-flight provisions. This gives a correct, up-to-date availability count.
	// 4. Intentionally reset `zonalProvisionsSinceLastRefresh` to a new map of zeros
	//    to begin the count for the *next* refresh cycle.

	// 1. READ the most current "after" state.
	provisionsAfterApiCall := oldConfigData.zonalProvisionsSinceLastRefresh

	// 2 & 3. COMPUTE and ADJUST the new counts based on the number of provisions that happened *during* the API call.
	for zone, countAfter := range provisionsAfterApiCall {
		countBefore := 0
		if provisionsBeforeApiCall != nil {
			countBefore = provisionsBeforeApiCall[zone]
		}
		newProvisions := countAfter - countBefore
		if _, ok := i.zonalInstanceCount[zone]; ok && newProvisions != 0 {
			klog.Infof("FlexAdvisor[async-worker]: reconciling provisions made during refresh %v->%v, flexibilityScopeKey=%v, ia=%s, zone=%v", i.zonalInstanceCount[zone], i.zonalInstanceCount[zone]-newProvisions, i.flexibilityScopeKey, i.threadSafeFullString(), zone)
			i.zonalInstanceCount[zone] -= newProvisions
		}
	}

	// 4. RESET the delta counter for the next cycle.
	i.zonalProvisionsSinceLastRefresh = make(map[string]int)
}

// NewSnapshot returns a Snapshot of InstanceAvailability
func (i *InstanceAvailability) NewSnapshot() *instanceavailability.Snapshot {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	availableInstanceCount := make(map[string]int)
	for zone, count := range i.zonalInstanceCount {
		availableInstanceCount[zone] = count - i.zonalProvisionsSinceLastRefresh[zone]
	}

	return instanceavailability.NewSnapshot(i.provider, i.flexibilityScopeKey, i.instanceConfigKey, i.guidanceId, availableInstanceCount, i.zonalGcePreferenceScore)
}

// SetProvider sets instanceavailability.Provider
func (i *InstanceAvailability) SetProvider(provider instanceavailability.Provider) {
	i.provider = provider
}

// SetDecisionChan sets ProvisioningDecisionNotification chan
func (i *InstanceAvailability) SetDecisionChan(decisionChan chan ProvisioningDecisionNotification) {
	i.decisionChan = decisionChan
}

// ZonalProvisionCounts maps a zone to the number of instances being provisioned there.
type ZonalProvisionCounts map[string]int

// ZonalProvisionsSinceLastRefresh returns a snapshot of zonal provisioning counts since last cache refresh.
func (i *InstanceAvailability) ZonalProvisionsSinceLastRefresh() ZonalProvisionCounts {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	zonalCount := make(ZonalProvisionCounts)
	for zone, count := range i.zonalProvisionsSinceLastRefresh {
		zonalCount[zone] = count
	}
	return zonalCount
}

// InstanceConfigKey returns the instance configuration key.
func (i *InstanceAvailability) InstanceConfigKey() string {
	return i.instanceConfigKey
}

// FlexibilityScopeKey returns the flexibility scope key.
func (i *InstanceAvailability) FlexibilityScopeKey() string {
	return i.flexibilityScopeKey
}

// GuidanceId returns the guidance ID.
func (i *InstanceAvailability) GuidanceId() string {
	return i.guidanceId
}
