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
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/processors/customresources"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/preemption"

	klog "k8s.io/klog/v2"
)

const unknownMachineFamily = "unknown"

// ResourceBackoff allows time-based backing off node groups based on resources required to provision nodes.
type ResourceBackoff struct {
	mu                     sync.RWMutex
	processor              customresources.CustomResourcesProcessor
	maxBackoffDuration     time.Duration
	initialBackoffDuration time.Duration
	backoffResetTimeout    time.Duration
	backoffDataByLocation  map[string]resourceBackoffData
}

type resourceBackoffData []resourceBackoffEntry

type resourceBackoffEntry struct {
	backoffKey   map[string]NodeResource
	backoffUntil time.Time
	lastDuration time.Duration
	errorInfo    cloudprovider.InstanceErrorInfo
}

// NewResourceBackoff creates new instance of resource based backoff.
// Resource based backoff allows time-based backing off node groups based on resources required to provision
// nodes. Internally ResourceBackoff uses NodeResources objects as backoff keys. NodeResources are obtained for
// NodeInfo using GetNodeResources function
//
// The checking if given set of resources is backed-off and removal of backoff keys use concept of matching.
func NewResourceBackoff(processor customresources.CustomResourcesProcessor, initialBackoffDuration time.Duration, maxBackoffDuration time.Duration, backoffResetTimeout time.Duration) backoff.Backoff {
	return &ResourceBackoff{
		processor:              processor,
		initialBackoffDuration: initialBackoffDuration,
		maxBackoffDuration:     maxBackoffDuration,
		backoffResetTimeout:    backoffResetTimeout,
		backoffDataByLocation:  make(map[string]resourceBackoffData),
	}
}

// Backoff execution for the resources used by nodes created by given node group.
func (b *ResourceBackoff) Backoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !IsStockout(nodeGroup, nodeInfo, errorInfo) {
		return currentTime
	}

	if !nodeGroup.Autoprovisioned() {
		// we are using resource backoff based on non-autoprovisioned node groups. Motivation case below:
		//
		// Groups:
		// A - 2 cpu
		// B - 4 cpu
		// C - 2 cpu, 1 local SSDs
		//
		// Stockout on local SSD will cause ALL groups to be backed-off, so pods that don't request SSDs
		// won't trigger scale up. Effectively, such node pool breaks the cluster for the duration of
		// stockout + backoff recovery.
		//
		// The same will hold true for any new resource we don't yet support explicitly in
		// resource-based back-off. Limiting the use of it to NAP ensures we'll only ever back-off on
		// resources already supported in NAP. Trade-off is attempting each of existing groups
		// (existing behavior) before backing off.
		return currentTime
	}

	if nodeInfo == nil {
		klog.Warningf("Not backing off nodeGroup %v by resources because nodeInfo is not provided", nodeGroup.Id())
		return currentTime
	}

	location := nodeInfo.Node().Labels[v1.LabelTopologyZone]
	if errorInfo.ErrorCode == gce.ErrorCodeQuotaExceeded {
		location = nodeInfo.Node().Labels[v1.LabelTopologyRegion]
	}

	resources, err := GetNodeResources(b.processor, nodeInfo, nodeGroup, location)
	if err != nil {
		klog.Warningf("Not backing off nodeGroup %v by resources; got error when getting resources for nodeInfo; %v", nodeGroup.Id(), err)
		return currentTime
	}
	var maxBackoffTime time.Time
	expensiveResourcesFound := false
	for _, resource := range resources {
		if resource.CostClass == ExpensiveCostClass {
			// also add separate backoff for each expensive resource
			backoffTime := b.backoffResources(addVirtualResources(NodeResources{resource}, nodeInfo, nodeGroup), errorInfo, currentTime)
			if backoffTime.After(maxBackoffTime) {
				maxBackoffTime = backoffTime
			}
			expensiveResourcesFound = true
		}
	}
	if !expensiveResourcesFound {
		// We backoff whole resource set only in case no expensive resources were part of the set.
		// In other case it does not make sense as whole set would be strictly less selective backoff key.
		maxBackoffTime = b.backoffResources(addVirtualResources(resources, nodeInfo, nodeGroup), errorInfo, currentTime)
	}

	klog.Infof("Applying resource based backoff to node group %s for resources %+v", getNodeGroupId(nodeGroup), resources)
	return maxBackoffTime
}

func addVirtualResources(resources NodeResources, nodeInfo *framework.NodeInfo, nodeGroup cloudprovider.NodeGroup) NodeResources {
	if len(resources) == 0 {
		// unexpected; just return empty resources.
		return resources
	}

	// the preemptible split is irrelevant for DWS, no sense to add vm_priority
	if resources[0].CapacityClass == FlexStartCapacityClass {
		return resources
	}

	location := resources[0].Location

	vmPriority := int64(1)
	if preemptionType := preemption.TypeFromLabels(nodeInfo.Node().Labels); preemptionType != preemption.NoPreemption {
		vmPriority = 2
	}

	machineFamily, err := gke.GetMachineFamilyFromNodeGroup(nodeGroup)
	if err != nil {
		klog.Warningf("Could not get machine family from node group %s", getNodeGroupId(nodeGroup))
		machineFamily = unknownMachineFamily
	}

	// Assign fake vm_priority resource value 1 for standard VMs and 2 for preemption VMs.
	// That way we ensure that backing off resources for standard VMs back offs also preemption VMs but not
	// the other way around.
	vmPriorityResource := NodeResource{
		Type:          "vm_priority",
		MachineFamily: machineFamily,
		Value:         vmPriority,
		Location:      location,
		CostClass:     StandardCostClass,
		CapacityClass: StandardCapacityClass,
	}

	return append(resources, vmPriorityResource)
}

func getNodeGroupId(nodeGroup cloudprovider.NodeGroup) any {
	if nodeGroup == nil {
		return "nil"
	}
	return nodeGroup.Id()
}

// IsBackedOff returns true if execution is backed off for resources used by nodes created by given node group.
// It checks if there exists a backed off resources object, which matches resources obtained from nodeInfo passed as parameter.
func (b *ResourceBackoff) BackoffStatus(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, currentTime time.Time) backoff.Status {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if nodeInfo == nil {
		klog.Warningf("Not checking backoff for nodeGroup %v by resources because nodeInfo is not provided", nodeGroup.Id())
		return backoff.Status{IsBackedOff: false}
	}

	locations := []string{nodeInfo.Node().Labels[v1.LabelTopologyZone], nodeInfo.Node().Labels[v1.LabelTopologyRegion]}
	for _, location := range locations {
		resources, err := GetNodeResources(b.processor, nodeInfo, nodeGroup, location)
		if err != nil {
			klog.Warningf("Not checking backoff for nodeGroup %v by resources; got error when getting resources for nodeInfo; %v", nodeGroup.Id(), err)
			continue
		}

		backoffStatus, _ := b.areResourcesBackedOff(addVirtualResources(resources, nodeInfo, nodeGroup), currentTime)
		if backoffStatus.IsBackedOff {
			return backoffStatus
		}
	}

	return backoff.Status{IsBackedOff: false}
}

// RemoveBackoff removes backoff data for resources used by nodes created by given node group.
// It removes all backoff keys which match resources obtained from nodeInfo passed as parameter.
func (b *ResourceBackoff) RemoveBackoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if nodeInfo == nil {
		klog.Warningf("Not removing backoff for nodeGroup %v by resources because nodeInfo is not provided", nodeGroup.Id())
		return
	}
	resources, err := GetNodeResources(b.processor, nodeInfo, nodeGroup, nodeInfo.Node().Labels[v1.LabelTopologyZone])
	if err != nil {
		klog.Warningf("Not removing backoff for nodeGroup %v by resources; got error when getting resources for nodeInfo; %v", nodeGroup.Id(), err)
		return
	}

	b.removeBackoffForResources(addVirtualResources(resources, nodeInfo, nodeGroup))
	for _, resource := range resources {
		if resource.CostClass == ExpensiveCostClass {
			// remove backoff for each expensive resource separately
			b.removeBackoffForResources(addVirtualResources(NodeResources{resource}, nodeInfo, nodeGroup))
		}
	}
}

// RemoveStaleBackoffData removes stale backoff data.
func (b *ResourceBackoff) RemoveStaleBackoffData(currentTime time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for location, backoffData := range b.backoffDataByLocation {
		newBackoffData := make(resourceBackoffData, 0)
		for _, backoffEntry := range backoffData {
			if backoffEntry.backoffUntil.Add(b.backoffResetTimeout).After(currentTime) {
				newBackoffData = append(newBackoffData, backoffEntry)
			}
		}
		if len(newBackoffData) > 0 {
			b.backoffDataByLocation[location] = newBackoffData
		} else {
			delete(b.backoffDataByLocation, location)
		}
	}
}

func (b *ResourceBackoff) backoffResources(resources NodeResources, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) time.Time {
	location := b.singleLocation(resources)
	if location == "" {
		klog.Warningf("BackoffResources requires all resources to be from single location")
		return currentTime
	}

	if backoffStatus, until := b.areResourcesBackedOff(resources, currentTime); backoffStatus.IsBackedOff {
		return until
	}

	backoffData := b.backoffDataByLocation[location]
	var newBackoffData = make(resourceBackoffData, 0)

	backoffDuration := b.initialBackoffDuration
	for _, backoffEntry := range backoffData {
		if b.resourcesOverrideBackoffKey(backoffEntry.backoffKey, resources) {
			if b.resourcesEqualToKey(backoffEntry.backoffKey, resources) {
				backoffDuration = backoffEntry.lastDuration * 2
				backoffEntry.errorInfo = errorInfo
				if backoffDuration > b.maxBackoffDuration {
					backoffDuration = b.maxBackoffDuration
				}
			} else {
				// even if technically we are dropping "equal" entry here we want to suppress the logging as same entry will be added later on
				klog.V(2).Infof("Dropping resource backoff entry %+v; because backoff is requested for more general one %+v", backoffEntry, resources)
			}
		} else {
			newBackoffData = append(newBackoffData, backoffEntry)
		}
	}
	newBackoffEntry := b.newBackoffEntry(resources, currentTime.Add(backoffDuration), errorInfo, backoffDuration)

	klog.V(2).Infof("Adding resource backoff entry %+v", newBackoffEntry)
	newBackoffData = append(newBackoffData, newBackoffEntry)
	b.backoffDataByLocation[location] = newBackoffData

	return newBackoffEntry.backoffUntil
}

func (b *ResourceBackoff) newBackoffEntry(resources NodeResources, backoffUntil time.Time, errorInfo cloudprovider.InstanceErrorInfo, lastDuration time.Duration) resourceBackoffEntry {
	backoffEntry := resourceBackoffEntry{
		backoffUntil: backoffUntil,
		lastDuration: lastDuration,
		backoffKey:   newBackoffKey(resources),
		errorInfo:    errorInfo,
	}
	return backoffEntry
}

func newBackoffKey(resources NodeResources) map[string]NodeResource {
	backoffKey := make(map[string]NodeResource)
	for _, resource := range resources {
		backoffKey[resource.getKey()] = resource
	}
	return backoffKey
}

func (b *ResourceBackoff) areResourcesBackedOff(resources NodeResources, currentTime time.Time) (backoff.Status, time.Time) {
	location := b.singleLocation(resources)
	if location == "" {
		klog.Warningf("AreResourcesBackedOff requires all resources to be from single location")
		return backoff.Status{IsBackedOff: false}, time.Time{}
	}

	var backoffReason cloudprovider.InstanceErrorInfo
	maxBackoffTime := currentTime
	for _, backoffEntry := range b.backoffDataByLocation[location] {
		if backoffEntry.backoffUntil.After(currentTime) && b.backoffKeyCoversResources(backoffEntry.backoffKey, resources) {
			if backoffEntry.backoffUntil.After(maxBackoffTime) {
				maxBackoffTime = backoffEntry.backoffUntil
				backoffReason = backoffEntry.errorInfo
			}
		}
	}
	if maxBackoffTime.After(currentTime) {
		return backoff.Status{IsBackedOff: true, ErrorInfo: backoffReason}, maxBackoffTime
	}
	return backoff.Status{IsBackedOff: false}, maxBackoffTime
}

func (b *ResourceBackoff) removeBackoffForResources(resources NodeResources) {
	location := b.singleLocation(resources)
	if location == "" {
		klog.Warningf("RemoveBackoffForResources requires all resources to be from single location")
		return
	}

	var newBackoffData = make(resourceBackoffData, 0)
	for _, backoffEntry := range b.backoffDataByLocation[location] {
		// we leave just non-matching entries
		if !b.backoffKeyCoversResources(backoffEntry.backoffKey, resources) {
			newBackoffData = append(newBackoffData, backoffEntry)
		} else {
			klog.V(2).Infof("Removing backoff for resources=%+v resulted in removing backoff entry %+v", resources, backoffEntry)
		}
	}
	b.backoffDataByLocation[location] = newBackoffData
}

// A NodeResources R matches the given constraint NodeResources C in the same location if:
//
// for every resource in C there exists a resource in R such that:
//   - resource.Type == constraint.Type
//   - resource.Value >= constraint.Value
//
// This is to avoid matching e.g. a CPU-only request with combined {CPU, memory} constraint.
//
// This method assumes both keys refer to resources in the same location.
func (b *ResourceBackoff) backoffKeyCoversResources(backoffKey map[string]NodeResource, resources NodeResources) bool {
	return b.backoffKeyCoversBackoffKey(backoffKey, newBackoffKey(resources))
}

func (b *ResourceBackoff) resourcesOverrideBackoffKey(backoffKey map[string]NodeResource, resources NodeResources) bool {
	return b.backoffKeyCoversBackoffKey(newBackoffKey(resources), backoffKey)
}

func (b *ResourceBackoff) backoffKeyCoversBackoffKey(backoffKeyA, backoffKeyB map[string]NodeResource) bool {
	for _, backoffKeyResource := range backoffKeyA {
		resource, found := backoffKeyB[backoffKeyResource.getKey()]
		if !found || backoffKeyResource.Value > resource.Value {
			return false
		}
	}
	return true
}

func (b *ResourceBackoff) resourcesEqualToKey(backoffKey map[string]NodeResource, resources NodeResources) bool {
	if len(resources) != len(backoffKey) {
		return false
	}
	for _, resource := range resources {
		keyResource, found := backoffKey[resource.getKey()]
		if !found {
			return false
		}
		if keyResource.Value != resource.Value {
			return false
		}
	}
	return true
}

func (b *ResourceBackoff) singleLocation(resources NodeResources) string {
	location := ""
	for _, resource := range resources {
		if location == "" {
			location = resource.Location
		}
		if location != resource.Location {
			return ""
		}
	}
	return location
}
