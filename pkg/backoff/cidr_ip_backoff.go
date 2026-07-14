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
	"strings"
	"sync"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	base_backoff "k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	klog "k8s.io/klog/v2"
)

type CidrIpBackoff struct {
	mu                     sync.RWMutex
	initialDuration        time.Duration
	maxDuration            time.Duration
	cidrIpBackoffResetTime time.Duration
	cidrIpBackoffs         map[string]*exponentialBackoff
	napBackoff             *exponentialBackoff
}

// NewCidrIpBackoff initialises CidrIpBackoff.
func NewCidrIpBackoff(initialBackoffDuration, maxBackoffDuration time.Duration, backoffResetTime time.Duration) base_backoff.Backoff {
	return &CidrIpBackoff{
		initialDuration:        initialBackoffDuration,
		maxDuration:            maxBackoffDuration,
		cidrIpBackoffResetTime: backoffResetTime,
		cidrIpBackoffs:         make(map[string]*exponentialBackoff),
		napBackoff:             NewExponentialBackoff(initialBackoffDuration, maxBackoffDuration, backoffResetTime),
	}
}

// Backoff execution for the given node group. Returns time till execution is backed off.
func (b *CidrIpBackoff) Backoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()

	if errorInfo.ErrorCode != gce.ErrorIPSpaceExhausted {
		return currentTime
	}
	var until time.Time
	gkeMig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		return currentTime
	}
	// If IP ranges are not configured explicitly on a node pool, which is always true
	// in NAP, subnetwork and pod IP range are selected by the control plane.
	// Therefore, if we encounter IP exhaustion error on a fresh NAP node, it means that the control plane
	// could not find an IP range with enough IP addresses and we should backoff the entire NAP.
	if gkeMig.Autoprovisioned() {
		// In NAP, we first create an empty node pool, and then we scale it up to the desired size.
		// Because of that, the IP space exhaustion error will throw on already existing node pool
		// during its initial scale up. We don't want to backoff NAP when scaling up a node pool
		// that existed before. We need to differentiate between a fresh (but already existing)
		// node pool and an older one.
		isInitialScaleUp, err := isInitialScaleUp(nodeGroup)
		if err != nil {
			klog.Errorf("Failed to check if scale up was initial for node group %q, err: %v", nodeGroup.Id(), err)
			return currentTime
		}
		if isInitialScaleUp {
			klog.Infof("Could not find an IP range with enough addresses for nodes or pods for node group %q, backing off NAP", nodeGroup.Id())
			until = b.napBackoff.Backoff(errorInfo, currentTime)
		}
	}
	podIpv4CidrBlock := getPodIpv4CidrBlockForNodeGroup(nodeGroup)
	if podIpv4CidrBlock == "" {
		klog.Errorf("Error getting podIpv4CidrBlock for node group %s, returning currentTime", nodeGroup.Id())
		return currentTime
	}

	if !strings.Contains(errorInfo.ErrorMessage, podIpv4CidrBlock) {
		subnet := getNodeGroupSubnetwork(nodeGroup)
		klog.Infof("Pod CIDR not found in error message, applying backoff for the entire subnet %q: %s", subnet, errorInfo.ErrorMessage)
		backoffEntry := b.backoffExponential(subnet, errorInfo, currentTime)
		b.cidrIpBackoffs[subnet] = backoffEntry
		if backoffEntry.BackoffUntil().After(until) {
			until = backoffEntry.BackoffUntil()
		}
		return until
	}

	klog.Infof("Applying backoff for cidr: %s", podIpv4CidrBlock)
	backoffEntry := b.backoffExponential(podIpv4CidrBlock, errorInfo, currentTime)
	b.cidrIpBackoffs[podIpv4CidrBlock] = backoffEntry
	if backoffEntry.BackoffUntil().After(until) {
		return backoffEntry.BackoffUntil()
	}
	return until
}

// BackoffStatus returns whether the execution is backed off for the given node group and error info when the node group is backed off.
func (b *CidrIpBackoff) BackoffStatus(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, currentTime time.Time) base_backoff.Status {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !nodeGroup.Exist() && b.napBackoff.BackoffUntil().After(currentTime) {
		return base_backoff.Status{IsBackedOff: true, ErrorInfo: b.napBackoff.ErrorInfo()}
	}
	podIpv4CidrBlock := getPodIpv4CidrBlockForNodeGroup(nodeGroup)
	subnet := getNodeGroupSubnetwork(nodeGroup)

	var backoffEntry *exponentialBackoff
	podIPEntry := b.cidrIpBackoffs[podIpv4CidrBlock]
	subnetEntry := b.cidrIpBackoffs[subnet]

	if subnetEntry == nil && podIPEntry == nil {
		return base_backoff.Status{IsBackedOff: false}
	}
	if subnetEntry == nil {
		backoffEntry = podIPEntry
	} else if podIPEntry == nil {
		backoffEntry = subnetEntry
	} else {
		if subnetEntry.BackoffUntil().After(podIPEntry.BackoffUntil()) {
			backoffEntry = subnetEntry
		} else {
			backoffEntry = podIPEntry
		}
	}

	if currentTime.After(backoffEntry.BackoffUntil()) {
		return base_backoff.Status{IsBackedOff: false}
	}

	return base_backoff.Status{IsBackedOff: true, ErrorInfo: backoffEntry.ErrorInfo()}
}

// RemoveBackoff removes backoff data for the given node group.
func (b *CidrIpBackoff) RemoveBackoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo) {
	b.mu.Lock()
	defer b.mu.Unlock()

	subnet := getNodeGroupSubnetwork(nodeGroup)
	podIpv4CidrBlock := getPodIpv4CidrBlockForNodeGroup(nodeGroup)
	delete(b.cidrIpBackoffs, subnet)
	delete(b.cidrIpBackoffs, podIpv4CidrBlock)
}

// RemoveStaleBackoffData removes stale backoff data.
func (b *CidrIpBackoff) RemoveStaleBackoffData(currentTime time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for backoffKey, backoffEntry := range b.cidrIpBackoffs {
		// Remove backoff only on case its reset time has come
		if currentTime.After(backoffEntry.ResetTime()) {
			delete(b.cidrIpBackoffs, backoffKey)
		}
	}
	b.napBackoff.RemoveStaleBackoffData(currentTime)
}

func getNodeGroupSubnetwork(nodeGroup cloudprovider.NodeGroup) string {
	gkeMig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		return ""
	}
	return gkeMig.Spec().Subnetwork

}

func getPodIpv4CidrBlockForNodeGroup(nodeGroup cloudprovider.NodeGroup) string {
	gkeMig, ok := nodeGroup.(*gke.GkeMig)

	if !ok {
		return ""
	}

	podIpv4CidrBlock := gkeMig.Spec().PodIpv4CidrBlock
	return podIpv4CidrBlock
}

func (b *CidrIpBackoff) backoffExponential(podIpv4CidrBlock string, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) *exponentialBackoff {
	backoffEntry, ok := b.cidrIpBackoffs[podIpv4CidrBlock]

	if !ok {
		backoffEntry = NewExponentialBackoff(b.initialDuration, b.maxDuration, b.cidrIpBackoffResetTime)
		b.cidrIpBackoffs[podIpv4CidrBlock] = backoffEntry
	}

	backoffEntry.Backoff(errorInfo, currentTime)
	return backoffEntry
}

// isInitialScaleUp checks if this is the first scale up in autoprovisioned
// node pool. We assume that scale up is initial if there are no nodes in the group,
// or all nodes are in InstanceCreating state.
func isInitialScaleUp(nodeGroup cloudprovider.NodeGroup) (bool, error) {
	nodes, err := nodeGroup.Nodes()
	if err != nil {
		return false, err
	}
	isInitialScaleUp := true
	for _, instance := range nodes {
		if instance.Status.State != cloudprovider.InstanceCreating {
			isInitialScaleUp = false
		}
	}
	return isInitialScaleUp, nil
}
