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

package provreqcache

import (
	"sync"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/observers/loopstart"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	prpods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/klog/v2"
)

const maxNAPWaitTime = 15 * time.Minute

type provreqClient interface {
	ProvisioningRequests() ([]*provreqwrapper.ProvisioningRequest, error)
}

// QueuedProvisioningCache caches Provisioning Requests. It implements a LoopStartObserver interface to allow fetching ProvReqs
// for each Cluster Autoscaler iteration.
type QueuedProvisioningCache struct {
	client          provreqClient
	pendingProvReqs []*provreqwrapper.ProvisioningRequest
	// groupedPendingProvReqs groups QueuedProvisioningCached provisioning requests by namespace and then by the name.
	groupedPendingProvReqs map[string]map[string]*provreqwrapper.ProvisioningRequest
	// m protects provReqsWithRecentMigs and provReqsWithUpcomingMigs
	// provReqsWithRecentMigs is a list of ProvReqs that should be removed from "upcoming" list in the nearest loop start.
	// This is done to avoid race conditions due to node pools exiting out of the "upcoming" state in the middle
	// of autoscaling loop. ProvReqs will be still treated consistently as if they have upcoming node pools
	// (and thus ignored) during the whole loop.
	m sync.Mutex
	// provReqsWithRecentMigs is a set of provisioning requests for which upcoming node pools were recently initialized.
	// ProvReqs from this list will be removed from the provReqsWithUpcomingMigs set at the beginning of the nearest loop
	// to preserve consistency of the state within a single loop.
	provReqsWithRecentMigs   map[prpods.ProvReqID]bool
	provReqsWithUpcomingMigs map[prpods.ProvReqID]time.Time
}

// Validate statically that QueuedProvisioningCache implements the Observer interface.
var _ loopstart.Observer = &QueuedProvisioningCache{}

// NewQueuedProvisioningCache creates a QueuedProvisioningCache object.
func NewQueuedProvisioningCache(c provreqClient) *QueuedProvisioningCache {
	return &QueuedProvisioningCache{
		client:                   c,
		groupedPendingProvReqs:   map[string]map[string]*provreqwrapper.ProvisioningRequest{},
		m:                        sync.Mutex{},
		provReqsWithUpcomingMigs: map[prpods.ProvReqID]time.Time{},
		provReqsWithRecentMigs:   map[prpods.ProvReqID]bool{},
	}
}

// Refresh refreshes the QueuedProvisioningCache.
func (c *QueuedProvisioningCache) Refresh() {
	c.m.Lock()
	defer c.m.Unlock()
	prs, err := provreqstate.ProvisioningRequestsInState(c.client, provreqstate.PendingState)
	if err != nil {
		klog.Errorf("Failed to refresh ProvisioningRequest QueuedProvisioningCache: %s", err)
		return
	}
	c.pendingProvReqs = prs
	c.groupedPendingProvReqs = map[string]map[string]*provreqwrapper.ProvisioningRequest{}
	for _, pr := range prs {
		if _, ok := c.groupedPendingProvReqs[pr.Namespace]; !ok {
			c.groupedPendingProvReqs[pr.Namespace] = map[string]*provreqwrapper.ProvisioningRequest{}
		}
		c.groupedPendingProvReqs[pr.Namespace][pr.Name] = pr
	}
	for dpr := range c.provReqsWithRecentMigs {
		delete(c.provReqsWithUpcomingMigs, dpr)
		delete(c.provReqsWithRecentMigs, dpr)
	}
	now := time.Now()
	for upr, napStart := range c.provReqsWithUpcomingMigs {
		if now.After(napStart.Add(maxNAPWaitTime)) {
			delete(c.provReqsWithRecentMigs, upr)
		}
	}
}

// RegisterUpcomingProvReq registers upcoming node pool for a ProvisioningRequest.
func (c *QueuedProvisioningCache) RegisterUpcomingProvReq(id prpods.ProvReqID) {
	c.m.Lock()
	defer c.m.Unlock()
	c.provReqsWithUpcomingMigs[id] = time.Now()
}

// UnregisterUpcomingProvReq unregisters upcoming node pool for a ProvisioningRequest.
func (c *QueuedProvisioningCache) UnregisterUpcomingProvReq(id prpods.ProvReqID) {
	c.m.Lock()
	defer c.m.Unlock()
	c.provReqsWithRecentMigs[id] = true
}

// SplitByAsyncNAPStatus groups provisioning requests by the status of asynchronously-created node pool:
//   - withUpcomingNP - node pool is currently being created or recently (in this loop) finished creating
//   - notUpcoming - there is no upcoming node pool for this provisioning request
func (c *QueuedProvisioningCache) SplitByAsyncNAPStatus(in []*provreqwrapper.ProvisioningRequest) (
	withUpcomingNP []*provreqwrapper.ProvisioningRequest,
	withoutUpcomingNP []*provreqwrapper.ProvisioningRequest,
) {
	c.m.Lock()
	defer c.m.Unlock()
	for _, pr := range in {
		prID := prpods.GetProvReqID(pr)
		if c.provReqsWithRecentMigs[prID] || !c.provReqsWithUpcomingMigs[prID].IsZero() {
			withUpcomingNP = append(withUpcomingNP, pr)
		} else {
			withoutUpcomingNP = append(withoutUpcomingNP, pr)
		}
	}
	return withUpcomingNP, withoutUpcomingNP
}

// PendingProvReqs returns pending Provisioning Requests from the QueuedProvisioningCache.
func (c *QueuedProvisioningCache) PendingProvReqs() []*provreqwrapper.ProvisioningRequest {
	return c.pendingProvReqs
}

// IsUpcomingProvReq returns whether a provreq has a corresponding upcomming node pool.
func (c *QueuedProvisioningCache) IsUpcomingProvReq(id prpods.ProvReqID) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return !c.provReqsWithUpcomingMigs[id].IsZero()
}

// PendingProvReq returns an individual pending provisioning request from the QueuedProvisioningCache, or nil if it doesn't exist.
func (c *QueuedProvisioningCache) PendingProvReq(namespace, name string) *provreqwrapper.ProvisioningRequest {
	nsPrs, ok := c.groupedPendingProvReqs[namespace]
	if !ok {
		return nil
	}
	return nsPrs[name]
}
