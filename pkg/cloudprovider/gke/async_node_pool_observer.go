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

package gke

import (
	"context"
	"fmt"
	"sync"
	"time"

	klog "k8s.io/klog/v2"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

const (
	// maxClusterFetchFailures how many times cluster fetch can fail during node pool registertion check
	maxClusterFetchFailures = 10
	// delayBetweenFailedClusterCheckLoops the delay between each cluster fetch failure during node pool registration check
	delayBetweenFailedClusterCheckLoops = 10 * time.Second
	// periodicNodePoolSyncInterval maximum time in which node pool sync happens, in case communication with GKE failed in the past.
	periodicNodePoolSyncInterval = 1 * time.Minute
)

type nodePoolSyncResult struct {
	creationResult *MigCreateNodePoolResult
	err            error
}

type clusterProvider func() (gkeclient.Cluster, error)

type asyncNodePoolRegistrationObserver struct {
	context         context.Context
	clusterProvider clusterProvider
	domainURL       string
	nodePoolsMutex  sync.Mutex
	nodePools       map[string]*syncingNodePool
	executionMutex  sync.Mutex
	lastExecution   time.Time
}

func newAsyncNodePoolRegistrationObserver(context context.Context, clusterProvider clusterProvider, domainURL string) *asyncNodePoolRegistrationObserver {
	registrationMonitor := &asyncNodePoolRegistrationObserver{
		context:         context,
		clusterProvider: clusterProvider,
		nodePools:       make(map[string]*syncingNodePool),
		domainURL:       domainURL,
	}
	schedulePeriodicNodePoolSync(registrationMonitor, context)
	return registrationMonitor
}

// schedulePeriodicNodePoolSync it's a failsafe mechanism that periodically runs node pool synchronization in case it previously failed.
func schedulePeriodicNodePoolSync(synchronizer *asyncNodePoolRegistrationObserver, ctx context.Context) {
	go func(ctx context.Context) {
		for {
			select {
			case <-time.After(periodicNodePoolSyncInterval):
				synchronizer.syncNodePoolsOnTimer()
			case <-ctx.Done():
				klog.Info("Closing node pool synchronizer")
				synchronizer.close()
				return
			}
		}
	}(ctx)
}

func (m *asyncNodePoolRegistrationObserver) wait(gkeManager GkeManager, mainMig *GkeMig) (<-chan nodePoolSyncResult, error) {
	m.nodePoolsMutex.Lock()
	defer m.nodePoolsMutex.Unlock()
	nodePoolName := mainMig.NodePoolName()
	if _, contains := m.nodePools[nodePoolName]; contains {
		return nil, fmt.Errorf("node pool %s is already waiting for cluster registration", nodePoolName)
	}
	channel := make(chan nodePoolSyncResult, 1)
	m.nodePools[nodePoolName] = &syncingNodePool{
		gkeManager:  gkeManager,
		mainMig:     mainMig,
		syncStarted: time.Now(),
		resultChan:  channel,
	}
	go m.syncNodePools()
	return channel, nil
}

func (m *asyncNodePoolRegistrationObserver) stopWaitingFor(nodePoolName string) {
	m.sendSyncResult(nodePoolName, nodePoolSyncResult{err: fmt.Errorf("cancelled waiting for node pool")})
}

func (m *asyncNodePoolRegistrationObserver) syncNodePoolsOnTimer() {
	m.executionMutex.Lock()
	defer m.executionMutex.Unlock()
	syncingNodePools := m.syncingNodePoolNames()
	if len(syncingNodePools) == 0 {
		return
	}
	if time.Since(m.lastExecution) < periodicNodePoolSyncInterval {
		return
	}
	m.syncNodePoolsNoLock()
}

func (m *asyncNodePoolRegistrationObserver) syncNodePools() {
	m.executionMutex.Lock()
	defer m.executionMutex.Unlock()
	m.syncNodePoolsNoLock()
}

// syncNodePools periodically checks if upcoming node pools are reported by GKE as a part of the cluster.
func (m *asyncNodePoolRegistrationObserver) syncNodePoolsNoLock() {
	syncingNodePools := m.syncingNodePoolNames()
	if len(syncingNodePools) == 0 {
		return
	}
	defer func() {
		m.lastExecution = time.Now()
	}()
	klog.Infof("Starting node pools cluster registration checking loop")
	clusterFetchFailures := 0
	for len(syncingNodePools) > 0 {
		start := time.Now()
		cluster, err := m.clusterProvider()
		m.lastExecution = time.Now()
		if err != nil {
			clusterFetchFailures++
			if clusterFetchFailures >= maxClusterFetchFailures {
				klog.Warningf("Failed fetching cluster for node pools cluster registration check (failures: %d). Waiting %s and exitting the loop. Error: %v", clusterFetchFailures, delayBetweenFailedClusterCheckLoops, err)
				select {
				case <-m.context.Done():
					klog.Infof("Node pool synchronizer interrupted by closed context")
					return
				case <-time.After(delayBetweenFailedClusterCheckLoops):
				}
				// it's ok to break the loop. It is restarted periodically.
				break
			}
			klog.Warningf("Could not fetch cluster for node pools cluster registration check (failures: %d/%d). Waiting %s. Error: %v", clusterFetchFailures, maxClusterFetchFailures, migCreationCheckInterval, err)
		} else {
			clusterFetchFailures = 0
			klog.Infof("Fetched cluster to check %d node pools registration [time: %s] (nodePools: %v)", len(syncingNodePools), time.Since(start), syncingNodePools)
			m.syncWithCluster(&cluster)
		}
		m.cleanUpStaleNodePools()
		syncingNodePools = m.syncingNodePoolNames()
		if len(syncingNodePools) > 0 {
			select {
			case <-m.context.Done():
				klog.Infof("Node pool synchronizer interrupted by closed context")
				return
			case <-time.After(migCreationCheckInterval):
				syncingNodePools = m.syncingNodePoolNames()
			}
		}
	}
	klog.Infof("Stopping node pool cluster registration checking loop. There are no more upcoming node pools to sync.")
}

func (m *asyncNodePoolRegistrationObserver) syncingNodePoolNames() []string {
	m.nodePoolsMutex.Lock()
	defer m.nodePoolsMutex.Unlock()
	var names []string
	for name := range m.nodePools {
		names = append(names, name)
	}
	return names
}

func (m *asyncNodePoolRegistrationObserver) cleanUpStaleNodePools() {
	m.nodePoolsMutex.Lock()
	defer m.nodePoolsMutex.Unlock()
	for nodePoolName, nodePool := range m.nodePools {
		if time.Since(nodePool.syncStarted) > migCreationWaitTimeout {
			klog.Warningf("Node pool %s exceeded cluster registration timeout %s", nodePoolName, migCreationWaitTimeout)
			result := nodePoolSyncResult{err: fmt.Errorf("node pool %s exceeded cluster registration timeout %s", nodePoolName, migCreationWaitTimeout)}
			m.sendSyncResultNoLock(nodePoolName, result)
		}
	}
}

func (m *asyncNodePoolRegistrationObserver) sendSyncResult(nodePoolName string, result nodePoolSyncResult) {
	m.nodePoolsMutex.Lock()
	defer m.nodePoolsMutex.Unlock()
	m.sendSyncResultNoLock(nodePoolName, result)
}

func (m *asyncNodePoolRegistrationObserver) sendSyncResultNoLock(nodePoolName string, result nodePoolSyncResult) {
	nodePool := m.nodePools[nodePoolName]
	if nodePool != nil {
		delete(m.nodePools, nodePoolName)
		nodePool.resultChan <- result
	}
}

func (m *asyncNodePoolRegistrationObserver) syncWithCluster(cluster *gkeclient.Cluster) {
	m.nodePoolsMutex.Lock()
	defer m.nodePoolsMutex.Unlock()
	for _, nodePool := range cluster.NodePools {
		syncing := m.nodePools[nodePool.Name]
		if syncing == nil {
			continue
		}
		creationResult, err := syncing.buildMigCreationResult(nodePool, m.domainURL)
		if err != nil {
			klog.Warningf("Node pool %s registered in the cluster but migs are not ready: %v. Skipping...", nodePool.Name, err)
			continue
		}
		result := nodePoolSyncResult{creationResult: creationResult}
		m.sendSyncResultNoLock(nodePool.Name, result)
	}
}

func (m *asyncNodePoolRegistrationObserver) close() {
	m.nodePoolsMutex.Lock()
	defer m.nodePoolsMutex.Unlock()
	result := nodePoolSyncResult{err: fmt.Errorf("closed by context")}
	for name, nodePool := range m.nodePools {
		delete(m.nodePools, name)
		nodePool.resultChan <- result
	}
}

type syncingNodePool struct {
	gkeManager  GkeManager
	mainMig     *GkeMig
	syncStarted time.Time
	resultChan  chan<- nodePoolSyncResult
}

func (m *syncingNodePool) buildMigCreationResult(nodePool gkeclient.NodePool, domainUrl string) (*MigCreateNodePoolResult, error) {
	migs, err := nodePoolMIGs(m.gkeManager, domainUrl, nodePool)
	if err != nil {
		return nil, fmt.Errorf("could not build migs: %v", err)
	}
	AddMigsToNodePool(nodePool.Name, migs...)
	result := MigCreateNodePoolResult{}
	mainZone := m.mainMig.gceRef.Zone
	for _, mig := range migs {
		if m.mainMig.NodePoolName() == mig.NodePoolName() {
			// Compact Placement node pools are always in a single zone, but the zone is picked at random,
			// so it may be different than the zone originally assigned to the mig.
			if mainZone == mig.gceRef.Zone || mig.spec.PlacementGroup.UsesPlacement() {
				result.MainCreatedMig = mig
			} else {
				result.ExtraCreatedMigs = append(result.ExtraCreatedMigs, mig)
			}
		}
	}
	if result.MainCreatedMig == nil {
		return nil, fmt.Errorf("no mig registered in zone %s", mainZone)
	}
	return &result, nil
}
