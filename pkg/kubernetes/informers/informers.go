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

package informers

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/klog/v2"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
)

const (
	// InformerSyncWaitTimeout is the maximum period of time Cluster Autoscaler waits for informers to sync.
	InformerSyncWaitTimeout = 15 * time.Minute
)

// cloudProviderRefresher is a subset of the CloudProvider interface needed for refreshing the Cluster state. It's used as function param instead of the full
// CloudProvider, so that testing is easy.
type cloudProviderRefresher interface {
	Refresh() error
}

// WaitForInformerSyncWithClusterRefresh is a wrapper around SharedInformerFactory.WaitForCacheSync(), which periodically refreshes the GKE Cluster state
// while waiting for the informers to sync. If a Cluster state refresh results in Cluster Autoscaler option changes that require CA restart, CA is restarted.
// This is necessary to avoid the following race condition:
//  1. CA initializes, gets to the first Cluster refresh which happens as part of gke.BuildGKE() (e.g. cluster is being upgraded 1.33->1.34 with emulatedVersion=1.33,
//     1.34 CA initializes on the new master).
//  2. OptionsTracker computes the value for an AutoscalingOptions field based on the initial Cluster state (e.g. CurrentEmulatedVersion is still unset in Cluster proto at the very
//     beginning of a 1.33->1.34 upgrade when 1.34 CA first starts, so AutoscalingOptions.EnableDynamicResourceAllocation evaluates to true).
//  3. Further CA init logic initializes additional informers based on the initially computed value of the field (e.g. DRA informers are initialized because
//     AutoscalingOptions.EnableDynamicResourceAllocation evaluated to true).
//  4. The part of Cluster proto that the AutoscalingOptions field depends on changes in a way that should change the field value (e.g. currentEmulatedVersion is finally set to "1.33" as
//     part of a 1.33->1.34 upgrade, so now AutoscalingOptions.EnableDynamicResourceAllocation should be false). The Cluster state change also disables the corresponding K8s API
//     (e.g. DRA API should be enabled by default in 1.34, but if currentEmulatedVersion is set to "1.33", the API is disabled because kube-apiserver is emulating 1.33 behavior).
//  5. CA init logic gets to the informer sync. The set of informers that CA is waiting for is based on the old field value, but the additional informers never sync because their API
//     is now disabled. CA gets stuck waiting for the sync forever (e.g. CA waits for the DRA informers to sync based on the initial Cluster state with currentEmulatedVersion unset,
//     gets stuck forever because DRA API is actually disabled because of currentEmulatedVersion now being "1.33").
func WaitForInformerSyncWithClusterRefresh(informerFactory informers.SharedInformerFactory, cloudProvider cloudProviderRefresher, optsTracker *optstracking.OptionsTracker) error {
	// Set the refresh interval slightly higher than the GkeManager internal refresh interval, so that every refresh actually refreshes the Cluster state.
	refreshInterval := gke.ClusterRefreshInterval + 10*time.Second
	caNeedsRestart, err := waitForInformerSyncWithClusterRefresh(informerFactory, cloudProvider, optsTracker, refreshInterval, InformerSyncWaitTimeout)
	if err != nil {
		return err
	}
	if caNeedsRestart {
		// TODO(b/409515258): We could just return here, but the cleanup takes ~15 min, exiting with an error is faster.
		klog.Fatalf("Cluster Autoscaler configuration changed, restarting to pick up a new configuration.")
	}
	return nil
}

// waitForInformerSyncWithClusterRefresh is the same as the uppercase version, but exposing more parameters and not actually performing the CA restart for testability.
func waitForInformerSyncWithClusterRefresh(informerFactory informers.SharedInformerFactory, cloudProvider cloudProviderRefresher, optsTracker *optstracking.OptionsTracker, refreshInterval, timeout time.Duration) (caNeedsRestart bool, err error) {
	deadline := time.Now().Add(timeout)
	klog.Infof("Initializing resource informers, blocking until caches are synced or until %v", deadline)

	for time.Now().Before(deadline) {
		// Wait until all informers are synced, or until slightly more than gke.ClusterRefreshInterval time passes.
		if waitForInformerSyncWithTimeout(informerFactory, refreshInterval) {
			// All informers are synced, move on.
			klog.Info("Informer caches synced.")
			return false, nil
		}

		klog.Info("Informer caches not synced yet, refreshing CloudProvider state.")
		// Refresh the GKE Cluster state. AutoscalingOptions fields tracked by OptionsTracker that depend on the Cluster proto are recomputed as part of the refresh.
		// Note that a cloudProvider.Refresh() call only refreshes the Cluster state if more than gke.ClusterRefreshInterval time has passed since the last refresh.
		// We're intentionally using a slightly higher value for the timeout, so this call should always refresh the Cluster state.
		if err := cloudProvider.Refresh(); err != nil {
			return false, err
		}

		// Check if the Cluster state refresh above resulted in changing the value of an AutoscalingOptions field tracked by OptionsTracker. If so, Cluster Autoscaler has
		// to be restarted to re-initialize using the new value of the field.
		// After the loop is started, we normally wait 5min from the first loop before restarting CA due to option changes. It doesn't seem like this would be beneficial here,
		// as CA is not doing any useful work anyway, it's just waiting.
		if optsTracker.OptionChangesRequireRestart() {
			return true, nil
		}
	}

	return false, fmt.Errorf("timed out after waiting for informer sync for %v", timeout)
}

// waitForInformerSyncWithTimeout is a wrapper around SharedInformerFactory.WaitForCacheSync() which only waits until the provided timeout.
// Returns true if all informers are already synced, false if not all informers are synced before the timeout elapses.
func waitForInformerSyncWithTimeout(informerFactory informers.SharedInformerFactory, timeout time.Duration) bool {
	ctx, cancelCtx := context.WithTimeout(context.Background(), timeout)
	defer cancelCtx()

	informersSynced := informerFactory.WaitForCacheSync(ctx.Done())
	for _, synced := range informersSynced {
		if !synced {
			return false
		}
	}

	return true
}
