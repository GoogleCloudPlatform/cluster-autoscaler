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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	client_testing "k8s.io/client-go/testing"

	"k8s.io/autoscaler/cluster-autoscaler/config"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

type apiResourceDef struct {
	schema    schema.GroupVersionResource
	available bool
}

type clusterRefreshResult struct {
	cluster     gkeclient.Cluster
	err         error
	experiments map[string]bool
}

type fakeCloudProvider struct {
	optsTracker *optstracking.OptionsTracker

	clusterRefreshResults []clusterRefreshResult
	clusterRefreshIndex   int

	clusterRefreshCalls int // Separate from the index so that we can assert the number of calls easily.
}

func (p *fakeCloudProvider) Refresh() error {
	p.clusterRefreshCalls++

	refreshResult := p.clusterRefreshResults[p.clusterRefreshIndex]
	if p.clusterRefreshIndex < len(p.clusterRefreshResults)-1 {
		p.clusterRefreshIndex++
	}

	if refreshResult.err != nil {
		return refreshResult.err
	}

	// Experiments manager refresh normally happens in a completely separate goroutine, not as part of CloudProvider.Refresh(). It's very convenient
	// to mock the refresh here for test purposes though - no concurrency needed in the test.
	optstracking.ChangeTestExperimentsManager(p.optsTracker, experiments.NewMockManagerWithOptions(version.Version{}, refreshResult.experiments, nil))

	p.optsTracker.RecomputeOptions(refreshResult.cluster)
	return nil
}

func TestWaitForInformerSyncWithClusterRefresh(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		testName              string
		flagOpts              internalopts.AutoscalingOptions
		apiResource           apiResourceDef
		clusterRefreshResults []clusterRefreshResult
		wantRefreshCalls      int // Negative value means "don't assert the number of refresh calls"
		wantRestart           bool
		wantErr               error
	}{
		{
			testName: "restart_not_needed_if_api_available", // Happy path.
			flagOpts: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{DynamicResourceAllocationEnabled: true}},
			apiResource: apiResourceDef{
				schema:    schema.GroupVersionResource{Group: "resource.k8s.io", Version: "v1", Resource: "resourceclaims"},
				available: true,
			},
			clusterRefreshResults: []clusterRefreshResult{
				{cluster: gkeclient.Cluster{EmulatedClusterVersion: ""}},
			},
			// With the prod interval value, there shouldn't be any Cluster refreshes needed - the first informerFactory.WaitForCacheSync() call should just
			// exit before the interval elapses and short-circuit the logic. But in the tests the interval is set to 100ms which isn't always enough time
			// for the informers to sync - so a second informerFactory.WaitForCacheSync() is sometimes needed. This is fine, what matters to assert is that
			// the function returns with wantRestart=false and no error.
			wantRefreshCalls: -1, // Don't assert the number of calls, see above
			wantRestart:      false,
			wantErr:          nil,
		},
		{
			testName: "restart_after_cluster_proto_change",
			flagOpts: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{DynamicResourceAllocationEnabled: true}},
			apiResource: apiResourceDef{
				schema:    schema.GroupVersionResource{Group: "resource.k8s.io", Version: "v1", Resource: "resourceclaims"},
				available: false,
			},
			clusterRefreshResults: []clusterRefreshResult{
				{cluster: gkeclient.Cluster{EmulatedClusterVersion: ""}},
				{cluster: gkeclient.Cluster{EmulatedClusterVersion: "1.33"}},
			},
			wantRefreshCalls: 2,
			wantRestart:      true,
			wantErr:          nil,
		},
		{
			testName: "restart_after_experiment_change",
			flagOpts: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{DynamicResourceAllocationEnabled: true}},
			apiResource: apiResourceDef{
				schema:    schema.GroupVersionResource{Group: "resource.k8s.io", Version: "v1", Resource: "resourceclaims"},
				available: false,
			},
			clusterRefreshResults: []clusterRefreshResult{
				{cluster: gkeclient.Cluster{EmulatedClusterVersion: ""}},
				{cluster: gkeclient.Cluster{EmulatedClusterVersion: ""}, experiments: map[string]bool{"DRA::Enabled": false}},
			},
			wantRefreshCalls: 2,
			wantRestart:      true,
			wantErr:          nil,
		},
		{
			testName: "refresh_error_is_bubbled_up",
			flagOpts: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{DynamicResourceAllocationEnabled: true}},
			apiResource: apiResourceDef{
				schema:    schema.GroupVersionResource{Group: "resource.k8s.io", Version: "v1", Resource: "resourceclaims"},
				available: false,
			},
			clusterRefreshResults: []clusterRefreshResult{
				{cluster: gkeclient.Cluster{EmulatedClusterVersion: ""}},
				{cluster: gkeclient.Cluster{EmulatedClusterVersion: ""}, err: fmt.Errorf("simulated refresh error")},
			},
			wantRefreshCalls: 2,
			wantRestart:      false,
			wantErr:          cmpopts.AnyError,
		},
		{
			testName: "timeout_results_in_error",
			flagOpts: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{DynamicResourceAllocationEnabled: true}},
			apiResource: apiResourceDef{
				schema:    schema.GroupVersionResource{Group: "resource.k8s.io", Version: "v1", Resource: "resourceclaims"},
				available: false,
			},
			clusterRefreshResults: []clusterRefreshResult{
				{cluster: gkeclient.Cluster{EmulatedClusterVersion: ""}},
			},
			wantRefreshCalls: -1, // Don't assert the number of calls, doesn't matter here
			wantRestart:      false,
			wantErr:          cmpopts.AnyError,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			t.Parallel()

			optsTracker := optstracking.FakeOptionsTracker(tc.flagOpts, tc.clusterRefreshResults[0].cluster, experiments.NewMockManagerWithOptions(version.Version{}, tc.clusterRefreshResults[0].experiments, nil))
			cloudProvider := &fakeCloudProvider{
				optsTracker:           optsTracker,
				clusterRefreshResults: tc.clusterRefreshResults,
				clusterRefreshIndex:   0,
			}
			informerFactory := informerFactoryUsingResource(t, tc.apiResource)

			gotRestart, gotErr := waitForInformerSyncWithClusterRefresh(informerFactory, cloudProvider, optsTracker, 100*time.Millisecond, 300*time.Millisecond)
			if diff := cmp.Diff(tc.wantErr, gotErr, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("waitForInformerSyncWithClusterRefresh() unexpected error, diff (-want +got): %v", diff)
			}
			if tc.wantRestart != gotRestart {
				t.Fatalf("waitForInformerSyncWithClusterRefresh() wantCaRestart return value differs: want %v, got %v", tc.wantRestart, gotRestart)
			}
			if tc.wantRefreshCalls >= 0 {
				gotRefreshCalls := cloudProvider.clusterRefreshCalls
				if tc.wantRefreshCalls != gotRefreshCalls {
					t.Fatalf("waitForInformerSyncWithClusterRefresh() called CloudProvider.Refresh() an unexpected number of times: want %d, got %d", tc.wantRefreshCalls, gotRefreshCalls)
				}
			}
		})
	}
}

func informerFactoryUsingResource(t *testing.T, apiResource apiResourceDef) informers.SharedInformerFactory {
	clientSetFake := fake.NewClientset()
	if !apiResource.available {
		// Prepare a fake K8s client for which the provided resourceName can't be LISTed. This simulates the API being unavailable.
		// This has to be a Prepend, Appending doesn't work because the default Reactor handles the List and this one is never called.
		clientSetFake.PrependReactor("list", apiResource.schema.Resource, func(action client_testing.Action) (handled bool, ret runtime.Object, err error) {
			return true, nil, fmt.Errorf("%s API unavailable", apiResource.schema.Resource)
		})
	}
	informerFactory := informers.NewSharedInformerFactory(clientSetFake, 0)

	// Create an informer for the resource, this triggers tracking the informer in the factory. Without this step informerFactory.WaitForCacheSync() returns
	// immediately, because there are no informers to wait on.
	_, err := informerFactory.ForResource(apiResource.schema)
	if err != nil {
		t.Fatalf("informerFactor.ForResource() unexpected error: %v", err)
	}

	// Start the single tracked informer.
	ctx, cancelCtx := context.WithCancel(context.Background())
	informerFactory.Start(ctx.Done()) // We intentionally don't call informerFactory.WaitForCacheSync() afterwards because we're testing logic that calls it.
	// Cleanups are executed in LIFO order, and we need to cancel the context before calling informerFactory.Shutdown.
	t.Cleanup(informerFactory.Shutdown)
	t.Cleanup(cancelCtx)

	return informerFactory
}
