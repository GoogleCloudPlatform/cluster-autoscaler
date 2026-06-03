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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	caerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/kubernetes/pkg/util/slice"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/interfaces"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	gkeclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/manager"
	prpods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
)

// Test names for async_gke_manager conflict with tests for gke_manager
// To deduplicate the names use prefix: TestAsyncMgr_

// testAsyncGkeMgrDefaultTimeout default timeout for waiting on async operations
const testAsyncGkeMgrDefaultTimeout = 10 * time.Second

func TestAsyncMgr_AsyncNodePoolCreationWithScaleUpInMemAccounting(t *testing.T) {
	ctx := newAsyncGkeManagerTestContext(t)
	mig, nodePool := ctx.sampleMigWithNodePool()
	// when a node pool is scheduled to be created
	result, initializer := ctx.createNodePoolAsyncOrFail(mig)
	// and upcoming migs are scaled up during async creation
	upcomingMigs := result.AllCreatedMigs()
	assert.Equal(t, 3, len(upcomingMigs))
	assert.NoError(t, ctx.asyncGkeManager.SetMigSize(upcomingMigs[0], 10))
	assert.NoError(t, ctx.asyncGkeManager.SetMigSize(upcomingMigs[1], 5))
	assert.NoError(t, ctx.asyncGkeManager.SetMigSize(upcomingMigs[2], 2))
	ctx.gkeManager.finalizeNodePoolCreation(nodePool)
	// then async result contains information about asynchronously scaled up node groups
	asyncResult := initializer.AwaitSuccessfulResultOrFail(mig, nodePool)
	createdMigs := asyncResult.CreationResult.AllCreatedNodeGroups()
	assert.Equal(t, 3, len(createdMigs))
	assert.Equal(t, map[string]string{
		(createdMigs[0].Id()): upcomingMigs[0].Id(),
		(createdMigs[1].Id()): upcomingMigs[1].Id(),
		(createdMigs[2].Id()): upcomingMigs[2].Id(),
	}, asyncResult.CreatedToUpcomingMapping)
	// and initializer received scale-ups for upcoming node groups
	assert.Equal(t, map[string]int64{
		(upcomingMigs[0].Id()): 10,
		(upcomingMigs[1].Id()): 5,
		(upcomingMigs[2].Id()): 2,
	}, initializer.TargetSizes())
	ctx.assertNoOngoingNodePoolOps()
	// and migs are registered in cache
	ctx.assertMigRegisteredInCache(createdMigs[0])
	ctx.assertMigRegisteredInCache(createdMigs[1])
	ctx.assertMigRegisteredInCache(createdMigs[2])
	// and upcoming migs should be registered as injected
	for i := range createdMigs {
		createdMig := createdMigs[i].(*GkeMig)
		injectedMig := ctx.gkeManager.GetInjectedMig(createdMig)
		assert.Equal(t, mig.Id(), injectedMig.Id(), "Expected injected mig %v registered for created mig %v", mig.Id(), createdMig.Id())
	}
}

func TestAsyncMgr_AsyncNodePoolCreationPreservesMainMig(t *testing.T) {
	ctx := newAsyncGkeManagerTestContext(t)
	_, sampleNodePool := ctx.sampleMigWithNodePool()
	zones := sampleNodePool.Spec.Locations
	assert.Equal(t, 3, len(zones))
	for _, zone := range zones {
		mig, _ := ctx.sampleMigWithNodePool()
		mig.gceRef = gce.GceRef{Project: mig.gceRef.Project, Zone: zone, Name: mig.gceRef.Name}
		result, _ := ctx.createNodePoolAsyncOrFail(mig)
		assert.Equal(t, zone, result.MainCreatedMig.GceRef().Zone)
		assert.Equal(t, 2, len(result.ExtraCreatedMigs))
		assert.NotEqual(t, zone, result.ExtraCreatedMigs[0].GceRef().Zone)
		assert.NotEqual(t, zone, result.ExtraCreatedMigs[1].GceRef().Zone)
	}
}

func TestAsyncMgr_AsyncNodePoolCreationSetsNodePoolSpecInUpcomingMigs(t *testing.T) {
	ctx := newAsyncGkeManagerTestContext(t)
	mig, pool := ctx.sampleMigWithNodePool()
	result, _ := ctx.createNodePoolAsyncOrFail(mig)
	spec := result.MainCreatedMig.Spec()
	assert.Equal(t, pool.Name, spec.Labels[labels.GkeNodePoolLabel])
	assert.NotEmpty(t, spec.ClusterNetworkPath)
	assert.NotEmpty(t, spec.ClusterSubnetworkPath)
}

func TestAsyncMgr_AsyncNodePoolCreationWithTwoNodePools(t *testing.T) {
	ctx := newAsyncGkeManagerTestContext(t)
	mig1, nodePool1 := ctx.sampleMigWithNodePool()
	mig2, nodePool2 := ctx.sampleMigWithNodePool()
	// when two node pools are scheduled to be created
	result1, initializer1 := ctx.createNodePoolAsyncOrFail(mig1)
	result2, initializer2 := ctx.createNodePoolAsyncOrFail(mig2)
	// and upcoming migs are scaled up during async node pool creations
	assert.NoError(t, ctx.asyncGkeManager.SetMigSize(result1.MainCreatedMig, 10))
	assert.NoError(t, ctx.asyncGkeManager.SetMigSize(result2.MainCreatedMig, 7))
	ctx.gkeManager.finalizeNodePoolCreation(nodePool1)
	ctx.gkeManager.finalizeNodePoolCreation(nodePool2)
	// then async result should contains information about scaled up node pools
	assert.Equal(t, map[string]int64{
		(result1.MainCreatedMig.Id()): 10,
	}, initializer1.TargetSizes())
	assert.Equal(t, map[string]int64{
		(result2.MainCreatedMig.Id()): 7,
	}, initializer2.TargetSizes())
	ctx.assertNoOngoingNodePoolOps()
}

func TestAsyncMgr_AsyncNodePoolCreationWithDelayedNodePoolListing(t *testing.T) {
	// Warning: this test is slow (1s) - GKE node pool is checked periodically
	t.Parallel()
	ctx := newAsyncGkeManagerTestContext(t)
	mig, nodePool := ctx.sampleMigWithNodePool()
	// when cluster reports new node pool with some delay
	ctx.gkeManager.reportOnceEmptyCluster()
	// and node pool is created
	_, initializer := ctx.createNodePoolAsyncOrFail(mig)
	ctx.gkeManager.finalizeNodePoolCreation(nodePool)
	// then async node pool creation should still succeed
	initializer.AwaitSuccessfulResultOrFail(mig, nodePool)
	ctx.assertNoOngoingNodePoolOps()
}

func TestAsyncMgr_AsyncNodePoolCreationConsistencyBetweenScaleUpIterations(t *testing.T) {
	ctx := newAsyncGkeManagerTestContext(t)
	mig, nodePool := ctx.sampleMigWithNodePool()
	asyncGkeManager := ctx.asyncGkeManager

	// when node pool is scheduled to be created
	result, initializer := ctx.createNodePoolAsyncOrFail(mig)
	// then manager should return upcoming migs
	for _, upcoming := range result.AllCreatedMigs() {
		assert.True(t, asyncGkeManager.IsUpcoming(upcoming))
	}
	// and the same upcoming migs should be available for scale-up iteration
	assert.Equal(t, asyncGkeManager.GetGkeMigs(), result.AllCreatedMigs())

	// when node pool creation finishes
	ctx.gkeManager.finalizeNodePoolCreation(nodePool)
	// then async result should contain created node-groups still categorized as upcoming
	asyncResult := initializer.AwaitSuccessfulResultOrFail(mig, nodePool)
	for _, created := range asyncResult.CreationResult.AllCreatedNodeGroups() {
		assert.True(t, asyncGkeManager.IsUpcoming(created.(*GkeMig)))
	}
	// and scale-up logic should still see the upcoming node-groups
	assert.Equal(t, asyncGkeManager.GetGkeMigs(), result.AllCreatedMigs())

	// when manager is refreshed at the beginning on a new scale-up iteration
	ctx.awaitAsyncTasksAndRefresh()
	// then created migs are visible to the scale-up loop and not categorized as upcoming
	for _, mig := range asyncGkeManager.GetGkeMigs() {
		assert.False(t, asyncGkeManager.IsUpcoming(mig))
	}
}

func TestAsyncMgr_CreateNodePoolAsyncWithAsyncFailures(t *testing.T) {
	tests := []struct {
		name                 string
		setup                func(ctx *asyncGkeManagerTestContext) (*GkeMig, *gkeclient.NodePool)
		wantError            string
		wantNodePoolDeletion bool
	}{
		{
			name: "node pool creation error",
			setup: func(ctx *asyncGkeManagerTestContext) (*GkeMig, *gkeclient.NodePool) {
				mig, nodePool := ctx.sampleMigWithNodePool()
				gkeError := fmt.Errorf("sample error")
				ctx.gkeManager.failNodePoolCreation(mig.NodePoolName(), gkeError)
				return mig, nodePool
			},
			wantError:            "sample error",
			wantNodePoolDeletion: false,
		},
		{
			name: "node pool with error status",
			setup: func(ctx *asyncGkeManagerTestContext) (*GkeMig, *gkeclient.NodePool) {
				mig, nodePool := ctx.sampleMigWithNodePool()
				nodePool.Status = NodePoolErrorStatus
				return mig, nodePool
			},
			wantError:            "node pool has error status",
			wantNodePoolDeletion: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := newAsyncGkeManagerTestContext(t)
			mig, nodePool := test.setup(ctx)
			// when node pool is scheduled for creation
			_, initializer := ctx.createNodePoolAsyncOrFail(mig)
			ctx.gkeManager.finalizeNodePoolCreation(nodePool)
			ctx.gkeManager.finalizeNodePoolDeletion(nodePool.Name)
			// then initializer is executed with proper error
			asyncResult := initializer.AwaitResultOrFail()
			assert.ErrorContains(t, asyncResult.Error, test.wantError)
			// and node pool is cleaned up
			ctx.awaitAsyncTasks()
			assert.Equalf(t, test.wantNodePoolDeletion, ctx.gkeManager.attemptedDeletion(nodePool.Name), "Expected node pool deletion: %v", test.wantNodePoolDeletion)
			ctx.assertNoOngoingNodePoolOps()
		})
	}
}

func TestAsyncMgr_CreateNodePoolAsyncRetries(t *testing.T) {
	tests := []struct {
		name                   string
		err                    error
		errNumberOfRepetitions int
		followedBySuccess      bool
		maxConcurrentRetries   int
		wantErr                bool
		wantCleanUp            bool
	}{
		{
			name:                   "fail: on normal error",
			err:                    fmt.Errorf("err"),
			errNumberOfRepetitions: 1,
			followedBySuccess:      false,
			wantErr:                true,
		},
		{
			name:                   "success: retry after concurrent error with concurrentRetries",
			err:                    caerrors.NewAutoscalerError(gkeclient.GkeTooManyRequestsError, "too many requests error"),
			errNumberOfRepetitions: 1,
			followedBySuccess:      true,
			maxConcurrentRetries:   1,
			wantErr:                false,
		},
		{
			name:                   "fail: retry after concurrent error",
			err:                    caerrors.NewAutoscalerError(gkeclient.GkeTooManyRequestsError, "too many requests error"),
			errNumberOfRepetitions: 1,
			followedBySuccess:      false,
			maxConcurrentRetries:   0,
			wantErr:                true,
		},
		{
			name:                   "fail: retry after persistent error",
			err:                    caerrors.NewAutoscalerError(gkeclient.GkePersistentOperationError, "presistent error"),
			errNumberOfRepetitions: 1,
			followedBySuccess:      false,
			maxConcurrentRetries:   1,
			wantErr:                true,
			wantCleanUp:            true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := newAsyncGkeManagerTestContext(t)
			ctx.asyncGkeManager.maxConcurrentErrorRetries = test.maxConcurrentRetries
			ctx.asyncGkeManager.initialBackoffDuration = 0 * time.Millisecond
			mig, nodePool := ctx.sampleMigWithNodePool()
			// when GKE create operations fail
			ctx.gkeManager.failNodePoolCreationWithRepeatingError(mig.NodePoolName(), test.err, test.errNumberOfRepetitions, test.followedBySuccess)
			// and node pool is scheduled for creation
			_, initializer := ctx.createNodePoolAsyncOrFail(mig)
			ctx.gkeManager.finalizeNodePoolCreation(nodePool)
			// then initializer is executed with the GKE error
			asyncResult := initializer.AwaitResultOrFail()
			if test.wantErr {
				assert.Error(t, asyncResult.Error)
			} else {
				assert.NoError(t, asyncResult.Error)
			}
			// finalizing deletion for potential clean up
			ctx.gkeManager.finalizeNodePoolDeletion(nodePool.Name)
			ctx.awaitAsyncTasks()
			assert.Equal(t, test.wantCleanUp, ctx.gkeManager.attemptedDeletion(nodePool.Name))
			ctx.assertNoOngoingNodePoolOps()
		})
	}
}

func TestAsyncMgr_ScaleUpOfProcessedUpcomingNodePoolShouldScaleUpCreatedNodeGroup(t *testing.T) {
	ctx := newAsyncGkeManagerTestContext(t)
	mig, nodePool := ctx.sampleMigWithNodePool()
	asyncGkeManager := ctx.asyncGkeManager
	// given already initialized async created node pool
	result, initializer := ctx.createNodePoolAsyncOrFail(mig)
	ctx.gkeManager.finalizeNodePoolCreation(nodePool)
	asyncResult := initializer.AwaitSuccessfulResultOrFail(mig, nodePool)
	ctx.awaitAsyncTasks()
	// when upcoming node group are scaled up
	upcomingMig := result.MainCreatedMig
	createdMig := asyncResult.CreationResult.MainCreatedNodeGroup
	assert.NoError(t, asyncGkeManager.SetMigSize(upcomingMig, 10))
	// then the underlying created node groups should scaled up as well
	assert.Equal(t, 10, ctx.gkeManager.GetMigInstances(createdMig.Id()))
	// and the same should happen for instance creation
	assert.NoError(t, asyncGkeManager.CreateInstances(upcomingMig, 1))
	assert.Equal(t, 11, ctx.gkeManager.GetMigInstances(createdMig.Id()))
	ctx.assertNoOngoingNodePoolOps()
}

func TestAsyncMgr_GkeOperationScheduling(t *testing.T) {
	ctx := newAsyncGkeManagerTestContext(t)
	workers := ctx.taskQueueWorkerCount
	assert.Equal(t, 0, workers%2) // for easier test cases
	queueSize := ctx.taskQueueSize
	capacity := workers + queueSize
	tests := []struct {
		name      string
		creations int
		deletions int
		wantErr   bool
	}{
		{
			name:      "success:creations=queue_size",
			creations: queueSize, // cannot use queue capacity as workers consume tasks asynchronously and there are race conditions
		},
		{
			name:      "success:deletions=queue_size",
			deletions: queueSize,
		},
		{
			name:      "success:creations+deletions=queue_size",
			creations: queueSize / 2,
			deletions: queueSize / 2,
		},
		{
			name:      "fail:creations>queue_capacity",
			creations: capacity + 1,
			wantErr:   true,
		},
		{
			name:      "fail:deletions>queue_capacity",
			deletions: capacity + 1,
			wantErr:   true,
		},
		{
			name:      "fail:creations+deletions>queue_capacity",
			creations: capacity/2 + 1,
			deletions: capacity / 2,
			wantErr:   true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := newAsyncGkeManagerTestContext(t)
			var err error = nil
			// when GKE operations pile up
			for range test.creations {
				if err == nil {
					mig, _ := ctx.sampleMigWithNodePool()
					initializer := newAsyncNodeGroupInitializer(t)
					_, err = ctx.asyncGkeManager.CreateNodePoolAsync(mig, initializer, initializer)
				}
			}
			for range test.deletions {
				if err == nil {
					mig, _ := ctx.sampleCreatedMigWithNodePool()
					err = ctx.asyncGkeManager.DeleteNodePoolAsync(mig, newAsyncNodeGroupFinalizer(t))
				}
			}
			// then scheduling should fail when task queue capacity is reached
			if test.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "queue is full")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func waitOrFail(t *testing.T, groupName string, wg *sync.WaitGroup) {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
	case <-time.After(testAsyncGkeMgrDefaultTimeout):
		assert.FailNow(t, "timeout: waitGroup timeout reached", "timeout %v reached when waiting for %s", testAsyncGkeMgrDefaultTimeout, groupName)
	}
}

func TestAsyncMgr_MigSizeDuringAsyncNodePoolCreation(t *testing.T) {
	ctx := newAsyncGkeManagerTestContext(t)
	mig, nodePool := ctx.sampleMigWithNodePool()
	asyncGkeManager := ctx.asyncGkeManager

	// when node pool is scheduled to be created
	result, initializer := ctx.createNodePoolAsyncOrFail(mig)
	// then upcoming node-groups should be empty
	for _, mig := range result.AllCreatedMigs() {
		size, err := asyncGkeManager.GetMigSize(mig)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), size)
	}

	// when upcoming mig is scale-up during async creation
	mainMigSize := int64(10)
	assert.NoError(t, asyncGkeManager.SetMigSize(result.MainCreatedMig, int64(mainMigSize)))
	// then its target size is updated
	size, err := asyncGkeManager.GetMigSize(result.MainCreatedMig)
	assert.NoError(t, err)
	assert.Equal(t, mainMigSize, size)

	// when creation is finalized
	ctx.gkeManager.finalizeNodePoolCreation(nodePool)
	asyncResult := initializer.AwaitSuccessfulResultOrFail(mig, nodePool)
	// then upcoming mig size should still include recent scale-up
	size, err = asyncGkeManager.GetMigSize(result.MainCreatedMig)
	assert.NoError(t, err)
	assert.Equal(t, mainMigSize, size)

	// when manager is refreshed
	ctx.awaitAsyncTasksAndRefresh()
	ctx.gkeManager.mock.On("GetMigSize", result.MainCreatedMig).Return(int64(0), fmt.Errorf("mig not found"))
	ctx.gkeManager.mock.On("GetMigSize", asyncResult.CreationResult.MainCreatedNodeGroup.(*GkeMig)).Return(mainMigSize, nil)
	// then there are no reported sizes for upcoming migs (there are no upcoming migs)
	_, err = asyncGkeManager.GetMigSize(result.MainCreatedMig)
	assert.ErrorContains(t, err, "mig not found")
	// and created migs contain recent scale-ups
	size, err = asyncGkeManager.GetMigSize(asyncResult.CreationResult.MainCreatedNodeGroup.(*GkeMig))
	assert.NoError(t, err)
	assert.Equal(t, mainMigSize, size)
}

func TestAsyncMgr_GetAllNodePoolNames(t *testing.T) {
	ctx := newAsyncGkeManagerTestContext(t)
	asyncGkeManager := ctx.asyncGkeManager
	asyncMig, asyncNodePool := ctx.sampleMigWithNodePool()

	// when there are 2 already created node pools
	createdMig1, createdNodePool1 := ctx.sampleCreatedMigWithNodePool()
	_, createdNodePool2 := ctx.sampleCreatedMigWithNodePool()
	// and there is one node pool under async creation
	ctx.createNodePoolAsyncOrFail(asyncMig)
	// then all 3 node pool names are reported
	names := asyncGkeManager.GetAllNodePoolNames()
	assert.Equal(t, sets.New(createdNodePool1.Name, createdNodePool2.Name, asyncNodePool.Name), names)

	// when already created node pool is asynchronously deleted
	ctx.deleteNodePoolAsyncOrFail(createdMig1)
	// then its name is not reported
	names = asyncGkeManager.GetAllNodePoolNames()
	assert.Equal(t, sets.New(createdNodePool2.Name, asyncNodePool.Name), names)
}

func TestAsyncMgr_GetMigNodesDuringAsyncNodePoolDeletion(t *testing.T) {
	ctx := newAsyncGkeManagerTestContext(t)
	asyncGkeManager := ctx.asyncGkeManager
	mig, _ := ctx.sampleCreatedMigWithNodePool()
	// when there is a node belonging to an already created node pool
	createdMigInstances := []gce.GceInstance{{NumericId: 123}}
	ctx.gkeManager.mock.On("GetMigNodes", mig).Return(createdMigInstances, nil)
	// then mig nodes are reported
	instances, err := asyncGkeManager.GetMigNodes(mig)
	assert.NoError(t, err)
	assert.Equal(t, createdMigInstances, instances)

	// when node pool is asynchronously deleted
	ctx.deleteNodePoolAsyncOrFail(mig)
	// then mig nodes are not reported
	instances, err = asyncGkeManager.GetMigNodes(mig)
	assert.NoError(t, err)
	assert.Empty(t, instances)
}

func TestAsyncMgr_GetMigNodesDuringAsyncNodePoolCreation(t *testing.T) {
	ctx := newAsyncGkeManagerTestContext(t)
	asyncGkeManager := ctx.asyncGkeManager
	mig, nodePool := ctx.sampleMigWithNodePool()
	// when node pool is async created
	result, initializer := ctx.createNodePoolAsyncOrFail(mig)
	// and it is scaled up
	assert.NoError(t, asyncGkeManager.SetMigSize(result.MainCreatedMig, 1))
	// then nodes are not reported for upcoming node pools (only target size is)
	instances, err := asyncGkeManager.GetMigNodes(result.MainCreatedMig)
	assert.NoError(t, err)
	assert.Empty(t, instances)

	// when node pool creation finishes and it has some nodes registered
	ctx.gkeManager.finalizeNodePoolCreation(nodePool)
	initializer.AwaitSuccessfulResultOrFail(mig, nodePool)
	nodes := []gce.GceInstance{{NumericId: 123}}
	ctx.gkeManager.mock.On("GetMigNodes", mig).Return(nodes, nil)
	// then mig nodes are reported
	reportedInstances, err := asyncGkeManager.GetMigNodes(mig)
	assert.NoError(t, err)
	assert.Equal(t, nodes, reportedInstances)
}

func TestAsyncMgr_ReturnsFilteredAsyncGkeMigs(t *testing.T) {
	ctx := newAsyncGkeManagerTestContext(t)
	asyncGkeManager := ctx.asyncGkeManager
	// there is uninitialized upcoming mig
	uninitializedMig, _ := ctx.sampleMigWithNodePool()
	result, _ := ctx.createNodePoolAsyncOrFail(uninitializedMig)
	upcomingMigs := result.AllCreatedMigs()
	// and one created mig
	createdMig, _ := ctx.sampleCreatedMigWithNodePool()
	ctx.gkeManager.mock.On("GetGkeMigsBlockedByServerError").Return(append(upcomingMigs, createdMig))
	ctx.gkeManager.mock.On("GetGkeMigsBlockedByNotFoundError").Return(append(upcomingMigs, createdMig))
	tests := []struct {
		name     string
		getMigs  func() []*GkeMig
		wantMigs []string
	}{
		{
			name: "GetGkeMigs()",
			getMigs: func() []*GkeMig {
				return asyncGkeManager.GetGkeMigs()
			},
			wantMigs: []string{createdMig.Id(), upcomingMigs[0].Id(), upcomingMigs[1].Id(), upcomingMigs[2].Id()},
		},
		{
			name: "GetGkeMigsBlockedByServerError()",
			getMigs: func() []*GkeMig {
				return asyncGkeManager.GetGkeMigsBlockedByServerError()
			},
			wantMigs: []string{createdMig.Id()},
		},
		{
			name: "GetGkeMigsBlockedByNotFoundError()",
			getMigs: func() []*GkeMig {
				return asyncGkeManager.GetGkeMigsBlockedByNotFoundError()
			},
			wantMigs: []string{createdMig.Id()},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			migs := test.getMigs()
			wantMigIds := slice.SortStrings(test.wantMigs)
			migIds := slice.SortStrings(nodeGroupIds(migs))
			assert.Equal(t, wantMigIds, migIds)
			for _, mig := range migs {
				assert.Equal(t, asyncGkeManager, mig.gkeManager)
			}
		})
	}
}

func TestAsyncMgr_DeleteNodePool(t *testing.T) {
	type cleanUpFunc func()
	noCleanup := func() {}
	tests := []struct {
		name             string
		migToRemove      func(ctx *asyncGkeManagerTestContext) (*GkeMig, *gkeclient.NodePool, cleanUpFunc)
		wantErr          string
		wantNoOngoingOps bool
	}{
		{
			name: "delete a node pool",
			migToRemove: func(ctx *asyncGkeManagerTestContext) (*GkeMig, *gkeclient.NodePool, cleanUpFunc) {
				mig, nodePool := ctx.sampleCreatedMigWithNodePool()
				return mig, nodePool, noCleanup
			},
			wantNoOngoingOps: true,
		},
		{
			name: "delete upcoming node pool queued for creation",
			migToRemove: func(ctx *asyncGkeManagerTestContext) (*GkeMig, *gkeclient.NodePool, cleanUpFunc) {
				var result MigCreateNodePoolResult
				var mig *GkeMig
				var nodePool *gkeclient.NodePool
				for range ctx.taskQueueWorkerCount + 1 {
					mig, nodePool = ctx.sampleMigWithNodePool()
					result, _ = ctx.createNodePoolAsyncOrFail(mig)
				}
				// last scheduled mig is queued
				return result.MainCreatedMig, nodePool, noCleanup
			},
			// there are scheduled node pools in test setup
			wantNoOngoingOps: false,
		},
		{
			name: "forbid deleting upcoming node pool being initialized",
			migToRemove: func(ctx *asyncGkeManagerTestContext) (*GkeMig, *gkeclient.NodePool, cleanUpFunc) {
				mig, nodePool := ctx.sampleMigWithNodePool()
				result, initializer := ctx.createNodePoolAsyncOrFail(mig)
				initializer.BlockInitialization()
				ctx.gkeManager.finalizeNodePoolCreation(nodePool)
				initializer.AwaitSuccessfulResultOrFail(mig, nodePool)
				return result.MainCreatedMig, nodePool, initializer.UnblockInitialization
			},
			wantErr:          "could not schedule node pool deletion: node pool is being created",
			wantNoOngoingOps: true,
		},
		{
			name: "forbid deleting upcoming node pool after initialization",
			migToRemove: func(ctx *asyncGkeManagerTestContext) (*GkeMig, *gkeclient.NodePool, cleanUpFunc) {
				mig, nodePool := ctx.sampleMigWithNodePool()
				result, initializer := ctx.createNodePoolAsyncOrFail(mig)
				ctx.gkeManager.finalizeNodePoolCreation(nodePool)
				initializer.AwaitSuccessfulResultOrFail(mig, nodePool)
				return result.MainCreatedMig, nodePool, noCleanup
			},
			wantErr:          "could not schedule node pool deletion: node pool is being created",
			wantNoOngoingOps: true,
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("Async/%s", test.name), func(t *testing.T) {
			ctx := newAsyncGkeManagerTestContext(t)
			asyncGkeManager := ctx.asyncGkeManager
			mig, nodePool, cleanUp := test.migToRemove(ctx)
			defer cleanUp()
			finalizer := newAsyncNodeGroupFinalizer(t)
			err := asyncGkeManager.DeleteNodePoolAsync(mig, finalizer)
			if len(test.wantErr) > 0 {
				assert.ErrorContains(t, err, test.wantErr)
				return
			}
			assert.NoError(t, err)
			ctx.gkeManager.finalizeNodePoolDeletion(nodePool.Name)
			finalizer.AwaitSuccessfulResultOrFail()
			if test.wantNoOngoingOps {
				ctx.assertNoOngoingNodePoolOps()
			}
		})
		t.Run(fmt.Sprintf("Sync/%s", test.name), func(t *testing.T) {
			ctx := newAsyncGkeManagerTestContext(t)
			asyncGkeManager := ctx.asyncGkeManager
			mig, _, cleanUp := test.migToRemove(ctx)
			defer cleanUp()
			ctx.gkeManager.mock.On("DeleteNodePool", mig).Return(nil)
			err := asyncGkeManager.DeleteNodePool(mig)
			if len(test.wantErr) > 0 {
				assert.ErrorContains(t, err, test.wantErr)
				return
			}
			assert.NoError(t, err)
			if test.wantNoOngoingOps {
				ctx.assertNoOngoingNodePoolOps()
			}
		})
	}
}

func TestAsyncMgr_DeleteNodePoolAsync(t *testing.T) {
	ctx := newAsyncGkeManagerTestContext(t)
	mig, _ := ctx.sampleCreatedMigWithNodePool()
	// when node pool is deleted
	ctx.awaitAsyncTasksAndRefresh()
	finalizer := ctx.deleteNodePoolAsyncOrFail(mig)
	// and GKE operation succeeds
	ctx.gkeManager.finalizeNodePoolDeletion(mig.NodePoolName())
	// then finalizer is executed without error
	asyncResult := finalizer.AwaitResultOrFail()
	assert.NoError(t, asyncResult.Error)
	ctx.assertNoOngoingNodePoolOps()
}

func TestAsyncMgr_DeleteNodePoolAsyncWithAsyncFailures(t *testing.T) {
	ctx := newAsyncGkeManagerTestContext(t)
	mig, _ := ctx.sampleCreatedMigWithNodePool()
	// when GKE delete operations fail
	gkeError := fmt.Errorf("sample error")

	ctx.gkeManager.failNodePoolDeletion(mig.NodePoolName(), gkeError)
	// and node pool is scheduled for deletion
	finalizer := ctx.deleteNodePoolAsyncOrFail(mig)
	ctx.gkeManager.finalizeNodePoolDeletion(mig.NodePoolName())
	// then finalizer is executed with the GKE error
	asyncResult := finalizer.AwaitResultOrFail()
	assert.ErrorContains(t, asyncResult.Error, "sample error")

	// when GKE operations succeed
	ctx.gkeManager.failNodePoolDeletion(mig.NodePoolName(), nil)
	// and node pool is deleted for the second time in next iteration
	ctx.awaitAsyncTasksAndRefresh()
	finalizer = ctx.deleteNodePoolAsyncOrFail(mig)
	// and GKE operation succeeds
	ctx.gkeManager.finalizeNodePoolDeletion(mig.NodePoolName())
	// then finalizer is executed without error
	asyncResult = finalizer.AwaitResultOrFail()
	assert.NoError(t, asyncResult.Error)
	ctx.assertNoOngoingNodePoolOps()
}

func TestAsyncMgr_DeleteNodePoolAsyncWithUpcomingQueuedNodePool(t *testing.T) {
	ctx := newAsyncGkeManagerTestContext(t)
	// given upcoming node pool queued for creation
	var creationResult MigCreateNodePoolResult
	var initializer *asyncNodeGroupInitializerMock
	var mig *GkeMig
	for range ctx.taskQueueWorkerCount + 1 {
		mig, _ = ctx.sampleMigWithNodePool()
		creationResult, initializer = ctx.createNodePoolAsyncOrFail(mig)
	}
	// when node pool is deleted
	finalizer := ctx.deleteNodePoolAsyncOrFail(mig)
	// then its initializer is invoked with an error
	creationAsyncResult := initializer.AwaitResultOrFail()
	assert.Equal(t, migRefs(creationResult.AllCreatedMigs()), nodeGroupRefs(creationAsyncResult.InjectedMigs))
	assert.ErrorContains(t, creationAsyncResult.Error, "node pool has been deleted when scheduled for creation")
	// and its finalizer is invoked with upcoming node-groups
	deletionAsyncResult := finalizer.AwaitResultOrFail()
	assert.Equal(t, migRefs(creationResult.AllCreatedMigs()), nodeGroupRefs(deletionAsyncResult.Migs))
	assert.NoError(t, deletionAsyncResult.Error)
}

func TestAsyncMgr_CreateInstances(t *testing.T) {
	testCtx := newAsyncGkeManagerTestContext(t)
	asyncGkeManager := testCtx.asyncGkeManager
	mig, _ := testCtx.sampleMigWithNodePool()
	result, _ := testCtx.createNodePoolAsyncOrFail(mig)
	// when instances are created for upcoming node-group
	err := asyncGkeManager.CreateInstances(result.MainCreatedMig, 10)
	assert.NoError(t, err)
	// then its size is updated
	size, err := asyncGkeManager.GetMigSize(result.MainCreatedMig)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), size)
}

func TestAsyncMgr_CreateResizeRequest(t *testing.T) {
	testCtx := newAsyncGkeManagerTestContext(t)
	asyncGkeManager := testCtx.asyncGkeManager
	mig, _ := testCtx.sampleMigWithNodePool()
	result, _ := testCtx.createNodePoolAsyncOrFail(mig)
	// when atomic resize request is created for upcoming node-group
	err := asyncGkeManager.CreateResizeRequest(result.MainCreatedMig, 10)
	assert.NoError(t, err)
	// then its size is updated
	size, err := asyncGkeManager.GetMigSize(result.MainCreatedMig)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), size)
}

func TestAsyncMgr_CreateFlexResizeRequests(t *testing.T) {
	testCtx := newAsyncGkeManagerTestContext(t)
	asyncGkeManager := testCtx.asyncGkeManager
	mig, _ := testCtx.sampleMigWithNodePool()
	result, _ := testCtx.createNodePoolAsyncOrFail(mig)
	// when resize requests are created for upcoming node-group
	err := asyncGkeManager.CreateFlexResizeRequests(result.MainCreatedMig, 10)
	assert.NoError(t, err)
	// then its size is updated
	size, err := asyncGkeManager.GetMigSize(result.MainCreatedMig)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), size)
}

func TestAsyncMgr_CreateQueuedInstances(t *testing.T) {
	testCtx := newAsyncGkeManagerTestContext(t)
	asyncGkeManager := testCtx.asyncGkeManager
	mig, _ := testCtx.sampleMigWithNodePool()
	result, _ := testCtx.createDWSNodePoolAsyncOrFail(mig)
	// when a resize request is created for upcoming node-group
	err := asyncGkeManager.CreateQueuedInstances(
		prpods.ProvReqID{Namespace: "default", Name: "pr"}, result.MainCreatedMig, 10, manager.UpdateProvReqDetails)
	assert.NoError(t, err)
	// then its size is updated
	size, err := asyncGkeManager.GetMigSize(result.MainCreatedMig)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), size)
}

func TestAsyncMgr_CreateQueuedInstancesWithoutDWS(t *testing.T) {
	testCtx := newAsyncGkeManagerTestContext(t)
	asyncGkeManager := testCtx.asyncGkeManager
	mig, _ := testCtx.sampleMigWithNodePool()
	result, _ := testCtx.createNodePoolAsyncOrFail(mig)
	// when a resize request is created for upcoming node-group
	err := asyncGkeManager.CreateQueuedInstances(
		prpods.ProvReqID{Namespace: "default", Name: "pr"}, result.MainCreatedMig, 10, manager.UpdateProvReqDetails)
	assert.Error(t, err)
}

func TestAsyncMgr_AdvanceResizeRequestCleanUp(t *testing.T) {
	testCtx := newAsyncGkeManagerTestContext(t)
	asyncGkeManager := testCtx.asyncGkeManager
	mig, _ := testCtx.sampleMigWithNodePool()
	result, _ := testCtx.createNodePoolAsyncOrFail(mig)
	// when atomic resize request is created for upcoming node-group
	err := asyncGkeManager.CreateResizeRequest(result.MainCreatedMig, 10)
	assert.NoError(t, err)
	// and atomic resize request is deleted for upcoming node-group
	err = asyncGkeManager.AdvanceResizeRequestCleanUp(resizerequestclient.ResizeRequestStatus{
		MigName:  result.MainCreatedMig.GceRef().Name,
		Zone:     result.MainCreatedMig.GceRef().Zone,
		ResizeBy: 11,
	})
	assert.Error(t, err)
}

func nodeGroupRefs(groups []interfaces.AutoprovisionedNodeGroup) []gce.GceRef {
	var result []gce.GceRef
	for _, group := range groups {
		mig := group.(*GkeMig)
		result = append(result, mig.GceRef())
	}
	return result
}

type asyncGkeManagerTestContext struct {
	t                          *testing.T
	domainUrl                  string
	nodePoolIdx                int
	migIdx                     int
	taskQueueWorkerCount       int
	taskQueueSize              int
	gkeManager                 *testExtendedGkeManager
	asyncGkeManager            *asyncGkeManager
	processedNodePoolsObserver *processedNodePoolsObserver
}

func newAsyncGkeManagerTestContext(t *testing.T) *asyncGkeManagerTestContext {
	domainURL := ""
	// some tests require to keep it even
	taskQueueWorkerCount := 2
	gkeManager := newTestExtendedGkeManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	taskQueueSize := 10
	gceCache := gce.NewGceCache()
	cache := NewGkeCache(gceCache, nodetemplate.NewCache())
	asyncGkeManager := newAsyncGkeManager(ctx, gkeManager, cache, taskQueueWorkerCount, taskQueueSize, domainURL)
	processedNodePoolsObserver := newProcessedNodePoolsObserver(asyncGkeManager)
	asyncGkeManager.processedNodePoolListener = processedNodePoolsObserver.onProcessedNodePool
	return &asyncGkeManagerTestContext{
		t:                          t,
		domainUrl:                  domainURL,
		taskQueueSize:              taskQueueSize,
		taskQueueWorkerCount:       taskQueueWorkerCount,
		gkeManager:                 gkeManager,
		asyncGkeManager:            asyncGkeManager,
		processedNodePoolsObserver: processedNodePoolsObserver,
	}
}

func (c *asyncGkeManagerTestContext) waitForNodePoolsToBeProcessed(duration time.Duration) error {
	return c.processedNodePoolsObserver.waitForNodePoolsToBeProcessed(duration)
}

func (c *asyncGkeManagerTestContext) createNodePoolAsyncOrFail(mig *GkeMig) (MigCreateNodePoolResult, *asyncNodeGroupInitializerMock) {
	initializer := newAsyncNodeGroupInitializer(c.t)
	result, err := c.asyncGkeManager.CreateNodePoolAsync(mig, initializer, initializer)
	assert.NoError(c.t, err)
	assert.NotEmpty(c.t, result.AllCreatedMigs())
	return result, initializer
}

func (c *asyncGkeManagerTestContext) createDWSNodePoolAsyncOrFail(mig *GkeMig) (MigCreateNodePoolResult, *asyncDWSNodeGroupInitializerMock) {
	initializer := newAsyncDWSNodeGroupInitializerMock(c.t)
	result, err := c.asyncGkeManager.CreateNodePoolAsync(mig, initializer, initializer)
	assert.NoError(c.t, err)
	assert.NotEmpty(c.t, result.AllCreatedMigs())
	return result, initializer
}

func (c *asyncGkeManagerTestContext) deleteNodePoolAsyncOrFail(mig *GkeMig) *asyncNodeGroupFinalizerMock {
	finalizer := newAsyncNodeGroupFinalizer(c.t)
	err := c.asyncGkeManager.DeleteNodePoolAsync(mig, finalizer)
	assert.NoError(c.t, err)
	return finalizer
}

// awaitAsyncTasks awaits all node pools to reach processed state.
func (c *asyncGkeManagerTestContext) awaitAsyncTasks() {
	err := c.waitForNodePoolsToBeProcessed(testAsyncGkeMgrDefaultTimeout)
	if err != nil {
		assert.FailNow(c.t, "timeout: node pool did not reach processed state", "node pools did not reach processed state in %v: %v", testAsyncGkeMgrDefaultTimeout, err)
	}
}

// awaitAsyncTasksAndRefresh awaits all node pools to reach processed state and refreshes asynGkeManager.
// Tests should not invoke asyncGkeManager.Refresh() directly because of a race condition between
// node pool state change after intialization/finalization and clean up of processed node pools done during refresh.
func (c *asyncGkeManagerTestContext) awaitAsyncTasksAndRefresh() {
	c.awaitAsyncTasks()
	err := c.asyncGkeManager.Refresh()
	assert.NoError(c.t, err)
}

func (c *asyncGkeManagerTestContext) sampleCreatedMigWithNodePool() (*GkeMig, *gkeclient.NodePool) {
	mig, nodePool := c.sampleMigWithNodePool()
	c.gkeManager.addNodePoolWithMig(nodePool, mig)
	return mig, nodePool
}

func (c *asyncGkeManagerTestContext) assertMigRegisteredInCache(ng interfaces.AutoprovisionedNodeGroup) {
	mig := ng.(*GkeMig)
	ref := mig.GceRef()
	registered := !c.asyncGkeManager.cache.LastMigRegistration(ref).IsZero()
	assert.Truef(c.t, registered, "Expected mig %v to be registered in the cache", ref)
}

func (c *asyncGkeManagerTestContext) assertNoOngoingNodePoolOps() {
	c.awaitAsyncTasksAndRefresh()
	c.asyncGkeManager.mutex.Lock()
	defer c.asyncGkeManager.mutex.Unlock()
	var upcoming []string
	for name := range c.asyncGkeManager.upcomingNodePools {
		upcoming = append(upcoming, name)
	}
	assert.Emptyf(c.t, upcoming, "Expected no upcoming node pools, found: %v", upcoming)
	var terminating []string
	for name := range c.asyncGkeManager.terminatingNodePools {
		terminating = append(terminating, name)
	}
	assert.Emptyf(c.t, upcoming, "Expected no terminating node pools, found: %v", terminating)
	var uninitialized []string
	for name := range c.asyncGkeManager.uninitializedGkeMigs {
		uninitialized = append(uninitialized, name)
	}
	assert.Emptyf(c.t, upcoming, "Expected no uninitialized migs, found: %v", uninitialized)
	assert.Emptyf(c.t, c.asyncGkeManager.bufferedCleanUps, "Expected no bufferedCleanUps, found: %v", c.asyncGkeManager.bufferedCleanUps)
	syncingNodePools := c.asyncGkeManager.nodePoolRegistrationObserver.syncingNodePoolNames()
	assert.Emptyf(c.t, syncingNodePools, "Expected no syncing node pools, found: %v", syncingNodePools)
}

func (c *asyncGkeManagerTestContext) sampleMigWithNodePool() (*GkeMig, *gkeclient.NodePool) {
	c.nodePoolIdx = c.nodePoolIdx + 1
	c.migIdx = c.migIdx + 1
	idx := c.nodePoolIdx
	migIdx := c.migIdx
	projectName := "test-project"
	nodePoolName := fmt.Sprintf("test-node-pool-%d", idx)
	nodePoolSpec := &gkeclient.NodePoolSpec{
		ClusterNetworkPath:    "projects/test-gke-dev/global/networks/test-network",
		ClusterSubnetworkPath: "projects/mendelski-gke-dev/regions/us-central1/subnetworks/test-subnet",
		Locations: []string{
			"test-zone-a",
			"test-zone-b",
			"test-zone-c",
		},
	}
	var instanceGroupUrls []string
	for i, loc := range nodePoolSpec.Locations {
		ref := gce.GceRef{Project: projectName, Zone: loc, Name: fmt.Sprintf("mig-%d-async-%d", migIdx, i)}
		url := fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instanceGroupManagers/%s", ref.Project, ref.Zone, ref.Name)
		instanceGroupUrls = append(instanceGroupUrls, url)
	}
	nodePool := &gkeclient.NodePool{
		Name:              nodePoolName,
		Spec:              nodePoolSpec,
		InstanceGroupUrls: instanceGroupUrls,
	}
	mig := &GkeMig{
		gceRef: gce.GceRef{Project: projectName, Zone: "test-zone", Name: fmt.Sprintf("mig-%d", migIdx)},
		spec: &gkeclient.NodePoolSpec{
			PlacementGroup: placement.Spec{GroupId: "", Policy: ""},
			Labels: map[string]string{
				(labels.GkeNodePoolLabel): nodePoolName,
			},
		},
	}
	AddMigsToNodePool(nodePool.Name, mig)
	c.gkeManager.setNodePoolSpecForMig(mig, nodePool.Spec)
	return mig, nodePool
}

func expectedUpcomingMigRefs(mig *GkeMig, nodePoolSpec *gkeclient.NodePoolSpec) []gce.GceRef {
	var refs []gce.GceRef
	for i, loc := range nodePoolSpec.Locations {
		refs = append(refs, gce.GceRef{Project: mig.gceRef.Project, Zone: loc, Name: fmt.Sprintf("%s-async-%d", mig.gceRef.Name, i)})
	}
	return refs
}

func expectedCreatedMigRefs(nodePool *gkeclient.NodePool) []gce.GceRef {
	var result []gce.GceRef
	for _, url := range nodePool.InstanceGroupUrls {
		result = append(result, parseInstanceGroupUrl(url))
	}
	return result
}

func parseInstanceGroupUrl(url string) gce.GceRef {
	chunks := strings.Split(url, "/")
	if !strings.HasPrefix(url, "https://www.googleapis.com/compute/v1/projects/") || len(chunks) != 11 {
		panic(fmt.Sprintf("could not parse instanceGroup url: %s", url))
	}
	return gce.GceRef{
		Project: chunks[6],
		Zone:    chunks[8],
		Name:    chunks[10],
	}
}

func migRefs(migs []*GkeMig) []gce.GceRef {
	var result []gce.GceRef
	for _, mig := range migs {
		result = append(result, mig.GceRef())
	}
	return result
}

// asyncNodeGroupInitializerMock implements interfaces.AsyncNodeGroupFinalizer
// Used to await node pool initialization in tests.
type asyncNodeGroupInitializerMock struct {
	t                   *testing.T
	executed            chan struct{}
	blockInitialization chan struct{}
	mutex               sync.Mutex // guards result and target sizes
	result              *interfaces.AsyncCreateNodePoolResult
	targetSizes         map[string]int64
}

func newAsyncNodeGroupInitializer(t *testing.T) *asyncNodeGroupInitializerMock {
	return &asyncNodeGroupInitializerMock{
		t:                   t,
		executed:            make(chan struct{}, 1),
		blockInitialization: make(chan struct{}, 1),
		targetSizes:         make(map[string]int64),
	}
}

func (m *asyncNodeGroupInitializerMock) GetTargetSize(nodeGroupName string) int64 {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.targetSizes[nodeGroupName]
}

func (m *asyncNodeGroupInitializerMock) SetTargetSize(nodeGroupName string, size int64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.targetSizes[nodeGroupName] = size
}

func (m *asyncNodeGroupInitializerMock) ChangeTargetSize(nodeGroupName string, delta int64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	size := m.targetSizes[nodeGroupName] + delta
	if size < 0 {
		size = 0
	}
	m.targetSizes[nodeGroupName] = size
}

func (m *asyncNodeGroupInitializerMock) TargetSizes() map[string]int64 {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.targetSizes
}

func (m *asyncNodeGroupInitializerMock) InitializeNodeGroup(result interfaces.AsyncCreateNodePoolResult) {
	m.mutex.Lock()
	if m.result != nil {
		m.mutex.Unlock()
		m.t.Fatalf("initializer was already executed")
		return
	}
	m.result = &result
	select {
	case m.executed <- struct{}{}:
	default:
	}
	// unlock after triggering executed subscribers for AwaitResultOrFail to be called when initialization is blocked
	m.mutex.Unlock()
	select {
	case m.blockInitialization <- struct{}{}:
	case <-time.After(testAsyncGkeMgrDefaultTimeout):
		assert.FailNow(m.t, "asyncNodeGroupInitializer was blocked beyond the timeout of: %v", testAsyncGkeMgrDefaultTimeout)
	}
	<-m.blockInitialization
}

type asyncDWSNodeGroupInitializerMock struct {
	asyncNodeGroupInitializerMock
}

func newAsyncDWSNodeGroupInitializerMock(t *testing.T) *asyncDWSNodeGroupInitializerMock {
	return &asyncDWSNodeGroupInitializerMock{*newAsyncNodeGroupInitializer(t)}
}

func (m *asyncDWSNodeGroupInitializerMock) ScheduleProvReqResize(pr prpods.ProvReqID, nodeGroupName string, delta int64, shouldUpdateProvReqDetails manager.ShouldUpdateProvReqDetails) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	size := m.targetSizes[nodeGroupName] + delta
	if size < 0 {
		size = 0
	}
	m.targetSizes[nodeGroupName] = size
}

func (m *asyncNodeGroupInitializerMock) AwaitResultOrFail() *interfaces.AsyncCreateNodePoolResult {
	select {
	case x := <-m.executed:
		m.executed <- x
	case <-time.After(testAsyncGkeMgrDefaultTimeout):
		m.t.Fatalf("initializer was not executed within the timeout: %v", testAsyncGkeMgrDefaultTimeout)
		return nil
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.result
}

func (m *asyncNodeGroupInitializerMock) AwaitSuccessfulResultOrFail(mig *GkeMig, nodePool *gkeclient.NodePool) *interfaces.AsyncCreateNodePoolResult {
	asyncResult := m.AwaitResultOrFail()
	assert.NoError(m.t, asyncResult.Error)
	assert.Equal(m.t, expectedUpcomingMigRefs(mig, nodePool.Spec), nodeGroupRefs(asyncResult.InjectedMigs))
	assert.Equal(m.t, expectedCreatedMigRefs(nodePool), nodeGroupRefs(asyncResult.CreationResult.AllCreatedNodeGroups()))
	return asyncResult
}

func (m *asyncNodeGroupInitializerMock) BlockInitialization() {
	select {
	case m.blockInitialization <- struct{}{}:
	default:
	}
}

func (m *asyncNodeGroupInitializerMock) UnblockInitialization() {
	select {
	case <-m.blockInitialization:
	default:
	}
}

// asyncNodeGroupFinalizerMock implements interfaces.AsyncNodeGroupFinalizer
// Used to await node pool finalization in tests.
type asyncNodeGroupFinalizerMock struct {
	t        *testing.T
	executed chan struct{}
	mutex    sync.Mutex // guards result
	result   *interfaces.AsyncDeleteNodePoolResult
}

func newAsyncNodeGroupFinalizer(t *testing.T) *asyncNodeGroupFinalizerMock {
	return &asyncNodeGroupFinalizerMock{
		t:        t,
		executed: make(chan struct{}, 1),
	}
}

func (m *asyncNodeGroupFinalizerMock) FinalizeNodeGroup(result interfaces.AsyncDeleteNodePoolResult) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.result != nil {
		m.t.Fatalf("finalizer was already executed")
		return
	}
	m.result = &result
	m.executed <- struct{}{}
}

func (m *asyncNodeGroupFinalizerMock) AwaitResultOrFail() *interfaces.AsyncDeleteNodePoolResult {
	select {
	case x := <-m.executed:
		m.executed <- x
	case <-time.After(testAsyncGkeMgrDefaultTimeout):
		m.t.Fatalf("finalizer was not executed within the timeout: %v", testAsyncGkeMgrDefaultTimeout)
		return nil
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.result
}

func (m *asyncNodeGroupFinalizerMock) AwaitSuccessfulResultOrFail() *interfaces.AsyncDeleteNodePoolResult {
	result := m.AwaitResultOrFail()
	assert.NoError(m.t, result.Error)
	return result
}

type repeatingError struct {
	error               error
	NumberOfRepetitions int
	FollowedBySuccess   bool
}

func createRepeatingError(err error, numberOfRepetitions int, followedBySuccess bool) repeatingError {
	return repeatingError{error: err, NumberOfRepetitions: numberOfRepetitions, FollowedBySuccess: followedBySuccess}
}

type testExtendedGkeManager struct {
	extendedGkeManager
	mock                   *GkeManagerMock
	t                      *testing.T
	mutex                  sync.Mutex
	domainUrl              string
	nodePools              map[*gkeclient.NodePool][]*GkeMig
	nodePoolSpecs          map[*GkeMig]*gkeclient.NodePoolSpec
	nodePoolCreationErrs   map[string]repeatingError
	nodePoolDeletionErrs   map[string]repeatingError
	nodePoolCreationSignal map[string]chan *gkeclient.NodePool
	nodePoolDeletionSignal map[string]chan struct{}
	migInstances           map[string]int64
	reportEmptyClusterOnce bool
	deletedNodePools       map[string]struct{}
}

func newTestExtendedGkeManager(t *testing.T) *testExtendedGkeManager {
	gkeManagerMock := &GkeManagerMock{}
	gkeManagerMock.On("Refresh").Return(nil)
	gkeManagerMock.On("RefreshForce").Return(nil)
	return &testExtendedGkeManager{
		extendedGkeManager:     gkeManagerMock,
		mock:                   gkeManagerMock,
		t:                      t,
		domainUrl:              "",
		nodePools:              make(map[*gkeclient.NodePool][]*GkeMig),
		nodePoolSpecs:          make(map[*GkeMig]*gkeclient.NodePoolSpec),
		nodePoolCreationErrs:   make(map[string]repeatingError),
		nodePoolDeletionErrs:   make(map[string]repeatingError),
		nodePoolCreationSignal: make(map[string]chan *gkeclient.NodePool),
		nodePoolDeletionSignal: make(map[string]chan struct{}),
		migInstances:           make(map[string]int64),
		deletedNodePools:       make(map[string]struct{}),
	}
}

func (c *testExtendedGkeManager) addNodePoolWithMig(nodePool *gkeclient.NodePool, mig *GkeMig) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.nodePools[nodePool] = []*GkeMig{mig}
}

func (c *testExtendedGkeManager) setNodePoolSpecForMig(mig *GkeMig, nodePoolSpec *gkeclient.NodePoolSpec) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.nodePoolSpecs[mig] = nodePoolSpec
}

// NewNodePoolSpec overrides extendedGkeManager
func (c *testExtendedGkeManager) NewNodePoolSpec(mig *GkeMig) (*gkeclient.NodePoolSpec, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if spec, found := c.nodePoolSpecs[mig]; found {
		return spec, nil
	}
	return nil, fmt.Errorf("nodePoolSpec not defined for mig %s", mig.Id())
}

// CreateNodePoolNoRefresh overrides extendedGkeManager
func (c *testExtendedGkeManager) CreateNodePoolNoRefresh(nodePoolName string, nodePoolSpec *gkeclient.NodePoolSpec) error {
	creationErr := c.nodePoolCreationErrs[nodePoolName]
	if creationErr.NumberOfRepetitions > 1 || (creationErr.NumberOfRepetitions == 1 && creationErr.FollowedBySuccess) {
		creationErr.NumberOfRepetitions--
		c.nodePoolCreationErrs[nodePoolName] = creationErr
		return creationErr.error
	}

	c.mutex.Lock()
	if _, found := c.nodePoolCreationSignal[nodePoolName]; !found {
		c.nodePoolCreationSignal[nodePoolName] = make(chan *gkeclient.NodePool, 1)
	}
	creationCh := c.nodePoolCreationSignal[nodePoolName]
	c.mutex.Unlock()
	nodePool := <-creationCh
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if creationErr.error == nil || creationErr.NumberOfRepetitions == 0 {
		migs, migsErr := nodePoolMIGs(c, c.domainUrl, *nodePool)
		AddMigsToNodePool(nodePool.Name, migs...)
		assert.NoError(c.t, migsErr)
		c.nodePools[nodePool] = migs
		return nil
	}
	return creationErr.error
}

func (c *testExtendedGkeManager) failNodePoolCreation(nodePoolName string, err error) {
	c.failNodePoolCreationWithRepeatingError(nodePoolName, err, 1, false)
}

func (c *testExtendedGkeManager) failNodePoolCreationWithRepeatingError(nodePoolName string, err error, numberOfRepetitions int, followedBySuccess bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.nodePoolCreationErrs[nodePoolName] = createRepeatingError(err, numberOfRepetitions, followedBySuccess)
}

func (c *testExtendedGkeManager) finalizeNodePoolCreation(nodePool *gkeclient.NodePool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	nodePoolName := nodePool.Name
	if _, found := c.nodePoolCreationSignal[nodePoolName]; !found {
		c.nodePoolCreationSignal[nodePoolName] = make(chan *gkeclient.NodePool, 1)
	}
	creationCh := c.nodePoolCreationSignal[nodePoolName]
	creationCh <- nodePool
}

func (c *testExtendedGkeManager) SetMigSize(mig gce.Mig, size int64) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.migInstances[mig.Id()] = size
	return nil
}

func (c *testExtendedGkeManager) CreateInstances(mig gce.Mig, delta int64) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.migInstances[mig.Id()] += delta
	return nil
}

func (c *testExtendedGkeManager) GetMigInstances(migId string) int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return int(c.migInstances[migId])
}

// DeleteNodePoolNoRefresh overrides extendedGkeManager
func (c *testExtendedGkeManager) DeleteNodePoolNoRefresh(mig *GkeMig) error {
	c.mutex.Lock()
	nodePoolName := mig.NodePoolName()
	if _, found := c.nodePoolDeletionSignal[nodePoolName]; !found {
		c.nodePoolDeletionSignal[nodePoolName] = make(chan struct{}, 1)
	}
	deletionCh := c.nodePoolDeletionSignal[nodePoolName]
	c.mutex.Unlock()
	<-deletionCh
	c.mutex.Lock()
	defer c.mutex.Unlock()
	err := c.nodePoolDeletionErrs[nodePoolName]
	if err.error == nil {
		var nodePool *gkeclient.NodePool = nil
		for np := range c.nodePools {
			if np.Name == nodePoolName {
				nodePool = np
			}
		}
		delete(c.nodePools, nodePool)
	}
	c.deletedNodePools[nodePoolName] = struct{}{}
	return err.error
}

func (c *testExtendedGkeManager) attemptedDeletion(nodePoolName string) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	_, found := c.deletedNodePools[nodePoolName]
	return found
}

func (c *testExtendedGkeManager) failNodePoolDeletion(nodePoolName string, err error) {
	if err == nil {
		c.failNodePoolDeletionWithRepeatingError(nodePoolName, createRepeatingError(nil, 0, false))
	} else {
		c.failNodePoolDeletionWithRepeatingError(nodePoolName, createRepeatingError(err, 1, false))
	}
}

func (c *testExtendedGkeManager) failNodePoolDeletionWithRepeatingError(nodePoolName string, err repeatingError) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.nodePoolDeletionErrs[nodePoolName] = err
}

func (c *testExtendedGkeManager) finalizeNodePoolDeletion(nodePoolName string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if _, found := c.nodePoolDeletionSignal[nodePoolName]; !found {
		c.nodePoolDeletionSignal[nodePoolName] = make(chan struct{}, 1)
	}
	deletionCh := c.nodePoolDeletionSignal[nodePoolName]
	select {
	case deletionCh <- struct{}{}:
	default:
	}
}

// GetCluster overides extendedGkeManager
func (c *testExtendedGkeManager) GetCluster() (gkeclient.Cluster, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.reportEmptyClusterOnce {
		c.reportEmptyClusterOnce = false
		return gkeclient.Cluster{}, nil
	}
	var nodePools []gkeclient.NodePool
	for np := range c.nodePools {
		nodePools = append(nodePools, *np)
	}
	return gkeclient.Cluster{NodePools: nodePools}, nil
}

// GetGkeMigs overides extendedGkeManager
func (c *testExtendedGkeManager) GetGkeMigs() []*GkeMig {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var migs []*GkeMig
	for _, npm := range c.nodePools {
		migs = append(migs, npm...)
	}
	return migs
}

// GetAllNodePoolNames overides extendedGkeManager
func (c *testExtendedGkeManager) GetAllNodePoolNames() sets.Set[string] {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	names := sets.New[string]()
	for np := range c.nodePools {
		names.Insert(np.Name)
	}
	return names
}

func (c *testExtendedGkeManager) reportOnceEmptyCluster() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.reportEmptyClusterOnce = true
}

type processedNodePoolsObserver struct {
	asyncGkeManager *asyncGkeManager
	// processedNodePoolsCond used for waiting until all node pools reach processed state.
	processedNodePoolsCond *sync.Cond
}

func newProcessedNodePoolsObserver(asyncGkeManager *asyncGkeManager) *processedNodePoolsObserver {
	return &processedNodePoolsObserver{
		asyncGkeManager:        asyncGkeManager,
		processedNodePoolsCond: sync.NewCond(&asyncGkeManager.mutex),
	}
}

func (o *processedNodePoolsObserver) onProcessedNodePool(nodePoolName string) {
	// Calls not holding the main mutex must lock before broadcasting.
	// Otherwise there may be a race condition between checking node pool states ( node pool state is guarded by a separate mutex)
	// and the processedNodePoolsCond.Wait().
	o.processedNodePoolsCond.L.Lock()
	defer o.processedNodePoolsCond.L.Unlock()
	o.processedNodePoolsCond.Broadcast()
}

func (o *processedNodePoolsObserver) waitForNodePoolsToBeProcessed(duration time.Duration) error {
	asyncGkeManager := o.asyncGkeManager
	o.processedNodePoolsCond.L.Lock()
	defer o.processedNodePoolsCond.L.Unlock()
	hasUnprocessedNodePools := func() bool {
		for _, upcoming := range asyncGkeManager.upcomingNodePools {
			if status, _ := upcoming.status(); status != nodePoolCreationStatusProcessed {
				return true
			}
		}
		for _, terminating := range asyncGkeManager.terminatingNodePools {
			if status, _ := terminating.status(); status != nodePoolTerminationStatusProcessed {
				return true
			}
		}
		return false
	}
	ctx, cancel := context.WithTimeout(asyncGkeManager.context, duration)
	defer cancel()
	context.AfterFunc(ctx, func() {
		// Lock is required to make sure that the Broadcast is not executed before Wait.
		// It would result in a lost signal and potantial deadlock.
		o.processedNodePoolsCond.L.Lock()
		defer o.processedNodePoolsCond.L.Unlock()
		o.processedNodePoolsCond.Broadcast()
	})
	for hasUnprocessedNodePools() {
		o.processedNodePoolsCond.Wait()
		if err := ctx.Err(); err != nil {
			// timeout
			return err
		}
	}
	return nil
}
