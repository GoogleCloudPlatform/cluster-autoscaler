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

package scaleup

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate/utils"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/client-go/kubernetes/fake"
	kube_record "k8s.io/client-go/tools/record"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/manager"
	prpods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
)

func TestInitializeNodeGroup_NoDeadlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t)
		prCache := provreqcache.NewQueuedProvisioningCache(fakeClient)

		fakeKubeClient := fake.NewSimpleClientset()
		fakeLogRecorder, _ := utils.NewStatusMapRecorder(fakeKubeClient, "kube-system", kube_record.NewFakeRecorder(5), false, "test-configmap")

		cloudProvider := &gke.GkeCloudProviderMock{}
		cloudProvider.On("GetAvailableGPUTypes").Return(map[string]struct{}{})
		cloudProvider.On("GetNodeGpuConfig", mock.Anything).Return((*cloudprovider.GpuConfig)(nil))

		autoscalingContext := &ca_context.AutoscalingContext{
			CloudProvider: cloudProvider,
			AutoscalingKubeClients: ca_context.AutoscalingKubeClients{
				LogRecorder: fakeLogRecorder,
			},
		}

		scaleUpExecutor := newScaleUpExecutor(autoscalingContext, &nodegroupchange.NodeGroupChangeObserversList{}, nil)

		initializer := NewAsyncDWSNodeGroupInitializer(
			framework.NewTestNodeInfo(&v1.Node{}), // nodeInfo
			nil,                                   // triggeringPods
			scaleUpExecutor,
			taints.TaintConfig{},                 // taintConfig
			nil,                                  // daemonSets
			&status.NoOpScaleUpStatusProcessor{}, // scaleUpStatusProcessor
			autoscalingContext,
			prCache,
		)

		gkeManagerMock := &gke.GkeManagerMock{}
		gkeManagerMock.On("CreateQueuedInstances", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)
		gkeManagerMock.On("GetMigTemplateNodeInfo", mock.Anything).Return((*framework.NodeInfo)(nil), assert.AnError)
		gkeManagerMock.On("GetMigSize", mock.Anything).Return(int64(0), nil)

		// Set up a valid GKE Node Group (GkeMig) that uses our mock manager.
		// Fails when it calls `CreateQueuedInstances`, reliably
		// simulating a scale-up failure.
		gceRef := gce.GceRef{Project: "project", Zone: "zone", Name: "ng1"}
		ng := gke.NewGkeMig(gceRef, "", gkeManagerMock)

		// Schedule PRs for the upcoming Node Group mapping to simulate
		// multiple scale-up requests that will all fail during initialization.
		initializer.ScheduleProvReqResize(prpods.ProvReqID{Namespace: "ns1", Name: "pr1"}, "upcoming-ng1", 1, manager.DoNotUpdateProvReqDetails)
		initializer.ScheduleProvReqResize(prpods.ProvReqID{Namespace: "ns1", Name: "pr2"}, "upcoming-ng1", 1, manager.DoNotUpdateProvReqDetails)
		initializer.ScheduleProvReqResize(prpods.ProvReqID{Namespace: "ns1", Name: "pr3"}, "upcoming-ng1", 1, manager.DoNotUpdateProvReqDetails)

		// Since len(plans) will be 1 (for ng.Id()), but the plan has 3 items that will all fail,
		// without a channel with the appropriate size in executeInitializationPlans,
		// it would deadlock trying to write 3 errors to a channel of size 1.
		result := nodegroups.AsyncNodeGroupCreationResult{
			CreationResult: nodegroups.CreateNodeGroupResult{
				MainCreatedNodeGroup: ng,
			},
			CreatedToUpcomingMapping: map[string]string{ng.Id(): "upcoming-ng1"},
		}

		done := make(chan struct{})

		go func() {
			initializer.InitializeNodeGroup(result)
			close(done)
		}()

		// Wait for the background goroutine to either finish or block indefinitely (deadlock).
		synctest.Wait()

		select {
		case <-done:
			// Success: InitializeNodeGroup finished without deadlocking.
			assert.True(t, true)
		default:
			t.Fatal("InitializeNodeGroup deadlocked!")
		}
	})
}
